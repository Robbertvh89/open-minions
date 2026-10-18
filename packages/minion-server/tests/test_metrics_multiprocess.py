"""/metrics sums the samples of all worker processes (TASK-7.4.6, prometheus multiprocess mode)."""

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest
from minion_server.metrics import Metrics, mark_worker_dead, prepare_metrics_dir
from prometheus_client import multiprocess
from prometheus_client.parser import text_string_to_metric_families

_CHILD = """
import sys
from minion_server.metrics import Metrics
m = Metrics()
for _ in range(int(sys.argv[1])):
    m.observe("/x", "GET", 200)
m.observe_acquire("assigned", 0.01)
"""


def _spawn(directory: Path, n: int) -> None:
    env = {**os.environ, "PROMETHEUS_MULTIPROC_DIR": str(directory)}
    subprocess.run([sys.executable, "-c", _CHILD, str(n)], env=env, check=True)  # noqa: S603


def test_render_sums_counters_and_histograms_across_processes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _spawn(tmp_path, 3)
    _spawn(tmp_path, 4)  # two separate processes (pids), both gone by the time we render
    assert len(list(tmp_path.glob("*.db"))) >= 4
    monkeypatch.setenv("PROMETHEUS_MULTIPROC_DIR", str(tmp_path))
    text = Metrics().render().decode()
    samples = {
        (s.name, tuple(sorted(s.labels.items()))): s.value
        for fam in text_string_to_metric_families(text)
        for s in fam.samples
    }
    key = ("minion_http_requests_total", (("method", "GET"), ("route", "/x"), ("status", "200")))
    assert samples[key] == 7
    assert samples[("minion_acquire_seconds_count", (("outcome", "assigned"),))] == 2


def test_default_dir_is_a_fresh_private_mkdtemp() -> None:
    a, a_tmp = prepare_metrics_dir(None)
    b, _ = prepare_metrics_dir(None)
    try:
        assert a != b
        assert a_tmp is True
        assert Path(a).stat().st_mode & 0o777 == 0o700
        assert Path(a).stat().st_uid == os.geteuid()
    finally:
        Path(a).rmdir()
        Path(b).rmdir()


def test_operator_dir_with_foreign_mode_is_refused(tmp_path: Path) -> None:
    d = tmp_path / "m"
    d.mkdir()
    d.chmod(0o755)
    (d / "counter_1.db").write_bytes(b"x")
    with pytest.raises(SystemExit) as exc:
        prepare_metrics_dir(str(d))
    assert exc.value.code == 2
    assert (d / "counter_1.db").exists()  # refused before touching anything


def test_operator_dir_that_is_a_symlink_is_refused(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir(mode=0o700)
    (real / "counter_1.db").write_bytes(b"x")
    link = tmp_path / "link"
    link.symlink_to(real)
    with pytest.raises(SystemExit) as exc:
        prepare_metrics_dir(str(link))
    assert exc.value.code == 2
    assert (real / "counter_1.db").exists()


def test_operator_dir_that_is_a_file_is_refused(tmp_path: Path) -> None:
    f = tmp_path / "f"
    f.write_bytes(b"x")
    with pytest.raises(SystemExit):
        prepare_metrics_dir(str(f))


def test_only_metric_files_are_deleted(tmp_path: Path) -> None:
    d = tmp_path / "m"
    d.mkdir(mode=0o700)
    outside = tmp_path / "outside.db"
    outside.write_bytes(b"keep")
    for name in ("counter_1.db", "histogram_2.db", "gauge_all_3.db"):
        (d / name).write_bytes(b"x")
    (d / "notes.txt").write_bytes(b"keep")
    (d / "other.db").write_bytes(b"keep")
    (d / "counter_link.db").symlink_to(outside)  # a symlink named like a metric file
    (d / "sub").mkdir()
    (d / "sub" / "counter_9.db").write_bytes(b"keep")
    path, is_tmp = prepare_metrics_dir(str(d))
    assert (path, is_tmp) == (str(d), False)
    left = sorted(p.name for p in d.iterdir())
    assert left == ["counter_link.db", "notes.txt", "other.db", "sub"]
    assert outside.read_bytes() == b"keep"
    assert (d / "sub" / "counter_9.db").exists()


def test_missing_operator_dir_is_created_private(tmp_path: Path) -> None:
    d = tmp_path / "new"
    prepare_metrics_dir(str(d))
    assert Path(d).stat().st_mode & 0o777 == 0o700


def test_mark_worker_dead_calls_prometheus_with_this_pid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[int] = []
    monkeypatch.setattr(multiprocess, "mark_process_dead", calls.append)
    monkeypatch.delenv("PROMETHEUS_MULTIPROC_DIR", raising=False)
    mark_worker_dead()
    assert calls == []
    monkeypatch.setenv("PROMETHEUS_MULTIPROC_DIR", str(tmp_path))
    mark_worker_dead()
    assert calls == [os.getpid()]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.mark.integration
def test_two_worker_server_metrics_total_equals_requests_sent(tmp_path: Path) -> None:
    port = _free_port()
    env = {
        **os.environ,
        "MINION_DATABASE_URL": "postgresql+asyncpg://x:y@127.0.0.1:1/none",  # never connected
        "MINION_S3_ACCESS_KEY": "ak",
        "MINION_S3_SECRET_KEY": "sk",
        "MINION_MODELS_FILE": str(Path(__file__).parent / "models.test.yaml"),
        "MINION_SERVER_WORKERS": "2",
        "MINION_METRICS_ENABLED": "true",
        "MINION_PORT": str(port),
        "MINION_SPLITTER_POLL_S": "60",
    }
    env.pop("MINION_METRICS_MULTIPROC_DIR", None)
    env.pop("PROMETHEUS_MULTIPROC_DIR", None)
    n = 40
    url = f"http://127.0.0.1:{port}"
    proc = subprocess.Popen(
        [sys.executable, "-c", "from minion_server.app import main; main()"], env=env, cwd=tmp_path
    )
    try:
        deadline = time.monotonic() + 60
        while True:
            try:
                if httpx.get(f"{url}/healthz").status_code == 200:
                    break
            except httpx.TransportError:
                pass
            assert proc.poll() is None, "server exited"
            assert time.monotonic() < deadline, "server did not start"
            time.sleep(0.3)
        ready = httpx.get(f"{url}/metrics").text  # health probes above are counted already
        before = _healthz_total(ready)
        with httpx.Client(base_url=url) as c:  # keep-alive pins a connection: use fresh clients
            for _ in range(n):
                assert httpx.get(f"{url}/healthz").status_code == 200
            assert c.get("/healthz").status_code == 200
        total = _healthz_total(httpx.get(f"{url}/metrics").text)
        assert total - before == n + 1
    finally:
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=30) == 0


def _healthz_total(text: str) -> float:
    return sum(
        s.value
        for fam in text_string_to_metric_families(text)
        for s in fam.samples
        if s.name == "minion_http_requests_total" and s.labels["route"] == "/healthz"
    )
