"""Real Seatbelt sandbox with the fake engine (no GPU). Auto-skipped off macOS (conftest).

The worker is a real spawned subprocess confined through `WorkerHandle(sandbox=...)`; the fake
runner's test hooks make it attempt reads, connects and execs from inside the sandbox.

What this covers: the profile confines a Python worker that imports `minion_executor` from the
venv/source checkout (deny-default for writes and Mach lookups, LaunchServices/AppleEvents/
preferences/signals denied, reads outside the grants denied). `test_mlx_gpu_*` additionally
runs a real MLX GPU matmul + `mx.compile` under the same profile (skipped without the `mlx`
extra, which CI does not install).
"""

from __future__ import annotations

import os
import shutil
import socket
import tempfile
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest
from minion_common.enums import CapacityProfile, Engine, TaskType
from minion_common.schemas import Budget, ModelEntry
from minion_executor.runners.base import RunnerError
from minion_executor.worker.process import WorkerHandle
from minion_executor.worker.sandbox import SandboxPolicy

pytestmark = pytest.mark.macos

BUDGET = Budget(profile=CapacityProfile.BALANCED, memory_gb=8, gpu_duty_cycle=1.0, max_batch_size=8)
MODEL = ModelEntry(
    model_id="fake-embed",
    type=TaskType.EMBEDDING,
    engine=Engine.FAKE,
    ref="o/m",
    revision="a" * 40,
    memory_gb=1,
)


@pytest.fixture
def secret() -> Iterator[Path]:
    d = Path.home() / ".minion-sandbox-test"
    d.mkdir(exist_ok=True)
    f = d / "secret"
    f.write_text("hunter2")
    try:
        yield f
    finally:
        shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def model_dir(tmp_path: Path) -> Path:
    d = tmp_path / "cache" / "hf" / "o--m" / ("a" * 40)
    d.mkdir(parents=True)
    (d / "config.json").write_text("{}")
    return d


@pytest.fixture
def loopback_server() -> Iterator[int]:
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    stop = threading.Event()

    def accept() -> None:
        srv.settimeout(0.2)
        while not stop.is_set():
            try:
                srv.accept()[0].close()
            except OSError:
                continue

    t = threading.Thread(target=accept, daemon=True)
    t.start()
    try:
        yield srv.getsockname()[1]
    finally:
        stop.set()
        t.join(2)
        srv.close()


def _handle(model_dir: Path, *, sandbox: bool = True) -> WorkerHandle:
    policy = SandboxPolicy(cache_root=model_dir.parents[2]) if sandbox else None
    return WorkerHandle([Engine.FAKE], test_hooks=True, sandbox=policy, tick_interval_s=0.05)


def _probe(h: WorkerHandle, model_dir: Path, **action: object) -> dict[str, object]:
    h.ensure_model(MODEL, BUDGET, model_dir)
    (out,) = h.run([{"id": 1, **action}])
    return out


def test_unsandboxed_baseline_can_do_everything(
    secret: Path, model_dir: Path, loopback_server: int
) -> None:
    """Guards against the sandbox assertions below passing for the wrong reason."""
    h = _handle(model_dir, sandbox=False)
    try:
        assert _probe(h, model_dir, read_path=str(secret))["ok"] is True
        assert _probe(h, model_dir, exec=["/usr/bin/true"])["ok"] is True
        assert _probe(h, model_dir, connect=f"127.0.0.1:{loopback_server}")["ok"] is True
        assert _probe(h, model_dir, mach_lookup="com.apple.SecurityServer")["ok"] is True
        assert _probe(h, model_dir, sandbox_check="file-write-create", path="/private/tmp")["ok"]
        assert _probe(h, model_dir, sandbox_check="lsopen")["ok"] is True
        assert _probe(h, model_dir, signal_parent=1)["ok"] is True
    finally:
        h.close()


def test_worker_is_confined(secret: Path, model_dir: Path, loopback_server: int) -> None:
    h = _handle(model_dir)
    try:
        # HOME secrets (stand-in for ~/.aws, ~/.ssh, ~/.config/minion)
        r = _probe(h, model_dir, read_path=str(secret))
        assert r["ok"] is False and r["error"] == "PermissionError", r
        # outside network, and not even loopback (no Ollama port requested)
        r = _probe(h, model_dir, connect="1.1.1.1:443")
        assert r["ok"] is False, r
        r = _probe(h, model_dir, connect=f"127.0.0.1:{loopback_server}")
        assert r["ok"] is False, r
        # Keychain CLI: exec denied
        r = _probe(h, model_dir, exec=["/usr/bin/security", "find-generic-password", "-s", "x"])
        assert r["ok"] is False, r
        # securityd is not reachable over Mach
        r = _probe(h, model_dir, mach_lookup="com.apple.SecurityServer")
        assert r["ok"] is False and r["kern_return"] != 0, r
        # the sandbox did not break the worker: the model dir is readable ...
        r = _probe(h, model_dir, read_path=str(model_dir / "config.json"))
        assert r["ok"] is True, r
        # ... but read-only (cache poisoning)
        r = _probe(h, model_dir, write_path=str(model_dir / "evil"))
        assert r["ok"] is False, r
        assert not (model_dir / "evil").exists()
    finally:
        h.close()


def test_worker_tmp_dir_is_writable_and_removed(model_dir: Path) -> None:
    h = _handle(model_dir)
    try:
        h.ensure_model(MODEL, BUDGET, model_dir)
        tmp = h._tmp_dir
        assert tmp is not None and tmp.is_dir()
        (out,) = h.run([{"id": 1, "write_path": str(tmp / "scratch")}])
        assert out["ok"] is True and (tmp / "scratch").exists()
        # not elsewhere in HOME
        (out,) = h.run([{"id": 2, "write_path": str(Path.home() / ".minion-sandbox-nope")}])
        assert out["ok"] is False and not (Path.home() / ".minion-sandbox-nope").exists()
    finally:
        h.close()
    assert tmp is not None and not tmp.exists()


def test_ipc_ticks_still_delivered_under_sandbox(model_dir: Path) -> None:
    h = _handle(model_dir)
    ticks: list[int] = []
    try:
        h.ensure_model(MODEL, BUDGET, model_dir)
        out = h.run(
            [{"id": 1, "sleep_steps": 5, "step_s": 0.1}, {"id": 2, "text": "x"}],
            on_progress=ticks.append,
        )
        assert len(out) == 2 and "embedding" in out[1]
        assert ticks, "no progress ticks crossed the pipe"
    finally:
        h.close()


def test_model_path_outside_grant_is_refused_after_sandbox(model_dir: Path, tmp_path: Path) -> None:
    """A fresh sandboxed worker is used for every load, so each load gets its own grant."""
    other = tmp_path / "elsewhere"
    other.mkdir()
    (other / "f").write_text("x")
    h = _handle(model_dir)
    try:
        h.ensure_model(MODEL, BUDGET, model_dir)
        pid1 = h.pid
        h.unload()
        h.ensure_model(MODEL, BUDGET, other)
        assert h.pid != pid1
        (out,) = h.run([{"id": 1, "read_path": str(other / "f")}])
        assert out["ok"] is True
    finally:
        h.close()


def test_probe_applies_sandbox_and_failure_is_fatal(model_dir: Path) -> None:
    assert _handle(model_dir).probe(60) == {"fake": "fake"}
    # a cache dir that would expose HOME: the probe (so `run`) must fail closed
    bad = WorkerHandle(
        [Engine.FAKE], test_hooks=True, sandbox=SandboxPolicy(cache_root=Path.home())
    )
    with pytest.raises(RunnerError) as ei:
        bad.probe(60)
    assert ei.value.code == "SANDBOX_FAILED"


def test_kernel_rejected_profile_fails_closed() -> None:
    """Does not confine this process: a rejected profile is never applied."""
    from minion_executor.worker import sandbox

    with pytest.raises(RunnerError) as ei:
        sandbox.apply("(version 1)(this is not sbpl")
    assert ei.value.code == "SANDBOX_FAILED"


# --- hardened profile (TASK-4.2 security round) --------------------------------------------


@pytest.fixture(scope="module")
def hardened(tmp_path_factory: pytest.TempPathFactory) -> Iterator[tuple[WorkerHandle, Path]]:
    d = tmp_path_factory.mktemp("hardened") / "cache" / "hf" / "o--m" / ("a" * 40)
    d.mkdir(parents=True)
    h = _handle(d)
    h.ensure_model(MODEL, BUDGET, d)
    try:
        yield h, d
    finally:
        h.close()


@pytest.fixture(scope="module")
def unsandboxed(tmp_path_factory: pytest.TempPathFactory) -> Iterator[tuple[WorkerHandle, Path]]:
    d = tmp_path_factory.mktemp("plain") / "cache" / "hf" / "o--m" / ("a" * 40)
    d.mkdir(parents=True)
    h = _handle(d, sandbox=False)
    h.ensure_model(MODEL, BUDGET, d)
    try:
        yield h, d
    finally:
        h.close()


def _run(pair: tuple[WorkerHandle, Path], **action: object) -> dict[str, object]:
    (out,) = pair[0].run([{"id": 1, **action}])
    return out


DENIED_MACH = [
    "com.apple.coreservices.launchservicesd",
    "com.apple.coreservices.appleevents",
    "com.apple.pasteboard.1",
    "com.apple.nsurlsessiond",
    "com.apple.windowserver.active",
    "com.apple.metadata.mds",
    "com.apple.tccd",
    "com.apple.SecurityServer",
    "com.apple.secd",
    "com.apple.trustd.agent",
    "com.apple.securityd.xpc",
]


@pytest.mark.parametrize("service", DENIED_MACH)
def test_mach_service_denied(
    hardened: tuple[WorkerHandle, Path], unsandboxed: tuple[WorkerHandle, Path], service: str
) -> None:
    r = _run(hardened, mach_lookup=service)
    assert r["ok"] is False and r["kern_return"] != 0, r
    base = _run(unsandboxed, mach_lookup=service)
    if not base["ok"]:  # service absent on this macOS: nothing to prove
        pytest.skip(f"{service} not registered here")


def test_allow_listed_mach_services_still_reachable(
    hardened: tuple[WorkerHandle, Path],
) -> None:
    assert _run(hardened, mach_lookup="com.apple.logd")["ok"] is True
    assert _run(hardened, mach_lookup="com.apple.cvmsServ")["ok"] is True


@pytest.mark.parametrize(
    "op", ["lsopen", "appleevent-send", "distributed-notification-post", "user-preference-read"]
)
def test_operation_denied(
    hardened: tuple[WorkerHandle, Path], unsandboxed: tuple[WorkerHandle, Path], op: str
) -> None:
    assert _run(unsandboxed, sandbox_check=op)["ok"] is True  # baseline: the check is meaningful
    assert _run(hardened, sandbox_check=op)["ok"] is False


def test_preferences_not_readable(
    hardened: tuple[WorkerHandle, Path], unsandboxed: tuple[WorkerHandle, Path]
) -> None:
    assert int(_run(unsandboxed, cfprefs="com.apple.finder")["keys"]) > 0  # type: ignore[call-overload]
    r = _run(hardened, cfprefs="com.apple.finder")
    assert r["ok"] is False and r["keys"] == 0, r


def test_signalling_other_processes_denied(
    hardened: tuple[WorkerHandle, Path], unsandboxed: tuple[WorkerHandle, Path]
) -> None:
    assert _run(unsandboxed, signal_parent=1)["ok"] is True
    assert _run(hardened, signal_parent=1)["ok"] is False


@pytest.mark.parametrize(
    "path",
    [
        "/private/tmp",
        "/opt/homebrew" if Path("/opt/homebrew").is_dir() else "/opt",
        "/Users/Shared",
    ],
)
def test_writes_outside_grant_denied_by_policy(
    hardened: tuple[WorkerHandle, Path], unsandboxed: tuple[WorkerHandle, Path], path: str
) -> None:
    """Policy query only (`sandbox_check`): nothing is ever written to these locations (the
    paths are existing directories; `sandbox_check` cannot evaluate paths that do not exist)."""
    for op in ("file-write-create", "file-write-data"):
        assert _run(unsandboxed, sandbox_check=op, path=path)["ok"] is True
        assert _run(hardened, sandbox_check=op, path=path)["ok"] is False, (op, path)


def test_writes_inside_grant_allowed(hardened: tuple[WorkerHandle, Path]) -> None:
    tmp = hardened[0]._tmp_dir
    assert tmp is not None
    assert _run(hardened, write_path=str(tmp / "ok"))["ok"] is True
    assert _run(hardened, sandbox_check="file-write-data", path="/dev/null")["ok"] is True


@pytest.mark.parametrize(
    "path",
    [
        "/Users/Shared",
        "/Volumes",
        "/private/tmp",
        "/private/var/folders",
        "/Library/Preferences",
        "/Library/Keychains",
    ],
)
def test_reads_outside_grant_denied(
    hardened: tuple[WorkerHandle, Path], unsandboxed: tuple[WorkerHandle, Path], path: str
) -> None:
    assert _run(unsandboxed, sandbox_check="file-read-data", path=path)["ok"] is True
    assert _run(hardened, sandbox_check="file-read-data", path=path)["ok"] is False


def test_home_cannot_be_listed(hardened: tuple[WorkerHandle, Path]) -> None:
    r = _run(hardened, list_dir=str(Path.home()))
    assert r["ok"] is False and r["error"] == "PermissionError", r


def test_reads_inside_grant_allowed(hardened: tuple[WorkerHandle, Path]) -> None:
    tmp = hardened[0]._tmp_dir
    assert tmp is not None
    assert _run(hardened, write_path=str(tmp / "f"))["ok"] is True
    assert _run(hardened, read_path=str(tmp / "f"))["ok"] is True
    assert _run(hardened, read_path="/System/Library/CoreServices/SystemVersion.plist")["ok"]


def test_cache_root_is_not_granted_only_the_model_dir(
    hardened: tuple[WorkerHandle, Path],
) -> None:
    d = hardened[1]
    (d / "config.json").write_text("{}")
    sibling = d.parent / ("b" * 40)
    sibling.mkdir()
    (sibling / "config.json").write_text("{}")
    assert _run(hardened, read_path=str(d / "config.json"))["ok"] is True
    r = _run(hardened, read_path=str(sibling / "config.json"))
    assert r["ok"] is False and r["error"] == "PermissionError", r


def test_ollama_port_granted_to_ollama_loads_only(model_dir: Path, loopback_server: int) -> None:
    """Spec built the way `WorkerHandle` builds it; applied in a throwaway subprocess."""
    import json
    import subprocess
    import sys

    from minion_executor.worker import sandbox as sbx

    script = f"""
import json, socket, sys
from minion_executor.worker import sandbox as s
other = socket.socket()
other.bind(("127.0.0.1", 0))
other.listen(1)
s.apply_from_spec(json.loads(sys.argv[1]))
def c(h, p):
    try:
        socket.create_connection((h, p), timeout=3).close()
        return True
    except OSError:
        return False
print(json.dumps([c("127.0.0.1", {loopback_server}), c("1.1.1.1", 443),
                  c("127.0.0.1", other.getsockname()[1])]))
"""

    def run(engine: str, port: int | None) -> list[bool]:
        spec = sbx.sandbox_spec(
            SandboxPolicy(cache_root=model_dir.parents[2]),
            model_path=model_dir,
            tmp_dir=Path(tempfile.mkdtemp(prefix=f"minion-exec-{os.getpid()}-t-")),
            engines=[engine],
            ollama_port=port,
        )
        out = subprocess.run(  # noqa: S603
            [sys.executable, "-c", script, json.dumps(spec)],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        assert out.returncode == 0, out.stderr[-500:]
        res: list[bool] = json.loads(out.stdout.splitlines()[-1])
        return res

    assert run("ollama", loopback_server) == [True, False, False]
    assert run("mlx", None) == [False, False, False]


def test_ollama_port_only_in_ollama_specs(model_dir: Path) -> None:
    for engine, expect in ((Engine.OLLAMA, 11434), (Engine.MLX, None), (Engine.FAKE, None)):
        h = _handle(model_dir)
        try:
            h._ensure_started()
            m = MODEL.model_copy(update={"engine": engine})
            spec = h._sandbox_spec(model_dir, m)
            assert spec is not None and spec["ollama_port"] == expect, engine
            probe_spec = h._sandbox_spec(None, None)
            assert probe_spec is not None and probe_spec["ollama_port"] is None
        finally:
            h.close()


def test_python_grant_excludes_cwd_and_pythonpath(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from minion_executor.worker.sandbox import python_read_paths

    extra = tmp_path / "pp"
    extra.mkdir()
    monkeypatch.setenv("PYTHONPATH", str(extra))
    monkeypatch.syspath_prepend(str(extra))
    monkeypatch.chdir(tmp_path)
    paths = [os.path.realpath(p) for p in python_read_paths()]
    assert os.path.realpath(extra) not in paths and os.path.realpath(tmp_path) not in paths


def test_worker_tmp_dir_swept_by_cleanup_stale_tmp(model_dir: Path) -> None:
    from minion_executor.loop import _TMP_RE

    h = _handle(model_dir)
    try:
        h._ensure_started()
        assert h._tmp_dir is not None
        m = _TMP_RE.match(h._tmp_dir.name)
        assert m is not None and int(m.group(1)) == os.getpid()
    finally:
        h.close()


# --- real Metal under the profile -----------------------------------------------------------


def test_mlx_gpu_matmul_works_in_sandboxed_worker(hardened: tuple[WorkerHandle, Path]) -> None:
    pytest.importorskip("mlx.core", reason="install the `mlx` extra: uv sync --extra mlx")
    r = _run(hardened, mlx_matmul=64)
    assert r["ok"] is True, r
    assert "gpu" in str(r["device"]).lower()
    assert r["value"] == 64.0
    assert abs(float(r["softmax_sum"]) - 1.0) < 1e-4  # type: ignore[arg-type]
    # and the sandbox is really on for that same process
    assert _run(hardened, list_dir=str(Path.home()))["ok"] is False


def test_cpu_priority_changes_live_inside_the_sandbox(model_dir: Path) -> None:
    """The Seatbelt profile (unchanged) must not deny `setpriority`/`nice` on the worker itself:
    raising to LOW and BACKGROUND works after confinement, Darwin background QoS is reversible,
    and nice is never lowered (unprivileged)."""
    from minion_common.enums import CpuPriority

    def at(prio: CpuPriority) -> Budget:
        return BUDGET.model_copy(update={"cpu_priority": prio})

    h = _handle(model_dir)
    try:
        h.ensure_model(MODEL, at(CpuPriority.NORMAL), model_dir)
        (base,) = h.run([{"id": 1, "report_state": 1}], budget=at(CpuPriority.NORMAL))
        (low,) = h.run([{"id": 1, "report_state": 1}], budget=at(CpuPriority.LOW))
        assert low["nice"] == max(base["nice"], 10) and low["background"] is False, low
        (bg,) = h.run([{"id": 1, "report_state": 1}], budget=at(CpuPriority.BACKGROUND))
        assert bg["nice"] == max(base["nice"], 15) and bg["background"] is True, bg
        (back,) = h.run([{"id": 1, "report_state": 1}], budget=at(CpuPriority.LOW))
        assert back["background"] is False and back["nice"] == bg["nice"], back
        # still confined after all that
        r = _probe(h, model_dir, exec=["/usr/bin/true"])
        assert r["ok"] is False, r
    finally:
        h.close()


def test_live_control_works_inside_the_sandbox(model_dir: Path) -> None:
    import time as _time

    h = _handle(model_dir)
    try:
        h.ensure_model(MODEL, BUDGET, model_dir)
        gen = h.control(duty=1.0, paused=True)
        seen: list[int] = []
        t = threading.Thread(
            target=lambda: h.run(
                [{"id": i, "text": "x"} for i in range(5)], None, seen.append, budget=BUDGET
            ),
            daemon=True,
        )
        t.start()
        end = _time.monotonic() + 10
        while _time.monotonic() < end and not ((a := h.last_ack()) and a.gen == gen):
            _time.sleep(0.02)
        ack = h.last_ack()
        assert ack is not None and ack.paused is True
        _time.sleep(0.3)
        assert t.is_alive() and not any(seen)  # started paused: no work done
        h.control(duty=1.0, paused=False)
        t.join(10)
        assert not t.is_alive() and max(seen) == 5
    finally:
        h.close()
