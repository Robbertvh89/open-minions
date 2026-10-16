"""The model-fetch child under a real Seatbelt profile. Auto-skipped off macOS (conftest).

`test_model_cache_hub.py` (`-m model`) additionally runs the real hub download through the
same sandbox. Here the fetch is a module-level fake (spawn context) that probes what a
compromised child could try."""

from __future__ import annotations

import json
import shutil
import socket
import tempfile
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest
from minion_common.enums import Engine, TaskType
from minion_common.schemas import ModelEntry
from minion_executor.model_cache import FetchEnv, ModelCache

pytestmark = pytest.mark.macos

MODEL = ModelEntry(
    model_id="m",
    type=TaskType.EMBEDDING,
    engine=Engine.MLX,
    ref="org/m",
    revision="a" * 40,
    memory_gb=1,
)


@pytest.fixture
def home_ca() -> Iterator[Path]:
    d = Path.home() / ".minion-fetch-sandbox-test"
    d.mkdir(exist_ok=True)
    f = d / "ca.pem"
    f.write_text("PEM")
    try:
        yield f
    finally:
        shutil.rmtree(d, ignore_errors=True)


@pytest.fixture(autouse=True)
def _cleanup_probe() -> Iterator[None]:
    yield
    (Path.home() / ".minion-fetch-sandbox-probe").unlink(missing_ok=True)


@pytest.fixture
def unix_socket() -> Iterator[str]:
    """A listening AF_UNIX socket in a dir we create (short path: sun_path is limited)."""
    d = tempfile.mkdtemp(prefix="mx-", dir="/tmp")
    path = str(Path(d) / "s.sock")
    srv = socket.socket(socket.AF_UNIX)
    srv.bind(path)
    srv.listen(8)  # connect() succeeds against the backlog without an accept loop
    try:
        yield path
    finally:
        srv.close()
        shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def loopback_port() -> Iterator[int]:
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

    threading.Thread(target=accept, daemon=True).start()
    try:
        yield int(srv.getsockname()[1])
    finally:
        stop.set()
        srv.close()


async def _probe(tmp_path: Path, env: dict[str, str]) -> dict[str, bool]:
    import exec_fetch_helpers as h

    cache = ModelCache(
        tmp_path / "c",
        10**6,
        fetch_env=FetchEnv.capture(env),
        fetch=h.sandbox_probe_fetch,
        min_free_bytes=0,
    )
    path = await cache.ensure(MODEL)
    assert path is not None
    got: dict[str, bool] = json.loads((path / "config.json").read_text())
    return got


async def test_fetch_child_is_confined_and_loopback_and_unix_sockets_denied(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    home_ca: Path,
    loopback_port: int,
    unix_socket: str,
) -> None:
    monkeypatch.syspath_prepend(str(Path(__file__).parent))
    # baseline: the listeners are reachable from this (unsandboxed) process
    socket.create_connection(("127.0.0.1", loopback_port), timeout=3).close()
    with socket.socket(socket.AF_UNIX) as s:
        s.connect(unix_socket)

    got = await _probe(
        tmp_path / "a",
        {
            "SSL_CERT_FILE": str(home_ca),
            "HF_HUB_PROBE_PORT": str(loopback_port),
            "HF_HUB_PROBE_UNIX": unix_socket,
        },
    )
    assert got == {
        "list_home": False,  # HOME unreadable
        "write_outside": False,  # writes only to the staging dir
        "write_tmp": False,
        "write_staging": True,
        "read_ca": True,  # a CA bundle under HOME is re-allowed (as a file literal)
        "exec": False,
        "net": False,  # localhost is denied (only https and a configured proxy are open)
        "unix": False,  # docker.sock / ssh-agent style sockets are denied
        "procargs": False,  # other processes' launch env is unreadable
    }
    assert not (Path.home() / ".minion-fetch-sandbox-probe").exists()


async def test_fetch_child_may_reach_a_configured_local_proxy_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, loopback_port: int
) -> None:
    monkeypatch.syspath_prepend(str(Path(__file__).parent))
    other = socket.socket()
    other.bind(("127.0.0.1", 0))
    other.listen(1)
    try:
        env = {"HTTPS_PROXY": f"http://127.0.0.1:{loopback_port}"}
        assert (await _probe(tmp_path / "a", {**env, "HF_HUB_PROBE_PORT": str(loopback_port)}))[
            "net"
        ] is True
        other_port = str(other.getsockname()[1])
        assert (await _probe(tmp_path / "b", {**env, "HF_HUB_PROBE_PORT": other_port}))[
            "net"
        ] is False
    finally:
        other.close()
