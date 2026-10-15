"""The model-fetch child under a real Seatbelt profile. Auto-skipped off macOS (conftest).

`test_model_cache_hub.py` (`-m model`) additionally runs the real hub download through the
same sandbox. Here the fetch is a module-level fake (spawn context) that probes what a
compromised child could try."""

from __future__ import annotations

import json
import shutil
import socket
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


async def test_fetch_child_is_confined_but_has_network_and_ca(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    home_ca: Path,
    loopback_port: int,
) -> None:
    monkeypatch.syspath_prepend(str(Path(__file__).parent))
    import exec_fetch_helpers as h

    fe = FetchEnv.capture({"SSL_CERT_FILE": str(home_ca), "HF_HUB_PROBE_PORT": str(loopback_port)})
    cache = ModelCache(
        tmp_path / "c", 10**6, fetch_env=fe, fetch=h.sandbox_probe_fetch, min_free_bytes=0
    )
    path = await cache.ensure(MODEL)
    assert path is not None
    got = json.loads((path / "config.json").read_text())
    assert got == {
        "list_home": False,  # HOME unreadable
        "write_outside": False,  # writes only to the staging dir
        "write_tmp": False,
        "write_staging": True,
        "read_ca": True,  # a CA bundle under HOME is re-allowed
        "exec": False,
        "net": True,  # outbound network stays on (hub / proxy)
    }
    assert not (Path.home() / ".minion-fetch-sandbox-probe").exists()
