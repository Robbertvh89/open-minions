"""Real Seatbelt sandbox with the fake engine (no GPU). Auto-skipped off macOS (conftest).

The worker is a real spawned subprocess confined through `WorkerHandle(sandbox=...)`; the fake
runner's test hooks make it attempt reads, connects and execs from inside the sandbox.

What this covers vs. not: the profile is proven to confine a Python worker that imports
`minion_executor` from the venv/source checkout. Importing mlx / Metal under the profile is
not covered here (mlx is not a CI dependency); the profile keeps `(allow default)` outside the
denied areas precisely so Metal, IOKit and dyld keep working, and TASK-4.3's `gpu` tests run
the real MLX e2e through the sandbox.
"""

from __future__ import annotations

import shutil
import socket
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


def _handle(model_dir: Path, *, sandbox: bool = True, port: int | None = None) -> WorkerHandle:
    policy = SandboxPolicy(cache_root=model_dir.parents[2], ollama_port=port) if sandbox else None
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


def test_ollama_port_is_the_only_network_allowed(model_dir: Path, loopback_server: int) -> None:
    h = _handle(model_dir, port=loopback_server)
    try:
        assert _probe(h, model_dir, connect=f"127.0.0.1:{loopback_server}")["ok"] is True
        assert _probe(h, model_dir, connect="1.1.1.1:443")["ok"] is False
        other = socket.socket()
        other.bind(("127.0.0.1", 0))
        other.listen(1)
        try:
            r = _probe(h, model_dir, connect=f"127.0.0.1:{other.getsockname()[1]}")
            assert r["ok"] is False, r
        finally:
            other.close()
    finally:
        h.close()


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
    # a spec the worker cannot turn into a profile: the probe (so `run`) must fail closed
    with pytest.raises(RunnerError) as ei:
        _handle(model_dir, port=99999).probe(60)
    assert ei.value.code == "SANDBOX_FAILED"


def test_kernel_rejected_profile_fails_closed() -> None:
    """Does not confine this process: a rejected profile is never applied."""
    from minion_executor.worker import sandbox

    with pytest.raises(RunnerError) as ei:
        sandbox.apply("(version 1)(this is not sbpl")
    assert ei.value.code == "SANDBOX_FAILED"
