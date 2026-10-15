from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
from multiprocessing.process import BaseProcess
from pathlib import Path

import pytest
from minion_common.enums import CapacityProfile, Engine, TaskType
from minion_common.schemas import Budget, ModelEntry
from minion_executor.capacity.profiles import budget_for, parse_profile
from minion_executor.runners import create_runner
from minion_executor.runners.base import Runner, RunnerError
from minion_executor.worker.process import (
    WorkerCrashed,
    WorkerHandle,
    env_allowed,
    restrict_environ,
    scrub_env,
)


def _model(t: TaskType = TaskType.EMBEDDING, mid: str = "fake-embed") -> ModelEntry:
    return ModelEntry(model_id=mid, type=t, engine=Engine.MLX, ref="x", memory_gb=1)


BUDGET = Budget(profile=CapacityProfile.BALANCED, memory_gb=8, gpu_duty_cycle=0.5, max_batch_size=8)


def test_fake_embedding_deterministic() -> None:
    r1, r2 = create_runner("fake"), create_runner("fake")
    for r in (r1, r2):
        r.load(_model(), BUDGET)
    batch = [{"id": 1, "text": "hello"}, {"id": 2, "text": "world"}]
    assert r1.run(batch) == r2.run(batch)
    out = r1.run(batch)
    assert out[0]["embedding"] != out[1]["embedding"]


def test_fake_completion_echo_and_fail() -> None:
    r = create_runner("fake", test_hooks=True)
    r.load(_model(TaskType.COMPLETION, "c"), BUDGET)
    assert r.run([{"id": 1, "text": "hi"}]) == [{"id": 1, "completion": "hi"}]
    with pytest.raises(RunnerError) as ei:
        r.run([{"id": 2, "fail": "oom"}])
    assert ei.value.code == "INJECTED_FAILURE"  # data never chooses the failure code


def test_fake_hooks_inert_without_flag() -> None:
    r = create_runner("fake")
    r.load(_model(TaskType.COMPLETION, "c"), BUDGET)
    out = r.run([{"id": 1, "text": "hi", "fail": "oom", "sleep_s": 60, "echo_env": "PATH"}])
    assert out == [{"id": 1, "completion": "hi"}]


def test_unknown_runner() -> None:
    with pytest.raises(ValueError):
        create_runner("nope")


def test_worker_round_trip_in_subprocess() -> None:
    w = WorkerHandle("fake", test_hooks=True)
    try:
        w.ensure_model(_model(), BUDGET)
        assert w.pid is not None and w.pid != os.getpid()
        pid = w.pid
        out = w.run([{"id": 1, "text": "a"}])
        assert out == create_runner_loaded().run([{"id": 1, "text": "a"}])
        w.ensure_model(_model(), BUDGET)  # warm: same process
        assert w.pid == pid
        with pytest.raises(RunnerError) as ei:
            w.run([{"fail": "oom"}])
        assert ei.value.code == "INJECTED_FAILURE"
        w.run([{"id": 1, "text": "a"}])  # worker survives runner errors
        w.ensure_model(_model(mid="other"), BUDGET)  # switch -> new process
        assert w.pid != pid
    finally:
        w.close()


def create_runner_loaded() -> Runner:
    r = create_runner("fake")
    r.load(_model(), BUDGET)
    return r


def test_worker_crash_detected() -> None:
    w = WorkerHandle("fake")
    try:
        w.ensure_model(_model(), BUDGET)
        pid = w.pid
        assert pid is not None
        os.kill(pid, signal.SIGKILL)
        time.sleep(0.2)
        with pytest.raises(WorkerCrashed):
            w.run([{"id": 1, "text": "a"}])
        # handle recovers with a fresh worker
        w.ensure_model(_model(), BUDGET)
        assert w.run([{"id": 1, "text": "a"}])
    finally:
        w.close()


def test_profiles_to_budget() -> None:
    b = budget_for(CapacityProfile.BALANCED, total_gb=64)
    assert (b.memory_gb, b.gpu_duty_cycle, b.max_batch_size) == (25.6, 0.5, 8)
    assert budget_for(CapacityProfile.LIGHT, total_gb=16).memory_gb == 4
    assert budget_for(CapacityProfile.FULL, total_gb=100).memory_gb == 70
    assert budget_for(CapacityProfile.FULL).memory_gb > 0
    assert parse_profile("light") is CapacityProfile.LIGHT
    with pytest.raises(ValueError):
        parse_profile("custom")
    with pytest.raises(ValueError):
        parse_profile("bogus")


def test_kill_before_start_never_spawns() -> None:
    w = WorkerHandle("fake")
    w.kill()
    with pytest.raises(WorkerCrashed):
        w.ensure_model(_model(), BUDGET)
    assert w.pid is None
    w.close()


def test_stop_during_ensure_model_spawns_no_worker() -> None:
    """kill() is lock-free: it lands while ensure_model is blocked, which then must not spawn."""
    w = WorkerHandle("fake")
    errors: list[BaseException] = []
    entered = threading.Event()

    def load() -> None:
        entered.set()
        try:
            w.ensure_model(_model(), BUDGET)
        except BaseException as e:
            errors.append(e)

    with w._lock:  # simulate a call in flight holding the handle lock
        t = threading.Thread(target=load)
        t.start()
        assert entered.wait(5)
        w.kill()  # must not block on the lock
    t.join(10)
    assert not t.is_alive()
    assert len(errors) == 1 and isinstance(errors[0], WorkerCrashed)
    assert w.pid is None
    w.close()


def test_kill_interrupts_inflight_run() -> None:
    w = WorkerHandle("fake", test_hooks=True)
    w.ensure_model(_model(), BUDGET)
    errors: list[BaseException] = []
    started = threading.Event()

    def go() -> None:
        started.set()
        try:
            w.run([{"id": 1, "sleep_s": 30}])
        except BaseException as e:
            errors.append(e)

    t = threading.Thread(target=go)
    t.start()
    assert started.wait(5)
    w.kill()
    t.join(15)
    assert not t.is_alive()
    assert isinstance(errors[0], WorkerCrashed)
    with pytest.raises(WorkerCrashed):  # stopped for good
        w.ensure_model(_model(), BUDGET)
    w.close()


def test_run_timeout_is_per_op_and_recoverable() -> None:
    w = WorkerHandle("fake", test_hooks=True, run_timeout_s=0.3, load_timeout_s=30)
    try:
        w.ensure_model(_model(), BUDGET)  # load uses the (long) load timeout
        with pytest.raises(WorkerCrashed, match="timed out"):
            w.run([{"id": 1, "sleep_s": 30}])
        w.ensure_model(_model(), BUDGET)  # not closed: a fresh worker comes up
        assert w.run([{"id": 1, "text": "a"}])
    finally:
        w.close()


def test_oversize_message_is_a_crash() -> None:
    w = WorkerHandle("fake", max_message_bytes=4096)
    try:
        w.ensure_model(_model(), BUDGET)
        with pytest.raises(WorkerCrashed):
            w.run([{"id": 1, "text": "x" * 10_000}])
        w.ensure_model(_model(), BUDGET)
        assert w.run([{"id": 1, "text": "ok"}])
    finally:
        w.close()


def test_scrub_env_is_an_allowlist() -> None:
    env = {
        "PATH": "/bin",
        "HOME": "/h",
        "LC_ALL": "C",
        "MLX_X": "1",
        "HF_HOME": "/hf",
        "HF_TOKEN": "t",
        "DATABASE_URL": "postgres://u:p@h/db",
        "FOO_PASS": "p",
        "AWS_ACCESS_KEY_ID": "a",
        "MINION_EXECUTOR_TOKEN": "t",
    }
    assert scrub_env(env) == {
        "PATH": "/bin",
        "HOME": "/h",
        "LC_ALL": "C",
        "MLX_X": "1",
        "HF_HOME": "/hf",
    }


def test_worker_env_has_no_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("HF_TOKEN", "DATABASE_URL", "FOO_PASS", "MINION_EXECUTOR_TOKEN"):
        monkeypatch.setenv(name, "super-secret")
    saved = dict(os.environ)
    restrict_environ()  # what main does before any spawn: closes the spawn-inheritance window
    w = WorkerHandle("fake", test_hooks=True)
    try:
        w.ensure_model(_model(), BUDGET)
        names = ["HF_TOKEN", "DATABASE_URL", "FOO_PASS", "MINION_EXECUTOR_TOKEN", "PATH", "HOME"]
        out = w.run([{"id": i, "echo_env": n} for i, n in enumerate(names)])
        assert [o["present"] for o in out] == [False, False, False, False, True, True]
    finally:
        w.close()
        os.environ.clear()
        os.environ.update(saved)


def test_restrict_environ_keeps_only_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HF_TOKEN", "x")
    monkeypatch.setenv("MLX_FOO", "1")
    saved = dict(os.environ)
    try:
        restrict_environ()
        assert "HF_TOKEN" not in os.environ and os.environ["MLX_FOO"] == "1"
        assert all(env_allowed(k) for k in os.environ)
    finally:
        os.environ.clear()
        os.environ.update(saved)


def test_kill_between_check_and_assignment_leaves_no_worker() -> None:
    w = WorkerHandle("fake")
    spawned: list[BaseProcess] = []

    def hook(proc: BaseProcess) -> None:
        spawned.append(proc)
        w.kill()  # lands after the _closed check, before self._proc is assigned

    w._after_spawn_hook = hook
    t0 = time.monotonic()
    with pytest.raises(WorkerCrashed):
        w.ensure_model(_model(), BUDGET)
    assert w.pid is None
    assert spawned and not spawned[0].is_alive()
    w.close()
    assert time.monotonic() - t0 < 10


HARNESS = """
import threading
import time

from minion_common.schemas import Budget, ModelEntry
from minion_executor.worker.process import WorkerHandle


def main():
    w = WorkerHandle("fake", test_hooks=True, parent_watch_interval_s=0.1)
    w.ensure_model(
        ModelEntry(model_id="m", type="embedding", engine="fake", ref="x", memory_gb=1),
        Budget.model_validate(
            {"profile": "balanced", "memory_gb": 8, "gpu_duty_cycle": 0.5, "max_batch_size": 8}
        ),
    )
    threading.Thread(target=lambda: w.run([{"id": 1, "sleep_s": 60}]), daemon=True).start()
    print(w.pid, flush=True)
    time.sleep(600)


if __name__ == "__main__":
    main()
"""


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def test_worker_exits_when_control_process_is_killed(tmp_path: Path) -> None:
    script = tmp_path / "harness.py"
    script.write_text(HARNESS)
    proc = subprocess.Popen(  # noqa: S603
        [sys.executable, str(script)], stdout=subprocess.PIPE, text=True
    )
    try:
        assert proc.stdout is not None
        worker_pid = int(proc.stdout.readline())
        assert _alive(worker_pid)
        proc.kill()  # SIGKILL: no cleanup handlers run in the control process
        proc.wait(10)
        deadline = time.monotonic() + 15
        while _alive(worker_pid):
            assert time.monotonic() < deadline, "orphaned worker still running"
            time.sleep(0.05)
    finally:
        if proc.poll() is None:
            proc.kill()
