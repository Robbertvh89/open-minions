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
from minion_common.schemas import Budget, CompletionParams, EmbeddingParams, ModelEntry
from minion_executor.capacity.profiles import budget_for, parse_profile
from minion_executor.runners import create_runner
from minion_executor.runners.base import EngineConfig, Runner, RunnerError
from minion_executor.worker.process import (
    WorkerCrashed,
    WorkerHandle,
    env_allowed,
    restrict_environ,
    scrub_env,
)
from minion_executor.worker.throttle import DutyCyclePacer


def _model(t: TaskType = TaskType.EMBEDDING, mid: str = "fake-embed") -> ModelEntry:
    return ModelEntry(
        model_id=mid, type=t, engine=Engine.FAKE, ref="o/m", revision="a" * 40, memory_gb=1
    )


BUDGET = Budget(profile=CapacityProfile.BALANCED, memory_gb=8, gpu_duty_cycle=0.5, max_batch_size=8)


class _NoopCtl:
    def __init__(self) -> None:
        self.calls: list[int | None] = []

    def checkpoint(self, records_done: int | None = None) -> None:
        self.calls.append(records_done)


def _fake(
    t: TaskType = TaskType.EMBEDDING, *, hooks: bool = False, cfg: EngineConfig | None = None
) -> Runner:
    r = create_runner(Engine.FAKE, t, test_hooks=hooks)
    r.load(_model(t, "m"), None, BUDGET, cfg or EngineConfig())
    return r


def test_fake_embedding_deterministic_and_honours_normalize() -> None:
    r1, r2 = _fake(), _fake()
    batch = [{"id": 1, "text": "hello"}, {"id": 2, "prompt": "world"}]
    ctl = _NoopCtl()
    assert r1.run(batch, EmbeddingParams(), ctl) == r2.run(batch, EmbeddingParams(), ctl)
    out = r1.run(batch, EmbeddingParams(), ctl)
    assert out[0]["embedding"] != out[1]["embedding"]
    assert sum(x * x for x in out[0]["embedding"]) == pytest.approx(1.0, abs=1e-4)
    raw = r1.run(batch, EmbeddingParams(normalize=False), ctl)
    assert sum(x * x for x in raw[0]["embedding"]) != pytest.approx(1.0, abs=1e-3)
    assert ctl.calls[-2:] == [1, 2]  # checkpoint per record


def test_fake_completion_inputs_fail_and_retryable() -> None:
    r = _fake(TaskType.COMPLETION, hooks=True)
    p, ctl = CompletionParams(), _NoopCtl()
    assert r.run([{"id": 1, "text": "hi"}], p, ctl) == [{"id": 1, "completion": "hi"}]
    out = r.run([{"id": 2, "messages": [{"role": "user", "content": "yo"}]}], p, ctl)
    assert out == [{"id": 2, "completion": "yo"}]
    with pytest.raises(RunnerError) as ei:
        r.run([{"id": 3, "fail": "oom"}], p, ctl)
    assert ei.value.code == "INJECTED_FAILURE"  # data never chooses the failure code
    assert ei.value.retryable
    with pytest.raises(RunnerError) as ei:
        r.run([{"id": 3, "fail": "x", "retryable": False}], p, ctl)
    assert not ei.value.retryable


def test_fake_prompt_too_long_is_per_record() -> None:
    r = _fake(TaskType.COMPLETION, cfg=EngineConfig(max_prompt_tokens=300))
    batch = [{"id": 1, "prompt": "x" * 100}, {"id": 2, "prompt": "x" * 100 + "y" * 100}]
    out = r.run(batch, CompletionParams(max_tokens=256), _NoopCtl())
    assert out[0] == {"id": 1, "error": "PROMPT_TOO_LONG"}  # 100 > 300 - 256
    assert out[1] == {"id": 2, "error": "PROMPT_TOO_LONG"}
    out = r.run(batch[:1], CompletionParams(max_tokens=100), _NoopCtl())
    assert out == [{"id": 1, "completion": "x" * 100}]


def test_fake_hooks_inert_without_flag() -> None:
    r = _fake(TaskType.COMPLETION)
    rec = {"id": 1, "text": "hi", "fail": "oom", "sleep_s": 60, "echo_env": "PATH"}
    assert r.run([rec], CompletionParams(), _NoopCtl()) == [{"id": 1, "completion": "hi"}]


def test_fake_memory_in_use_zeros() -> None:
    assert _fake().memory_in_use() == {"active_bytes": 0, "peak_bytes": 0}


def test_unavailable_engine_and_unknown() -> None:
    with pytest.raises(ValueError):
        create_runner(Engine.MLX, TaskType.EMBEDDING)  # real engines land in M3.3
    with pytest.raises(ValueError):
        Engine("nope")


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, s: float) -> None:
        self.slept.append(s)
        self.now += s


def test_duty_cycle_pacer_sleeps_proportionally() -> None:
    c = _Clock()
    p = DutyCyclePacer(0.5, c, c.sleep)
    c.now += 2.0  # a 2 s step at d=0.5 -> sleep 2 s
    assert p.step_done() == pytest.approx(2.0)
    c.now += 1.0  # the pause itself is not counted as busy time
    assert p.step_done() == pytest.approx(1.0)
    c2 = _Clock()
    p2 = DutyCyclePacer(0.25, c2, c2.sleep)
    c2.now += 1.0
    assert p2.step_done() == pytest.approx(3.0)  # t*(1-d)/d
    assert c2.slept == [pytest.approx(3.0)]


def test_duty_cycle_pacer_full_duty_never_sleeps_and_caps() -> None:
    c = _Clock()
    p = DutyCyclePacer(1.0, c, c.sleep)
    c.now += 5.0
    assert p.step_done() == 0.0 and c.slept == []
    capped = DutyCyclePacer(0.1, c, c.sleep, max_sleep_s=7.0)
    c.now += 10.0
    assert capped.step_done() == 7.0
    with pytest.raises(ValueError):
        DutyCyclePacer(0.0, c, c.sleep)


def test_worker_round_trip_in_subprocess() -> None:
    w = WorkerHandle([Engine.FAKE], test_hooks=True)
    try:
        w.ensure_model(_model(), BUDGET)
        assert w.pid is not None and w.pid != os.getpid()
        pid = w.pid
        out = w.run([{"id": 1, "text": "a"}])
        assert out == create_runner_loaded().run(
            [{"id": 1, "text": "a"}], EmbeddingParams(), _NoopCtl()
        )
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
    return _fake()


def test_worker_crash_detected() -> None:
    w = WorkerHandle([Engine.FAKE])
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
    w = WorkerHandle([Engine.FAKE])
    w.kill()
    with pytest.raises(WorkerCrashed):
        w.ensure_model(_model(), BUDGET)
    assert w.pid is None
    w.close()


def test_stop_during_ensure_model_spawns_no_worker() -> None:
    """kill() is lock-free: it lands while ensure_model is blocked, which then must not spawn."""
    w = WorkerHandle([Engine.FAKE])
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
    w = WorkerHandle([Engine.FAKE], test_hooks=True)
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
    w = WorkerHandle([Engine.FAKE], test_hooks=True, step_timeout_s=0.3, load_timeout_s=30)
    try:
        w.ensure_model(_model(), BUDGET)  # load uses the (long) load timeout
        with pytest.raises(WorkerCrashed, match="timed out"):
            w.run([{"id": 1, "sleep_s": 30}])
        w.ensure_model(_model(), BUDGET)  # not closed: a fresh worker comes up
        assert w.run([{"id": 1, "text": "a"}])
    finally:
        w.close()


def test_oversize_message_is_a_crash() -> None:
    w = WorkerHandle([Engine.FAKE], max_message_bytes=4096)
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
    w = WorkerHandle([Engine.FAKE], test_hooks=True)
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
    w = WorkerHandle([Engine.FAKE])
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


def test_interrupt_between_spawn_and_assignment_aborts_and_leaves_no_worker() -> None:
    w = WorkerHandle([Engine.FAKE])
    spawned: list[BaseProcess] = []

    def hook(proc: BaseProcess) -> None:
        spawned.append(proc)
        w.interrupt()  # lands before self._proc is assigned: nothing to kill yet

    w._after_spawn_hook = hook
    with pytest.raises(WorkerCrashed):
        w.ensure_model(_model(), BUDGET)
    assert w.pid is None and w.loaded_model_id is None
    assert spawned and not spawned[0].is_alive()
    w._after_spawn_hook = None
    w.ensure_model(_model(), BUDGET)  # the handle stays usable (unlike kill())
    try:
        assert w.pid is not None
    finally:
        w.close()


HARNESS = """
import threading
import time

from minion_common.enums import Engine
from minion_common.enums import Engine
from minion_common.schemas import Budget, ModelEntry
from minion_executor.worker.process import WorkerHandle


def main():
    w = WorkerHandle([Engine.FAKE], test_hooks=True, parent_watch_interval_s=0.1)
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


def test_interrupt_kills_worker_but_allows_respawn() -> None:
    h = WorkerHandle([Engine.FAKE])
    try:
        h.ensure_model(_model(), BUDGET)
        first = h.pid
        assert first is not None
        h.interrupt()
        h.interrupt()  # idempotent
        h.ensure_model(_model(), BUDGET)  # dead idle worker: respawned, model reloaded
        assert h.pid is not None and h.pid != first
        assert h.run([{"id": 1, "text": "a"}])[0]["id"] == 1
    finally:
        h.close()


def test_interrupt_aborts_inflight_call_then_respawns() -> None:
    h = WorkerHandle([Engine.FAKE], test_hooks=True)
    try:
        h.ensure_model(_model(TaskType.COMPLETION, "c"), BUDGET)
        t = threading.Timer(0.5, h.interrupt)
        t.start()
        t0 = time.monotonic()
        with pytest.raises(WorkerCrashed):
            h.run([{"id": 1, "text": "x", "sleep_s": 30}])
        assert time.monotonic() - t0 < 10
        h.ensure_model(_model(TaskType.COMPLETION, "c"), BUDGET)
        assert h.run([{"id": 2, "text": "ok"}]) == [{"id": 2, "completion": "ok"}]
    finally:
        h.close()


def test_ensure_model_reads_interrupt_generation_once(monkeypatch: pytest.MonkeyPatch) -> None:
    reads = iter([1, 2, 2, 2])

    class Racy(WorkerHandle):
        @property
        def _interrupt_gen(self) -> int:
            return next(reads)

        @_interrupt_gen.setter
        def _interrupt_gen(self, value: int) -> None:
            pass

    h = Racy([Engine.FAKE])
    monkeypatch.setattr(h, "_reset", lambda: None)
    monkeypatch.setattr(h, "_call", lambda *a, **k: {})
    h.ensure_model(_model(), BUDGET)
    # armed with the generation seen at the check; the later interrupt (2) stays unconsumed
    assert h._seen_gen == 1


FULL_DUTY = Budget(profile=CapacityProfile.FULL, memory_gb=8, gpu_duty_cycle=1.0, max_batch_size=8)


def test_ticks_keep_long_run_alive_past_step_timeout() -> None:
    w = WorkerHandle([Engine.FAKE], test_hooks=True, step_timeout_s=1.0, tick_interval_s=0.05)
    seen: list[int] = []
    try:
        w.ensure_model(_model(TaskType.COMPLETION, "c"), FULL_DUTY)
        t0 = time.monotonic()
        out = w.run(
            [{"id": 1, "sleep_steps": 20, "step_s": 0.15}, {"id": 2, "text": "z"}],
            on_progress=seen.append,
        )
        assert time.monotonic() - t0 > 1.0  # outlived the inactivity timeout
        assert out[-1] == {"id": 2, "completion": "z"}
        assert seen and all(isinstance(n, int) for n in seen)
    finally:
        w.close()


def test_no_ticks_means_worker_crashed() -> None:
    w = WorkerHandle([Engine.FAKE], test_hooks=True, step_timeout_s=0.5, tick_interval_s=0.05)
    try:
        w.ensure_model(_model(TaskType.COMPLETION, "c"), FULL_DUTY)
        with pytest.raises(WorkerCrashed, match="timed out"):
            w.run([{"id": 1, "sleep_s": 30}])
    finally:
        w.close()


def test_runner_error_retryable_round_trips() -> None:
    w = WorkerHandle([Engine.FAKE], test_hooks=True)
    try:
        w.ensure_model(_model(TaskType.COMPLETION, "c"), BUDGET)
        with pytest.raises(RunnerError) as ei:
            w.run([{"id": 1, "fail": "x", "retryable": False}])
        assert ei.value.code == "INJECTED_FAILURE" and ei.value.retryable is False
        with pytest.raises(RunnerError) as ei:
            w.run([{"id": 1, "fail": "x"}])
        assert ei.value.retryable is True
    finally:
        w.close()


def test_engine_outside_set_is_refused_non_retryable() -> None:
    w = WorkerHandle([Engine.OLLAMA])
    try:
        with pytest.raises(RunnerError) as ei:
            w.ensure_model(_model(), BUDGET)  # fake model on an ollama-only worker
        assert ei.value.code == "ENGINE_NOT_ALLOWED" and not ei.value.retryable
    finally:
        w.close()


def test_probe_reports_versions_and_leaves_handle_usable() -> None:
    w = WorkerHandle([Engine.FAKE])
    try:
        assert w.probe() == {"fake": "fake"}
        assert w.pid is None  # probe worker shut down
        w.ensure_model(_model(), BUDGET)
        assert w.run([{"id": 1, "text": "a"}])
    finally:
        w.close()


def test_stats_and_unload() -> None:
    w = WorkerHandle([Engine.FAKE])
    try:
        w.ensure_model(_model(), BUDGET)
        st = w.stats()
        assert st["active_bytes"] == 0 and st["peak_bytes"] == 0 and st["max_rss_bytes"] > 0
        pid = w.pid
        w.unload()
        assert w.pid is None and w.loaded_model_id is None
        w.ensure_model(_model(), BUDGET)  # respawnable
        assert w.pid != pid
    finally:
        w.close()


def test_worker_run_without_load_and_prompt_too_long() -> None:
    w = WorkerHandle([Engine.FAKE], engine_config=EngineConfig(max_prompt_tokens=300))
    try:
        w.ensure_model(_model(TaskType.COMPLETION, "c"), FULL_DUTY)
        out = w.run(
            [{"id": 1, "prompt": "x" * 100}, {"id": 2, "prompt": "ok"}],
            CompletionParams(max_tokens=256),
        )
        assert out == [{"id": 1, "error": "PROMPT_TOO_LONG"}, {"id": 2, "completion": "ok"}]
    finally:
        w.close()
