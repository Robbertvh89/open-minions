from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
from multiprocessing.process import BaseProcess
from pathlib import Path
from typing import Any

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

    def will_sleep(self) -> bool:
        return False

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


def test_missing_hf_extra_is_never_advertised_and_unknown_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib

    from minion_executor.runners import probe_engines

    def no_module(name: str) -> object:
        raise ImportError(name)

    monkeypatch.setattr(importlib, "import_module", no_module)
    assert probe_engines([Engine.HF, Engine.FAKE], EngineConfig()) == {"hf": None, "fake": "fake"}
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


def test_duty_cycle_pacer_full_duty_never_sleeps_and_long_steps_not_capped() -> None:
    c = _Clock()
    p = DutyCyclePacer(1.0, c, c.sleep)
    c.now += 5.0
    assert p.step_done() == 0.0 and c.slept == []
    long = DutyCyclePacer(0.1, c, c.sleep)
    c.now += 1000.0  # 1000 s step at d=0.1 -> 9000 s pause, not clamped
    assert long.step_done() == pytest.approx(9000.0)
    with pytest.raises(ValueError):
        DutyCyclePacer(0.0, c, c.sleep)


def test_pacer_ticks_carry_records_done_midbatch() -> None:
    """With duty < 1 the pacer's own ticks must not swallow the records_done update."""
    b = Budget(profile=CapacityProfile.BALANCED, memory_gb=8, gpu_duty_cycle=0.5, max_batch_size=8)
    w = WorkerHandle([Engine.FAKE], test_hooks=True, tick_interval_s=0.1)
    seen: list[int] = []
    try:
        w.ensure_model(_model(TaskType.COMPLETION, "c"), b)
        batch = [{"id": i, "sleep_s": 0.3, "text": "x"} for i in range(6)]
        w.run(batch, on_progress=seen.append)
    finally:
        w.close()
    assert seen == sorted(seen) and len(set(seen)) >= 3 and max(seen) >= 3


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
    assert b.cpu_priority is not None
    with pytest.raises(ValueError):
        parse_profile("custom")  # needs [profiles.custom] in executor.toml
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


def test_engine_outside_set_is_refused_retryable() -> None:
    w = WorkerHandle([Engine.OLLAMA])
    try:
        with pytest.raises(RunnerError) as ei:
            w.ensure_model(_model(), BUDGET)  # fake model on an ollama-only worker
        assert ei.value.code == "ENGINE_NOT_ALLOWED" and ei.value.retryable
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


def test_unload_asks_the_worker_to_unload_before_the_kill(monkeypatch: pytest.MonkeyPatch) -> None:
    """The runner gets a graceful `unload` op (Ollama `keep_alive: 0`) with a short timeout."""
    w = WorkerHandle([Engine.FAKE])
    try:
        w.ensure_model(_model(), BUDGET)
        proc = w._proc
        assert proc is not None
        sent: list[tuple[str, float]] = []
        real_call = w._call

        def spy(msg: dict[str, object], timeout_s: float, **kw: object) -> dict:  # type: ignore[type-arg]
            sent.append((str(msg["op"]), timeout_s))
            assert kw.get("hard_deadline") is not None  # ticks cannot extend the unload
            return real_call(msg, timeout_s, **kw)  # type: ignore[arg-type]

        monkeypatch.setattr(w, "_call", spy)
        w.unload()
        assert sent == [("unload", 5.0)]
        assert not proc.is_alive() and w.pid is None
    finally:
        w.close()


def test_unload_still_kills_when_the_graceful_unload_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    w = WorkerHandle([Engine.FAKE])
    try:
        w.ensure_model(_model(), BUDGET)
        proc = w._proc
        assert proc is not None

        def hang(*a: object, **k: object) -> dict:  # type: ignore[type-arg]
            raise WorkerCrashed("worker timed out")

        monkeypatch.setattr(w, "_call", hang)
        w.unload()
        assert not proc.is_alive() and w.pid is None
    finally:
        w.close()


def test_unload_without_a_model_does_not_spawn_a_worker() -> None:
    w = WorkerHandle([Engine.FAKE])
    w.unload()
    assert w.pid is None


def test_worker_unload_op_drops_the_runner_and_release_flag_crosses_the_pipe() -> None:
    import multiprocessing as mp

    from minion_executor.worker.process import DEFAULT_MAX_MESSAGE_BYTES, _recv, _send, worker_main

    parent, child = mp.get_context("spawn").Pipe()
    t = threading.Thread(target=worker_main, args=(child, ["fake"]), daemon=True)
    t.start()

    def call(msg: dict[str, object]) -> dict[str, object]:
        _send(parent, msg)
        assert parent.poll(10)
        return _recv(parent, DEFAULT_MAX_MESSAGE_BYTES)

    model = _model().model_dump(mode="json")
    assert call({"op": "load", "model": model, "budget": BUDGET.model_dump(mode="json")})["ok"]
    assert call({"op": "unload"}) == {"ok": True, "result": None}
    assert call({"op": "run", "batch": [], "params": {}})["code"] == "not_loaded"
    assert call({"op": "shutdown"})["ok"]
    t.join(5)


def test_runner_error_release_flag_crosses_the_pipe() -> None:
    w = WorkerHandle([Engine.FAKE], test_hooks=True)
    try:
        w.ensure_model(_model(TaskType.COMPLETION, "c"), BUDGET)
        with pytest.raises(RunnerError) as ei:
            w.run([{"id": 1, "fail": "x", "release": True}])
        assert ei.value.release is True
        with pytest.raises(RunnerError) as ei:
            w.run([{"id": 1, "fail": "x"}])
        assert ei.value.release is False
    finally:
        w.close()


def test_model_integrity_error_resets_the_worker() -> None:
    w = WorkerHandle([Engine.FAKE], test_hooks=True)
    try:
        w.ensure_model(_model(), BUDGET)
        assert w.pid is not None

        def boom(*a: object, **k: object) -> dict:  # type: ignore[type-arg]
            raise RunnerError("MODEL_INTEGRITY", "x", release=True)

        w._call = boom  # type: ignore[method-assign]
        with pytest.raises(RunnerError):
            w.run([])
        assert w.pid is None
    finally:
        w.close()


def test_model_switch_sends_unload_before_the_respawn(monkeypatch: pytest.MonkeyPatch) -> None:
    w = WorkerHandle([Engine.FAKE])
    try:
        w.ensure_model(_model(mid="a"), BUDGET)
        first = w._proc
        assert first is not None
        ops: list[str] = []
        real_call = w._call

        def spy(msg: dict, *a: object, **k: object) -> dict:  # type: ignore[type-arg]
            ops.append(str(msg["op"]))
            return real_call(msg, *a, **k)  # type: ignore[arg-type]

        monkeypatch.setattr(w, "_call", spy)
        w.ensure_model(_model(mid="b"), BUDGET)
        assert ops == ["unload", "load"]
        assert not first.is_alive() and w.loaded_model_id == "b"
    finally:
        w.close()


def _hostile_worker(mode: str) -> tuple[WorkerHandle, BaseProcess]:
    """A handle whose 'worker' is a thread-less fake pipe peer misbehaving on `unload`."""
    w = WorkerHandle([Engine.FAKE])
    w.ensure_model(_model(), BUDGET)
    proc = w._proc
    assert proc is not None
    real_send_conn = w._conn

    class Peer:
        """Replaces the control-side Connection: answers `unload` hostilely."""

        def __init__(self) -> None:
            self.sent = 0

        def send_bytes(self, data: bytes) -> None:
            self.sent += 1

        def poll(self, timeout: float = 0.0) -> bool:
            time.sleep(min(timeout, 0.01))
            return True

        def recv_bytes(self, maxlength: int) -> bytes:
            return {
                "list": b"[]",
                "no_ok": b'{"result": 1}',
                "garbage": b"not json",
                "ticks": b'{"tick": 1}',
            }[mode]

        def close(self) -> None:
            if real_send_conn is not None:
                real_send_conn.close()

    w._conn = Peer()  # type: ignore[assignment]
    return w, proc


@pytest.mark.parametrize("mode", ["list", "no_ok", "garbage", "ticks"])
def test_hostile_unload_reply_still_kills_the_worker_within_the_deadline(
    mode: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import minion_executor.worker.process as wp

    monkeypatch.setattr(wp, "UNLOAD_TIMEOUT_S", 0.5)
    w, proc = _hostile_worker(mode)
    t0 = time.monotonic()
    w.unload()
    assert time.monotonic() - t0 < 5
    assert not proc.is_alive() and w.pid is None


def test_kill_releases_an_ollama_model_best_effort(monkeypatch: pytest.MonkeyPatch) -> None:
    import minion_executor.runners.ollama as om

    seen: list[tuple[str, str]] = []
    monkeypatch.setattr(om, "release_model", lambda url, ref: seen.append((url, ref)))
    w = WorkerHandle([Engine.FAKE])
    w.ensure_model(_model(), BUDGET)
    w._ollama_ref = "qwen2.5:0.5b"
    w.interrupt()
    w.kill()
    assert seen and seen[0][1] == "qwen2.5:0.5b"
    w.close()


def test_mlx_workers_get_use_torch_off_and_others_do_not() -> None:
    w = WorkerHandle([Engine.MLX, Engine.HF])
    assert w._engine_env == {}
    mlx = ModelEntry(
        model_id="m", type=TaskType.EMBEDDING, engine=Engine.MLX, ref="o/m",
        revision="a" * 40, memory_gb=1,
    )  # fmt: skip
    w._call = lambda *a, **k: {"ok": True}  # type: ignore[method-assign]
    w.ensure_model(mlx, BUDGET)
    assert w._engine_env == {"USE_TORCH": "0"}
    hf = mlx.model_copy(update={"engine": Engine.HF, "model_id": "h"})
    w.ensure_model(hf, BUDGET)
    assert w._engine_env == {}


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


def test_self_test_exit_code_maps_to_sandbox_failed_not_a_crash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import multiprocessing as mp

    from minion_executor.worker.sandbox import SANDBOX_FAILED, SELF_TEST_EXIT_CODE

    class DeadProc:
        exitcode = SELF_TEST_EXIT_CODE

        def is_alive(self) -> bool:
            return False

        def join(self, timeout: float | None = None) -> None: ...

    for close_pipe in (False, True):  # exited while silent, and pipe EOF
        w = WorkerHandle([Engine.FAKE])
        ours, theirs = mp.Pipe()
        if close_pipe:
            theirs.close()
        w._proc = DeadProc()  # type: ignore[assignment]
        monkeypatch.setattr(w, "_ensure_started", lambda c=ours: c)
        with pytest.raises(RunnerError) as ei:
            w.stats()
        assert ei.value.code == SANDBOX_FAILED and ei.value.retryable
        assert w._proc is None  # reset


def test_probe_sandbox_spec_carries_the_ollama_port(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from minion_executor.runners.base import EngineConfig
    from minion_executor.worker.sandbox import SandboxPolicy

    cfg = EngineConfig(ollama_url="http://127.0.0.1:23456")
    w = WorkerHandle([Engine.OLLAMA, Engine.FAKE], engine_config=cfg, sandbox=SandboxPolicy())
    w._tmp_dir = tmp_path
    monkeypatch.setattr(w, "_ensure_started", lambda: None)
    spec = w._sandbox_spec(None, None)
    assert spec is not None and spec["ollama_port"] == 23456
    no_ollama = WorkerHandle([Engine.FAKE], engine_config=cfg, sandbox=SandboxPolicy())
    no_ollama._tmp_dir = tmp_path
    monkeypatch.setattr(no_ollama, "_ensure_started", lambda: None)
    spec = no_ollama._sandbox_spec(None, None)
    assert spec is not None and spec["ollama_port"] is None


# -- adversarial workers: partial frames must not defeat the deadlines --------------------------
def _adversary(conn: object, mode: str) -> None:  # runs in a forked child
    import struct

    c: Any = conn
    c.recv_bytes()  # the request
    fd = c.fileno()
    if mode == "drip":
        for b in struct.pack("!i", 12) + b'{"ok": true}':
            os.write(fd, bytes([b]))
            time.sleep(1.0)
    elif mode == "header":
        os.write(fd, b"\x00\x00")
    elif mode == "body":
        for _ in range(3):
            c.send_bytes(b'{"tick": 1}')
            time.sleep(0.1)
        os.write(fd, struct.pack("!i", 100) + b"abc")
    elif mode == "huge":
        os.write(fd, struct.pack("!i", 64 * 1024 * 1024))
    time.sleep(60)


def _adversarial_handle(mode: str, step_timeout_s: float) -> tuple[WorkerHandle, BaseProcess]:
    import multiprocessing as mp

    ctx = mp.get_context("fork")
    parent, child = ctx.Pipe()
    proc = ctx.Process(target=_adversary, args=(child, mode), daemon=True)
    proc.start()
    child.close()
    w = WorkerHandle([Engine.FAKE], step_timeout_s=step_timeout_s)
    w._proc, w._conn, w._model_id = proc, parent, "m"
    return w, proc


@pytest.mark.parametrize("mode", ["drip", "header", "body", "huge"])
def test_partial_frame_stall_on_unload_ends_in_a_kill_within_the_deadline(
    mode: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import minion_executor.worker.process as wp

    monkeypatch.setattr(wp, "UNLOAD_TIMEOUT_S", 1.0)
    w, proc = _adversarial_handle(mode, 300.0)
    t0 = time.monotonic()
    w.unload()
    assert time.monotonic() - t0 < 4
    assert not proc.is_alive() and w.pid is None


@pytest.mark.parametrize("mode", ["drip", "header", "body", "huge"])
def test_partial_frame_stall_on_run_ends_in_a_kill_within_the_step_timeout(mode: str) -> None:
    w, proc = _adversarial_handle(mode, 1.0)
    t0 = time.monotonic()
    with pytest.raises(WorkerCrashed):
        w.run([{"id": 1}])
    assert time.monotonic() - t0 < 4
    assert not proc.is_alive() and w.pid is None


def test_deeply_nested_json_reply_is_a_crash_not_an_exception() -> None:
    w, proc = _hostile_worker("garbage")
    depth = 200_000
    real = w._conn

    class Nest:
        def __getattr__(self, name: str) -> Any:
            return getattr(real, name)

        def poll(self, timeout: float = 0.0) -> bool:
            return True

        def send_bytes(self, data: bytes) -> None:
            pass

        def recv_bytes(self, maxlength: int) -> bytes:
            return b"[" * depth + b"]" * depth

    w._conn = Nest()  # type: ignore[assignment]
    with pytest.raises(WorkerCrashed):
        w.run([{"id": 1}])
    assert not proc.is_alive()


def test_hard_reset_and_graceful_unload_both_release_from_the_control_side(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import minion_executor.runners.ollama as om

    seen: list[str] = []
    monkeypatch.setattr(om, "release_model", lambda url, ref: seen.append(ref))
    w = WorkerHandle([Engine.FAKE])
    w.ensure_model(_model(), BUDGET)
    w._ollama_ref = "q:1"
    w._reset()  # e.g. a timed-out step
    assert seen == ["q:1"]
    w.ensure_model(_model(), BUDGET)
    w._ollama_ref = "q:2"
    w.unload()  # the worker said ok, but its word is not trusted: the control side releases too
    assert seen == ["q:1", "q:2"]
    w.close()


def test_a_failed_worker_unload_still_releases_once(monkeypatch: pytest.MonkeyPatch) -> None:
    import minion_executor.runners.ollama as om
    import minion_executor.worker.process as wp

    seen: list[str] = []
    monkeypatch.setattr(om, "release_model", lambda url, ref: seen.append(ref))
    monkeypatch.setattr(wp, "UNLOAD_TIMEOUT_S", 0.5)
    w, proc = _hostile_worker("no_ok")
    w._ollama_ref = "q:3"
    w.unload()
    assert not proc.is_alive() and seen == ["q:3"]


def test_kill_then_the_failing_calls_reset_release_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import minion_executor.runners.ollama as om

    seen: list[str] = []
    monkeypatch.setattr(om, "release_model", lambda url, ref: seen.append(ref))
    for hard_stop in ("kill", "interrupt"):
        seen.clear()
        w = WorkerHandle([Engine.FAKE])
        w.ensure_model(_model(), BUDGET)
        w._ollama_ref = "q:4"
        getattr(w, hard_stop)()
        w._reset()  # what the in-flight call's failure path does
        assert seen == ["q:4"], hard_stop
        w.close()


def test_kill_during_an_inflight_run_releases_the_ollama_model_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import minion_executor.runners.ollama as om

    seen: list[str] = []
    monkeypatch.setattr(om, "release_model", lambda url, ref: seen.append(ref))
    w = WorkerHandle([Engine.FAKE], test_hooks=True)
    w.ensure_model(_model(), BUDGET)
    w._ollama_ref = "q:5"
    errors: list[BaseException] = []
    in_flight = threading.Event()

    def go() -> None:
        try:
            # record 1 finishes at once (progress tick), record 2 keeps the run in flight
            w.run([{"id": 1}, {"id": 2, "sleep_s": 30}], on_progress=lambda n: in_flight.set())
        except BaseException as e:
            errors.append(e)

    t = threading.Thread(target=go)
    t.start()
    assert in_flight.wait(15)  # a tick arrived: the run is genuinely in flight
    w.kill()
    t.join(15)
    assert not t.is_alive() and isinstance(errors[0], WorkerCrashed)
    assert seen == ["q:5"]
    w.close()
