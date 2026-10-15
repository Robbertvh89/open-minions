from __future__ import annotations

import os
import signal
import time

import pytest
from minion_common.enums import CapacityProfile, Engine, TaskType
from minion_common.schemas import Budget, ModelEntry
from minion_executor.capacity.profiles import budget_for, parse_profile
from minion_executor.runners import create_runner
from minion_executor.runners.base import Runner, RunnerError
from minion_executor.worker.process import WorkerCrashed, WorkerHandle


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
    r = create_runner("fake")
    r.load(_model(TaskType.COMPLETION, "c"), BUDGET)
    assert r.run([{"id": 1, "text": "hi"}]) == [{"id": 1, "completion": "hi"}]
    with pytest.raises(RunnerError) as ei:
        r.run([{"id": 2, "fail": "oom"}])
    assert ei.value.code == "oom"


def test_unknown_runner() -> None:
    with pytest.raises(ValueError):
        create_runner("nope")


def test_worker_round_trip_in_subprocess() -> None:
    w = WorkerHandle("fake")
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
        assert ei.value.code == "oom"
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
