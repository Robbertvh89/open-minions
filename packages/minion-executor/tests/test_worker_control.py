"""Live worker control (TASK-5.3): governor-driven duty and pause, per-batch budget, CPU priority,
footprint, pacer accuracy. Everything here uses the fake engine (no GPU, no network)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from minion_common.enums import CapacityProfile, CpuPriority, Engine, TaskType
from minion_common.schemas import Budget, ModelEntry
from minion_executor.worker.control import WorkerCtl, parse_ack, parse_usage
from minion_executor.worker.footprint import process_footprint_bytes
from minion_executor.worker.process import WorkerCrashed, WorkerHandle
from minion_executor.worker.throttle import DutyCyclePacer

MODEL = ModelEntry(
    model_id="fake-c",
    type=TaskType.COMPLETION,
    engine=Engine.FAKE,
    ref="o/m",
    revision="a" * 40,
    memory_gb=1,
)


def _budget(duty: float = 1.0, prio: CpuPriority | None = None, batch: int = 8) -> Budget:
    return Budget(
        profile=CapacityProfile.BALANCED,
        memory_gb=8,
        gpu_duty_cycle=duty,
        max_batch_size=batch,
        cpu_priority=prio,
    )


def _records(n: int, step_s: float) -> list[dict[str, Any]]:
    return [{"id": i, "sleep_s": step_s, "text": "x"} for i in range(n)]


def _until(cond: Callable[[], bool], timeout: float = 10.0) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return True
        time.sleep(0.02)
    return cond()


class _Bg:
    """Runs `w.run(...)` on a thread, recording progress and the outcome."""

    def __init__(self, w: WorkerHandle, batch: list[dict[str, Any]], **kw: Any) -> None:
        self.seen: list[int] = []
        self.result: list[dict[str, Any]] | None = None
        self.error: BaseException | None = None
        self.elapsed = 0.0

        def go() -> None:
            t0 = time.monotonic()
            try:
                self.result = w.run(batch, None, self.seen.append, **kw)
            except BaseException as exc:
                self.error = exc
            self.elapsed = time.monotonic() - t0

        self.thread = threading.Thread(target=go, daemon=True)
        self.thread.start()

    @property
    def progress(self) -> int:
        return max(self.seen, default=0)

    def finished(self) -> bool:
        return not self.thread.is_alive()

    def join(self, timeout: float = 20.0) -> None:
        self.thread.join(timeout)
        assert not self.thread.is_alive(), "run did not finish"
        assert self.error is None, self.error


@pytest.fixture
def worker() -> Iterator[WorkerHandle]:
    w = WorkerHandle([Engine.FAKE], test_hooks=True, tick_interval_s=0.05)
    try:
        yield w
    finally:
        w.close()


# -- pacer (pure) -------------------------------------------------------------------------------
class _Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.waits: list[float] = []

    def __call__(self) -> float:
        return self.now

    def wait(self, s: float) -> bool:
        self.waits.append(s)
        self.now += s
        return False


def test_pacer_set_duty_applies_to_the_next_step() -> None:
    c = _Clock()
    p = DutyCyclePacer(0.5, c, c.wait)
    c.now += 1.0
    assert p.step_done() == pytest.approx(1.0)
    p.set_duty(0.25)
    c.now += 1.0
    assert p.step_done() == pytest.approx(3.0)
    p.set_duty(1.0)
    c.now += 1.0
    assert p.step_done() == 0.0
    with pytest.raises(ValueError):
        p.set_duty(0.0)
    with pytest.raises(ValueError):
        p.set_duty(float("nan"))


def test_pacer_accumulates_busy_and_wall() -> None:
    c = _Clock()
    p = DutyCyclePacer(0.5, c, c.wait)
    for _ in range(3):
        c.now += 2.0
        p.step_done()
    assert p.busy_s == pytest.approx(6.0)
    assert p.wall_s == pytest.approx(12.0)
    p.reset()
    assert p.busy_s == 0.0 and p.wall_s == 0.0


def test_pacer_wait_interrupted_by_a_duty_change_rederives_the_sleep() -> None:
    c = _Clock()
    p = DutyCyclePacer(0.1, c, lambda s: False)  # replaced below
    calls: list[float] = []

    def wait(s: float) -> bool:
        calls.append(s)
        if len(calls) == 1:
            c.now += 4.0  # 4 s into a 9 s sleep ...
            p.set_duty(0.5)  # ... the governor raises the duty: 1 s step now owes only 1 s total
            return True
        c.now += s
        return False

    p._wait = wait
    c.now += 1.0
    p.step_done()
    assert calls[0] == pytest.approx(9.0)
    assert len(calls) == 1  # 4 s already slept covers the 1 s now owed: nothing more to wait


def test_pacer_pause_time_is_excluded_from_busy_wall_and_slept() -> None:
    c = _Clock()
    p = DutyCyclePacer(0.5, c, lambda s: False)
    state = {"n": 0}

    def wait(s: float) -> bool:
        state["n"] += 1
        if state["n"] == 1:
            c.now += 1.0
            c.now += 100.0  # paused mid-sleep for 100 s
            p.exclude(100.0)
            return True
        c.now += s
        return False

    p._wait = wait
    c.now += 3.0
    assert p.step_done() == pytest.approx(3.0)  # 1 + the remaining 2, not 103
    assert p.busy_s == pytest.approx(3.0)
    assert p.wall_s == pytest.approx(6.0)


def test_pacer_oversleep_is_credited_against_the_next_wait() -> None:
    c = _Clock()

    def sluggish(s: float) -> bool:
        c.now += s + 0.5  # the scheduler wakes us 0.5 s late
        return False

    p = DutyCyclePacer(0.5, c, sluggish)
    c.now += 1.0
    assert p.step_done() == pytest.approx(1.5)  # owed 1.0, took 1.5
    c.now += 1.0
    assert p.step_done() == pytest.approx(0.5 + 0.5)  # owed 1.0 - 0.5 credit = 0.5 (+0.5 late)
    assert p.busy_s / p.wall_s == pytest.approx(2.0 / 4.5)  # only the last 0.5 s is unrepaid


# -- message validation ----------------------------------------------------------------------
def test_ctl_message_is_parsed_strictly() -> None:
    ok = {"op": "ctl", "gen": 3, "duty": 0.5, "paused": False, "trim": False}
    assert WorkerCtl.from_msg(ok) == WorkerCtl(3, 0.5, False, False)
    assert WorkerCtl.from_msg(WorkerCtl(1, 1, True, True).to_msg()) == WorkerCtl(1, 1.0, True, True)
    bad: list[dict[str, Any]] = [
        {**ok, "gen": True},
        {**ok, "gen": -1},
        {**ok, "gen": "3"},
        {**ok, "duty": 0},
        {**ok, "duty": 1.5},
        {**ok, "duty": float("nan")},
        {**ok, "duty": "0.5"},
        {**ok, "duty": True},
        {**ok, "paused": 0},
        {**ok, "trim": None},
    ]
    for m in bad:
        with pytest.raises(ValueError):
            WorkerCtl.from_msg(m)
    for k in ("gen", "duty", "paused", "trim"):
        with pytest.raises(ValueError):
            WorkerCtl.from_msg({kk: v for kk, v in ok.items() if kk != k})


def test_ack_and_usage_parsing() -> None:
    assert parse_ack({"tick": 1}, issued=2, at=1.0) is None
    a = parse_ack({"tick": 1, "ack": 2, "paused": True}, issued=2, at=1.0)
    assert a is not None and a.gen == 2 and a.paused is True and a.at == 1.0
    for bad in (
        {"ack": 3, "paused": False},  # never issued
        {"ack": -1, "paused": False},
        {"ack": True, "paused": False},
        {"ack": "1", "paused": False},
        {"ack": 1},
        {"ack": 1, "paused": 1},
        {"paused": True},
    ):
        with pytest.raises(ValueError):
            parse_ack({"tick": 1, **bad}, issued=2, at=0.0)
    assert parse_usage({"tick": 1}) is None
    u = parse_usage({"busy_s": 1, "wall_s": 4.0})
    assert u is not None and u.share == 0.25
    for bad_u in (
        {"busy_s": 1},
        {"busy_s": -1, "wall_s": 1},
        {"busy_s": float("nan"), "wall_s": 1},
        {"busy_s": float("inf"), "wall_s": 1},
        {"busy_s": True, "wall_s": 1},
        {"busy_s": "1", "wall_s": 1},
    ):
        with pytest.raises(ValueError):
            parse_usage(bad_u)


def _adversary(conn: Any, payload: bytes) -> None:  # forked child: answer the request with a tick
    conn.recv_bytes()
    conn.send_bytes(payload)
    time.sleep(30)


@pytest.mark.parametrize(
    "payload",
    [
        b'{"tick": 1, "ack": "x", "paused": false}',
        b'{"tick": 1, "ack": 99, "paused": false}',
        b'{"tick": 1, "ack": 0, "paused": "no"}',
        b'{"tick": 1, "paused": true}',
        b'{"tick": 1, "busy_s": -1, "wall_s": 1}',
        b'{"tick": 1, "busy_s": NaN, "wall_s": 1}',
        b'{"tick": 1, "busy_s": 1}',
        b'{"tick": 1, "busy_s": true, "wall_s": 1}',
    ],
)
def test_malformed_ack_or_usage_is_a_protocol_violation(payload: bytes) -> None:
    import multiprocessing as mp

    ctx = mp.get_context("fork")
    parent, child = ctx.Pipe()
    proc = ctx.Process(target=_adversary, args=(child, payload), daemon=True)
    proc.start()
    child.close()
    w = WorkerHandle([Engine.FAKE], step_timeout_s=5.0)
    w._proc, w._conn, w._model_id = proc, parent, "m"
    with pytest.raises(WorkerCrashed):
        w.run([{"id": 1}])
    assert not proc.is_alive() and w.pid is None and w.last_ack() is None


# -- live control: pause, duty, ack ------------------------------------------------------------
def test_pause_stops_progress_and_resume_continues(worker: WorkerHandle) -> None:
    worker.ensure_model(MODEL, _budget())
    bg = _Bg(worker, _records(80, 0.05))  # ~4 s
    assert _until(lambda: bg.progress >= 3)
    gen = worker.control(duty=1.0, paused=True)
    assert _until(lambda: (a := worker.last_ack()) is not None and a.gen == gen)
    ack = worker.last_ack()
    assert ack is not None and ack.paused is True
    frozen = bg.progress
    time.sleep(0.6)
    assert bg.progress == frozen and not bg.finished()
    gen2 = worker.control(duty=1.0, paused=False)
    assert gen2 > gen
    bg.join()
    assert bg.result is not None and len(bg.result) == 80
    last = worker.last_ack()
    assert last is not None and last.gen == gen2 and last.paused is False


def test_paused_longer_than_the_step_timeout_is_not_a_hang() -> None:
    w = WorkerHandle([Engine.FAKE], test_hooks=True, tick_interval_s=0.1, step_timeout_s=1.0)
    try:
        w.ensure_model(MODEL, _budget())
        bg = _Bg(w, _records(20, 0.05))
        assert _until(lambda: bg.progress >= 2)
        w.control(duty=1.0, paused=True)
        time.sleep(2.5)  # > 2x the inactivity limit
        assert not bg.finished() and bg.error is None
        pid = w.pid
        w.control(duty=1.0, paused=False)
        bg.join()
        assert w.pid == pid and bg.result is not None and len(bg.result) == 20
    finally:
        w.close()


def test_duty_change_cuts_a_running_pacer_sleep_short(worker: WorkerHandle) -> None:
    worker.ensure_model(MODEL, _budget(duty=0.05))  # a 0.1 s step then sleeps 1.9 s
    bg = _Bg(worker, _records(6, 0.1))  # ~11 s if the duty were not raised
    assert _until(lambda: bg.progress >= 1)
    t0 = time.monotonic()
    gen = worker.control(duty=1.0, paused=False)
    assert _until(lambda: (a := worker.last_ack()) is not None and a.gen == gen, 3.0)
    assert time.monotonic() - t0 < 1.5  # acked mid-sleep, not after it
    bg.join(10.0)
    assert bg.elapsed < 6.0


def test_pause_during_a_pacer_sleep_takes_effect_at_once(worker: WorkerHandle) -> None:
    worker.ensure_model(MODEL, _budget(duty=0.05))
    bg = _Bg(worker, _records(4, 0.1))
    assert _until(lambda: bg.progress >= 1)
    t0 = time.monotonic()
    gen = worker.control(duty=0.05, paused=True)
    assert _until(lambda: (a := worker.last_ack()) is not None and a.gen == gen, 3.0)
    assert time.monotonic() - t0 < 1.5
    frozen = bg.progress
    time.sleep(0.4)
    assert bg.progress == frozen
    worker.control(duty=1.0, paused=False)
    bg.join()


def test_stale_generation_is_ignored(worker: WorkerHandle) -> None:
    worker.ensure_model(MODEL, _budget())
    bg = _Bg(worker, _records(60, 0.05))
    assert _until(lambda: bg.progress >= 2)
    g1 = worker.control(duty=1.0, paused=True)
    assert _until(lambda: (a := worker.last_ack()) is not None and a.gen == g1)
    conn = worker._conn
    assert conn is not None
    stale = WorkerCtl(gen=g1 - 1, duty=1.0, paused=False).to_msg()
    conn.send_bytes(json.dumps(stale).encode())  # an out-of-order older message
    time.sleep(0.4)
    ack = worker.last_ack()
    assert ack is not None and ack.gen == g1 and ack.paused is True
    frozen = bg.progress
    time.sleep(0.3)
    assert bg.progress == frozen  # still paused
    worker.control(duty=1.0, paused=False)
    bg.join()


def test_invalid_control_is_rejected_before_anything_is_sent(worker: WorkerHandle) -> None:
    worker.ensure_model(MODEL, _budget())
    for duty in (0.0, -1.0, 1.5, float("nan")):
        with pytest.raises(ValueError):
            worker.control(duty=duty, paused=False)
    assert worker.run([{"id": 1, "text": "a"}])  # protocol still in sync


def test_control_while_idle_is_silent_and_does_not_desync_calls(worker: WorkerHandle) -> None:
    worker.ensure_model(MODEL, _budget())
    for i in range(5):
        worker.control(duty=0.5 + i / 10, paused=False, trim=i == 2)
    assert worker.stats()["max_rss_bytes"] > 0  # the reply is stats', not a stray ctl reply
    assert worker.last_ack() is None  # nothing acked outside a run
    out = worker.run([{"id": 1, "text": "a"}])
    assert out[0]["completion"] == "a"
    ack = worker.last_ack()
    assert ack is not None and ack.gen == 5  # the run started under the newest state


def test_control_before_any_worker_is_sent_with_the_first_run() -> None:
    w = WorkerHandle([Engine.FAKE], test_hooks=True, tick_interval_s=0.05)
    try:
        gen = w.control(duty=1.0, paused=True)  # no process yet: just stored
        w.ensure_model(MODEL, _budget())
        bg = _Bg(w, _records(3, 0.01))
        assert _until(lambda: (a := w.last_ack()) is not None and a.gen == gen)
        time.sleep(0.3)
        assert not bg.finished()  # it started paused and has done no work
        w.control(duty=1.0, paused=False)
        bg.join()
    finally:
        w.close()


def test_control_after_close_or_crash_never_raises(worker: WorkerHandle) -> None:
    worker.ensure_model(MODEL, _budget())
    worker.kill()
    worker.control(duty=0.5, paused=False)
    worker.close()
    worker.control(duty=0.5, paused=True)


# -- budget and trim -----------------------------------------------------------------------------
def _state(w: WorkerHandle, budget: Budget | None = None) -> dict[str, Any]:
    (out,) = w.run([{"id": 1, "report_state": 1}], budget=budget)
    return out


def test_run_budget_reaches_the_runner_only_when_it_changes(worker: WorkerHandle) -> None:
    first = _budget(0.5, batch=8)
    worker.ensure_model(MODEL, first)
    assert _state(worker)["budget"] == first.model_dump(mode="json")
    smaller = _budget(0.5, batch=2)
    assert _state(worker, smaller)["budget"] == smaller.model_dump(mode="json")
    assert _state(worker)["budget"] == smaller.model_dump(mode="json")  # None keeps the last


def test_budget_duty_drives_the_pacer_until_control_overrides_it(worker: WorkerHandle) -> None:
    worker.ensure_model(MODEL, _budget(1.0))
    worker.run(_records(10, 0.02), budget=_budget(0.5))
    share = worker.gpu_share()
    assert share is not None and abs(share - 0.5) < 0.05
    worker.control(duty=0.25, paused=False)
    worker.run(_records(10, 0.02), budget=_budget(0.5))  # ctl wins over the budget's duty
    share = worker.gpu_share()
    assert share is not None and abs(share - 0.25) < 0.05


def test_trim_is_one_shot_and_works_idle_and_mid_run(worker: WorkerHandle) -> None:
    worker.ensure_model(MODEL, _budget())
    worker.control(duty=1.0, paused=False, trim=True)  # idle
    assert _state(worker)["trims"] == 1
    bg = _Bg(worker, [*_records(40, 0.05), {"id": "s", "report_state": 1}])
    assert _until(lambda: bg.progress >= 2)
    worker.control(duty=1.0, paused=False, trim=True)  # mid-run
    bg.join()
    assert bg.result is not None and bg.result[-1]["trims"] == 2
    assert _state(worker)["trims"] == 2  # not re-sent with later runs


# -- CPU priority ---------------------------------------------------------------------------
def test_cpu_priority_is_raised_live_and_never_lowered(worker: WorkerHandle) -> None:
    worker.ensure_model(MODEL, _budget(prio=CpuPriority.NORMAL))
    base = _state(worker)["nice"]
    low = _state(worker, _budget(prio=CpuPriority.LOW))
    assert low["nice"] == max(base, 10)  # base: inherited, other tests may have niced us
    assert low["background"] is False
    back = _state(worker, _budget(prio=CpuPriority.NORMAL))
    assert back["nice"] == low["nice"]  # unprivileged: lowering needs a respawn
    bg = _state(worker, _budget(prio=CpuPriority.BACKGROUND))
    assert bg["nice"] == max(base, 15)
    assert bg["background"] is (sys.platform == "darwin")
    again = _state(worker, _budget(prio=CpuPriority.LOW))
    assert again["background"] is False  # Darwin background QoS is reversible


def test_throttled_worker_gets_few_threads_env() -> None:
    for nice, expect in ((0, False), (10, True)):
        w = WorkerHandle([Engine.FAKE], test_hooks=True, nice=nice)
        try:
            w.ensure_model(MODEL, _budget())
            (out,) = w.run([{"id": 1, "echo_env": "OMP_NUM_THREADS"}])
            assert out["present"] is expect
        finally:
            w.close()


# -- pacer accuracy ------------------------------------------------------------------------------
@pytest.mark.parametrize("duty", [0.3, 0.5, 1.0])
def test_measured_gpu_share_is_within_3pp_of_the_duty_cycle(
    worker: WorkerHandle, duty: float
) -> None:
    """Control-law check with the fake runner: busy/wall as the pacer measures it (pauses
    excluded) hits the target, also when the machine is loaded (oversleep is credited back)."""
    worker.ensure_model(MODEL, _budget(duty))
    t0 = time.monotonic()
    out = worker.run(_records(40, 0.02))
    external = time.monotonic() - t0
    assert len(out) == 40
    share = worker.gpu_share()
    assert share is not None
    assert abs(share - duty) <= 0.03, share
    if duty < 1.0:  # the wall clock agrees: 0.8 s of work paced to ~0.8 / duty seconds
        assert external >= 0.8 / duty * 0.9


def test_gpu_share_survives_a_pause_in_the_middle(worker: WorkerHandle) -> None:
    worker.ensure_model(MODEL, _budget(0.5))
    bg = _Bg(worker, _records(40, 0.02))
    assert _until(lambda: bg.progress >= 5)
    worker.control(duty=0.5, paused=True)
    time.sleep(0.8)  # would wreck the share if pause time counted as wall
    worker.control(duty=0.5, paused=False)
    bg.join()
    share = worker.gpu_share()
    assert share is not None and abs(share - 0.5) <= 0.03, share


# -- footprint -------------------------------------------------------------------------------------
def test_footprint_of_own_and_child_process() -> None:
    own = process_footprint_bytes(os.getpid())
    if sys.platform != "darwin" and not sys.platform.startswith("linux"):
        assert own is None
        return
    assert own is not None and 1_000_000 < own < 64 * 1024**3
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        assert _until(lambda: (process_footprint_bytes(child.pid) or 0) > 1_000_000, 10.0)
        got = process_footprint_bytes(child.pid)
        assert got is not None and got > 1_000_000
    finally:
        child.kill()
        child.wait()


def test_footprint_tracks_allocation() -> None:
    before = process_footprint_bytes(os.getpid())
    if before is None:
        pytest.skip("no footprint source on this platform")
    blob = bytearray(200 * 1024**2)
    for i in range(0, len(blob), 4096):
        blob[i] = 1  # touch every page
    after = process_footprint_bytes(os.getpid())
    assert after is not None and after - before > 150 * 1024**2
    del blob


def test_footprint_unknown_pids_are_none() -> None:
    assert process_footprint_bytes(0) is None
    assert process_footprint_bytes(-5) is None
    assert process_footprint_bytes(2**22 + 12345) is None


# -- runner protocol -----------------------------------------------------------------------------
def test_every_runner_class_implements_set_budget_and_trim() -> None:
    from minion_executor.runners.fake import FakeRunner
    from minion_executor.runners.hf import HfCompletionRunner, HfEmbeddingRunner
    from minion_executor.runners.mlx_completion import MlxCompletionRunner
    from minion_executor.runners.mlx_embedding import MlxEmbeddingRunner
    from minion_executor.runners.ollama import OllamaRunner

    for cls in (
        FakeRunner,
        HfCompletionRunner,
        HfEmbeddingRunner,
        MlxCompletionRunner,
        MlxEmbeddingRunner,
        OllamaRunner,
    ):
        assert callable(getattr(cls, "set_budget", None)), cls
        assert callable(getattr(cls, "trim", None)), cls


def test_ollama_set_budget_changes_the_fan_out_cap() -> None:
    from minion_executor.runners.ollama import OllamaRunner

    r = OllamaRunner()
    r.set_budget(_budget(batch=3))
    assert r._budget is not None and r._budget.max_batch_size == 3
    r.trim()  # nothing cached in-process: a no-op, must not raise
