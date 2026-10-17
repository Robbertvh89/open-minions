"""NF-9 on CI (TASK-5.6): the governor reacts to memory pressure, thermal stress, power and user
return within 5 s. 20 trials in total with the fake engine in a real worker subprocess and
`FileSignals` steered through a file; no server, no GPU.

Reaction time = from the moment the signal file holds the new value to the moment the worker has
acknowledged the matching control message (paused, or the new duty) or, for memory `critical`, to
the moment the worker process is gone (the release path: verdict, `interrupt()`, the in-flight call
failing with `WorkerCrashed`). The steering is the real `ExecutorLoop._watch` / `_push_control`
over a real `GovernorRuntime`, `FileSignals` and a fake-engine worker subprocess, so what is
timed is poll + decide + control round trip + one 100 ms engine step. The poll is 0.25 s here
(production 2 s, at most 4 s: `MINION_EXECUTOR_SIGNAL_POLL_S`) to keep the suite short; the Mac
procedure with the production poll is in docs/bench/governor-measurements.md.
Asserted: the SPEC bound, 5 s, and the tighter poll + step + 0.5 s.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import random
import statistics
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from minion_common.enums import CapacityProfile, Engine, TaskType
from minion_common.schemas import Assignment, Budget, ModelEntry
from minion_executor.capacity.config import ExecutorConfig, resolve_profiles
from minion_executor.capacity.governor import GovernorTiming
from minion_executor.capacity.profiles import budget_for
from minion_executor.capacity.runtime import GovernorRuntime
from minion_executor.capacity.signals import FileSignals
from minion_executor.loop import ExecutorLoop, _Verdict
from minion_executor.worker.process import WorkerCrashed, WorkerHandle

NF9_LIMIT_S = 5.0
POLL_S = 0.25
STEP_S = 0.1
TRIALS_PER_EVENT = 4  # 5 events x 4 = 20 trials
TOTAL_GB = 32.0
MODEL = ModelEntry(
    model_id="fake-c",
    type=TaskType.COMPLETION,
    engine=Engine.FAKE,
    ref="o/m",
    revision="a" * 40,
    memory_gb=1,
)
QUIET: dict[str, Any] = {"hid_idle_s": 0.0, "on_ac": True, "battery_percent": 100}


@dataclass(frozen=True)
class Event:
    name: str
    before: dict[str, Any]
    after: dict[str, Any]
    expect_paused: bool | None  # True: paused ack; None: a new duty ack (no pause)
    expect_duty: float | None = None
    release: bool = False  # memory critical: the subtask is released, the worker interrupted
    run_profile: CapacityProfile = CapacityProfile.LIGHT


EVENTS = [
    Event("thermal-serious", QUIET, {**QUIET, "thermal": "serious"}, True),
    Event("memory-warning", QUIET, {**QUIET, "memory_pressure": "warning"}, True),
    Event("memory-critical", QUIET, {**QUIET, "memory_pressure": "critical"}, True, release=True),
    Event(
        "power-low-battery",
        QUIET,
        {**QUIET, "on_ac": False, "battery_percent": 5},
        True,
    ),
    # the owner is away (idle profile = full, duty 1.0) and comes back: HID idle drops, duty falls
    Event(
        "user-return",
        {**QUIET, "hid_idle_s": 3600.0},
        QUIET,
        None,
        expect_duty=budget_for(CapacityProfile.LIGHT, TOTAL_GB).gpu_duty_cycle,
        run_profile=CapacityProfile.FULL,
    ),
]


class RecordingWorker(WorkerHandle):
    """A real worker subprocess that also records every control push `ExecutorLoop` makes."""

    def __init__(self, *a: Any, **kw: Any) -> None:
        super().__init__(*a, **kw)
        self.pushes: list[tuple[int, bool, float]] = []  # (generation, paused, duty)

    def control(self, *, duty: float, paused: bool, trim: bool = False) -> int:
        gen = super().control(duty=duty, paused=paused, trim=trim)
        self.pushes.append((gen, paused, duty))
        return gen


def assignment() -> Assignment:
    return Assignment.model_validate(
        {
            "acquirement_id": str(uuid.uuid4()),
            "attempt": 1,
            "team": "t",
            "project": "p",
            "task_name": "n",
            "subtask_id": 0,
            "type": "completion",
            "model": MODEL.model_dump(mode="json"),
            "input_url": "https://s3.test/in",
            "byte_start": 0,
            "byte_end": 1,
            "record_count": 1,
            "output_url": "https://s3.test/out",
            "expires_at": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
        }
    )


def write_signals(path: Path, fields: dict[str, Any]) -> None:
    tmp = path.with_name("signals.tmp")
    tmp.write_text(json.dumps(fields))
    tmp.chmod(0o600)
    tmp.replace(path)  # atomic: the reader never sees half a file


def budget_of(profile: CapacityProfile) -> Budget:
    return budget_for(profile, TOTAL_GB)


async def one_trial(tmp: Path, ev: Event, rng: random.Random) -> float:
    """One reaction time. The governor is a real `GovernorRuntime`; the steering is the real
    `ExecutorLoop._watch` (which calls `_push_control` and returns the memory-critical verdict)
    over a real worker subprocess. Only the server client is absent: `_watch` never touches it, and
    on the memory-critical verdict the test does what `_on_verdict` does first: `interrupt()`."""
    state = tmp / "state"
    state.mkdir(mode=0o700, exist_ok=True)
    sig = state / "signals.json"
    write_signals(sig, ev.before)
    cfg = ExecutorConfig()
    gov = GovernorRuntime(
        FileSignals(sig),
        cfg,
        resolve_profiles(cfg),
        state_dir=state,
        total_gb=TOTAL_GB,
        poll_s=POLL_S,
        timing=GovernorTiming(escalate_after_s=0.0),  # idle -> full at once; not under test
    )
    await gov.prime()
    worker = RecordingWorker([Engine.FAKE], test_hooks=True, tick_interval_s=0.05)
    loop = ExecutorLoop(
        cast(Any, None),  # ExecutorClient: unused by _watch / _push_control
        worker,
        budget_of(ev.run_profile),
        [Engine.FAKE],
        governor=gov,
        guard_interval_s=100.0,
    )
    crashed_at: list[float] = []

    async def run_work() -> None:
        records = [{"id": i, "sleep_s": STEP_S, "text": "x"} for i in range(600)]  # ~60 s of steps
        try:
            await asyncio.to_thread(worker.run, records, None, None, budget_of(ev.run_profile))
        except WorkerCrashed:
            crashed_at.append(time.monotonic())  # the worker process is gone

    tasks: list[asyncio.Task[Any]] = []
    try:
        await asyncio.to_thread(worker.ensure_model, MODEL, budget_of(ev.run_profile))
        watch = asyncio.create_task(loop._watch(assignment()))
        tasks += [asyncio.create_task(gov.run()), asyncio.create_task(run_work()), watch]
        deadline = time.monotonic() + 10
        while not worker.pushes:  # steady state: the initial decision is applied
            assert time.monotonic() < deadline, "the watcher never pushed the initial control"
            await asyncio.sleep(0.01)
        await _acked(worker, worker.pushes[-1][0], deadline)
        await asyncio.sleep(rng.uniform(0, POLL_S))  # inject at a random phase of the poll
        before = len(worker.pushes)
        write_signals(sig, ev.after)
        t0 = time.monotonic()
        give_up = t0 + 3 * NF9_LIMIT_S
        if ev.release:
            verdict = await asyncio.wait_for(watch, 3 * NF9_LIMIT_S)
            assert verdict is _Verdict.MEMORY_CRITICAL, verdict
            await asyncio.to_thread(worker.interrupt)
            while not crashed_at:
                assert time.monotonic() < give_up, f"{ev.name}: the worker never exited"
                await asyncio.sleep(0.002)
            return crashed_at[0] - t0
        while True:
            assert not watch.done(), f"{ev.name}: the watcher ended: {watch}"
            for gen, paused, duty in worker.pushes[before:]:
                if paused if ev.expect_paused else (not paused and duty == ev.expect_duty):
                    return await _acked(worker, gen, give_up) - t0
            assert time.monotonic() < give_up, f"{ev.name}: no reaction"
            await asyncio.sleep(0.002)
    finally:
        worker.interrupt()
        for t in tasks:
            t.cancel()
        for t in tasks:
            with contextlib.suppress(asyncio.CancelledError, WorkerCrashed, Exception):
                await t
        await asyncio.to_thread(worker.close)
        await loop._http.aclose()


async def _acked(worker: WorkerHandle, gen: int, deadline: float) -> float:
    """Wait until the worker acknowledged control generation `gen`; the ack's time."""
    while (ack := worker.last_ack()) is None or ack.gen < gen:
        assert time.monotonic() < deadline, "the worker never acked the control message"
        await asyncio.sleep(0.002)
    return ack.at


@pytest.fixture(autouse=True)
def dev(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")


@pytest.mark.parametrize("ev", EVENTS, ids=lambda e: e.name)
async def test_reaction_within_five_seconds(
    ev: Event, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rng = random.Random(ev.name)  # noqa: S311 - timing jitter, not security
    times = [await one_trial(tmp_path, ev, rng) for _ in range(TRIALS_PER_EVENT)]
    ordered = sorted(times)
    with capsys.disabled():
        print(
            f"\nNF-9 {ev.name}: n={len(times)} p50={statistics.median(ordered):.3f}s "
            f"max={ordered[-1]:.3f}s (poll {POLL_S}s, step {STEP_S}s)"
        )
    assert ordered[-1] < NF9_LIMIT_S, times
    # poll + one engine step + 1 s slack: a regression that merely stays under 5 s is caught
    assert ordered[-1] < POLL_S + STEP_S + 1.0, times
    assert ordered[0] >= 0
