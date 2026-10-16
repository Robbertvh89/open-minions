"""NF-9 on CI (TASK-5.6): the governor reacts to memory pressure, thermal stress, power and user
return within 5 s. 20 trials in total with the fake engine in a real worker subprocess and
`FileSignals` steered through a file; no server, no GPU.

Reaction time = from the moment the signal file holds the new value to the moment the worker has
acknowledged the matching control message (paused, or the new duty) or, for memory `critical`,
has been interrupted (the release path). The bridge below mirrors the loop's `_push_control` and
release handling (`loop.py`), so what is timed is poll + decide + control round trip + one 100 ms
engine step. The poll is 0.25 s here (production 2 s, bounded by `MINION_EXECUTOR_SIGNAL_POLL_S`)
to keep the suite short; the Mac procedure with the production poll is in
docs/bench/governor-measurements.md. The bound asserted is the SPEC one, 5 s.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import random
import statistics
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from minion_common.enums import CapacityProfile, Engine, TaskType
from minion_common.schemas import Budget, ModelEntry
from minion_executor.capacity.config import ExecutorConfig, resolve_profiles
from minion_executor.capacity.governor import GovernorTiming
from minion_executor.capacity.profiles import budget_for
from minion_executor.capacity.runtime import GovernorRuntime
from minion_executor.capacity.signals import FileSignals
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


@dataclass
class Bridge:
    """Pushes governor decisions to the worker like `ExecutorLoop._push_control`, and records
    (control-side ack time, paused, duty) per push. A critical memory verdict interrupts."""

    gov: GovernorRuntime
    worker: WorkerHandle
    acks: list[tuple[float, bool, float]] = field(default_factory=list)
    released_at: float | None = None

    async def run(self) -> None:
        version, key = self.gov.version, None
        while True:
            d = self.gov.decision
            if d.release_now:
                self.worker.interrupt()
                self.released_at = time.monotonic()
                return
            k = (d.budget.gpu_duty_cycle, d.paused)
            if k != key:
                key = k
                gen = await asyncio.to_thread(self.worker.control, duty=k[0], paused=k[1])
                ack = None
                while ack is None or ack.gen < gen:
                    await asyncio.sleep(0.002)
                    ack = self.worker.last_ack()
                self.acks.append((ack.at, k[1], k[0]))
            version = await self.gov.wait_change(version, 0.5)


def write_signals(path: Path, fields: dict[str, Any]) -> None:
    tmp = path.with_name("signals.tmp")
    tmp.write_text(json.dumps(fields))
    tmp.chmod(0o600)
    tmp.replace(path)  # atomic: the reader never sees half a file


def budget_of(profile: CapacityProfile) -> Budget:
    return budget_for(profile, TOTAL_GB)


async def one_trial(tmp: Path, ev: Event, rng: random.Random) -> float:
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
    worker = WorkerHandle([Engine.FAKE], test_hooks=True, tick_interval_s=0.05)
    bridge = Bridge(gov, worker)
    tasks: list[asyncio.Task[Any]] = []
    try:
        await asyncio.to_thread(worker.ensure_model, MODEL, budget_of(ev.run_profile))
        records = [{"id": i, "sleep_s": STEP_S, "text": "x"} for i in range(600)]  # ~60 s of steps
        work = asyncio.create_task(
            asyncio.to_thread(worker.run, records, None, None, budget_of(ev.run_profile))
        )
        tasks += [work, asyncio.create_task(gov.run()), asyncio.create_task(bridge.run())]
        deadline = time.monotonic() + 10
        while not bridge.acks:  # steady state: the initial decision is applied
            assert time.monotonic() < deadline, "worker never acked the initial control"
            await asyncio.sleep(0.01)
        await asyncio.sleep(rng.uniform(0, POLL_S))  # inject at a random phase of the poll
        before = len(bridge.acks)
        write_signals(sig, ev.after)
        t0 = time.monotonic()
        while True:
            if ev.release:
                if bridge.released_at is not None:
                    return bridge.released_at - t0
            else:
                for at, paused, duty in bridge.acks[before:]:
                    if paused if ev.expect_paused else (not paused and duty == ev.expect_duty):
                        return at - t0
            assert time.monotonic() - t0 < 3 * NF9_LIMIT_S, f"{ev.name}: no reaction"
            await asyncio.sleep(0.002)
    finally:
        worker.interrupt()
        for t in tasks:
            t.cancel()
        for t in tasks:
            with contextlib.suppress(asyncio.CancelledError, WorkerCrashed, Exception):
                await t
        await asyncio.to_thread(worker.close)


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
    assert ordered[0] >= 0
