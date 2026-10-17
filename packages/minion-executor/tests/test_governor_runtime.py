"""GovernorRuntime: polling, fail-closed start, change notification, log-once, seeded state."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from minion_common.enums import CapacityProfile as P
from minion_common.enums import PausedReason as R
from minion_executor.capacity.config import ExecutorConfig, resolve_profiles
from minion_executor.capacity.control import CONTROL_FILE, OwnerControl, write_control
from minion_executor.capacity.profiles import budget_for
from minion_executor.capacity.runtime import FixedGovernor, GovernorRuntime
from minion_executor.capacity.signals import (
    MemPressure,
    SignalError,
    SignalSnapshot,
    Thermal,
)
from structlog.testing import capture_logs

TOTAL = 32.0
T0 = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)


class Source:
    def __init__(self) -> None:
        self.reads = 0
        self.fail: str | None = None
        self.fields: dict[str, object] = {}

    def read(self) -> SignalSnapshot:
        self.reads += 1
        if self.fail:
            raise SignalError(self.fail)
        base: dict[str, object] = {
            "hid_idle_s": 0.0,
            "on_ac": True,
            "battery_percent": None,
            "low_power_mode": False,
            "thermal": Thermal.NOMINAL,
            "memory_pressure": MemPressure.NORMAL,
            "taken_at": T0,
        }
        base.update(self.fields)
        return SignalSnapshot(**base)  # type: ignore[arg-type]


class Clock:
    def __init__(self) -> None:
        self.now = T0

    def __call__(self) -> datetime:
        return self.now

    def advance(self, s: float) -> None:
        self.now += timedelta(seconds=s)


@pytest.fixture
def state_dir(tmp_path: Path) -> Path:
    d = tmp_path / "state"
    d.mkdir(mode=0o700)
    return d


def make(
    state_dir: Path,
    source: Source,
    clock: Clock,
    pinned: P | None = None,
    **kw: object,
) -> GovernorRuntime:
    cfg = ExecutorConfig()
    return GovernorRuntime(
        source,
        cfg,
        resolve_profiles(cfg),
        state_dir=state_dir,
        total_gb=TOTAL,
        pinned=pinned,
        clock=clock,
        local_clock=lambda: clock().replace(tzinfo=None),
        **kw,  # type: ignore[arg-type]
    )


async def test_fails_closed_until_the_first_poll(state_dir: Path) -> None:
    gov = make(state_dir, Source(), Clock())
    d = gov.decision
    assert d.paused_reason is R.SIGNALS_UNAVAILABLE and gov.version == 0
    await gov.prime()
    assert gov.decision.paused_reason is None
    assert gov.version == 1  # the change woke waiters


async def test_runtime_starts_from_the_seeded_initial_state(state_dir: Path) -> None:
    src, clock = Source(), Clock()
    gov = make(state_dir, src, clock)
    await gov.prime()
    assert gov.decision.budget == budget_for(P.LIGHT, TOTAL)
    # the user goes idle: full is an escalation, held (60 s) at the per-dimension minimum
    src.fields = {"hid_idle_s": 11 * 60.0}
    clock.advance(2)
    d = await gov.poll_once()
    assert d.profile is P.LIGHT and d.candidate is P.FULL
    assert d.budget == budget_for(P.LIGHT, TOTAL)  # nothing rises before the hold is over
    assert gov.nominal == budget_for(P.LIGHT, TOTAL)
    clock.advance(61)
    d = await gov.poll_once()
    assert d.profile is P.FULL and d.budget == budget_for(P.FULL, TOTAL)


async def test_deescalation_uses_decision_budget_not_the_profile_budget(state_dir: Path) -> None:
    src, clock = Source(), Clock()
    gov = make(state_dir, src, clock, pinned=None)
    src.fields = {"hid_idle_s": 11 * 60.0}
    await gov.prime()
    clock.advance(61)
    await gov.poll_once()
    assert gov.decision.profile is P.FULL
    src.fields = {"hid_idle_s": 0.0}  # the user returns: down to light at once
    clock.advance(2)
    d = await gov.poll_once()
    assert d.profile is P.LIGHT and d.budget == budget_for(P.LIGHT, TOTAL)


async def test_read_failures_pause_after_three_and_recover(state_dir: Path) -> None:
    src, clock = Source(), Clock()
    gov = make(state_dir, src, clock)
    await gov.prime()
    src.fail = "boom"
    for _ in range(2):
        clock.advance(2)
        assert (await gov.poll_once()).paused_reason is None  # one or two failures keep the last
    clock.advance(2)
    assert (await gov.poll_once()).paused_reason is R.SIGNALS_UNAVAILABLE
    src.fail = None
    clock.advance(2)
    assert (await gov.poll_once()).paused_reason is R.SIGNALS_UNAVAILABLE  # held 10 s
    clock.advance(11)
    assert (await gov.poll_once()).paused_reason is None


async def test_one_source_object_for_the_whole_run(state_dir: Path) -> None:
    src, clock = Source(), Clock()
    gov = make(state_dir, src, clock)
    for _ in range(3):
        await gov.poll_once()
    assert src.reads == 3


async def test_signal_failure_is_logged_once_per_change(state_dir: Path) -> None:
    src, clock = Source(), Clock()
    gov = make(state_dir, src, clock)
    await gov.prime()
    src.fail = "framework wedged"
    with capture_logs() as logs:
        for _ in range(5):
            clock.advance(2)
            await gov.poll_once()
        src.fail = "other"
        await gov.poll_once()
    failed = [e for e in logs if e["event"] == "signal_read_failed"]
    assert [e["reason"] for e in failed] == ["framework wedged", "other"]


async def test_owner_pause_resume_and_unusable_control_logged_once(state_dir: Path) -> None:
    src, clock = Source(), Clock()
    gov = make(state_dir, src, clock)
    await gov.prime()
    write_control(state_dir, OwnerControl(paused=True))
    clock.advance(2)
    assert (await gov.poll_once()).paused_reason is R.USER_PAUSED
    (state_dir / CONTROL_FILE).write_text("{not json")
    (state_dir / CONTROL_FILE).chmod(0o600)
    with capture_logs() as logs:
        for _ in range(4):
            clock.advance(2)
            d = await gov.poll_once()
        assert d.paused_reason is R.CONTROL_INVALID
        (state_dir / CONTROL_FILE).write_text('{"paused": "yes"}')
        await gov.poll_once()
    unusable = [e for e in logs if e["event"] == "owner_control_unusable"]
    assert len(unusable) == 2  # once per distinct reason, not per poll
    write_control(state_dir, OwnerControl(paused=False))
    clock.advance(2)
    assert (await gov.poll_once()).paused_reason is None


async def test_memory_critical_sets_release_now_and_wakes_waiters(state_dir: Path) -> None:
    src, clock = Source(), Clock()
    gov = make(state_dir, src, clock)
    await gov.prime()
    v = gov.version
    waiter = asyncio.create_task(gov.wait_change(v, 5.0))
    await asyncio.sleep(0)
    src.fields = {"memory_pressure": MemPressure.CRITICAL}
    clock.advance(2)
    await gov.poll_once()
    assert await asyncio.wait_for(waiter, 1) == v + 1
    d = gov.decision
    assert d.release_now and d.trim and d.paused_reason is R.MEMORY_PRESSURE


async def test_pause_expiry_is_level_triggered(state_dir: Path) -> None:
    src, clock = Source(), Clock()
    gov = make(state_dir, src, clock)
    await gov.prime()
    src.fields = {"thermal": Thermal.SERIOUS}
    clock.advance(2)
    assert not (await gov.poll_once()).pause_expired
    clock.advance(61)
    assert (await gov.poll_once()).pause_expired
    clock.advance(2)
    assert (await gov.poll_once()).pause_expired  # stays true while the pause holds


async def test_wait_change_returns_at_once_for_a_stale_version_and_times_out_unchanged(
    state_dir: Path,
) -> None:
    gov = make(state_dir, Source(), Clock())
    await gov.prime()
    assert await gov.wait_change(0, 5.0) == gov.version  # already newer
    v = gov.version
    assert await gov.wait_change(v, 0.05) == v  # nothing changed


async def test_run_polls_until_cancelled_and_failsafe_on_a_bug(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    src, clock = Source(), Clock()
    gov = make(state_dir, src, clock, poll_s=0.01)
    task = asyncio.create_task(gov.run())
    await asyncio.sleep(0.1)
    assert src.reads >= 3 and gov.decision.paused_reason is None

    def bug(*a: object, **k: object) -> None:
        raise RuntimeError("bug")

    with monkeypatch.context() as mp:
        mp.setattr("minion_executor.capacity.runtime.decide", bug)
        await asyncio.sleep(0.1)
        assert gov.decision.paused_reason is R.SIGNALS_UNAVAILABLE  # fail closed, loop survives
    await asyncio.sleep(0.1)
    assert gov.decision.paused_reason is None
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_fixed_governor_is_static_and_update_wakes() -> None:
    from minion_common.schemas import Budget

    b = Budget.model_validate(
        {"profile": "balanced", "memory_gb": 8, "gpu_duty_cycle": 0.5, "max_batch_size": 2}
    )
    gov = FixedGovernor(b)
    assert gov.decision.budget == b and not gov.decision.paused
    v = gov.version
    waiter = asyncio.create_task(gov.wait_change(v, 5.0))
    await asyncio.sleep(0)
    gov.update(gov.decision.__class__(**{**gov.decision.__dict__, "paused_reason": R.THERMAL}))
    assert await asyncio.wait_for(waiter, 1) == v + 1


# --- fail closed: hung control read, stale decision, repeated poll errors -------------------------


async def test_hung_control_read_is_one_daemon_thread_and_fails_closed(state_dir: Path) -> None:
    import threading

    release = threading.Event()
    calls: list[int] = []

    def reader(_: Path) -> None:
        calls.append(1)
        release.wait(10)

    gov = make(state_dir, Source(), Clock(), control_timeout_s=0.05, control_reader=reader)
    try:
        d1 = await gov.poll_once()
        assert d1.paused_reason is R.CONTROL_INVALID  # timed out: unusable, never "no file"
        for _ in range(5):
            d = await gov.poll_once()
            assert d.paused_reason is R.CONTROL_INVALID
        assert calls == [1]  # no further thread while the first read hangs
        assert await asyncio.to_thread(lambda: 7) == 7  # the default pool is untouched
    finally:
        release.set()
    await asyncio.sleep(0.05)
    gov._read_control = lambda _: None  # recovered: reads work again
    assert (await gov.poll_once()).paused_reason is None


class Mono:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


async def test_stale_decision_is_paused_and_expires_to_a_release(state_dir: Path) -> None:
    mono = Mono()
    gov = make(state_dir, Source(), Clock(), mono=mono)  # stale after max(10, 3 x 2) s
    await gov.prime()
    assert gov.decision.paused_reason is None
    mono.t += 9
    assert gov.decision.paused_reason is None
    mono.t += 2  # 11 s without a poll
    d = gov.decision
    assert d.paused_reason is R.SIGNALS_UNAVAILABLE and not d.pause_expired
    mono.t += 60  # paused-stale for pause_release_after_s: the in-flight subtask is released
    assert gov.decision.pause_expired
    await gov.poll_once()  # a poll makes it fresh again
    assert gov.decision.paused_reason is None and not gov.decision.pause_expired


async def test_repeated_decide_errors_expire_the_pause(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import minion_executor.capacity.runtime as rt

    mono = Mono()
    gov = make(state_dir, Source(), Clock(), mono=mono, poll_s=0.01)

    def boom(*a: object, **k: object) -> None:
        raise RuntimeError("bug")

    monkeypatch.setattr(rt, "decide", boom)
    task = asyncio.create_task(gov.run())
    try:
        for _ in range(200):
            if gov._error_since is not None:  # the first failed poll happened
                break
            await asyncio.sleep(0.01)
        assert gov.decision.paused_reason is R.SIGNALS_UNAVAILABLE
        assert not gov.decision.pause_expired  # the first error: the pause just began
        mono.t += 61
        for _ in range(200):
            if gov.decision.pause_expired:
                break
            await asyncio.sleep(0.01)
        assert gov.decision.pause_expired  # ... and it has lasted 60 s: release in-flight work
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_recovery_from_staleness_wakes_waiters_even_with_an_unchanged_decision(
    state_dir: Path,
) -> None:
    mono = Mono()
    gov = make(state_dir, Source(), Clock(), mono=mono)
    await gov.prime()
    await gov.poll_once()
    v = gov.version
    await gov.poll_once()  # same decision, fresh: nothing to announce
    assert gov.version == v
    mono.t += 30  # the polling task stalled: `decision` reads fail-closed meanwhile
    assert gov.decision.paused_reason is R.SIGNALS_UNAVAILABLE
    await gov.poll_once()  # same decision key as before, but what `decision` returns changed
    assert gov.version == v + 1 and gov.decision.paused_reason is None


@pytest.mark.parametrize(
    ("on_call", "wanted"), [("auto", True), ("light", True), ("pause", True), ("ignore", False)]
)
async def test_runtime_tells_the_source_whether_call_state_is_wanted(
    state_dir: Path, on_call: str, wanted: bool
) -> None:
    class WantSource(Source):
        want: bool | None = None

        def set_want_call_state(self, want: bool) -> None:
            self.want = want

    src, clock = WantSource(), Clock()
    cfg = ExecutorConfig(on_call=on_call)
    gov = GovernorRuntime(
        src,
        cfg,
        resolve_profiles(cfg),
        state_dir=state_dir,
        total_gb=TOTAL,
        clock=clock,
        local_clock=lambda: clock().replace(tzinfo=None),
    )
    await gov.poll_once()
    assert src.want is wanted
