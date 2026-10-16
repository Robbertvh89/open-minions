"""Pure capacity governor (SPEC FR-52/53, ADR-0006 D5): signals + config + control -> decision.

No I/O and no clock: `now` (UTC) and `local_now` (wall clock, for schedules) are passed in, and
all memory of the past lives in the immutable `GovState` that `decide` returns. The runtime that
polls signals and applies the result is M4.5.

Rules:
- Profile (FR-52). Owner `profile <name>` > `run --profile` > schedule (first match) > idle on AC
  (`idle_profile`) > `active_profile`. Owner/pinned choices apply at once. Automatic changes
  de-escalate at once and escalate only after the target held for `escalate_after_s` (60 s).
- Pause (FR-53). Owner pause and the signal conditions pause at once. A signal pause lifts only
  after the conditions were clear for `resume_after_s` (10 s), so a short blip cannot make the
  60 s pause-release fire. An owner `resume` lifts the owner pause at once. Reason priority for the
  single reported reason: user_paused > memory_pressure > thermal > low_power_mode > low_battery >
  on_battery > signals_unavailable.
- Memory pressure `warning` -> pause + trim; `critical` -> pause + `release_now` (the in-flight
  subtask is released at once). Paused for `pause_release_after_s` (60 s) -> `pause_expired`.
- A failed read (`snap=None`) keeps the last snapshot; `signal_failures_to_pause` (3) in a row
  pause with `signals_unavailable`. With no snapshot at all the executor fails closed at once.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, time, timedelta

from minion_common.enums import CapacityProfile, PausedReason
from minion_common.schemas import Budget

from minion_executor.capacity.config import DAYS, ExecutorConfig, ScheduleEntry
from minion_executor.capacity.control import OwnerControl
from minion_executor.capacity.profiles import ProfileSpec, budget_for
from minion_executor.capacity.signals import MemPressure, SignalSnapshot, Thermal


@dataclass(frozen=True)
class GovernorTiming:
    escalate_after_s: float = 60.0
    resume_after_s: float = 10.0
    pause_release_after_s: float = 60.0
    signal_failures_to_pause: int = 3


@dataclass(frozen=True)
class GovState:
    profile: CapacityProfile | None = None  # profile currently applied
    candidate: CapacityProfile | None = None  # higher profile waiting out the escalation timer
    candidate_since: datetime | None = None
    signal_reason: PausedReason | None = None  # held signal pause (hysteresis)
    clear_since: datetime | None = None
    paused_since: datetime | None = None
    failures: int = 0
    last_snapshot: SignalSnapshot | None = None


@dataclass(frozen=True)
class Decision:
    profile: CapacityProfile
    budget: Budget
    paused_reason: PausedReason | None
    release_now: bool  # memory pressure critical: interrupt and release the in-flight subtask
    pause_expired: bool  # paused for >= pause_release_after_s: release the in-flight subtask
    trim: bool  # free allocator caches (memory pressure warning or worse)

    @property
    def paused(self) -> bool:
        return self.paused_reason is not None


def _rank(spec: ProfileSpec) -> tuple[float, int]:
    return (spec.gpu_duty_cycle, spec.max_batch_size)


def _in_window(entry: ScheduleEntry, local_now: datetime) -> bool:
    t: time = local_now.time().replace(second=0, microsecond=0, tzinfo=None)
    today = DAYS[local_now.weekday()]
    yesterday = DAYS[(local_now.weekday() - 1) % 7]
    start, end = entry.start_time, entry.end_time

    def day_ok(d: str) -> bool:
        return entry.days is None or d in entry.days

    if start < end:
        return start <= t < end and day_ok(today)
    # overnight window: `days` names the day it starts on
    return (t >= start and day_ok(today)) or (t < end and day_ok(yesterday))


def _target_profile(
    snap: SignalSnapshot,
    cfg: ExecutorConfig,
    specs: dict[CapacityProfile, ProfileSpec],
    owner: OwnerControl | None,
    pinned: CapacityProfile | None,
    local_now: datetime,
) -> tuple[CapacityProfile, bool]:
    """(profile, explicit). Explicit choices (owner, `--profile`) bypass the escalation timer."""
    if owner is not None and owner.profile in specs:
        return owner.profile, True
    if pinned is not None and pinned in specs:
        return pinned, True
    for entry in cfg.schedule:
        if entry.profile in specs and _in_window(entry, local_now):
            return entry.profile, False
    if snap.on_ac and snap.hid_idle_s >= cfg.idle_after_min * 60:
        return cfg.idle_profile, False
    return cfg.active_profile, False


def _signal_reason(
    snap: SignalSnapshot | None, cfg: ExecutorConfig, failures: int, timing: GovernorTiming
) -> PausedReason | None:
    """Highest-priority signal condition that currently demands a pause."""
    if snap is None or failures >= timing.signal_failures_to_pause:
        return PausedReason.SIGNALS_UNAVAILABLE
    if snap.memory_pressure is not MemPressure.NORMAL:
        return PausedReason.MEMORY_PRESSURE
    if snap.thermal in (Thermal.SERIOUS, Thermal.CRITICAL):
        return PausedReason.THERMAL
    if snap.low_power_mode:
        return PausedReason.LOW_POWER_MODE
    if not snap.on_ac:
        if not cfg.allow_on_battery:
            return PausedReason.ON_BATTERY
        if snap.battery_percent is not None and snap.battery_percent < cfg.min_battery_percent:
            return PausedReason.LOW_BATTERY
    return None


def _secs(later: datetime, earlier: datetime) -> float:
    return (later - earlier) / timedelta(seconds=1)


def decide(
    snap: SignalSnapshot | None,
    cfg: ExecutorConfig,
    specs: dict[CapacityProfile, ProfileSpec],
    owner: OwnerControl | None,
    now: datetime,
    local_now: datetime,
    state: GovState,
    *,
    total_gb: float,
    pinned: CapacityProfile | None = None,
    timing: GovernorTiming | None = None,
) -> tuple[Decision, GovState]:
    """One governor step. `snap=None` is a failed signal read. Pure: same inputs, same outputs."""
    t = timing or GovernorTiming()

    failures = 0 if snap is not None else state.failures + 1
    effective = snap if snap is not None else state.last_snapshot
    last = snap if snap is not None else state.last_snapshot

    # --- pause -------------------------------------------------------------------------------
    raw = _signal_reason(effective, cfg, failures, t)
    sig_reason, clear_since = state.signal_reason, state.clear_since
    if raw is not None:
        sig_reason, clear_since = raw, None
    elif sig_reason is not None:
        clear_since = clear_since or now
        if _secs(now, clear_since) >= t.resume_after_s:
            sig_reason, clear_since = None, None
    else:
        clear_since = None
    user = owner is not None and owner.is_paused(now)
    reason = PausedReason.USER_PAUSED if user else sig_reason
    paused_since = None if reason is None else (state.paused_since or now)
    pause_expired = paused_since is not None and _secs(now, paused_since) >= t.pause_release_after_s

    # --- profile -----------------------------------------------------------------------------
    if effective is None:  # no snapshot yet: fail closed (paused above), pick the cautious profile
        target, explicit = cfg.active_profile, False
        if owner is not None and owner.profile in specs:
            target, explicit = owner.profile, True
        elif pinned is not None and pinned in specs:
            target, explicit = pinned, True
    else:
        target, explicit = _target_profile(effective, cfg, specs, owner, pinned, local_now)

    cur, cand, cand_since = state.profile, state.candidate, state.candidate_since
    if explicit or cur is None or cur not in specs or target == cur:
        applied, cand, cand_since = target, None, None
    elif _rank(specs[target]) < _rank(specs[cur]):
        applied, cand, cand_since = target, None, None  # de-escalate at once
    else:
        if cand != target or cand_since is None:
            cand, cand_since = target, now
        if _secs(now, cand_since) >= t.escalate_after_s:
            applied, cand, cand_since = target, None, None
        else:
            applied = cur

    mem = effective.memory_pressure if effective is not None else MemPressure.NORMAL
    decision = Decision(
        profile=applied,
        budget=budget_for(applied, total_gb, specs),
        paused_reason=reason,
        release_now=mem is MemPressure.CRITICAL,
        pause_expired=pause_expired,
        trim=mem is not MemPressure.NORMAL,
    )
    new_state = replace(
        state,
        profile=applied,
        candidate=cand,
        candidate_since=cand_since,
        signal_reason=sig_reason,
        clear_since=clear_since,
        paused_since=paused_since,
        failures=failures,
        last_snapshot=last,
    )
    return decision, new_state
