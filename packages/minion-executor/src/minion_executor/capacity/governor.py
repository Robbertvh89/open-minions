"""Pure capacity governor (SPEC FR-52/53, ADR-0006 D5): signals + config + control -> decision.

No I/O and no clock: `now` (UTC) and `local_now` (wall clock, for schedules) are passed in, and
all memory of the past lives in the immutable `GovState` that `decide` returns. The runtime that
polls signals and applies the result is M4.5.

Rules:
- Profile (FR-52). Owner `profile <name>` > `run --profile` > schedule (first match) > idle on AC
  (`idle_profile`) > `active_profile`. Owner/pinned choices apply at once. Automatic changes
  compare the resolved budgets per dimension (memory, GPU duty, batch size, CPU priority): a
  target that is no higher in every dimension applies at once; one that is no lower in every
  dimension, or mixed, is held for `escalate_after_s` (60 s). While held, `Decision.budget` is the
  per-dimension minimum of the current and target budgets, so a dimension that goes down is lowered
  at once and one that goes up waits (invariant 6); `Decision.profile` stays the current profile
  name. After the hold the full target applies.
- Pause (FR-53). Owner pause and the signal conditions pause at once. A signal pause lifts only
  after the conditions were clear for `resume_after_s` (10 s), so a short blip cannot make the
  60 s pause-release fire. An owner `resume` lifts the owner pause at once. Reason priority for the
  single reported reason: user_paused > control_invalid > memory_pressure > thermal >
  low_power_mode > low_battery > on_battery > signals_unavailable.
- Unusable owner control (present but invalid/unsafe, see `ControlUnusable`) fails closed: paused
  with `control_invalid` and automatic profile selection, at once, until the file is fixed.
- Memory pressure `warning` -> pause + trim; `critical` -> pause + `release_now` (the in-flight
  subtask is released at once). Paused for `pause_release_after_s` (60 s) -> `pause_expired`.
- Time. Pass a UTC `now` (and the wall clock as `local_now`). If `now` is earlier than a stored
  timer start (clock stepped back) that timer restarts at `now`. A forward jump may skip a
  hysteresis wait; that is accepted.
- `release_now` and `pause_expired` are level-triggered: they stay true on every step while the
  condition holds, so the caller (M4.5) must act on them idempotently.
- A failed read (`snap=None`) keeps the last snapshot; `signal_failures_to_pause` (3) in a row
  pause with `signals_unavailable`. With no snapshot at all the executor fails closed at once.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, time, timedelta
from enum import StrEnum

from minion_common.enums import CapacityProfile, CpuPriority, PausedReason
from minion_common.schemas import Budget

from minion_executor.capacity.config import DAYS, ExecutorConfig, ScheduleEntry
from minion_executor.capacity.control import ControlUnusable, OwnerControl
from minion_executor.capacity.profiles import ProfileSpec, budget_for
from minion_executor.capacity.signals import MemPressure, SignalSnapshot, Thermal


class ProfileSource(StrEnum):
    """Which rule chose the target profile (FR-52 precedence)."""

    OWNER = "owner"
    PINNED = "pinned"
    SCHEDULE = "schedule"
    IDLE = "idle"
    ACTIVE = "active"


_EXPLICIT = (ProfileSource.OWNER, ProfileSource.PINNED)


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
    profile_source: ProfileSource  # rule behind the target profile (may be held back, see below)
    candidate: CapacityProfile | None  # escalation waiting out the hold; `profile` is still lower
    candidate_since: datetime | None

    @property
    def paused(self) -> bool:
        return self.paused_reason is not None


_CPU_ORDER = {CpuPriority.BACKGROUND: 0, CpuPriority.LOW: 1, CpuPriority.NORMAL: 2}


def _dims(b: Budget) -> tuple[float, float, int, int]:
    """(memory, duty, batch, cpu) of a resolved budget; a missing cpu priority counts as low."""
    return (
        b.memory_gb,
        b.gpu_duty_cycle,
        b.max_batch_size,
        _CPU_ORDER[b.cpu_priority or CpuPriority.LOW],
    )


def _applies_at_once(target: Budget, current: Budget) -> bool:
    """De-escalation test on resolved budgets: no dimension rises (see module docstring)."""
    return all(a <= b for a, b in zip(_dims(target), _dims(current), strict=True))


def _min_budget(current: Budget, target: Budget) -> Budget:
    """Per-dimension minimum, used while an escalation is held. Keeps `current.profile`."""
    cpu = min(
        current.cpu_priority or CpuPriority.LOW,
        target.cpu_priority or CpuPriority.LOW,
        key=_CPU_ORDER.__getitem__,
    )
    return Budget(
        profile=current.profile,
        memory_gb=min(current.memory_gb, target.memory_gb),
        gpu_duty_cycle=min(current.gpu_duty_cycle, target.gpu_duty_cycle),
        max_batch_size=min(current.max_batch_size, target.max_batch_size),
        cpu_priority=cpu,
    )


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
) -> tuple[CapacityProfile, ProfileSource]:
    """(profile, source). Owner and `--profile` choices bypass the escalation timer."""
    if owner is not None and owner.profile in specs:
        return owner.profile, ProfileSource.OWNER
    if pinned is not None and pinned in specs:
        return pinned, ProfileSource.PINNED
    for entry in cfg.schedule:
        if entry.profile in specs and _in_window(entry, local_now):
            return entry.profile, ProfileSource.SCHEDULE
    if snap.on_ac and snap.hid_idle_s >= cfg.idle_after_min * 60:
        return cfg.idle_profile, ProfileSource.IDLE
    return cfg.active_profile, ProfileSource.ACTIVE


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
        # an unknown percentage on battery fails closed
        if snap.battery_percent is None or snap.battery_percent < cfg.min_battery_percent:
            return PausedReason.LOW_BATTERY
    return None


def initial_state(cfg: ExecutorConfig, pinned: CapacityProfile | None = None) -> GovState:
    """Start state: the run's start profile is already applied, so escalation holds from startup."""
    return GovState(profile=pinned or cfg.active_profile)


def _secs(later: datetime, earlier: datetime) -> float:
    return (later - earlier) / timedelta(seconds=1)


def decide(
    snap: SignalSnapshot | None,
    cfg: ExecutorConfig,
    specs: dict[CapacityProfile, ProfileSpec],
    owner: OwnerControl | ControlUnusable | None,
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
    unusable = isinstance(owner, ControlUnusable)
    ctl = None if isinstance(owner, ControlUnusable) else owner

    def _not_future(ts: datetime | None) -> datetime | None:
        return now if ts is not None and ts > now else ts  # clock stepped back: restart the timer

    state = replace(
        state,
        candidate_since=_not_future(state.candidate_since),
        clear_since=_not_future(state.clear_since),
        paused_since=_not_future(state.paused_since),
    )

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
    user = ctl is not None and ctl.is_paused(now)
    reason = PausedReason.USER_PAUSED if user else sig_reason
    if unusable:  # then ctl is None, so `user` is False
        reason = PausedReason.CONTROL_INVALID
    paused_since = None if reason is None else (state.paused_since or now)
    pause_expired = paused_since is not None and _secs(now, paused_since) >= t.pause_release_after_s

    # --- profile -----------------------------------------------------------------------------
    if effective is None:  # no snapshot yet: fail closed (paused above), pick the cautious profile
        target, source = cfg.active_profile, ProfileSource.ACTIVE
        if ctl is not None and ctl.profile in specs:
            target, source = ctl.profile, ProfileSource.OWNER
        elif pinned is not None and pinned in specs:
            target, source = pinned, ProfileSource.PINNED
    else:
        target, source = _target_profile(effective, cfg, specs, ctl, pinned, local_now)

    held: Budget | None = None
    cur, cand, cand_since = state.profile, state.candidate, state.candidate_since
    if source in _EXPLICIT or cur is None or cur not in specs or target == cur:
        applied, cand, cand_since = target, None, None
    elif _applies_at_once(budget_for(target, total_gb, specs), budget_for(cur, total_gb, specs)):
        applied, cand, cand_since = target, None, None  # de-escalate at once
    else:
        if cand != target or cand_since is None:
            cand, cand_since = target, now
        if _secs(now, cand_since) >= t.escalate_after_s:
            applied, cand, cand_since = target, None, None
        else:
            applied = cur
            held = _min_budget(
                budget_for(cur, total_gb, specs), budget_for(target, total_gb, specs)
            )

    mem = effective.memory_pressure if effective is not None else MemPressure.NORMAL
    decision = Decision(
        profile=applied,
        budget=held or budget_for(applied, total_gb, specs),
        paused_reason=reason,
        release_now=mem is MemPressure.CRITICAL,
        pause_expired=pause_expired,
        trim=mem is not MemPressure.NORMAL,
        profile_source=source,
        candidate=cand,
        candidate_since=cand_since,
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
