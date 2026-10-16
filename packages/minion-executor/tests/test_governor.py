"""Pure governor: table-driven transitions with an explicit clock (no I/O, no sleeping)."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from minion_common.enums import CapacityProfile as P
from minion_common.enums import PausedReason as R
from minion_executor.capacity.config import ExecutorConfig, resolve_profiles
from minion_executor.capacity.control import ControlUnusable, OwnerControl
from minion_executor.capacity.governor import (
    Decision,
    GovernorTiming,
    GovState,
    decide,
    initial_state,
)
from minion_executor.capacity.signals import MemPressure, SignalSnapshot, Thermal

T0 = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)  # a Wednesday
TOTAL = 32.0


def snap(**kw: object) -> SignalSnapshot:
    base: dict[str, object] = dict(
        hid_idle_s=0.0,
        on_ac=True,
        battery_percent=None,
        low_power_mode=False,
        thermal=Thermal.NOMINAL,
        memory_pressure=MemPressure.NORMAL,
        taken_at=T0,
    )
    base.update(kw)
    return SignalSnapshot(**base)  # type: ignore[arg-type]


IDLE = 11 * 60.0  # past the 10 min default


class Gov:
    """Drives `decide` step by step with a fake clock."""

    def __init__(
        self, cfg: ExecutorConfig | None = None, start: bool = False, **kw: object
    ) -> None:
        self.cfg = cfg or ExecutorConfig()
        self.specs = resolve_profiles(self.cfg)
        pin = kw.get("pinned")
        assert pin is None or isinstance(pin, P)
        self.state = initial_state(self.cfg, pin) if start else GovState()
        self.kw = kw
        self.t = T0
        self.d: Decision

    def step(
        self,
        s: SignalSnapshot | None,
        dt: float = 0.0,
        owner: OwnerControl | ControlUnusable | None = None,
        local: datetime | None = None,
    ) -> Decision:
        self.t += timedelta(seconds=dt)
        self.d, self.state = decide(
            s,
            self.cfg,
            self.specs,
            owner,
            self.t,
            local or self.t.replace(tzinfo=None),
            self.state,
            total_gb=TOTAL,
            **self.kw,  # type: ignore[arg-type]
        )
        return self.d


# --- profile selection (FR-52) ----------------------------------------------------------------


def test_first_decision_is_active_profile_and_budget_follows() -> None:
    g = Gov()
    d = g.step(snap())
    assert d.profile is P.LIGHT and d.paused_reason is None
    assert d.budget.profile is P.LIGHT and d.budget.gpu_duty_cycle == 0.30
    assert d.budget.memory_gb == pytest.approx(TOTAL * 0.25)


def test_idle_on_ac_escalates_only_after_60s() -> None:
    g = Gov()
    g.step(snap())
    assert g.step(snap(hid_idle_s=IDLE), 1).profile is P.LIGHT  # candidate starts
    assert g.step(snap(hid_idle_s=IDLE), 58).profile is P.LIGHT  # held 58 s
    assert g.step(snap(hid_idle_s=IDLE), 1).profile is P.LIGHT  # held 59 s
    assert g.step(snap(hid_idle_s=IDLE), 1).profile is P.FULL  # held 60 s
    assert g.state.candidate is None


def test_escalation_timer_resets_if_target_flaps() -> None:
    g = Gov()
    g.step(snap())
    g.step(snap(hid_idle_s=IDLE), 1)
    g.step(snap(hid_idle_s=IDLE), 50)
    assert g.step(snap(hid_idle_s=0), 1).profile is P.LIGHT  # user back: candidate dropped
    g.step(snap(hid_idle_s=IDLE), 1)
    assert g.step(snap(hid_idle_s=IDLE), 30).profile is P.LIGHT  # timer restarted


def test_user_return_deescalates_at_once() -> None:
    g = Gov(pinned=None)
    g.step(snap(hid_idle_s=IDLE))
    g.step(snap(hid_idle_s=IDLE), 61)
    assert g.d.profile is P.FULL
    assert g.step(snap(hid_idle_s=1.0), 0.5).profile is P.LIGHT


def test_idle_profile_not_used_on_battery() -> None:
    cfg = ExecutorConfig(allow_on_battery=True, min_battery_percent=10)
    g = Gov(cfg)
    g.step(snap(on_ac=False, battery_percent=80, hid_idle_s=IDLE))
    d = g.step(snap(on_ac=False, battery_percent=80, hid_idle_s=IDLE), 120)
    assert d.profile is P.LIGHT and d.paused_reason is None


def test_custom_idle_threshold_and_profiles() -> None:
    cfg = ExecutorConfig(active_profile=P.BALANCED, idle_profile=P.BALANCED, idle_after_min=1)
    g = Gov(cfg)
    assert g.step(snap(hid_idle_s=59)).profile is P.BALANCED


def test_pinned_profile_applies_at_once_and_ignores_idle() -> None:
    g = Gov(pinned=P.FULL)
    assert g.step(snap()).profile is P.FULL
    assert g.step(snap(hid_idle_s=0), 100).profile is P.FULL


def test_owner_profile_overrides_pin_and_auto_returns() -> None:
    g = Gov(pinned=P.BALANCED)
    assert g.step(snap()).profile is P.BALANCED
    assert g.step(snap(), 1, owner=OwnerControl(profile=P.FULL)).profile is P.FULL
    # back to auto: falls back to the pin, which is explicit
    assert g.step(snap(), 1, owner=OwnerControl(profile=None)).profile is P.BALANCED
    g2 = Gov()
    g2.step(snap(), owner=OwnerControl(profile=P.FULL))
    assert g2.step(snap(), 1, owner=OwnerControl()).profile is P.LIGHT  # de-escalation is instant


def test_owner_profile_undefined_custom_is_ignored() -> None:
    g = Gov()
    assert g.step(snap(), owner=OwnerControl(profile=P.CUSTOM)).profile is P.LIGHT


SCHED_CFG = ExecutorConfig.model_validate(
    {
        "schedule": [
            {"profile": "full", "start": "19:00", "end": "07:00"},
            {"profile": "balanced", "start": "09:00", "end": "17:00", "days": ["mon", "tue"]},
        ]
    }
)


@pytest.mark.parametrize(
    ("local", "expected"),
    [
        (datetime(2026, 10, 14, 20, 0), P.FULL),  # Wed evening
        (datetime(2026, 10, 14, 6, 59), P.FULL),  # Wed early morning (overnight window)
        (datetime(2026, 10, 14, 7, 0), P.LIGHT),  # end is exclusive
        (datetime(2026, 10, 14, 12, 0), P.LIGHT),  # Wed: second entry is mon/tue only
        (datetime(2026, 10, 13, 12, 0), P.BALANCED),  # Tue
        (datetime(2026, 10, 13, 17, 0), P.LIGHT),  # end exclusive
        (datetime(2026, 10, 13, 18, 59), P.LIGHT),
        (datetime(2026, 10, 13, 19, 0), P.FULL),
    ],
)
def test_schedule_windows(local: datetime, expected: P) -> None:
    g = Gov(SCHED_CFG)
    assert g.step(snap(), local=local).profile is expected


def test_schedule_overrides_idle_and_overnight_days_use_start_day() -> None:
    cfg = ExecutorConfig.model_validate(
        {"schedule": [{"profile": "full", "start": "22:00", "end": "06:00", "days": ["fri"]}]}
    )
    g = Gov(cfg)
    sat_early = datetime(2026, 10, 17, 3, 0)  # Saturday 03:00 belongs to Friday's window
    assert g.step(snap(), local=sat_early).profile is P.FULL
    g = Gov(cfg)
    fri_early = datetime(2026, 10, 16, 3, 0)  # Friday 03:00 belongs to Thursday's window
    assert g.step(snap(hid_idle_s=0), local=fri_early).profile is P.LIGHT
    g = Gov(SCHED_CFG)  # schedule beats "user active"
    assert g.step(snap(hid_idle_s=0), local=datetime(2026, 10, 14, 23, 0)).profile is P.FULL


def test_schedule_end_of_window_deescalates_at_once() -> None:
    g = Gov(SCHED_CFG)
    assert g.step(snap(), local=datetime(2026, 10, 14, 6, 59)).profile is P.FULL
    assert g.step(snap(), 60, local=datetime(2026, 10, 14, 7, 0)).profile is P.LIGHT


# --- pause rules (FR-53) ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("kw", "cfg_kw", "reason"),
    [
        ({"memory_pressure": MemPressure.WARNING}, {}, R.MEMORY_PRESSURE),
        ({"memory_pressure": MemPressure.CRITICAL}, {}, R.MEMORY_PRESSURE),
        ({"thermal": Thermal.SERIOUS}, {}, R.THERMAL),
        ({"thermal": Thermal.CRITICAL}, {}, R.THERMAL),
        ({"low_power_mode": True}, {}, R.LOW_POWER_MODE),
        ({"on_ac": False, "battery_percent": 90}, {}, R.ON_BATTERY),
        ({"on_ac": False, "battery_percent": 29}, {"allow_on_battery": True}, R.LOW_BATTERY),
        ({"on_ac": False}, {"allow_on_battery": True}, R.LOW_BATTERY),  # unknown %: closed
        ({"on_ac": False, "battery_percent": 30}, {"allow_on_battery": True}, None),
        ({"on_ac": True, "battery_percent": 5}, {}, None),  # charging
        ({"thermal": Thermal.FAIR}, {}, None),
        ({}, {}, None),
    ],
)
def test_pause_reasons(kw: dict[str, object], cfg_kw: dict[str, object], reason: R | None) -> None:
    g = Gov(ExecutorConfig(**cfg_kw))
    d = g.step(snap(**kw))
    assert d.paused_reason is reason and d.paused is (reason is not None)


def test_reason_priority() -> None:
    everything = snap(
        on_ac=False,
        battery_percent=1,
        low_power_mode=True,
        thermal=Thermal.CRITICAL,
        memory_pressure=MemPressure.WARNING,
    )
    cfg = ExecutorConfig(allow_on_battery=True)
    g = Gov(cfg)
    assert g.step(everything).paused_reason is R.MEMORY_PRESSURE
    g = Gov(cfg)
    assert (
        g.step(replace(everything, memory_pressure=MemPressure.NORMAL)).paused_reason is R.THERMAL
    )
    g = Gov(cfg)
    s = replace(everything, memory_pressure=MemPressure.NORMAL, thermal=Thermal.NOMINAL)
    assert g.step(s).paused_reason is R.LOW_POWER_MODE
    assert g.step(replace(s, low_power_mode=False), 20).paused_reason is R.LOW_BATTERY
    g = Gov(ExecutorConfig())
    assert g.step(replace(s, low_power_mode=False)).paused_reason is R.ON_BATTERY
    owner = OwnerControl(paused=True)
    assert g.step(everything, 1, owner=owner).paused_reason is R.USER_PAUSED


def test_memory_warning_trims_and_critical_releases() -> None:
    g = Gov()
    d = g.step(snap(memory_pressure=MemPressure.WARNING))
    assert d.trim and not d.release_now and d.paused
    d = g.step(snap(memory_pressure=MemPressure.CRITICAL), 1)
    assert d.trim and d.release_now and d.paused
    d = g.step(snap(), 1)  # pressure gone: flags drop at once, pause is held by hysteresis
    assert not d.trim and not d.release_now and d.paused_reason is R.MEMORY_PRESSURE


def test_signal_pause_needs_10s_clear_to_resume() -> None:
    g = Gov()
    assert g.step(snap(thermal=Thermal.SERIOUS)).paused
    assert g.step(snap(), 1).paused_reason is R.THERMAL  # clear starts
    assert g.step(snap(), 8.9).paused  # 8.9 s clear
    assert g.step(snap(), 1.2).paused_reason is None  # >= 10 s clear
    assert g.state.signal_reason is None


def test_blip_during_clear_window_restarts_it() -> None:
    g = Gov()
    g.step(snap(thermal=Thermal.SERIOUS))
    g.step(snap(), 1)
    g.step(snap(), 8)
    assert g.step(snap(thermal=Thermal.SERIOUS), 1).paused  # blip
    assert g.step(snap(), 1).paused
    assert g.step(snap(), 9).paused  # only 9 s since the blip
    assert not g.step(snap(), 1.5).paused


def test_reason_changes_while_held() -> None:
    g = Gov()
    g.step(snap(thermal=Thermal.SERIOUS))
    assert g.step(snap(low_power_mode=True), 1).paused_reason is R.LOW_POWER_MODE
    assert (
        g.step(snap(), 1).paused_reason is R.LOW_POWER_MODE
    )  # held reason reported while clearing


def test_owner_pause_is_immediate_both_ways_and_expires() -> None:
    g = Gov()
    g.step(snap())
    assert g.step(snap(), 1, owner=OwnerControl(paused=True)).paused_reason is R.USER_PAUSED
    assert g.step(snap(), 1, owner=OwnerControl()).paused_reason is None  # resume: no 10 s wait
    until = g.t + timedelta(minutes=30)
    owner = OwnerControl(paused=True, paused_until=until)
    assert g.step(snap(), 1, owner=owner).paused_reason is R.USER_PAUSED
    assert g.step(snap(), 29 * 60, owner=owner).paused
    assert g.step(snap(), 60, owner=owner).paused_reason is None  # expired


def test_resume_while_signal_still_bad_stays_paused() -> None:
    g = Gov()
    g.step(snap(thermal=Thermal.SERIOUS), owner=OwnerControl(paused=True))
    assert g.step(snap(thermal=Thermal.SERIOUS), 1, owner=OwnerControl()).paused_reason is R.THERMAL


def test_pause_release_timer() -> None:
    g = Gov()
    assert not g.step(snap(thermal=Thermal.SERIOUS)).pause_expired
    assert not g.step(snap(thermal=Thermal.SERIOUS), 59).pause_expired
    assert g.step(snap(thermal=Thermal.SERIOUS), 1).pause_expired
    assert g.step(snap(thermal=Thermal.SERIOUS), 100).pause_expired
    g.step(snap(), 1)
    d = g.step(snap(), 10)  # resumed
    assert not d.paused and not d.pause_expired and g.state.paused_since is None


def test_pause_release_timer_spans_reason_changes_and_survives_blips() -> None:
    g = Gov()
    g.step(snap(thermal=Thermal.SERIOUS))
    g.step(snap(low_power_mode=True), 30)  # reason changes, still paused: timer continues
    assert g.step(snap(low_power_mode=True), 30).pause_expired
    g2 = Gov()  # 5 s blip never reaches 60 s: no release (the point of the 10 s window)
    g2.step(snap(thermal=Thermal.SERIOUS))
    g2.step(snap(), 5)
    assert not g2.step(snap(), 10).paused
    g2.step(snap(thermal=Thermal.SERIOUS), 1)
    assert not g2.step(snap(thermal=Thermal.SERIOUS), 30).pause_expired


def test_custom_timing() -> None:
    t = GovernorTiming(escalate_after_s=5, resume_after_s=2, pause_release_after_s=3)
    g = Gov(timing=t)
    g.step(snap(thermal=Thermal.SERIOUS))
    assert g.step(snap(thermal=Thermal.SERIOUS), 3).pause_expired
    g.step(snap(), 1)
    assert not g.step(snap(), 2).paused
    g.step(snap(hid_idle_s=IDLE), 1)
    assert g.step(snap(hid_idle_s=IDLE), 5).profile is P.FULL


# --- signals_unavailable ----------------------------------------------------------------------


def test_no_snapshot_ever_fails_closed_at_once() -> None:
    g = Gov()
    d = g.step(None)
    assert d.paused_reason is R.SIGNALS_UNAVAILABLE and d.profile is P.LIGHT
    assert not d.release_now and not d.trim


def test_one_or_two_failures_keep_last_value_three_pause() -> None:
    g = Gov()
    good = snap(hid_idle_s=5)
    assert not g.step(good).paused
    assert not g.step(None, 2).paused and g.state.failures == 1
    assert not g.step(None, 2).paused and g.state.failures == 2
    assert g.step(None, 2).paused_reason is R.SIGNALS_UNAVAILABLE
    assert g.step(good, 2).paused_reason is R.SIGNALS_UNAVAILABLE  # held for 10 s
    assert g.state.failures == 0
    assert not g.step(good, 10).paused


def test_failure_counter_resets_on_success() -> None:
    g = Gov()
    g.step(snap())
    g.step(None)
    g.step(None)
    g.step(snap())
    assert not g.step(None).paused and g.state.failures == 1


def test_failures_keep_last_bad_value_too() -> None:
    g = Gov()
    g.step(snap(thermal=Thermal.SERIOUS))
    assert g.step(None, 2).paused_reason is R.THERMAL  # last known value persists


def test_failures_with_no_snapshot_still_honour_owner_profile() -> None:
    g = Gov()
    d = g.step(None, owner=OwnerControl(profile=P.BALANCED))
    assert d.profile is P.BALANCED and d.paused


def test_decide_is_pure() -> None:
    g = Gov()
    s = snap(hid_idle_s=IDLE, thermal=Thermal.SERIOUS)
    a = decide(s, g.cfg, g.specs, None, T0, T0, GovState(), total_gb=TOTAL)
    b = decide(s, g.cfg, g.specs, None, T0, T0, GovState(), total_gb=TOTAL)
    assert a == b


# --- resolved-budget comparison (escalation vs de-escalation) ----------------------------------


def _custom(mem: float, duty: float, batch: int, cpu: str) -> ExecutorConfig:
    return ExecutorConfig(
        idle_profile=P.CUSTOM,
        profiles={
            "custom": {
                "memory_fraction": mem,
                "gpu_duty_cycle": duty,
                "max_batch_size": batch,
                "cpu_priority": cpu,
            }
        },
    )


def test_user_return_from_mixed_custom_applies_at_once() -> None:
    # reviewer repro: custom (0.7 mem, duty 0.2, batch 32, normal) -> light lowers memory + cpu
    g = Gov(_custom(0.7, 0.2, 32, "normal"))
    g.step(snap(hid_idle_s=IDLE))
    g.step(snap(hid_idle_s=IDLE), 61)
    assert g.d.profile is P.CUSTOM
    d = g.step(snap(hid_idle_s=1.0), 0.5)
    assert d.profile is P.LIGHT and d.candidate is None


@pytest.mark.parametrize(
    ("custom", "at_once"),
    [
        ((0.25, 0.30, 4, "background"), True),  # equal in every dimension
        ((0.20, 0.20, 2, "background"), False),  # lower everywhere: light is an escalation
        ((0.30, 0.50, 8, "normal"), True),  # higher everywhere: light is lower -> at once
        ((0.20, 0.50, 8, "background"), False),  # mixed, memory/cpu not lowered
        ((0.20, 0.50, 8, "low"), True),  # mixed, but light lowers the cpu priority
        ((0.30, 0.20, 2, "background"), True),  # mixed, memory goes down
    ],
)
def test_escalation_table(custom: tuple[float, float, int, str], at_once: bool) -> None:
    # current = custom (applied via the pin of a previous owner choice), target = light (active)
    cfg = _custom(*custom)
    g = Gov(cfg)
    g.step(snap(), owner=OwnerControl(profile=P.CUSTOM))
    assert g.d.profile is P.CUSTOM
    d = g.step(snap(), 1, owner=OwnerControl())  # owner back to auto: target light, automatic
    assert (d.profile is P.LIGHT) is at_once
    if not at_once:
        assert d.candidate is P.LIGHT and d.candidate_since is not None
        assert g.step(snap(), 60).profile is P.LIGHT


def test_decision_reports_profile_source() -> None:
    g = Gov(pinned=P.BALANCED)
    assert g.step(snap()).profile_source.value == "pinned"
    assert g.step(snap(), owner=OwnerControl(profile=P.FULL)).profile_source.value == "owner"
    g2 = Gov()
    assert g2.step(snap(hid_idle_s=IDLE)).profile_source.value == "idle"
    assert g2.step(snap(), 1).profile_source.value == "active"


# --- clock, initial state, battery, control_invalid ---------------------------------------------


def test_initial_state_holds_escalation_from_startup() -> None:
    g = Gov(start=True)
    assert g.step(snap(hid_idle_s=IDLE)).profile is P.LIGHT
    assert g.step(snap(hid_idle_s=IDLE), 59).profile is P.LIGHT
    assert g.step(snap(hid_idle_s=IDLE), 1).profile is P.FULL


def test_initial_state_uses_pin() -> None:
    cfg = ExecutorConfig()
    assert initial_state(cfg, P.BALANCED).profile is P.BALANCED
    assert initial_state(cfg).profile is cfg.active_profile


def test_backward_clock_jump_restarts_timers() -> None:
    g = Gov(start=True)
    g.step(snap(hid_idle_s=IDLE))
    g.step(snap(hid_idle_s=IDLE), 50)
    g.t -= timedelta(hours=1)  # clock stepped back
    assert g.step(snap(hid_idle_s=IDLE)).profile is P.LIGHT
    assert g.state.candidate_since == g.t  # restarted at now
    assert g.step(snap(hid_idle_s=IDLE), 59).profile is P.LIGHT
    assert g.step(snap(hid_idle_s=IDLE), 1).profile is P.FULL
    # pause/clear timers too
    g2 = Gov()
    g2.step(snap(thermal=Thermal.SERIOUS))
    g2.step(snap(), 1)
    g2.step(snap(), 5)
    g2.t -= timedelta(hours=1)
    assert g2.step(snap()).paused_reason is R.THERMAL
    assert g2.state.clear_since == g2.t and g2.state.paused_since == g2.t


@pytest.mark.parametrize("owner", [ControlUnusable("bad"), None])
def test_control_unusable_pauses_with_control_invalid(owner: ControlUnusable | None) -> None:
    g = Gov(pinned=P.BALANCED)
    d = g.step(snap(), owner=owner)
    if owner is None:
        assert d.paused_reason is None
        return
    assert d.paused_reason is R.CONTROL_INVALID and d.profile is P.BALANCED  # auto -> the pin
    # signal conditions rank below it
    assert g.step(snap(thermal=Thermal.SERIOUS), 1, owner=owner).paused_reason is R.CONTROL_INVALID
    # fixed file: resumes after the signal clear window only if a signal pause was latched
    assert g.step(snap(), 1, owner=OwnerControl()).paused_reason is R.THERMAL  # latched, clearing
    assert g.step(snap(), 11, owner=OwnerControl()).paused_reason is None


def test_user_pause_reported_for_valid_control() -> None:
    g = Gov()
    assert g.step(snap(), owner=OwnerControl(paused=True)).paused_reason is R.USER_PAUSED


def test_control_unusable_ignores_owner_profile_and_idle() -> None:
    g = Gov()
    d = g.step(snap(hid_idle_s=IDLE), owner=ControlUnusable("x"))
    assert d.paused_reason is R.CONTROL_INVALID and d.profile is P.FULL  # idle rule, not owner
