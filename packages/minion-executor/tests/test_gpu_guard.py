"""GpuDutyGuard: fake clock, synthetic per-client counters."""

from __future__ import annotations

import pytest
from minion_executor.capacity.gpu_guard import (
    GpuAction,
    GpuDutyGuard,
    GpuGuardConfig,
    GpuVerdict,
)

NS = 1_000_000_000
STEP = 2.0


class Rig:
    """Drives the guard at 2 s intervals; `rate` = GPU seconds per wall second for one client."""

    def __init__(self, duty: float = 0.3, *, idle: bool = False, cfg: GpuGuardConfig | None = None):
        self.g = GpuDutyGuard(cfg)
        self.t = 100.0
        self.counters: dict[str, int] = {"0xa": 5 * NS}
        self.g.note_ctl(duty, False, self.t)
        self.g.start(42, self.t, idle=idle)
        self.last: GpuVerdict = self.g.observe(self.t, dict(self.counters), interval_s=STEP)

    def run(self, seconds: float, rate: float, cid: str = "0xa") -> list[GpuVerdict]:
        out = []
        for _ in range(round(seconds / STEP)):
            self.t += STEP
            self.counters[cid] = self.counters.get(cid, 0) + int(rate * STEP * NS)
            self.last = self.g.observe(self.t, dict(self.counters), interval_s=STEP)
            out.append(self.last)
        return out


def actions(vs: list[GpuVerdict]) -> set[GpuAction]:
    return {v.action for v in vs}


def test_honest_at_duty_is_ok() -> None:
    r = Rig(0.3)
    assert actions(r.run(60, 0.3)) == {GpuAction.OK}
    assert r.last.share == pytest.approx(0.3)
    assert r.last.limit == pytest.approx(0.4)


def test_short_span_is_not_judged() -> None:
    r = Rig(0.3)
    first = r.run(6, 1.0)  # span 6 s < 8 s
    assert actions(first) == {GpuAction.OK}
    assert first[-1].share is None


def test_over_gates_with_formula() -> None:
    r = Rig(0.3)
    vs = r.run(10, 0.6)
    v = vs[-1]
    assert v.action is GpuAction.GATE and v.reason == "over_duty"
    assert v.share == pytest.approx(0.6)
    # busy 6 s over 10 s: to reach 0.3 the window needs 20 s of wall -> +10 s, capped 0.75 x 2
    assert v.stop_s == pytest.approx(1.5)


def test_gate_stop_below_cap_matches_formula() -> None:
    r = Rig(0.8)
    v = r.run(10, 0.91)[-1]  # busy 9.1 s / 10 s; limit 0.9
    assert v.action is GpuAction.GATE
    assert v.stop_s == pytest.approx(9.1 / 0.8 - 10.0)


def test_persistent_overuse_kills_after_30s() -> None:
    r = Rig(0.3)
    vs = r.run(60, 0.9)
    kill = next(i for i, v in enumerate(vs) if v.action is GpuAction.KILL)
    first_gate = next(i for i, v in enumerate(vs) if v.action is GpuAction.GATE)
    assert (kill - first_gate) * STEP == pytest.approx(30.0)
    assert vs[kill].reason == "over_duty"
    assert all(v.action is GpuAction.GATE for v in vs[first_gate:kill])


def test_recovery_resets_the_kill_timer() -> None:
    r = Rig(0.3)
    assert GpuAction.GATE in actions(r.run(20, 0.9))
    assert actions(r.run(30, 0.1)[-3:]) == {GpuAction.OK}
    assert actions(r.run(20, 0.9)) <= {GpuAction.OK, GpuAction.GATE}


def test_duty_drop_has_grace_no_false_positive() -> None:
    r = Rig(1.0)
    r.run(20, 0.9)
    r.g.note_ctl(0.3, False, r.t)
    # honest worker obeys the drop at once; the window still holds 0.9 shares, grace keeps the old
    # duty in force until the old busy time has left the window
    vs = r.run(15, 0.3)
    assert actions(vs) == {GpuAction.OK}
    vs = r.run(30, 0.3)
    assert actions(vs) == {GpuAction.OK}
    assert vs[-1].limit == pytest.approx(0.4)


def test_duty_drop_then_slow_obedience_within_grace() -> None:
    r = Rig(0.9)
    r.run(20, 0.8)
    r.g.note_ctl(0.3, False, r.t)
    assert actions(r.run(10, 0.3)) == {GpuAction.OK}


def test_paused_rogue_is_killed_after_grace() -> None:
    r = Rig(0.3)
    r.run(20, 0.3)
    r.g.note_ctl(0.3, True, r.t)
    paused_at = r.t
    vs = r.run(30, 0.8)
    kill = next(v for v in vs if v.action is GpuAction.KILL)
    assert kill.reason == "paused"
    assert kill.limit == pytest.approx(0.10)
    idx = vs.index(kill)
    assert (idx + 1) * STEP <= 5.0 + 2.0 + 2 * STEP, paused_at


def test_paused_honest_worker_that_stops_is_ok() -> None:
    r = Rig(0.3)
    r.run(20, 0.3)
    r.g.note_ctl(0.3, True, r.t)
    assert actions(r.run(60, 0.0)) == {GpuAction.OK}


def test_work_in_pause_grace_is_not_charged() -> None:
    r = Rig(0.3)
    r.run(20, 0.3)
    r.g.note_ctl(0.3, True, r.t)
    vs = r.run(4, 0.5)  # inside the 5 s grace: judged by the old rule, within the limit
    vs += r.run(60, 0.0)
    assert actions(vs) == {GpuAction.OK}


def test_resume_after_pause_uses_duty_again() -> None:
    r = Rig(0.3)
    r.g.note_ctl(0.3, True, r.t)
    r.run(20, 0.0)
    r.g.note_ctl(0.3, False, r.t)
    assert actions(r.run(40, 0.3)) == {GpuAction.OK}


def test_idle_worker_over_ten_percent_is_killed() -> None:
    r = Rig(0.3, idle=True)
    vs = r.run(6, 0.5)
    assert vs[-1].action is GpuAction.KILL and vs[-1].reason == "idle"
    r2 = Rig(0.3, idle=True)
    assert actions(r2.run(60, 0.05)) == {GpuAction.OK}


def test_idle_needs_no_ctl() -> None:
    g = GpuDutyGuard()
    g.start(1, 0.0, idle=True)
    g.observe(0.0, {"a": 0}, interval_s=10)
    assert g.observe(4.0, {"a": 4 * NS}, interval_s=10).action is GpuAction.KILL


def test_unknown_duty_never_acts() -> None:
    g = GpuDutyGuard()
    g.start(1, 0.0)
    g.observe(0.0, {"a": 0}, interval_s=2)
    assert g.observe(20.0, {"a": 20 * NS}, interval_s=2).action is GpuAction.OK


def test_client_churn_kills() -> None:
    r = Rig(0.9)
    vs = []
    for i in range(6):
        r.counters = {f"0x{i:x}": 0}
        r.t += STEP
        vs.append(r.g.observe(r.t, dict(r.counters), interval_s=STEP))
    assert vs[0].action is GpuAction.OK
    assert vs[-1].action is GpuAction.KILL and vs[-1].reason == "clients"


def test_four_clients_are_fine() -> None:
    r = Rig(0.9)
    r.counters = {"0xa": 0, "0xb": 0, "0xc": 0, "0xd": 0}
    assert actions(r.run(20, 0.1)) == {GpuAction.OK}


def test_counter_reset_never_negative() -> None:
    r = Rig(0.3)
    r.run(10, 0.2)
    r.counters["0xa"] = 0  # reset
    vs = r.run(20, 0.2)
    assert all((v.share or 0) >= 0 for v in vs)
    assert actions(vs) == {GpuAction.OK}


def test_vanished_client_keeps_its_time() -> None:
    r = Rig(0.3)
    r.run(10, 0.2)
    r.t += STEP
    r.g.observe(r.t, {}, interval_s=STEP)  # client gone for a sample
    r.t += STEP
    v = r.g.observe(r.t, {"0xa": r.counters["0xa"] + NS}, interval_s=STEP)
    assert v.share is not None and v.share >= 0


def test_duty_one_never_acts() -> None:
    r = Rig(1.0)
    vs = r.run(120, 1.0)
    assert actions(vs) == {GpuAction.OK}
    assert vs[-1].limit is not None and vs[-1].limit >= 1.1


def test_three_meter_errors_kill_and_success_resets() -> None:
    r = Rig(0.3)
    assert r.g.observe_error(r.t).action is GpuAction.OK
    assert r.g.observe_error(r.t).action is GpuAction.OK
    r.run(2, 0.1)  # success resets
    assert r.g.observe_error(r.t).action is GpuAction.OK
    assert r.g.observe_error(r.t).action is GpuAction.OK
    v = r.g.observe_error(r.t)
    assert v.action is GpuAction.KILL and v.reason == "meter"


def test_none_clients_resets() -> None:
    r = Rig(0.3)
    r.run(20, 0.9)
    assert r.g.observe(r.t + 1, None, interval_s=STEP).action is GpuAction.OK
    r.g.start(42, r.t + 1)
    r.t += 1
    r.g.observe(r.t, dict(r.counters), interval_s=STEP)
    assert actions(r.run(4, 0.9)) == {GpuAction.OK}  # new window, span too short


def test_start_rebaselines_same_pid() -> None:
    r = Rig(0.3)
    r.run(10, 0.1)
    r.counters["0xa"] += 500 * NS  # busy while no batch was running
    r.t += 60
    r.g.start(42, r.t)
    r.g.observe(r.t, dict(r.counters), interval_s=STEP)
    assert actions(r.run(20, 0.1)) == {GpuAction.OK}


def test_new_pid_forgets_clients() -> None:
    r = Rig(0.3)
    r.counters = {f"0x{i:x}": 0 for i in range(4)}
    r.run(4, 0.0)
    r.g.start(43, r.t)
    r.counters = {"0x10": 0}
    r.g.observe(r.t, dict(r.counters), interval_s=STEP)
    assert actions(r.run(4, 0.0)) == {GpuAction.OK}
