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


def test_persistent_overuse_kills_when_the_excess_bucket_fills() -> None:
    r = Rig(0.3)
    vs = r.run(80, 0.9)
    kill = next(i for i, v in enumerate(vs) if v.action is GpuAction.KILL)
    assert vs[kill].reason == "over_duty"
    # excess rate 0.9 - 0.4 = 0.5 GPU-s per second against a 20 GPU-s budget: about 40 s
    assert 36.0 <= (kill + 1) * STEP <= 46.0
    assert GpuAction.GATE in actions(vs[:kill])


def test_recovery_drains_the_bucket() -> None:
    """Not a timer reset: the excess of a burst is paid back by a quiet stretch (GPU-seconds)."""
    r = Rig(0.3)
    assert GpuAction.KILL not in actions(r.run(20, 0.9))  # 10 GPU-s of excess
    assert GpuAction.KILL not in actions(r.run(40, 0.1))  # drains 0.3 GPU-s per second
    assert r.g._acc == pytest.approx(0.0, abs=0.5)
    assert GpuAction.KILL not in actions(r.run(20, 0.9))  # a fresh burst starts from empty


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
    r.g.note_pause_ack(r.t + 1.0)  # the worker acked the pause and keeps burning
    paused_at = r.t
    vs = r.run(30, 0.8)
    kill = next(v for v in vs if v.action is GpuAction.KILL)
    assert kill.reason == "paused"
    assert kill.limit == pytest.approx(0.10)
    idx = vs.index(kill)
    assert (idx + 1) * STEP <= 1.0 + 1.0 + 2.0 + 2 * STEP, paused_at


def test_pause_mid_long_honest_step_is_not_killed() -> None:
    """Paused during a 6 s step: the GPU stays busy until the step ends and the worker acks;
    after the ack it is quiet. No kill, however long the step."""
    r = Rig(0.3)
    r.run(20, 0.3)
    r.g.note_ctl(0.3, True, r.t)
    assert GpuAction.KILL not in actions(r.run(6, 1.0))  # finishing the step, unacked
    r.g.note_pause_ack(r.t)
    assert GpuAction.KILL not in actions(r.run(60, 0.0))


def test_unacked_pause_that_keeps_burning_is_killed() -> None:
    r = Rig(0.3)
    r.run(20, 0.3)
    r.g.note_ctl(0.3, True, r.t)
    vs = r.run(100, 0.8)  # never acks and keeps burning: the over-budget rule still bites
    kill = next(i for i, v in enumerate(vs) if v.action is GpuAction.KILL)
    assert (kill + 1) * STEP <= 60.0 + 2.0 + 2 * STEP


def _sawtooth(r: Rig, burn: float, rest: float, total: float) -> float | None:
    """Seconds until KILL for burn(100 %)/rest(0) repeating, or None."""
    t = 0.0
    while t < total:
        for rate, length in ((1.0, burn), (0.0, rest)):
            for v in r.run(length, rate):
                t += STEP
                if v.action is GpuAction.KILL:
                    return t
    return None


@pytest.mark.parametrize("duty", [0.3, 0.5])
@pytest.mark.parametrize("burn,rest", [(24, 8), (30, 8), (30, 10)])
def test_sawtooth_is_killed_in_bounded_time(duty: float, burn: int, rest: int) -> None:
    r = Rig(duty)
    killed = _sawtooth(r, burn, rest, 600)
    assert killed is not None and killed <= 120.0, killed


def test_honest_just_under_the_limit_never_accumulates() -> None:
    r = Rig(0.3)
    assert actions(r.run(1200, 0.3 + 0.09)) == {GpuAction.OK}


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


class Sim:
    """A piecewise-constant GPU rate timeline sampled at jittered 2 s intervals."""

    def __init__(self, duty: float, pattern: list[tuple[float, float]], horizon: float) -> None:
        import random

        self.rnd = random.Random(1)  # noqa: S311
        self.g = GpuDutyGuard()
        self.g.note_ctl(duty, False, 0.0)
        self.g.start(1, 0.0)
        segs: list[tuple[float, float, float]] = []
        t = 0.0
        while t < horizon + 120:
            for dur, rate in pattern:
                segs.append((t, t + dur, rate))
                t += dur
        self.segs, self.horizon = segs, horizon

    def cum(self, x: float) -> float:
        c = 0.0
        for a, b, r in self.segs:
            if a >= x:
                break
            c += r * (min(b, x) - a)
        return c

    def run(self) -> tuple[float | None, float]:
        """(kill time or None, max bucket)"""
        t, mx = 0.0, 0.0
        self.g.observe(t, {"a": 0}, interval_s=STEP)
        while t < self.horizon:
            t += STEP * self.rnd.uniform(0.75, 1.25)
            v = self.g.observe(t, {"a": int(self.cum(t) * NS)}, interval_s=STEP)
            mx = max(mx, self.g._acc)
            if v.action is GpuAction.KILL:
                return t, mx
        return None, mx


@pytest.mark.parametrize("duty,steps", [(0.3, (5, 10, 20, 25)), (0.5, (5, 10, 20, 30))])
@pytest.mark.parametrize("decode_s", [0, 20])
def test_honest_long_unsplittable_step_is_never_killed(
    duty: float, steps: tuple[int, ...], decode_s: int
) -> None:
    """An mlx-lm prefill in one mx.eval runs unpaced for S seconds, then the pacer repays
    S * (1 - duty) / duty of sleep, then paced decode. Over 30 min: no kill."""
    for step in steps:
        pattern = [(float(step), 1.0), (step * (1 - duty) / duty, 0.0)]
        if decode_s:
            pattern.append((float(decode_s), duty))
        killed, peak = Sim(duty, pattern, 1800).run()
        assert killed is None, (duty, step, decode_s, peak)
        assert peak < GpuGuardConfig().kill_budget_gpu_s


def test_burn_rest_at_the_duty_share_never_accumulates() -> None:
    killed, peak = Sim(0.5, [(8.0, 1.0), (8.0, 0.0)], 1800).run()
    assert killed is None and peak < 8.0, peak


@pytest.mark.parametrize(
    "duty,pattern,bound",
    [
        (0.3, [(1000.0, 1.0)], 45.0),  # steady rogue: 20 / (1 - 0.4) = 33 s
        (0.5, [(1000.0, 1.0)], 60.0),  # 20 / (1 - 0.6) = 50 s
        (0.3, [(24.0, 1.0), (8.0, 0.0)], 100.0),
        (0.3, [(30.0, 1.0), (8.0, 0.0)], 100.0),
        (0.3, [(30.0, 1.0), (10.0, 0.0)], 100.0),
        (0.5, [(24.0, 1.0), (8.0, 0.0)], 200.0),
        (0.5, [(30.0, 1.0), (8.0, 0.0)], 200.0),
        (0.5, [(30.0, 1.0), (10.0, 0.0)], 200.0),
    ],
)
def test_rogues_are_killed_within_bounds(
    duty: float, pattern: list[tuple[float, float]], bound: float
) -> None:
    killed, _ = Sim(duty, pattern, 600).run()
    assert killed is not None and killed <= bound, killed


def test_respawned_rogue_does_not_get_a_fresh_allowance() -> None:
    """Kill + respawn churn: the bucket survives the pid and drains only by the quiet gap, so a
    rogue running flat out gets about a sample interval per life (security's churn pattern,
    with the back-off the loop applies)."""
    import random

    rnd = random.Random(3)  # noqa: S311
    g = GpuDutyGuard()
    g.note_ctl(0.3, False, 0.0)
    t, busy, pid, kills = 0.0, 0.0, 0, 0
    while t < 3600:
        pid += 1
        t += 3.0  # model load
        g.start(pid, t)
        cum = 0.0
        g.observe(t, {"c": 0}, interval_s=STEP)
        while t < 3600:
            step = STEP * rnd.uniform(0.75, 1.25)
            t += step
            cum += step
            busy += step
            if g.observe(t, {"c": int(cum * NS)}, interval_s=STEP).action is GpuAction.KILL:
                break
        kills += 1
        g.stop()
        t += min(300.0, 10.0 * 2 ** min(kills - 1, 6)) * rnd.uniform(0.5, 1.0)
    assert busy / t <= 0.4, busy / t


def test_first_pause_ack_is_kept() -> None:
    r = Rig(0.3)
    r.run(20, 0.3)
    r.g.note_ctl(0.3, True, r.t)
    r.g.note_pause_ack(r.t + 1.0)
    r.g.note_pause_ack(r.t + 50.0)  # a re-sent ack of the same generation must not move it
    assert r.g._ack_at == pytest.approx(r.t + 1.0)


def test_ticking_without_a_paused_ack_is_killed_after_grace() -> None:
    r = Rig(0.3)
    r.run(20, 0.3)
    paused_at = r.t
    r.g.note_ctl(0.3, True, paused_at)
    vs = r.run(8, 0.8)  # inside grace + ack window: still finishing a step
    assert GpuAction.KILL not in actions(vs[:2])
    r.g.note_tick(paused_at + 6.0)  # a checkpoint after grace, no paused ack
    vs = r.run(12, 0.8)
    assert any(v.action is GpuAction.KILL and v.reason == "paused" for v in vs)


def test_silent_unacked_pause_is_not_killed_at_once() -> None:
    """No ticks (a long honest step): the quiet period waits for the ack."""
    r = Rig(0.3)
    r.run(20, 0.3)
    r.g.note_ctl(0.3, True, r.t)
    assert all(v.reason != "paused" for v in r.run(12, 0.8))


def test_counter_regression_is_a_meter_error() -> None:
    r = Rig(0.3)
    r.run(10, 0.3)
    for i in range(2):
        r.t += STEP
        r.counters["0xa"] -= 3 * NS  # the live client's counter went backwards
        v = r.g.observe(r.t, dict(r.counters), interval_s=STEP)
        assert v.action is GpuAction.OK, i
    r.t += STEP
    r.counters["0xa"] -= 3 * NS
    v = r.g.observe(r.t, dict(r.counters), interval_s=STEP)
    assert v.action is GpuAction.KILL and v.reason == "meter"
    r.t += STEP  # a normal reading clears the count
    r.counters["0xa"] += NS
    assert r.g.observe(r.t, dict(r.counters), interval_s=STEP).action is GpuAction.OK


def _drive(
    g: GpuDutyGuard,
    segs: list[tuple[float, float]],
    ctl: list[tuple[float, float, bool]],
    *,
    acks: tuple[float, ...] = (),
    idle: tuple[tuple[float, float], ...] = (),
) -> tuple[float | None, float]:
    import random

    rnd = random.Random(0)  # noqa: S311
    tl, t = [], 0.0
    for dur, rate in segs:
        tl.append((t, t + dur, rate))
        t += dur
    end = t

    def cum(x: float) -> float:
        c = 0.0
        for a, b, r in tl:
            if a >= x:
                break
            c += r * (min(b, x) - a)
        return c

    t, mx = 0.0, 0.0
    ev, ak = sorted(ctl), sorted(acks)
    g.observe(0.0, {"a": 0}, interval_s=STEP)
    while t < end:
        t += STEP * rnd.uniform(0.75, 1.25)
        while ev and ev[0][0] <= t:
            et, du, pa = ev.pop(0)
            g.note_ctl(du, pa, et)
        while ak and ak[0] <= t:
            g.note_pause_ack(ak.pop(0))
        g.set_idle(any(a <= t < b for a, b in idle), t)
        v = g.observe(t, {"a": int(cum(t) * NS)}, interval_s=STEP)
        mx = max(mx, g._acc)
        if v.action is GpuAction.KILL:
            return t, mx
    return None, mx


@pytest.mark.parametrize("step", [10, 15, 20])
def test_bucket_carry_through_pause_and_idle_is_repaid(step: int) -> None:
    """Long step, a pause cuts its repay sleep, paused 60 s (acked), resumed, idle 10 min, next
    long step: the quiet time drains the bucket, no kill."""
    d = 0.3
    repay = step * (1 - d) / d
    g = GpuDutyGuard()
    g.start(1, 0.0)
    g.note_ctl(d, False, 0.0)
    p0 = step + 2
    segs = [(step, 1.0), (2, 0.0), (60, 0.0), (600, 0.0), (step, 1.0), (repay, 0.0)]
    ctl = [(0.0, d, False), (p0, d, True), (p0 + 60, d, False)]
    killed, _ = _drive(g, segs, ctl, acks=(p0 + 0.5,), idle=((p0 + 60, p0 + 660),))
    assert killed is None


@pytest.mark.parametrize("step", [10, 15, 20])
def test_bucket_carry_through_full_duty_is_repaid(step: int) -> None:
    """light -> full mid-repay (the owed sleep vanishes), 30 min at full, back to light, long
    step: the bucket drained while the limit was above 1."""
    d = 0.3
    repay = step * (1 - d) / d
    g = GpuDutyGuard()
    g.start(1, 0.0)
    g.note_ctl(d, False, 0.0)
    segs = [(step, 1.0), (2, 0.0), (1800, 0.5), (30, 0.0), (step, 1.0), (repay, 0.0)]
    ctl = [(0.0, d, False), (step + 2, 1.0, False), (step + 2 + 1800, d, False)]
    killed, _ = _drive(g, segs, ctl)
    assert killed is None


def test_quiet_samples_charge_the_bucket() -> None:
    """A gap-timed rogue: bursts that the samples only see in idle mode still fill the bucket."""
    g = GpuDutyGuard(GpuGuardConfig(quiet_min_span_s=1000.0))  # never the quiet share rule
    g.start(1, 0.0, idle=True)
    t, cum = 0.0, 0.0
    g.observe(t, {"a": 0}, interval_s=STEP)
    killed = None
    for _ in range(100):
        t += STEP
        cum += STEP  # 100 % busy while "idle"
        v = g.observe(t, {"a": int(cum * NS)}, interval_s=STEP)
        if v.action is GpuAction.KILL:
            killed = t
            break
    assert killed is not None and killed <= 40.0  # 20 / (1 - 0.1) ~ 22 s


def test_sampler_gap_scales_busy_with_dt() -> None:
    g = GpuDutyGuard()
    g.start(1, 0.0)
    g.note_ctl(0.3, False, 0.0)
    g.observe(0.0, {"a": 0}, interval_s=STEP)
    # 100 s gap during which the worker was 30 % busy: not an excess once dt is capped alike
    g.observe(100.0, {"a": int(30 * NS)}, interval_s=STEP)
    assert g._acc == pytest.approx(0.0, abs=0.01)


def _edge_sim(
    duty: float,
    rate: float,
    sub_s: float,
    gap_s: float,
    horizon: float,
    *,
    seed: int = 0,
) -> tuple[float | None, float]:
    """Subtasks of `sub_s` at GPU `rate` separated by idle gaps of `gap_s`; the busy/idle edges
    are reported to the guard when they happen (like the loop), samples are jittered 2 s."""
    import random

    rnd = random.Random(seed)  # noqa: S311
    g = GpuDutyGuard()
    g.start(1, 0.0)
    g.note_ctl(duty, False, 0.0)
    g.set_idle(False, 0.0)
    edges: list[tuple[float, bool]] = []  # (time, idle)
    t = 0.0
    while t < horizon + 10:
        edges.append((t + sub_s, True))
        edges.append((t + sub_s + gap_s, False))
        t += sub_s + gap_s
    cycle = sub_s + gap_s

    def cum(x: float) -> float:
        full, rem = divmod(x, cycle)
        return (full * sub_s + min(rem, sub_s)) * rate

    t, mx, ei = 0.0, 0.0, 0
    g.observe(0.0, {"a": 0}, interval_s=STEP)
    while t < horizon:
        t += STEP * rnd.uniform(0.75, 1.25)
        while ei < len(edges) and edges[ei][0] <= t:
            g.set_idle(edges[ei][1], edges[ei][0])
            ei += 1
        v = g.observe(t, {"a": int(cum(t) * NS)}, interval_s=STEP)
        mx = max(mx, g._acc)
        if v.action is GpuAction.KILL:
            return t, mx
    return None, mx


def test_honest_full_duty_worker_with_short_subtasks_is_never_killed() -> None:
    """100 % busy inside 1.8 s subtasks, 0.3 s idle gaps, duty 1.0, 8 h: the busy-mode GPU time
    before a busy -> idle edge is not charged against the idle margin."""
    killed, peak = _edge_sim(1.0, 1.0, 1.8, 0.3, 8 * 3600)
    assert killed is None and peak < 1.0, (killed, peak)


def test_honest_paced_worker_with_short_subtasks_is_never_killed() -> None:
    killed, peak = _edge_sim(0.75, 0.75, 1.8, 0.3, 8 * 3600)
    assert killed is None and peak < 1.0, (killed, peak)


@pytest.mark.parametrize("duty,bound", [(0.3, 120.0), (0.5, 200.0)])
def test_rogue_through_short_subtasks_is_killed_within_bounds(duty: float, bound: float) -> None:
    killed, _ = _edge_sim(duty, 1.0, 1.8, 0.3, 600)
    assert killed is not None and killed <= bound, killed
