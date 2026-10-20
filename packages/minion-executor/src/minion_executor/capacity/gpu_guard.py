"""Control-side GPU duty guard (TASK-15; ADR-0006 D3 "enforcement is control-side").

A pure state machine: it is fed per-client `accumulatedGPUTime` readings of the worker pid (see
`gpu_meter.read_gpu_clients_iokit`) and the owner's duty from the governor, and answers OK, GATE
(stop the worker for `stop_s`, then continue) or KILL. It never talks to the worker, so it does not
depend on the worker's cooperation (invariant 6). No clock, no I/O: callers pass `now`.

Rules
-----
- Share = GPU busy time of the worker's clients over a sliding window of `window_s`, divided by the
  window's wall time (reporting and GATE). Busy time is summed from per-client **positive deltas**
  (a client that vanishes never produces a negative); the first reading after `start()` only
  sets the baseline. A live client whose counter goes **backwards** is a meter error (the
  reading cannot be trusted), not a silent re-baseline.
- Limit = allowed + `margin`. `allowed` is the highest duty in force over
  [now - `window_s` - `grace_s`, now], so a duty drop gets the same ack latency as the other D7
  deadlines and cannot cause a false positive. Duty 1.0 means a limit above 1: the guard never acts.
- Over the limit -> GATE with `stop_s` that brings the window back to the duty (not just to the
  limit), at most `max_stop_frac` x the sampling interval.
- KILL on an **excess bucket in GPU-seconds**: every sample adds the GPU time used since the last
  one and subtracts what the limit allows for the elapsed wall time,
  `acc = max(0, acc + busy - limit * dt)`; `acc >= kill_budget_gpu_s` -> KILL. GPU time, not wall
  time, because an honest worker may run one unsplittable step (an mlx-lm prefill of 8-16k tokens
  in a single `mx.eval`, 4-25 s) at full speed and then repays it with a sleep: that burst is an
  excess of `S * (1 - limit)` GPU-s which the repay sleep drains again. The budget is 20 GPU-s: an
  honest unpaced step of up to 33 s at light (limit 0.4), 50 s at balanced (0.6) and any at full
  never reaches it, while a steady rogue at light dies in 20 / (1 - 0.4) = 33 s and at balanced in
  50 s. A burn/rest sawtooth cannot reset it (only a long enough rest drains it), and a worker
  whose long-run share is at most the limit never accumulates (it drains at least as fast as it
  fills): 8 s burn / 8 s rest at balanced (share = duty) stays flat.
- The bucket advances in every mode: in quiet mode (idle, paused-acked) the busy time is charged
  against the margin (`limit = margin`); a sample that straddles the busy -> quiet edge (the edge
  is recorded when it happens, `set_idle`) is allowed the limit in force before the edge for the
  part before it and the margin for the rest, so the busy-mode GPU time of a 1.5-2 s subtask is
  never charged against the idle margin. The mirror edge (idle -> busy, also recorded when it
  happens) is split the same way: the idle span that falls inside the sample is allowed the
  margin, the rest the busy limit (a rogue that burns only inside idle gaps shorter than a
  sample is charged for them, also at duty 1.0). At duty 1.0 the busy limit is above 1 and the
  bucket only drains, except for quiet samples and idle spans, which still charge against the
  margin. A worker that is paused or idle after a long step repays it at `margin` GPU-s per
  second; a gap without a worker, also one after a quiet-mode KILL, drains at the last
  busy-mode limit (the quiet KILL itself is share-based and immediate).
  Caveat: an unacked pause is judged as busy mode until `window_s + grace_s` have passed (the
  duty in force before the pause stays allowed), then at the margin: a long uncheckpointed
  step under a pause can be killed after about 28 s at light and 37 s at full.
- The bucket is **per executor run, not per worker pid**: a new pid re-baselines the counters but
  keeps `acc`, drained by `limit * gap` for the time without a worker, and a KILL leaves it at or
  above the budget. A rogue that is killed and respawned therefore gets about one sample interval
  of GPU per life, not a fresh 20 GPU-s.
- Paused by the governor or an idle warm worker: allowed is 0, so the limit is `margin`; the
  share is measured since the quiet period began and over the limit -> KILL at once (gating a
  paused worker to 0 is pointless). A paused worker may be mid-step (a long honest step ends
  only at its next checkpoint), so the quiet period of a pause begins when the worker **acked**
  the pause (`note_pause_ack`, the first ack after the pause began, + `ack_settle_s`). Unacked,
  the pause is judged as quiet (a) from `pause_unacked_bound_s` on, or (b) from `grace_s` after
  the pause once the worker has been seen **ticking** (`note_tick`) after that point without a
  paused ack: a checkpoint applies pending control first, so a tick without an ack is a worker
  ignoring the pause.
- More than `max_clients` distinct client ids for the pid -> KILL (a worker opening and closing
  GPU clients between samples to hide its time; an honest MLX worker has one).
- `max_meter_errors` consecutive meter errors -> KILL with reason "meter" (fail closed, like the
  signal reads in D2).
"""

from __future__ import annotations

import enum
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class GpuGuardConfig:
    window_s: float = 10.0
    min_span_frac: float = 0.8
    margin: float = 0.10
    grace_s: float = 5.0
    kill_budget_gpu_s: float = 20.0  # excess GPU-seconds over the limit before a KILL
    max_stop_frac: float = 0.75
    max_clients: int = 4
    max_meter_errors: int = 3
    quiet_min_span_s: float = 2.0  # judging span when paused / idle (limit is only `margin`)
    ack_settle_s: float = 1.0  # in-flight GPU work after a pause ack
    pause_unacked_bound_s: float = 60.0  # an unacked, tick-less pause is judged as quiet after this


class GpuAction(enum.Enum):
    OK = "ok"
    GATE = "gate"
    KILL = "kill"


@dataclass(frozen=True)
class GpuVerdict:
    action: GpuAction
    share: float | None = None
    limit: float | None = None
    stop_s: float = 0.0
    reason: str = ""  # over_duty | paused | idle | clients | meter


_OK = GpuVerdict(GpuAction.OK)


class GpuDutyGuard:
    def __init__(self, cfg: GpuGuardConfig | None = None) -> None:
        self.cfg = cfg or GpuGuardConfig()
        self._pid: int | None = None
        self._seen: dict[str, int] = {}  # last value per client id (kept while the pid is)
        self._ids: set[str] = set()  # distinct client ids of the pid
        self._ctl: list[tuple[float, float, bool]] = []  # (since, duty, paused), ascending
        self._ack_at: float | None = None  # when the worker first acked the current pause
        self._tick_at: float | None = None  # latest worker tick (control-side monotonic)
        self._idle = False
        self._quiet_span: tuple[float, float] | None = None  # last idle span (start, end)
        self._acc = 0.0  # excess GPU-seconds; per run, survives worker pids
        self._acc_t: float | None = None  # when `_acc` was last advanced
        self._last_limit = 0.0  # limit used then (drains the bucket across a worker gap)
        self._reset_window(0.0)
        self._errors = 0

    # -- lifecycle ---------------------------------------------------------------------------

    def _reset_window(self, now: float) -> None:
        self._samples: deque[tuple[float, float]] = deque()  # (t, cumulative busy seconds)
        self._cum = 0.0
        self._primed = False
        self._started = now

    def start(self, pid: int, now: float, *, idle: bool = False) -> None:
        """Fresh window for `pid`. The client baseline and id set are kept when the pid is the
        same as before; the first reading only re-baselines, so GPU work between `stop` and
        `start` is not charged. The excess bucket is kept (drained for the gap): a respawn does
        not buy a fresh allowance."""
        if pid != self._pid:
            self._pid, self._seen, self._ids = pid, {}, set()
        self._drain_gap(now)
        self._idle = idle
        self._quiet_span = None
        self._errors = 0
        self._reset_window(now)

    def _drain_gap(self, now: float) -> None:
        if self._acc_t is not None and now > self._acc_t:
            gap = min(now - self._acc_t, 3600.0)
            self._acc = max(0.0, self._acc - self._last_limit * gap)
        self._acc_t = now

    def set_idle(self, idle: bool, now: float) -> None:
        """Switch between busy and idle mode without touching the window or baselines. Entering
        idle starts the quiet period at `now`: only samples since then are judged against 0."""
        if idle == self._idle:
            return
        self._idle = idle
        if idle:
            self._started = now
        else:  # idle -> busy: the idle span is charged at the margin in the next busy sample
            self._quiet_span = (self._started, now)

    def stop(self) -> None:
        """No live worker: forget the pid and the window (the excess bucket stays)."""
        self._pid, self._seen, self._ids = None, {}, set()
        self._quiet_span = None
        self._errors = 0
        self._reset_window(0.0)

    def note_ctl(self, duty: float, paused: bool, now: float) -> None:
        """Record the duty / pause state in force from `now` on (idempotent for no change)."""
        if self._ctl and self._ctl[-1][1:] == (duty, paused):
            return
        if self._ctl and self._ctl[-1][0] >= now:
            self._ctl.pop()  # same instant: the latest wins
        self._ctl.append((now, duty, paused))
        if self._ctl[-2:-1] and self._ctl[-2][2] != paused:
            self._ack_at = None  # a new pause / resume is not yet acked
        cutoff = now - (self.cfg.window_s + self.cfg.grace_s) * 4
        while len(self._ctl) > 1 and self._ctl[1][0] <= cutoff:
            self._ctl.pop(0)

    def note_pause_ack(self, at: float) -> None:
        """The worker acked the pause in force (control-side monotonic time of the ack). Only the
        first ack after the pause began counts: a re-sent ack must not move the quiet period."""
        if self._ack_at is None and self._ctl and self._ctl[-1][2] and at >= self._ctl[-1][0]:
            self._ack_at = at

    def note_tick(self, at: float) -> None:
        """Latest worker tick (a checkpoint was reached)."""
        self._tick_at = at

    # -- inputs ------------------------------------------------------------------------------

    def observe_error(self, now: float) -> GpuVerdict:
        """A meter read failed. Fail closed after `max_meter_errors` in a row."""
        self._errors += 1
        if self._errors >= self.cfg.max_meter_errors:
            return GpuVerdict(GpuAction.KILL, reason="meter")
        return _OK

    def observe(
        self, now: float, clients: Mapping[str, int] | None, *, interval_s: float
    ) -> GpuVerdict:
        """One reading of the worker pid's clients (`id -> accumulated ns`); None = no live
        worker (everything resets). `interval_s` is the sampling period (bounds the gate)."""
        if clients is None:
            self._errors = 0
            self._reset_window(now)
            return _OK
        if self._samples and now <= self._samples[-1][0]:
            return _OK  # clock did not advance: nothing to learn
        if any(cid in self._seen and v < self._seen[cid] for cid, v in clients.items()):
            self._seen.update(clients)  # re-baseline, but a backwards counter is a bad reading
            return self.observe_error(now)
        self._errors = 0
        self._ids.update(clients)
        if len(self._ids) > self.cfg.max_clients:
            return GpuVerdict(GpuAction.KILL, reason="clients")
        for cid, value in clients.items():
            if self._primed:
                self._cum += max(0, value - self._seen.get(cid, 0)) / 1e9
            self._seen[cid] = value
        self._primed = True
        prev = self._samples[-1] if self._samples else None
        self._samples.append((now, self._cum))
        busy, dt = (self._cum - prev[1], now - prev[0]) if prev is not None else (0.0, 0.0)
        if dt > self.cfg.window_s:  # a sampler gap: cap dt and scale the busy time with it
            busy *= self.cfg.window_s / dt
            dt = self.cfg.window_s
        return self._judge(now, interval_s, busy, dt, prev[0] if prev is not None else now)

    # -- judgement ---------------------------------------------------------------------------

    def _state_at_end(self) -> tuple[float, bool, float]:
        """`(duty, paused, since)` in force now; since = when that state began."""
        t, duty, paused = self._ctl[-1]
        return duty, paused, t

    def _allowed(self, lo: float, now: float) -> float:
        """Highest duty in force over [lo, now] (a paused stretch counts 0)."""
        best = 0.0
        for i, (since, duty, paused) in enumerate(self._ctl):
            end = self._ctl[i + 1][0] if i + 1 < len(self._ctl) else now
            if end >= lo and since <= now and not paused:
                best = max(best, duty)
        return best

    def _quiet_from(self, now: float) -> float | None:
        """Start of the period in which the worker must use ~no GPU (idle, or paused and acked),
        or None while it may still run."""
        cfg = self.cfg
        if self._idle:
            return self._started
        _, paused, since = self._state_at_end()
        if not paused:
            return None
        quiet: float | None = None
        if self._ack_at is not None:
            quiet = self._ack_at + cfg.ack_settle_s
        elif self._tick_at is not None and self._tick_at >= since + cfg.grace_s:
            quiet = since + cfg.grace_s  # ticking without a paused ack: ignoring the pause
        elif now - since >= cfg.pause_unacked_bound_s:
            quiet = since + cfg.pause_unacked_bound_s
        return quiet if quiet is not None and quiet <= now else None

    def _judge(
        self, now: float, interval_s: float, busy_delta: float, dt: float, prev_t: float
    ) -> GpuVerdict:
        cfg = self.cfg
        if not self._ctl and not self._idle:
            return _OK  # duty unknown: nothing to enforce against
        quiet_from = self._quiet_from(now)
        lower = now - cfg.window_s
        if quiet_from is not None:
            lower = max(lower, quiet_from)
        # keep one anchor at or before `lower`
        while len(self._samples) > 1 and self._samples[1][0] <= now - cfg.window_s:
            self._samples.popleft()
        anchor = next((s for s in reversed(self._samples) if s[0] <= lower), self._samples[0])
        if quiet_from is not None and anchor[0] < quiet_from:
            # first sample inside the quiet period is the baseline; work before it was legitimate
            anchor = next((s for s in self._samples if s[0] >= quiet_from), self._samples[-1])
        span = now - anchor[0]
        need = cfg.quiet_min_span_s if quiet_from is not None else cfg.min_span_frac * cfg.window_s
        if quiet_from is None:
            allowed = self._allowed(now - cfg.window_s - cfg.grace_s, now)
            limit = allowed + cfg.margin
        else:
            allowed, limit = 0.0, cfg.margin
        # the bucket: excess GPU-seconds over what the limit allows. It advances in every mode
        # (quiet samples charge their busy time against the margin, a gap-timed rogue cannot
        # escape; at duty 1.0 the limit is above 1 and the bucket only drains).
        allow = limit * dt
        if quiet_from is None:
            self._last_limit = limit  # a gap without a worker drains at the duty in force
            if self._quiet_span is not None:
                # the sample straddles the idle -> busy edge: the idle part gets the margin only,
                # the busy tail of the previous subtask and the new busy part the busy limit
                q = min(self._quiet_span[1], now) - max(self._quiet_span[0], prev_t, now - dt)
                if q > 0:
                    allow = cfg.margin * q + limit * (dt - q)
        elif prev_t < quiet_from:
            # the sample straddles the busy -> quiet edge: the part before the edge was busy mode
            # and is allowed at the limit in force then, only the rest at the margin
            pre = min(quiet_from, now) - max(prev_t, now - dt)
            if pre > 0:
                busy_limit = (
                    self._allowed(quiet_from - cfg.window_s - cfg.grace_s, quiet_from) + cfg.margin
                )
                allow = busy_limit * pre + limit * (dt - pre)
        self._acc = max(0.0, self._acc + busy_delta - allow)
        self._acc_t = now
        if self._acc >= cfg.kill_budget_gpu_s:
            return GpuVerdict(GpuAction.KILL, None, limit, reason="over_duty")
        if span < need or span <= 0:
            return _OK
        busy = self._cum - anchor[1]
        share = busy / span
        if quiet_from is not None:
            if share <= limit:
                return GpuVerdict(GpuAction.OK, share, limit)
            return GpuVerdict(
                GpuAction.KILL, share, limit, reason="idle" if self._idle else "paused"
            )
        if allowed >= 1.0 or share <= limit:
            return GpuVerdict(GpuAction.OK, share, limit)
        cap = cfg.max_stop_frac * interval_s
        need_stop = busy / allowed - span if allowed > 0 else cap
        stop_s = min(max(need_stop, 0.0), cap)
        return GpuVerdict(GpuAction.GATE, share, limit, stop_s, "over_duty")
