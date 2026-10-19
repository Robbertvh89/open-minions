"""Control-side GPU duty guard (TASK-15; ADR-0006 D3 "enforcement is control-side").

A pure state machine: it is fed per-client `accumulatedGPUTime` readings of the worker pid (see
`gpu_meter.read_gpu_clients_iokit`) and the owner's duty from the governor, and answers OK, GATE
(stop the worker for `stop_s`, then continue) or KILL. It never talks to the worker, so it does not
depend on the worker's cooperation (invariant 6). No clock, no I/O: callers pass `now`.

Rules
-----
- Share = GPU busy time of the worker's clients over a sliding window of `window_s`, divided by the
  window's wall time. Busy time is summed from per-client **positive deltas** (a counter that goes
  backwards, or a client that vanishes, never produces a negative); the first reading after
  `start()` only sets the baseline.
- Limit = allowed + `margin`. `allowed` is the highest duty in force over
  [window start - `grace_s`, now], so a duty drop gets the same ack latency as the other D7
  deadlines and cannot cause a false positive. A share is judged once the span is at least
  `min_span_frac` x window. Duty 1.0 means a limit above 1: the guard never acts.
- Over the limit -> GATE with `stop_s` that brings the window back to the duty (not just to the
  limit), at most `max_stop_frac` x the sampling interval.
- KILL on a leaky over-budget accumulator (a burn/rest sawtooth must not reset it): while the
  share is over the limit, `acc` grows by the elapsed time; while it is under, `acc` drains by
  `drain_frac` (0.25) x the elapsed time, floor 0. `acc >= kill_after_s` -> KILL. Chosen so that
  any repeating pattern that is over the limit for more than `drain_frac / (1 + drain_frac)` = 20 %
  of the time accumulates without bound (burn 24 s / rest 8 s, 30/8 and 30/10 at light and
  balanced are over ~90 % of the time and die in about 35-40 s), while a worker that never
  exceeds the limit never accumulates. A steady rogue still dies `kill_after_s` after its
  first over-limit sample.
- Paused by the governor or an idle warm worker: allowed is 0, so the limit is `margin`; the
  share is measured since the quiet period began and over the limit -> KILL at once (gating a
  paused worker to 0 is pointless). A paused worker may be mid-step (a long honest step ends
  only at its next checkpoint), so the quiet period of a pause begins when the worker **acked**
  the pause (`note_pause_ack`, + `ack_settle_s`); an unacked pause is judged only after
  `pause_unacked_bound_s` (the pause-release bound), so an honest long step is never killed
  for finishing.
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
    kill_after_s: float = 30.0
    max_stop_frac: float = 0.75
    max_clients: int = 4
    max_meter_errors: int = 3
    quiet_min_span_s: float = 2.0  # judging span when paused / idle (limit is only `margin`)
    drain_frac: float = 0.25  # over-budget accumulator drain per clean second
    ack_settle_s: float = 1.0  # in-flight GPU work after a pause ack
    pause_unacked_bound_s: float = 60.0  # an unacked pause is judged as quiet after this


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
        self._ack_at: float | None = None  # when the worker acked the current pause
        self._idle = False
        self._reset_window(0.0)
        self._errors = 0

    # -- lifecycle ---------------------------------------------------------------------------

    def _reset_window(self, now: float) -> None:
        self._samples: deque[tuple[float, float]] = deque()  # (t, cumulative busy seconds)
        self._cum = 0.0
        self._primed = False
        self._acc = 0.0  # leaky over-budget seconds
        self._over_prev = False
        self._last_judged: float | None = None
        self._started = now

    def start(self, pid: int, now: float, *, idle: bool = False) -> None:
        """Fresh window for `pid`. The client baseline and id set are kept when the pid is the
        same as before; the first reading only re-baselines, so GPU work between `stop` and
        `start` is not charged."""
        if pid != self._pid:
            self._pid, self._seen, self._ids = pid, {}, set()
        self._idle = idle
        self._errors = 0
        self._reset_window(now)

    def set_idle(self, idle: bool, now: float) -> None:
        """Switch between busy and idle mode without touching the window or baselines. Entering
        idle starts the quiet period at `now`: only samples since then are judged against 0."""
        if idle == self._idle:
            return
        self._idle = idle
        if idle:
            self._started = now

    def stop(self) -> None:
        """No live worker: forget the pid and the window."""
        self._pid, self._seen, self._ids = None, {}, set()
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
        """The worker acked the pause in force (control-side monotonic time of the ack)."""
        if self._ctl and self._ctl[-1][2] and at >= self._ctl[-1][0]:
            self._ack_at = at

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
        self._errors = 0
        if clients is None:
            self._reset_window(now)
            return _OK
        if self._samples and now <= self._samples[-1][0]:
            return _OK  # clock did not advance: nothing to learn
        self._ids.update(clients)
        if len(self._ids) > self.cfg.max_clients:
            return GpuVerdict(GpuAction.KILL, reason="clients")
        for cid, value in clients.items():
            if self._primed:
                self._cum += max(0, value - self._seen.get(cid, 0)) / 1e9
            self._seen[cid] = value
        self._primed = True
        self._samples.append((now, self._cum))
        return self._judge(now, interval_s)

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

    def _judge(self, now: float, interval_s: float) -> GpuVerdict:
        cfg = self.cfg
        if not self._ctl and not self._idle:
            return _OK  # duty unknown: nothing to enforce against
        quiet_from: float | None = None
        if self._idle:
            quiet_from = self._started
        else:
            _, paused, since = self._state_at_end()
            if paused:
                if self._ack_at is not None:
                    quiet_from = self._ack_at + cfg.ack_settle_s
                elif now - since >= cfg.pause_unacked_bound_s:
                    quiet_from = since + cfg.pause_unacked_bound_s
                if quiet_from is not None and quiet_from > now:
                    quiet_from = None
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
        if span < need or span <= 0:
            return _OK
        busy = self._cum - anchor[1]
        share = busy / span
        if quiet_from is not None:
            allowed = 0.0
        else:
            allowed = self._allowed(now - cfg.window_s - cfg.grace_s, now)
            if allowed >= 1.0:
                self._acc, self._over_prev = 0.0, False
                return GpuVerdict(GpuAction.OK, share, allowed + cfg.margin)
        limit = allowed + cfg.margin
        dt = 0.0 if self._last_judged is None else min(now - self._last_judged, cfg.window_s)
        self._last_judged = now
        if share <= limit:
            self._acc = max(0.0, self._acc - cfg.drain_frac * dt)
            self._over_prev = False
            return GpuVerdict(GpuAction.OK, share, limit)
        if quiet_from is not None:
            return GpuVerdict(
                GpuAction.KILL, share, limit, reason="idle" if self._idle else "paused"
            )
        if self._over_prev:
            self._acc += dt
        self._over_prev = True
        if self._acc >= cfg.kill_after_s:
            return GpuVerdict(GpuAction.KILL, share, limit, reason="over_duty")
        cap = cfg.max_stop_frac * interval_s
        need_stop = busy / allowed - span if allowed > 0 else cap
        stop_s = min(max(need_stop, 0.0), cap)
        return GpuVerdict(GpuAction.GATE, share, limit, stop_s, "over_duty")
