"""GPU duty-cycle pacing (SPEC §4.6, invariant 6). Pure: clock and wait are injected.

The duty can change while a batch runs (`set_duty`, driven by the governor). The wait is
interruptible: it returns True when something (a control message) cut it short, and the pacer then
re-derives how much sleep is still owed under the possibly new duty.
"""

from __future__ import annotations

from collections.abc import Callable

_MAX_CREDIT_S = 0.5  # how much oversleep a later step may repay (bounds the catch-up burst)
QUANTUM_S = 0.05  # busy time accumulated across steps before the pacer sleeps (duty < 1)


def _check(duty: float) -> float:
    if not 0 < duty <= 1:  # also rejects NaN
        raise ValueError("duty must be in (0, 1]")
    return duty


class DutyCyclePacer:
    """Quantum pacing. Busy time accumulates across steps; once it reaches `quantum_s` (or the duty
    was just lowered) the step ends a quantum: the pacer waits `busy * (1 - d) / d`, so busy time
    is a fraction `d` of wall time, and the accumulator resets. Between quanta steps cost no sync
    and no sleep, so the CPU can build the next graph while the GPU computes (a sync every step
    would serialise them and under-use the granted duty). `d == 1` never waits.

    Callers ask `quantum_due()` *before* syncing the GPU: when True they drain async work, and the
    following `step_done()` is then guaranteed to sleep (the sync time counts as busy).

    Oversleep (a late wake-up) is credited against the next wait, so the measured share
    converges on `d` even when the scheduler is slow.

    `busy_s` / `wall_s` accumulate since `reset()`; time reported through `exclude()` (a pause)
    counts as neither."""

    def __init__(
        self,
        duty: float,
        clock: Callable[[], float],
        wait: Callable[[float], bool | None],
        quantum_s: float = QUANTUM_S,
    ) -> None:
        self._duty = _check(duty)
        self._quantum = max(0.0, quantum_s)
        self._acc = 0.0  # busy seconds since the last sleep
        self._force = False  # duty just dropped: the next step drains and sleeps at once
        self._clock = clock
        self._wait = wait
        self._start = self._last = clock()
        self._busy = 0.0
        self._excluded = 0.0
        self._credit = 0.0  # <= 0: sleep already taken beyond what was owed

    @property
    def duty(self) -> float:
        return self._duty

    @property
    def busy_s(self) -> float:
        return self._busy

    @property
    def wall_s(self) -> float:
        return max(0.0, self._clock() - self._start - self._excluded)

    def set_duty(self, duty: float) -> None:
        _check(duty)
        if duty != self._duty:
            self._force = self._force or duty < self._duty
            self._duty = duty
            self._credit = 0.0  # the credit was earned under the old duty

    def reset(self) -> None:
        """Mark the start of a run: the next step is measured from now."""
        self._start = self._last = self._clock()
        self._busy = self._excluded = self._credit = self._acc = 0.0
        self._force = False

    def quantum_due(self) -> bool:
        """True if the step in progress will end a quantum (so it must sync the GPU first)."""
        if self._duty >= 1.0:
            return False
        return self._force or self._acc + max(0.0, self._clock() - self._last) >= self._quantum

    def exclude(self, seconds: float) -> None:
        """Remove `seconds` just spent paused from both busy and wall time."""
        seconds = max(0.0, seconds)
        self._last += seconds
        self._excluded += seconds

    def flush(self) -> float:
        """End the run: pay the sleep owed for the partial quantum (the batch's results are in, so
        the GPU is idle). Without it a stream of short batches, each under one quantum, would
        never be paced."""
        if self._duty >= 1.0 or self._acc + (self._clock() - self._last) <= 0.0:
            return 0.0
        self._force = True
        return self.step_done()

    def step_done(self) -> float:
        """Pace after a finished step; returns the seconds slept (pauses excluded)."""
        step = max(0.0, self._clock() - self._last)
        self._busy += step
        self._acc += step
        if self._duty >= 1.0:
            self._acc = 0.0
            self._force = False
            self._credit = 0.0
            self._last = self._clock()
            return 0.0
        if not self._force and self._acc < self._quantum:
            self._last = self._clock()  # mid-quantum: no sync, no sleep
            return 0.0
        self._force = False
        busy = self._acc
        slept = 0.0
        owed = 0.0
        while self._duty < 1.0:  # set_duty() mid-wait changes duty and clears the credit
            owed = busy * (1.0 - self._duty) / self._duty + self._credit
            left = owed - slept
            if left <= 0:
                break
            t0, ex0 = self._clock(), self._excluded
            interrupted = self._wait(left)
            slept += max(0.0, self._clock() - t0 - (self._excluded - ex0))
            if not interrupted:
                break
        else:
            owed = slept = 0.0  # duty is 1: nothing owed, no credit to carry
        self._credit = max(-_MAX_CREDIT_S, min(0.0, owed - slept))
        self._acc = 0.0
        self._force = False  # a duty change during the wait was already paid for by it
        self._last = self._clock()
        return slept
