"""GPU duty-cycle pacing (SPEC §4.6, invariant 6). Pure: clock and wait are injected.

The duty can change while a batch runs (`set_duty`, driven by the governor). The wait is
interruptible: it returns True when something (a control message) cut it short, and the pacer then
re-derives how much sleep is still owed under the possibly new duty.
"""

from __future__ import annotations

from collections.abc import Callable

_MAX_CREDIT_S = 0.5  # how much oversleep a later step may repay (bounds the catch-up burst)


def _check(duty: float) -> float:
    if not 0 < duty <= 1:  # also rejects NaN
        raise ValueError("duty must be in (0, 1]")
    return duty


class DutyCyclePacer:
    """After a step that took `t` seconds, wait `t * (1 - d) / d` so busy time is a fraction `d`
    of wall time. `d == 1` never waits. Oversleep (a late wake-up) is credited against the next
    wait, so the measured share converges on `d` even when the scheduler is slow.

    `busy_s` / `wall_s` accumulate since `reset()`; time reported through `exclude()` (a pause)
    counts as neither."""

    def __init__(
        self,
        duty: float,
        clock: Callable[[], float],
        wait: Callable[[float], bool | None],
    ) -> None:
        self._duty = _check(duty)
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
            self._duty = duty
            self._credit = 0.0  # the credit was earned under the old duty

    def reset(self) -> None:
        """Mark the start of a run: the next step is measured from now."""
        self._start = self._last = self._clock()
        self._busy = self._excluded = self._credit = 0.0

    def exclude(self, seconds: float) -> None:
        """Remove `seconds` just spent paused from both busy and wall time."""
        seconds = max(0.0, seconds)
        self._last += seconds
        self._excluded += seconds

    def step_done(self) -> float:
        """Pace after a finished step; returns the seconds slept (pauses excluded)."""
        busy = max(0.0, self._clock() - self._last)
        self._busy += busy
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
        self._last = self._clock()
        return slept
