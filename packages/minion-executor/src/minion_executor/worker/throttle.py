"""Static GPU duty-cycle pacing (SPEC §4.6, invariant 6). Pure: clock and sleep are injected."""

from __future__ import annotations

from collections.abc import Callable

MAX_PACE_SLEEP_S = 60.0


class DutyCyclePacer:
    """After a step that took `t` seconds, sleep `t * (1 - d) / d` so busy time is a fraction `d`
    of wall time. `d == 1` never sleeps."""

    def __init__(
        self,
        duty: float,
        clock: Callable[[], float],
        sleep: Callable[[float], None],
        *,
        max_sleep_s: float = MAX_PACE_SLEEP_S,
    ) -> None:
        if not 0 < duty <= 1:
            raise ValueError("duty must be in (0, 1]")
        self._duty = duty
        self._clock = clock
        self._sleep = sleep
        self._max = max_sleep_s
        self._last = clock()

    def reset(self) -> None:
        """Mark the start of a run: the next step is measured from now."""
        self._last = self._clock()

    def step_done(self) -> float:
        """Pace after a finished step; returns the seconds slept."""
        busy = max(0.0, self._clock() - self._last)
        pause = 0.0
        if self._duty < 1.0:
            pause = min(busy * (1.0 - self._duty) / self._duty, self._max)
            if pause > 0:
                self._sleep(pause)
        self._last = self._clock()
        return pause
