"""In-memory minimum-interval limiter for executor endpoints (NF-5, TASK-6.2, plan D4).

Soft, per-replica state: a counter here is never coordination state. Correctness (one owner per
attempt, stale writes rejected) stays in the database (CLAUDE.md invariants 2 and 3); this only
stops a misbehaving executor from hammering register/acquire/presence. With N replicas an executor
may be admitted up to N times per interval; per-IP limits belong at the proxy.
"""

import math
import time
import uuid
from collections import OrderedDict
from collections.abc import Callable

MAX_ENTRIES = 10_000


class MinIntervalLimiter:
    """At most one admitted call per `(executor_id, op)` within `min_interval_s`.

    Monotonic clock, bounded LRU (the least recently seen key is dropped, which at worst admits
    one extra call). Single event loop: no await between read and write, so no lock is needed.
    """

    def __init__(
        self,
        intervals: dict[str, float],
        *,
        clock: Callable[[], float] = time.monotonic,
        max_entries: int = MAX_ENTRIES,
    ) -> None:
        self._intervals = intervals
        self._clock = clock
        self._max = max_entries
        self._last: OrderedDict[tuple[uuid.UUID, str], float] = OrderedDict()

    def check(self, executor_id: uuid.UUID, op: str) -> float | None:
        """Admit the call (None) or return the seconds to wait. Only admitted calls are recorded,
        so a rejected retry does not extend the wait."""
        interval = self._intervals.get(op, 0.0)
        if interval <= 0:
            return None
        now = self._clock()
        key = (executor_id, op)
        last = self._last.get(key)
        if last is not None and now - last < interval:
            return interval - (now - last)
        self._last[key] = now
        self._last.move_to_end(key)
        while len(self._last) > self._max:
            self._last.popitem(last=False)
        return None


def retry_after_header(wait_s: float) -> str:
    """Whole seconds, at least 1 (the executor honours it as a lower bound)."""
    return str(max(1, math.ceil(wait_s)))
