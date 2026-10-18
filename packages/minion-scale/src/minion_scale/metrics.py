"""Client-side measurements of a scale run: latency histograms, status counts, event-loop lag.

No database imports here (the simulated executors use this module): database-side statistics are
in `dbstats.py`. Everything is plain data (`to_dict` / `merge`) so worker processes can ship their
recorder to the parent. Only op names, status codes and numbers are stored, never URLs or bodies.
"""

import asyncio
import contextlib
import time
from collections import Counter
from typing import Any

# Upper bounds in seconds; one overflow bucket beyond the last.
BUCKETS_S: tuple[float, ...] = (
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
)
LAG_INTERVAL_S = 0.05

# Recorded for a call that failed below HTTP (connect/timeout/protocol error).
TRANSPORT = 0


class Histogram:
    def __init__(self) -> None:
        self.counts = [0] * (len(BUCKETS_S) + 1)
        self.n = 0
        self.total_s = 0.0
        self.max_s = 0.0

    def observe(self, seconds: float) -> None:
        i = 0
        while i < len(BUCKETS_S) and seconds > BUCKETS_S[i]:
            i += 1
        self.counts[i] += 1
        self.n += 1
        self.total_s += seconds
        self.max_s = max(self.max_s, seconds)

    def quantile(self, q: float) -> float:
        """Upper bound of the bucket holding the q-quantile (the max for the overflow bucket)."""
        if self.n == 0:
            return 0.0
        rank = q * self.n
        seen = 0
        for i, c in enumerate(self.counts):
            seen += c
            if seen >= rank:
                return BUCKETS_S[i] if i < len(BUCKETS_S) else self.max_s
        return self.max_s

    def merge(self, other: "Histogram") -> None:
        self.counts = [a + b for a, b in zip(self.counts, other.counts, strict=True)]
        self.n += other.n
        self.total_s += other.total_s
        self.max_s = max(self.max_s, other.max_s)

    def to_dict(self) -> dict[str, Any]:
        return {
            "count": self.n,
            "mean_s": round(self.total_s / self.n, 6) if self.n else 0.0,
            "p50_s": self.quantile(0.50),
            "p90_s": self.quantile(0.90),
            "p99_s": self.quantile(0.99),
            "max_s": round(self.max_s, 6),
            "total_s": round(self.total_s, 6),
            "bucket_le_s": [*BUCKETS_S, None],  # None = overflow
            "bucket_counts": list(self.counts),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Histogram":
        h = cls()
        h.counts = list(d["bucket_counts"])
        h.n = int(d["count"])
        h.total_s = float(d["total_s"])
        h.max_s = float(d["max_s"])
        return h


class Recorder:
    """Per-op latency histograms, per-op status counts and named event counters."""

    def __init__(self) -> None:
        self.latency: dict[str, Histogram] = {}
        self.statuses: dict[str, Counter[int]] = {}
        self.events: Counter[str] = Counter()
        self.lag = Histogram()

    def record(self, op: str, seconds: float, status: int) -> None:
        self.latency.setdefault(op, Histogram()).observe(seconds)
        self.statuses.setdefault(op, Counter())[status] += 1

    def count(self, event: str, n: int = 1) -> None:
        self.events[event] += n

    def _status_total(self, pred: Any) -> int:
        return sum(n for c in self.statuses.values() for s, n in c.items() if pred(s))

    @property
    def conflicts_409(self) -> int:
        return self._status_total(lambda s: s == 409)

    @property
    def rate_limited_429(self) -> int:
        return self._status_total(lambda s: s == 429)

    @property
    def server_errors_5xx(self) -> int:
        """5xx other than 503: a 503 is the server shedding load on purpose (TASK-7.4.4)."""
        return self._status_total(lambda s: 500 <= s <= 599 and s != 503)

    @property
    def shed_503(self) -> int:
        return self._status_total(lambda s: s == 503)

    @property
    def transport_errors(self) -> int:
        return self._status_total(lambda s: s == TRANSPORT)

    def merge(self, other: "Recorder") -> None:
        for op, h in other.latency.items():
            self.latency.setdefault(op, Histogram()).merge(h)
        for op, c in other.statuses.items():
            self.statuses.setdefault(op, Counter()).update(c)
        self.events.update(other.events)
        self.lag.merge(other.lag)

    def to_dict(self) -> dict[str, Any]:
        return {
            "calls": {
                op: {
                    **h.to_dict(),
                    "statuses": {
                        str(s): n for s, n in sorted(self.statuses.get(op, Counter()).items())
                    },
                }
                for op, h in sorted(self.latency.items())
            },
            "conflicts_409": self.conflicts_409,
            "rate_limited_429": self.rate_limited_429,
            "server_errors_5xx": self.server_errors_5xx,
            "shed_503": self.shed_503,
            "transport_errors": self.transport_errors,
            "events": dict(sorted(self.events.items())),
            "event_loop_lag": self.lag.to_dict(),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Recorder":
        r = cls()
        for op, call in d["calls"].items():
            r.latency[op] = Histogram.from_dict(call)
            r.statuses[op] = Counter({int(s): n for s, n in call["statuses"].items()})
        r.events.update(d["events"])
        r.lag = Histogram.from_dict(d["event_loop_lag"])
        return r


def merge_dicts(parts: list[dict[str, Any]]) -> Recorder:
    total = Recorder()
    for p in parts:
        total.merge(Recorder.from_dict(p))
    return total


async def monitor_lag(
    rec: Recorder, stop: asyncio.Event, interval_s: float = LAG_INTERVAL_S
) -> None:
    """Record how late a fixed-interval sleep wakes up: the loop's scheduling delay."""
    while not stop.is_set():
        t0 = time.perf_counter()
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
        rec.lag.observe(max(0.0, time.perf_counter() - t0 - interval_s))
