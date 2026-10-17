"""Client-side recorder (TASK-7.2 b)."""

import asyncio

from minion_scale.metrics import BUCKETS_S, TRANSPORT, Histogram, Recorder, merge_dicts, monitor_lag


def test_histogram_buckets_and_quantiles() -> None:
    h = Histogram()
    for _ in range(98):
        h.observe(0.004)  # <= 5 ms bucket
    h.observe(0.3)
    h.observe(99.0)  # overflow
    assert h.n == 100 and h.max_s == 99.0
    assert h.quantile(0.5) == 0.005
    assert h.quantile(0.99) == 0.5
    assert h.quantile(1.0) == 99.0  # overflow bucket reports the observed max
    assert sum(h.counts) == 100 and len(h.counts) == len(BUCKETS_S) + 1


def test_recorder_counts_statuses_and_round_trips_through_dicts() -> None:
    r = Recorder()
    r.record("acquire_assigned", 0.01, 200)
    r.record("acquire_assigned", 0.02, 429)
    r.record("complete", 0.03, 409)
    r.record("complete", 0.03, 503)
    r.record("heartbeat", 0.01, TRANSPORT)
    r.count("completed", 3)
    assert (r.conflicts_409, r.rate_limited_429, r.server_errors_5xx, r.transport_errors) == (
        1,
        1,
        1,
        1,
    )
    merged = merge_dicts([r.to_dict(), r.to_dict()])
    d = merged.to_dict()
    assert d["calls"]["complete"]["count"] == 4
    assert d["calls"]["complete"]["statuses"] == {"409": 2, "503": 2}
    assert d["conflicts_409"] == 2 and d["events"]["completed"] == 6


async def test_lag_monitor_stops_and_records() -> None:
    r = Recorder()
    stop = asyncio.Event()
    task = asyncio.create_task(monitor_lag(r, stop, 0.01))
    await asyncio.sleep(0.1)
    stop.set()
    await task
    assert r.lag.n >= 3
