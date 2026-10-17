from __future__ import annotations

import pytest
from minion_executor.runners.fg_probe import (
    ProbeError,
    calibrate_size,
    check_duration,
    decode_reply,
    run_schedule,
)


class Clock:
    def __init__(self) -> None:
        self.t = 100.0

    def now(self) -> float:
        return self.t

    def sleep(self, s: float) -> None:
        self.t += s


def test_schedule_on_time_frames() -> None:
    c = Clock()

    def step() -> None:
        c.t += 0.005

    r = run_schedule(2.0, step, clock=c.now, sleep=c.sleep)
    assert r.missed == 0
    assert len(r.latencies_ms) == 60
    assert all(v == pytest.approx(5.0) for v in r.latencies_ms)


def test_schedule_overrun_skips_and_counts_missed() -> None:
    c = Clock()
    calls = 0

    def step() -> None:
        nonlocal calls
        calls += 1
        c.t += 0.1 if calls == 3 else 0.005  # one 100 ms stall (~3 slots)

    r = run_schedule(1.0, step, clock=c.now, sleep=c.sleep)
    assert r.missed == 2
    assert max(r.latencies_ms) > 33.4
    assert len(r.latencies_ms) + r.missed == 30


def test_duration_bounds() -> None:
    for bad in (0.5, 601.0):
        with pytest.raises(ValueError):
            check_duration(bad)
    assert check_duration(1.0) == 1.0 and check_duration(600.0) == 600.0


def test_calibrate_size_converges() -> None:
    size = calibrate_size(lambda n: n * n / 1e6)  # 5 ms at n ~ 2236
    assert abs(size - 2236) <= 32


def test_decode_reply_validates() -> None:
    import struct

    assert decode_reply(struct.pack("<2d", 1.0, 2.5)) == [1.0, 2.5]
    for bad in (b"", b"123", struct.pack("<3d", 1, 2, 3)):
        with pytest.raises(ProbeError):
            decode_reply(bad, max_values=2)
