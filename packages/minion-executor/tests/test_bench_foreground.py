from __future__ import annotations

import math

import pytest
from minion_common.enums import CapacityProfile as P
from minion_executor.bench_foreground import (
    FRAME_PERIOD_MS,
    FrameStats,
    PhaseMeasure,
    aggregate,
    failures,
    frame_stats,
    nf7_proxy_verdict,
    percentile,
)


def pm(p95: float, miss: float = 0.0, launch: float | None = None) -> PhaseMeasure:
    return PhaseMeasure(FrameStats(p95 / 2, p95, miss, 100), launch)


def test_percentile_small_samples() -> None:
    assert percentile([], 95) == 0.0
    assert percentile([4.0], 95) == 4.0
    assert percentile([1.0, 3.0], 50) == 2.0
    assert percentile([1.0, 2.0, 3.0, 4.0, 5.0], 95) == pytest.approx(4.8)


def test_frame_stats_counts_late_and_skipped() -> None:
    fs = frame_stats([5.0, 10.0, FRAME_PERIOD_MS + 1, FRAME_PERIOD_MS], missed=2)
    assert fs.frames == 6
    assert fs.miss_rate == pytest.approx(3 / 6)
    with pytest.raises(ValueError):
        frame_stats([], -1)
    assert frame_stats([], 0).miss_rate == 0.0


def test_ratio_threshold_edges() -> None:
    assert nf7_proxy_verdict(pm(10.0), pm(11.0)).frame_ok
    assert not nf7_proxy_verdict(pm(10.0), pm(11.01)).frame_ok


def test_miss_delta_edges() -> None:
    assert nf7_proxy_verdict(pm(10, 0.02), pm(10, 0.03)).miss_ok
    assert not nf7_proxy_verdict(pm(10, 0.02), pm(10, 0.0301)).miss_ok


def test_launch_edges_and_partial() -> None:
    assert nf7_proxy_verdict(pm(10, 0, 1.0), pm(10, 0, 1.1)).launch_ok is True
    v = nf7_proxy_verdict(pm(10, 0, 1.0), pm(10, 0, 1.11))
    assert v.launch_ok is False and not v.passed
    p = nf7_proxy_verdict(pm(10), pm(10, 0, 1.0))
    assert p.partial and p.launch_p50_ratio is None and p.passed


def test_zero_baseline_guards() -> None:
    assert nf7_proxy_verdict(pm(0.0), pm(0.0)).frame_p95_ratio == 1.0
    assert math.isinf(nf7_proxy_verdict(pm(0.0), pm(1.0)).frame_p95_ratio)
    assert not nf7_proxy_verdict(pm(0.0), pm(1.0)).passed


def _rows(profile: P, loaded: list[float]) -> list[tuple[str, P, PhaseMeasure, PhaseMeasure]]:
    return [("m", profile, pm(10.0), pm(x)) for x in loaded]


def test_median_over_three_repeats() -> None:
    ok = aggregate(_rows(P.LIGHT, [10.0, 10.5, 30.0]))
    assert ok[("m", P.LIGHT)].passed
    bad = aggregate(_rows(P.LIGHT, [10.0, 20.0, 30.0]))
    assert not bad[("m", P.LIGHT)].passed
    assert failures(bad) == [("m", P.LIGHT)]


def test_full_never_judged() -> None:
    v = aggregate(_rows(P.FULL, [50.0, 50.0, 50.0]))
    assert not v[("m", P.FULL)].passed
    assert failures(v) == []
