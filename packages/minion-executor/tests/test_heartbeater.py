"""Heartbeater unit tests: interval validation and jitter."""

import uuid

import pytest
from minion_executor.heartbeat import Heartbeater


def _hb(interval: float) -> Heartbeater:
    return Heartbeater(
        None,  # type: ignore[arg-type]
        uuid.uuid4(),
        interval_s=interval,
        budget=None,
        total=1,
        initial_output_url="https://s3.test/out",
    )


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf"), 0.5, 3601.0])
def test_rejects_bad_intervals(bad: float) -> None:
    with pytest.raises(ValueError, match="heartbeat interval"):
        _hb(bad)


def test_jitter_never_goes_below_interval() -> None:
    hb = _hb(10.0)
    assert all(10.0 <= hb._jittered(10.0) <= 11.0 for _ in range(500))
