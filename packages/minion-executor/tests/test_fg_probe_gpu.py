"""Real MLX frame probe on the Apple GPU (opt in: `pytest -m gpu`; needs the mlx extra)."""

from __future__ import annotations

import statistics

import pytest
from minion_executor.runners.fg_probe import MlxFrameProbe

pytestmark = pytest.mark.gpu


def test_probe_calibrates_and_runs() -> None:
    pytest.importorskip("mlx.core")
    probe = MlxFrameProbe()
    size = probe.calibrate()
    res = probe.run(2.0, size)
    assert len(res.latencies_ms) + res.missed >= 50
    assert 2.0 <= statistics.median(res.latencies_ms) <= 15.0
