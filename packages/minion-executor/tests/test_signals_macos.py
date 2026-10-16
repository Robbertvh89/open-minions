"""Live macOS signal reads (no mocks). Auto-skipped off macOS (conftest `macos` marker)."""

from __future__ import annotations

import pytest
from minion_executor.capacity.signals import (
    MacSignals,
    MemPressure,
    SignalSnapshot,
    Thermal,
    default_source,
)

pytestmark = pytest.mark.macos


def test_mac_signals_read_plausible_values() -> None:
    s = MacSignals().read()
    assert isinstance(s, SignalSnapshot)
    assert 0 <= s.hid_idle_s < 10 * 365 * 86400
    assert isinstance(s.on_ac, bool) and isinstance(s.low_power_mode, bool)
    assert s.battery_percent is None or 0 <= s.battery_percent <= 100
    assert s.thermal in set(Thermal)
    assert s.memory_pressure in set(MemPressure)
    assert s.taken_at.tzinfo is not None


def test_mac_signals_repeat_reads_and_default_source() -> None:
    src = default_source(None)
    assert isinstance(src, MacSignals)
    for _ in range(20):
        src.read()


def test_mac_signals_carry_no_identifying_data() -> None:
    s = MacSignals().read()
    assert not any(isinstance(v, str) and len(v) > 20 for v in vars(s).values())
