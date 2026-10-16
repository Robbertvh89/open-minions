"""Live macOS signal reads (no mocks). Auto-skipped off macOS (conftest `macos` marker)."""

from __future__ import annotations

import os
import threading

import pytest
from minion_executor.capacity.signals import (
    MacSignals,
    MemPressure,
    SignalSnapshot,
    Thermal,
    default_source,
)
from minion_executor.worker.footprint import process_footprint_bytes

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


def test_mac_signals_reads_do_not_leak_core_foundation_objects() -> None:
    """5000 reads on a bare worker thread (as the runtime does) must not grow the footprint."""
    src = MacSignals()
    fp: dict[str, int] = {}

    def work() -> None:
        for _ in range(2000):  # warm up
            src.read()
        fp["before"] = process_footprint_bytes(os.getpid()) or 0
        for _ in range(5000):
            src.read()
        fp["after"] = process_footprint_bytes(os.getpid()) or 0

    t = threading.Thread(target=work)
    t.start()
    t.join()
    assert fp["before"] > 0
    assert fp["after"] - fp["before"] < 1_000_000
