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
    src = default_source(None, want_call=True)
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


def test_mac_signals_call_active_is_a_bool() -> None:
    """Reads the CoreAudio / CoreMediaIO flags; opens no device, so no permission prompt.

    Real microphone/camera activation is NOT exercised here (not verifiable without starting them).
    """
    m = MacSignals()
    assert isinstance(m.read().call_active, bool)  # waits for the fresh read (<= 0.5 s)
    assert m._call is not None and m._call.settled()


def test_mac_signals_skips_the_call_probe_when_not_wanted() -> None:
    m = MacSignals()
    m.set_want_call_state(False)
    assert m.read().call_active is None
    assert m._call is None  # no thread, no CoreMediaIO load
    m.set_want_call_state(True)
    assert isinstance(m.read().call_active, bool)
    assert m._call is not None


def test_mac_signals_hung_call_probe_does_not_block_safety_read() -> None:
    import threading

    class Hang:
        def call_active(self) -> bool | None:
            threading.Event().wait(30)
            return True

    from minion_executor.capacity.signals import AsyncCallProbe

    m = MacSignals()
    m._call = AsyncCallProbe(Hang(), timeout_s=0.05)
    import time

    for i in range(3):
        t0 = time.monotonic()
        snap = m.read()
        took = time.monotonic() - t0
        assert snap.call_active is None and snap.thermal is not None
        if i > 0:  # a probe already hung is not waited for
            assert took < 0.04, took


def test_default_source_with_want_call_false_builds_no_probe() -> None:
    src = default_source(None, want_call=False)
    assert isinstance(src, MacSignals)
    assert src._call is None and src.read().call_active is None
    assert src._call is None
