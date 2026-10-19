"""Signal mapping, NullSignals and the dev-only FileSignals (pure; runs on every platform)."""

from __future__ import annotations

import asyncio
import json
import threading
import time
from pathlib import Path

import pytest
from minion_executor.capacity.signals import (
    FileSignals,
    MemPressure,
    NullSignals,
    SignalError,
    SignalSnapshot,
    Thermal,
    default_source,
    power_from_iops,
    pressure_from_sysctl,
    read_memory_pressure,
    read_with_timeout,
    thermal_from_ns,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0, Thermal.NOMINAL),
        (1, Thermal.FAIR),
        (2, Thermal.SERIOUS),
        (3, Thermal.CRITICAL),
        (99, Thermal.CRITICAL),
        (-1, Thermal.CRITICAL),
    ],
)
def test_thermal_from_ns(value: int, expected: Thermal) -> None:
    assert thermal_from_ns(value) is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (1, MemPressure.NORMAL),
        (2, MemPressure.WARNING),
        (4, MemPressure.CRITICAL),
        (0, MemPressure.CRITICAL),
        (3, MemPressure.CRITICAL),
    ],
)
def test_pressure_from_sysctl(value: int, expected: MemPressure) -> None:
    assert pressure_from_sysctl(value) is expected


BATT = {"Type": "InternalBattery", "Is Present": True, "Current Capacity": 42, "Max Capacity": 100}


@pytest.mark.parametrize(
    ("providing", "descs", "expected"),
    [
        ("AC Power", [BATT], (True, 42)),
        ("Battery Power", [BATT], (False, 42)),
        ("UPS Power", [BATT], (False, 42)),  # not AC: conservative
        ("AC Power", [], (True, None)),  # desktop
        ("AC Power", [{**BATT, "Current Capacity": 50, "Max Capacity": 200}], (True, 25)),
        ("AC Power", [{**BATT, "Max Capacity": 0}], (True, None)),
        ("AC Power", [{**BATT, "Current Capacity": 150}], (True, 100)),
        ("AC Power", [{**BATT, "Is Present": False}], (True, None)),
        ("AC Power", [{"Type": "UPS", "Current Capacity": 10, "Max Capacity": 100}], (True, None)),
        ("AC Power", [{**BATT, "Current Capacity": True}], (True, None)),
        ("", [BATT], (False, 42)),
    ],
)
def test_power_from_iops(
    providing: str, descs: list[dict[str, object]], expected: tuple[bool, int | None]
) -> None:
    assert power_from_iops(providing, descs) == expected


def test_null_signals_are_quiet() -> None:
    s = NullSignals().read()
    assert s.on_ac and not s.low_power_mode
    assert s.thermal is Thermal.NOMINAL and s.memory_pressure is MemPressure.NORMAL
    assert s.hid_idle_s == 0.0 and s.taken_at.tzinfo is not None


def _file(tmp_path: Path, payload: object, mode: int = 0o600) -> Path:
    d = tmp_path / "sig"
    d.mkdir(exist_ok=True)
    d.chmod(0o700)
    p = d / "signals.json"
    p.write_text(payload if isinstance(payload, str) else json.dumps(payload))
    p.chmod(mode)
    return p


def test_file_signals_need_dev(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MINION_EXECUTOR_DEV", raising=False)
    with pytest.raises(SignalError, match="DEV"):
        FileSignals(_file(tmp_path, {}))
    with pytest.raises(SignalError, match="DEV"):
        default_source(_file(tmp_path, {}), want_call=True)


def test_file_signals_read(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    p = _file(
        tmp_path,
        {"hid_idle_s": 700, "on_ac": False, "battery_percent": 20, "thermal": "serious"},
    )
    s = default_source(p, want_call=True).read()
    assert (s.hid_idle_s, s.on_ac, s.battery_percent, s.thermal) == (
        700.0,
        False,
        20,
        Thermal.SERIOUS,
    )
    assert s.memory_pressure is MemPressure.NORMAL
    p.write_text(json.dumps({"memory_pressure": "critical"}))  # re-read on every call
    assert FileSignals(p).read().memory_pressure is MemPressure.CRITICAL


@pytest.mark.parametrize(
    "payload",
    [
        "not json",
        {"surprise": 1},
        {"thermal": "lava"},
        {"battery_percent": 101},
        {"hid_idle_s": -1},
        {"hid_idle_s": "NaN"},
    ],
)
def test_file_signals_invalid_is_signal_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, payload: object
) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    with pytest.raises(SignalError, match="invalid signals file"):
        FileSignals(_file(tmp_path, payload)).read()


def test_file_signals_unsafe_or_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    p = _file(tmp_path, {}, mode=0o666)
    with pytest.raises(SignalError, match="writable"):
        FileSignals(p).read()
    p.chmod(0o600)
    link = p.parent / "link.json"
    link.symlink_to(p)
    with pytest.raises(SignalError):
        FileSignals(link).read()
    p.unlink()
    with pytest.raises(SignalError, match="does not exist"):
        FileSignals(p).read()
    p.write_text("x" * 5000)
    p.chmod(0o600)
    with pytest.raises(SignalError, match="larger"):
        FileSignals(p).read()


def test_file_signals_error_does_not_echo_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    with pytest.raises(SignalError) as ei:
        FileSignals(_file(tmp_path, {"thermal": "SECRET-VALUE"})).read()
    assert "SECRET-VALUE" not in str(ei.value)


async def test_read_with_timeout_returns_and_times_out() -> None:
    snap = await read_with_timeout(NullSignals(), 1.0)
    assert isinstance(snap, SignalSnapshot)

    class Hung:
        def read(self) -> SignalSnapshot:
            time.sleep(0.5)
            return NullSignals().read()

    t0 = time.monotonic()
    with pytest.raises(SignalError, match="timed out"):
        await read_with_timeout(Hung(), 0.05)
    assert time.monotonic() - t0 < 0.4
    await asyncio.sleep(0.5)  # let the abandoned thread finish


def test_default_source_off_mac_is_null(monkeypatch: pytest.MonkeyPatch) -> None:
    import types

    import minion_executor.capacity.signals as sg

    monkeypatch.setattr(sg, "sys", types.SimpleNamespace(platform="linux"))
    assert isinstance(default_source(None, want_call=True), NullSignals)


async def test_hung_source_is_bounded_to_one_extra_thread_and_to_thread_still_works() -> None:
    import threading

    release = threading.Event()
    finished = threading.Event()

    class Hung:
        calls = 0

        def read(self) -> SignalSnapshot:
            Hung.calls += 1
            release.wait(5)
            try:
                return NullSignals().read()
            finally:
                finished.set()

    src = Hung()
    before = threading.active_count()
    with pytest.raises(SignalError, match="timed out"):
        await read_with_timeout(src, 0.05)
    for _ in range(10):
        with pytest.raises(SignalError, match="previous read still hung"):
            await read_with_timeout(src, 0.05)
    assert Hung.calls == 1
    assert threading.active_count() <= before + 1
    assert await asyncio.to_thread(lambda: 7) == 7  # the default pool is untouched
    release.set()
    assert await asyncio.to_thread(finished.wait, 5)  # the abandoned read has finished
    import concurrent.futures

    from minion_executor.capacity import signals as sig_mod

    fut = sig_mod._inflight[src]
    await asyncio.to_thread(concurrent.futures.wait, [fut], 5)  # the in-flight read has resolved
    assert fut.done()
    assert (await read_with_timeout(src, 1.0)) is not None


async def test_read_errors_propagate_and_signal_threads_are_daemons() -> None:
    import threading

    seen: list[bool] = []

    class Boom:
        def read(self) -> SignalSnapshot:
            seen.append(threading.current_thread().daemon)
            raise SignalError("nope")

    with pytest.raises(SignalError, match="nope"):
        await read_with_timeout(Boom(), 1.0)
    assert seen == [True]


async def test_unhashable_or_non_weakrefable_source_raises_signal_error_before_thread() -> None:
    import threading

    class Slotted:
        __slots__ = ()

        def read(self) -> SignalSnapshot:
            raise AssertionError("must not run")

    before = threading.active_count()
    with pytest.raises(SignalError, match="weak"):
        await read_with_timeout(Slotted())
    assert threading.active_count() <= before


def test_call_from_flags() -> None:
    from minion_executor.capacity.signals import call_from_flags

    assert call_from_flags(False, []) is False
    assert call_from_flags(False, [False, False]) is False
    assert call_from_flags(True, [None]) is True
    assert call_from_flags(False, [True]) is True
    assert call_from_flags(None, [False]) is None
    assert call_from_flags(False, [None]) is None


def test_file_signals_call_active_optional(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    p = tmp_path / "s.json"
    p.write_text("{}")
    p.chmod(0o600)
    assert FileSignals(p).read().call_active is None
    p.write_text('{"call_active": true}')
    assert FileSignals(p).read().call_active is True


def test_null_signals_call_unknown() -> None:
    assert NullSignals().read().call_active is None


class _Reader:
    def __init__(self, gate: threading.Event | None = None, value: bool | None = True) -> None:
        self.gate, self.value, self.calls = gate, value, 0

    def call_active(self) -> bool | None:
        self.calls += 1
        if self.gate is not None:
            self.gate.wait(30)
        return self.value


def test_async_call_probe_hung_reader_never_blocks_and_starts_one_thread() -> None:
    from minion_executor.capacity.signals import AsyncCallProbe

    gate = threading.Event()
    r = _Reader(gate)
    t = [0.0]
    p = AsyncCallProbe(r, timeout_s=0.5, clock=lambda: t[0])
    try:
        for _ in range(5):
            t[0] += 1.0
            assert p.value() is None
        assert r.calls == 1  # one read in flight, nothing piles up
        assert p.settled() is False
    finally:
        gate.set()


def test_async_call_probe_change_is_visible_on_the_very_next_call() -> None:
    from minion_executor.capacity.signals import AsyncCallProbe

    r = _Reader(value=False)
    p = AsyncCallProbe(r)
    assert p.value() is False  # waits for the fresh read, not "the previous value"
    r.value = True
    assert p.value() is True
    r.value = False
    assert p.value() is False
    assert p.read() == (False, True)


def test_async_call_probe_stale_value_is_unknown() -> None:
    from minion_executor.capacity.signals import AsyncCallProbe

    r = _Reader(value=True)
    t = [0.0]
    p = AsyncCallProbe(r, timeout_s=0.05, clock=lambda: t[0])
    assert p.value() is True
    r.gate = threading.Event()  # the next read hangs
    t[0] += 100.0
    assert p.value() is None  # never the stale True
    r.gate.set()


def test_async_call_probe_failing_reader_is_unknown() -> None:
    from minion_executor.capacity.signals import AsyncCallProbe

    class Boom:
        def call_active(self) -> bool | None:
            raise OSError("x")

    p = AsyncCallProbe(Boom())
    assert p.value() is None
    assert p.settled() is True


def test_call_probe_none_when_frameworks_fail_to_load(monkeypatch: pytest.MonkeyPatch) -> None:
    from minion_executor.capacity import signals

    def boom(*a: object, **k: object) -> object:
        raise OSError("no framework")

    monkeypatch.setattr("ctypes.CDLL", boom)
    assert signals._CallProbe().call_active() is None


def test_call_probe_none_when_calls_return_nonzero() -> None:
    from minion_executor.capacity import signals

    class Fn:
        def __call__(self, *a: object) -> int:
            return -1

    class Lib:
        AudioObjectGetPropertyData = Fn()
        AudioObjectGetPropertyDataSize = Fn()
        CMIOObjectGetPropertyData = Fn()
        CMIOObjectGetPropertyDataSize = Fn()

    probe = signals._CallProbe.__new__(signals._CallProbe)
    probe._audio = Lib()
    probe._cmio = Lib()
    assert probe.call_active() is None


def test_startup_read_times_out_on_a_hung_source() -> None:
    from minion_executor.capacity.signals import SignalError, _startup_read

    gate = threading.Event()

    class Hung:
        def read(self) -> SignalSnapshot:
            gate.wait(30)
            raise AssertionError

    try:
        with pytest.raises(SignalError, match="timed out"):
            _startup_read(Hung(), 0.05)
    finally:
        gate.set()


def test_async_call_probe_recovers_after_a_hung_read_finishes() -> None:
    from minion_executor.capacity.signals import AsyncCallProbe

    gate = threading.Event()
    r = _Reader(gate, value=True)
    p = AsyncCallProbe(r, timeout_s=0.1)
    assert p.value() is None  # hung
    gate.set()  # the hung read finishes
    deadline = time.monotonic() + 5
    while not p.settled() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert p.value() is True  # a new read started and returned its value
    assert r.calls == 2


def test_read_memory_pressure_from_the_fake_signals_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    p = _file(tmp_path, {"memory_pressure": "warning"})
    assert read_memory_pressure(p) is MemPressure.WARNING


def test_read_memory_pressure_unreadable_or_invalid_file_is_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    assert read_memory_pressure(tmp_path / "missing.json") is None
    assert read_memory_pressure(_file(tmp_path, "{not json")) is None


def test_read_memory_pressure_is_none_off_darwin(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.platform", "linux")
    assert read_memory_pressure() is None
