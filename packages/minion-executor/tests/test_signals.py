"""Signal mapping, NullSignals and the dev-only FileSignals (pure; runs on every platform)."""

from __future__ import annotations

import asyncio
import json
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
        default_source(_file(tmp_path, {}))


def test_file_signals_read(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    p = _file(
        tmp_path,
        {"hid_idle_s": 700, "on_ac": False, "battery_percent": 20, "thermal": "serious"},
    )
    s = default_source(p).read()
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
    assert isinstance(default_source(None), NullSignals)


async def test_hung_source_is_bounded_to_one_extra_thread_and_to_thread_still_works() -> None:
    import threading

    release = threading.Event()

    class Hung:
        calls = 0

        def read(self) -> SignalSnapshot:
            Hung.calls += 1
            release.wait(5)
            return NullSignals().read()

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
    for _ in range(100):  # the abandoned read finishes; the next read goes through again
        await asyncio.sleep(0.02)
        try:
            await read_with_timeout(src, 1.0)
            break
        except SignalError:
            continue
    else:
        pytest.fail("source never recovered")


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
