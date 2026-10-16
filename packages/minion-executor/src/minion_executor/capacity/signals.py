"""Host signals for the capacity governor (SPEC FR-52/53, ADR-0006 D2).

`MacSignals` reads HID idle time, power source and battery, Low Power Mode, thermal state and
memory pressure. All reads are passive: they never create a power assertion, so the Mac can still
sleep (invariant 6, ADR-0006 D14). pyobjc is imported lazily inside `MacSignals`, so importing this
module works everywhere (Linux CI). Nothing here logs or returns identifying data (the IOKit power
source description carries a battery serial number; only the percentage and the source type leave
`power_from_iops`).

Reads are synchronous and fast (microseconds); the async runtime wraps them in
`read_with_timeout` so a wedged framework call can never stall the control loop.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import ctypes
import ctypes.util
import os
import sys
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from minion_executor.capacity.safefile import SafeFileError, read_private_file

MAX_SIGNAL_FILE_BYTES = 4096
DEFAULT_READ_TIMEOUT_S = 1.5


class Thermal(StrEnum):
    NOMINAL = "nominal"
    FAIR = "fair"
    SERIOUS = "serious"
    CRITICAL = "critical"


class MemPressure(StrEnum):
    NORMAL = "normal"
    WARNING = "warning"
    CRITICAL = "critical"


class SignalError(RuntimeError):
    """A signal could not be read. The governor counts consecutive failures (FR-53)."""


@dataclass(frozen=True)
class SignalSnapshot:
    hid_idle_s: float
    on_ac: bool
    battery_percent: int | None  # None: no battery (desktop) or unknown
    low_power_mode: bool
    thermal: Thermal
    memory_pressure: MemPressure
    taken_at: datetime


class SignalSource(Protocol):
    def read(self) -> SignalSnapshot: ...


# --- pure mapping helpers (unit-tested on every platform) -------------------------------------


def thermal_from_ns(value: int) -> Thermal:
    """NSProcessInfoThermalState: 0 nominal, 1 fair, 2 serious, 3 critical. Unknown -> critical."""
    return {0: Thermal.NOMINAL, 1: Thermal.FAIR, 2: Thermal.SERIOUS}.get(value, Thermal.CRITICAL)


def pressure_from_sysctl(level: int) -> MemPressure:
    """kern.memorystatus_vm_pressure_level: 1 normal, 2 warn, 4 critical. Unknown -> critical."""
    return {1: MemPressure.NORMAL, 2: MemPressure.WARNING}.get(level, MemPressure.CRITICAL)


def power_from_iops(
    providing_type: str, descriptions: Sequence[Mapping[str, Any]]
) -> tuple[bool, int | None]:
    """(on_ac, battery_percent) from IOKit's providing source type and source descriptions.

    Only `AC Power` counts as AC: a UPS or an unknown source is treated as battery (conservative,
    invariant 6). The percentage comes from the first internal battery that reports a capacity.
    """
    on_ac = providing_type == "AC Power"
    percent: int | None = None
    for d in descriptions:
        if d.get("Type") != "InternalBattery" or d.get("Is Present") is False:
            continue
        cur, mx = d.get("Current Capacity"), d.get("Max Capacity")
        if isinstance(cur, int) and isinstance(mx, int) and not isinstance(cur, bool) and mx > 0:
            percent = max(0, min(100, round(100 * cur / mx)))
            break
    return on_ac, percent


# --- sources ----------------------------------------------------------------------------------


class NullSignals:
    """Off macOS (Linux CI, dev): AC power, nominal, user active. Never pauses, never idles."""

    def read(self) -> SignalSnapshot:
        return SignalSnapshot(0.0, True, None, False, Thermal.NOMINAL, MemPressure.NORMAL, _now())


_CF_RETAINED: dict[str, Any] = {"retval": {"already_cfretained": True}}


class MacSignals:
    """Real macOS readers. Raises SignalError at construction if a framework is unusable."""

    _objc: Any
    _quartz: Any
    _foundation: Any
    _iokit: dict[str, Any]
    _libc: Any

    def __init__(self) -> None:
        if sys.platform != "darwin":
            raise SignalError("MacSignals needs macOS")
        try:
            import Foundation
            import objc
            import Quartz

            bundle = objc.loadBundle(
                "IOKit", {}, bundle_path="/System/Library/Frameworks/IOKit.framework"
            )
            iokit: dict[str, Any] = {}
            objc.loadBundleFunctions(
                bundle,
                iokit,
                [
                    # "Copy" functions return +1 CF objects: tell pyobjc to adopt (not retain) them
                    ("IOPSCopyPowerSourcesInfo", b"@", "", _CF_RETAINED),
                    ("IOPSCopyPowerSourcesList", b"@@", "", _CF_RETAINED),
                    ("IOPSGetProvidingPowerSourceType", b"@@"),
                    ("IOPSGetPowerSourceDescription", b"@@@"),
                ],
            )
            self._libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.dylib", use_errno=True)
        except (ImportError, AttributeError, OSError, ValueError) as exc:
            raise SignalError(
                f"macOS signal frameworks unavailable: {type(exc).__name__}"
            ) from None
        if "IOPSCopyPowerSourcesInfo" not in iokit:
            raise SignalError("IOKit power source functions unavailable")
        self._objc = objc
        self._quartz = Quartz
        self._foundation = Foundation
        self._iokit = iokit

    def _hid_idle_s(self) -> float:
        q = self._quartz
        v = q.CGEventSourceSecondsSinceLastEventType(
            q.kCGEventSourceStateHIDSystemState, q.kCGAnyInputEventType
        )
        if not isinstance(v, float) or v < 0 or v != v:
            raise SignalError("HID idle time unreadable")
        return v

    def _power(self) -> tuple[bool, int | None]:
        f = self._iokit
        info = f["IOPSCopyPowerSourcesInfo"]()
        if info is None:
            raise SignalError("power source info unreadable")
        providing = f["IOPSGetProvidingPowerSourceType"](info)
        if not isinstance(providing, str):
            raise SignalError("power source type unreadable")
        descs: list[dict[str, Any]] = []
        for ps in f["IOPSCopyPowerSourcesList"](info) or []:
            d = f["IOPSGetPowerSourceDescription"](info, ps)
            if d is not None:
                # copy only what the pure helper reads, so the serial number never travels
                descs.append(
                    {
                        k: d.get(k)
                        for k in ("Type", "Is Present", "Current Capacity", "Max Capacity")
                    }
                )
        return power_from_iops(providing, descs)

    def _memory_pressure(self) -> MemPressure:
        val = ctypes.c_int(0)
        size = ctypes.c_size_t(ctypes.sizeof(val))
        rc = self._libc.sysctlbyname(
            b"kern.memorystatus_vm_pressure_level",
            ctypes.byref(val),
            ctypes.byref(size),
            None,
            ctypes.c_size_t(0),
        )
        if rc != 0:
            raise SignalError("memory pressure unreadable")
        return pressure_from_sysctl(val.value)

    def read(self) -> SignalSnapshot:
        try:
            # reads run on a bare worker thread with no ambient pool: drain autoreleased objects
            with self._objc.autorelease_pool():
                info = self._foundation.NSProcessInfo.processInfo()
                on_ac, percent = self._power()
                return SignalSnapshot(
                    hid_idle_s=self._hid_idle_s(),
                    on_ac=on_ac,
                    battery_percent=percent,
                    low_power_mode=bool(info.isLowPowerModeEnabled()),
                    thermal=thermal_from_ns(int(info.thermalState())),
                    memory_pressure=self._memory_pressure(),
                    taken_at=_now(),
                )
        except SignalError:
            raise
        except Exception as exc:  # any pyobjc/ctypes failure is "unreadable", never a crash
            raise SignalError(f"signal read failed: {type(exc).__name__}") from None


class _FileSnapshot(BaseModel):
    """Strict schema of the dev signal file. Every field defaults to the quiet value."""

    model_config = ConfigDict(extra="forbid")

    hid_idle_s: float = Field(default=0.0, ge=0, le=1e9, allow_inf_nan=False)
    on_ac: bool = True
    battery_percent: int | None = Field(default=None, ge=0, le=100)
    low_power_mode: bool = False
    thermal: Thermal = Thermal.NOMINAL
    memory_pressure: MemPressure = MemPressure.NORMAL


class FileSignals:
    """Dev/test only: a JSON snapshot read on every call (ADR-0006 D2). Needs MINION_EXECUTOR_DEV=1.

    The file must be ours and not writable by others, and not a symlink. A missing or invalid file
    is a SignalError, which is how tests inject `signals_unavailable`.
    """

    def __init__(self, path: Path) -> None:
        if os.environ.get("MINION_EXECUTOR_DEV") != "1":
            raise SignalError(
                "MINION_EXECUTOR_FAKE_SIGNALS is only allowed with MINION_EXECUTOR_DEV=1"
            )
        self._path = path

    def read(self) -> SignalSnapshot:
        try:
            data = read_private_file(self._path, "signals file", MAX_SIGNAL_FILE_BYTES)
        except SafeFileError as exc:
            raise SignalError(str(exc)) from None
        if data is None:
            raise SignalError(f"signals file {self._path} does not exist")
        try:
            snap = _FileSnapshot.model_validate_json(data)
        except ValidationError as exc:
            fields = ", ".join(".".join(str(p) for p in e["loc"]) for e in exc.errors())
            raise SignalError(f"invalid signals file ({fields})") from None
        return SignalSnapshot(**snap.model_dump(), taken_at=_now())


def _now() -> datetime:
    return datetime.now(UTC)


def default_source(fake_signals: Path | None) -> SignalSource:
    """File source (dev), real macOS source, or the null source elsewhere.

    On macOS the real source is probed once: a source that cannot be read at startup raises
    SignalError, which `run` maps to exit 2 (fail closed, invariant 6).
    """
    if fake_signals is not None:
        src: SignalSource = FileSignals(fake_signals)
    elif sys.platform == "darwin":
        src = MacSignals()
    else:
        return NullSignals()
    src.read()
    return src


# At most one read in flight per source (keyed by id): a wedged framework call must not pile up
# threads, and its thread must not keep the interpreter alive (daemon, so SIGTERM exits cleanly).
_inflight: dict[int, concurrent.futures.Future[SignalSnapshot]] = {}


async def read_with_timeout(
    source: SignalSource, timeout_s: float = DEFAULT_READ_TIMEOUT_S
) -> SignalSnapshot:
    """Read on a dedicated daemon thread; a hung read becomes a SignalError, never a stall.

    While a previous read of the same source is still running, no new thread is started and
    SignalError("previous read still hung") is raised at once. The default `to_thread` pool is
    never used, so a hung read cannot starve it.
    """
    key = id(source)
    prev = _inflight.get(key)
    if prev is not None and not prev.done():
        raise SignalError("previous read still hung")
    fut: concurrent.futures.Future[SignalSnapshot] = concurrent.futures.Future()
    _inflight[key] = fut

    def _forget(f: concurrent.futures.Future[SignalSnapshot]) -> None:
        if _inflight.get(key) is f:
            del _inflight[key]

    fut.add_done_callback(_forget)

    def _run() -> None:
        if not fut.set_running_or_notify_cancel():
            return
        try:
            fut.set_result(source.read())
        except BaseException as exc:  # delivered to the awaiting loop
            fut.set_exception(exc)

    threading.Thread(target=_run, name="signal-read", daemon=True).start()
    try:
        return await asyncio.wait_for(asyncio.wrap_future(fut), timeout_s)
    except TimeoutError:
        raise SignalError("signal read timed out") from None
