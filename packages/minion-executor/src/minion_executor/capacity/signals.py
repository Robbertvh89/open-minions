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
import time
import weakref
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import structlog
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from minion_executor.capacity.safefile import SafeFileError, read_private_file

MAX_SIGNAL_FILE_BYTES = 4096
DEFAULT_READ_TIMEOUT_S = 1.5
CALL_PROBE_TIMEOUT_S = 0.5  # a call-state read in flight longer than this counts as hung
CALL_PROBE_MAX_AGE_S = 15.0  # a cached call state older than this is unknown

log = structlog.get_logger()


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
    # microphone or camera running somewhere on the Mac (FR-53 call-aware); None: unknown, which
    # the governor treats as "not in a call" (an optional comfort feature, ADR-0006 D16)
    call_active: bool | None = None


class SignalSource(Protocol):
    def read(self) -> SignalSnapshot: ...


@runtime_checkable
class CallAwareSource(Protocol):
    """A source that can skip its call probe (`on_call = "ignore"`)."""

    def set_want_call_state(self, want: bool) -> None: ...


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


def _fourcc(code: bytes) -> int:
    return int.from_bytes(code, "big")


class _PropAddress(ctypes.Structure):
    """AudioObjectPropertyAddress / CMIOObjectPropertyAddress: selector, scope, element."""

    _fields_ = [
        ("selector", ctypes.c_uint32),
        ("scope", ctypes.c_uint32),
        ("element", ctypes.c_uint32),
    ]


_SYSTEM_OBJECT = 1
_SCOPE_GLOBAL = _fourcc(b"glob")
_ELEMENT_MAIN = 0
_DEFAULT_INPUT_DEVICE = _fourcc(b"dIn ")  # kAudioHardwarePropertyDefaultInputDevice
_CMIO_DEVICES = _fourcc(b"dev#")  # kCMIOHardwarePropertyDevices
_IS_RUNNING_SOMEWHERE = _fourcc(b"gone")  # kAudioDevicePropertyDeviceIsRunningSomewhere (== CMIO)
_PROCESS_OBJECTS = _fourcc(b"prs#")  # kAudioHardwarePropertyProcessObjectList (macOS 14+)
_PROCESS_RUNNING_INPUT = _fourcc(b"piri")  # kAudioProcessPropertyIsRunningInput (macOS 14+)
_MAX_PROCESSES = 4096
_MAX_CAMERAS = 64


def call_from_flags(mic: bool | None, cameras: Sequence[bool | None]) -> bool | None:
    """Combine the microphone and camera "running somewhere" flags. Any True is a call; otherwise
    any unreadable flag leaves it unknown (None); else False."""
    flags = [mic, *cameras]
    if any(f is True for f in flags):
        return True
    return None if any(f is None for f in flags) else False


class _CallProbe:
    """Reads the "is running somewhere" property of the default input device (CoreAudio) and of
    every camera (CoreMediaIO). Public APIs; they only query device state and never open a device,
    so no TCC (camera/microphone) prompt appears and no indicator lights up. Device names, UIDs
    and counts are never read or kept. Any failure yields None (unknown)."""

    def __init__(self) -> None:
        self._audio: Any = None
        self._cmio: Any = None
        try:
            audio = ctypes.CDLL("/System/Library/Frameworks/CoreAudio.framework/CoreAudio")
            fn = audio.AudioObjectGetPropertyData
            fn.argtypes = [
                ctypes.c_uint32,
                ctypes.POINTER(_PropAddress),
                ctypes.c_uint32,
                ctypes.c_void_p,
                ctypes.POINTER(ctypes.c_uint32),
                ctypes.c_void_p,
            ]
            fn.restype = ctypes.c_int32
            size_fn = audio.AudioObjectGetPropertyDataSize
            size_fn.argtypes = [
                ctypes.c_uint32,
                ctypes.POINTER(_PropAddress),
                ctypes.c_uint32,
                ctypes.c_void_p,
                ctypes.POINTER(ctypes.c_uint32),
            ]
            size_fn.restype = ctypes.c_int32
            self._audio = audio
        except (OSError, AttributeError):
            self._audio = None
        try:
            cmio = ctypes.CDLL("/System/Library/Frameworks/CoreMediaIO.framework/CoreMediaIO")
            size_fn = cmio.CMIOObjectGetPropertyDataSize
            size_fn.argtypes = [
                ctypes.c_uint32,
                ctypes.POINTER(_PropAddress),
                ctypes.c_uint32,
                ctypes.c_void_p,
                ctypes.POINTER(ctypes.c_uint32),
            ]
            size_fn.restype = ctypes.c_int32
            data_fn = cmio.CMIOObjectGetPropertyData
            data_fn.argtypes = [
                ctypes.c_uint32,
                ctypes.POINTER(_PropAddress),
                ctypes.c_uint32,
                ctypes.c_void_p,
                ctypes.c_uint32,
                ctypes.POINTER(ctypes.c_uint32),
                ctypes.c_void_p,
            ]
            data_fn.restype = ctypes.c_int32
            self._cmio = cmio
        except (OSError, AttributeError):
            self._cmio = None

    def _get(self, obj: int, selector: int, buf: Any, size: int | None = None) -> int:
        addr = _PropAddress(selector, _SCOPE_GLOBAL, _ELEMENT_MAIN)
        used = ctypes.c_uint32(ctypes.sizeof(buf) if size is None else size)
        rc: int = self._audio.AudioObjectGetPropertyData(
            obj, ctypes.byref(addr), 0, None, ctypes.byref(used), ctypes.byref(buf)
        )
        return rc

    def _mic_by_process(self) -> bool | None:
        """Input-only: is any process running audio *input*? (macOS 14+ process objects; output
        on a headset or USB device does not count.) None: unsupported or unreadable."""
        addr = _PropAddress(_PROCESS_OBJECTS, _SCOPE_GLOBAL, _ELEMENT_MAIN)
        size = ctypes.c_uint32(0)
        if self._audio.AudioObjectGetPropertyDataSize(
            _SYSTEM_OBJECT, ctypes.byref(addr), 0, None, ctypes.byref(size)
        ):
            return None
        n = size.value // ctypes.sizeof(ctypes.c_uint32)
        if n == 0:
            return False
        if n > _MAX_PROCESSES:
            return None
        ids = (ctypes.c_uint32 * n)()
        used = ctypes.c_uint32(ctypes.sizeof(ids))
        if self._audio.AudioObjectGetPropertyData(
            _SYSTEM_OBJECT, ctypes.byref(addr), 0, None, ctypes.byref(used), ids
        ):
            return None
        readable = 0
        for obj in ids[: used.value // ctypes.sizeof(ctypes.c_uint32)]:
            running = ctypes.c_uint32(0)
            if self._get(obj, _PROCESS_RUNNING_INPUT, running):
                continue  # a process may vanish between the list and the read
            readable += 1
            if running.value != 0:
                return True
        return False if readable else None

    def _mic_by_device(self) -> bool | None:
        """Fallback: "running somewhere" of the default input device. Also true while a combined
        headset/USB device only plays output, or an always-open microphone app holds it."""
        dev = ctypes.c_uint32(0)
        if self._get(_SYSTEM_OBJECT, _DEFAULT_INPUT_DEVICE, dev):
            return None
        if dev.value == 0:  # kAudioObjectUnknown: no input device, so no microphone in use
            return False
        running = ctypes.c_uint32(0)
        if self._get(dev.value, _IS_RUNNING_SOMEWHERE, running):
            return None
        return running.value != 0

    def _mic(self) -> bool | None:
        if self._audio is None:
            return None
        by_process = self._mic_by_process()
        return by_process if by_process is not None else self._mic_by_device()

    def _cameras(self) -> list[bool | None]:
        if self._cmio is None:
            return [None]
        size_fn, data_fn = (
            self._cmio.CMIOObjectGetPropertyDataSize,
            self._cmio.CMIOObjectGetPropertyData,
        )
        addr = _PropAddress(_CMIO_DEVICES, _SCOPE_GLOBAL, _ELEMENT_MAIN)
        size = ctypes.c_uint32(0)
        if size_fn(_SYSTEM_OBJECT, ctypes.byref(addr), 0, None, ctypes.byref(size)):
            return [None]
        n = size.value // ctypes.sizeof(ctypes.c_uint32)
        if n == 0:
            return []
        if n > _MAX_CAMERAS:
            return [None]
        ids = (ctypes.c_uint32 * n)()
        used = ctypes.c_uint32(0)
        data_size = ctypes.c_uint32(ctypes.sizeof(ids))
        if data_fn(_SYSTEM_OBJECT, ctypes.byref(addr), 0, None, data_size, ctypes.byref(used), ids):
            return [None]
        out: list[bool | None] = []
        for dev in ids[: used.value // ctypes.sizeof(ctypes.c_uint32)]:
            running = ctypes.c_uint32(0)
            raddr = _PropAddress(_IS_RUNNING_SOMEWHERE, _SCOPE_GLOBAL, _ELEMENT_MAIN)
            rused = ctypes.c_uint32(0)
            rc = data_fn(
                dev,
                ctypes.byref(raddr),
                0,
                None,
                ctypes.c_uint32(ctypes.sizeof(running)),
                ctypes.byref(rused),
                ctypes.byref(running),
            )
            out.append(None if rc else running.value != 0)
        return out

    def call_active(self) -> bool | None:
        try:
            return call_from_flags(self._mic(), self._cameras())
        except Exception:  # never let an optional signal break a read
            return None


class CallReader(Protocol):
    def call_active(self) -> bool | None: ...


class AsyncCallProbe:
    """Runs a (possibly slow or wedged) call reader on its own daemon thread, apart from the safety
    read. At most one read is in flight. `value()` starts a read when none is running and waits up
    to `timeout_s` for it, so a change is seen on the very next call; a read that is already hung
    is not waited for (cached value, or None). Hung, failed or stale reads give None (unknown,
    which the governor treats as not in a call) and can never make the safety signals unavailable.
    """

    def __init__(
        self,
        reader: CallReader,
        timeout_s: float = CALL_PROBE_TIMEOUT_S,
        max_age_s: float = CALL_PROBE_MAX_AGE_S,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._reader = reader
        self._timeout_s = timeout_s
        self._max_age_s = max_age_s
        self._clock = clock
        self._lock = threading.Lock()
        self._started: float | None = None  # start of the read in flight
        self._value: bool | None = None
        self._finished: float | None = None

    def _run(self, done: threading.Event) -> None:
        try:
            val = self._reader.call_active()
        except Exception:  # an optional signal never raises
            val = None
        with self._lock:
            self._value, self._finished, self._started = val, self._clock(), None
        done.set()

    def settled(self) -> bool:
        """True once a read has finished at least once."""
        with self._lock:
            return self._finished is not None

    def begin(self) -> tuple[threading.Event | None, float]:
        """Start a read if none is in flight; returns (event to wait on or None, start time)."""
        now = self._clock()
        with self._lock:
            if self._started is None:
                done = threading.Event()
                self._started = now
                try:
                    threading.Thread(
                        target=self._run, args=(done,), name="call-probe", daemon=True
                    ).start()
                    return done, now
                except RuntimeError:  # cannot start a thread: unknown
                    self._started = None
            return None, now

    def finish(self, wait_for: threading.Event | None, began: float) -> tuple[bool | None, bool]:
        """(value, settled): wait for the read begun at `began` until its timeout, then take both
        under one lock. A read already hung when `begin` ran is not waited for."""
        if wait_for is not None:
            wait_for.wait(max(0.0, self._timeout_s - (self._clock() - began)))
        now = self._clock()
        with self._lock:
            settled = self._finished is not None
            if self._started is not None and now - self._started > self._timeout_s:
                return None, settled  # hung (or the read we started hung)
            if self._finished is None or now - self._finished > self._max_age_s:
                return None, settled
            return self._value, settled

    def read(self) -> tuple[bool | None, bool]:
        """(value, settled) after starting a read and waiting for it."""
        return self.finish(*self.begin())

    def value(self) -> bool | None:
        return self.read()[0]


_CF_RETAINED: dict[str, Any] = {"retval": {"already_cfretained": True}}


def _sysctl_memory_pressure(libc: Any) -> MemPressure:
    val = ctypes.c_int(0)
    size = ctypes.c_size_t(ctypes.sizeof(val))
    rc = libc.sysctlbyname(
        b"kern.memorystatus_vm_pressure_level",
        ctypes.byref(val),
        ctypes.byref(size),
        None,
        ctypes.c_size_t(0),
    )
    if rc != 0:
        raise SignalError("memory pressure unreadable")
    return pressure_from_sysctl(val.value)


def read_memory_pressure(fake_signals: Path | None = None) -> MemPressure | None:
    """Only the memory-pressure level (one sysctl; no IOKit, no call probe). With a dev signals
    file, that file's level. None when unreadable or not on macOS: callers treat it as unknown."""
    try:
        if fake_signals is not None:
            return FileSignals(fake_signals).read().memory_pressure
        if sys.platform != "darwin":
            return None
        libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.dylib", use_errno=True)
        return _sysctl_memory_pressure(libc)
    except Exception:  # best effort
        return None


class MacSignals:
    """Real macOS readers. Raises SignalError at construction if a framework is unusable."""

    _objc: Any
    _quartz: Any
    _foundation: Any
    _iokit: dict[str, Any]
    _libc: Any
    _call: AsyncCallProbe | None
    _call_warned: bool
    _want_call: bool

    def __init__(self, want_call: bool = True) -> None:
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
        self._call = None  # built lazily: no CoreMediaIO load or thread unless call state is wanted
        self._call_warned = False
        self._want_call = want_call

    def set_want_call_state(self, want: bool) -> None:
        """Whether the governor uses call state (`on_call` != ignore); set from config each poll."""
        self._want_call = want

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
        return _sysctl_memory_pressure(self._libc)

    def _call_begin(self) -> tuple[threading.Event | None, float] | None:
        if not self._want_call:
            return None
        if self._call is None:
            self._call = AsyncCallProbe(_CallProbe())
        return self._call.begin()

    def _call_finish(self, began: tuple[threading.Event | None, float] | None) -> bool | None:
        if began is None or self._call is None:
            return None
        call, _settled = self._call.finish(*began)
        # the read waits for a fresh value, so None is unreadable: failed, or hung (never settled)
        if call is None and not self._call_warned:
            self._call_warned = True
            log.info("call_probe_unavailable")
        return call

    def read(self) -> SignalSnapshot:
        try:
            # reads run on a bare worker thread with no ambient pool: drain autoreleased objects
            with self._objc.autorelease_pool():
                began = self._call_begin()  # the probe runs while the safety reads are taken
                info = self._foundation.NSProcessInfo.processInfo()
                on_ac, percent = self._power()
                hid_idle_s = self._hid_idle_s()
                low_power = bool(info.isLowPowerModeEnabled())
                thermal = thermal_from_ns(int(info.thermalState()))
                pressure = self._memory_pressure()
                return SignalSnapshot(
                    hid_idle_s=hid_idle_s,
                    on_ac=on_ac,
                    battery_percent=percent,
                    low_power_mode=low_power,
                    thermal=thermal,
                    memory_pressure=pressure,
                    taken_at=_now(),
                    call_active=self._call_finish(began),
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
    call_active: bool | None = None


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


def default_source(fake_signals: Path | None, *, want_call: bool) -> SignalSource:
    """File source (dev), real macOS source, or the null source elsewhere.

    On macOS the real source is probed once: a source that cannot be read at startup raises
    SignalError, which `run` maps to exit 2 (fail closed, invariant 6).
    """
    if fake_signals is not None:
        src: SignalSource = FileSignals(fake_signals)
    elif sys.platform == "darwin":
        src = MacSignals(want_call=want_call)
    else:
        return NullSignals()
    _startup_read(src)
    return src


def _startup_read(src: SignalSource, timeout_s: float = DEFAULT_READ_TIMEOUT_S) -> None:
    """The startup probe read, on a daemon thread with the same timeout as `read_with_timeout`."""
    fut: concurrent.futures.Future[SignalSnapshot] = concurrent.futures.Future()

    def _run() -> None:
        try:
            fut.set_result(src.read())
        except BaseException as exc:
            fut.set_exception(exc)

    threading.Thread(target=_run, name="signal-startup", daemon=True).start()
    try:
        fut.result(timeout_s)
    except concurrent.futures.TimeoutError:
        raise SignalError("signal read timed out") from None


# At most one read in flight per source: a wedged framework call must not pile up threads, and its
# thread must not keep the interpreter alive (daemon, so SIGTERM exits cleanly). Weakly keyed by
# source, so entries vanish with their source; a finished (done) entry is simply replaced.
_inflight: weakref.WeakKeyDictionary[SignalSource, concurrent.futures.Future[SignalSnapshot]] = (
    weakref.WeakKeyDictionary()
)


async def read_with_timeout(
    source: SignalSource, timeout_s: float = DEFAULT_READ_TIMEOUT_S
) -> SignalSnapshot:
    """Read on a dedicated daemon thread; a hung read becomes a SignalError, never a stall.

    While a previous read of the same source is still running, no new thread is started and
    SignalError("previous read still hung") is raised at once. The default `to_thread` pool is
    never used, so a hung read cannot starve it.
    """
    fut: concurrent.futures.Future[SignalSnapshot] = concurrent.futures.Future()
    try:
        prev = _inflight.get(source)
        if prev is not None and not prev.done():
            raise SignalError("previous read still hung")
        _inflight[source] = fut
    except TypeError:  # unhashable or not weak-referenceable: refuse before starting a thread
        raise SignalError("signal source must be hashable and weak-referenceable") from None

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
