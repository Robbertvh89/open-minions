"""GPU meters from `ioreg` (bench only, NF-8; ADR-0006 Gap 1).

Primary metric: the **per-process GPU time** of the worker. `ioreg -r -l -w 0 -c
AGXDeviceUserClient` lists one user client per Metal device connection of every process, with
`"IOUserClientCreator" = "pid N, name"` and `"AppUsage" = ({"API"="Metal", "lastSubmittedTime"=...,
"accumulatedGPUTime"=<ns>}, ...)`. The delta of the worker pid's summed `accumulatedGPUTime` over a
window, divided by the window's wall time, is the share of the GPU our process kept busy. Other
apps, the display and DVFS do not enter, so no baseline needs subtracting.

Secondary, informational: `Device Utilization %` from `ioreg -r -d 1 -w 0 -c IOAccelerator`. It is
device-wide (every process) and frequency-scaled, so it is not a valid per-process share on a Mac
that is being used.

Two readers of the per-process counters: `read_gpu_clients` shells out to `ioreg` (bench), and
`read_gpu_clients_iokit` walks the IORegistry in-process through ctypes (about 1 ms of CPU against
about 15 ms for the child; the run-time duty guard uses it). Both return the same shape.

`-r` matters: without it `-c` still walks the whole registry (about 240 ms and 7 MB on an M3 Pro
against about 15 ms and 20 KB for the filtered form). The ioreg path runs a subprocess (the
worker sandbox denies exec of `ioreg`), so it is for the control/bench process only; the sandbox
does allow IOKit user clients, but the in-process reader is used from the control process too,
because the guard must not depend on the worker. Absent off macOS: `GpuMeterError`.
"""

from __future__ import annotations

import ctypes
import re
import statistics
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass

IOREG = "/usr/sbin/ioreg"
IOREG_ARGV = (IOREG, "-r", "-d", "1", "-w", "0", "-c", "IOAccelerator")
IOREG_CLIENTS_ARGV = (IOREG, "-r", "-l", "-w", "0", "-c", "AGXDeviceUserClient")
MAX_OUTPUT_BYTES = 8 * 1024 * 1024
DEFAULT_INTERVAL_S = 0.5  # ~2 Hz
_UTIL = re.compile(r'"Device Utilization %"\s*=\s*(\d+(?:\.\d+)?)')
_BLOCK = re.compile(r"^[ |]*\+-o ", re.MULTILINE)
_CLIENT_ID = re.compile(r"\bid (0x[0-9a-fA-F]{1,16})\b")
_CREATOR = re.compile(r'"IOUserClientCreator"\s*=\s*"pid (\d{1,10})(?!\d),')
_GPU_TIME = re.compile(r'"accumulatedGPUTime"\s*=\s*(\d{1,20})(?!\d)')
_CREATOR_VALUE = re.compile(r"pid (\d{1,10})(?!\d),")
IOKIT_PATH = "/System/Library/Frameworks/IOKit.framework/IOKit"
CF_PATH = "/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation"
MAX_IOKIT_CLIENTS = 100_000  # clients walked per read
MAX_USAGE_ENTRIES = 1_000  # AppUsage entries per client; more fails the read
_WALK_ATTEMPTS = 3  # registry walks tried before the read fails
_CF_UTF8 = 0x08000100
_CF_NUMBER_SINT64 = 4
_CREATOR_BUF = 256


class GpuMeterError(RuntimeError):
    """ioreg is missing, failed or reported no GPU utilization / user clients."""


def _ioreg(argv: Sequence[str], timeout_s: float = 5.0, cap: int = MAX_OUTPUT_BYTES) -> str:
    """Run ioreg (absolute path, fixed argv) and return stdout. Output is read up to `cap` bytes
    (more: error), decoded with errors="replace"; a run past `timeout_s` is killed."""
    try:
        proc = subprocess.Popen(  # noqa: S603 - fixed argv, absolute path
            list(argv), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
        )
    except FileNotFoundError:
        raise GpuMeterError("ioreg not found: the GPU meter needs macOS") from None
    except (OSError, subprocess.SubprocessError) as exc:
        raise GpuMeterError(f"ioreg failed: {type(exc).__name__}") from None
    stdout = proc.stdout
    if stdout is None:  # pragma: no cover - PIPE always gives one
        raise GpuMeterError("ioreg failed: no stdout")
    timed_out = threading.Event()

    def _expire() -> None:
        timed_out.set()
        proc.kill()

    timer = threading.Timer(timeout_s, _expire)
    timer.start()
    data = b""
    try:
        data = stdout.read(cap + 1)
    except (OSError, ValueError):
        pass
    finally:
        timer.cancel()
        if len(data) > cap:
            proc.kill()
        stdout.close()
        rc = proc.wait()
    if timed_out.is_set():
        raise GpuMeterError("ioreg timed out")
    if len(data) > cap:
        raise GpuMeterError("ioreg output too large")
    if rc != 0:
        raise GpuMeterError(f"ioreg exited {rc}")
    return data.decode("utf-8", errors="replace")


def parse_device_utilization(text: str) -> float | None:
    """Highest `Device Utilization %` in `ioreg` output (percent, 0..100), or None if absent.

    With several accelerators the busiest wins: the share is about the machine's GPU, and an
    integrated part next to a discrete one is not a case on Apple Silicon anyway."""
    values = [float(m) for m in _UTIL.findall(text)]
    values = [v for v in values if 0 <= v <= 100]
    return max(values) if values else None


def read_gpu_utilization(timeout_s: float = 5.0) -> float:
    """One device-wide ioreg sample in percent. Raises GpuMeterError (never returns a guess)."""
    value = parse_device_utilization(_ioreg(IOREG_ARGV, timeout_s))
    if value is None:
        raise GpuMeterError("ioreg output has no 'Device Utilization %' (no Apple GPU?)")
    return value


def parse_gpu_clients(text: str) -> dict[int, dict[str, int]]:
    """`pid -> {client id -> summed accumulatedGPUTime (ns)}` from `AGXDeviceUserClient` output.

    A process with several user clients has several entries (callers sum them). A client without
    a parsable creator is skipped; a client with no Metal usage counts 0. Pure and tolerant:
    unknown lines are ignored."""
    out: dict[int, dict[str, int]] = {}
    for n, block in enumerate(_BLOCK.split(text)[1:]):
        creator = _CREATOR.search(block)
        if creator is None:
            continue
        ident = _CLIENT_ID.search(block)
        cid = ident.group(1) if ident else f"#{n}"
        total = sum(int(v) for v in _GPU_TIME.findall(block))
        out.setdefault(int(creator.group(1)), {})[cid] = total
    return out


def read_gpu_clients(timeout_s: float = 5.0) -> dict[int, dict[str, int]]:
    """All GPU user clients by pid. Raises GpuMeterError when there are none (no Apple GPU)."""
    text = _ioreg(IOREG_CLIENTS_ARGV, timeout_s)
    if "AGXDeviceUserClient" not in text:
        raise GpuMeterError("ioreg lists no AGXDeviceUserClient (no Apple GPU?)")
    if not any(_CREATOR.search(b) and _GPU_TIME.search(b) for b in _BLOCK.split(text)[1:]):
        # a changed ioreg format must fail loudly, not read as a process that used no GPU
        raise GpuMeterError("no AGXDeviceUserClient has a creator pid and accumulatedGPUTime")
    return parse_gpu_clients(text)


def read_pid_gpu_clients(pid: int | None) -> dict[str, int] | None:
    """`client id -> ns` of one process ({} if it has no client yet), None without a pid."""
    if pid is None:
        return None
    return read_gpu_clients().get(pid, {})


def probe_meters() -> None:
    """Fail fast and clearly off macOS / without an Apple GPU (both meters)."""
    read_gpu_utilization()
    read_gpu_clients()


@dataclass(frozen=True)
class GpuSample:
    at: float  # time.monotonic(), midpoint of the reads
    percent: float  # device-wide (informational)
    clients: dict[str, int] | None = None  # worker pid's user clients: id -> accumulated ns


class GpuSampler:
    """Background thread sampling both meters every `interval_s`. Any failed read ends the thread
    and is raised by `stop()` as `GpuMeterError`: a broken meter never passes as a quiet GPU."""

    def __init__(
        self,
        read: Callable[[], float] = read_gpu_utilization,
        interval_s: float = DEFAULT_INTERVAL_S,
        mono: Callable[[], float] = time.monotonic,
        read_clients: Callable[[], dict[str, int] | None] | None = None,
    ) -> None:
        self._read, self._interval, self._mono = read, interval_s, mono
        self._read_clients = read_clients
        self._stop = threading.Event()
        self._samples: list[GpuSample] = []
        self._error: GpuMeterError | None = None
        self._thread = threading.Thread(target=self._loop, name="gpu-sampler", daemon=True)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                t0 = self._mono()
                percent = self._read()
                clients = self._read_clients() if self._read_clients is not None else None
                self._samples.append(GpuSample((t0 + self._mono()) / 2, percent, clients))
            except GpuMeterError as exc:
                self._error = exc
                return
            except Exception as exc:  # never lose the failure in a dying thread
                self._error = GpuMeterError(f"GPU meter failed: {type(exc).__name__}")
                return
            self._stop.wait(self._interval)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> list[GpuSample]:
        self._stop.set()
        self._thread.join(timeout=10)
        if self._error is not None:
            raise self._error
        return list(self._samples)

    def snapshot(self) -> list[GpuSample]:
        return list(self._samples)


def make_sampler(pid_fn: Callable[[], int | None]) -> GpuSampler:
    """The real sampler for the process `pid_fn()` returns (looked up each sample: the worker is
    spawned after the sampler starts)."""
    return GpuSampler(read_clients=lambda: read_pid_gpu_clients(pid_fn()))


def window_percent(samples: list[GpuSample], start: float, end: float) -> list[float]:
    """Device percent values of the samples taken in `[start, end]`."""
    return [s.percent for s in samples if start <= s.at <= end]


def process_gpu_share(samples: list[GpuSample], start: float, end: float) -> float:
    """Share of wall time the process kept the GPU busy over `[start, end]` (a fraction).

    Uses the last sample at or before each bound, so the divisor is the true span between the two
    readings. Per client: the start value is 0 when the client is not yet listed (it appeared
    inside the window), the end value is the last one seen (a client that closed inside the window
    keeps what it accumulated). Positive deltas are summed per consecutive sample pair (each
    reading compared with the client's last seen value), so a counter that goes backwards costs at
    most one interval. Clients of the process are summed.
    Raises GpuMeterError without a usable pair of readings."""
    usable = [s for s in samples if s.clients is not None]
    before = [s for s in usable if s.at <= start]
    after = [s for s in usable if s.at <= end]
    if not before or not after or after[-1].at <= before[-1].at:
        raise GpuMeterError("not enough per-process GPU readings in the window")
    first, last = before[-1], after[-1]
    window = [s for s in usable if first.at <= s.at <= last.at]
    delta_ns = 0
    seen = dict(first.clients or {})  # last value per client across the window
    for cur in window[1:]:
        for cid, v in (cur.clients or {}).items():
            # a client new in this interval started at 0; one missing from a sample and back later
            # continues from its last seen value; a counter that dropped (reset) loses at most
            # this one interval, never the rest of the window
            delta_ns += max(0, v - seen.get(cid, 0))
            seen[cid] = v
    return delta_ns / 1e9 / (last.at - first.at)


def median_percent(values: list[float]) -> float:
    if not values:
        raise GpuMeterError("no GPU samples in the window")
    return statistics.median(values)


def mean_percent(values: list[float]) -> float:
    if not values:
        raise GpuMeterError("no GPU samples in the window")
    return statistics.fmean(values)


# --- in-process IOKit reader (control process, macOS) -------------------------------------------


@dataclass(frozen=True)
class RawClient:
    """One AGXDeviceUserClient as read from the registry, before validation."""

    registry_id: int
    creator: str | None  # "pid N, name"
    gpu_times_ns: tuple[int, ...]  # accumulatedGPUTime of each AppUsage entry


def pid_from_creator(creator: str | None) -> int | None:
    """Pid out of an `IOUserClientCreator` value ("pid 123, python3"), None if it does not parse."""
    if creator is None:
        return None
    m = _CREATOR_VALUE.match(creator)
    return int(m.group(1)) if m else None


def clients_by_pid(raw: Iterable[RawClient]) -> dict[int, dict[str, int]]:
    """Same shape as `parse_gpu_clients`: `pid -> {"0x<registry id>" -> summed ns}`. A client
    without a parsable creator is skipped; negative times count 0."""
    out: dict[int, dict[str, int]] = {}
    for client in raw:
        pid = pid_from_creator(client.creator)
        if pid is None:
            continue
        total = sum(max(0, t) for t in client.gpu_times_ns)
        out.setdefault(pid, {})[f"0x{client.registry_id:x}"] = total
    return out


class _IokitApi:
    """ctypes bindings. Every CF value is type-checked before it is used (a wrong type would
    crash the process) and every io_object / CF reference is released."""

    def __init__(self) -> None:
        try:
            self.iokit = ctypes.CDLL(IOKIT_PATH)
            self.cf = ctypes.CDLL(CF_PATH)
        except OSError:
            raise GpuMeterError("IOKit is not available: the GPU meter needs macOS") from None
        vp, u32 = ctypes.c_void_p, ctypes.c_uint32
        io, cf = self.iokit, self.cf
        io.IOServiceMatching.restype = vp
        io.IOServiceMatching.argtypes = [ctypes.c_char_p]
        io.IOServiceGetMatchingServices.argtypes = [u32, vp, ctypes.POINTER(u32)]
        io.IOServiceGetMatchingServices.restype = ctypes.c_int
        io.IOIteratorNext.argtypes = [u32]
        io.IOIteratorNext.restype = u32
        io.IOObjectRelease.argtypes = [u32]
        io.IOObjectRelease.restype = ctypes.c_int
        io.IORegistryEntryGetRegistryEntryID.argtypes = [u32, ctypes.POINTER(ctypes.c_uint64)]
        io.IORegistryEntryGetRegistryEntryID.restype = ctypes.c_int
        io.IORegistryEntryGetChildIterator.argtypes = [u32, ctypes.c_char_p, ctypes.POINTER(u32)]
        io.IORegistryEntryGetChildIterator.restype = ctypes.c_int
        io.IOObjectConformsTo.argtypes = [u32, ctypes.c_char_p]
        io.IOObjectConformsTo.restype = ctypes.c_int  # boolean_t, not a C bool
        io.IOIteratorIsValid.argtypes = [u32]
        io.IOIteratorIsValid.restype = ctypes.c_int
        io.IORegistryEntryCreateCFProperty.argtypes = [u32, vp, vp, u32]
        io.IORegistryEntryCreateCFProperty.restype = vp
        cf.CFStringCreateWithCString.argtypes = [vp, ctypes.c_char_p, u32]
        cf.CFStringCreateWithCString.restype = vp
        cf.CFStringGetCString.argtypes = [vp, ctypes.c_char_p, ctypes.c_long, u32]
        cf.CFStringGetCString.restype = ctypes.c_bool
        cf.CFArrayGetCount.argtypes = [vp]
        cf.CFArrayGetCount.restype = ctypes.c_long
        cf.CFArrayGetValueAtIndex.argtypes = [vp, ctypes.c_long]
        cf.CFArrayGetValueAtIndex.restype = vp
        cf.CFDictionaryGetValue.argtypes = [vp, vp]
        cf.CFDictionaryGetValue.restype = vp
        cf.CFNumberGetValue.argtypes = [vp, ctypes.c_int, vp]
        cf.CFNumberGetValue.restype = ctypes.c_bool
        cf.CFGetTypeID.argtypes = [vp]
        cf.CFGetTypeID.restype = ctypes.c_ulong
        cf.CFRelease.argtypes = [vp]
        cf.CFRelease.restype = None
        for name in ("CFStringGetTypeID", "CFArrayGetTypeID", "CFDictionaryGetTypeID"):
            getattr(cf, name).argtypes = []
            getattr(cf, name).restype = ctypes.c_ulong
        cf.CFNumberGetTypeID.argtypes = []
        cf.CFNumberGetTypeID.restype = ctypes.c_ulong
        self.t_string = cf.CFStringGetTypeID()
        self.t_array = cf.CFArrayGetTypeID()
        self.t_dict = cf.CFDictionaryGetTypeID()
        self.t_number = cf.CFNumberGetTypeID()
        self._keys: list[int] = []
        self.k_creator = self._key("IOUserClientCreator")
        self.k_usage = self._key("AppUsage")
        self.k_gpu = self._key("accumulatedGPUTime")

    def _key(self, name: str) -> int:
        ref = self.cf.CFStringCreateWithCString(None, name.encode(), _CF_UTF8)
        if not ref:
            raise GpuMeterError("IOKit reader: CFString allocation failed")
        self._keys.append(ref)  # process-lifetime constants
        return int(ref)

    def _is(self, ref: int | None, type_id: int) -> bool:
        return bool(ref) and self.cf.CFGetTypeID(ref) == type_id

    def _creator(self, entry: int) -> str | None:
        ref = self.iokit.IORegistryEntryCreateCFProperty(entry, self.k_creator, None, 0)
        if not ref:
            return None
        try:
            if not self._is(ref, self.t_string):
                return None
            buf = ctypes.create_string_buffer(_CREATOR_BUF)
            if not self.cf.CFStringGetCString(ref, buf, _CREATOR_BUF, _CF_UTF8):
                return None
            return buf.value.decode("utf-8", errors="replace")
        finally:
            self.cf.CFRelease(ref)

    def _gpu_times(self, entry: int) -> tuple[int, ...]:
        usage = self.iokit.IORegistryEntryCreateCFProperty(entry, self.k_usage, None, 0)
        if not usage:
            return ()
        try:
            if not self._is(usage, self.t_array):
                return ()
            times: list[int] = []
            if self.cf.CFArrayGetCount(usage) > MAX_USAGE_ENTRIES:
                raise GpuMeterError("IOKit reader: AppUsage has too many entries")
            for i in range(self.cf.CFArrayGetCount(usage)):
                item = self.cf.CFArrayGetValueAtIndex(usage, i)  # borrowed
                if not self._is(item, self.t_dict):
                    continue
                num = self.cf.CFDictionaryGetValue(item, self.k_gpu)  # borrowed
                if not self._is(num, self.t_number):
                    continue
                val = ctypes.c_int64()
                if self.cf.CFNumberGetValue(num, _CF_NUMBER_SINT64, ctypes.byref(val)):
                    times.append(val.value)
            return tuple(times)
        finally:
            self.cf.CFRelease(usage)

    def _client(self, entry: int) -> RawClient | None:
        rid = ctypes.c_uint64()
        if self.iokit.IORegistryEntryGetRegistryEntryID(entry, ctypes.byref(rid)) != 0:
            return None
        return RawClient(rid.value, self._creator(entry), self._gpu_times(entry))

    def read(self) -> list[RawClient]:
        """One consistent walk. The registry changes under us (a client opens or closes); an
        iterator that became invalid during the walk may have skipped entries, so the walk is
        repeated (bounded) and, if it never completes cleanly, the read fails (fail closed)."""
        for _ in range(_WALK_ATTEMPTS):
            out: list[RawClient] = []
            if self._walk(out):
                return out
        raise GpuMeterError("IOKit reader: the registry kept changing during the walk")

    def _next(self, it: int, visited: list[int]) -> int:
        """IOIteratorNext, counted: every entry visited (not only the matching ones) is capped."""
        entry = int(self.iokit.IOIteratorNext(it))
        if entry:
            visited[0] += 1
            if visited[0] > MAX_IOKIT_CLIENTS:
                self.iokit.IOObjectRelease(entry)
                raise GpuMeterError("IOKit reader: too many registry entries")
        return entry

    def _walk(self, out: list[RawClient]) -> bool:
        io = self.iokit
        acc_iter = ctypes.c_uint32()
        matching = io.IOServiceMatching(b"IOAccelerator")  # consumed by the call below
        if not matching:
            raise GpuMeterError("IOKit reader: IOServiceMatching failed")
        if io.IOServiceGetMatchingServices(0, matching, ctypes.byref(acc_iter)) != 0:
            raise GpuMeterError("IOKit reader: no IOAccelerator service (no Apple GPU?)")
        visited = [0]
        valid = True
        try:
            while accel := self._next(acc_iter.value, visited):
                try:
                    valid = self._walk_children(accel, out, visited) and valid
                finally:
                    io.IOObjectRelease(accel)
            return bool(io.IOIteratorIsValid(acc_iter.value)) and valid
        finally:
            io.IOObjectRelease(acc_iter.value)

    def _walk_children(self, accel: int, out: list[RawClient], visited: list[int]) -> bool:
        """Append the accelerator's user clients; False if the child iterator went stale."""
        io = self.iokit
        children = ctypes.c_uint32()
        if io.IORegistryEntryGetChildIterator(accel, b"IOService", ctypes.byref(children)) != 0:
            return True
        try:
            while entry := self._next(children.value, visited):
                try:
                    if io.IOObjectConformsTo(entry, b"AGXDeviceUserClient") != 0:
                        client = self._client(entry)
                        if client is not None:
                            out.append(client)
                finally:
                    io.IOObjectRelease(entry)
            return bool(io.IOIteratorIsValid(children.value))
        finally:
            io.IOObjectRelease(children.value)


class IokitGpuReader:
    """In-process equivalent of `read_gpu_clients`. Reads are serialised by a lock, so one
    instance can be shared with `asyncio.to_thread`."""

    def __init__(self, raw_reader: Callable[[], list[RawClient]] | None = None) -> None:
        self._raw = raw_reader
        self._lock = threading.Lock()

    def __call__(self) -> dict[int, dict[str, int]]:
        """All GPU user clients by pid. Raises GpuMeterError when there are none."""
        with self._lock:
            if self._raw is None:
                self._raw = _IokitApi().read
            raw = self._raw()
        if not raw:
            raise GpuMeterError("IOKit lists no AGXDeviceUserClient (no Apple GPU?)")
        clients = clients_by_pid(raw)
        if not clients:
            # a changed registry layout must fail loudly, not read as a process that used no GPU
            raise GpuMeterError("no AGXDeviceUserClient has a parsable creator pid")
        return clients


def read_gpu_clients_iokit() -> dict[int, dict[str, int]]:
    """All GPU user clients by pid via in-process IOKit (see `IokitGpuReader`)."""
    return _default_reader()()


_reader: IokitGpuReader | None = None
_reader_lock = threading.Lock()


def _default_reader() -> IokitGpuReader:
    global _reader
    with _reader_lock:
        if _reader is None:
            _reader = IokitGpuReader()
        return _reader


def default_gpu_reader() -> Callable[[], dict[int, dict[str, int]]] | None:
    """The in-process reader on darwin, None elsewhere (the duty guard is then inert)."""
    if sys.platform != "darwin":
        return None
    return read_gpu_clients_iokit
