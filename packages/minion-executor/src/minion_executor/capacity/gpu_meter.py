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

`-r` matters: without it `-c` still walks the whole registry (about 240 ms and 7 MB on an M3 Pro
against about 15 ms and 20 KB for the filtered form). This runs `ioreg` as a subprocess, so it is
used from the control/bench process only (the worker sandbox forbids exec) and never by the product
loop. Absent off macOS: `GpuMeterError`.
"""

from __future__ import annotations

import itertools
import re
import statistics
import subprocess
import threading
import time
from collections.abc import Callable, Sequence
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
    keeps what it accumulated). Positive deltas are summed per consecutive sample pair, so a
    counter that goes backwards costs at most one interval. Clients of the process are summed.
    Raises GpuMeterError without a usable pair of readings."""
    usable = [s for s in samples if s.clients is not None]
    before = [s for s in usable if s.at <= start]
    after = [s for s in usable if s.at <= end]
    if not before or not after or after[-1].at <= before[-1].at:
        raise GpuMeterError("not enough per-process GPU readings in the window")
    first, last = before[-1], after[-1]
    window = [s for s in usable if first.at <= s.at <= last.at]
    delta_ns = 0
    for prev, cur in itertools.pairwise(window):
        before_ns = prev.clients or {}
        # a client new in this interval started at 0; a counter that dropped (reset) loses at
        # most this one interval, never the rest of the window
        delta_ns += sum(max(0, v - before_ns.get(cid, 0)) for cid, v in (cur.clients or {}).items())
    return delta_ns / 1e9 / (last.at - first.at)


def median_percent(values: list[float]) -> float:
    if not values:
        raise GpuMeterError("no GPU samples in the window")
    return statistics.median(values)


def mean_percent(values: list[float]) -> float:
    if not values:
        raise GpuMeterError("no GPU samples in the window")
    return statistics.fmean(values)
