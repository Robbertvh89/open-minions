"""GPU busy share from `ioreg` (bench only, NF-8; ADR-0006 Gap 1).

`ioreg -r -d 1 -w 0 -c IOAccelerator` lists the accelerators with a `PerformanceStatistics`
dictionary whose `"Device Utilization %"` is the share of the last sampling window the GPU was busy
(0..100). This runs `ioreg` as a subprocess, so it is used from the control/bench process only (the
worker sandbox forbids exec) and never by the product loop. Absent off macOS: `GpuMeterError`.
"""

from __future__ import annotations

import re
import statistics
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

IOREG = "/usr/sbin/ioreg"
IOREG_ARGV = (IOREG, "-r", "-d", "1", "-w", "0", "-c", "IOAccelerator")
DEFAULT_INTERVAL_S = 0.25  # ~4 Hz
_UTIL = re.compile(r'"Device Utilization %"\s*=\s*(\d+(?:\.\d+)?)')


class GpuMeterError(RuntimeError):
    """ioreg is missing, failed or reported no GPU utilization."""


def parse_device_utilization(text: str) -> float | None:
    """Highest `Device Utilization %` in `ioreg` output (percent, 0..100), or None if absent.

    With several accelerators the busiest wins: the share is about the machine's GPU, and an
    integrated part next to a discrete one is not a case on Apple Silicon anyway."""
    values = [float(m) for m in _UTIL.findall(text)]
    values = [v for v in values if 0 <= v <= 100]
    return max(values) if values else None


def read_gpu_utilization(timeout_s: float = 5.0) -> float:
    """One ioreg sample in percent. Raises GpuMeterError (never returns a guess)."""
    try:
        out = subprocess.run(  # noqa: S603 - fixed argv, absolute path
            IOREG_ARGV, capture_output=True, text=True, timeout=timeout_s, check=False
        )
    except FileNotFoundError:
        raise GpuMeterError("ioreg not found: the GPU meter needs macOS") from None
    except (OSError, subprocess.SubprocessError) as exc:
        raise GpuMeterError(f"ioreg failed: {type(exc).__name__}") from None
    if out.returncode != 0:
        raise GpuMeterError(f"ioreg exited {out.returncode}")
    value = parse_device_utilization(out.stdout)
    if value is None:
        raise GpuMeterError("ioreg output has no 'Device Utilization %' (no Apple GPU?)")
    return value


@dataclass(frozen=True)
class GpuSample:
    at: float  # time.monotonic()
    percent: float


class GpuSampler:
    """Background thread sampling `read` every `interval_s`. A failed read ends the thread and
    is raised by `stop()`, so a broken meter can never pass as a quiet GPU."""

    def __init__(
        self,
        read: Callable[[], float] = read_gpu_utilization,
        interval_s: float = DEFAULT_INTERVAL_S,
        mono: Callable[[], float] = time.monotonic,
    ) -> None:
        self._read, self._interval, self._mono = read, interval_s, mono
        self._stop = threading.Event()
        self._samples: list[GpuSample] = []
        self._error: GpuMeterError | None = None
        self._thread = threading.Thread(target=self._loop, name="gpu-sampler", daemon=True)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._samples.append(GpuSample(self._mono(), self._read()))
            except GpuMeterError as exc:
                self._error = exc
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


def window_percent(samples: list[GpuSample], start: float, end: float) -> list[float]:
    """Percent values of the samples taken in `[start, end]`."""
    return [s.percent for s in samples if start <= s.at <= end]


def median_percent(values: list[float]) -> float:
    if not values:
        raise GpuMeterError("no GPU samples in the window")
    return statistics.median(values)


def mean_percent(values: list[float]) -> float:
    if not values:
        raise GpuMeterError("no GPU samples in the window")
    return statistics.fmean(values)
