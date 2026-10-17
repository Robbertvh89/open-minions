"""NF-7 foreground-usability proxy: pure metrics and verdict logic (TASK-6.9).

No engine imports here. The frame probe lives in `runners/fg_probe.py`; this module turns its
latencies into frame statistics and applies the NF-7 thresholds (SPEC NF-7, ADR-0006).
"""

from __future__ import annotations

import contextlib
import math
import os
import signal
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from minion_common.enums import CapacityProfile

FRAME_PERIOD_MS = 1000.0 / 30.0
RATIO_LIMIT = 1.10
MISS_DELTA_PP = 1.0
JUDGED: frozenset[CapacityProfile] = frozenset({CapacityProfile.LIGHT, CapacityProfile.BALANCED})

_EPS = 1e-9


@dataclass(frozen=True)
class FrameStats:
    p50_ms: float
    p95_ms: float
    miss_rate: float
    frames: int


@dataclass(frozen=True)
class PhaseMeasure:
    frames: FrameStats
    launch_p50_s: float | None = None


@dataclass(frozen=True)
class Nf7Verdict:
    frame_p95_ratio: float
    miss_delta_pp: float
    launch_p50_ratio: float | None
    frame_ok: bool
    miss_ok: bool
    launch_ok: bool | None
    passed: bool

    @property
    def partial(self) -> bool:
        """Launch time was not measured: judged on frames only."""
        return self.launch_ok is None


def percentile(values: Sequence[float], q: float) -> float:
    """Linear-interpolated percentile (q in 0..100); 0.0 for an empty sample."""
    if not values:
        return 0.0
    s = sorted(values)
    if len(s) == 1:
        return s[0]
    pos = (len(s) - 1) * q / 100.0
    lo = math.floor(pos)
    hi = math.ceil(pos)
    return s[lo] + (s[hi] - s[lo]) * (pos - lo)


def frame_stats(
    latencies_ms: Sequence[float], missed: int, *, period_ms: float = FRAME_PERIOD_MS
) -> FrameStats:
    """Latency is completion minus the *scheduled* start. A frame misses its deadline when its
    latency exceeds the period; `missed` counts frames skipped because the probe fell behind
    (they count as misses and in the total)."""
    if missed < 0:
        raise ValueError("missed must be >= 0")
    late = sum(1 for v in latencies_ms if v > period_ms)
    total = len(latencies_ms) + missed
    return FrameStats(
        p50_ms=percentile(latencies_ms, 50),
        p95_ms=percentile(latencies_ms, 95),
        miss_rate=(late + missed) / total if total else 0.0,
        frames=total,
    )


def _ratio(loaded: float, baseline: float) -> float:
    if baseline <= _EPS:
        return 1.0 if loaded <= _EPS else math.inf
    return loaded / baseline


def nf7_proxy_verdict(baseline: PhaseMeasure, loaded: PhaseMeasure) -> Nf7Verdict:
    frame_ratio = _ratio(loaded.frames.p95_ms, baseline.frames.p95_ms)
    delta_pp = (loaded.frames.miss_rate - baseline.frames.miss_rate) * 100.0
    launch_ratio: float | None = None
    launch_ok: bool | None = None
    if baseline.launch_p50_s is not None and loaded.launch_p50_s is not None:
        launch_ratio = _ratio(loaded.launch_p50_s, baseline.launch_p50_s)
        launch_ok = launch_ratio <= RATIO_LIMIT + _EPS
    frame_ok = frame_ratio <= RATIO_LIMIT + _EPS
    miss_ok = delta_pp <= MISS_DELTA_PP + _EPS
    return Nf7Verdict(
        frame_p95_ratio=frame_ratio,
        miss_delta_pp=delta_pp,
        launch_p50_ratio=launch_ratio,
        frame_ok=frame_ok,
        miss_ok=miss_ok,
        launch_ok=launch_ok,
        passed=frame_ok and miss_ok and launch_ok is not False,
    )


def _median_opt(values: list[float | None]) -> float | None:
    present = [v for v in values if v is not None]
    return statistics.median(present) if present else None


def _median_measure(ms: Sequence[PhaseMeasure]) -> PhaseMeasure:
    return PhaseMeasure(
        frames=FrameStats(
            p50_ms=statistics.median(m.frames.p50_ms for m in ms),
            p95_ms=statistics.median(m.frames.p95_ms for m in ms),
            miss_rate=statistics.median(m.frames.miss_rate for m in ms),
            frames=sum(m.frames.frames for m in ms),
        ),
        launch_p50_s=_median_opt([m.launch_p50_s for m in ms]),
    )


def aggregate(
    rows: Sequence[tuple[str, CapacityProfile, PhaseMeasure, PhaseMeasure]],
) -> dict[tuple[str, CapacityProfile], Nf7Verdict]:
    """Median of each metric over the repeats of (model, profile), then the thresholds.

    `full` is informational: its verdict is computed but callers must not judge it
    (see `is_judged`)."""
    groups: dict[tuple[str, CapacityProfile], list[tuple[PhaseMeasure, PhaseMeasure]]] = (
        defaultdict(list)
    )
    for model, profile, base, loaded in rows:
        groups[(model, profile)].append((base, loaded))
    return {
        key: nf7_proxy_verdict(
            _median_measure([b for b, _ in pairs]), _median_measure([ld for _, ld in pairs])
        )
        for key, pairs in groups.items()
    }


def is_judged(profile: CapacityProfile) -> bool:
    return profile in JUDGED


def failures(
    verdicts: Mapping[tuple[str, CapacityProfile], Nf7Verdict],
) -> list[tuple[str, CapacityProfile]]:
    return [k for k, v in verdicts.items() if is_judged(k[1]) and not v.passed]


OSASCRIPT = "/usr/bin/osascript"
PGREP = "/usr/bin/pgrep"
LAUNCH_TIMEOUT_S = 15.0
EXIT_WAIT_S = 5.0
GAP_S = 1.0


def _running_pids(
    proc_name: str, run: Callable[..., subprocess.CompletedProcess[str]]
) -> list[int] | None:
    """PIDs of this user's `proc_name` processes; [] if none, None if pgrep itself failed."""
    try:
        r = run(
            [PGREP, "-u", str(os.getuid()), "-x", proc_name],
            capture_output=True,
            text=True,
            timeout=5.0,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if r.returncode == 1:
        return []
    if r.returncode != 0:
        return None
    try:
        return [int(x) for x in r.stdout.split()]
    except ValueError:
        return None


def measure_app_launch(
    n: int,
    *,
    bundle_id: str = "com.apple.calculator",
    proc_name: str = "Calculator",
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    kill: Callable[[int, int], None] = os.kill,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    platform: str = sys.platform,
) -> list[float] | None:
    """Wall time (s) of `n` cold launches of a small app, or None when unavailable.

    One warm-up launch is excluded. If the app is already running it belongs to the owner: it is
    never touched and the result is None. Only the instance we launched is terminated (SIGTERM),
    then we wait for it to exit. None on non-darwin and on any osascript/TCC error."""
    if platform != "darwin" or n < 1:
        return None
    if _running_pids(proc_name, run) != []:
        return None
    argv = [OSASCRIPT, "-e", f'tell application id "{bundle_id}" to launch']
    samples: list[float] = []

    def close_ours() -> bool:
        pids = _running_pids(proc_name, run)
        if pids is None:
            return False
        for pid in pids:
            with contextlib.suppress(OSError):
                kill(pid, signal.SIGTERM)
        deadline = clock() + EXIT_WAIT_S
        while _running_pids(proc_name, run):
            if clock() >= deadline:
                return False
            sleep(0.1)
        sleep(GAP_S)
        return True

    for i in range(n + 1):
        t0 = clock()
        try:
            r = run(argv, capture_output=True, text=True, timeout=LAUNCH_TIMEOUT_S, check=False)
        except (OSError, subprocess.SubprocessError):
            close_ours()
            return None
        elapsed = clock() - t0
        ok = r.returncode == 0
        closed = close_ours()
        if not ok or not closed:
            return None
        if i > 0:
            samples.append(elapsed)
    return samples
