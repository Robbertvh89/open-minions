"""NF-7 foreground-usability proxy: pure metrics and verdict logic (TASK-6.9).

No engine imports here. The frame probe lives in `runners/fg_probe.py`; this module turns its
latencies into frame statistics and applies the NF-7 thresholds (SPEC NF-7, ADR-0006).
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import os
import signal
import statistics
import subprocess
import sys
import threading
import time
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import structlog
from minion_common.enums import CapacityProfile, Engine
from minion_common.schemas import Budget, ModelEntry

from minion_executor.bench import (
    PROBE_TIMEOUT_S,
    SCHEMA_VERSION,
    BenchConfig,
    _params,
    host_environment,
    synthetic_records,
)
from minion_executor.capacity.gpu_meter import (
    GpuMeterError,
    GpuSampler,
    make_sampler,
    probe_meters,
    process_gpu_share,
)
from minion_executor.model_cache import ModelCache
from minion_executor.runners.base import RunnerError
from minion_executor.runners.fg_probe import FrameProbe, ProbeResult, check_duration
from minion_executor.worker.process import WorkerCrashed, WorkerHandle

log = structlog.get_logger()

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


# -- orchestration --

SETTLE_S = 2.0  # between starting the load and the loaded window
SHARE_SETTLE_S = 1.5  # dropped from the start of the loaded window for the worker GPU share
MIN_WORKER_SHARE = 0.02  # a loaded window with less worker GPU time did not test anything

SamplerFactory = Callable[[Callable[[], int | None]], GpuSampler]
Launcher = Callable[[int], list[float] | None]


@dataclass(frozen=True)
class ForegroundCheckConfig:
    duration_s: float = 60.0
    launches: int = 20
    repeats: int = 3
    max_tokens: int = 128
    seed: int = 0
    settle_s: float = SETTLE_S

    def __post_init__(self) -> None:
        check_duration(self.duration_s)
        if not 0 <= self.launches <= 100:
            raise ValueError("launches must be in 0..100")
        if not 1 <= self.repeats <= 20:
            raise ValueError("repeats must be in 1..20")
        if not 1 <= self.max_tokens <= 4096:
            raise ValueError("max_tokens must be in 1..4096")
        if not 0.0 <= self.settle_s <= 60.0:
            raise ValueError("settle must be in 0..60 seconds")


@dataclass
class ForegroundRow:
    model_id: str
    engine: str
    type: str
    profile: str
    repeat: int
    status: str  # ok | unavailable | error
    detail: str = ""
    baseline: PhaseMeasure | None = None
    loaded: PhaseMeasure | None = None
    worker_share: float | None = None  # worker GPU busy share in the loaded probe window


@dataclass
class ForegroundReport:
    environment: dict[str, Any]
    config: dict[str, Any]
    probe_size: int | None = None
    results: list[ForegroundRow] = field(default_factory=list)

    def verdicts(self) -> dict[tuple[str, CapacityProfile], Nf7Verdict]:
        return aggregate(
            [
                (r.model_id, CapacityProfile(r.profile), r.baseline, r.loaded)
                for r in self.results
                if r.status == "ok" and r.baseline is not None and r.loaded is not None
            ]
        )

    @property
    def failed(self) -> bool:
        """Exit-1 condition: a judged (light/balanced) aggregate fails, or a judged row errored
        (no verdict can come from it). `full` and `unavailable` rows are informational."""
        errored = any(
            r.status == "error" and is_judged(CapacityProfile(r.profile)) for r in self.results
        )
        return errored or bool(failures(self.verdicts()))

    def to_json(self) -> dict[str, Any]:
        """Derived figures only: no timestamps, hostnames, paths or payloads."""

        def phase(m: PhaseMeasure | None) -> dict[str, Any] | None:
            return (
                None if m is None else {"frames": asdict(m.frames), "launch_p50_s": m.launch_p50_s}
            )

        return {
            "schema_version": SCHEMA_VERSION,
            "kind": "foreground-check",
            "environment": self.environment,
            "config": self.config,
            "probe_size": self.probe_size,
            "rows": [
                {
                    "model_id": r.model_id,
                    "engine": r.engine,
                    "type": r.type,
                    "profile": r.profile,
                    "repeat": r.repeat,
                    "status": r.status,
                    "detail": r.detail,
                    "worker_share": r.worker_share,
                    "baseline": phase(r.baseline),
                    "loaded": phase(r.loaded),
                }
                for r in self.results
            ],
            "verdicts": [
                {
                    "model_id": m,
                    "profile": p.value,
                    "judged": is_judged(p),
                    "partial": v.partial,
                    **asdict(v),
                }
                for (m, p), v in self.verdicts().items()
            ],
        }


def _phase(
    probe: FrameProbe, launcher: Launcher | None, cfg: ForegroundCheckConfig, size: int
) -> PhaseMeasure:
    res: ProbeResult = probe.run(cfg.duration_s, size)
    launches = launcher(cfg.launches) if launcher is not None and cfg.launches > 0 else None
    return PhaseMeasure(
        frames=frame_stats(res.latencies_ms, res.missed),
        launch_p50_s=statistics.median(launches) if launches else None,
    )


class _LoadThread:
    """Keeps the worker busy with back-to-back batches until stopped."""

    def __init__(
        self, worker: WorkerHandle, batch: list[dict[str, Any]], params: Any, budget: Budget
    ) -> None:
        self._stop = threading.Event()
        self.error: BaseException | None = None
        self._args = (worker, batch, params, budget)
        self._thread = threading.Thread(target=self._loop, name="fg-load", daemon=True)

    def _loop(self) -> None:
        worker, batch, params, budget = self._args
        try:
            while not self._stop.is_set():
                worker.run(batch, params, budget=budget)
        except BaseException as exc:  # reported as a row error, never swallowed
            self.error = exc

    def __enter__(self) -> _LoadThread:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._thread.join(timeout=120)


def _measure_row(
    worker: WorkerHandle,
    entry: ModelEntry,
    path: Path | None,
    budget: Budget,
    cfg: ForegroundCheckConfig,
    repeat: int,
    size: int,
    probe: FrameProbe,
    launcher: Launcher | None,
    sampler_factory: SamplerFactory,
) -> ForegroundRow:
    row = ForegroundRow(
        model_id=entry.model_id,
        engine=entry.engine.value,
        type=entry.type.value,
        profile=budget.profile.value,
        repeat=repeat,
        status="ok",
    )
    if entry.memory_gb > budget.memory_gb:
        row.status, row.detail = "unavailable", "model does not fit the profile's memory budget"
        return row
    params = _params(entry, BenchConfig(max_tokens=cfg.max_tokens))
    batch = synthetic_records(entry.type, budget.max_batch_size, cfg.seed)
    sampler = sampler_factory(lambda: worker.pid)
    sampler.start()
    try:
        worker.ensure_model(entry, budget, path)
        worker.run(batch, params, budget=budget)  # warm-up, excluded
        time.sleep(cfg.settle_s)
        row.baseline = _phase(probe, launcher, cfg, size)  # worker loaded but idle
        with _LoadThread(worker, batch, params, budget) as load:
            time.sleep(cfg.settle_s)
            t0 = time.monotonic()
            row.loaded = _phase(probe, launcher, cfg, size)
            t1 = t0 + cfg.duration_s
        load_error = load.error
    finally:
        samples = sampler.stop()
    if load_error is not None:
        row.status, row.detail = "error", f"background load failed: {type(load_error).__name__}"
        return row
    if not samples:
        raise GpuMeterError("the GPU sampler produced no samples")
    settle = min(SHARE_SETTLE_S, cfg.duration_s / 4)
    row.worker_share = process_gpu_share(samples, t0 + settle, min(t1, time.monotonic()))
    if row.worker_share < MIN_WORKER_SHARE:
        row.status, row.detail = "error", "background load did not run"
    return row


def _failure(
    entry: ModelEntry, profile: CapacityProfile, status: str, detail: str
) -> ForegroundRow:
    return ForegroundRow(
        entry.model_id, entry.engine.value, entry.type.value, profile.value, 1, status, detail
    )


async def run_foreground_check(
    cache: ModelCache,
    worker_factory: Callable[[Engine, Budget], WorkerHandle],
    entries: Sequence[ModelEntry],
    budgets: dict[CapacityProfile, Budget],
    cfg: ForegroundCheckConfig,
    *,
    probe: FrameProbe,
    launcher: Launcher | None,
    sampler_factory: SamplerFactory = make_sampler,
    meter_probe: Callable[[], object] = probe_meters,
    environment: dict[str, Any] | None = None,
) -> ForegroundReport:
    """NF-7 proxy per (entry, profile, repeat): frame probe + app launches with the worker idle,
    then again while it runs back-to-back batches at the profile's budget. A missing GPU meter or
    a probe failure (`ProbeError`) aborts the whole check."""
    meter_probe()
    report = ForegroundReport(
        environment=environment if environment is not None else host_environment(),
        config={**asdict(cfg), "profiles": [p.value for p in budgets]},
    )
    size = await asyncio.to_thread(probe.calibrate)
    report.probe_size = size
    for entry in entries:
        probe_worker = worker_factory(entry.engine, next(iter(budgets.values())))
        try:
            found = await asyncio.to_thread(probe_worker.probe, PROBE_TIMEOUT_S)
        except (RunnerError, WorkerCrashed) as exc:
            reason: str | None = f"probe failed: {type(exc).__name__}"
        else:
            reason = None if found.get(entry.engine.value) else "engine not available on this Mac"
        finally:
            await asyncio.to_thread(probe_worker.close)
        if reason is not None:
            report.results.extend(_failure(entry, p, "unavailable", reason) for p in budgets)
            continue
        try:
            path = await cache.ensure(entry)
        except (RunnerError, OSError) as exc:
            report.results.extend(
                _failure(entry, p, "error", f"model fetch failed: {type(exc).__name__}")
                for p in budgets
            )
            continue
        try:
            for profile, budget in budgets.items():
                for repeat in range(1, cfg.repeats + 1):
                    log.info("foreground_check", model_id=entry.model_id, profile=profile.value)
                    worker = worker_factory(entry.engine, budget)  # fresh per row
                    try:
                        row = await asyncio.to_thread(
                            _measure_row,
                            worker,
                            entry,
                            path,
                            budget,
                            cfg,
                            repeat,
                            size,
                            probe,
                            launcher,
                            sampler_factory,
                        )
                    except (RunnerError, WorkerCrashed) as exc:
                        row = _failure(entry, profile, "error", type(exc).__name__)
                        row.repeat = repeat
                    finally:
                        with contextlib.suppress(Exception):
                            await asyncio.to_thread(worker.unload)
                        await asyncio.to_thread(worker.close)
                    report.results.append(row)
        finally:
            cache.release_in_use()
    return report


# -- output --


def _f(v: float | None, digits: int = 2) -> str:
    return "-" if v is None else f"{v:.{digits}f}"


def _ok(v: bool | None) -> str:
    return "-" if v is None else ("pass" if v else "FAIL")


def render_foreground_table(report: ForegroundReport) -> str:
    """Per-row measurements, then the aggregate verdicts (median over repeats). `full` rows are
    informational and never judged."""
    lines = [
        "| model | profile | rep | status | worker GPU | p95 ms base | p95 ms loaded "
        "| miss % base | miss % loaded | launch s base | launch s loaded |",
        "|" + "---|" * 11,
    ]
    for r in report.results:
        b, ld = r.baseline, r.loaded
        status = r.status if not r.detail else f"{r.status} ({r.detail})"
        lines.append(
            f"| {r.model_id} | {r.profile} | {r.repeat} | {status} "
            f"| {'-' if r.worker_share is None else f'{r.worker_share * 100:.0f}%'} "
            f"| {_f(b.frames.p95_ms if b else None)} | {_f(ld.frames.p95_ms if ld else None)} "
            f"| {_f(b.frames.miss_rate * 100 if b else None, 1)} "
            f"| {_f(ld.frames.miss_rate * 100 if ld else None, 1)} "
            f"| {_f(b.launch_p50_s if b else None)} | {_f(ld.launch_p50_s if ld else None)} |"
        )
    lines += [
        "",
        "| model | profile | judged | p95 ratio (<= 1.10) | miss delta pp (<= 1.0) "
        "| launch ratio (<= 1.10) | verdict |",
        "|" + "---|" * 7,
    ]
    for (m, p), v in report.verdicts().items():
        verdict = ("pass" if v.passed else "FAIL") if is_judged(p) else "info"
        if v.partial:
            verdict += " (frames only)"
        lines.append(
            f"| {m} | {p.value} | {'yes' if is_judged(p) else 'no'} "
            f"| {_f(v.frame_p95_ratio)} {_ok(v.frame_ok)} | {_f(v.miss_delta_pp)} {_ok(v.miss_ok)} "
            f"| {_f(v.launch_p50_ratio)} {_ok(v.launch_ok)} | {verdict} |"
        )
    return "\n".join(lines)
