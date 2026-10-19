"""`minion-executor bench --budget-check` and `--cpu-check` (TASK-5.6, ADR-0006 Gap 1, NF-4/NF-8).

Budget check, per (model, profile, repeat), through the real worker path (model cache, sandboxed
`WorkerHandle`, runner), engine-agnostic like `bench.py`:

1. a fresh worker loads the model and runs one warm-up batch (excluded);
2. idle phase for `baseline_s`: only the device-wide utilization is read (informational);
3. unpaced phase at duty 1.0 -> `unpaced_share` (skipped for a profile whose duty is 1.0);
4. a 2 s gap, then the paced phase at the profile's `gpu_duty_cycle` -> `measured_share`.

Primary metric: the worker pid's GPU time. `ioreg` lists the worker's Metal user clients with
`accumulatedGPUTime` (ns); the delta over a phase window divided by the window's wall time is the
share of the GPU our process kept busy (`gpu_meter.process_gpu_share`). It needs no baseline: other
apps, the display and frequency scaling do not enter. The first 1.5 s of each phase are dropped (the
pacer and the GPU clock settle). `Device Utilization %` is device-wide and DVFS-affected; it is
reported as an informational column only. Sampling is ~2 Hz from the control process, started
before the model loads, so the footprint peak (10 Hz, `WorkerHandle.footprint_bytes`, i.e.
`phys_footprint`) includes the load. The pacer's own `busy_s / wall_s` is reported too (advisory:
the worker is the party being limited). Verdicts follow ADR-0006 Gap 1; see `nf8_verdict`. Rows
with status `unavailable` (engine missing, model too big for the profile) are excluded from
pass/fail; any `error` row fails the check. Payloads are never logged or written.

`--cpu-check` (NF-4) measures the CPU of this process while a real `GovernorRuntime` polls, i.e. the
control-process governor only. A whole `minion-executor run` (control plus worker process tree) is
measured with `ps`, see docs/bench/governor-measurements.md.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import statistics
import tempfile
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import structlog
from minion_common.enums import CapacityProfile, Engine
from minion_common.schemas import Budget, ModelEntry

from minion_executor.bench import (
    GIB,
    PROBE_TIMEOUT_S,
    SCHEMA_VERSION,
    BenchConfig,
    _params,
    host_environment,
    synthetic_records,
)
from minion_executor.capacity.config import ExecutorConfig, resolve_profiles
from minion_executor.capacity.control import OwnerControl, write_control
from minion_executor.capacity.gpu_meter import (
    GpuMeterError,
    GpuSample,
    GpuSampler,
    make_sampler,
    probe_meters,
    process_gpu_share,
    window_percent,
)
from minion_executor.capacity.runtime import GovernorRuntime
from minion_executor.capacity.signals import SignalSnapshot, SignalSource
from minion_executor.model_cache import ModelCache
from minion_executor.runners.base import RunnerError
from minion_executor.worker.process import WorkerCrashed, WorkerHandle

log = structlog.get_logger()

SETTLE_S = 1.5  # dropped from the start of each phase window
PHASE_GAP_S = 2.0  # idle between the unpaced and the paced phase
TOLERANCE = 0.10  # NF-8: +-10 pp (accuracy) and +10 pp (hard bound)
MEMORY_FACTOR = 1.10  # NF-8 / D7: worker footprint <= memory_gb x 1.10
DEFAULT_DURATION_S = 60.0
DEFAULT_BASELINE_S = 20.0
FOOTPRINT_INTERVAL_S = 0.1  # 10 Hz
CPU_LIMIT_PCT = 1.0  # NF-4

# -- verdicts (pure) --


@dataclass(frozen=True)
class Nf8Verdict:
    expected_share: float | None  # duty x unpaced share (None: no unpaced phase, duty 1.0)
    hard_ok: bool  # measured <= duty + 10 pp (the invariant-6 check)
    accuracy_ok: bool | None  # |measured - expected| <= 10 pp (None: nothing to compare with)
    memory_ok: bool  # footprint peak <= memory_gb x 1.10

    @property
    def passed(self) -> bool:
        return self.hard_ok and self.accuracy_ok is not False and self.memory_ok


def nf8_verdict(
    *,
    duty: float,
    measured: float,
    unpaced: float | None,
    footprint_peak_gb: float,
    memory_gb: float,
) -> Nf8Verdict:
    """NF-8 as restated in ADR-0006 Gap 1. Shares are fractions (0..1) of the worker's own GPU time.

    Hard bound: `measured <= duty + 0.10`. Accuracy: `|measured - duty x unpaced| <= 0.10`, where
    `unpaced` is the same workload's share with the pacer off, measured the same way, so CPU-bound
    phases the pacer cannot compress are not held against it; without an unpaced phase (duty 1.0)
    there is no accuracy verdict. Memory: `footprint_peak_gb <= memory_gb x 1.10`."""
    for name, v in (("duty", duty), ("measured", measured), ("unpaced", unpaced)):
        if v is not None and (not math.isfinite(v) or v < 0):
            raise ValueError(f"{name} must be a finite non-negative number")
    expected = None if unpaced is None else duty * unpaced
    return Nf8Verdict(
        expected_share=expected,
        hard_ok=measured <= duty + TOLERANCE + 1e-9,
        accuracy_ok=None if expected is None else abs(measured - expected) <= TOLERANCE + 1e-9,
        memory_ok=footprint_peak_gb <= memory_gb * MEMORY_FACTOR + 1e-9,
    )


# -- results --


@dataclass
class BudgetCheckResult:
    model_id: str
    engine: str
    type: str
    profile: str
    repeat: int
    status: str  # ok | unavailable | error
    detail: str = ""
    duty: float | None = None
    budget_memory_gb: float | None = None
    max_batch_size: int | None = None
    unpaced_share: float | None = None
    measured_share: float | None = None
    max_window_share: float | None = None  # highest sliding GPU_GUARD_WINDOW_S share (paced phase)
    gpu_clients: int | None = None  # distinct GPU user-client ids of the worker pid in the run
    expected_share: float | None = None
    worker_reported_share: float | None = None
    unpaced_reported_share: float | None = None
    footprint_peak_gb: float | None = None
    worker_rss_peak_gb: float | None = None
    engine_peak_gb: float | None = None
    device_baseline_pct: float | None = None  # informational: device-wide, not per process
    device_unpaced_pct: float | None = None
    device_paced_pct: float | None = None
    gpu_samples: int = 0
    duration_s: float = 0.0
    records_done: int = 0
    hard_ok: bool | None = None
    accuracy_ok: bool | None = None
    memory_ok: bool | None = None
    passed: bool | None = None


@dataclass
class BudgetCheckReport:
    environment: dict[str, Any]
    config: dict[str, Any]
    results: list[BudgetCheckResult] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "kind": "budget-check",
            "environment": self.environment,
            "config": self.config,
            "results": [asdict(r) for r in self.results],
        }

    @property
    def all_passed(self) -> bool:
        """Every `ok` row passes and no row errored. `unavailable` rows (engine missing, model too
        big for the profile) are not judged; with nothing judged the check does not pass."""
        ok = [r for r in self.results if r.status == "ok"]
        return (
            bool(ok)
            and all(r.passed for r in ok)
            and not any(r.status == "error" for r in self.results)
        )


@dataclass(frozen=True)
class BudgetCheckConfig:
    duration_s: float = DEFAULT_DURATION_S
    baseline_s: float = DEFAULT_BASELINE_S
    repeats: int = 1
    max_tokens: int = 128
    seed: int = 0

    def __post_init__(self) -> None:
        if not 1.0 <= self.duration_s <= 3600.0:
            raise ValueError("duration must be in 1..3600 seconds")
        if not 1.0 <= self.baseline_s <= 600.0:
            raise ValueError("baseline must be in 1..600 seconds")
        if not 1 <= self.repeats <= 20:
            raise ValueError("repeats must be in 1..20")
        if not 1 <= self.max_tokens <= 4096:
            raise ValueError("max_tokens must be in 1..4096")


def parse_profiles(raw: str) -> list[CapacityProfile]:
    try:
        out = [CapacityProfile(p.strip()) for p in raw.split(",") if p.strip()]
    except ValueError:
        raise ValueError("profiles must be a comma-separated list of profile names") from None
    if not out:
        raise ValueError("no profiles given")
    return list(dict.fromkeys(out))


# -- measurement --


class _FootprintSampler:
    """10 Hz peak of the worker footprint, from the control side."""

    def __init__(self, worker: WorkerHandle) -> None:
        self._worker = worker
        self._stop = threading.Event()
        self.peak = 0
        self._thread = threading.Thread(target=self._loop, name="footprint", daemon=True)

    def _loop(self) -> None:
        while not self._stop.is_set():
            with contextlib.suppress(Exception):
                v = self._worker.footprint_bytes()
                if v is not None:
                    self.peak = max(self.peak, v)
            self._stop.wait(FOOTPRINT_INTERVAL_S)

    def __enter__(self) -> _FootprintSampler:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._thread.join(timeout=5)


@dataclass
class _Phase:
    t0: float
    t1: float
    reported_share: float | None
    records: int


def _with_duty(budget: Budget, duty: float) -> Budget:
    return Budget.model_validate({**budget.model_dump(), "gpu_duty_cycle": duty})


def _run_phase(
    worker: WorkerHandle,
    batch: list[dict[str, Any]],
    params: Any,
    budget: Budget,
    duration_s: float,
) -> _Phase:
    """Run `batch` repeatedly for `duration_s`. The reported share is the pacer's busy/wall of
    each call, weighted by the call's wall time."""
    t0 = time.monotonic()
    weighted = total = 0.0
    done = 0
    while time.monotonic() - t0 < duration_s:
        t = time.monotonic()
        out = worker.run(batch, params, budget=budget)
        dt = time.monotonic() - t
        done += sum(1 for r in out if "error" not in r)
        if (s := worker.gpu_share()) is not None:
            weighted += s * dt
            total += dt
    return _Phase(t0, time.monotonic(), weighted / total if total else None, done)


SamplerFactory = Callable[[Callable[[], int | None]], GpuSampler]  # worker pid getter -> sampler


@dataclass
class _Phases:
    idle: tuple[float, float]  # (t0, t1) of the idle phase
    unpaced: _Phase | None
    paced: _Phase


def _run_phases(
    worker: WorkerHandle,
    entry: ModelEntry,
    path: Path | None,
    budget: Budget,
    cfg: BudgetCheckConfig,
) -> _Phases:
    """Load, warm up (excluded), idle, unpaced (duty < 1 only), gap, paced."""
    unpaced_budget = _with_duty(budget, 1.0)
    params = _params(entry, BenchConfig(max_tokens=cfg.max_tokens))
    batch = synthetic_records(entry.type, budget.max_batch_size, cfg.seed)
    worker.ensure_model(entry, unpaced_budget, path)
    worker.run(batch, params, budget=unpaced_budget)  # warm-up, excluded
    t_idle = time.monotonic()
    time.sleep(cfg.baseline_s)  # nothing of ours is running
    idle = (t_idle, time.monotonic())
    unpaced: _Phase | None = None
    if budget.gpu_duty_cycle < 1.0:
        unpaced = _run_phase(worker, batch, params, unpaced_budget, cfg.duration_s)
        time.sleep(PHASE_GAP_S)  # let the GPU go quiet so the phases do not bleed into each other
    return _Phases(idle, unpaced, _run_phase(worker, batch, params, budget, cfg.duration_s))


def _device_pct(samples: list[GpuSample], t0: float, t1: float) -> float | None:
    values = window_percent(samples, t0, t1)
    return statistics.fmean(values) if values else None


GPU_GUARD_WINDOW_S = 10.0  # the run-time guard's window (capacity/gpu_guard.py, TASK-15)


def max_window_share(
    samples: list[GpuSample], start: float, end: float, window_s: float = GPU_GUARD_WINDOW_S
) -> float | None:
    """Highest share over any `window_s` sliding window ending at a sample inside `[start, end]`
    (what the run-time GPU duty guard judges). None if the span is shorter than one window."""
    best: float | None = None
    for s in samples:
        if s.clients is None or s.at < start + window_s or s.at > end:
            continue
        try:
            v = process_gpu_share(samples, s.at - window_s, s.at)
        except GpuMeterError:
            continue
        best = v if best is None else max(best, v)
    return best


def distinct_clients(samples: list[GpuSample]) -> int:
    """Number of distinct GPU user-client ids seen for the worker pid (the guard's max_clients)."""
    ids: set[str] = set()
    for s in samples:
        ids.update(s.clients or {})
    return len(ids)


def _fill_shares(
    res: BudgetCheckResult, samples: list[GpuSample], ph: _Phases, cfg: BudgetCheckConfig
) -> None:
    """Per-process GPU shares (primary) and device utilization (informational), settled windows."""
    if not samples:
        raise GpuMeterError("the GPU sampler produced no samples")
    settle = min(SETTLE_S, cfg.duration_s / 4)
    res.measured_share = process_gpu_share(samples, ph.paced.t0 + settle, ph.paced.t1)
    res.max_window_share = max_window_share(samples, ph.paced.t0 + settle, ph.paced.t1)
    res.gpu_clients = distinct_clients(samples)
    res.device_paced_pct = _device_pct(samples, ph.paced.t0 + settle, ph.paced.t1)
    res.device_baseline_pct = _device_pct(samples, *ph.idle)
    if ph.unpaced is not None:
        res.unpaced_share = process_gpu_share(samples, ph.unpaced.t0 + settle, ph.unpaced.t1)
        res.device_unpaced_pct = _device_pct(samples, ph.unpaced.t0 + settle, ph.unpaced.t1)
        res.unpaced_reported_share = ph.unpaced.reported_share
    res.worker_reported_share = ph.paced.reported_share
    res.gpu_samples = len(samples)
    res.duration_s = ph.paced.t1 - ph.paced.t0
    res.records_done = ph.paced.records


UNPACED_SHARE_MAX = 1.05  # a process cannot keep one GPU busy more than its wall time


def _attribution_anomaly(res: BudgetCheckResult, ph: _Phases) -> str | None:
    """A meter that attributes nothing (or too much) must never pass as a quiet or exact GPU."""
    if ph.paced.records > 0 and not (res.measured_share and res.measured_share > 0):
        return "no GPU time attributed to the worker"
    if (
        ph.unpaced is not None
        and ph.unpaced.records > 0
        and not (res.unpaced_share and res.unpaced_share > 0)
    ):
        return "no GPU time attributed to the worker in the unpaced phase"
    if res.unpaced_share is not None and res.unpaced_share > UNPACED_SHARE_MAX:
        return f"unpaced share {res.unpaced_share:.2f} > {UNPACED_SHARE_MAX}: meter anomaly"
    return None


def _judge(res: BudgetCheckResult, budget: Budget) -> None:
    if res.measured_share is None or res.footprint_peak_gb is None:
        raise RuntimeError("_judge needs the measurements first")
    v = nf8_verdict(
        duty=budget.gpu_duty_cycle,
        measured=res.measured_share,
        unpaced=res.unpaced_share,
        footprint_peak_gb=res.footprint_peak_gb,
        memory_gb=budget.memory_gb,
    )
    res.expected_share = v.expected_share
    res.hard_ok, res.accuracy_ok, res.memory_ok, res.passed = (
        v.hard_ok,
        v.accuracy_ok,
        v.memory_ok,
        v.passed,
    )


def _check_one(
    worker: WorkerHandle,
    entry: ModelEntry,
    path: Path | None,
    budget: Budget,
    cfg: BudgetCheckConfig,
    repeat: int,
    sampler_factory: SamplerFactory,
) -> BudgetCheckResult:
    res = BudgetCheckResult(
        model_id=entry.model_id,
        engine=entry.engine.value,
        type=entry.type.value,
        profile=budget.profile.value,
        repeat=repeat,
        status="ok",
        duty=budget.gpu_duty_cycle,
        budget_memory_gb=budget.memory_gb,
        max_batch_size=budget.max_batch_size,
    )
    if entry.memory_gb > budget.memory_gb:
        res.status, res.detail = "unavailable", "model does not fit the profile's memory budget"
        return res
    sampler = sampler_factory(lambda: worker.pid)
    sampler.start()  # both samplers run from before the load: the peak includes it
    try:
        with _FootprintSampler(worker) as fp:
            phases = _run_phases(worker, entry, path, budget, cfg)
    finally:
        samples = sampler.stop()
    _fill_shares(res, samples, phases, cfg)
    if (anomaly := _attribution_anomaly(res, phases)) is not None:
        res.status, res.detail = "error", anomaly
        return res
    stats = worker.stats()
    res.footprint_peak_gb = fp.peak / GIB
    res.worker_rss_peak_gb = stats.get("max_rss_bytes", 0) / GIB
    res.engine_peak_gb = stats.get("peak_bytes", 0) / GIB
    if phases.paced.records == 0:
        res.status, res.detail = "error", "no record succeeded"
        return res
    _judge(res, budget)
    return res


def _failure(
    entry: ModelEntry, profile: CapacityProfile, repeat: int, status: str, detail: str
) -> BudgetCheckResult:
    return BudgetCheckResult(
        model_id=entry.model_id,
        engine=entry.engine.value,
        type=entry.type.value,
        profile=profile.value,
        repeat=repeat,
        status=status,
        detail=detail,
    )


async def run_budget_check(
    cache: ModelCache,
    worker_factory: Callable[[Engine, Budget], WorkerHandle],
    entries: Sequence[ModelEntry],
    budgets: dict[CapacityProfile, Budget],
    cfg: BudgetCheckConfig,
    *,
    environment: dict[str, Any] | None = None,
    sampler_factory: SamplerFactory = make_sampler,
    meter_probe: Callable[[], object] = probe_meters,
) -> BudgetCheckReport:
    """Check every (entry, profile, repeat). A failing model/engine becomes a row, but a missing
    GPU meter aborts the whole check (`GpuMeterError`): there is nothing to measure without it."""
    meter_probe()  # fail fast and clearly off macOS / without an Apple GPU
    report = BudgetCheckReport(
        environment=environment if environment is not None else host_environment(),
        config={
            **asdict(cfg),
            "profiles": [p.value for p in budgets],
            "tolerance": TOLERANCE,
            "memory_factor": MEMORY_FACTOR,
        },
    )
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
            report.results.extend(_failure(entry, p, 1, "unavailable", reason) for p in budgets)
            continue
        try:
            path = await cache.ensure(entry)
        except (RunnerError, OSError) as exc:
            report.results.extend(
                _failure(entry, p, 1, "error", f"model fetch failed: {type(exc).__name__}")
                for p in budgets
            )
            continue
        try:
            for profile, budget in budgets.items():
                for repeat in range(1, cfg.repeats + 1):
                    log.info("budget_check", model_id=entry.model_id, profile=profile.value)
                    worker = worker_factory(entry.engine, budget)  # fresh: per-row memory peaks
                    try:
                        row = await asyncio.to_thread(
                            _check_one, worker, entry, path, budget, cfg, repeat, sampler_factory
                        )
                    except GpuMeterError:
                        raise
                    except (RunnerError, WorkerCrashed) as exc:
                        row = _failure(entry, profile, repeat, "error", type(exc).__name__)
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


def _pct(v: float | None) -> str:
    return "-" if v is None else f"{v * 100:.0f}%"


def _verdict(v: bool | None) -> str:
    return "-" if v is None else ("pass" if v else "FAIL")


def _dev(r: BudgetCheckResult) -> str:
    def one(v: float | None) -> str:
        return "-" if v is None else f"{v:.0f}"

    return f"{one(r.device_baseline_pct)}/{one(r.device_unpaced_pct)}/{one(r.device_paced_pct)}%"


def render_budget_table(report: BudgetCheckReport) -> str:
    """Markdown table, one row per (model, profile, repeat). Shares are the worker's own GPU time;
    the device column (idle/unpaced/paced utilization, device-wide) is informational."""
    head = (
        "| model | profile | rep | status | duty | unpaced | expected | measured "
        "| max 10 s window | GPU clients | pacer-reported | device idle/unpaced/paced (info) "
        "| footprint GB | budget GB "
        "| hard (<= duty+10pp) | accuracy (+-10pp) | memory (<= 1.1x) |"
    )
    lines = [head, "|" + "---|" * 17]
    for r in report.results:
        status = r.status if not r.detail else f"{r.status} ({r.detail})"
        lines.append(
            f"| {r.model_id} | {r.profile} | {r.repeat} | {status} | {_pct(r.duty)} "
            f"| {_pct(r.unpaced_share)} | {_pct(r.expected_share)} | {_pct(r.measured_share)} "
            f"| {_pct(r.max_window_share)} | {'-' if r.gpu_clients is None else r.gpu_clients} "
            f"| {_pct(r.worker_reported_share)} | {_dev(r)} | {_f(r.footprint_peak_gb)} "
            f"| {_f(r.budget_memory_gb, 1)} | {_verdict(r.hard_ok)} | {_verdict(r.accuracy_ok)} "
            f"| {_verdict(r.memory_ok)} |"
        )
    return "\n".join(lines)


# -- NF-4: control-process CPU while idle / paused --


@dataclass
class CpuResult:
    state: str  # idle | paused
    poll_s: float
    duration_s: float
    cpu_s: float
    cpu_pct: float
    passed: bool


def cpu_percent(cpu_s: float, wall_s: float) -> float:
    if wall_s <= 0:
        raise ValueError("wall time must be positive")
    return 100.0 * cpu_s / wall_s


async def measure_governor_cpu(
    source: SignalSource,
    *,
    paused: bool,
    duration_s: float,
    poll_s: float,
    config: ExecutorConfig,
    total_gb: float,
    state_dir: Path | None = None,
    cpu_clock: Callable[[], float] = time.process_time,
    limit_pct: float = CPU_LIMIT_PCT,
) -> CpuResult:
    """CPU of this process (all threads) while a real `GovernorRuntime` polls `source` and nothing
    else runs: the idle (or owner-paused) executor control process, minus the server round trips.
    The 60 s `ps` procedure for a real `minion-executor run` is in
    docs/bench/governor-measurements.md."""
    with tempfile.TemporaryDirectory(prefix="minion-nf4-") as tmp:
        sd = state_dir or Path(tmp) / "state"
        if paused:
            write_control(sd, OwnerControl(paused=True))
        else:
            sd.mkdir(mode=0o700, exist_ok=True)
        gov = GovernorRuntime(
            source,
            config,
            resolve_profiles(config),
            state_dir=sd,
            total_gb=total_gb,
            poll_s=poll_s,
        )
        await gov.prime()
        task = asyncio.create_task(gov.run())
        try:
            await asyncio.sleep(min(1.0, duration_s / 4))  # settle (imports, first reads)
            c0, w0 = cpu_clock(), time.monotonic()
            await asyncio.sleep(duration_s)
            cpu_s, wall_s = cpu_clock() - c0, time.monotonic() - w0
            if paused and gov.decision.paused_reason is None:
                raise RuntimeError("the governor did not pause: the measurement would be invalid")
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
    pct = cpu_percent(cpu_s, wall_s)
    return CpuResult(
        state="paused" if paused else "idle",
        poll_s=poll_s,
        duration_s=wall_s,
        cpu_s=cpu_s,
        cpu_pct=pct,
        passed=pct < limit_pct,
    )


def render_cpu_table(results: Sequence[CpuResult]) -> str:
    lines = [
        "| state | poll s | window s | cpu s | cpu % | NF-4 (< 1 %) |",
        "|---|---|---|---|---|---|",
    ]
    lines += [
        f"| {r.state} | {r.poll_s:g} | {r.duration_s:.0f} | {r.cpu_s:.3f} | {r.cpu_pct:.3f} "
        f"| {'pass' if r.passed else 'FAIL'} |"
        for r in results
    ]
    return "\n".join(lines)


# -- NF-9 on a real Mac (opt-in; docs/bench/governor-measurements.md) --

RETURN_DROP_S = 5.0  # HID idle falling by more than this between two polls is a user return


def user_return_reaction_s(
    prev: SignalSnapshot | None, snap: SignalSnapshot, now: datetime
) -> float | None:
    """Seconds from the user's last input (back-dated: `taken_at - hid_idle_s`) to `now`, if
    `snap` shows the user came back since `prev` (idle time dropped by more than 5 s), else None.
    The HID idle counter restarts at the exact moment of the input, so T0 needs no stopwatch."""
    if prev is None or prev.hid_idle_s - snap.hid_idle_s <= RETURN_DROP_S:
        return None
    event = snap.taken_at.timestamp() - snap.hid_idle_s
    return max(0.0, now.timestamp() - event)


async def watch_reactions(
    source: SignalSource,
    *,
    duration_s: float,
    poll_s: float,
    config: ExecutorConfig,
    total_gb: float,
    emit: Callable[[str], None],
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> None:
    """Run a real `GovernorRuntime` on `source` for `duration_s` and print, with epoch seconds,
    every decision change and every user return. For memory pressure / thermal / power the operator
    prints the epoch (`python3 -c 'import time; print(time.time())'`) just before triggering the
    condition and subtracts it."""
    with tempfile.TemporaryDirectory(prefix="minion-nf9-") as tmp:
        sd = Path(tmp) / "state"
        sd.mkdir(mode=0o700)
        gov = GovernorRuntime(
            source, config, resolve_profiles(config), state_dir=sd, total_gb=total_gb, poll_s=poll_s
        )
        await gov.prime()
        task = asyncio.create_task(gov.run())
        end = time.monotonic() + duration_s
        version, prev = gov.version, gov.snapshot
        last_line = ""
        emit(f"{now().timestamp():.3f} start poll_s={poll_s:g}")
        try:
            while (left := end - time.monotonic()) > 0:
                version = await gov.wait_change(version, min(left, poll_s / 4))
                snap, t = gov.snapshot, now()
                if snap is not None and snap is not prev:
                    if (r := user_return_reaction_s(prev, snap, t)) is not None:
                        emit(f"{t.timestamp():.3f} user-return reaction_s={r:.3f}")
                    prev = snap
                d = gov.decision
                line = (
                    f"paused={d.paused_reason.value if d.paused_reason else '-'} "
                    f"profile={d.profile.value} duty={d.budget.gpu_duty_cycle:g} "
                    f"release_now={d.release_now}"
                )
                if line != last_line:
                    last_line = line
                    emit(f"{t.timestamp():.3f} decision {line}")
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
