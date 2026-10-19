"""`minion-executor bench`: engine throughput on this Mac (D13, ADR-0005).

`run_bench` is engine-agnostic. It goes through the real path (model cache, then a sandboxed
`WorkerHandle`, then the runner) and uses only `ModelCache`, `WorkerHandle` and `stats()`; engines
are never imported here (invariant 7). No server and no token are involved.

Method, per (model, batch size): a fresh worker loads the model (`load_s`), one warm-up batch of
that size is run and excluded, then `records` synthetic records are run in batches of that size and
only those calls are timed (`run_s`). A fresh worker per batch size makes the memory peaks per row
instead of a running maximum. Payloads are never logged or written to the output.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.metadata
import platform
import random
import subprocess
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

import structlog
import yaml
from minion_common.enums import Engine, TaskType
from minion_common.schemas import Budget, CompletionParams, EmbeddingParams, ModelEntry, TaskParams

from minion_executor.model_cache import ModelCache
from minion_executor.runners.base import Record, RunnerError
from minion_executor.worker.process import WorkerCrashed, WorkerHandle

log = structlog.get_logger()

SCHEMA_VERSION = 1
GIB = 1024**3
# Bench probes keep a fixed 90 s (a hand-run dev tool; not MINION_EXECUTOR_PROBE_TIMEOUT_S).
PROBE_TIMEOUT_S = 90.0
DEFAULT_RECORDS = 256
DEFAULT_BATCH_SIZES = (1, 8, 32)
DEFAULT_MAX_TOKENS = 128
MAX_RECORDS = 100_000
MAX_BATCH_SIZE = 4096
LIBRARIES = ("mlx", "mlx-lm", "torch", "transformers", "safetensors")

Status = Literal["ok", "unavailable", "error"]

_WORDS = [
    "river",
    "lantern",
    "copper",
    "meadow",
    "signal",
    "harbor",
    "velvet",
    "orbit",
    "window",
    "paper",
    "engine",
    "garden",
    "thunder",
    "pencil",
    "marble",
    "forest",
    "ladder",
    "compass",
    "whisper",
    "candle",
    "bridge",
    "anchor",
    "falcon",
    "summit",
    "pocket",
    "mirror",
    "saddle",
    "tunnel",
    "violet",
    "button",
    "carpet",
    "desert",
    "feather",
    "glacier",
    "hammer",
    "island",
    "jungle",
    "kettle",
    "lemon",
    "magnet",
    "needle",
    "ocean",
    "pepper",
    "quartz",
    "rocket",
    "sponge",
    "timber",
    "umbrella",
    "valley",
    "walnut",
    "yellow",
    "zephyr",
    "basket",
    "cobble",
    "dagger",
    "ember",
    "fabric",
    "gravel",
    "hollow",
    "ivory",
]


@dataclass(frozen=True)
class BenchConfig:
    records: int = DEFAULT_RECORDS
    batch_sizes: tuple[int, ...] = DEFAULT_BATCH_SIZES
    max_tokens: int = DEFAULT_MAX_TOKENS
    seed: int = 0

    def __post_init__(self) -> None:
        if not 1 <= self.records <= MAX_RECORDS:
            raise ValueError(f"records must be in 1..{MAX_RECORDS}")
        if not self.batch_sizes or any(not 1 <= b <= MAX_BATCH_SIZE for b in self.batch_sizes):
            raise ValueError(f"batch sizes must be in 1..{MAX_BATCH_SIZE}")
        if not 1 <= self.max_tokens <= 4096:
            raise ValueError("max_tokens must be in 1..4096")


@dataclass(frozen=True)
class PendingModel:
    """A model listed in the bench file but not benchable yet (e.g. an unpinned Ollama digest).
    Reported as `unavailable` with `reason`; deliberately not a `ModelEntry`, so registry
    validation stays strict."""

    model_id: str
    type: TaskType
    engine: Engine
    ref: str
    reason: str


@dataclass
class BenchResult:
    model_id: str
    engine: str
    type: str
    status: Status
    detail: str = ""  # short reason for unavailable/error; never payload
    batch_size: int | None = None
    records: int = 0
    error_records: int = 0
    load_s: float | None = None
    run_s: float | None = None
    prompt_tokens: int = 0
    completion_tokens: int = 0
    tok_s: float | None = None
    prompt_tok_s: float | None = None
    records_per_h: float | None = None
    engine_peak_gb: float | None = None
    worker_rss_peak_gb: float | None = None
    engine_version: str | None = None


@dataclass
class BenchReport:
    environment: dict[str, Any]
    config: dict[str, Any]
    results: list[BenchResult] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "environment": self.environment,
            "config": self.config,
            "results": [asdict(r) for r in self.results],
        }


# -- inputs --


def synthetic_records(type: TaskType, n: int, seed: int = 0) -> list[Record]:
    """Deterministic fake records: the same (type, n, seed) always gives the same list."""
    rng = random.Random(f"{type.value}:{seed}")  # noqa: S311 - not security
    out: list[Record] = []
    for i in range(n):
        if type is TaskType.EMBEDDING:
            text = " ".join(rng.choices(_WORDS, k=rng.randint(30, 80)))
            out.append({"id": f"bench-{i}", "text": text})
        else:
            topic = " ".join(rng.choices(_WORDS, k=rng.randint(3, 6)))
            out.append(
                {"id": f"bench-{i}", "prompt": f"Write a long, detailed essay about: {topic}."}
            )
    return out


def parse_batch_sizes(raw: str) -> tuple[int, ...]:
    try:
        sizes = tuple(int(p) for p in raw.split(",") if p.strip())
    except ValueError:
        raise ValueError("batch sizes must be comma-separated integers") from None
    if not sizes or any(not 1 <= s <= MAX_BATCH_SIZE for s in sizes):
        raise ValueError(f"batch sizes must be integers in 1..{MAX_BATCH_SIZE}")
    return sizes


def load_bench_models(path: Path) -> tuple[list[ModelEntry], list[PendingModel]]:
    """Read the bench file: strict `models:` plus optional `pending:` (reported unavailable)."""
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(raw, dict):
            raise ValueError("top level must be a mapping")
        models, pend = raw.get("models") or [], raw.get("pending") or []
        if not isinstance(models, list) or not isinstance(pend, list):
            raise ValueError("`models` and `pending` must be lists")
        entries = [ModelEntry.model_validate(m) for m in models]
        pending = [
            PendingModel(
                model_id=str(p["model_id"]),
                type=TaskType(p["type"]),
                engine=Engine(p["engine"]),
                ref=str(p["ref"]),
                reason=str(p.get("reason", "not pinned")),
            )
            for p in pend
        ]
    except yaml.YAMLError as exc:
        raise ValueError(f"invalid YAML in bench models file: {type(exc).__name__}") from None
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError(f"invalid bench models file: {type(exc).__name__}: {exc}") from None
    ids = [e.model_id for e in entries] + [p.model_id for p in pending]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate model_id in bench models file")
    return entries, pending


# -- environment --


def _sysctl(name: str) -> str | None:
    try:
        out = subprocess.run(  # noqa: S603 - fixed argv
            ["/usr/sbin/sysctl", "-n", name],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout.strip() or None if out.returncode == 0 else None


def library_versions() -> dict[str, str | None]:
    """Installed versions from package metadata only (no engine import in this process)."""
    out: dict[str, str | None] = {}
    for name in LIBRARIES:
        try:
            out[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            out[name] = None
    return out


def host_environment() -> dict[str, Any]:
    mem = _sysctl("hw.memsize")
    return {
        "chip": _sysctl("machdep.cpu.brand_string") or platform.machine() or "unknown",
        "macos": platform.mac_ver()[0] or platform.platform(),
        "memory_gb": round(int(mem) / GIB, 1) if mem and mem.isdigit() else None,
        "python": platform.python_version(),
        "libraries": library_versions(),
    }


# -- maths --


def rates(
    records: int, run_s: float, prompt_tokens: int, completion_tokens: int
) -> tuple[float | None, float | None, float | None]:
    """`(tok_s, prompt_tok_s, records_per_h)` for a timed run. Token rates are `None` when the
    engine reported no tokens of that kind (e.g. completion tokens for embeddings)."""
    if run_s <= 0:
        return None, None, None
    tok_s = completion_tokens / run_s if completion_tokens else None
    prompt_tok_s = prompt_tokens / run_s if prompt_tokens else None
    return tok_s, prompt_tok_s, records / run_s * 3600.0


def _params(entry: ModelEntry, config: BenchConfig) -> TaskParams:
    if entry.type is TaskType.EMBEDDING:
        return EmbeddingParams()
    return CompletionParams(max_tokens=config.max_tokens, temperature=0.0)


def _count(records: Sequence[Record], key: str) -> int:
    return sum(v for r in records if isinstance(v := r.get(key), int) and not isinstance(v, bool))


# -- run --


def _bench_one_size(
    worker: WorkerHandle,
    entry: ModelEntry,
    path: Path | None,
    budget: Budget,
    config: BenchConfig,
    batch_size: int,
    version: str | None,
) -> BenchResult:
    res = BenchResult(
        model_id=entry.model_id,
        engine=entry.engine.value,
        type=entry.type.value,
        status="ok",
        batch_size=batch_size,
        engine_version=version,
    )
    params = _params(entry, config)
    records = synthetic_records(entry.type, config.records + batch_size, config.seed)
    warmup, timed = records[:batch_size], records[batch_size:]
    t0 = time.perf_counter()
    worker.ensure_model(entry, budget, path)
    res.load_s = time.perf_counter() - t0
    worker.run(warmup, params)  # excluded: kernel compilation, caches, lazy allocation
    run_s = 0.0
    done: list[Record] = []
    for i in range(0, len(timed), batch_size):
        chunk = timed[i : i + batch_size]
        t = time.perf_counter()
        out = worker.run(chunk, params)
        run_s += time.perf_counter() - t
        done.extend(out)
    stats = worker.stats()
    res.run_s = run_s
    res.records = len(timed)
    res.error_records = sum(1 for r in done if "error" in r)
    if res.error_records == res.records:
        res.status = "error"
        res.detail = "every record failed"
    # embedding runners report per-record `tokens` (input tokens); completions `prompt_tokens`.
    # Failed records are excluded from every count so they cannot inflate throughput.
    good = [r for r in done if "error" not in r]
    res.prompt_tokens = _count(
        good, "tokens" if entry.type is TaskType.EMBEDDING else "prompt_tokens"
    )
    res.completion_tokens = _count(good, "completion_tokens")
    if res.status != "error":
        res.tok_s, res.prompt_tok_s, res.records_per_h = rates(
            len(good), run_s, res.prompt_tokens, res.completion_tokens
        )
    res.engine_peak_gb = stats.get("peak_bytes", 0) / GIB
    res.worker_rss_peak_gb = stats.get("max_rss_bytes", 0) / GIB
    return res


def _failure(entry: ModelEntry | PendingModel, status: Status, detail: str) -> BenchResult:
    return BenchResult(
        model_id=entry.model_id,
        engine=entry.engine.value,
        type=entry.type.value,
        status=status,
        detail=detail,
    )


def _code(exc: BaseException) -> str:
    """A short, payload-free description of a failure."""
    if isinstance(exc, RunnerError):
        return exc.code
    return type(exc).__name__


async def run_bench(
    cache: ModelCache,
    worker_factory: Callable[[Engine], WorkerHandle],
    entries: Sequence[ModelEntry],
    config: BenchConfig,
    budget: Budget,
    *,
    pending: Sequence[PendingModel] = (),
    environment: dict[str, Any] | None = None,
) -> BenchReport:
    """Benchmark every entry at every batch size. An engine that does not probe as available, a
    pending model, or a model that fails shows up as a result row and never aborts the run."""
    report = BenchReport(
        environment=environment if environment is not None else host_environment(),
        config=asdict(config),
    )
    versions: dict[Engine, str | None] = {}
    unavailable: dict[Engine, str] = {}
    for engine in dict.fromkeys(e.engine for e in entries):
        probe_worker = worker_factory(engine)
        try:
            found = await asyncio.to_thread(probe_worker.probe, PROBE_TIMEOUT_S)
        except (RunnerError, WorkerCrashed) as exc:
            unavailable[engine] = f"probe failed: {_code(exc)}"
        else:
            if found.get(engine.value):
                versions[engine] = found[engine.value]
            else:
                unavailable[engine] = "engine not available on this Mac"
        finally:
            await asyncio.to_thread(probe_worker.close)

    for entry in entries:
        if entry.engine in unavailable:
            report.results.append(_failure(entry, "unavailable", unavailable[entry.engine]))
            continue
        report.results.extend(
            await _bench_model(cache, worker_factory, entry, config, budget, versions)
        )
    report.results.extend(_failure(p, "unavailable", p.reason) for p in pending)
    return report


async def _bench_model(
    cache: ModelCache,
    worker_factory: Callable[[Engine], WorkerHandle],
    entry: ModelEntry,
    config: BenchConfig,
    budget: Budget,
    versions: dict[Engine, str | None],
) -> list[BenchResult]:
    log.info("bench_model", model_id=entry.model_id, engine=entry.engine.value)
    out: list[BenchResult] = []
    try:
        path = await cache.ensure(entry)
    except (RunnerError, OSError) as exc:  # ModelFetchError is a RunnerError
        return [_failure(entry, "error", f"model fetch failed: {_code(exc)}")]
    try:
        for size in config.batch_sizes:
            worker = worker_factory(entry.engine)  # fresh worker: per-row memory peaks
            try:
                out.append(
                    await asyncio.to_thread(
                        _bench_one_size,
                        worker,
                        entry,
                        path,
                        budget,
                        config,
                        size,
                        versions.get(entry.engine),
                    )
                )
            except (RunnerError, WorkerCrashed) as exc:
                row = _failure(entry, "error", _code(exc))
                row.batch_size = size
                out.append(row)
            finally:
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(worker.unload)
                await asyncio.to_thread(worker.close)
    finally:
        cache.release_in_use()
    return out


# -- output --


def _f(v: float | None, digits: int = 1) -> str:
    return "-" if v is None else f"{v:.{digits}f}"


def render_table(report: BenchReport) -> str:
    """Markdown table, one row per (model, batch size)."""
    head = (
        "| model | engine | type | batch | status | errors | load s | run s | tok/s | prompt tok/s "
        "| records/h | engine peak GB | worker RSS peak GB |"
    )
    lines = [head, "|" + "---|" * 13]
    for r in report.results:
        status = r.status if not r.detail else f"{r.status} ({r.detail})"
        batch = "-" if r.batch_size is None else str(r.batch_size)
        lines.append(
            f"| {r.model_id} | {r.engine} | {r.type} | {batch} | {status} | {r.error_records} "
            f"| {_f(r.load_s, 2)} "
            f"| {_f(r.run_s, 2)} | {_f(r.tok_s)} | {_f(r.prompt_tok_s)} | {_f(r.records_per_h, 0)} "
            f"| {_f(r.engine_peak_gb, 2)} | {_f(r.worker_rss_peak_gb, 2)} |"
        )
    return "\n".join(lines)
