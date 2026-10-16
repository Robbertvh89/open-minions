"""Executor CLI. `run` is the control loop; `bench` measures engines; `status` lands with M4."""

from __future__ import annotations

import asyncio
import json
import os
import platform
import signal
import socket
import stat
import subprocess
import sys
from pathlib import Path
from typing import Annotated
from urllib.parse import urlsplit

import httpx
import structlog
import typer
from minion_common.client import LOCAL_HOSTS, ExecutorClient, require_secure_url
from minion_common.enums import CapacityProfile, Engine
from minion_common.schemas import Budget, RegisterRequest
from pydantic import SecretStr

from minion_executor import bench as bench_mod
from minion_executor.capacity.config import load_config, resolve_profiles
from minion_executor.capacity.profiles import budget_for, nice_for, parse_profile, total_memory_gb
from minion_executor.heartbeat import check_interval
from minion_executor.loop import ExecutorLoop
from minion_executor.model_cache import FetchEnv, ModelCache
from minion_executor.runners.base import EngineConfig, RunnerError
from minion_executor.secrets import ENV_VAR, load_token
from minion_executor.settings import ExecutorSettings
from minion_executor.worker.process import WorkerCrashed, WorkerHandle, restrict_environ
from minion_executor.worker.sandbox import SandboxPolicy

app = typer.Typer(help="open-minions executor: donate idle Apple Silicon GPU capacity.")


def _chip() -> str:
    try:
        out = subprocess.run(
            ["/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass
    return platform.machine() or "unknown"


DEFAULT_HEARTBEAT_INTERVAL_S = 300.0
DEV_ENV = "MINION_EXECUTOR_DEV"
TEST_HOOKS_ENV = "MINION_EXECUTOR_TEST_HOOKS"
SERVER_TIMEOUT_ENV = "MINION_HEARTBEAT_TIMEOUT_S"  # server/watchdog setting; optional sanity check
MIN_TIMEOUT_FACTOR = 2.0
# `--engines auto` never includes Ollama: its daemon runs outside our sandbox and `/api/pull`
# writes to ~/.ollama outside the cache cap, so it needs an explicit `--engines ollama` (ADR-0009).
AUTO_ENGINES = [Engine.MLX, Engine.HF]
MAX_KEEP_ALIVE_S = 300
NO_ENGINE_MSG = (
    "no inference engine available (on Apple Silicon install the MLX extra: "
    "`uv sync --package minion-executor --extra mlx` (or `--extra hf`), or start Ollama and pass "
    "`--engines ollama`; "
    "use --engines fake with MINION_EXECUTOR_DEV=1 for development)"
)


class StartupError(Exception):
    """Startup failed (no engine, probe failure); the CLI maps it to exit 2."""


def _is_loopback(url: str) -> bool:
    return (urlsplit(url).hostname or "").lower() in LOCAL_HOSTS


def _require_dev_for_fake() -> None:
    if os.environ.get(DEV_ENV) != "1":
        raise ValueError("engine 'fake' serves no real work; set MINION_EXECUTOR_DEV=1 (dev only)")


def _requested_engines(engines: str, runner: str | None) -> list[Engine]:
    """Engines to probe. `auto` is the real engines only: `fake` is never implicit."""
    if runner is not None:  # hidden dev override
        if runner != "fake":
            raise ValueError(f"unknown runner: {runner!r}")
        _require_dev_for_fake()
        return [Engine.FAKE]
    if engines.strip().lower() == "auto":
        return list(AUTO_ENGINES)
    out: list[Engine] = []
    for name in (n.strip().lower() for n in engines.split(",")):
        try:
            engine = Engine(name)
        except ValueError:
            raise ValueError(f"unknown engine: {name!r}") from None
        if engine is Engine.FAKE:
            _require_dev_for_fake()
        if engine not in out:
            out.append(engine)
    if not out:
        raise ValueError("no engines requested")
    return out


PROBE_TIMEOUT_S = 90.0


def keep_alive_s(idle_unload_s: float) -> int:
    """How long Ollama may keep a model after a request: the idle unload time, capped. With idle
    unload disabled (0) it still gets a short bounded value, never "forever"."""
    return max(1, min(int(idle_unload_s), MAX_KEEP_ALIVE_S)) if idle_unload_s > 0 else 60


def sandbox_policy(settings: ExecutorSettings, engines: list[Engine]) -> SandboxPolicy | None:
    """The worker sandbox for these settings (`None` only with the dev opt-out)."""
    if not settings.sandbox:
        return None
    return SandboxPolicy(cache_root=settings.model_cache_dir)


def probe_available(requested: list[Engine], sandbox: SandboxPolicy | None = None) -> list[Engine]:
    """Ask a worker subprocess which of `requested` work here (engines are never imported in
    the control process, invariant 7). Call after `restrict_environ()` so the probe worker
    inherits no secrets. The probe worker is sandboxed like a real one, so a broken sandbox
    surfaces here as `SANDBOX_FAILED` (the CLI exits 2) and engine imports are proven to work
    under the profile."""
    found = WorkerHandle(requested, sandbox=sandbox).probe(PROBE_TIMEOUT_S)
    return [e for e in requested if found.get(e.value)]


def _validate_heartbeat(interval_s: float) -> None:
    """The interval must be finite and within [1 s, 1 h] (the upper bound keeps the refreshed
    presigned URL valid between beats). The server's heartbeat timeout must be >= 2x the interval,
    else live executors get reclaimed between beats; checked only when the operator exports it."""
    check_interval(interval_s)
    raw = os.environ.get(SERVER_TIMEOUT_ENV)
    if raw:
        try:
            timeout = float(raw)
        except ValueError:
            raise ValueError(f"{SERVER_TIMEOUT_ENV} is not a number") from None
        if timeout < MIN_TIMEOUT_FACTOR * interval_s:
            raise ValueError(
                f"{SERVER_TIMEOUT_ENV} ({timeout:g}s) must be >= {MIN_TIMEOUT_FACTOR:g}x the "
                f"heartbeat interval ({interval_s:g}s)"
            )


async def _run(
    server: str,
    profile: CapacityProfile,
    requested: list[Engine],
    token: SecretStr,
    budget: Budget,
    heartbeat_interval_s: float = DEFAULT_HEARTBEAT_INTERVAL_S,
) -> None:
    nice = nice_for(budget.cpu_priority)
    settings = ExecutorSettings()
    try:
        # Before restrict_environ(). FetchEnv keeps HF_TOKEN / proxy / CA values in this
        # process's memory (not in os.environ) and hands them to the fetch subprocess only.
        fetch_env = FetchEnv.capture()
    except ValueError as exc:
        raise StartupError(str(exc)) from exc
    hooks = os.environ.get(DEV_ENV) == "1" and os.environ.get(TEST_HOOKS_ENV) == "1"
    async with (
        ExecutorClient(server, token) as client,
        httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as http,
    ):
        # httpx reads proxy env when the clients are built (above); from here on the control
        # process holds no secrets, so neither the probe nor the workers inherit any.
        restrict_environ()
        stop_requested = asyncio.Event()
        aloop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            aloop.add_signal_handler(sig, stop_requested.set)  # also covers the probe
        try:
            available = await asyncio.to_thread(
                probe_available, requested, sandbox_policy(settings, requested)
            )
        except (RunnerError, WorkerCrashed) as exc:
            raise StartupError(str(exc)) from exc
        if not available:
            raise StartupError(NO_ENGINE_MSG)
        typer.echo(f"engines: {','.join(e.value for e in available)}")
        reg = RegisterRequest(
            hostname=socket.gethostname(),
            chip=_chip(),
            total_memory_gb=total_memory_gb(),
            os_version=platform.platform()[:100],
            engines=available,
            profile=profile,
            budget=budget,
        )
        worker = WorkerHandle(
            available,
            nice=nice,
            test_hooks=hooks,
            sandbox=sandbox_policy(settings, available),
            engine_config=EngineConfig(keep_alive_s=keep_alive_s(settings.idle_unload_s)),
        )
        loop = ExecutorLoop(
            client,
            worker,
            budget,
            available,
            http=http,
            register=reg,
            allow_http_loopback=_is_loopback(server),
            heartbeat_interval_s=heartbeat_interval_s,
            idle_unload_s=settings.idle_unload_s,
            model_cache=ModelCache(
                settings.model_cache_dir,
                int(settings.model_cache_gb * 1024**3),
                fetch_env=fetch_env,
                download_timeout_s=settings.download_timeout_s,
                min_free_bytes=int(settings.min_free_disk_gb * 1024**3),
                nice=nice,
                sandbox=settings.fetch_sandbox,
            ),
        )
        for sig in (signal.SIGTERM, signal.SIGINT):
            aloop.add_signal_handler(sig, loop.stop)
        if stop_requested.is_set():
            loop.stop()
        await loop.run()


@app.command()
def run(
    server: Annotated[str, typer.Option(help="Minion server base URL (https, or localhost)")] = (
        "http://localhost:8080"
    ),
    profile: Annotated[
        str | None,
        typer.Option(
            help=(
                "Pin a capacity profile: light, balanced, full or custom (SPEC §4.6). "
                "Omitted: adaptive (FR-52); until the governor lands (M4.5) the executor "
                "starts on active_profile from executor.toml (default light)"
            )
        ),
    ] = None,
    engines: Annotated[
        str,
        typer.Option(
            help=(
                "Engines to offer: auto (probe mlx and hf; Ollama is opt-in) "
                "or a list such as mlx,ollama"
            )
        ),
    ] = "auto",
    runner: Annotated[
        str | None,
        typer.Option(hidden=True, help="Dev only: `fake` (needs MINION_EXECUTOR_DEV=1)"),
    ] = None,
    heartbeat_interval_s: Annotated[
        float,
        typer.Option(
            "--heartbeat-interval",
            envvar="MINION_EXECUTOR_HEARTBEAT_INTERVAL_S",
            help="Seconds between heartbeats (1 to 3600). Server timeout must be >= 2x this.",
        ),
    ] = DEFAULT_HEARTBEAT_INTERVAL_S,
) -> None:
    """Poll the server and run subtasks. Token: $MINION_EXECUTOR_TOKEN or Keychain."""
    try:
        require_secure_url(server)
        requested = _requested_engines(engines, runner)
        _validate_heartbeat(heartbeat_interval_s)
        settings = ExecutorSettings()  # fail fast on bad MINION_EXECUTOR_* values
        config = load_config(settings.config_path)  # before restrict_environ(); exit 2 if unsafe
        specs = resolve_profiles(config)
        prof = parse_profile(profile if profile is not None else config.active_profile, specs)
        budget = budget_for(prof, specs=specs)
        token = load_token()  # fail fast, before starting anything
    except (ValueError, RuntimeError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=2) from None
    os.environ.pop(ENV_VAR, None)  # keep the token out of every child process' environment
    try:
        asyncio.run(_run(server, prof, requested, token, budget, heartbeat_interval_s))
    except StartupError as exc:  # no engine / probe failure; runtime errors surface
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=2) from None


@app.command()
def status() -> None:
    """Show current budget, task and contribution."""
    typer.echo("status not implemented yet")
    raise typer.Exit(code=1)


def _check_out_path(out: Path) -> None:
    """Fail before the (long) run if the report cannot be written. Symlinks are refused."""
    parent = out.parent
    if not parent.is_dir() or not os.access(parent, os.W_OK):
        raise ValueError(f"--out directory is not a writable directory: {parent}")
    if out.is_symlink():
        raise ValueError("--out must not be a symlink")
    if out.exists() and not out.is_file():
        raise ValueError("--out must be a regular file")


def _write_out(out: Path, text: str) -> None:
    # O_NONBLOCK so a FIFO cannot hang the open; O_TRUNC only after the S_ISREG check below
    fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o644)
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise OSError("--out must be a regular file")
    os.ftruncate(fd, 0)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(text)


@app.command()
def bench(
    models: Annotated[
        Path, typer.Option("--models", help="Bench models file (see deploy/bench-models.yaml)")
    ] = Path("deploy/bench-models.yaml"),
    model: Annotated[
        list[str] | None,
        typer.Option("--model", help="model_id from the file (repeatable; default: all)"),
    ] = None,
    records: Annotated[int, typer.Option(help="Timed records per batch size")] = (
        bench_mod.DEFAULT_RECORDS
    ),
    batch_sizes: Annotated[str, typer.Option(help="Comma-separated batch sizes")] = "1,8,32",
    max_tokens: Annotated[int, typer.Option(help="Completion max_tokens")] = (
        bench_mod.DEFAULT_MAX_TOKENS
    ),
    profile: Annotated[
        str, typer.Option(help="Capacity profile: light, balanced, full or custom")
    ] = "full",
    out: Annotated[Path | None, typer.Option(help="Also write the JSON report here")] = None,
) -> None:
    """Benchmark engines on this Mac through the real worker path. No server or token needed."""
    try:
        settings = ExecutorSettings()
        specs = resolve_profiles(load_config(settings.config_path))
        prof = parse_profile(profile, specs)
        config = bench_mod.BenchConfig(
            records=records,
            batch_sizes=bench_mod.parse_batch_sizes(batch_sizes),
            max_tokens=max_tokens,
        )
        entries, pending = bench_mod.load_bench_models(models)
        if model:
            known = {e.model_id for e in entries} | {p.model_id for p in pending}
            if unknown := [m for m in model if m not in known]:
                raise ValueError(f"unknown model id(s): {', '.join(unknown)}")
            entries = [e for e in entries if e.model_id in model]
            pending = [p for p in pending if p.model_id in model]
        if any(e.engine is Engine.FAKE for e in entries):
            _require_dev_for_fake()
        if out is not None:
            _check_out_path(out)
        fetch_env = FetchEnv.capture()
    except (ValueError, OSError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=2) from None
    # stdout carries only the table; progress logs go to stderr
    saved_log_config = structlog.get_config()
    try:
        structlog.configure(logger_factory=structlog.PrintLoggerFactory(file=sys.stderr))
        restrict_environ()  # fetch_env holds the secrets; workers inherit none
        base = budget_for(prof, specs=specs)
        # sweep honestly: the profile's batch cap must not clamp the requested batch sizes
        budget = Budget.model_validate(
            {**base.model_dump(), "max_batch_size": max(base.max_batch_size, *config.batch_sizes)}
        )
        engines = list(dict.fromkeys(e.engine for e in entries))
        sandbox = sandbox_policy(settings, engines)

        def factory(engine: Engine) -> WorkerHandle:
            return WorkerHandle(
                [engine],
                nice=nice_for(base.cpu_priority),
                sandbox=sandbox,
                engine_config=EngineConfig(keep_alive_s=keep_alive_s(settings.idle_unload_s)),
            )

        cache = ModelCache(
            settings.model_cache_dir,
            int(settings.model_cache_gb * 1024**3),
            fetch_env=fetch_env,
            download_timeout_s=settings.download_timeout_s,
            min_free_bytes=int(settings.min_free_disk_gb * 1024**3),
            nice=nice_for(base.cpu_priority),
            sandbox=settings.fetch_sandbox,
        )
        report = asyncio.run(
            bench_mod.run_bench(cache, factory, entries, config, budget, pending=pending)
        )
    finally:
        structlog.configure(**saved_log_config)  # do not leave a logger bound to this stderr
    typer.echo(bench_mod.render_table(report))
    if out is not None:
        try:
            _write_out(out, json.dumps(report.to_json(), indent=2) + "\n")
        except OSError as exc:
            typer.echo(f"error: cannot write --out: {exc}", err=True)
            raise typer.Exit(code=1) from None
    if not any(r.status == "ok" for r in report.results):
        raise typer.Exit(code=1)
