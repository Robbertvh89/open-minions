"""Executor CLI. `run` is the M1 control loop; `status` / `bench` land with M4 / M3."""

from __future__ import annotations

import asyncio
import os
import platform
import signal
import socket
import subprocess
from typing import Annotated
from urllib.parse import urlsplit

import httpx
import typer
from minion_common.client import LOCAL_HOSTS, ExecutorClient, require_secure_url
from minion_common.enums import CapacityProfile, Engine
from minion_common.schemas import RegisterRequest
from pydantic import SecretStr

from minion_executor.capacity.profiles import PROFILES, budget_for, parse_profile, total_memory_gb
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
REAL_ENGINES = [Engine.MLX, Engine.OLLAMA, Engine.HF]
NO_ENGINE_MSG = (
    "no inference engine available (mlx/ollama/hf runners land in a later release; "
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
        return list(REAL_ENGINES)
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


def sandbox_policy(settings: ExecutorSettings, engines: list[Engine]) -> SandboxPolicy | None:
    """The worker sandbox for these settings (`None` only with the dev opt-out)."""
    if not settings.sandbox:
        return None
    port = urlsplit(EngineConfig().ollama_url).port if Engine.OLLAMA in engines else None
    return SandboxPolicy(cache_root=settings.model_cache_dir, ollama_port=port)


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
    heartbeat_interval_s: float = DEFAULT_HEARTBEAT_INTERVAL_S,
) -> None:
    budget = budget_for(profile)
    settings = ExecutorSettings()
    try:
        # Before restrict_environ(): HF_TOKEN / proxy / CA go to the fetch subprocess only.
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
            nice=PROFILES[profile].nice,
            test_hooks=hooks,
            sandbox=sandbox_policy(settings, available),
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
        str, typer.Option(help="Capacity profile: light, balanced or full (SPEC §4.6)")
    ] = "balanced",
    engines: Annotated[
        str,
        typer.Option(
            help="Engines to offer: auto (probe mlx, ollama, hf) or a list such as mlx,ollama"
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
        prof = parse_profile(profile)
        requested = _requested_engines(engines, runner)
        _validate_heartbeat(heartbeat_interval_s)
        ExecutorSettings()  # fail fast on bad MINION_EXECUTOR_* values
        token = load_token()  # fail fast, before starting anything
    except (ValueError, RuntimeError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=2) from None
    os.environ.pop(ENV_VAR, None)  # keep the token out of every child process' environment
    try:
        asyncio.run(_run(server, prof, requested, token, heartbeat_interval_s))
    except StartupError as exc:  # no engine / probe failure; runtime errors surface
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=2) from None


@app.command()
def status() -> None:
    """Show current budget, task and contribution."""
    typer.echo("status not implemented yet")
    raise typer.Exit(code=1)
