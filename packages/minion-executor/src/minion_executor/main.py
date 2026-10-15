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

import typer
from minion_common.client import LOCAL_HOSTS, ExecutorClient, require_secure_url
from minion_common.enums import CapacityProfile, Engine
from minion_common.schemas import RegisterRequest
from pydantic import SecretStr

from minion_executor.capacity.profiles import PROFILES, budget_for, parse_profile, total_memory_gb
from minion_executor.loop import ExecutorLoop
from minion_executor.runners import create_runner
from minion_executor.secrets import ENV_VAR, load_token
from minion_executor.worker.process import WorkerHandle, restrict_environ

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


MIN_HEARTBEAT_INTERVAL_S = 1.0
DEFAULT_HEARTBEAT_INTERVAL_S = 300.0
DEV_ENV = "MINION_EXECUTOR_DEV"
TEST_HOOKS_ENV = "MINION_EXECUTOR_TEST_HOOKS"
SERVER_TIMEOUT_ENV = "MINION_HEARTBEAT_TIMEOUT_S"  # server/watchdog setting; optional sanity check
MIN_TIMEOUT_FACTOR = 2.0
DEFAULT_RUNNER = "mlx"  # real engines land in M3; until then only `--runner fake` (dev) works


def _is_loopback(url: str) -> bool:
    return (urlsplit(url).hostname or "").lower() in LOCAL_HOSTS


def _validate_runner(name: str) -> None:
    """Fail fast (exit 2 via ValueError) on unknown runners; `fake` needs MINION_EXECUTOR_DEV=1."""
    if name == "fake" and os.environ.get(DEV_ENV) != "1":
        raise ValueError(f"runner 'fake' serves no real work; set {DEV_ENV}=1 to use it (dev only)")
    create_runner(name)  # raises ValueError for unknown names; engine imports are lazy


def _validate_heartbeat(interval_s: float) -> None:
    """The server's heartbeat timeout must exceed the interval with margin (>= 2x), else live
    executors get reclaimed between beats. Checked only when the operator exports the timeout."""
    if interval_s < MIN_HEARTBEAT_INTERVAL_S:
        raise ValueError(f"heartbeat interval must be >= {MIN_HEARTBEAT_INTERVAL_S:g}s")
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
    runner: str,
    token: SecretStr,
    heartbeat_interval_s: float = DEFAULT_HEARTBEAT_INTERVAL_S,
) -> None:
    budget = budget_for(profile)
    engines = [Engine.FAKE] if runner == "fake" else [Engine.MLX]
    reg = RegisterRequest(
        hostname=socket.gethostname(),
        chip=_chip(),
        total_memory_gb=total_memory_gb(),
        os_version=platform.platform()[:100],
        engines=engines,
        profile=profile,
        budget=budget,
    )
    hooks = os.environ.get(DEV_ENV) == "1" and os.environ.get(TEST_HOOKS_ENV) == "1"
    worker = WorkerHandle(runner, nice=PROFILES[profile].nice, test_hooks=hooks)
    async with ExecutorClient(server, token) as client:
        loop = ExecutorLoop(
            client,
            worker,
            budget,
            engines,
            register=reg,
            allow_http_loopback=_is_loopback(server),
            heartbeat_interval_s=heartbeat_interval_s,
        )
        # httpx reads proxy env when the clients were built (client + loop above); from
        # here on the control process holds no secrets, so spawned workers inherit none.
        restrict_environ()
        aloop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            aloop.add_signal_handler(sig, loop.stop)
        await loop.run()


@app.command()
def run(
    server: Annotated[str, typer.Option(help="Minion server base URL (https, or localhost)")] = (
        "http://localhost:8080"
    ),
    profile: Annotated[
        str, typer.Option(help="Capacity profile: light, balanced or full (SPEC §4.6)")
    ] = "balanced",
    runner: Annotated[
        str, typer.Option(hidden=True, help="Runner name (`fake` needs MINION_EXECUTOR_DEV=1)")
    ] = DEFAULT_RUNNER,
    heartbeat_interval_s: Annotated[
        float,
        typer.Option(
            "--heartbeat-interval",
            envvar="MINION_EXECUTOR_HEARTBEAT_INTERVAL_S",
            help="Seconds between heartbeats (>= 1). Server timeout must be >= 2x this.",
        ),
    ] = DEFAULT_HEARTBEAT_INTERVAL_S,
) -> None:
    """Poll the server and run subtasks. Token: $MINION_EXECUTOR_TOKEN or Keychain."""
    try:
        require_secure_url(server)
        prof = parse_profile(profile)
        _validate_runner(runner)
        _validate_heartbeat(heartbeat_interval_s)
        token = load_token()  # fail fast, before starting anything
    except (ValueError, RuntimeError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=2) from None
    os.environ.pop(ENV_VAR, None)  # keep the token out of every child process' environment
    asyncio.run(_run(server, prof, runner, token, heartbeat_interval_s))


@app.command()
def status() -> None:
    """Show current budget, task and contribution."""
    typer.echo("status not implemented yet")
    raise typer.Exit(code=1)
