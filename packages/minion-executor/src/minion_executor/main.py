"""Executor CLI. `run` is the M1 control loop; `status` / `bench` land with M4 / M3."""

from __future__ import annotations

import asyncio
import os
import platform
import signal
import socket
import subprocess
from typing import Annotated

import typer
from minion_common.client import ExecutorClient, require_secure_url
from minion_common.enums import CapacityProfile, Engine
from minion_common.schemas import RegisterRequest

from minion_executor.capacity.profiles import PROFILES, budget_for, total_memory_gb
from minion_executor.loop import ExecutorLoop
from minion_executor.secrets import load_token
from minion_executor.worker.process import WorkerHandle

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


async def _run(server: str, profile: CapacityProfile, runner: str) -> None:
    token = load_token()
    budget = budget_for(profile)
    engines = list(Engine) if runner == "fake" else [Engine.MLX]
    reg = RegisterRequest(
        hostname=socket.gethostname(),
        chip=_chip(),
        total_memory_gb=total_memory_gb(),
        os_version=platform.platform()[:100],
        engines=engines,
        profile=profile,
        budget=budget,
    )
    worker = WorkerHandle(runner, nice=PROFILES[profile].nice)
    async with ExecutorClient(server, token) as client:
        loop = ExecutorLoop(client, worker, budget, engines, register=reg)
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
        CapacityProfile, typer.Option(help="Capacity profile (SPEC §4.6)")
    ] = CapacityProfile.BALANCED,
    runner: Annotated[str, typer.Option(hidden=True, help="Runner name")] = "fake",
) -> None:
    """Poll the server and run subtasks. Token: $MINION_EXECUTOR_TOKEN or Keychain."""
    try:
        require_secure_url(server)
        if profile not in PROFILES:
            raise ValueError(f"profile {profile.value!r} needs explicit values (not built in)")
        if not os.environ.get("MINION_EXECUTOR_TOKEN"):
            load_token()  # fail fast, before starting anything
    except (ValueError, RuntimeError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=2) from None
    asyncio.run(_run(server, profile, runner))


@app.command()
def status() -> None:
    """Show current budget, task and contribution."""
    typer.echo("status not implemented yet")
    raise typer.Exit(code=1)
