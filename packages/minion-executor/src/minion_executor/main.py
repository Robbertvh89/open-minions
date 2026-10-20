"""Executor CLI. `run` is the control loop; `bench` measures engines; `status` reads status.json."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import platform
import signal
import socket
import stat
import subprocess
import sys
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Annotated, NoReturn
from urllib.parse import urlsplit

import httpx
import structlog
import typer
from minion_common.client import LOCAL_HOSTS, ApiError, ExecutorClient, require_secure_url
from minion_common.enums import CapacityProfile, Engine
from minion_common.schemas import Budget, PreferenceRequest, RegisterRequest
from pydantic import SecretStr

from minion_executor import bench as bench_mod
from minion_executor import bench_budget as budget_mod
from minion_executor import bench_foreground as fg_mod
from minion_executor.capacity.config import ExecutorConfig, OnCall, load_config, resolve_profiles
from minion_executor.capacity.control import CONTROL_FILE, parse_duration, update_control
from minion_executor.capacity.governor import GovernorTiming
from minion_executor.capacity.gpu_meter import GpuMeterError, default_gpu_reader
from minion_executor.capacity.instance import AlreadyRunningError, acquire_instance_lock
from minion_executor.capacity.profiles import (
    ProfileSpec,
    budget_for,
    nice_for,
    parse_profile,
    total_memory_gb,
)
from minion_executor.capacity.runtime import GovernorRuntime
from minion_executor.capacity.safefile import SafeFileError, ensure_private_dir
from minion_executor.capacity.signals import (
    MemPressure,
    SignalSource,
    default_source,
    read_memory_pressure,
)
from minion_executor.capacity.status import STATUS_FILE, read_status, running
from minion_executor.capacity.status import render as render_status
from minion_executor.heartbeat import check_interval
from minion_executor.loop import ExecutorLoop, GovernorStoppedError
from minion_executor.menubar_agent import menubar_app
from minion_executor.model_cache import FetchEnv, ModelCache
from minion_executor.runners.base import EngineConfig, RunnerError
from minion_executor.runners.fg_probe import MlxFrameProbe, ProbeError
from minion_executor.secrets import ENV_VAR, TokenUnavailableError, load_token
from minion_executor.service import (
    SERVER_AUTH_CODES,
    check_launchd_timeout,
    check_server_url,
    clear_failure,
    record_failure,
    sanitize,
    service_app,
    service_mode,
)
from minion_executor.settings import (
    DEFAULT_PROBE_TIMEOUT_S,
    PROBE_TIMEOUT_MAX_S,
    PROBE_TIMEOUT_MIN_S,
    ExecutorSettings,
)
from minion_executor.worker.process import (
    WorkerCrashed,
    WorkerHandle,
    WorkerTimeout,
    restrict_environ,
)
from minion_executor.worker.sandbox import SANDBOX_FAILED, SandboxPolicy

app = typer.Typer(help="open-minions executor: donate idle Apple Silicon GPU capacity.")
app.add_typer(service_app, name="service")
app.add_typer(menubar_app, name="menubar")


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
    "`uv sync --all-packages --extra mlx` (or `--extra hf`), or start Ollama and pass "
    "`--engines ollama`; "
    "use --engines fake with MINION_EXECUTOR_DEV=1 for development)"
)


METERED_ENGINES = frozenset({Engine.MLX, Engine.HF})  # run in the worker pid: GPU time is its own


def resolve_gpu_reader(
    engines: list[Engine],
    factory: Callable[[], Callable[[], dict[int, dict[str, int]]] | None] = default_gpu_reader,
) -> Callable[[], dict[int, dict[str, int]]] | None:
    """The GPU meter for the duty guard, probed once. None (one log line) when no metered engine
    (mlx, hf: their GPU work is in the worker pid) is advertised - the dev fake or Ollama only -
    or where no reader exists (non-darwin). A reader that exists but fails its first read is a
    StartupError (exit 2): an unmeasurable worker must not run unguarded (invariant 6)."""
    log = structlog.get_logger("minion_executor.main")
    reader = factory()
    if not METERED_ENGINES.intersection(engines) or reader is None:
        log.info(
            "gpu_duty_guard_disabled",
            reason="no_metered_engine" if reader is not None else "no_reader",
        )
        return None
    try:
        reader()
    except Exception as exc:
        log.error("gpu_meter_probe_failed", error=type(exc).__name__)
        raise StartupError("the GPU meter is unavailable, refusing to run unguarded") from exc
    return reader


class StartupError(Exception):
    """Startup failed (no engine, probe failure); the CLI maps it to exit 2.

    `permanent` is False for a probe crash / timeout (a restart may work); a missing engine or a
    failed sandbox self-test is permanent.
    """

    def __init__(self, message: str, *, permanent: bool = True) -> None:
        super().__init__(message)
        self.permanent = permanent


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


PROBE_TIMEOUT_S = DEFAULT_PROBE_TIMEOUT_S  # default; MINION_EXECUTOR_PROBE_TIMEOUT_S overrides
PROBE_PRESSURE_FACTOR = 2.0  # timeout multiplier while memory pressure is warn/critical
PROBE_PRESSURE_MAX_S = 300.0  # ... but never extended beyond this (invariant 6)
PROBE_TIMEOUT_ENV = "MINION_EXECUTOR_PROBE_TIMEOUT_S"


def keep_alive_s(idle_unload_s: float) -> int:
    """How long Ollama may keep a model after a request: the idle unload time, capped. With idle
    unload disabled (0) it still gets a short bounded value, never "forever"."""
    return max(1, min(int(idle_unload_s), MAX_KEEP_ALIVE_S)) if idle_unload_s > 0 else 60


def sandbox_policy(settings: ExecutorSettings, engines: list[Engine]) -> SandboxPolicy | None:
    """The worker sandbox for these settings (`None` only with the dev opt-out)."""
    if not settings.sandbox:
        return None
    return SandboxPolicy(cache_root=settings.model_cache_dir)


_active_probe: WorkerHandle | None = None
_probe_cancelled = threading.Event()


def kill_active_probe() -> None:
    """Abort the engine probe (second stop signal during startup). Lock-free. Sets a flag first,
    so it also works when the probe thread has not created its worker yet."""
    _probe_cancelled.set()
    handle = _active_probe
    if handle is not None:
        # Runs on the event loop from a signal handler; the probe's finally/_reset joins it.
        handle.kill(reap=False)


def probe_timeout_s(base_s: float, pressure: MemPressure | None) -> float:
    """The probe timeout: `base_s`, doubled under warn/critical memory pressure but capped at
    300 s (never below `base_s`: an operator who set more keeps it).

    We extend rather than wait for the pressure to clear: it may never clear on a busy Mac, a
    wait delays startup for nothing when the probe would have fit, and a longer deadline needs no
    extra interruption path (the probe is already killed by the stop signals).
    """
    if pressure in (MemPressure.WARNING, MemPressure.CRITICAL):
        return max(base_s, min(base_s * PROBE_PRESSURE_FACTOR, PROBE_PRESSURE_MAX_S))
    return base_s


def probe_available(
    requested: list[Engine],
    sandbox: SandboxPolicy | None = None,
    *,
    timeout_s: float = PROBE_TIMEOUT_S,
    fake_signals: Path | None = None,
) -> list[Engine]:
    """Ask a worker subprocess which of `requested` work here (engines are never imported in
    the control process, invariant 7). Call after `restrict_environ()` so the probe worker
    inherits no secrets. The probe worker is sandboxed like a real one, so a broken sandbox
    surfaces here as `SANDBOX_FAILED` (the CLI exits 2) and engine imports are proven to work
    under the profile. `timeout_s` is `settings.probe_timeout_s` (read by the caller before
    `restrict_environ()` strips MINION_EXECUTOR_*), extended under memory pressure;
    a timeout is reported with its likely cause and that knob."""
    global _active_probe
    base_s = timeout_s
    pressure = read_memory_pressure(fake_signals)
    timeout_s = probe_timeout_s(base_s, pressure)
    if timeout_s != base_s:
        structlog.get_logger("minion_executor.main").warning(
            "engine_probe_timeout_extended",
            memory_pressure=pressure.value if pressure else None,
            timeout_s=timeout_s,
        )
    handle = WorkerHandle(requested, sandbox=sandbox)
    _active_probe = handle
    try:
        if _probe_cancelled.is_set():  # cancelled before we got here: never spawn
            handle.kill()
        try:
            found = handle.probe(timeout_s)
        except WorkerTimeout as exc:
            raise WorkerCrashed(
                f"engine probe timed out after {timeout_s:g}s: starting the worker and importing "
                "the engine is slow, most likely because the Mac is under memory pressure "
                "(swapping) or this is the first import after boot; close other apps, or raise "
                f"{PROBE_TIMEOUT_ENV} ({PROBE_TIMEOUT_MIN_S:g}..{PROBE_TIMEOUT_MAX_S:g} s)"
            ) from exc
    finally:
        _active_probe = None
    return [e for e in requested if found.get(e.value)]


def warn_if_ollama(available: list[Engine]) -> None:
    """One startup warning when Ollama is enabled: its daemon runs outside the worker, so the GPU
    duty cycle is only paced per request and is not metered or enforced by the duty guard
    (ADR-0006 D17, ADR-0009). Fixed fields only."""
    if Engine.OLLAMA in available:
        structlog.get_logger("minion_executor.main").warning(
            "ollama_duty_cycle_not_enforced", reason="daemon_outside_worker"
        )


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


@dataclass(frozen=True)
class CapacityPlan:
    """Everything `_run` needs to build the governor, resolved (and failed fast) by `run`."""

    config: ExecutorConfig
    specs: dict[CapacityProfile, ProfileSpec]
    pinned: CapacityProfile | None  # `run --profile`: no adaptive switching, pause still applies
    source: (
        SignalSource  # one object for the whole run (the one-read-in-flight bound is per source)
    )
    settings: ExecutorSettings


def build_governor(plan: CapacityPlan) -> GovernorRuntime:
    s = plan.settings
    return GovernorRuntime(
        plan.source,
        plan.config,
        plan.specs,
        state_dir=s.state_dir,
        total_gb=total_memory_gb(),
        pinned=plan.pinned,
        timing=GovernorTiming(
            escalate_after_s=s.escalate_after_s,
            resume_after_s=s.resume_after_s,
            pause_release_after_s=s.pause_release_after_s,
        ),
        poll_s=s.signal_poll_s,
    )


async def _run(
    server: str,
    profile: CapacityProfile,
    requested: list[Engine],
    token: SecretStr,
    budget: Budget,
    heartbeat_interval_s: float = DEFAULT_HEARTBEAT_INTERVAL_S,
    plan: CapacityPlan | None = None,
) -> None:
    nice = nice_for(budget.cpu_priority)
    settings = plan.settings if plan is not None else ExecutorSettings()
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
        second_signal = False

        def on_early_signal() -> None:  # covers the probe: a second signal kills it and exits
            nonlocal second_signal
            if stop_requested.is_set():
                second_signal = True
                kill_active_probe()
            stop_requested.set()

        _probe_cancelled.clear()
        for sig in (signal.SIGTERM, signal.SIGINT):
            aloop.add_signal_handler(sig, on_early_signal)
        try:
            available = await asyncio.to_thread(
                partial(
                    probe_available,
                    requested,
                    sandbox_policy(settings, requested),
                    timeout_s=settings.probe_timeout_s,
                    fake_signals=settings.fake_signals,
                )
            )
        except (RunnerError, WorkerCrashed) as exc:
            if second_signal:
                return  # stopped by the user during startup: clean exit
            permanent = isinstance(exc, RunnerError) and exc.code == SANDBOX_FAILED
            raise StartupError(str(exc), permanent=permanent) from exc
        if second_signal:
            return
        if not available:
            raise StartupError(NO_ENGINE_MSG)
        typer.echo(f"engines: {','.join(e.value for e in available)}")
        warn_if_ollama(available)
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
        gpu_reader = resolve_gpu_reader(available)
        governor = build_governor(plan) if plan is not None else None
        # fetch niceness is read per fetch (at spawn), not live; fixed only without a governor
        nice_fn = (lambda: nice_for(governor.decision.budget.cpu_priority)) if governor else None
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
            stop_grace_s=settings.stop_grace_s,
            max_output_bytes=settings.max_output_bytes,
            governor=governor,
            presence_interval_s=settings.presence_interval_s,
            acquire_pace_s=settings.acquire_pace_s,
            first_acquire_jitter_s=settings.first_acquire_jitter_s,
            gpu_reader=gpu_reader,
            complete_shed_budget_s=settings.complete_shed_budget_s,
            downgrade_batch_s=settings.downgrade_batch_s,
            guard_interval_s=settings.signal_poll_s,
            status_dir=settings.state_dir,
            model_cache=ModelCache(
                settings.model_cache_dir,
                int(settings.model_cache_gb * 1024**3),
                fetch_env=fetch_env,
                download_timeout_s=settings.download_timeout_s,
                min_free_bytes=int(settings.min_free_disk_gb * 1024**3),
                nice=nice,
                nice_fn=nice_fn,
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
                "Omitted: adaptive (FR-52), starting on active_profile from executor.toml "
                "(default light)"
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
        check_server_url(server)
        requested = _requested_engines(engines, runner)
        _validate_heartbeat(heartbeat_interval_s)
        settings = ExecutorSettings()  # fail fast on bad MINION_EXECUTOR_* values
        check_launchd_timeout(settings)
        config = load_config(settings.config_path)  # before restrict_environ(); exit 2 if unsafe
        specs = resolve_profiles(config)
        pinned = parse_profile(profile, specs) if profile is not None else None
        prof = pinned or parse_profile(config.active_profile, specs)
        budget = budget_for(prof, specs=specs)
        # An unsafe or uncreatable state dir (control.json, status.json) fails closed: exit 2.
        ensure_private_dir(settings.state_dir)
        source = default_source(
            settings.fake_signals, want_call=config.on_call is not OnCall.IGNORE
        )  # SignalError (a RuntimeError): exit 2
        plan = CapacityPlan(config, specs, pinned, source, settings)
        token = load_token()  # fail fast, before starting anything
    except TokenUnavailableError as exc:
        # A locked Keychain / timeout is transient (exit 1: launchd retries); a missing item is not.
        _failure("token_unavailable", str(exc), permanent=not exc.transient)
    except (ValueError, RuntimeError) as exc:
        _failure("config_invalid", str(exc), permanent=True)
    try:  # one executor per state dir: held until this function returns (or the process dies)
        lock_fd = acquire_instance_lock(settings.state_dir)
    except AlreadyRunningError as exc:  # the other instance may stop: retry, do not give up
        _failure("instance_lock_held", str(exc), permanent=False)
    except SafeFileError as exc:
        _failure("instance_lock", str(exc), permanent=True)
    if service_mode():
        clear_failure(settings.state_dir)
    os.environ.pop(ENV_VAR, None)  # keep the token out of every child process' environment
    # restrict_environ() (inside _run) scrubs the service env vars: decide once, before it.
    in_service = service_mode()
    state_dir = settings.state_dir
    try:
        asyncio.run(_run(server, prof, requested, token, budget, heartbeat_interval_s, plan))
    except StartupError as exc:  # no engine / probe failure; runtime errors surface
        _failure(
            "startup_failed",
            str(exc),
            permanent=exc.permanent,
            in_service=in_service,
            state_dir=state_dir,
        )
    except ApiError as exc:
        # Permanent only when the server itself refused us: 401/403 carrying its own error code
        # (a proxy/WAF 403 has none), 400 with a server code, or 422.
        # 429/5xx and the rest are retried.
        if exc.status_code == 429 or not 400 <= exc.status_code < 500:
            raise
        auth = exc.status_code in (401, 403) and exc.code in SERVER_AUTH_CODES
        detail = f"server answered {exc.status_code} {sanitize(exc.code, 64)}"
        rejected = exc.status_code == 422 or (exc.status_code == 400 and exc.code != "HTTP_ERROR")
        reason = "credential_rejected" if auth else "request_rejected" if rejected else None
        _failure(
            reason or "request_refused",
            detail,
            permanent=reason is not None,
            code=1,
            in_service=in_service,
            state_dir=state_dir,
        )
    except GovernorStoppedError as exc:  # nothing steers the budget: exit non-zero, never run on
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from None
    finally:
        os.close(lock_fd)


def _failure(
    reason: str,
    detail: str,
    *,
    permanent: bool,
    code: int = 2,
    in_service: bool | None = None,
    state_dir: Path | None = None,
) -> NoReturn:
    """End `run` after a failure.

    Under launchd (KeepAlive SuccessfulExit=false) a *permanent* one (a restart cannot fix it) exits
    0 with one log line `permanent_failure ts=<UTC> reason=...` and is recorded in the state dir
    for `service status`, so the service is not relaunched every ThrottleInterval. A transient one
    exits 1 there (relaunched after the throttle). Outside the service both exit `code`.
    """
    detail = sanitize(detail, 500)
    typer.echo(f"error: {detail}", err=True)
    if service_mode() if in_service is None else in_service:
        ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        if not permanent:
            typer.echo(
                f"transient_failure ts={ts} reason={reason}: exiting 1 for a relaunch", err=True
            )
            raise typer.Exit(code=1)
        with contextlib.suppress(Exception):
            record_failure(state_dir or ExecutorSettings().state_dir, reason, detail, ts)
        typer.echo(
            f"permanent_failure ts={ts} reason={reason}: exiting 0 so launchd does not relaunch; "
            "fix the cause, then `launchctl kickstart` (or reinstall) the service",
            err=True,
        )
        raise typer.Exit(code=0)
    raise typer.Exit(code=code)


def _warn(message: str) -> None:
    typer.echo(f"warning: {message}", err=True)


async def _send_preference(
    server: str,
    token: SecretStr,
    pref: PreferenceRequest,
    transport: httpx.AsyncBaseTransport | None = None,
) -> None:
    async with ExecutorClient(server, token, transport=transport) as client:
        await client.set_preference(pref)


@app.command()
def prefer(
    server: Annotated[str, typer.Option(help="Minion server base URL (https, or localhost)")] = (
        "http://localhost:8080"
    ),
    team: Annotated[str | None, typer.Option(help="Prefer subtasks of this team")] = None,
    project: Annotated[str | None, typer.Option(help="...and this project (needs --team)")] = None,
    task_name: Annotated[str | None, typer.Option(help="...and this task (needs --project)")] = (
        None
    ),
    clear: Annotated[bool, typer.Option("--clear", help="Remove the preference")] = False,
) -> None:
    """Set or clear this executor's work preference (G4/FR-22): a hint, not a filter.

    Matching subtasks are acquired first; otherwise plain FIFO. Token: $MINION_EXECUTOR_TOKEN
    or Keychain."""
    try:
        require_secure_url(server)
        check_server_url(server)
        if clear:
            if team or project or task_name:
                raise ValueError("--clear cannot be combined with --team/--project/--task-name")
            pref = PreferenceRequest()
        else:
            if not team:
                raise ValueError("give --team (optionally with --project, --task-name) or --clear")
            if task_name and not project:
                raise ValueError("--task-name needs --project")
            pref = PreferenceRequest(team=team, project=project, task_name=task_name)
        token = load_token()
    except TokenUnavailableError as exc:
        typer.echo(f"error: {sanitize(str(exc), 500)}", err=True)
        raise typer.Exit(code=2 if not exc.transient else 1) from None
    except ValueError as exc:  # includes pydantic ValidationError (bad slug)
        typer.echo(f"error: {sanitize(str(exc), 500)}", err=True)
        raise typer.Exit(code=2) from None
    try:
        asyncio.run(_send_preference(server, token, pref))
    except ApiError as exc:
        typer.echo(f"error: server answered {exc.status_code} {sanitize(exc.code, 64)}", err=True)
        raise typer.Exit(code=1) from None
    except httpx.HTTPError as exc:
        typer.echo(f"error: cannot reach the server ({type(exc).__name__})", err=True)
        raise typer.Exit(code=1) from None
    if clear:
        typer.echo("preference cleared")
    else:
        fields = (("team", team), ("project", project), ("task", task_name))
        parts = [f"{k}={v}" for k, v in fields if v]
        typer.echo("preference set: " + " ".join(parts))


@app.command()
def pause(
    duration: Annotated[
        str | None,
        typer.Option(
            "--for", help="Pause for this long, e.g. 30m, 1h, 2h30m (default: until resume)"
        ),
    ] = None,
) -> None:
    """Pause the executor (no new subtasks; a running executor reads this within seconds)."""
    try:
        settings = ExecutorSettings()
        delta = parse_duration(duration) if duration is not None else None
        ctl = update_control(settings.state_dir, paused=True, pause_for=delta, warn=_warn)
    except (ValueError, SafeFileError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=2) from None
    until = f" until {ctl.paused_until.isoformat(timespec='seconds')}" if ctl.paused_until else ""
    typer.echo(f"paused{until} ({settings.state_dir / CONTROL_FILE})")


@app.command()
def resume() -> None:
    """Lift an owner pause."""
    try:
        settings = ExecutorSettings()
        update_control(settings.state_dir, paused=False, warn=_warn)
    except (ValueError, SafeFileError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=2) from None
    typer.echo(f"resumed ({settings.state_dir / CONTROL_FILE})")


@app.command("profile")
def profile_cmd(
    name: Annotated[
        str,
        typer.Argument(
            help="light, balanced, full, custom, or auto "
            "(back to the run's --profile pin, or adaptive if none)"
        ),
    ],
) -> None:
    """Pin the capacity profile of the running executor, or return to automatic switching."""
    try:
        settings = ExecutorSettings()
        if name == "auto":
            update_control(settings.state_dir, clear_profile=True, warn=_warn)
        else:
            specs = resolve_profiles(load_config(settings.config_path))
            update_control(settings.state_dir, profile=parse_profile(name, specs), warn=_warn)
    except (ValueError, SafeFileError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=2) from None
    typer.echo(f"profile: {name} ({settings.state_dir / CONTROL_FILE})")


@app.command()
def status() -> None:
    """Show the running executor's profile, budget, pause reason, current subtask and signals.

    Exit 1 when no executor is running (no status file, a stopped or stale one).
    """
    try:
        settings = ExecutorSettings()
        st = read_status(settings.state_dir)
    except (ValueError, SafeFileError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=2) from None
    if st is None:
        typer.echo(f"no executor is running (no {settings.state_dir / STATUS_FILE})")
        raise typer.Exit(code=1)
    alive, why = running(st)
    if not alive:
        typer.echo(f"no executor is running: {why}")
    typer.echo(render_status(st))
    if not alive:
        raise typer.Exit(code=1)


@menubar_app.callback()
def menubar(ctx: typer.Context) -> None:
    """Show a menu-bar item to pause/resume and pick the profile (macOS, as your own user).

    Talks only to the local status.json / control.json: no server, no token. The subcommands
    install/uninstall/status manage the launch-at-login agent.
    """
    if ctx.invoked_subcommand is not None:
        return
    if sys.platform != "darwin":
        typer.echo("error: the menu-bar app needs macOS", err=True)
        raise typer.Exit(code=2)
    if os.geteuid() == 0:
        typer.echo("error: refusing to run the menu-bar app as root", err=True)
        raise typer.Exit(code=2)
    try:
        settings = ExecutorSettings()  # same validation as `run`
        state_dir, config_path = settings.state_dir, settings.config_path
        ensure_private_dir(state_dir)
    except (ValueError, SafeFileError) as exc:  # pydantic's ValidationError is a ValueError
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=2) from None
    from minion_executor.menubar.lock import acquire_menubar_lock

    try:
        lock_fd = acquire_menubar_lock(state_dir)
    except SafeFileError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=2) from None
    if lock_fd is None:
        # Exit 0: launchd's KeepAlive must not restart a second instance in a loop.
        typer.echo("the menu-bar app is already running")
        return
    from minion_executor.menubar.app import run as run_menubar  # no AppKit import off macOS

    run_menubar(state_dir, config_path)  # lock_fd stays open (and locked) until the process exits
    os.close(lock_fd)


def _check_out_path(out: Path) -> None:
    """Fail before the (long) run if the report cannot be written. Symlinks are refused."""
    parent = out.parent
    if not parent.is_dir() or not os.access(parent, os.W_OK):
        raise ValueError(f"--out directory is not a writable directory: {parent}")
    if out.is_symlink():
        raise ValueError("--out must not be a symlink")
    if out.exists() and not out.is_file():
        raise ValueError("--out must be a regular file")


def _emit(table: str, data: dict[str, object], out: Path | None) -> None:
    """Markdown to stdout, JSON to `--out` (exit 1 when it cannot be written)."""
    typer.echo(table)
    if out is not None:
        try:
            _write_out(out, json.dumps(data, indent=2) + "\n")
        except OSError as exc:
            typer.echo(f"error: cannot write --out: {exc}", err=True)
            raise typer.Exit(code=1) from None


@contextlib.contextmanager
def _logs_to_stderr() -> Iterator[None]:
    """Keep stdout for the tables / timestamps (structlog prints to stdout by default)."""
    saved = structlog.get_config()
    try:
        structlog.configure(logger_factory=structlog.PrintLoggerFactory(file=sys.stderr))
        yield
    finally:
        structlog.configure(**saved)


def _watch_reactions(duration: float, out: Path | None) -> None:
    try:
        if out is not None:
            raise ValueError("--watch-reactions prints to stdout; --out is not supported")
        if not 1.0 <= duration <= 3600.0:
            raise ValueError("duration must be in 1..3600 seconds")
        settings = ExecutorSettings()
        config = load_config(settings.config_path)
        source = default_source(
            settings.fake_signals, want_call=config.on_call is not OnCall.IGNORE
        )
    except (ValueError, OSError, RuntimeError) as exc:  # SignalError is a RuntimeError
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=2) from None
    with _logs_to_stderr():
        asyncio.run(
            budget_mod.watch_reactions(
                source,
                duration_s=duration,
                poll_s=settings.signal_poll_s,
                config=config,
                total_gb=total_memory_gb(),
                emit=lambda line: typer.echo(line),
            )
        )


def _cpu_check(duration: float, out: Path | None) -> None:
    """NF-4 on this process: a real GovernorRuntime polling the real signals, idle then paused."""
    try:
        if not 1.0 <= duration <= 3600.0:
            raise ValueError("duration must be in 1..3600 seconds")
        if out is not None:
            _check_out_path(out)
        settings = ExecutorSettings()
        config = load_config(settings.config_path)
        source = default_source(
            settings.fake_signals, want_call=config.on_call is not OnCall.IGNORE
        )
    except (ValueError, OSError, RuntimeError) as exc:  # SignalError is a RuntimeError
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=2) from None
    poll_s, total_gb = settings.signal_poll_s, total_memory_gb()

    async def both() -> list[budget_mod.CpuResult]:
        return [
            await budget_mod.measure_governor_cpu(
                source,
                paused=p,
                duration_s=duration,
                poll_s=poll_s,
                config=config,
                total_gb=total_gb,
            )
            for p in (False, True)
        ]

    with _logs_to_stderr():
        results = asyncio.run(both())
    _emit(
        budget_mod.render_cpu_table(results),
        {"schema_version": 1, "kind": "cpu-check", "results": [r.__dict__ for r in results]},
        out,
    )
    if not all(r.passed for r in results):
        raise typer.Exit(code=1)


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
    budget_check: Annotated[
        bool,
        typer.Option(
            "--budget-check",
            help="NF-8: worker GPU time share (ioreg) and footprint per profile",
        ),
    ] = False,
    profiles: Annotated[
        str, typer.Option(help="--budget-check/--foreground-check: comma-separated profiles")
    ] = "light,balanced,full",
    duration: Annotated[
        float,
        typer.Option(help="--budget-check/--foreground-check: s per phase; --cpu-check: window"),
    ] = budget_mod.DEFAULT_DURATION_S,
    baseline_s: Annotated[
        float, typer.Option(help="--budget-check: idle seconds before the phases (device info)")
    ] = budget_mod.DEFAULT_BASELINE_S,
    repeats: Annotated[
        int, typer.Option(help="--budget-check/--foreground-check: repetitions")
    ] = 1,
    cpu_check: Annotated[
        bool,
        typer.Option("--cpu-check", help="NF-4: governor CPU % while idle and while paused"),
    ] = False,
    watch_reactions: Annotated[
        bool,
        typer.Option(
            "--watch-reactions",
            help="NF-9 on this Mac: print timestamped governor reactions for --duration seconds",
        ),
    ] = False,
    foreground_check: Annotated[
        bool,
        typer.Option(
            "--foreground-check",
            help="NF-7 proxy: GPU frame-latency probe and app-launch time, idle vs loaded worker",
        ),
    ] = False,
    launches: Annotated[
        int, typer.Option(help="--foreground-check: app launches per window (0 = skip)")
    ] = 20,
) -> None:
    """Benchmark engines on this Mac through the real worker path. No server or token needed."""
    if sum((cpu_check, budget_check, watch_reactions, foreground_check)) > 1:
        typer.echo(
            "error: --budget-check, --cpu-check, --watch-reactions, --foreground-check "
            "are separate",
            err=True,
        )
        raise typer.Exit(code=2)
    if watch_reactions:
        _watch_reactions(duration, out)
        return
    if cpu_check:
        _cpu_check(duration, out)
        return
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
        check_cfg = budget_mod.BudgetCheckConfig(
            duration_s=duration, baseline_s=baseline_s, repeats=repeats, max_tokens=max_tokens
        )
        check_profiles = [
            parse_profile(p.value, specs) for p in budget_mod.parse_profiles(profiles)
        ]
        fg_cfg = (
            fg_mod.ForegroundCheckConfig(
                duration_s=duration, launches=launches, repeats=repeats, max_tokens=max_tokens
            )
            if foreground_check
            else None
        )
        if (budget_check or foreground_check) and not entries:
            raise ValueError("this check needs at least one benchable --model")
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

        def budget_factory(engine: Engine, b: Budget) -> WorkerHandle:
            return WorkerHandle(
                [engine],
                nice=nice_for(b.cpu_priority),
                sandbox=sandbox,
                engine_config=EngineConfig(keep_alive_s=keep_alive_s(settings.idle_unload_s)),
            )

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
        if budget_check:
            budgets = {p: budget_for(p, specs=specs) for p in check_profiles}
            try:
                check = asyncio.run(
                    budget_mod.run_budget_check(cache, budget_factory, entries, budgets, check_cfg)
                )
            except GpuMeterError as exc:
                typer.echo(f"error: {exc}", err=True)
                raise typer.Exit(code=2) from None
            _emit(budget_mod.render_budget_table(check), check.to_json(), out)
            if not check.all_passed:
                raise typer.Exit(code=1)
            return
        if fg_cfg is not None:
            budgets = {p: budget_for(p, specs=specs) for p in check_profiles}
            try:
                fg = asyncio.run(
                    fg_mod.run_foreground_check(
                        cache,
                        budget_factory,
                        entries,
                        budgets,
                        fg_cfg,
                        probe=MlxFrameProbe(),
                        launcher=lambda n: fg_mod.measure_app_launch(n),
                    )
                )
            except (GpuMeterError, ProbeError) as exc:
                typer.echo(f"error: {exc}", err=True)
                raise typer.Exit(code=2) from None
            _emit(fg_mod.render_foreground_table(fg), fg.to_json(), out)
            if fg.failed:
                raise typer.Exit(code=1)
            return
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
