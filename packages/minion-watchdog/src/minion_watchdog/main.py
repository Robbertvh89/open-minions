"""Watchdog (ARCHITECTURE §3.4): reclaim silent acquirements and heal cancelled tasks.

Server-side process: talks to PostgreSQL through the server's repositories (executors never do).
Safe to run as several replicas (SKIP LOCKED + re-check per row).
"""

import asyncio
import contextlib
import logging
import os
import signal
import time

import structlog
from minion_server.db.session import create_engine, create_sessionmaker
from minion_server.services.reclaim import (
    ReclaimResult,
    reclaim_stale,
    settle_stuck,
    sweep_cancelled,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from minion_watchdog.settings import WatchdogSettings

log = structlog.get_logger()

_EXPIRED = frozenset(
    {
        ReclaimResult.EXPIRED_REQUEUED,
        ReclaimResult.EXPIRED_FAILED,
        ReclaimResult.EXPIRED_CANCELLED,
    }
)
SLOW_TICK_MS = 1000  # a tick slower than this is logged at info, else at debug
MAX_ROUNDS_PER_TICK = 100  # bound one tick when a mass outage leaves more than a batch stale


def backoff_s(interval_s: float, failures: int, cap_s: float) -> float:
    """Sleep after `failures` consecutive failed ticks: doubles per failure, capped."""
    return float(min(interval_s * 2 ** min(failures, 20), max(interval_s, cap_s)))


async def tick(
    sm: async_sessionmaker[AsyncSession], cfg: WatchdogSettings, stop: asyncio.Event
) -> None:
    """One pass: reclaim (repeated while batches come back full), settle, sweep cancelled."""
    started = time.monotonic()
    rounds = 0
    for _ in range(MAX_ROUNDS_PER_TICK):
        rounds += 1
        results = await reclaim_stale(
            sm,
            timeout_s=cfg.heartbeat_timeout_s,
            batch_size=cfg.watchdog_batch_size,
            max_lease_s=cfg.max_lease_s,
            unacked_free_cap=cfg.release_limit,
        )
        if results:
            log.info("watchdog_reclaim", **{k.value: v for k, v in results.items()})
        expired = sum(n for r, n in results.items() if r in _EXPIRED)
        if expired < cfg.watchdog_batch_size or stop.is_set():
            break
    # Backstop for the post-commit task settle (ADR-0015); own try so it never stalls the rest.
    try:
        await settle_stuck(sm)
    except Exception as exc:
        log.error("watchdog_settle_failed", error_class=type(exc).__name__)
    # Housekeeping: a failing sweep must not stall reclaim nor trigger tick backoff.
    try:
        await sweep_cancelled(sm, window_s=cfg.cancel_sweep_window_s)
    except Exception as exc:
        log.error("watchdog_sweep_failed", error_class=type(exc).__name__)
    duration_ms = round((time.monotonic() - started) * 1000)
    emit = log.info if duration_ms > SLOW_TICK_MS else log.debug
    emit("watchdog_tick", duration_ms=duration_ms, rounds=rounds)


async def _sleep(stop: asyncio.Event, seconds: float) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)


async def _tick_or_stop(
    sm: async_sessionmaker[AsyncSession], cfg: WatchdogSettings, stop: asyncio.Event
) -> bool:
    """Run one tick, abandoning it as soon as `stop` is set. False if it was abandoned.

    Cancelling mid-tick is safe: every reclaim/sweep step is its own short transaction, which
    rolls back on cancellation.
    """
    work = asyncio.create_task(tick(sm, cfg, stop))
    waiter = asyncio.create_task(stop.wait())
    try:
        await asyncio.wait({work, waiter}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        waiter.cancel()
        if not work.done():
            work.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await work
    if not work.done() or work.cancelled():
        return False
    work.result()  # re-raise a tick error
    return True


def touch_health_file(path: str | None) -> None:
    """Create (0600) or bump the liveness file. Never raises: health must not stop the loop."""
    if not path:
        return
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
        try:
            os.utime(fd)
        finally:
            os.close(fd)
    except OSError as exc:
        log.error("watchdog_health_file_failed", error_class=type(exc).__name__)


async def run_watchdog(
    sm: async_sessionmaker[AsyncSession], cfg: WatchdogSettings, stop: asyncio.Event
) -> None:
    """Loop until `stop` is set. Errors are logged (class only) and retried with backoff."""
    failures = 0
    touch_health_file(cfg.watchdog_health_file)
    while not stop.is_set():
        try:
            if not await _tick_or_stop(sm, cfg, stop):
                break
            failures = 0
            touch_health_file(cfg.watchdog_health_file)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failures += 1
            log.error("watchdog_tick_failed", error_class=type(exc).__name__, failures=failures)
        delay = (
            cfg.watchdog_interval_s
            if failures == 0
            else backoff_s(cfg.watchdog_interval_s, failures, cfg.watchdog_max_backoff_s)
        )
        await _sleep(stop, delay)
    log.info("watchdog_stopped")


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    )


async def _amain() -> None:
    cfg = WatchdogSettings()
    engine = create_engine(
        cfg.database_url,
        pool_size=cfg.watchdog_db_pool_size,
        max_overflow=0,
        connect_args={"command_timeout": cfg.db_command_timeout_s},
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    log.info(
        "watchdog_started",
        interval_s=cfg.watchdog_interval_s,
        heartbeat_timeout_s=cfg.heartbeat_timeout_s,
        max_lease_s=cfg.max_lease_s,
    )
    try:
        await run_watchdog(create_sessionmaker(engine), cfg, stop)
    finally:
        await engine.dispose()


def main() -> None:
    configure_logging()
    asyncio.run(_amain())
