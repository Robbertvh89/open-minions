"""Watchdog (ARCHITECTURE §3.4): reclaim silent acquirements and heal cancelled tasks.

Server-side process: talks to PostgreSQL through the server's repositories (executors never do).
Safe to run as several replicas (SKIP LOCKED + re-check per row).
"""

import asyncio
import contextlib
import logging
import signal

import structlog
from minion_server.db.session import create_engine, create_sessionmaker
from minion_server.services.reclaim import ReclaimResult, reclaim_stale, sweep_cancelled
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
MAX_ROUNDS_PER_TICK = 100  # bound one tick when a mass outage leaves more than a batch stale


def backoff_s(interval_s: float, failures: int, cap_s: float) -> float:
    """Sleep after `failures` consecutive failed ticks: doubles per failure, capped."""
    return float(min(interval_s * 2 ** min(failures, 20), max(interval_s, cap_s)))


async def tick(
    sm: async_sessionmaker[AsyncSession], cfg: WatchdogSettings, stop: asyncio.Event
) -> None:
    """One pass: reclaim (repeated while batches come back full), then sweep cancelled tasks."""
    for _ in range(MAX_ROUNDS_PER_TICK):
        results = await reclaim_stale(
            sm,
            timeout_s=cfg.heartbeat_timeout_s,
            batch_size=cfg.watchdog_batch_size,
            max_lease_s=cfg.max_lease_s,
        )
        if results:
            log.info("watchdog_reclaim", **{k.value: v for k, v in results.items()})
        expired = sum(n for r, n in results.items() if r in _EXPIRED)
        if expired < cfg.watchdog_batch_size or stop.is_set():
            break
    await sweep_cancelled(sm, window_s=cfg.cancel_sweep_window_s)


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


async def run_watchdog(
    sm: async_sessionmaker[AsyncSession], cfg: WatchdogSettings, stop: asyncio.Event
) -> None:
    """Loop until `stop` is set. Errors are logged (class only) and retried with backoff."""
    failures = 0
    while not stop.is_set():
        try:
            if not await _tick_or_stop(sm, cfg, stop):
                break
            failures = 0
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
        pool_size=cfg.db_pool_size,
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
