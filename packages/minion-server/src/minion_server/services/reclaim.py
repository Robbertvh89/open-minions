"""Watchdog reclaim (plan D3, ARCHITECTURE §3.4): expire silent acquirements, requeue or fail.

Reclaim never touches `attempt` (the next acquire bumps it) and never talks to S3: the expired
attempt's output stays at its attempt-scoped key, unpromoted. Ownership is only ever decided by
the acquire INSERT; reclaim just deletes the row under a row lock.
"""

import time
import uuid
from collections import Counter
from enum import StrEnum

import structlog
from minion_common.enums import AttemptOutcome, SubtaskStatus
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from minion_server.repositories import completion as completion_repo
from minion_server.repositories import reclaim as repo
from minion_server.repositories import tasks as tasks_repo
from minion_server.services.completion import finish_failed, finish_unacknowledged, settle_in_tx

log = structlog.get_logger()


class ReclaimResult(StrEnum):
    EXPIRED_REQUEUED = "EXPIRED_REQUEUED"
    EXPIRED_FAILED = "EXPIRED_FAILED"
    EXPIRED_CANCELLED = "EXPIRED_CANCELLED"
    SKIPPED = "SKIPPED"  # gone, or an owner op / another watchdog holds the lock
    NOT_STALE = "NOT_STALE"  # a heartbeat landed while we waited


_BY_STATUS = {
    SubtaskStatus.PENDING: ReclaimResult.EXPIRED_REQUEUED,
    SubtaskStatus.FAILED: ReclaimResult.EXPIRED_FAILED,
    SubtaskStatus.CANCELLED: ReclaimResult.EXPIRED_CANCELLED,
}

_EXPIRED = frozenset(_BY_STATUS.values())


async def reclaim_one(
    session: AsyncSession,
    acquirement_id: uuid.UUID,
    *,
    timeout_s: float,
    max_lease_s: float | None = None,
    unacked_free_cap: int = 0,
) -> ReclaimResult:
    """Reclaim one acquirement inside the caller's open transaction (caller commits).

    Reclaimed when the heartbeat is older than `timeout_s`, or (if `max_lease_s` is set) when the
    acquirement itself is older than `max_lease_s`: an executor that keeps heartbeating but never
    finishes cannot hold a subtask forever. Both are recorded as EXPIRED and consume budget.
    """
    o = await repo.lock_for_reclaim(session, acquirement_id)
    if o is None:
        return ReclaimResult.SKIPPED
    if await repo.heartbeat_stale(session, acquirement_id, timeout_s):
        if unacked_free_cap > 0 and await repo.never_acknowledged(session, acquirement_id):
            status = await finish_unacknowledged(session, o, free_cap=unacked_free_cap)
            return _BY_STATUS[status]
        code, message = "HEARTBEAT_TIMEOUT", "no heartbeat"
    elif max_lease_s is not None and await repo.lease_expired(session, acquirement_id, max_lease_s):
        code, message = "MAX_LEASE_EXCEEDED", "max lease exceeded"
    else:
        return ReclaimResult.NOT_STALE
    status = await finish_failed(
        session,
        o,
        outcome=AttemptOutcome.EXPIRED,
        error={"code": code, "message": message, "retryable": True},
        retryable=True,
    )
    return _BY_STATUS[status]


async def reclaim_stale(
    sm: async_sessionmaker[AsyncSession],
    *,
    timeout_s: float,
    batch_size: int,
    max_lease_s: float | None = None,
    unacked_free_cap: int = 0,
) -> Counter[ReclaimResult]:
    """One short transaction per stale acquirement, so a slow owner op never stalls the batch."""
    async with sm() as s:
        ids = await repo.stale_ids(s, timeout_s, batch_size)
        if max_lease_s is not None:
            seen = set(ids)
            scan_started = time.monotonic()
            over = await repo.over_lease_ids(s, max_lease_s, batch_size)
            log.debug(
                "over_lease_scan",
                scan_ms=round((time.monotonic() - scan_started) * 1000),
                hits=len(over),
            )
            ids += [i for i in over if i not in seen]
    results: Counter[ReclaimResult] = Counter()
    for aid in ids:
        try:
            async with sm() as s, s.begin():
                res = await reclaim_one(
                    s,
                    aid,
                    timeout_s=timeout_s,
                    max_lease_s=max_lease_s,
                    unacked_free_cap=unacked_free_cap,
                )
        except Exception as exc:  # one bad row must not block the rest of the batch
            log.error("reclaim_failed", acquirement_id=str(aid), error_type=type(exc).__name__)
            continue
        results[res] += 1
        if res in _EXPIRED:
            log.info("subtask_reclaimed", acquirement_id=str(aid), result=res.value)
    return results


async def sweep_cancelled(
    sm: async_sessionmaker[AsyncSession],
    *,
    window_s: float = 3600,
    batch: int = tasks_repo.CANCEL_BATCH,
) -> int:
    """PENDING subtasks of recently cancelled tasks -> CANCELLED (plan D4 healing sweep).

    Batched: one short transaction per `batch` rows, until a batch comes back short.
    """
    total = 0
    while True:
        async with sm() as s, s.begin():
            n = await tasks_repo.sweep_cancelled(s, window_s, batch)
        total += n
        if n < batch:
            break
    if total:
        log.info("cancelled_pending_swept", count=total)
    return total


async def settle_stuck(sm: async_sessionmaker[AsyncSession], *, batch: int = 100) -> int:
    """Backstop for ADR-0015: finish live tasks whose counters already cover every subtask.

    Covers a settle lost after the owner op committed (crash, DB blip), a reclaim that failed the
    last subtask, and a retried complete. One short transaction per task. Returns the number of
    tasks made terminal.
    """
    async with sm() as s:
        keys = await completion_repo.settleable_tasks(s, batch)
    settled = 0
    for key in keys:
        try:
            async with sm() as s, s.begin():
                if await settle_in_tx(s, key):
                    settled += 1
        except Exception as exc:  # one bad task must not block the rest
            log.error("task_settle_failed", error=type(exc).__name__)
    if settled:
        log.info("tasks_settled", count=settled)
    return settled
