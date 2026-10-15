"""Watchdog reclaim (plan D3, ARCHITECTURE §3.4): expire silent acquirements, requeue or fail.

Reclaim never touches `attempt` (the next acquire bumps it) and never talks to S3: the expired
attempt's output stays at its attempt-scoped key, unpromoted. Ownership is only ever decided by
the acquire INSERT; reclaim just deletes the row under a row lock.
"""

import uuid
from collections import Counter
from enum import StrEnum

import structlog
from minion_common.enums import AttemptOutcome, SubtaskStatus
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from minion_server.repositories import reclaim as repo
from minion_server.services.completion import finish_failed

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
    session: AsyncSession, acquirement_id: uuid.UUID, *, timeout_s: float
) -> ReclaimResult:
    """Reclaim one acquirement inside the caller's open transaction (caller commits)."""
    o = await repo.lock_for_reclaim(session, acquirement_id)
    if o is None:
        return ReclaimResult.SKIPPED
    if not await repo.heartbeat_stale(session, acquirement_id, timeout_s):
        return ReclaimResult.NOT_STALE
    status = await finish_failed(
        session,
        o,
        outcome=AttemptOutcome.EXPIRED,
        error={"code": "HEARTBEAT_TIMEOUT", "message": "no heartbeat", "retryable": True},
        retryable=True,
    )
    return _BY_STATUS[status]


async def reclaim_stale(
    sm: async_sessionmaker[AsyncSession], *, timeout_s: float, batch_size: int
) -> Counter[ReclaimResult]:
    """One short transaction per stale acquirement, so a slow owner op never stalls the batch."""
    async with sm() as s:
        ids = await repo.stale_ids(s, timeout_s, batch_size)
    results: Counter[ReclaimResult] = Counter()
    for aid in ids:
        try:
            async with sm() as s, s.begin():
                res = await reclaim_one(s, aid, timeout_s=timeout_s)
        except Exception as exc:  # one bad row must not block the rest of the batch
            log.error("reclaim_failed", acquirement_id=str(aid), error_type=type(exc).__name__)
            continue
        results[res] += 1
        if res in _EXPIRED:
            log.info("subtask_reclaimed", acquirement_id=str(aid), result=res.value)
    return results
