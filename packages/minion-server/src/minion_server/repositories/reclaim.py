"""Watchdog reclaim persistence. SQL lives here only.

Reclaim shares the lock order of every owner op: acquirement -> heartbeat -> subtask -> task
(ARCHITECTURE §4), so it cannot deadlock against complete / fail / release / heartbeat.
"""

import uuid
from typing import Any

from sqlalchemy import Select, exists, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.db.models import ExecutorHeartbeat, SubtaskAcquirement
from minion_server.repositories.completion import Owned, owned


def _cutoff(timeout_s: float) -> Any:
    return func.now() - func.make_interval(0, 0, 0, 0, 0, 0, timeout_s)


async def stale_ids(session: AsyncSession, timeout_s: float, limit: int) -> list[uuid.UUID]:
    """Unlocked scan (uses the `last_seen_at` index); each hit is re-checked under its lock."""
    rows = await session.execute(
        select(ExecutorHeartbeat.acquirement_id)
        .where(ExecutorHeartbeat.last_seen_at < _cutoff(timeout_s))
        .order_by(ExecutorHeartbeat.last_seen_at)
        .limit(limit)
    )
    return list(rows.scalars())


def over_lease_stmt(max_lease_s: float, limit: int) -> Select[uuid.UUID]:
    """The max-lease scan; served by `ix_minion_subtask_acquirement_acquired_at` (0008)."""
    return (
        select(SubtaskAcquirement.acquirement_id)
        .where(SubtaskAcquirement.acquired_at < _cutoff(max_lease_s))
        .order_by(SubtaskAcquirement.acquired_at)
        .limit(limit)
    )


async def over_lease_ids(session: AsyncSession, max_lease_s: float, limit: int) -> list[uuid.UUID]:
    """Acquirements older than the max lease, however fresh their heartbeat (unlocked scan).

    Uses `ix_minion_subtask_acquirement_acquired_at` (range scan on `acquired_at`), so cost
    follows the number of expired rows, not the table size.
    """
    rows = await session.execute(over_lease_stmt(max_lease_s, limit))
    return list(rows.scalars())


async def lease_expired(
    session: AsyncSession, acquirement_id: uuid.UUID, max_lease_s: float
) -> bool:
    """Fresh statement after the lock, like `heartbeat_stale`."""
    return bool(
        (
            await session.execute(
                select(
                    exists().where(
                        SubtaskAcquirement.acquirement_id == acquirement_id,
                        SubtaskAcquirement.acquired_at < _cutoff(max_lease_s),
                    )
                )
            )
        ).scalar_one()
    )


async def lock_for_reclaim(session: AsyncSession, acquirement_id: uuid.UUID) -> Owned | None:
    """Lock the acquirement; None if it is gone or an owner op / other watchdog holds the lock."""
    row = (
        await session.execute(
            select(SubtaskAcquirement)
            .where(SubtaskAcquirement.acquirement_id == acquirement_id)
            .with_for_update(skip_locked=True)
        )
    ).scalar_one_or_none()
    if row is None:
        return None
    return await owned(session, row)


async def heartbeat_stale(
    session: AsyncSession, acquirement_id: uuid.UUID, timeout_s: float
) -> bool:
    """Fresh statement after the lock: under READ COMMITTED it sees a beat committed meanwhile."""
    return bool(
        (
            await session.execute(
                select(
                    exists().where(
                        ExecutorHeartbeat.acquirement_id == acquirement_id,
                        ExecutorHeartbeat.last_seen_at < _cutoff(timeout_s),
                    )
                )
            )
        ).scalar_one()
    )
