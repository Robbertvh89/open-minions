"""Watchdog reclaim persistence. SQL lives here only.

Reclaim shares the lock order of every owner op: acquirement -> heartbeat -> subtask -> task
(ARCHITECTURE §4), so it cannot deadlock against complete / fail / release / heartbeat.
"""

import uuid
from typing import Any

from sqlalchemy import exists, func, select
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
