"""Heartbeat row persistence. SQL lives here only."""

import uuid
from typing import Any

from sqlalchemy import exists, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.db.models import ExecutorHeartbeat, SubtaskAcquirement


async def beat(
    session: AsyncSession, acquirement_id: uuid.UUID, progress: dict[str, Any] | None
) -> None:
    """Refresh `last_seen_at`; keep the previous progress when the beat carries none.

    Caller must hold the acquirement row lock (`lock_owned`), which serializes against reclaim.
    """
    values: dict[str, Any] = {"last_seen_at": func.now()}
    if progress is not None:
        values["progress"] = progress
    await session.execute(
        update(ExecutorHeartbeat)
        .where(ExecutorHeartbeat.acquirement_id == acquirement_id)
        .values(**values)
    )


async def too_soon(session: AsyncSession, acquirement_id: uuid.UUID, min_interval_s: float) -> bool:
    """True if the last beat is younger than `min_interval_s` (caller holds the row lock).

    The claim's own heartbeat row (`last_seen_at = acquired_at`) is no beat: the executor's first
    heartbeat, the ack (ADR-0014), must never be rate limited.
    """
    if min_interval_s <= 0:
        return False
    return bool(
        (
            await session.execute(
                select(
                    exists().where(
                        ExecutorHeartbeat.acquirement_id == acquirement_id,
                        SubtaskAcquirement.acquirement_id == acquirement_id,
                        ExecutorHeartbeat.last_seen_at != SubtaskAcquirement.acquired_at,
                        ExecutorHeartbeat.last_seen_at
                        > func.now() - func.make_interval(0, 0, 0, 0, 0, 0, min_interval_s),
                    )
                )
            )
        ).scalar_one()
    )
