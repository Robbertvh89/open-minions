"""Heartbeat row persistence. SQL lives here only."""

import uuid
from typing import Any

from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.db.models import ExecutorHeartbeat


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
