"""Executor persistence. SQL lives here only."""

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from minion_common.enums import ExecutorStatus
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.db.models import Executor, ExecutorPreference


@dataclass(frozen=True)
class ExecutorIdentity:
    executor_id: uuid.UUID
    owner: str
    pool: str
    status: ExecutorStatus
    token_expires_at: datetime | None
    token_hash: str


async def get_by_token_hash(session: AsyncSession, token_hash: str) -> ExecutorIdentity | None:
    row = (
        await session.execute(
            select(
                Executor.executor_id,
                Executor.owner,
                Executor.pool,
                Executor.status,
                Executor.token_expires_at,
                Executor.token_hash,
            ).where(Executor.token_hash == token_hash)
        )
    ).one_or_none()
    if row is None:
        return None
    return ExecutorIdentity(row[0], row[1], row[2], ExecutorStatus(row[3]), row[4], row[5])


async def insert_unregistered(
    session: AsyncSession,
    *,
    token_hash: str,
    owner: str,
    pool: str,
    token_expires_at: datetime,
) -> uuid.UUID:
    executor_id = uuid.uuid4()
    session.add(
        Executor(
            executor_id=executor_id,
            token_hash=token_hash,
            hostname="(unregistered)",
            owner=owner,
            pool=pool,
            token_expires_at=token_expires_at,
        )
    )
    await session.flush()
    return executor_id


async def revoke(session: AsyncSession, executor_id: uuid.UUID) -> bool:
    """Mark the executor REVOKED; returns False if it does not exist."""
    result = await session.execute(
        update(Executor)
        .where(Executor.executor_id == executor_id)
        .values(status=ExecutorStatus.REVOKED.value)
    )
    return bool(result.rowcount)  # type: ignore[attr-defined]


async def register(
    session: AsyncSession,
    executor_id: uuid.UUID,
    *,
    hostname: str,
    chip: str,
    total_memory_gb: float,
    os_version: str,
    engines: list[str],
    profile: str,
    budget: dict[str, Any],
) -> bool:
    """Record host capabilities. A REVOKED executor is never touched (cannot un-revoke itself)."""
    result = await session.execute(
        update(Executor)
        .where(
            Executor.executor_id == executor_id,
            Executor.status != ExecutorStatus.REVOKED.value,
        )
        .values(
            hostname=hostname,
            chip=chip,
            total_memory_gb=total_memory_gb,
            os_version=os_version,
            engines=engines,
            profile=profile,
            budget=budget,
            last_seen_at=func.now(),
        )
    )
    return bool(result.rowcount)  # type: ignore[attr-defined]


async def touch(
    session: AsyncSession,
    executor_id: uuid.UUID,
    *,
    engines: list[str],
    profile: str,
    budget: dict[str, Any],
) -> None:
    """Acquire-time refresh of the reported budget and liveness."""
    await session.execute(
        update(Executor)
        .where(
            Executor.executor_id == executor_id,
            Executor.status != ExecutorStatus.REVOKED.value,
        )
        .values(
            engines=engines,
            profile=profile,
            budget=budget,
            paused_reason=None,  # an acquiring executor is by definition not paused
            last_seen_at=func.now(),
        )
    )


async def report_state(
    session: AsyncSession,
    executor_id: uuid.UUID,
    *,
    profile: str | None,
    budget: dict[str, Any] | None,
    paused_reason: str | None,
) -> bool:
    """Heartbeat/presence-time refresh of liveness and the reported budget/pause state."""
    values: dict[str, Any] = {"last_seen_at": func.now(), "paused_reason": paused_reason}
    if profile is not None:
        values["profile"] = profile
    if budget is not None:
        values["budget"] = budget
    result = await session.execute(
        update(Executor)
        .where(
            Executor.executor_id == executor_id,
            Executor.status != ExecutorStatus.REVOKED.value,
        )
        .values(**values)
    )
    return bool(result.rowcount)  # type: ignore[attr-defined]


async def set_preference(
    session: AsyncSession,
    executor_id: uuid.UUID,
    *,
    team: str | None,
    project: str | None,
    task_name: str | None,
) -> None:
    stmt = pg_insert(ExecutorPreference).values(
        executor_id=executor_id, team=team, project=project, task_name=task_name
    )
    await session.execute(
        stmt.on_conflict_do_update(
            index_elements=[ExecutorPreference.executor_id],
            set_={
                "team": stmt.excluded.team,
                "project": stmt.excluded.project,
                "task_name": stmt.excluded.task_name,
                "updated_at": func.now(),
            },
        )
    )


async def get_preference(
    session: AsyncSession, executor_id: uuid.UUID
) -> tuple[str | None, str | None, str | None] | None:
    """`(team, project, task_name)` with None wildcards, or None if no (non-empty) preference."""
    row = (
        await session.execute(
            select(
                ExecutorPreference.team, ExecutorPreference.project, ExecutorPreference.task_name
            ).where(ExecutorPreference.executor_id == executor_id)
        )
    ).one_or_none()
    if row is None or all(v is None for v in row):
        return None
    return (row[0], row[1], row[2])
