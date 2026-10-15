"""Executor persistence. SQL lives here only."""

import uuid
from dataclasses import dataclass
from datetime import datetime

from minion_common.enums import ExecutorStatus
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.db.models import Executor


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
