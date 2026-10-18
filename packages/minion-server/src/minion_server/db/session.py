"""Async engine and sessionmaker. The only place the engine is constructed."""

from collections.abc import AsyncIterator
from typing import Any

from fastapi import Request
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def create_engine(
    database_url: SecretStr,
    *,
    pool_size: int = 20,
    max_overflow: int = 40,
    pool_timeout: float = 5.0,
    connect_args: dict[str, Any] | None = None,
    pre_ping: bool = True,
) -> AsyncEngine:
    return create_async_engine(
        database_url.get_secret_value(),
        pool_pre_ping=pre_ping,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,  # wait for a connection, then TimeoutError -> 503 (TASK-7.4.4)
        connect_args=connect_args or {},
    )


def create_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    """Request-scoped session for handlers (they call `session.begin()` themselves)."""
    async with request.app.state.sessionmaker() as session:
        yield session
