"""Async engine and sessionmaker. The only place the engine is constructed."""

from collections.abc import AsyncIterator

from fastapi import Request
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def create_engine(database_url: SecretStr) -> AsyncEngine:
    return create_async_engine(database_url.get_secret_value(), pool_pre_ping=True)


def create_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    """Request-scoped session for handlers (they call `session.begin()` themselves)."""
    async with request.app.state.sessionmaker() as session:
        yield session
