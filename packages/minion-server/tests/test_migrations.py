"""Migrations match the ORM models and round-trips (down to base and back)."""

import asyncio
from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from minion_server.db.models import Base
from sqlalchemy import Connection, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from testcontainers.community.postgres import PostgresContainer

REPO_ROOT = Path(__file__).resolve().parents[3]

pytestmark = pytest.mark.integration


def alembic_config(url: str) -> Config:
    cfg = Config(str(REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


def _diff(conn: Connection) -> list[object]:
    ctx = MigrationContext.configure(conn)
    return list(compare_metadata(ctx, Base.metadata))


async def test_migrated_schema_matches_models(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        diffs = await conn.run_sync(_diff)
    assert diffs == []


@pytest.fixture
def fresh_url() -> Iterator[str]:
    with PostgresContainer("postgres:16-alpine", driver="asyncpg") as pg:
        yield pg.get_connection_url()


async def _table_count(url: str) -> int:
    eng = create_async_engine(url)
    try:
        async with eng.connect() as conn:
            res = await conn.execute(
                text("SELECT count(*) FROM pg_tables WHERE tablename LIKE 'minion_%'")
            )
            return int(res.scalar_one())
    finally:
        await eng.dispose()


def test_0002_downgrade_removes_token_expiry_only(fresh_url: str) -> None:
    cfg = alembic_config(fresh_url)
    command.upgrade(cfg, "head")
    command.downgrade(cfg, "0001")
    assert asyncio.run(_table_count(fresh_url)) == 7
    command.upgrade(cfg, "head")


def test_downgrade_upgrade_round_trip(fresh_url: str) -> None:
    cfg = alembic_config(fresh_url)
    command.upgrade(cfg, "head")
    assert asyncio.run(_table_count(fresh_url)) == 7
    command.downgrade(cfg, "base")
    assert asyncio.run(_table_count(fresh_url)) == 0
    command.upgrade(cfg, "head")
    assert asyncio.run(_table_count(fresh_url)) == 7
