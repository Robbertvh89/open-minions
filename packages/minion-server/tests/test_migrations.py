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


async def _has_column(url: str, table: str, column: str) -> bool:
    eng = create_async_engine(url)
    try:
        async with eng.connect() as conn:
            res = await conn.execute(
                text(
                    "SELECT count(*) FROM information_schema.columns "
                    "WHERE table_name = :t AND column_name = :c"
                ),
                {"t": table, "c": column},
            )
            return int(res.scalar_one()) == 1
    finally:
        await eng.dispose()


def test_0003_task_error_round_trip(fresh_url: str) -> None:
    cfg = alembic_config(fresh_url)
    command.upgrade(cfg, "head")
    assert asyncio.run(_has_column(fresh_url, "minion_task", "error"))
    command.downgrade(cfg, "0002")
    assert not asyncio.run(_has_column(fresh_url, "minion_task", "error"))
    command.upgrade(cfg, "head")
    assert asyncio.run(_has_column(fresh_url, "minion_task", "error"))


def test_downgrade_upgrade_round_trip(fresh_url: str) -> None:
    cfg = alembic_config(fresh_url)
    command.upgrade(cfg, "head")
    assert asyncio.run(_table_count(fresh_url)) == 7
    command.downgrade(cfg, "base")
    assert asyncio.run(_table_count(fresh_url)) == 0
    command.upgrade(cfg, "head")
    assert asyncio.run(_table_count(fresh_url)) == 7


async def _has_constraint(url: str, name: str) -> bool:
    eng = create_async_engine(url)
    try:
        async with eng.connect() as conn:
            res = await conn.execute(
                text("SELECT count(*) FROM pg_constraint WHERE conname = :n"), {"n": name}
            )
            return int(res.scalar_one()) == 1
    finally:
        await eng.dispose()


def test_0004_splitter_backoff_and_unique_output_round_trip(fresh_url: str) -> None:
    cfg = alembic_config(fresh_url)
    command.upgrade(cfg, "head")
    assert asyncio.run(_has_column(fresh_url, "minion_task", "split_attempts"))
    assert asyncio.run(_has_column(fresh_url, "minion_task", "next_split_at"))
    assert asyncio.run(_has_constraint(fresh_url, "uq_minion_task_output_uri"))
    command.downgrade(cfg, "0003")
    assert not asyncio.run(_has_column(fresh_url, "minion_task", "split_attempts"))
    assert not asyncio.run(_has_column(fresh_url, "minion_task", "next_split_at"))
    assert not asyncio.run(_has_constraint(fresh_url, "uq_minion_task_output_uri"))
    command.upgrade(cfg, "head")


async def _scalar(url: str, sql: str) -> int:
    eng = create_async_engine(url)
    try:
        async with eng.connect() as conn:
            return int((await conn.execute(text(sql))).scalar_one())
    finally:
        await eng.dispose()


def test_0007_readonly_views_round_trip(fresh_url: str) -> None:
    views = "SELECT count(*) FROM pg_views WHERE schemaname = 'minion_ro'"
    schema = "SELECT count(*) FROM pg_namespace WHERE nspname = 'minion_ro'"
    role = "SELECT count(*) FROM pg_roles WHERE rolname = 'minion_readonly'"
    cfg = alembic_config(fresh_url)
    command.upgrade(cfg, "head")
    assert asyncio.run(_scalar(fresh_url, views)) == 5
    assert asyncio.run(_scalar(fresh_url, role)) == 1
    command.downgrade(cfg, "0006")
    assert asyncio.run(_scalar(fresh_url, schema)) == 0
    assert asyncio.run(_table_count(fresh_url)) == 7  # tables untouched
    command.upgrade(cfg, "head")
    assert asyncio.run(_scalar(fresh_url, views)) == 5


async def _view_privileges(url: str) -> list[tuple[str, bool]]:
    eng = create_async_engine(url)
    try:
        async with eng.connect() as conn:
            res = await conn.execute(
                text(
                    "SELECT viewname, has_table_privilege('minion_readonly',"
                    " format('%I.%I', schemaname, viewname), 'SELECT')"
                    " FROM pg_views WHERE schemaname = 'minion_ro' ORDER BY 1"
                )
            )
            return [(r[0], bool(r[1])) for r in res]
    finally:
        await eng.dispose()


def test_0007_minion_readonly_can_select_every_view(fresh_url: str) -> None:
    """A view added or recreated later must be granted again (db-migration skill)."""
    command.upgrade(alembic_config(fresh_url), "head")
    privs = asyncio.run(_view_privileges(fresh_url))
    assert len(privs) == 5
    assert all(ok for _, ok in privs), privs
