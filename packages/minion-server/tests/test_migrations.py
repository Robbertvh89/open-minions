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
    assert asyncio.run(_table_count(fresh_url)) == 8
    command.downgrade(cfg, "base")
    assert asyncio.run(_table_count(fresh_url)) == 0
    command.upgrade(cfg, "head")
    assert asyncio.run(_table_count(fresh_url)) == 8


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


async def _index_exists(url: str, name: str) -> bool:
    eng = create_async_engine(url)
    try:
        async with eng.connect() as conn:
            res = await conn.execute(
                text("SELECT count(*) FROM pg_indexes WHERE indexname = :n"), {"n": name}
            )
            return int(res.scalar_one()) == 1
    finally:
        await eng.dispose()


def test_0008_acquired_at_index_round_trip(fresh_url: str) -> None:
    name = "ix_minion_subtask_acquirement_acquired_at"
    cfg = alembic_config(fresh_url)
    command.upgrade(cfg, "head")
    assert asyncio.run(_index_exists(fresh_url, name))
    command.downgrade(cfg, "0007")
    assert not asyncio.run(_index_exists(fresh_url, name))
    command.upgrade(cfg, "head")
    assert asyncio.run(_index_exists(fresh_url, name))


async def _subtask_reloptions(url: str) -> list[str]:
    eng = create_async_engine(url)
    try:
        async with eng.connect() as conn:
            res = await conn.execute(
                text("SELECT reloptions FROM pg_class WHERE relname = 'minion_subtask'")
            )
            return sorted(res.scalar_one() or [])
    finally:
        await eng.dispose()


def test_0010_subtask_autovacuum_round_trip(fresh_url: str) -> None:
    cfg = alembic_config(fresh_url)
    command.upgrade(cfg, "head")
    assert asyncio.run(_subtask_reloptions(fresh_url)) == [
        "autovacuum_analyze_scale_factor=0.02",
        "autovacuum_vacuum_scale_factor=0.02",
        "autovacuum_vacuum_threshold=1000",
    ]
    command.downgrade(cfg, "0009")
    assert asyncio.run(_subtask_reloptions(fresh_url)) == []
    command.upgrade(cfg, "head")
    assert len(asyncio.run(_subtask_reloptions(fresh_url))) == 3


async def _exec(url: str, sql: str) -> None:
    eng = create_async_engine(url)
    try:
        async with eng.begin() as conn:
            await conn.execute(text(sql))
    finally:
        await eng.dispose()


async def _view_cols(url: str) -> list[tuple[str, str]]:
    eng = create_async_engine(url)
    try:
        async with eng.connect() as conn:
            res = await conn.execute(
                text(
                    "SELECT a.attname, format_type(a.atttypid, a.atttypmod)"
                    " FROM pg_attribute a WHERE a.attrelid = 'minion_ro.task'::regclass"
                    " AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum"
                )
            )
            return [(r[0], r[1]) for r in res]
    finally:
        await eng.dispose()


_INSERT_TASK_0010 = (
    "INSERT INTO minion_task (team, project, task_name, type, model_id, engine, model_memory_gb,"
    " input_uri, output_uri, created_by, status, total_subtasks, succeeded_subtasks,"
    " failed_subtasks) VALUES ('t', 'p', '{n}', 'embedding', 'm', 'mlx', 1, 's3://i',"
    " 's3://o/{n}', 'u', 'RUNNING', 10, {s}, {f})"
)


def test_0011_task_counters_backfill_and_round_trip(fresh_url: str) -> None:
    cfg = alembic_config(fresh_url)
    command.upgrade(cfg, "0010")
    before = asyncio.run(_view_cols(fresh_url))
    asyncio.run(_exec(fresh_url, _INSERT_TASK_0010.format(n="a", s=4, f=2)))
    asyncio.run(_exec(fresh_url, _INSERT_TASK_0010.format(n="z", s=0, f=0)))

    command.upgrade(cfg, "0011")
    assert asyncio.run(_view_cols(fresh_url)) == before  # same names and types
    assert (
        asyncio.run(_scalar(fresh_url, "SELECT sum(succeeded + failed) FROM minion_task_counter"))
        == 6
    )
    assert asyncio.run(_scalar(fresh_url, "SELECT count(*) FROM minion_task_counter")) == 1
    assert (
        asyncio.run(
            _scalar(
                fresh_url,
                "SELECT count(*) FROM minion_ro.task WHERE task_name='a' AND succeeded_subtasks=4"
                " AND failed_subtasks=2",
            )
        )
        == 1
    )
    cols = (
        "SELECT count(*) FROM information_schema.columns WHERE table_name='minion_task'"
        " AND column_name IN ('succeeded_subtasks', 'failed_subtasks')"
    )
    assert asyncio.run(_scalar(fresh_url, cols)) == 0
    assert asyncio.run(_table_count(fresh_url)) == 8
    privs = asyncio.run(_view_privileges(fresh_url))
    assert all(ok for _, ok in privs), privs

    # a second shard is summed by the view; the downgrade folds all shards back
    asyncio.run(
        _exec(
            fresh_url,
            "INSERT INTO minion_task_counter VALUES ('t', 'p', 'a', 5, 3, 1)",
        )
    )
    command.downgrade(cfg, "0010")
    assert asyncio.run(_scalar(fresh_url, cols)) == 2
    assert asyncio.run(_view_cols(fresh_url)) == before
    assert asyncio.run(_table_count(fresh_url)) == 7
    assert (
        asyncio.run(
            _scalar(
                fresh_url,
                "SELECT succeeded_subtasks * 100 + failed_subtasks FROM minion_task"
                " WHERE task_name='a'",
            )
        )
        == 7 * 100 + 3
    )
    command.upgrade(cfg, "head")
    assert (
        asyncio.run(
            _scalar(fresh_url, "SELECT succeeded_subtasks FROM minion_ro.task WHERE task_name='a'")
        )
        == 7
    )


def test_0012_executor_id_index_round_trip(fresh_url: str) -> None:
    name = "ix_minion_subtask_acquirement_executor_id"
    cfg = alembic_config(fresh_url)
    command.upgrade(cfg, "head")
    assert asyncio.run(_index_exists(fresh_url, name))
    command.downgrade(cfg, "0011")
    assert not asyncio.run(_index_exists(fresh_url, name))
    command.upgrade(cfg, "head")
    assert asyncio.run(_index_exists(fresh_url, name))


def test_0013_cancelled_backfill_and_round_trip(fresh_url: str) -> None:
    cfg = alembic_config(fresh_url)
    command.upgrade(cfg, "0012")
    asyncio.run(
        _exec(
            fresh_url,
            "INSERT INTO minion_task (team, project, task_name, type, model_id, engine,"
            " model_memory_gb, input_uri, output_uri, created_by, status, total_subtasks)"
            " VALUES ('t', 'p', 'c', 'embedding', 'm', 'mlx', 1, 's3://i', 's3://o/c', 'u',"
            " 'RUNNING', 40)",
        )
    )
    # 40 subtasks; CANCELLED on every id except multiples of 5; shard 3 has a prior counter row
    asyncio.run(
        _exec(
            fresh_url,
            "INSERT INTO minion_subtask (team, project, task_name, subtask_id, status,"
            " input_object_key, byte_start, byte_end, first_record, record_count)"
            " SELECT 't', 'p', 'c', g, CASE WHEN g % 5 = 0 THEN 'PENDING' ELSE 'CANCELLED' END,"
            " 'k', 0, 1, 0, 1 FROM generate_series(0, 39) g",
        )
    )
    asyncio.run(_exec(fresh_url, "INSERT INTO minion_task_counter VALUES ('t', 'p', 'c', 3, 2, 0)"))
    expected = asyncio.run(
        _scalar(fresh_url, "SELECT count(*) FROM minion_subtask WHERE status = 'CANCELLED'")
    )

    command.upgrade(cfg, "0013")
    assert asyncio.run(_scalar(fresh_url, "SELECT sum(cancelled) FROM minion_task_counter")) == (
        expected
    )
    # per shard equal to the grouped aggregate (no mismatching shard)
    mismatch = (
        "SELECT count(*) FROM minion_task_counter c WHERE c.cancelled <> ("
        "SELECT count(*) FROM minion_subtask s WHERE s.status = 'CANCELLED'"
        " AND s.subtask_id % 16 = c.shard)"
    )
    assert asyncio.run(_scalar(fresh_url, mismatch)) == 0
    assert (
        asyncio.run(_scalar(fresh_url, "SELECT succeeded FROM minion_task_counter WHERE shard = 3"))
        == 2
    )
    assert asyncio.run(_has_column(fresh_url, "minion_task_counter", "cancelled"))

    command.downgrade(cfg, "0012")
    assert not asyncio.run(_has_column(fresh_url, "minion_task_counter", "cancelled"))
    command.upgrade(cfg, "head")
    assert asyncio.run(_has_column(fresh_url, "minion_task_counter", "cancelled"))


def test_0014_team_live_index_round_trip(fresh_url: str) -> None:
    name = "ix_minion_task_team_status"
    cfg = alembic_config(fresh_url)
    command.upgrade(cfg, "head")
    assert asyncio.run(_index_exists(fresh_url, name))
    command.downgrade(cfg, "0013")
    assert not asyncio.run(_index_exists(fresh_url, name))
    command.upgrade(cfg, "head")
    assert asyncio.run(_index_exists(fresh_url, name))


def test_0015_split_failures_round_trip(fresh_url: str) -> None:
    cfg = alembic_config(fresh_url)
    command.upgrade(cfg, "head")
    assert asyncio.run(_has_column(fresh_url, "minion_task", "split_failures"))
    assert asyncio.run(_has_constraint(fresh_url, "ck_minion_task_split_failures_nonneg"))
    command.downgrade(cfg, "0014")
    assert not asyncio.run(_has_column(fresh_url, "minion_task", "split_failures"))
    assert not asyncio.run(_has_constraint(fresh_url, "ck_minion_task_split_failures_nonneg"))
    assert asyncio.run(_has_column(fresh_url, "minion_task", "split_attempts"))
    command.upgrade(cfg, "head")


async def _indexdef(url: str, name: str) -> str:
    eng = create_async_engine(url)
    try:
        async with eng.connect() as conn:
            res = await conn.execute(
                text("SELECT indexdef FROM pg_indexes WHERE indexname = :n"), {"n": name}
            )
            return str(res.scalar_one())
    finally:
        await eng.dispose()


def test_0016_live_queue_expression_round_trip(fresh_url: str) -> None:
    name = "ix_minion_task_live_queue"
    cfg = alembic_config(fresh_url)
    command.upgrade(cfg, "head")
    assert "(- priority)" in asyncio.run(_indexdef(fresh_url, name))
    command.downgrade(cfg, "0015")
    assert "priority DESC" in asyncio.run(_indexdef(fresh_url, name))
    command.upgrade(cfg, "head")
    assert "(- priority)" in asyncio.run(_indexdef(fresh_url, name))
