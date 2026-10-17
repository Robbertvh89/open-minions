"""Database statistics (TASK-7.2 b)."""

import pytest
from minion_scale.dbstats import db_report, docker_stats, snapshot, statement_deltas
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


def _st(qid: str, calls: int, ms: float) -> dict[str, object]:
    return {
        "queryid": qid,
        "calls": calls,
        "total_exec_ms": ms,
        "rows": calls,
        "shared_blks_hit": calls * 2,
        "shared_blks_read": 0,
        "query": f"select {qid}",
    }


def test_statement_deltas_subtract_the_baseline_and_sort_by_time() -> None:
    before = [_st("a", 10, 5.0), _st("b", 1, 1.0)]
    after = [_st("a", 30, 25.0), _st("b", 1, 1.0), _st("c", 4, 100.0)]
    out = statement_deltas(before, after)
    assert out is not None
    assert [d["query"] for d in out] == ["select c", "select a"]  # b made no calls
    assert out[1]["calls"] == 20 and out[1]["total_exec_ms"] == 20.0
    assert out[1]["mean_exec_ms"] == 1.0
    assert statement_deltas(before, None) is None


def test_docker_stats_needs_a_project_name() -> None:
    assert docker_stats("") is None


@pytest.mark.integration
async def test_snapshot_without_pg_stat_statements(fresh_url: str) -> None:
    eng = create_async_engine(fresh_url)
    try:
        sm = async_sessionmaker(eng, expire_on_commit=False)
        before, after = await snapshot(sm), await snapshot(sm)
    finally:
        await eng.dispose()
    assert before["statements"] is None  # extension not preloaded in the test container
    names = {t["table"] for t in before["tables"]}
    assert {"minion_subtask", "minion_task", "minion_subtask_acquirement"} <= names
    assert before["connections"]["total"] >= 1 and before["connections"]["max"] > 0
    report = db_report(before, after)
    assert report["pg_stat_statements"] is None


@pytest.mark.integration
async def test_statements_are_limited_to_this_role_and_non_utility() -> None:
    from minion_scale.dbstats import ensure_extension
    from sqlalchemy import text
    from testcontainers.community.postgres import PostgresContainer

    pg = PostgresContainer("postgres:16-alpine", driver="asyncpg").with_command(
        "postgres -c shared_preload_libraries=pg_stat_statements -c pg_stat_statements.track=all"
    )
    with pg:
        url = pg.get_connection_url()
        eng = create_async_engine(url)
        try:
            sm = async_sessionmaker(eng, expire_on_commit=False)
            assert await ensure_extension(sm) is True
            async with eng.connect() as c:
                await c.execute(text("CREATE ROLE other LOGIN PASSWORD 'other'"))
                await c.execute(text("GRANT ALL ON SCHEMA public TO other"))
                await c.commit()
            other = create_async_engine(make_url(url).set(username="other", password="other"))
            async with other.connect() as c:
                await c.execute(text("SELECT 1 AS other_role_marker"))
            await other.dispose()
            async with eng.connect() as c:
                await c.execute(text("SELECT 1 AS own_select_marker"))
                await c.execute(text("CREATE TABLE utility_marker (secret_value int)"))
                await c.execute(text("SET application_name = 'utility-set-marker'"))
                await c.commit()
            snap = await snapshot(sm)
        finally:
            await eng.dispose()
    queries = " ".join(s["query"] for s in snap["statements"] or [])
    assert "own_select_marker" in queries  # this role's SELECT is reported
    assert "other_role_marker" not in queries  # another role's is not
    assert "utility_marker" not in queries and "utility-set-marker" not in queries
