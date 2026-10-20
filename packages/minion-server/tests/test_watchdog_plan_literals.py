"""TASK-57: watchdog / settle / cancel-sweep / splitter-claim statements keep their index paths
under a generic (prepared) plan.

asyncpg prepares every statement; after five runs Postgres may switch to a generic plan, which
cannot see a bound status value and so loses partial-index paths. Statuses are therefore SQL
literals (`status_literal`). For each statement the scan nodes of the generic plan must equal
those of the plan with every value inlined, at 10k live + 40k history tasks.
"""

import json
import re
from collections.abc import Callable
from typing import Any

import pytest
from minion_server.repositories import tasks as tasks_repo
from minion_server.repositories.completion import settleable_stmt
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

pytestmark = pytest.mark.integration

LIVE = 10_000
HISTORY = 40_000

SEED = """
INSERT INTO minion_task (team, project, task_name, type, model_id, engine, model_memory_gb,
  input_uri, output_uri, priority, status, created_by, created_at, finished_at)
SELECT 'old' || (g % 20), 'p', 'n' || g, 'embedding', 'm1', 'fake', 1, 'in', 's3://old/' || g,
  0, 'SUCCEEDED', 't', now() - interval '30 days' - make_interval(secs => g),
  now() - interval '29 days' - make_interval(secs => g)
FROM generate_series(1, {hist}) AS g;
INSERT INTO minion_task (team, project, task_name, type, model_id, engine, model_memory_gb,
  input_uri, output_uri, priority, status, created_by, created_at, total_subtasks)
SELECT 'live' || (g % 20), 'p', 'l' || g, 'embedding', 'm1', 'fake', 1, 'in', 's3://l/' || g,
  0, CASE WHEN g % 10 = 0 THEN 'PENDING' WHEN g % 50 = 1 THEN 'SPLITTING' ELSE 'RUNNING' END,
  't', now() - make_interval(secs => g), 0
FROM generate_series(1, {live}) AS g;
INSERT INTO minion_task (team, project, task_name, type, model_id, engine, model_memory_gb,
  input_uri, output_uri, priority, status, created_by, created_at, finished_at)
SELECT 'live' || (g % 20), 'p', 'c' || g, 'embedding', 'm1', 'fake', 1, 'in', 's3://c/' || g,
  0, 'CANCELLED', 't', now() - make_interval(secs => g), now() - make_interval(secs => g)
FROM generate_series(1, 100) AS g;
INSERT INTO minion_subtask (team, project, task_name, subtask_id, status, input_object_key,
  byte_start, byte_end, first_record, record_count)
SELECT team, project, task_name, s, 'PENDING', 'k', 0, 1, 0, 1
FROM minion_task, generate_series(1, 2) AS s WHERE task_name ~ '^[lc]';
INSERT INTO minion_task_counter (team, project, task_name, shard, succeeded, failed)
SELECT team, project, task_name, 0, 1, 0 FROM minion_task
WHERE task_name ~ '^l';
VACUUM ANALYZE minion_task;
VACUUM ANALYZE minion_subtask;
VACUUM ANALYZE minion_task_counter
"""


def _nodes(n: dict[str, Any]) -> list[dict[str, Any]]:
    out = [n]
    for c in n.get("Plans", []):
        out.extend(_nodes(c))
    return out


def _scans(plan: list[dict[str, Any]]) -> list[str]:
    out = []
    for n in _nodes(plan[0]["Plan"]):
        if "Scan" in n["Node Type"] and n["Node Type"] != "CTE Scan":
            out.append(f"{n['Node Type']}:{n.get('Index Name') or n.get('Relation Name')}")
    return sorted(out)


async def _explain(conn: AsyncConnection, sql: str) -> list[dict[str, Any]]:
    raw = (await conn.exec_driver_sql("EXPLAIN (FORMAT JSON) " + sql)).scalar_one()
    return json.loads(raw) if isinstance(raw, str) else raw  # type: ignore[no-any-return]


def _fmt(v: Any) -> str:
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)


async def _plans(conn: AsyncConnection, stmt: Any, name: str) -> tuple[list[str], list[str]]:
    c: Any = stmt.compile(
        dialect=postgresql.asyncpg.dialect(),  # type: ignore[no-untyped-call]
        compile_kwargs={"render_postcompile": True},
    )
    sql = str(c)
    vals = [c.params[n] for n in c.positiontup]
    lit = sql
    for i in sorted(range(len(vals)), reverse=True):
        lit = re.sub(rf"\${i + 1}(?!\d)", _fmt(vals[i]), lit)
    # asyncpg-dialect SQL carries ::TYPE casts on params; they survive literal substitution.
    literal_scans = _scans(await _explain(conn, lit))
    await conn.exec_driver_sql("SET plan_cache_mode = force_generic_plan")
    try:
        await conn.exec_driver_sql(f"PREPARE {name} AS {sql}")
        # no ANALYZE: a prepared EXPLAIN EXECUTE with force_generic_plan shows the generic plan
        args = ", ".join(_fmt(v) for v in vals)
        generic = _scans(
            await _explain(conn, f"EXECUTE {name}({args})" if vals else f"EXECUTE {name}")
        )
    finally:
        await conn.exec_driver_sql("RESET plan_cache_mode")
    return literal_scans, generic


CASES: dict[str, Callable[[], Any]] = {
    "settleable": lambda: settleable_stmt(100),
    "sweep": lambda: tasks_repo._sweep_stmt(3600.0, 10_000),
    "cancel": lambda: tasks_repo._cancel_stmt("live3", "p", "l3"),
    "claim": tasks_repo.claim_select_stmt,
}


async def test_generic_plans_equal_literal_plans(fresh_url: str) -> None:
    engine = create_async_engine(fresh_url)
    try:
        async with engine.connect() as conn:
            await conn.execution_options(isolation_level="AUTOCOMMIT")
            for part in SEED.format(hist=HISTORY, live=LIVE).split(";"):
                await conn.exec_driver_sql(part)
        async with engine.connect() as conn:
            for i, (label, build) in enumerate(CASES.items()):
                literal, generic = await _plans(conn, build(), f"p57_{i}")
                assert literal == generic, f"{label}: literal {literal} != generic {generic}"
    finally:
        await engine.dispose()
