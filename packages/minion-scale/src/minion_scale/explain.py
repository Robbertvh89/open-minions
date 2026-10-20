"""EXPLAIN (ANALYZE, BUFFERS) of the hot queries (operator tooling, rolled back).

The statements are the repository's own builders (candidate, stale-heartbeat and over-lease
scans), compiled with inline literals so they can be EXPLAINed. Plans contain table and index
names and row estimates, no row values.

Plan kind: literals are inlined, so PostgreSQL plans each statement for those exact values (a
"custom" plan). The server runs the same statements through asyncpg prepared statements, which
switch to a generic plan after five executions; a generic plan can differ (the report says
`plan_kind: custom`). Compare with the `pg_stat_statements` timings of the real run.
"""

import re
from typing import Any

from minion_common.enums import Engine
from minion_server.repositories.acquisition import candidates_stmt
from minion_server.repositories.completion import settleable_stmt
from minion_server.repositories.reclaim import over_lease_stmt, stale_stmt
from sqlalchemy import Select, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from minion_scale.seed import DEFAULT_POOL

Sm = async_sessionmaker[AsyncSession]

Statement = Select[Any] | str

# Grafana panel SQL, copied verbatim from deploy/grafana/dashboards/tasks.json (a unit test fails
# on drift). They run as written; there are no parameters.
GRAFANA_STATEMENTS: dict[str, str] = {
    "grafana_task_status": (
        "SELECT status, count(*) AS tasks FROM minion_ro.task GROUP BY status ORDER BY status"
    ),
    "grafana_live_tasks": (
        "SELECT team, project, task_name, status, succeeded_subtasks, failed_subtasks, "
        "total_subtasks, updated_at\nFROM minion_ro.task\n"
        "WHERE status IN ('SPLITTING', 'PENDING', 'RUNNING')\nORDER BY created_at DESC LIMIT 50"
    ),
    "grafana_subtask_status": (
        "SELECT status, count(*) AS subtasks FROM minion_ro.subtask GROUP BY status ORDER BY status"
    ),
}

EXPLAIN = "EXPLAIN (ANALYZE, BUFFERS) "
STATEMENT_TIMEOUT_MS = 60_000
_PLANNING = re.compile(r"Planning Time: ([0-9.]+) ms")
_EXECUTION = re.compile(r"Execution Time: ([0-9.]+) ms")
_BUFFERS = re.compile(r"Buffers: shared (.*)")


def compile_literal(stmt: Select[Any]) -> str:
    return str(
        stmt.compile(
            dialect=postgresql.dialect(),  # type: ignore[no-untyped-call]
            compile_kwargs={"literal_binds": True},
        )
    )


def hot_statements(
    *,
    model_ids: list[str],
    team: str,
    heartbeat_timeout_s: float,
    max_lease_s: float,
    memory_gb: float = 8.0,
    pool: str = DEFAULT_POOL,
    limit: int = 500,
    settle_batch: int = 100,
) -> dict[str, Statement]:
    common: dict[str, Any] = {
        "engines": [Engine.FAKE],
        "model_ids": model_ids,
        "memory_gb": memory_gb,
        "pool": pool,
    }
    return {
        "candidate": candidates_stmt(preference=None, **common),
        "candidate_preference": candidates_stmt(preference=(team, None, None), **common),
        "stale_heartbeat": stale_stmt(heartbeat_timeout_s, limit),
        "over_lease": over_lease_stmt(max_lease_s, limit),
        "settleable_tasks": settleable_stmt(settle_batch),
        **GRAFANA_STATEMENTS,
    }


def parse_plan(lines: list[str]) -> dict[str, Any]:
    joined = "\n".join(lines)

    def num(rx: re.Pattern[str]) -> float | None:
        m = rx.search(joined)
        return float(m.group(1)) if m else None

    buffers = _BUFFERS.search(joined)
    return {
        "planning_ms": num(_PLANNING),
        "execution_ms": num(_EXECUTION),
        "top_buffers": buffers.group(1).strip() if buffers else None,
        "seq_scan": "Seq Scan" in joined,
        "plan": lines,
    }


async def explain_all(sm: Sm, statements: dict[str, Statement]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for name, stmt in statements.items():
        sql = stmt if isinstance(stmt, str) else compile_literal(stmt)
        async with sm() as session, session.begin():
            await session.execute(text(f"SET LOCAL statement_timeout = {STATEMENT_TIMEOUT_MS}"))
            await session.execute(text("SET TRANSACTION READ ONLY"))
            rows = await session.execute(text(EXPLAIN + sql.replace(":", r"\:")))
            lines = [r[0] for r in rows.all()]
        out.append(
            {
                "name": name,
                "plan_kind": "custom",
                "sql": " ".join(sql.split()),
                **parse_plan(lines),
            }
        )
    return out
