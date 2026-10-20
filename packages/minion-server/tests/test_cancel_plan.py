"""TASK-53: cancel / sweep of a freshly split large task must not depend on planner statistics.

A new database (no ANALYZE, autovacuum off for the table) with a 25k-subtask task and other
tasks around it. `cancel_pending_subtasks` once took ~60 s here (CPU-bound, bad plan from the
default selectivity estimates); the shape now pins the plan: a MATERIALIZED locking CTE joined
to the UPDATE on the primary key.
"""

import re
import time

import pytest
from minion_server.repositories import tasks as tasks_repo
from minion_server.services.reclaim import sweep_cancelled
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from conftest import seed_task

pytestmark = pytest.mark.integration

N = 25_000
LIMIT_S = 2.0


async def test_cancel_fresh_large_task_without_analyze(fresh_url: str) -> None:
    eng = create_async_engine(
        fresh_url, connect_args={"server_settings": {"statement_timeout": "10s"}}
    )
    sm = async_sessionmaker(eng, expire_on_commit=False)
    try:
        async with sm() as s, s.begin():
            await s.execute(text("ALTER TABLE minion_subtask SET (autovacuum_enabled = false)"))
        await seed_task(sm, "big", N, team="acq")
        for i in range(3):  # other data around it, also never analyzed
            await seed_task(sm, f"other{i}", 3000, team=f"t{i}")
        async with sm() as s:
            stats = (
                await s.execute(
                    text(
                        "SELECT last_analyze, last_autoanalyze FROM pg_stat_user_tables "
                        "WHERE relname='minion_subtask'"
                    )
                )
            ).one()
            assert stats[0] is None and stats[1] is None

        async with sm() as s:  # plan recorded in TASK-53 notes; the CTE must run once
            stmt = tasks_repo._cancel_stmt("acq", "p", "big").compile(
                dialect=eng.dialect, compile_kwargs={"literal_binds": True}
            )
            plan = await _explain(s, str(stmt))
            print(plan)
            await s.rollback()
        assert "Semi Join" not in plan
        assert re.search(r"CTE Scan on c .*loops=1\)", plan), plan

        t0 = time.monotonic()
        total = 0
        while True:
            async with sm() as s, s.begin():
                n = await tasks_repo.cancel_pending_subtasks(s, "acq", "p", "big")
            total += n
            if n == 0:
                break
        took = time.monotonic() - t0
        assert total == N
        assert took < LIMIT_S, f"cancel took {took:.2f}s"

        # sweep: same shape, a CANCELLED task with a PENDING straggler
        async with sm() as s, s.begin():
            await s.execute(
                text(
                    "UPDATE minion_task SET status='CANCELLED', finished_at=now() "
                    "WHERE task_name='other0'"
                )
            )
        t0 = time.monotonic()
        # smoke check only (3k rows); the shape is pinned by the plan assertion above
        swept = await sweep_cancelled(sm)
        assert swept == 3000
        assert time.monotonic() - t0 < LIMIT_S
    finally:
        await eng.dispose()


async def _explain(s: AsyncSession, sql: str) -> str:
    rows = (await s.execute(text("EXPLAIN (ANALYZE, BUFFERS) " + sql))).all()
    return "\n".join(r[0] for r in rows)
