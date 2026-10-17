"""The max-lease scan uses ix_minion_subtask_acquirement_acquired_at (TASK-7.1)."""

import pytest
from minion_server.repositories.reclaim import over_lease_stmt
from sqlalchemy import text
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.integration

INDEX = "ix_minion_subtask_acquirement_acquired_at"
SEED = """
INSERT INTO minion_subtask_acquirement
  (acquirement_id, team, project, task_name, subtask_id, executor_id, attempt, acquired_at)
SELECT gen_random_uuid(), 't', 'p', 'n', g, gen_random_uuid(), 1,
       now() - make_interval(secs => g)
FROM generate_series(1, 20000) AS g
"""
# The production statement, compiled with inline literals so EXPLAIN can run it.
SCAN = "EXPLAIN " + str(
    over_lease_stmt(19990.0, 100).compile(
        dialect=postgresql.dialect(),  # type: ignore[no-untyped-call]
        compile_kwargs={"literal_binds": True},
    )
)


async def test_over_lease_scan_uses_acquired_at_index(fresh_url: str) -> None:
    eng = create_async_engine(fresh_url, isolation_level="AUTOCOMMIT")
    try:
        async with eng.connect() as conn:
            # session_replication_role needs superuser (the testcontainers user is one).
            # Seed rows only; FKs to subtask/executor are irrelevant to the plan.
            await conn.execute(text("SET session_replication_role = replica"))
            await conn.execute(text(SEED))
            await conn.execute(text("RESET session_replication_role"))
            await conn.execute(text("ANALYZE minion_subtask_acquirement"))
            plan = "\n".join(r[0] for r in await conn.execute(text(SCAN)))
    finally:
        await eng.dispose()
    assert INDEX in plan, plan
