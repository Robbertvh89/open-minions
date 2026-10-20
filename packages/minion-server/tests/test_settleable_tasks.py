"""`settleable_tasks` picks live tasks whose counters cover every subtask (ADR-0015, TASK-34)."""

import pytest
from minion_server.repositories.completion import settleable_tasks
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from conftest import seed_task

pytestmark = pytest.mark.integration


async def test_settleable_tasks_semantics(fresh_url: str) -> None:
    engine = create_async_engine(fresh_url)
    sm = async_sessionmaker(engine, expire_on_commit=False)
    try:
        for name in (
            "short",
            "covered",
            "over",
            "nocounter",
            "done",
            "split",
            "canc",
            "run",
            "empty",
        ):
            await seed_task(sm, name, 4)

        async def counters(name: str, per_shard: list[tuple[int, int, int]]) -> None:
            async with sm() as s, s.begin():
                for shard, ok, bad in per_shard:
                    await s.execute(
                        text(
                            "INSERT INTO minion_task_counter"
                            " VALUES ('acq', 'p', :n, :sh, :ok, :bad)"
                        ),
                        {"n": name, "sh": shard, "ok": ok, "bad": bad},
                    )

        await counters("short", [(0, 2, 1)])  # 3 of 4
        await counters("covered", [(0, 1, 1), (5, 2, 0)])  # 4 of 4 across two shards
        await counters("over", [(3, 4, 1)])  # more than total still counts
        await counters("done", [(0, 4, 0)])
        for name in ("split", "canc", "run", "empty"):
            await counters(name, [(0, 4, 0)])
        async with sm() as s, s.begin():
            for name, status in (
                ("done", "SUCCEEDED"),
                ("split", "SPLITTING"),
                ("canc", "CANCELLED"),
            ):
                await s.execute(
                    text("UPDATE minion_task SET status=:st WHERE task_name=:n"),
                    {"st": status, "n": name},
                )
            await s.execute(text("UPDATE minion_task SET status='RUNNING' WHERE task_name='run'"))
            await s.execute(text("UPDATE minion_task SET total_subtasks=0 WHERE task_name='empty'"))

        async def found(limit: int) -> set[str]:
            async with sm() as s:
                return {k.task_name for k in await settleable_tasks(s, limit)}

        assert await found(100) == {
            "covered",
            "over",
            "run",
        }  # only PENDING/RUNNING, covered, total > 0
        assert len(await found(1)) == 1
    finally:
        await engine.dispose()
