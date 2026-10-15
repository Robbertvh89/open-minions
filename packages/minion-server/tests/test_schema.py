"""Baseline schema: 7 tables and the acquisition UNIQUE constraint (CLAUDE.md invariant 2)."""

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.integration

EXPECTED_TABLES = {
    "minion_task",
    "minion_subtask",
    "minion_subtask_acquirement",
    "minion_executor_heartbeat",
    "minion_executor",
    "minion_executor_preference",
    "minion_subtask_attempt",
}


async def test_baseline_creates_seven_tables(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        rows = await conn.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name LIKE 'minion_%'"
            )
        )
        assert {r[0] for r in rows} == EXPECTED_TABLES


async def test_only_one_acquirement_insert_wins(engine: AsyncEngine) -> None:
    key = {"team": "t", "project": "p", "task_name": f"n-{uuid.uuid4()}", "subtask_id": 0}
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO minion_task (team, project, task_name, type, model_id, engine, "
                "model_memory_gb, input_uri, output_uri, created_by) VALUES "
                "(:team, :project, :task_name, 'embedding', 'm', 'mlx', 1, 's3://in', "
                "'s3://out', 'test')"
            ),
            key,
        )
        await conn.execute(
            text(
                "INSERT INTO minion_subtask (team, project, task_name, subtask_id, "
                "input_object_key, byte_start, byte_end, first_record, record_count) VALUES "
                "(:team, :project, :task_name, :subtask_id, 'k', 0, 10, 0, 1)"
            ),
            key,
        )
        executors = [uuid.uuid4(), uuid.uuid4()]
        for i, ex in enumerate(executors):
            await conn.execute(
                text(
                    "INSERT INTO minion_executor (executor_id, token_hash, hostname, owner) "
                    "VALUES (:id, :h, 'host', 'owner')"
                ),
                {"id": ex, "h": f"{uuid.uuid4().hex}{i}"},
            )

    insert = text(
        "INSERT INTO minion_subtask_acquirement (acquirement_id, team, project, task_name, "
        "subtask_id, executor_id, attempt) VALUES (:aid, :team, :project, :task_name, "
        ":subtask_id, :ex, 1) "
        "ON CONFLICT ON CONSTRAINT uq_minion_subtask_acquirement_subtask DO NOTHING "
        "RETURNING acquirement_id"
    )
    winners = []
    for ex in executors:
        async with engine.begin() as conn:
            res = await conn.execute(insert, {**key, "aid": uuid.uuid4(), "ex": ex})
            winners.extend(res.scalars().all())
    assert len(winners) == 1
