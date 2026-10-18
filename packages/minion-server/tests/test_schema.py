"""Baseline schema: 7 tables and the acquisition UNIQUE constraint (CLAUDE.md invariant 2)."""

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

pytestmark = pytest.mark.integration

EXPECTED_TABLES = {
    "minion_task",
    "minion_subtask",
    "minion_subtask_acquirement",
    "minion_executor_heartbeat",
    "minion_executor",
    "minion_executor_preference",
    "minion_subtask_attempt",
    "minion_task_counter",
}


async def test_schema_has_the_eight_tables(engine: AsyncEngine) -> None:
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
                "'s3://out/' || CAST(:task_name AS varchar), 'test')"
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
    winners: list[uuid.UUID] = []
    for ex in executors:
        async with engine.begin() as conn:
            res = await conn.execute(insert, {**key, "aid": uuid.uuid4(), "ex": ex})
            winners.extend(res.scalars().all())
    assert len(winners) == 1


TASK_INSERT = (
    "INSERT INTO minion_task (team, project, task_name, type, model_id, engine, "
    "model_memory_gb, input_uri, output_uri, created_by{extra_cols}) VALUES "
    "('t', 'p', :name, 'embedding', 'm', 'mlx', 1, 's3://in', "
    "'s3://out/' || CAST(:name AS varchar), 'test'{extra_vals})"
)
SUBTASK_INSERT = (
    "INSERT INTO minion_subtask (team, project, task_name, subtask_id, input_object_key, "
    "byte_start, byte_end, first_record, record_count, attempt) VALUES "
    "('t', 'p', :name, 0, 'k', :bs, :be, :fr, :rc, :at)"
)


async def _insert_task(conn: AsyncConnection, override: str = "") -> str:
    """Insert a minimal task; `override` is an optional `column=value` pair."""
    name = f"n-{uuid.uuid4()}"
    col, _, val = override.partition("=")
    sql = TASK_INSERT.format(
        extra_cols=f", {col}" if col else "", extra_vals=f", {val}" if val else ""
    )
    await conn.execute(text(sql), {"name": name})
    return name


@pytest.mark.parametrize(
    ("bs", "be", "fr", "rc", "at", "constraint"),
    [
        (10, 10, 0, 1, 0, "byte_range"),
        (10, 5, 0, 1, 0, "byte_range"),
        (0, 10, 0, 0, 0, "record_count_positive"),
        (0, 10, -1, 1, 0, "first_record_nonneg"),
        (0, 10, 0, 1, -1, "attempt_nonneg"),
    ],
)
async def test_subtask_checks_reject_bad_rows(
    engine: AsyncEngine, bs: int, be: int, fr: int, rc: int, at: int, constraint: str
) -> None:
    with pytest.raises(IntegrityError, match=f"ck_minion_subtask_{constraint}"):
        async with engine.begin() as conn:
            name = await _insert_task(conn)
            await conn.execute(
                text(SUBTASK_INSERT),
                {"name": name, "bs": bs, "be": be, "fr": fr, "rc": rc, "at": at},
            )


@pytest.mark.parametrize(
    ("vals", "constraint"),
    [((-1, 0), "succeeded_nonneg"), ((0, -1), "failed_nonneg")],
)
async def test_task_counter_checks_reject_negative(
    engine: AsyncEngine, vals: tuple[int, int], constraint: str
) -> None:
    with pytest.raises(IntegrityError, match=f"ck_minion_task_counter_{constraint}"):
        async with engine.begin() as conn:
            name = await _insert_task(conn)
            await conn.execute(
                text(
                    "INSERT INTO minion_task_counter (team, project, task_name, shard, succeeded,"
                    " failed) VALUES ('t', 'p', :name, 0, :s, :f)"
                ),
                {"name": name, "s": vals[0], "f": vals[1]},
            )


async def test_task_counter_cascades_with_task(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        name = await _insert_task(conn)
        await conn.execute(
            text(
                "INSERT INTO minion_task_counter (team, project, task_name, shard)"
                " VALUES ('t', 'p', :name, 3)"
            ),
            {"name": name},
        )
        await conn.execute(text("DELETE FROM minion_task WHERE task_name = :name"), {"name": name})
        left = await conn.execute(
            text("SELECT count(*) FROM minion_task_counter WHERE task_name = :name"),
            {"name": name},
        )
        assert left.scalar_one() == 0


@pytest.mark.parametrize(
    ("extra", "constraint"),
    [
        ("records_per_subtask=0", "records_per_subtask_positive"),
        ("max_attempts=0", "max_attempts_positive"),
        ("total_subtasks=-1", "total_subtasks_nonneg"),
    ],
)
async def test_task_checks_reject_bad_rows(
    engine: AsyncEngine, extra: str, constraint: str
) -> None:
    with pytest.raises(IntegrityError, match=f"ck_minion_task_{constraint}"):
        async with engine.begin() as conn:
            await _insert_task(conn, extra)


async def test_executor_rejects_unknown_engine(engine: AsyncEngine) -> None:
    with pytest.raises(IntegrityError, match="ck_minion_executor_engines_known"):
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO minion_executor (executor_id, token_hash, hostname, owner, "
                    "engines) VALUES (:id, :h, 'host', 'owner', ARRAY['mlx','bogus']::varchar[])"
                ),
                {"id": uuid.uuid4(), "h": uuid.uuid4().hex},
            )


async def test_cannot_delete_executor_owning_acquirement(engine: AsyncEngine) -> None:
    ex = uuid.uuid4()
    async with engine.begin() as conn:
        name = await _insert_task(conn)
        await conn.execute(
            text(SUBTASK_INSERT), {"name": name, "bs": 0, "be": 1, "fr": 0, "rc": 1, "at": 0}
        )
        await conn.execute(
            text(
                "INSERT INTO minion_executor (executor_id, token_hash, hostname, owner) "
                "VALUES (:id, :h, 'host', 'owner')"
            ),
            {"id": ex, "h": uuid.uuid4().hex},
        )
        await conn.execute(
            text(
                "INSERT INTO minion_subtask_acquirement (acquirement_id, team, project, "
                "task_name, subtask_id, executor_id, attempt) "
                "VALUES (:aid, 't', 'p', :name, 0, :ex, 1)"
            ),
            {"aid": uuid.uuid4(), "name": name, "ex": ex},
        )
    with pytest.raises(IntegrityError, match="fk_minion_subtask_acquirement_executor_id"):
        async with engine.begin() as conn:
            await conn.execute(
                text("DELETE FROM minion_executor WHERE executor_id = :id"), {"id": ex}
            )


async def test_attempt_acquirement_id_is_unique(engine: AsyncEngine) -> None:
    aid = uuid.uuid4()
    insert = text(
        "INSERT INTO minion_subtask_attempt (team, project, task_name, subtask_id, attempt, "
        "acquirement_id, outcome, started_at) "
        "VALUES ('t', 'p', 'n', 0, 1, :aid, 'FAILED', now())"
    )
    async with engine.begin() as conn:
        await conn.execute(insert, {"aid": aid})
    with pytest.raises(IntegrityError, match="uq_minion_subtask_attempt_acquirement_id"):
        async with engine.begin() as conn:
            await conn.execute(insert, {"aid": aid})
