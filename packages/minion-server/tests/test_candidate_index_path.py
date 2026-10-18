"""Candidate query: FR-22 order equals the old single-join form, and it stays on index paths
(TASK-7.4.2)."""

import random
import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import pytest
from minion_common.enums import Engine, SubtaskStatus, TaskStatus
from minion_server.db.models import Subtask, Task
from minion_server.repositories.acquisition import candidates_stmt
from sqlalchemy import Select, select, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

pytestmark = pytest.mark.integration

TEAMS = ["a", "b", "c"]
MODELS = ["m1", "m2", "m3"]
ENGINES = ["mlx", "fake", "hf"]
TASK_STATUSES = ["PENDING", "RUNNING", "RUNNING", "SPLITTING", "SUCCEEDED", "CANCELLED"]
SUBTASK_STATUSES = ["PENDING", "PENDING", "PENDING", "RUNNING", "SUCCEEDED"]


def old_candidates_stmt(
    *,
    engines: list[Engine],
    model_ids: list[str],
    memory_gb: float,
    pool: str,
    preference: tuple[str | None, str | None, str | None] | None,
    limit: int,
) -> Select[Any]:
    """The pre-7.4.2 statement, verbatim: the oracle for FR-22 order."""
    stmt = (
        select(
            Subtask.team,
            Subtask.project,
            Subtask.task_name,
            Subtask.subtask_id,
            Task.model_id,
            Subtask.input_object_key,
            Subtask.byte_start,
            Subtask.byte_end,
            Subtask.record_count,
            Task.output_uri,
            Task.params,
        )
        .join(
            Task,
            (Task.team == Subtask.team)
            & (Task.project == Subtask.project)
            & (Task.task_name == Subtask.task_name),
        )
        .where(
            Subtask.status == SubtaskStatus.PENDING.value,
            Task.status.in_([TaskStatus.PENDING.value, TaskStatus.RUNNING.value]),
            Task.model_id.in_(list(model_ids)),
            Task.engine.in_([e.value for e in engines]),
            Task.model_memory_gb <= Decimal(str(memory_gb)),
            (Task.allowed_pool.is_(None)) | (Task.allowed_pool == pool),
        )
        .order_by(
            Task.priority.desc(),
            Task.created_at,
            Subtask.team,
            Subtask.project,
            Subtask.task_name,
            Subtask.subtask_id,
        )
        .limit(limit)
    )
    if preference is not None:
        for col, value in zip(
            (Subtask.team, Subtask.project, Subtask.task_name), preference, strict=True
        ):
            if value is not None:
                stmt = stmt.where(col == value)
    return cast("Select[Any]", stmt)


TASK_SQL = text(
    """INSERT INTO minion_task (team, project, task_name, type, model_id, engine,
       model_memory_gb, input_uri, output_uri, priority, allowed_pool, status, created_by,
       created_at)
       VALUES (:team, :project, :task_name, 'embedding', :model_id, :engine, :mem, 'in', :out,
       :priority, :pool, :status, 't', :created_at)"""
)
SUBTASK_SQL = text(
    """INSERT INTO minion_subtask (team, project, task_name, subtask_id, status,
       input_object_key, byte_start, byte_end, first_record, record_count)
       VALUES (:team, :project, :task_name, :sid, :status, 'k', 0, 1, 0, 1)"""
)


async def _seed(sm: async_sessionmaker[AsyncSession], rng: random.Random) -> list[tuple[str, ...]]:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    keys: list[tuple[str, ...]] = []
    tasks: list[dict[str, Any]] = []
    subtasks: list[dict[str, Any]] = []
    for i in range(rng.randint(4, 14)):
        team, project, name = rng.choice(TEAMS), rng.choice(["p", "q"]), f"n{i}"
        keys.append((team, project, name))
        tasks.append(
            {
                "team": team,
                "project": project,
                "task_name": name,
                "model_id": rng.choice(MODELS),
                "engine": rng.choice(ENGINES),
                "mem": rng.choice([1, 4, 16]),
                "out": f"s3://o/{i}",
                "priority": rng.choice([0, 0, 1, 5]),
                "pool": rng.choice([None, None, "default", "other"]),
                "status": rng.choice(TASK_STATUSES),
                # Few distinct values so created_at ties are common.
                "created_at": base + timedelta(seconds=rng.choice([0, 0, 1, 2])),
            }
        )
        for sid in range(rng.randint(0, 40)):
            subtasks.append(
                {
                    "team": team,
                    "project": project,
                    "task_name": name,
                    "sid": sid,
                    "status": rng.choice(SUBTASK_STATUSES),
                }
            )
    async with sm() as s, s.begin():
        if tasks:
            await s.execute(TASK_SQL, tasks)
        if subtasks:
            await s.execute(SUBTASK_SQL, subtasks)
    return keys


async def test_new_statement_matches_old_order(fresh_url: str) -> None:
    eng = create_async_engine(fresh_url)
    sm = async_sessionmaker(eng, expire_on_commit=False)
    try:
        checked = 0
        for seed in range(60):
            rng = random.Random(seed)  # noqa: S311 - test data
            async with eng.begin() as conn:
                await conn.execute(text("TRUNCATE minion_task CASCADE"))
            keys = await _seed(sm, rng)
            k = keys[rng.randrange(len(keys))]
            prefs: list[tuple[str | None, str | None, str | None] | None] = [
                None,
                (k[0], None, None),
                (k[0], k[1], None),
                (k[0], k[1], k[2]),
                (None, None, k[2]),
                (None, k[1], None),
                ("zzz", None, None),
            ]
            for pref in prefs:
                kwargs: dict[str, Any] = {
                    "engines": rng.sample([Engine.MLX, Engine.FAKE, Engine.HF], 2),
                    "model_ids": rng.sample(MODELS, rng.randint(1, 3)),
                    "memory_gb": rng.choice([0.5, 4.0, 8.0, 32.0]),
                    "pool": rng.choice(["default", "other"]),
                    "preference": pref,
                }
                for limit in (1, 16):
                    async with sm() as s:
                        old = (
                            await s.execute(
                                old_candidates_stmt(
                                    **{**kwargs, "model_ids": list(kwargs["model_ids"])},
                                    limit=limit,
                                )
                            )
                        ).all()
                        new = (await s.execute(candidates_stmt(**kwargs, limit=limit))).all()
                    assert [tuple(r) for r in new] == [tuple(r) for r in old], (seed, pref, limit)
                    checked += len(old)
        assert checked > 200  # the property was exercised on real, non-empty results
    finally:
        await eng.dispose()


SEED_BIG = """
INSERT INTO minion_task (team, project, task_name, type, model_id, engine, model_memory_gb,
  input_uri, output_uri, priority, status, created_by, created_at)
SELECT 'big', 'p', 'n' || g, 'embedding', 'm1', 'fake', 1, 'in', 's3://o/' || g,
  g % 3, 'RUNNING', 't', now() - make_interval(secs => g)
FROM generate_series(1, 12) AS g;
INSERT INTO minion_task (team, project, task_name, type, model_id, engine, model_memory_gb,
  input_uri, output_uri, priority, status, created_by, created_at)
SELECT 'old', 'p', 'n' || g, 'embedding', 'm1', 'fake', 1, 'in', 's3://old/' || g,
  0, 'SUCCEEDED', 't', now() - make_interval(secs => g)
FROM generate_series(1, 20000) AS g;
INSERT INTO minion_subtask (team, project, task_name, subtask_id, status, input_object_key,
  byte_start, byte_end, first_record, record_count)
SELECT 'big', 'p', 'n' || t, s, 'PENDING', 'k', 0, 1, 0, 1
FROM generate_series(1, 12) AS t, generate_series(1, {per_task}) AS s;
ANALYZE minion_task;
ANALYZE minion_subtask;
"""


def _literal_sql(pref: tuple[str | None, str | None, str | None] | None) -> str:
    stmt = candidates_stmt(
        engines=[Engine.FAKE],
        model_ids=["m1"],
        memory_gb=8.0,
        pool="default",
        preference=pref,
    )
    return str(
        stmt.compile(
            dialect=postgresql.dialect(),  # type: ignore[no-untyped-call]
            compile_kwargs={"literal_binds": True},
        )
    )


def _buffers(plan: str) -> int:
    first = re.search(r"Buffers: shared (.*)", plan)
    assert first
    return sum(int(n) for n in re.findall(r"(?:hit|read)=(\d+)", first.group(1)))


async def _plans(url: str, per_task: int) -> dict[str, str]:
    eng = create_async_engine(url, isolation_level="AUTOCOMMIT")
    out: dict[str, str] = {}
    try:
        async with eng.connect() as conn:
            await conn.execute(text("TRUNCATE minion_task CASCADE"))
            for stmt in SEED_BIG.format(per_task=per_task).split(";"):
                if stmt.strip():
                    await conn.execute(text(stmt))
            sql = _literal_sql(None)
            out["analyze"] = "\n".join(
                r[0] for r in await conn.execute(text("EXPLAIN (ANALYZE, BUFFERS) " + sql))
            )
            # Generic plan, as asyncpg's prepared statements use after 5 executions: bind
            # params stay unknown at plan time.
            prep, args = _prepared()
            await conn.execute(text("SET plan_cache_mode = force_generic_plan"))
            await conn.exec_driver_sql(prep)
            out["generic"] = "\n".join(
                r[0]
                for r in await conn.exec_driver_sql(f"EXPLAIN (ANALYZE, BUFFERS) EXECUTE q({args})")
            )
    finally:
        await eng.dispose()
    return out


def _prepared() -> tuple[str, str]:
    stmt = candidates_stmt(
        engines=[Engine.FAKE],
        model_ids=["m1"],
        memory_gb=8.0,
        pool="default",
        preference=None,
    )
    compiled = stmt.compile(
        dialect=postgresql.asyncpg.dialect(),  # type: ignore[no-untyped-call]
        compile_kwargs={"render_postcompile": True},
    )
    assert compiled.positiontup
    values = [compiled.params[name] for name in compiled.positiontup]
    args = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in values)
    return "PREPARE q AS " + str(compiled), args


@pytest.mark.parametrize("kind", ["analyze", "generic"])
async def test_candidate_plan_uses_both_indexes_without_seq_scan_or_sort(
    fresh_url: str, kind: str
) -> None:
    plan = (await _plans(fresh_url, 8_000))[kind]  # 96k pending subtasks
    assert "ix_minion_subtask_pending" in plan, plan
    assert "ix_minion_task_live_queue" in plan, plan
    assert "Seq Scan on minion_subtask" not in plan, plan
    # A Sort node over the subtask scan is the old plan; the bounded outer re-order is fine.
    assert not re.search(r"Sort\n(?:.*\n)*?.*on minion_subtask", plan) or "Incremental" in plan


async def test_candidate_buffers_do_not_grow_with_queue_length(fresh_url: str) -> None:
    small = _buffers((await _plans(fresh_url, 1_700))["analyze"])  # ~20k pending
    big = _buffers((await _plans(fresh_url, 8_400))["analyze"])  # ~100k pending
    assert big <= 2 * small, (small, big)
