"""Candidate query: FR-22 order equals the old single-join form, and it stays on index paths
(TASK-7.4.2)."""

import json
import random
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import pytest
from minion_common.enums import Engine, SubtaskStatus, TaskStatus
from minion_server.db.models import Subtask, Task
from minion_server.repositories.acquisition import candidates_stmt, claim
from sqlalchemy import Select, event, func, or_, select, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from conftest import make_executors

pytestmark = pytest.mark.integration

TEAMS = ["a", "b", "c"]
NAMES = ["x", "y"]
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
    seen: set[tuple[str, ...]] = set()
    for i in range(rng.randint(4, 14)):
        team, project, name = rng.choice(TEAMS), rng.choice(["p", "q"]), rng.choice(NAMES)
        if (team, project, name) in seen:
            continue
        seen.add((team, project, name))
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
                "priority": rng.choice([0, 0, 1, 5, -1000, -1, 1000]),
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
                for limit in (1, 16, 256):
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


def _prepared(pref: tuple[str | None, str | None, str | None] | None) -> tuple[str, str]:
    stmt = candidates_stmt(
        engines=[Engine.FAKE],
        model_ids=["m1"],
        memory_gb=8.0,
        pool="default",
        preference=pref,
    )
    compiled = stmt.compile(
        dialect=postgresql.asyncpg.dialect(),  # type: ignore[no-untyped-call]
        compile_kwargs={"render_postcompile": True},
    )
    assert compiled.positiontup
    values = [compiled.params[name] for name in compiled.positiontup]
    args = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in values)
    return "PREPARE q AS " + str(compiled), args


def _json_plan(rows: Any) -> dict[str, Any]:
    raw = rows.all()[0][0]
    doc = json.loads(raw) if isinstance(raw, str) else raw
    return cast("dict[str, Any]", doc[0])


def _nodes(node: dict[str, Any]) -> list[dict[str, Any]]:
    out = [node]
    for child in node.get("Plans", []):
        out.extend(_nodes(child))
    return out


def _buffers(plan: dict[str, Any]) -> int:
    root = plan["Plan"]
    return int(root.get("Shared Hit Blocks", 0)) + int(root.get("Shared Read Blocks", 0))


SORT_INPUT_BOUND = 2_000  # a few chunks; a scan of 10k live tasks per step is 40k


def assert_index_paths(
    plan: dict[str, Any],
    *,
    pending_index: bool = True,
    allow_bitmap: bool = False,
    sort_input: int = SORT_INPUT_BOUND,
) -> None:
    """The recursive ordered walk (TASK-54): a Recursive Union whose steps are Index Scans on the
    live-queue index with a row-compare Index Cond; the pending probes use
    ix_minion_subtask_pending; no Seq Scan on either table; no Sort ever sees more rows than the
    window (a Sort over all live tasks or all pending subtasks is the failure this guards): the
    rows a Sort READS (children's rows x loops) and returns are both bounded. A bitmap scan of the
    live tasks (the planner's pick for a few hundred tasks) is allowed only with `allow_bitmap`."""
    nodes = _nodes(plan["Plan"])
    text_plan = json.dumps(plan)
    if pending_index:
        assert "ix_minion_subtask_pending" in text_plan, text_plan
    else:  # large live queues: per-task probes may use ix_minion_subtask_progress + a tiny Sort
        assert any(
            n.get("Relation Name") == "minion_subtask" and "Index" in n["Node Type"] for n in nodes
        ), text_plan
    assert any(n["Node Type"] == "Recursive Union" for n in nodes), text_plan
    steps = [n for n in nodes if n.get("Index Name") == "ix_minion_task_live_queue"]
    assert steps, text_plan
    assert all("ROW(" in n.get("Index Cond", "") for n in steps), text_plan
    for n in nodes:
        if n["Node Type"] == "Seq Scan":
            assert n.get("Relation Name") not in ("minion_subtask", "minion_task"), text_plan
        if n["Node Type"] == "Sort":
            rows = int(n.get("Actual Rows", 0)) * int(n.get("Actual Loops", 1))
            read = sum(
                int(c.get("Actual Rows", 0)) * int(c.get("Actual Loops", 1))
                for c in n.get("Plans", [])
            )
            assert rows <= SORT_ROW_BOUND, (rows, text_plan)
            assert read <= sort_input, (read, text_plan)
        if not allow_bitmap:
            assert (
                n["Node Type"] != "Bitmap Heap Scan" or n.get("Relation Name") != "minion_task"
            ), text_plan


SORT_ROW_BOUND = 16 * 100  # window x pending rows per task in the seeds (loose; scans of 10k+ fail)


async def _seed_big(conn: Any, per_task: int, extra_empty_tasks: int = 0) -> None:
    await conn.execute(text("TRUNCATE minion_task CASCADE"))
    for stmt in SEED_BIG.format(per_task=per_task).split(";"):
        if stmt.strip():
            await conn.execute(text(stmt))
    if extra_empty_tasks:
        # Live, eligible tasks ahead of the pending ones in queue order, with no subtasks.
        await conn.execute(
            text(
                """INSERT INTO minion_task (team, project, task_name, type, model_id, engine,
                   model_memory_gb, input_uri, output_uri, priority, status, created_by,
                   created_at)
                   SELECT 'empty', 'p', 'e' || g, 'embedding', 'm1', 'fake', 1, 'in',
                   's3://e/' || g, 10, 'RUNNING', 't', now() - make_interval(secs => g)
                   FROM generate_series(1, :n) AS g"""
            ),
            {"n": extra_empty_tasks},
        )
        await conn.execute(text("ANALYZE minion_task"))


async def _plans(
    url: str,
    per_task: int,
    pref: tuple[str | None, str | None, str | None] | None = None,
    extra_empty_tasks: int = 0,
) -> dict[str, dict[str, Any]]:
    eng = create_async_engine(url, isolation_level="AUTOCOMMIT")
    out: dict[str, dict[str, Any]] = {}
    try:
        async with eng.connect() as conn:
            await _seed_big(conn, per_task, extra_empty_tasks)
            out["analyze"] = _json_plan(
                await conn.execute(
                    text("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + _literal_sql(pref))
                )
            )
            # Generic plan, as asyncpg's prepared statements use after 5 executions: bind
            # params stay unknown at plan time.
            prep, args = _prepared(pref)
            await conn.execute(text("SET plan_cache_mode = force_generic_plan"))
            await conn.exec_driver_sql(prep)
            out["generic"] = _json_plan(
                await conn.exec_driver_sql(
                    f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE q({args})"
                )
            )
    finally:
        await eng.dispose()
    return out


PREFS: list[tuple[str | None, str | None, str | None] | None] = [None, ("big", None, None)]


@pytest.mark.parametrize("pref", PREFS)
@pytest.mark.parametrize("kind", ["analyze", "generic"])
async def test_candidate_plan_uses_both_indexes_without_seq_scan_or_sort(
    fresh_url: str, kind: str, pref: tuple[str | None, str | None, str | None] | None
) -> None:
    plan = (await _plans(fresh_url, 8_000, pref))[kind]  # 96k pending subtasks
    assert_index_paths(plan)


async def test_candidate_buffers_do_not_grow_with_queue_length(fresh_url: str) -> None:
    small = _buffers((await _plans(fresh_url, 1_700))["analyze"])  # ~20k pending
    big = _buffers((await _plans(fresh_url, 8_400))["analyze"])  # ~100k pending
    assert big <= 2 * small, (small, big)


async def test_candidate_buffers_bounded_with_many_empty_live_tasks(fresh_url: str) -> None:
    # ~3 buffers per eligible live task without pending subtasks ahead of the first match.
    plan = (await _plans(fresh_url, 100, extra_empty_tasks=2_000))["analyze"]
    assert_index_paths(plan)
    assert _buffers(plan) < BUFFER_BOUND, _buffers(plan)


BUFFER_BOUND = 15_000


# --- TASK-30 AC#2: 5,000 live tasks no executor here can run ------------------------------------

UNRUNNABLE = 5_000
UNRUNNABLE_SEED = """
INSERT INTO minion_task (team, project, task_name, type, model_id, engine, model_memory_gb,
  input_uri, output_uri, allowed_pool, priority, status, created_by, created_at)
SELECT 'junk' || (g % 10), 'p', 'u' || g, 'embedding',
  CASE WHEN g % 4 = 3 THEN 'm-removed' ELSE 'm1' END,
  CASE WHEN g % 4 = 1 THEN 'ollama' ELSE 'fake' END,
  CASE WHEN g % 4 = 0 THEN 512 ELSE 1 END,
  'in', 's3://u/' || g,
  CASE WHEN g % 4 = 2 THEN 'nowhere' ELSE NULL END,
  1000, 'RUNNING', 't', now() - interval '1 day' - make_interval(secs => g)
FROM generate_series(1, {n}) AS g;
INSERT INTO minion_subtask (team, project, task_name, subtask_id, status, input_object_key,
  byte_start, byte_end, first_record, record_count)
SELECT 'junk' || (t % 10), 'p', 'u' || t, s, 'PENDING', 'k', 0, 1, 0, 1
FROM generate_series(1, {n}) AS t, generate_series(1, 3) AS s;
ANALYZE minion_task;
ANALYZE minion_subtask;
"""
# Measured: ~161 buffers, 3 subtask probes, <1 ms (both plan kinds; the recursive walk scans the
# ~5,012 live tasks in index order once, junk filtered in the heap, 12 runnable ones visited).
# Bound = ~1.5x measured, so a regression to one subtask probe per junk task (>= 5,000 x 3
# buffers) or a Seq Scan over the subtasks fails loudly.
UNRUNNABLE_BUFFER_BOUND = 230
UNRUNNABLE_SUBTASK_PROBES = 12  # one per eligible (runnable) task at most, never per junk task


async def _unrunnable_plans(url: str) -> dict[str, dict[str, Any]]:
    eng = create_async_engine(url, isolation_level="AUTOCOMMIT")
    out: dict[str, dict[str, Any]] = {}
    try:
        async with eng.connect() as conn:
            await _seed_big(conn, 100)
            for stmt in UNRUNNABLE_SEED.format(n=UNRUNNABLE).split(";"):
                if stmt.strip():
                    await conn.execute(text(stmt))
            sql = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + _literal_sql(None)
            runs = [_json_plan(await conn.execute(text(sql))) for _ in range(5)]
            runs.sort(key=lambda p: p["Execution Time"])
            out["analyze"] = runs[2]  # median of 5
            prep, args = _prepared(None)
            await conn.execute(text("SET plan_cache_mode = force_generic_plan"))
            await conn.exec_driver_sql(prep)
            out["generic"] = _json_plan(
                await conn.exec_driver_sql(
                    f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE q({args})"
                )
            )
    finally:
        await eng.dispose()
    return out


def _subtask_probes(plan: dict[str, Any]) -> int:
    return sum(
        int(n.get("Actual Loops", 1))
        for n in _nodes(plan["Plan"])
        if n.get("Relation Name") == "minion_subtask"
        or n.get("Index Name") == ("ix_minion_subtask_pending")
    )


@pytest.mark.parametrize("kind", ["analyze", "generic"])
async def test_candidate_bounded_with_5k_unrunnable_live_tasks(fresh_url: str, kind: str) -> None:
    plan = (await _unrunnable_plans(fresh_url))[kind]
    # The 5k junk tasks sort first but are filtered out in the walk's index scan.
    assert_index_paths(plan)
    probes = _subtask_probes(plan)
    buffers = _buffers(plan)
    print(f"TASK-30 {kind}: probes={probes} buffers={buffers} ms={plan['Execution Time']:.1f}")
    assert probes <= UNRUNNABLE_SUBTASK_PROBES, probes  # never one probe per unrunnable task
    assert buffers <= UNRUNNABLE_BUFFER_BOUND, buffers
    assert plan["Execution Time"] < 50, plan["Execution Time"]  # loose; NF-1 budget is 200 ms


async def test_candidate_rows_equal_oracle_and_claim_wins_with_5k_unrunnable(
    fresh_url: str,
) -> None:
    from minion_server.repositories.acquisition import claim

    from conftest import make_executors

    eng = create_async_engine(fresh_url)
    sm = async_sessionmaker(eng, expire_on_commit=False)
    try:
        async with eng.connect() as raw:
            raw = await raw.execution_options(isolation_level="AUTOCOMMIT")
            await _seed_big(raw, 100)
            for stmt in UNRUNNABLE_SEED.format(n=UNRUNNABLE).split(";"):
                if stmt.strip():
                    await raw.execute(text(stmt))
        kwargs: dict[str, Any] = {
            "engines": [Engine.FAKE],
            "model_ids": ["m1"],
            "memory_gb": 8.0,
            "pool": "default",
            "preference": None,
        }
        async with sm() as s:
            old = (await s.execute(old_candidates_stmt(**kwargs, limit=16))).all()
            new = (await s.execute(candidates_stmt(**kwargs, limit=16))).all()
        assert [tuple(r) for r in new] == [tuple(r) for r in old]
        assert len(new) == 16 and {r[0] for r in new} == {"big"}  # none of the junk
        ((principal, _),) = await make_executors(sm, 1)
        async with sm() as s:
            res = await claim(s, principal.executor_id, **kwargs, window=16)
            await s.rollback()
        assert res.won is not None
    finally:
        await eng.dispose()


# --- TASK-30 guard: the live-cap index must not displace the ordered live-queue walk -----------

GUARD_SEED = """
INSERT INTO minion_task (team, project, task_name, type, model_id, engine, model_memory_gb,
  input_uri, output_uri, priority, status, created_by, created_at)
SELECT 'g' || (g % 4), 'p', 'g' || g, 'embedding', 'm1', 'fake', 1, 'in', 's3://g/' || g,
  0, CASE WHEN g % 2 = 0 THEN 'RUNNING' ELSE 'PENDING' END, 't',
  now() - make_interval(secs => g)
FROM generate_series(1, 200) AS g;
INSERT INTO minion_subtask (team, project, task_name, subtask_id, status, input_object_key,
  byte_start, byte_end, first_record, record_count)
SELECT 'g' || (t % 4), 'p', 'g' || t, s, 'PENDING', 'k', 0, 1, 0, 1
FROM generate_series(1, 200) AS t, generate_series(1, 500) AS s;
ANALYZE minion_task;
ANALYZE minion_subtask;
"""
GUARD_BUFFER_BOUND = 30


@pytest.mark.parametrize("kind", ["analyze", "generic"])
async def test_cap_index_does_not_displace_live_queue_walk(fresh_url: str, kind: str) -> None:
    eng = create_async_engine(fresh_url, isolation_level="AUTOCOMMIT")
    try:
        async with eng.connect() as conn:
            await _seed_big(conn, 100)
            await conn.execute(text("DELETE FROM minion_task WHERE team = 'big'"))
            for stmt in GUARD_SEED.split(";"):
                if stmt.strip():
                    await conn.execute(text(stmt))
            idx = (
                await conn.execute(
                    text("SELECT 1 FROM pg_indexes WHERE indexname = 'ix_minion_task_team_status'")
                )
            ).all()
            assert idx, "the cap index must exist for this guard to mean anything"
            if kind == "analyze":
                plan = _json_plan(
                    await conn.execute(
                        text("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + _literal_sql(None))
                    )
                )
            else:
                prep, args = _prepared(None)
                await conn.execute(text("SET plan_cache_mode = force_generic_plan"))
                await conn.exec_driver_sql(prep)
                plan = _json_plan(
                    await conn.exec_driver_sql(
                        f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE q({args})"
                    )
                )
    finally:
        await eng.dispose()
    text_plan = json.dumps(plan)
    print(f"TASK-30 guard {kind}: buffers={_buffers(plan)} ms={plan['Execution Time']:.2f}")
    assert "ix_minion_task_live_queue" in text_plan, text_plan
    assert "ix_minion_task_team_status" not in text_plan, text_plan
    assert_index_paths(plan)
    assert _buffers(plan) <= GUARD_BUFFER_BOUND, _buffers(plan)


# --- TASK-54: the recursive walk from 12 to 10k live tasks, literal and generic plans ---------
# Buffers and probes are the assertions; timings are only a loose sanity check. TASK-29: the
# `ix_minion_task_queue` index is kept for the splitter claim and no candidate plan names it.

NOCACHE = {"prepared_statement_cache_size": 0}  # we PREPARE by hand
WALK_SEED = """
INSERT INTO minion_task (team, project, task_name, type, model_id, engine, model_memory_gb,
  input_uri, output_uri, priority, status, created_by, created_at)
SELECT 'old' || (g % 20), 'p', 'n' || g, 'embedding', 'm1', 'fake', 1, 'in', 's3://old/' || g,
  0, 'SUCCEEDED', 't', now() - interval '30 days' - make_interval(secs => g)
FROM generate_series(1, {hist}) AS g;
INSERT INTO minion_task (team, project, task_name, type, model_id, engine, model_memory_gb,
  input_uri, output_uri, priority, status, created_by, created_at)
SELECT 'live' || (g % 20), 'p', 'l' || g, 'embedding', {model}, 'fake', 1, 'in', 's3://l/' || g,
  {prio}, CASE WHEN g % 10 = 0 THEN 'PENDING' ELSE 'RUNNING' END, 't',
  now() - make_interval(secs => g)
FROM generate_series(1, {n}) AS g;
INSERT INTO minion_subtask (team, project, task_name, subtask_id, status, input_object_key,
  byte_start, byte_end, first_record, record_count)
SELECT 'live' || (t % 20), 'p', 'l' || t, s, 'PENDING', 'k', 0, 1, 0, 1
FROM generate_series(1, {n}) AS t, generate_series(1, {per}) AS s WHERE {pending_where}
"""
ANALYZE = ("ANALYZE minion_task", "ANALYZE minion_subtask")

ALL_MODELS = ["m1"]
TEN_MODEL_SERVED = ["m1", "m2", "m3"]  # tasks spread over m0..m9, this executor serves three

WALK_BUFFER_BOUND = 150  # measured 20-98
PROBE_BOUND = 15  # measured 12
Pref = tuple[str | None, str | None, str | None] | None


async def seed(
    conn: AsyncConnection,
    *,
    n: int,
    hist: int = 20_000,
    per: int = 5,
    prio: str = "0",
    model: str = "'m1'",
    pending_where: str = "true",
    analyze: bool = True,
) -> None:
    await conn.execute(text("TRUNCATE minion_task CASCADE"))
    sql = WALK_SEED.format(
        hist=hist, n=n, per=per, prio=prio, model=model, pending_where=pending_where
    )
    for stmt in sql.split(";"):
        if stmt.strip():
            await conn.execute(text(stmt))
    if analyze:
        for stmt in ANALYZE:
            await conn.execute(text(stmt))


def _kwargs(pref: Pref, models: list[str], limit: int) -> dict[str, Any]:
    return {
        "engines": [Engine.FAKE],
        "model_ids": models,
        "memory_gb": 8.0,
        "pool": "default",
        "preference": pref,
        "limit": limit,
    }


async def explain_both(conn: AsyncConnection, stmt: Select[Any]) -> dict[str, dict[str, Any]]:
    """EXPLAIN (ANALYZE, BUFFERS) of `stmt` as a literal (custom) plan and as a generic plan."""
    literal = str(
        stmt.compile(
            dialect=postgresql.dialect(),  # type: ignore[no-untyped-call]
            compile_kwargs={"literal_binds": True},
        )
    )
    out = {
        "literal": _json_plan(
            await conn.exec_driver_sql("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + literal)
        )
    }
    compiled = stmt.compile(
        dialect=postgresql.asyncpg.dialect(),  # type: ignore[no-untyped-call]
        compile_kwargs={"render_postcompile": True},
    )
    assert compiled.positiontup is not None
    values = [compiled.params[name] for name in compiled.positiontup]
    args = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in values)
    await conn.exec_driver_sql("SET plan_cache_mode = force_generic_plan")
    await conn.exec_driver_sql("DEALLOCATE ALL")
    await conn.exec_driver_sql("PREPARE q AS " + str(compiled))
    out["generic"] = _json_plan(
        await conn.exec_driver_sql(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE q({args})")
    )
    await conn.exec_driver_sql("RESET plan_cache_mode")
    return out


def outline(node: dict[str, Any], depth: int = 0) -> str:
    """Indented node list of a plan: readable assertion messages."""
    line = (
        "  " * depth
        + f"{node['Node Type']} {node.get('Relation Name', '')} {node.get('Index Name', '')}"
        + f" rows={node.get('Actual Rows')}x{node.get('Actual Loops')}\n"
    )
    return line + "".join(outline(c, depth + 1) for c in node.get("Plans", []))


# check_walk labels whose plan really has a Bitmap Heap Scan on minion_task (seen when the 10k/200
# grid was run; every other label must be a pure index walk).
BITMAP_OBSERVED = {"ten_models n=200 generic"}


def check_walk(
    label: str, plan: dict[str, Any], n: int, *, buffers: int = WALK_BUFFER_BOUND
) -> None:
    text_plan = json.dumps(plan)
    assert "ix_minion_task_queue" not in text_plan, (label, outline(plan["Plan"]))  # TASK-29
    # At 12 live tasks a generic plan may use a bitmap per step on the tiny set (<= 34 buffers
    # measured): the shape is asserted from 200 up, the bounds always.
    if n >= 200:
        # The ordered walk everywhere, except the one case where a per-step bitmap + top-N sort of
        # 200 tasks was observed (generic plan, ten models; bounded by n): BITMAP_OBSERVED.
        assert_index_paths(
            plan,
            pending_index=False,
            allow_bitmap=label in BITMAP_OBSERVED,
            sort_input=max(2_000, 6 * n),
        )
    got = _buffers(plan)
    probes = _subtask_probes(plan)
    print(f"{label}: buffers={got} probes={probes} ms={plan['Execution Time']:.2f}")
    assert got <= buffers, (label, got)
    assert probes <= PROBE_BOUND, (label, probes)
    assert plan["Execution Time"] < 100, (label, plan["Execution Time"])


async def assert_rows_equal_oracle(url: str, **kw: Any) -> None:
    eng = create_async_engine(url)
    try:
        async with eng.connect() as conn:
            old_kw = {k: v for k, v in kw.items() if k != "model_ids"}
            old = (
                await conn.execute(old_candidates_stmt(model_ids=list(kw["model_ids"]), **old_kw))
            ).all()
            new = (await conn.execute(candidates_stmt(**kw))).all()
        assert [tuple(r) for r in new] == [tuple(r) for r in old]
        assert old, "the case must return rows"
    finally:
        await eng.dispose()


SCENARIOS = {
    "mixed": {"prio": "g % 3", "model": "'m1'"},
    "same": {"prio": "0", "model": "'m1'"},
    "ten_models": {"prio": "g % 3", "model": "'m' || (g % 10)"},
}


@pytest.mark.parametrize("n", [12, 200, 1_000, 2_000, 5_000, 10_000])
@pytest.mark.parametrize("scenario", list(SCENARIOS))
async def test_walk_plan_shape_and_bounds(fresh_url: str, n: int, scenario: str) -> None:
    models = TEN_MODEL_SERVED if scenario == "ten_models" else ALL_MODELS
    eng = create_async_engine(fresh_url, isolation_level="AUTOCOMMIT", connect_args=NOCACHE)
    try:
        async with eng.connect() as conn:
            await seed(
                conn,
                n=n,
                prio=SCENARIOS[scenario]["prio"],
                model=SCENARIOS[scenario]["model"],
            )
            kw = _kwargs(None, models, 16)
            plans = await explain_both(conn, candidates_stmt(**kw))
    finally:
        await eng.dispose()
    for kind, plan in plans.items():
        check_walk(f"{scenario} n={n} {kind}", plan, n)
    await assert_rows_equal_oracle(fresh_url, **kw)


async def test_walk_at_10k_with_40k_history(fresh_url: str) -> None:
    eng = create_async_engine(fresh_url, isolation_level="AUTOCOMMIT", connect_args=NOCACHE)
    try:
        async with eng.connect() as conn:
            await seed(conn, n=10_000, hist=40_000, prio="g % 3")
            plans = await explain_both(conn, candidates_stmt(**_kwargs(None, ALL_MODELS, 16)))
    finally:
        await eng.dispose()
    for kind, plan in plans.items():
        check_walk(f"h40000 n=10000 {kind}", plan, 10_000)


async def test_walk_all_drained_costs_one_probe_per_live_task(fresh_url: str) -> None:
    n = 10_000
    eng = create_async_engine(fresh_url, isolation_level="AUTOCOMMIT", connect_args=NOCACHE)
    try:
        async with eng.connect() as conn:
            await seed(conn, n=n, pending_where="false")
            plans = await explain_both(conn, candidates_stmt(**_kwargs(None, ALL_MODELS, 16)))
            rows = (await conn.execute(text("SELECT 1"))).all()
            assert rows
    finally:
        await eng.dispose()
    for kind, plan in plans.items():
        probes, got = _subtask_probes(plan), _buffers(plan)
        print(f"drained {kind}: buffers={got} probes={probes} ms={plan['Execution Time']:.1f}")
        assert n <= probes <= n + 20, probes  # inherent: one pending check per live task
        assert got <= n // 2, got  # measured 4018: probe + chunk step overhead, main 274-654
        assert any(x["Node Type"] == "Recursive Union" for x in _nodes(plan["Plan"]))
        assert "ix_minion_task_queue" not in json.dumps(plan)


async def test_walk_with_team_preference(fresh_url: str) -> None:
    eng = create_async_engine(fresh_url, isolation_level="AUTOCOMMIT", connect_args=NOCACHE)
    try:
        async with eng.connect() as conn:
            await seed(conn, n=10_000, prio="g % 3")
            kw = _kwargs(("live3", None, None), ALL_MODELS, 16)
            plans = await explain_both(conn, candidates_stmt(**kw))
    finally:
        await eng.dispose()
    for kind, plan in plans.items():
        check_walk(f"preference n=10000 {kind}", plan, 10_000)
    await assert_rows_equal_oracle(fresh_url, **kw)


async def test_walk_window_256_with_one_pending_per_task(fresh_url: str) -> None:
    eng = create_async_engine(fresh_url, isolation_level="AUTOCOMMIT", connect_args=NOCACHE)
    try:
        async with eng.connect() as conn:
            await seed(conn, n=10_000, per=1)
            kw = _kwargs(None, ALL_MODELS, 256)
            plans = await explain_both(conn, candidates_stmt(**kw))
    finally:
        await eng.dispose()
    for kind, plan in plans.items():
        probes, got = _subtask_probes(plan), _buffers(plan)
        print(f"window256 {kind}: buffers={got} probes={probes} ms={plan['Execution Time']:.1f}")
        assert probes <= 3 * 256, probes  # measured 768: three probes per contributing task
        assert got <= 3_500, got
        assert_index_paths(plan, pending_index=False)
    await assert_rows_equal_oracle(fresh_url, **kw)


@pytest.mark.parametrize("live_at_analyze", [0, 1])
async def test_walk_survives_stale_statistics(fresh_url: str, live_at_analyze: int) -> None:
    """Statistics describe an (almost) empty queue, then 10k live tasks arrive without ANALYZE
    (autovacuum off): the plan must stay the walk."""
    eng = create_async_engine(fresh_url, isolation_level="AUTOCOMMIT", connect_args=NOCACHE)
    try:
        async with eng.connect() as conn:
            for table in ("minion_task", "minion_subtask"):
                await conn.execute(text(f"ALTER TABLE {table} SET (autovacuum_enabled = false)"))
            await seed(conn, n=live_at_analyze)  # 20k history, also ANALYZEs
            await seed_without_analyze(conn)
            kw = _kwargs(None, ALL_MODELS, 16)
            plans = await explain_both(conn, candidates_stmt(**kw))
    finally:
        await eng.dispose()
    for kind, plan in plans.items():
        check_walk(f"stale({live_at_analyze}) n=10000 {kind}", plan, 10_000, buffers=250)


async def test_walk_never_analyzed_tables_stay_bounded(fresh_url: str) -> None:
    """Statistics of EMPTY tables (fresh restore / pg_upgrade / first start), then 20k finished +
    10k live tasks arrive without ANALYZE. The planner may bitmap-scan the live tasks per step
    here, but the pending check runs on the 8-task chunk only: before the chunk, this case probed
    39,994 times (281k buffers, 126 ms); now it stays within a few thousand buffers."""
    eng = create_async_engine(fresh_url, isolation_level="AUTOCOMMIT", connect_args=NOCACHE)
    try:
        async with eng.connect() as conn:
            for table in ("minion_task", "minion_subtask"):
                await conn.execute(text(f"ALTER TABLE {table} SET (autovacuum_enabled = false)"))
            for stmt in ANALYZE:
                await conn.execute(text(stmt))  # on empty tables
            await seed(conn, n=10_000, prio="g % 3", analyze=False)
            kw = _kwargs(None, ALL_MODELS, 16)
            plans = await explain_both(conn, candidates_stmt(**kw))
    finally:
        await eng.dispose()
    for kind, plan in plans.items():
        got, probes = _buffers(plan), _subtask_probes(plan)
        print(
            f"never analyzed {kind}: buffers={got} probes={probes} ms={plan['Execution Time']:.1f}"
        )
        assert any(x["Node Type"] == "Recursive Union" for x in _nodes(plan["Plan"]))
        assert probes <= 200, probes  # was 39,994+
        assert got <= 3_000, got  # measured ~1,250; was 281,000
        assert_index_paths(plan, pending_index=False, allow_bitmap=True, sort_input=50_000)
    await assert_rows_equal_oracle(fresh_url, **kw)


async def seed_without_analyze(conn: AsyncConnection) -> None:
    """10k more live tasks (own keys 'z..') with 5 pending subtasks each; no ANALYZE."""
    sql = WALK_SEED.format(
        hist=0, n=10_000, per=5, prio="g % 3", model="'m1'", pending_where="true"
    )
    sql = sql.replace("'l' || g", "'z' || g").replace("'l' || t", "'z' || t")
    sql = sql.replace("'s3://l/' || g", "'s3://z/' || g")
    for stmt in sql.split(";"):
        if stmt.strip():
            await conn.execute(text(stmt))


async def test_full_claim_statement_walk_plans(fresh_url: str) -> None:
    eng = create_async_engine(fresh_url)
    captured: list[tuple[str, Any]] = []

    @event.listens_for(eng.sync_engine, "before_cursor_execute")
    def _capture(
        conn: Any, cursor: Any, statement: str, parameters: Any, context: Any, many: bool
    ) -> None:
        if "WITH RECURSIVE" in statement:
            captured.append((statement, parameters))

    sm = async_sessionmaker(eng, expire_on_commit=False)
    raw = create_async_engine(fresh_url, isolation_level="AUTOCOMMIT", connect_args=NOCACHE)
    try:
        async with raw.connect() as conn:
            await seed(conn, n=10_000)
        ((principal, _),) = await make_executors(sm, 1)
        async with sm() as s:
            res = await claim(
                s,
                principal.executor_id,
                engines=[Engine.FAKE],
                model_ids=["m1"],
                memory_gb=8.0,
                pool="default",
                preference=None,
                window=16,
            )
            assert res.won is not None
            await s.rollback()
        stmt, params = captured[-1]
        args = ", ".join(f"'{v}'" if not isinstance(v, int | float) else str(v) for v in params)
        async with raw.connect() as conn:
            for mode in ("force_custom_plan", "force_generic_plan"):
                await conn.exec_driver_sql(f"SET plan_cache_mode = {mode}")
                await conn.exec_driver_sql("BEGIN")
                try:
                    await conn.exec_driver_sql(f"PREPARE cq AS {stmt}")
                    plan = _json_plan(
                        await conn.exec_driver_sql(
                            f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE cq({args})"
                        )
                    )
                finally:
                    await conn.exec_driver_sql("ROLLBACK")
                    await conn.exec_driver_sql("DEALLOCATE ALL")
                got = _buffers(plan)
                print(f"claim {mode}: buffers={got} ms={plan['Execution Time']:.2f}")
                assert any(x["Node Type"] == "Recursive Union" for x in _nodes(plan["Plan"]))
                assert "ix_minion_task_live_queue" in json.dumps(plan)
                assert got <= 250, (mode, got)  # measured 113-137
    finally:
        await eng.dispose()
        await raw.dispose()


def _splitter_select() -> Select[Any]:
    """The splitter claim's SELECT (repositories/tasks.py claim_splitting), same predicates."""
    return (
        select(Task)
        .where(
            Task.status == TaskStatus.SPLITTING.value,
            or_(Task.next_split_at.is_(None), Task.next_split_at <= func.now()),
        )
        .order_by(Task.next_split_at.asc().nulls_first(), Task.created_at)
        .limit(1)
        .with_for_update(skip_locked=True, key_share=True)
    )


async def test_task29_splitter_claim_needs_ix_minion_task_queue(fresh_url: str) -> None:
    """TASK-29 evidence: the index is kept because the splitter poll uses it (O(SPLITTING))."""
    eng = create_async_engine(fresh_url, isolation_level="AUTOCOMMIT", connect_args=NOCACHE)
    try:
        async with eng.connect() as conn:
            await seed(conn, n=10_000, hist=40_000)
            await conn.execute(
                text(
                    "INSERT INTO minion_task (team, project, task_name, type, model_id, engine,"
                    " model_memory_gb, input_uri, output_uri, status, created_by)"
                    " SELECT 'sp', 'p', 's' || g, 'embedding', 'm1', 'fake', 1, 'in',"
                    " 's3://sp/' || g, 'SPLITTING', 't' FROM generate_series(1, 50) g"
                )
            )
            await conn.execute(text("ANALYZE minion_task"))
            await conn.exec_driver_sql("BEGIN")
            try:
                plans = await explain_both(conn, _splitter_select())
            finally:
                await conn.exec_driver_sql("ROLLBACK")
    finally:
        await eng.dispose()
    for kind, plan in plans.items():
        got = _buffers(plan)
        print(f"splitter {kind}: buffers={got} ms={plan['Execution Time']:.2f}")
        assert "ix_minion_task_queue" in json.dumps(plan), (kind, json.dumps(plan))
        assert got <= 20, (kind, got)  # measured 6
