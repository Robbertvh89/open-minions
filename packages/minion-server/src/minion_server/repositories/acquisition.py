"""Acquire persistence (ARCHITECTURE §3.2, plan D5). SQL lives here only.

Ownership is decided solely by `INSERT ... ON CONFLICT DO NOTHING` into
`minion_subtask_acquirement` (CLAUDE.md invariant 2). Candidate selection is deliberately unlocked.
"""

import re
import uuid
from collections.abc import Collection
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, cast

from minion_common.enums import Engine, SubtaskStatus, TaskStatus
from sqlalchemy import (
    ColumnElement,
    Select,
    and_,
    exists,
    func,
    literal,
    literal_column,
    select,
    true,
    update,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.db.models import (
    ExecutorHeartbeat,
    Subtask,
    SubtaskAcquirement,
    Task,
)

CANDIDATE_LIMIT = 16
MAX_WINDOW = 256  # matches ServerSettings.acquire_window le=256
ACQUIREMENT_CONSTRAINT = "uq_minion_subtask_acquirement_subtask"


@dataclass(frozen=True)
class Candidate:
    team: str
    project: str
    task_name: str
    subtask_id: int
    model_id: str
    input_object_key: str
    byte_start: int
    byte_end: int
    record_count: int
    output_uri: str
    params: dict[str, Any]


@dataclass(frozen=True)
class Won:
    acquirement_id: uuid.UUID
    attempt: int


def _lit(status: TaskStatus | SubtaskStatus) -> ColumnElement[str]:
    """A SQL string literal (never a bind param): asyncpg's generic plans cannot prove a bound
    status matches a partial-index predicate, a literal always can. Values are enum constants."""
    value = status.value
    if not re.fullmatch(r"[A-Z_]+", value):
        raise ValueError(f"unsafe status literal: {value!r}")
    return literal_column("'" + value + "'")


def candidates_stmt(
    *,
    engines: list[Engine],
    model_ids: Collection[str],
    memory_gb: float,
    pool: str,
    preference: tuple[str | None, str | None, str | None] | None,
    limit: int = CANDIDATE_LIMIT,
    with_priority: bool = False,
) -> Select[Any]:
    """The candidate query (also what the scale harness EXPLAINs).

    Two-step, index-driven: walk the eligible live tasks in FR-22 order on
    `ix_minion_task_live_queue`, and for each take its first `limit` PENDING subtasks from
    `ix_minion_subtask_pending` (LATERAL, ordered by subtask_id, LIMIT). Cost is
    O(limit + live tasks), independent of the number of pending subtasks.

    Order equivalence with the single-join form `ORDER BY priority DESC, created_at, team,
    project, task_name, subtask_id LIMIT k`: the task key (team, project, task_name) is the
    primary key, so the sort key is a total order and rows of one task are contiguous, ordered
    by subtask_id. The global first k rows therefore contain at most k rows of any one task, and
    those are that task's first k PENDING subtasks by subtask_id, which is exactly what each
    lateral branch returns. The outer ORDER BY + LIMIT then yields the same list.
    """
    live = (
        select(
            Task.team,
            Task.project,
            Task.task_name,
            Task.model_id,
            Task.output_uri,
            Task.params,
            Task.priority,
            Task.created_at,
        )
        .where(
            Task.status.in_([_lit(TaskStatus.PENDING), _lit(TaskStatus.RUNNING)]),
            Task.model_id.in_(list(model_ids)),  # removed models must not occupy the head
            Task.engine.in_([e.value for e in engines]),
            Task.model_memory_gb <= Decimal(str(memory_gb)),
            (Task.allowed_pool.is_(None)) | (Task.allowed_pool == pool),
        )
        .order_by(
            Task.priority.desc(),
            Task.created_at,
            Task.team,
            Task.project,
            Task.task_name,
        )
    )
    if preference is not None:
        for col, value in zip((Task.team, Task.project, Task.task_name), preference, strict=True):
            if value is not None:
                live = live.where(col == value)
    t = live.subquery("t")
    per_task = (
        select(
            Subtask.subtask_id,
            Subtask.input_object_key,
            Subtask.byte_start,
            Subtask.byte_end,
            Subtask.record_count,
        )
        .where(
            Subtask.team == t.c.team,
            Subtask.project == t.c.project,
            Subtask.task_name == t.c.task_name,
            Subtask.status == _lit(SubtaskStatus.PENDING),
        )
        .order_by(Subtask.subtask_id)
        .limit(limit)
        .lateral("s")
    )
    cols: list[ColumnElement[Any]] = [
        t.c.team,
        t.c.project,
        t.c.task_name,
        per_task.c.subtask_id,
        t.c.model_id,
        per_task.c.input_object_key,
        per_task.c.byte_start,
        per_task.c.byte_end,
        per_task.c.record_count,
        t.c.output_uri,
        t.c.params,
    ]
    if with_priority:
        cols.append(t.c.priority)
    stmt = (
        select(*cols)
        .select_from(t)
        .join(per_task, true())
        .order_by(
            t.c.priority.desc(),
            t.c.created_at,
            t.c.team,
            t.c.project,
            t.c.task_name,
            per_task.c.subtask_id,
        )
        .limit(limit)
    )
    return cast("Select[Any]", stmt)


async def candidates(
    session: AsyncSession,
    *,
    engines: list[Engine],
    model_ids: Collection[str],
    memory_gb: float,
    pool: str,
    preference: tuple[str | None, str | None, str | None] | None,
    limit: int = CANDIDATE_LIMIT,
) -> list[Candidate]:
    """Unlocked read of PENDING subtasks this executor may run, in queue order.

    `preference` filters (None entries are wildcards); `None` means no preference filter.
    """
    if not model_ids:
        return []
    stmt = candidates_stmt(
        engines=engines,
        model_ids=model_ids,
        memory_gb=memory_gb,
        pool=pool,
        preference=preference,
        limit=limit,
    )
    rows = (await session.execute(stmt)).all()
    return [
        Candidate(
            team=r[0],
            project=r[1],
            task_name=r[2],
            subtask_id=r[3],
            model_id=r[4],
            input_object_key=r[5],
            byte_start=r[6],
            byte_end=r[7],
            record_count=r[8],
            output_uri=r[9],
            params=r[10],
        )
        for r in rows
    ]


async def _after_win(
    session: AsyncSession, k: tuple[str, str, str, int], acquirement_id: uuid.UUID, attempt: int
) -> Won | None:
    """Steps that follow a won acquirement INSERT, in the same transaction. None = roll back."""
    team, project, task_name, subtask_id = k
    live = [TaskStatus.PENDING.value, TaskStatus.RUNNING.value]
    task_key = (Task.team == team, Task.project == project, Task.task_name == task_name)
    key = (
        Subtask.team == team,
        Subtask.project == project,
        Subtask.task_name == task_name,
        Subtask.subtask_id == subtask_id,
    )
    # Re-check closes the race with a concurrent completion that deleted the old acquirement.
    bumped = (
        await session.execute(
            update(Subtask)
            .where(*key, Subtask.status == SubtaskStatus.PENDING.value)
            .where(Subtask.attempt == attempt - 1)
            .where(exists(select(1).where(*task_key, Task.status.in_(live))))
            .values(status=SubtaskStatus.RUNNING.value, attempt=Subtask.attempt + 1)
            .returning(Subtask.attempt)
        )
    ).one_or_none()
    if bumped is None:
        return None
    await session.execute(pg_insert(ExecutorHeartbeat).values(acquirement_id=acquirement_id))
    await session.execute(
        update(Task)
        .where(*task_key, Task.status == TaskStatus.PENDING.value)
        .values(status=TaskStatus.RUNNING.value)
    )
    return Won(acquirement_id, attempt)


@dataclass(frozen=True)
class Claim:
    """Outcome of one `claim` round. `n_candidates == 0`: no work. `n_candidates > 0` with
    `candidate is None`: the pick lost the insert race. `candidate` set but `won is None`: the
    INSERT won but the re-check failed; the caller must roll back. Either way: read again."""

    n_candidates: int
    candidate: Candidate | None = None
    won: Won | None = None


async def claim(
    session: AsyncSession,
    executor_id: uuid.UUID,
    *,
    engines: list[Engine],
    model_ids: Collection[str],
    memory_gb: float,
    pool: str,
    preference: tuple[str | None, str | None, str | None] | None,
    window: int,
) -> Claim:
    """One-statement claim (ADR-0013): read the top `window` candidates, pick one at random among
    the highest priority present, `INSERT ... ON CONFLICT DO NOTHING` it into the acquirement
    table, all in a single round trip. Ownership is decided only by that INSERT (invariant 2).

    Runs in the caller's open transaction; on a win the re-check UPDATE, heartbeat insert and task
    update follow here. If the re-check fails the result has no `won` and the caller must roll back.
    """
    if not 1 <= window <= MAX_WINDOW:
        raise ValueError(f"claim window must be in 1..{MAX_WINDOW}, got {window}")
    if not model_ids:
        return Claim(0)
    cand = candidates_stmt(
        engines=engines,
        model_ids=model_ids,
        memory_gb=memory_gb,
        pool=pool,
        preference=preference,
        limit=window,
        with_priority=True,
    ).cte("cand")
    max_prio = select(func.max(cand.c.priority)).scalar_subquery()
    pick = (
        select(*cand.c)
        .where(cand.c.priority == max_prio)
        .order_by(func.random())
        .limit(1)
        .cte("pick")
    )
    live = [_lit(TaskStatus.PENDING), _lit(TaskStatus.RUNNING)]
    ins = (
        pg_insert(SubtaskAcquirement)
        .from_select(
            [
                "acquirement_id",
                "team",
                "project",
                "task_name",
                "subtask_id",
                "executor_id",
                "attempt",
            ],
            select(
                literal(uuid.uuid4(), type_=UUID(as_uuid=True)),
                Subtask.team,
                Subtask.project,
                Subtask.task_name,
                Subtask.subtask_id,
                literal(executor_id, type_=UUID(as_uuid=True)),
                Subtask.attempt + 1,
            )
            .select_from(pick)
            .join(
                Subtask,
                and_(
                    Subtask.team == pick.c.team,
                    Subtask.project == pick.c.project,
                    Subtask.task_name == pick.c.task_name,
                    Subtask.subtask_id == pick.c.subtask_id,
                ),
            )
            .join(
                Task,
                (Task.team == Subtask.team)
                & (Task.project == Subtask.project)
                & (Task.task_name == Subtask.task_name),
            )
            .where(Subtask.status == _lit(SubtaskStatus.PENDING), Task.status.in_(live)),
        )
        .on_conflict_do_nothing(constraint=ACQUIREMENT_CONSTRAINT)
        .returning(
            SubtaskAcquirement.acquirement_id,
            SubtaskAcquirement.attempt,
            SubtaskAcquirement.team,
            SubtaskAcquirement.project,
            SubtaskAcquirement.task_name,
            SubtaskAcquirement.subtask_id,
        )
        .cte("ins")
    )
    n = select(func.count().label("n")).select_from(cand).subquery("n")
    stmt = (
        select(
            n.c.n,
            ins.c.acquirement_id,
            ins.c.attempt,
            ins.c.team.label("ins_team"),
            ins.c.project.label("ins_project"),
            ins.c.task_name.label("ins_task_name"),
            ins.c.subtask_id.label("ins_subtask_id"),
            pick.c.team,
            pick.c.project,
            pick.c.task_name,
            pick.c.subtask_id,
            pick.c.model_id,
            pick.c.input_object_key,
            pick.c.byte_start,
            pick.c.byte_end,
            pick.c.record_count,
            pick.c.output_uri,
            pick.c.params,
        )
        .select_from(n)
        .outerjoin(ins, true())
        .outerjoin(pick, true())
    )
    row = (await session.execute(stmt)).one()
    return await _claim_from_row(session, tuple(row))


async def _claim_from_row(session: AsyncSession, row: tuple[Any, ...]) -> Claim:
    """Turn the claim statement's row into a `Claim`; runs the post-win steps."""
    if row[0] == 0 or row[1] is None:
        return Claim(row[0])
    if tuple(row[3:7]) != tuple(row[7:11]):  # inserted key != picked key: never serve the wrong one
        raise RuntimeError("claim: inserted acquirement key differs from the picked candidate")
    c = Candidate(
        team=row[7],
        project=row[8],
        task_name=row[9],
        subtask_id=row[10],
        model_id=row[11],
        input_object_key=row[12],
        byte_start=row[13],
        byte_end=row[14],
        record_count=row[15],
        output_uri=row[16],
        params=row[17],
    )
    won = await _after_win(session, (c.team, c.project, c.task_name, c.subtask_id), row[1], row[2])
    return Claim(row[0], c, won)
