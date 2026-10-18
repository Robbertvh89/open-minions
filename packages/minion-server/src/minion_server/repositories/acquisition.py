"""Acquire persistence (ARCHITECTURE §3.2, plan D5). SQL lives here only.

Ownership is decided solely by `INSERT ... ON CONFLICT DO NOTHING` into
`minion_subtask_acquirement` (CLAUDE.md invariant 2). Candidate selection is deliberately unlocked.
"""

import uuid
from collections.abc import Collection
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, cast

from minion_common.enums import Engine, SubtaskStatus, TaskStatus
from sqlalchemy import ColumnElement, Select, exists, literal, literal_column, select, true, update
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


def _lit(value: str) -> ColumnElement[str]:
    """A SQL string literal (never a bind param): asyncpg's generic plans cannot prove a bound
    status matches a partial-index predicate, a literal always can. Values are enum constants."""
    return literal_column("'" + value + "'")


def candidates_stmt(
    *,
    engines: list[Engine],
    model_ids: Collection[str],
    memory_gb: float,
    pool: str,
    preference: tuple[str | None, str | None, str | None] | None,
    limit: int = CANDIDATE_LIMIT,
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
            Task.status.in_([_lit(TaskStatus.PENDING.value), _lit(TaskStatus.RUNNING.value)]),
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
            Subtask.status == _lit(SubtaskStatus.PENDING.value),
        )
        .order_by(Subtask.subtask_id)
        .limit(limit)
        .lateral("s")
    )
    stmt = (
        select(
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
        )
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


async def try_acquire(session: AsyncSession, c: Candidate, executor_id: uuid.UUID) -> Won | None:
    """Race for one candidate inside the caller's open transaction.

    Returns None if another executor owns it, it is no longer PENDING, or its task is no longer
    PENDING/RUNNING (cancelled). The caller must roll back on None (so no acquirement survives a
    failed re-check); on success it commits.

    The task-status guard takes no task row lock (FOR SHARE would deadlock with the PENDING ->
    RUNNING task update below). Residual window under READ COMMITTED: a statement that began
    before a concurrent cancel committed can still win; that owner is told to stop by
    `heartbeat.cancel=true` and the sweep heals the rest.
    """
    acquirement_id = uuid.uuid4()
    live = [TaskStatus.PENDING.value, TaskStatus.RUNNING.value]
    task_key = (
        Task.team == c.team,
        Task.project == c.project,
        Task.task_name == c.task_name,
    )
    key = (
        Subtask.team == c.team,
        Subtask.project == c.project,
        Subtask.task_name == c.task_name,
        Subtask.subtask_id == c.subtask_id,
    )
    insert = (
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
                literal(acquirement_id, type_=UUID(as_uuid=True)),
                Subtask.team,
                Subtask.project,
                Subtask.task_name,
                Subtask.subtask_id,
                literal(executor_id, type_=UUID(as_uuid=True)),
                Subtask.attempt + 1,
            )
            .join(
                Task,
                (Task.team == Subtask.team)
                & (Task.project == Subtask.project)
                & (Task.task_name == Subtask.task_name),
            )
            .where(*key, Subtask.status == SubtaskStatus.PENDING.value, Task.status.in_(live)),
        )
        .on_conflict_do_nothing(constraint=ACQUIREMENT_CONSTRAINT)
        .returning(SubtaskAcquirement.acquirement_id, SubtaskAcquirement.attempt)
    )
    won = (await session.execute(insert)).one_or_none()
    if won is None:
        return None
    # Re-check closes the race with a concurrent completion that deleted the old acquirement.
    bumped = (
        await session.execute(
            update(Subtask)
            .where(*key, Subtask.status == SubtaskStatus.PENDING.value)
            .where(Subtask.attempt == won[1] - 1)
            .where(exists(select(1).where(*task_key, Task.status.in_(live))))
            .values(status=SubtaskStatus.RUNNING.value, attempt=Subtask.attempt + 1)
            .returning(Subtask.attempt)
        )
    ).one_or_none()
    if bumped is None:
        return None
    await session.execute(pg_insert(ExecutorHeartbeat).values(acquirement_id=won[0]))
    await session.execute(
        update(Task)
        .where(
            Task.team == c.team,
            Task.project == c.project,
            Task.task_name == c.task_name,
            Task.status == TaskStatus.PENDING.value,
        )
        .values(status=TaskStatus.RUNNING.value)
    )
    return Won(won[0], won[1])
