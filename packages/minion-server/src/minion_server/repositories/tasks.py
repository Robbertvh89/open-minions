"""Task and subtask persistence for creation and splitting. SQL lives here only."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from minion_common.enums import SubtaskStatus, TaskStatus
from minion_common.schemas import ModelEntry, TaskCreate
from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.db.models import Subtask, Task

OUTPUT_URI_CONSTRAINT = "uq_minion_task_output_uri"


class OutputUriInUseError(Exception):
    """Another task already writes to this output prefix."""


@dataclass(frozen=True)
class NewSubtask:
    subtask_id: int
    input_object_key: str
    byte_start: int
    byte_end: int
    first_record: int
    record_count: int


async def insert_task(
    session: AsyncSession,
    body: TaskCreate,
    *,
    model: ModelEntry,
    input_uri: str,
    output_uri: str,
    created_by: str,
) -> bool:
    """Insert a SPLITTING task. Returns False if `(team, project, task_name)` already exists.

    Raises `OutputUriInUseError` if the output prefix belongs to another task (unique constraint;
    no check-then-insert).
    """
    stmt = (
        pg_insert(Task)
        .values(
            team=body.team,
            project=body.project,
            task_name=body.task_name,
            type=body.type.value,
            model_id=model.model_id,
            engine=model.engine.value,
            model_memory_gb=model.memory_gb,
            input_uri=input_uri,
            output_uri=output_uri,
            params=body.params,
            records_per_subtask=body.records_per_subtask,
            priority=body.priority,
            allowed_pool=body.allowed_pool,
            created_by=created_by,
        )
        .on_conflict_do_nothing(index_elements=["team", "project", "task_name"])
        .returning(Task.task_name)
    )
    try:
        return (await session.execute(stmt)).first() is not None
    except IntegrityError as exc:
        if OUTPUT_URI_CONSTRAINT in str(exc.orig):
            raise OutputUriInUseError from exc
        raise


async def get_task(
    session: AsyncSession, team: str, project: str, task_name: str
) -> tuple[Task, dict[SubtaskStatus, int]] | None:
    task = (
        await session.execute(
            select(Task).where(
                Task.team == team, Task.project == project, Task.task_name == task_name
            )
        )
    ).scalar_one_or_none()
    if task is None:
        return None
    rows = await session.execute(
        select(Subtask.status, func.count())
        .where(Subtask.team == team, Subtask.project == project, Subtask.task_name == task_name)
        .group_by(Subtask.status)
    )
    return task, {SubtaskStatus(s): int(n) for s, n in rows.all()}


def _task_key(task: Task) -> tuple[Any, ...]:
    return (Task.team == task.team, Task.project == task.project, Task.task_name == task.task_name)


SPLIT_ATTEMPTS_EXHAUSTED = "SPLIT_ATTEMPTS_EXHAUSTED"


async def claim_splitting(session: AsyncSession, lease_s: float, max_attempts: int) -> Task | None:
    """Claim one due SPLITTING task with a lease; the caller commits straight away.

    `FOR NO KEY UPDATE SKIP LOCKED` serialises concurrent claimers only for the instant of the
    claim. The claim bumps `split_attempts` and pushes `next_split_at` out by `lease_s`, so other
    splitters skip the task while it is being worked on, and a crashed splitter's task becomes
    claimable again once the lease expires (counting as one attempt). `split_attempts` doubles as
    the fencing token for every later write by the claim holder. Tasks that already used
    `max_attempts` claims (e.g. a crash loop that never reaches the retry logic) are marked
    FAILED (`SPLIT_ATTEMPTS_EXHAUSTED`) instead of being handed out again.
    """
    while True:
        task = (
            await session.execute(
                select(Task)
                .where(
                    Task.status == TaskStatus.SPLITTING.value,
                    or_(Task.next_split_at.is_(None), Task.next_split_at <= func.now()),
                )
                .order_by(Task.next_split_at.asc().nulls_first(), Task.created_at)
                .limit(1)
                .with_for_update(skip_locked=True, key_share=True)
            )
        ).scalar_one_or_none()
        if task is None:
            return None
        if task.split_attempts >= max_attempts:
            await session.execute(
                update(Task)
                .where(*_task_key(task), Task.status == TaskStatus.SPLITTING.value)
                .values(
                    status=TaskStatus.FAILED.value,
                    error=SPLIT_ATTEMPTS_EXHAUSTED,
                    finished_at=datetime.now(UTC),
                    next_split_at=None,
                )
            )
            await session.refresh(task)
            session.expunge(task)
            continue
        break
    await session.execute(
        update(Task)
        .where(*_task_key(task))
        .values(
            split_attempts=Task.split_attempts + 1,
            next_split_at=func.now() + timedelta(seconds=lease_s),
        )
    )
    await session.refresh(task)
    return task


def _held(task: Task) -> tuple[Any, ...]:
    """WHERE clauses: still SPLITTING and still the claim identified by `split_attempts`."""
    return (
        *_task_key(task),
        Task.status == TaskStatus.SPLITTING.value,
        Task.split_attempts == task.split_attempts,
    )


async def renew_split_lease(session: AsyncSession, task: Task, lease_s: float) -> bool:
    """Extend the lease; False if another claim superseded ours (stop splitting)."""
    res = await session.execute(
        update(Task)
        .where(*_held(task))
        .values(next_split_at=func.now() + timedelta(seconds=lease_s))
    )
    return bool(res.rowcount)  # type: ignore[attr-defined]


async def existing_subtasks(
    session: AsyncSession, task: Task, ids: Sequence[int]
) -> dict[int, tuple[str, int, int]]:
    """`subtask_id -> (input_object_key, byte_start, byte_end)` for already-inserted rows."""
    rows = await session.execute(
        select(
            Subtask.subtask_id, Subtask.input_object_key, Subtask.byte_start, Subtask.byte_end
        ).where(
            Subtask.team == task.team,
            Subtask.project == task.project,
            Subtask.task_name == task.task_name,
            Subtask.subtask_id.in_(list(ids)),
        )
    )
    return {r[0]: (r[1], r[2], r[3]) for r in rows.all()}


async def count_subtasks(session: AsyncSession, task: Task) -> int:
    return int(
        (
            await session.execute(
                select(func.count()).where(
                    Subtask.team == task.team,
                    Subtask.project == task.project,
                    Subtask.task_name == task.task_name,
                )
            )
        ).scalar_one()
    )


async def insert_subtasks(session: AsyncSession, task: Task, rows: Sequence[NewSubtask]) -> None:
    """Idempotent bulk insert: re-running a split after a crash inserts the same rows again."""
    values: list[dict[str, Any]] = [
        {
            "team": task.team,
            "project": task.project,
            "task_name": task.task_name,
            "subtask_id": r.subtask_id,
            "attempt": 0,
            "max_attempts": task.max_attempts,
            "input_object_key": r.input_object_key,
            "byte_start": r.byte_start,
            "byte_end": r.byte_end,
            "first_record": r.first_record,
            "record_count": r.record_count,
        }
        for r in rows
    ]
    await session.execute(pg_insert(Subtask).values(values).on_conflict_do_nothing())


async def mark_split_done(session: AsyncSession, task: Task, total: int) -> bool:
    """Fenced on the claim's `split_attempts`; False means the lease was lost (no-op)."""
    res = await session.execute(
        update(Task)
        .where(*_held(task))
        .values(status=TaskStatus.PENDING.value, total_subtasks=total, next_split_at=None)
    )
    return bool(res.rowcount)  # type: ignore[attr-defined]


async def mark_split_failed(session: AsyncSession, task: Task, error: str) -> bool:
    res = await session.execute(
        update(Task)
        .where(*_held(task))
        .values(
            status=TaskStatus.FAILED.value,
            error=error,
            finished_at=datetime.now(UTC),
            next_split_at=None,
        )
    )
    return bool(res.rowcount)  # type: ignore[attr-defined]


async def schedule_split_retry(session: AsyncSession, task: Task, delay_s: float) -> bool:
    """Make the task claimable again after `delay_s` (exponential backoff)."""
    res = await session.execute(
        update(Task)
        .where(*_held(task))
        .values(next_split_at=func.now() + timedelta(seconds=delay_s))
    )
    return bool(res.rowcount)  # type: ignore[attr-defined]
