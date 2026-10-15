"""Complete / fail / release persistence (plan D6). SQL lives here only.

Every function runs inside the caller's open transaction. `lock_owned` takes the row lock on the
acquirement (`SELECT ... FOR UPDATE`) that serializes against the watchdog (ARCHITECTURE §4).
"""

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from minion_common.enums import AttemptOutcome, SubtaskStatus, TaskStatus
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.db.models import (
    Subtask,
    SubtaskAcquirement,
    SubtaskAttempt,
    Task,
)

BUDGET_OUTCOMES = (AttemptOutcome.FAILED.value, AttemptOutcome.EXPIRED.value)


@dataclass(frozen=True)
class Owned:
    acquirement_id: uuid.UUID
    team: str
    project: str
    task_name: str
    subtask_id: int
    executor_id: uuid.UUID
    attempt: int
    acquired_at: datetime
    record_count: int
    max_attempts: int
    task_output_uri: str


@dataclass(frozen=True)
class TaskCounts:
    total: int
    succeeded: int
    failed: int
    status: TaskStatus


async def lock_owned(
    session: AsyncSession, acquirement_id: uuid.UUID, executor_id: uuid.UUID
) -> Owned | None:
    """Lock the acquirement row; None if it is gone or belongs to another executor."""
    row = (
        await session.execute(
            select(SubtaskAcquirement)
            .where(SubtaskAcquirement.acquirement_id == acquirement_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if row is None or row.executor_id != executor_id:
        return None
    info = (
        await session.execute(
            select(Subtask.record_count, Subtask.max_attempts, Task.output_uri)
            .join(
                Task,
                (Task.team == Subtask.team)
                & (Task.project == Subtask.project)
                & (Task.task_name == Subtask.task_name),
            )
            .where(
                Subtask.team == row.team,
                Subtask.project == row.project,
                Subtask.task_name == row.task_name,
                Subtask.subtask_id == row.subtask_id,
            )
        )
    ).one()
    return Owned(
        acquirement_id=row.acquirement_id,
        team=row.team,
        project=row.project,
        task_name=row.task_name,
        subtask_id=row.subtask_id,
        executor_id=row.executor_id,
        attempt=row.attempt,
        acquired_at=row.acquired_at,
        record_count=info[0],
        max_attempts=info[1],
        task_output_uri=info[2],
    )


async def completed_by(session: AsyncSession, acquirement_id: uuid.UUID) -> uuid.UUID | None:
    """Executor that already completed this acquirement successfully (idempotent retries)."""
    return (
        await session.execute(
            select(SubtaskAttempt.executor_id).where(
                SubtaskAttempt.acquirement_id == acquirement_id,
                SubtaskAttempt.outcome == AttemptOutcome.SUCCEEDED.value,
            )
        )
    ).scalar_one_or_none()


async def failed_attempt_count(session: AsyncSession, o: Owned) -> int:
    """Attempts that consume the retry budget (FAILED + EXPIRED); RELEASED does not."""
    return int(
        (
            await session.execute(
                select(func.count())
                .select_from(SubtaskAttempt)
                .where(
                    SubtaskAttempt.team == o.team,
                    SubtaskAttempt.project == o.project,
                    SubtaskAttempt.task_name == o.task_name,
                    SubtaskAttempt.subtask_id == o.subtask_id,
                    SubtaskAttempt.outcome.in_(BUDGET_OUTCOMES),
                )
            )
        ).scalar_one()
    )


async def finish_attempt(
    session: AsyncSession,
    o: Owned,
    *,
    outcome: AttemptOutcome,
    subtask_status: SubtaskStatus,
    output_uri: str | None = None,
    error: dict[str, Any] | None = None,
) -> None:
    """Record the attempt, drop the acquirement (cascades heartbeat), move the subtask."""
    await session.execute(
        pg_insert(SubtaskAttempt).values(
            team=o.team,
            project=o.project,
            task_name=o.task_name,
            subtask_id=o.subtask_id,
            attempt=o.attempt,
            acquirement_id=o.acquirement_id,
            executor_id=o.executor_id,
            outcome=outcome.value,
            started_at=o.acquired_at,
            error=error,
        )
    )
    await session.execute(
        SubtaskAcquirement.__table__.delete().where(  # type: ignore[attr-defined]
            SubtaskAcquirement.acquirement_id == o.acquirement_id
        )
    )
    values: dict[str, Any] = {"status": subtask_status.value, "error": error}
    if output_uri is not None:
        values["output_uri"] = output_uri
    await session.execute(
        update(Subtask)
        .where(
            Subtask.team == o.team,
            Subtask.project == o.project,
            Subtask.task_name == o.task_name,
            Subtask.subtask_id == o.subtask_id,
        )
        .values(**values)
    )


async def bump_counters(
    session: AsyncSession, o: Owned, *, succeeded: int, failed: int
) -> TaskCounts:
    """Atomically bump task counters; the UPDATE row-locks the task for status derivation."""
    row = (
        await session.execute(
            update(Task)
            .where(Task.team == o.team, Task.project == o.project, Task.task_name == o.task_name)
            .values(
                succeeded_subtasks=Task.succeeded_subtasks + succeeded,
                failed_subtasks=Task.failed_subtasks + failed,
            )
            .returning(
                Task.total_subtasks, Task.succeeded_subtasks, Task.failed_subtasks, Task.status
            )
        )
    ).one()
    return TaskCounts(row[0], row[1], row[2], TaskStatus(row[3]))


async def set_task_terminal(session: AsyncSession, o: Owned, status: TaskStatus) -> None:
    await session.execute(
        update(Task)
        .where(
            Task.team == o.team,
            Task.project == o.project,
            Task.task_name == o.task_name,
            Task.status.in_([TaskStatus.PENDING.value, TaskStatus.RUNNING.value]),
        )
        .values(status=status.value, finished_at=func.now())
    )
