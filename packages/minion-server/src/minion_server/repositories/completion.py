"""Complete / fail / release persistence (plan D6). SQL lives here only.

Every function runs inside the caller's open transaction. `lock_owned` takes the row lock on the
acquirement (`SELECT ... FOR UPDATE`) that serializes against the watchdog (ARCHITECTURE §4).
"""

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from minion_common.enums import AttemptOutcome, SubtaskStatus, TaskStatus
from sqlalchemy import false, func, null, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.db.models import (
    Subtask,
    SubtaskAcquirement,
    SubtaskAttempt,
    Task,
    TaskCounter,
)

TASK_COUNTER_SHARDS = 16  # ADR-0015
BUDGET_OUTCOMES = (AttemptOutcome.FAILED.value, AttemptOutcome.EXPIRED.value)
NOT_ACKNOWLEDGED = "NOT_ACKNOWLEDGED"  # ADR-0014: EXPIRED, never heartbeated after the claim


def _unacked_expiry() -> Any:
    """Attempt rows that are free EXPIRED / NOT_ACKNOWLEDGED (history `error.code`)."""
    return (SubtaskAttempt.outcome == AttemptOutcome.EXPIRED.value) & (
        SubtaskAttempt.error["code"].astext == NOT_ACKNOWLEDGED
    )


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
    task_status: TaskStatus


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
    return await owned(session, row)


async def owned(session: AsyncSession, row: SubtaskAcquirement) -> Owned:
    """Build `Owned` from an already-locked acquirement row (reused by reclaim)."""
    info = (
        await session.execute(
            select(Subtask.record_count, Subtask.max_attempts, Task.output_uri, Task.status)
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
        task_status=TaskStatus(info[3]),
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
    """Attempts that consume the retry budget (FAILED + EXPIRED); RELEASED and the free
    NOT_ACKNOWLEDGED expiries (ADR-0014) do not."""
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
                    ~func.coalesce(_unacked_expiry(), false()),
                )
            )
        ).scalar_one()
    )


async def released_attempt_count(session: AsyncSession, o: Owned) -> int:
    """RELEASED attempts of this subtask (free until the release cap, then they count)."""
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
                    SubtaskAttempt.outcome == AttemptOutcome.RELEASED.value,
                )
            )
        ).scalar_one()
    )


async def unacknowledged_attempt_count(session: AsyncSession, o: Owned) -> int:
    """Earlier NOT_ACKNOWLEDGED expiries of this subtask (free until the cap, ADR-0014)."""
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
                    _unacked_expiry(),
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
    attempt_error: dict[str, Any] | None = None,
) -> None:
    """Record the attempt, drop the acquirement (cascades heartbeat), move the subtask.

    `error` goes on both rows; `attempt_error` (a release reason) only on the attempt history.
    """
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
            # SQL NULL (not JSON null) when there is no error / release reason
            error=(error if error is not None else attempt_error) or null(),
        )
    )
    await session.execute(
        SubtaskAcquirement.__table__.delete().where(  # type: ignore[attr-defined]
            SubtaskAcquirement.acquirement_id == o.acquirement_id
        )
    )
    values: dict[str, Any] = {
        "status": subtask_status.value,
        "error": error if error is not None else null(),
    }
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


def counter_shard(subtask_id: int) -> int:
    """Counter shard of a subtask (ADR-0015)."""
    return subtask_id % TASK_COUNTER_SHARDS


async def bump_counters(session: AsyncSession, o: Owned, *, succeeded: int, failed: int) -> None:
    """Add to this subtask's counter shard (upsert); the row lock is per shard, not per task."""
    ins = pg_insert(TaskCounter).values(
        team=o.team,
        project=o.project,
        task_name=o.task_name,
        shard=counter_shard(o.subtask_id),
        succeeded=succeeded,
        failed=failed,
    )
    await session.execute(
        ins.on_conflict_do_update(
            index_elements=["team", "project", "task_name", "shard"],
            set_={
                "succeeded": TaskCounter.succeeded + ins.excluded.succeeded,
                "failed": TaskCounter.failed + ins.excluded.failed,
            },
        )
    )


@dataclass(frozen=True)
class TaskKey:
    team: str
    project: str
    task_name: str


async def task_counts(session: AsyncSession, key: TaskKey) -> TaskCounts | None:
    """Task status, total and the shard sums; None if the task is gone."""
    row = (
        await session.execute(
            select(
                Task.total_subtasks,
                select(func.coalesce(func.sum(TaskCounter.succeeded), 0))
                .where(
                    TaskCounter.team == Task.team,
                    TaskCounter.project == Task.project,
                    TaskCounter.task_name == Task.task_name,
                )
                .scalar_subquery(),
                select(func.coalesce(func.sum(TaskCounter.failed), 0))
                .where(
                    TaskCounter.team == Task.team,
                    TaskCounter.project == Task.project,
                    TaskCounter.task_name == Task.task_name,
                )
                .scalar_subquery(),
                Task.status,
            ).where(
                Task.team == key.team, Task.project == key.project, Task.task_name == key.task_name
            )
        )
    ).one_or_none()
    if row is None:
        return None
    return TaskCounts(int(row[0]), int(row[1]), int(row[2]), TaskStatus(row[3]))


async def set_task_terminal(session: AsyncSession, key: TaskKey, status: TaskStatus) -> bool:
    """PENDING/RUNNING -> terminal. True for the one call that made the transition."""
    result = await session.execute(
        update(Task)
        .where(
            Task.team == key.team,
            Task.project == key.project,
            Task.task_name == key.task_name,
            Task.status.in_([TaskStatus.PENDING.value, TaskStatus.RUNNING.value]),
        )
        .values(status=status.value, finished_at=func.now())
    )
    return bool(result.rowcount == 1)  # type: ignore[attr-defined]


async def settleable_tasks(session: AsyncSession, limit: int) -> list[TaskKey]:
    """Live tasks whose counters already cover every subtask (a lost or crashed settle)."""
    rows = (
        await session.execute(
            select(Task.team, Task.project, Task.task_name)
            .join(
                TaskCounter,
                (TaskCounter.team == Task.team)
                & (TaskCounter.project == Task.project)
                & (TaskCounter.task_name == Task.task_name),
            )
            .where(
                Task.status.in_([TaskStatus.PENDING.value, TaskStatus.RUNNING.value]),
                Task.total_subtasks > 0,
            )
            .group_by(Task.team, Task.project, Task.task_name, Task.total_subtasks)
            .having(func.sum(TaskCounter.succeeded + TaskCounter.failed) >= Task.total_subtasks)
            .limit(limit)
        )
    ).all()
    return [TaskKey(r[0], r[1], r[2]) for r in rows]
