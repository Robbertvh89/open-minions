"""Complete / fail / release persistence (plan D6). SQL lives here only.

Every function runs inside the caller's open transaction. `lock_owned` takes the row lock on the
acquirement (`SELECT ... FOR UPDATE`) that serializes against the watchdog (ARCHITECTURE §4).
"""

import uuid
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from minion_common.enums import AttemptOutcome, SubtaskStatus, TaskStatus
from sqlalchemy import false, func, null, select, true, update
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


def unacked_expiry() -> Any:
    """Attempt rows that are free EXPIRED / NOT_ACKNOWLEDGED (history `error.code`)."""
    return (SubtaskAttempt.outcome == AttemptOutcome.EXPIRED.value) & (
        SubtaskAttempt.error["code"].astext == NOT_ACKNOWLEDGED
    )


def budget_attempt() -> Any:
    """Attempt rows that consume the retry budget (FAILED + EXPIRED, minus the free NOT_ACKNOWLEDGED
    expiries of ADR-0014). One definition for owner ops and the batched reclaim."""
    return SubtaskAttempt.outcome.in_(BUDGET_OUTCOMES) & ~func.coalesce(unacked_expiry(), false())


def attempt_row(
    o: "Owned",
    outcome: AttemptOutcome,
    error: dict[str, Any] | None,
    attempt_error: dict[str, Any] | None,
) -> dict[str, Any]:
    """Column values of the attempt-history row; shared by `finish_attempt` and the batched
    reclaim writer. `error` goes on both rows, `attempt_error` (a release reason) only here."""
    return {
        "team": o.team,
        "project": o.project,
        "task_name": o.task_name,
        "subtask_id": o.subtask_id,
        "attempt": o.attempt,
        "acquirement_id": o.acquirement_id,
        "executor_id": o.executor_id,
        "outcome": outcome.value,
        "started_at": o.acquired_at,
        # SQL NULL (not JSON null) when there is no error / release reason
        "error": (error if error is not None else attempt_error) or null(),
    }


def subtask_values(
    status: SubtaskStatus, error: dict[str, Any] | None, output_uri: str | None = None
) -> dict[str, Any]:
    """SET values of the subtask row after an attempt ends (shared, see `attempt_row`)."""
    values: dict[str, Any] = {
        "status": status.value,
        "error": error if error is not None else null(),
    }
    if output_uri is not None:
        values["output_uri"] = output_uri
    return values


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
                    budget_attempt(),
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
        pg_insert(SubtaskAttempt).values(**attempt_row(o, outcome, error, attempt_error))
    )
    await session.execute(
        SubtaskAcquirement.__table__.delete().where(  # type: ignore[attr-defined]
            SubtaskAcquirement.acquirement_id == o.acquirement_id
        )
    )
    await session.execute(
        update(Subtask)
        .where(
            Subtask.team == o.team,
            Subtask.project == o.project,
            Subtask.task_name == o.task_name,
            Subtask.subtask_id == o.subtask_id,
        )
        .values(**subtask_values(subtask_status, error, output_uri))
    )


def counter_shard(subtask_id: int) -> int:
    """Counter shard of a subtask (ADR-0015)."""
    return subtask_id % TASK_COUNTER_SHARDS


ShardKey = tuple[str, str, str, int]  # team, project, task_name, shard


async def bump_shard(
    session: AsyncSession,
    key: ShardKey,
    *,
    succeeded: int = 0,
    failed: int = 0,
    cancelled: int = 0,
) -> None:
    """Add to one counter shard (a single upsert). Shard locks are the last row locks of a txn."""
    team, project, task_name, shard = key
    ins = pg_insert(TaskCounter).values(
        team=team,
        project=project,
        task_name=task_name,
        shard=shard,
        succeeded=succeeded,
        failed=failed,
        cancelled=cancelled,
    )
    await session.execute(
        ins.on_conflict_do_update(
            index_elements=["team", "project", "task_name", "shard"],
            set_={
                "succeeded": TaskCounter.succeeded + ins.excluded.succeeded,
                "failed": TaskCounter.failed + ins.excluded.failed,
                "cancelled": TaskCounter.cancelled + ins.excluded.cancelled,
            },
        )
    )


async def bump_counters(
    session: AsyncSession, o: Owned, *, succeeded: int = 0, failed: int = 0, cancelled: int = 0
) -> None:
    """Add to this subtask's counter shard (upsert); the row lock is per shard, not per task."""
    await bump_shard(
        session,
        (o.team, o.project, o.task_name, counter_shard(o.subtask_id)),
        succeeded=succeeded,
        failed=failed,
        cancelled=cancelled,
    )


def shard_totals(keys: Iterable[tuple[str, str, str, int]]) -> list[tuple[ShardKey, int]]:
    """(team, project, task_name, subtask_id) rows -> [(shard key, n)] sorted by shard key."""
    counts: Counter[ShardKey] = Counter()
    for team, project, task_name, subtask_id in keys:
        counts[(team, project, task_name, counter_shard(subtask_id))] += 1
    return sorted(counts.items())


async def bump_cancelled(session: AsyncSession, keys: Iterable[tuple[str, str, str, int]]) -> None:
    """Add `cancelled` per (task, shard), in sorted shard-key order (deadlock-free)."""
    for key, n in shard_totals(keys):
        await bump_shard(session, key, cancelled=n)


@dataclass(frozen=True)
class TaskKey:
    team: str
    project: str
    task_name: str


def shard_sum(t: Any, col: Any) -> Any:
    """Correlated scalar subquery: sum of counter column `col` over the shards of task row `t`."""
    return (
        select(func.coalesce(func.sum(col), 0))
        .where(
            TaskCounter.team == t.team,
            TaskCounter.project == t.project,
            TaskCounter.task_name == t.task_name,
        )
        .scalar_subquery()
    )


async def task_counts(session: AsyncSession, key: TaskKey) -> TaskCounts | None:
    """Task status, total and the shard sums; None if the task is gone."""
    row = (
        await session.execute(
            select(
                Task.total_subtasks,
                shard_sum(Task, TaskCounter.succeeded),
                shard_sum(Task, TaskCounter.failed),
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


def settleable_stmt(limit: int) -> Any:  # Select[...]: the SQLAlchemy stubs here reject a typed row
    """The settleable-tasks select (shared with `minion-scale explain`).

    Drives from the live tasks and sums each one's counter shards in a lateral subquery (one PK
    range scan per live task), so the cost follows the live set and not the table of finished
    tasks (TASK-34: the plain join + GROUP BY let the planner scan `minion_task` by primary key and
    filter out the history). A task without counter rows sums to NULL and is not settleable.
    """
    done = (
        select(func.sum(TaskCounter.succeeded + TaskCounter.failed).label("done"))
        .where(
            TaskCounter.team == Task.team,
            TaskCounter.project == Task.project,
            TaskCounter.task_name == Task.task_name,
        )
        .correlate(Task)
        .lateral("c")
    )
    return (
        select(Task.team, Task.project, Task.task_name)
        .join(done, true())
        .where(
            Task.status.in_([TaskStatus.PENDING.value, TaskStatus.RUNNING.value]),
            Task.total_subtasks > 0,
            done.c.done >= Task.total_subtasks,
        )
        .limit(limit)
    )


async def settleable_tasks(session: AsyncSession, limit: int) -> list[TaskKey]:
    """Live tasks whose counters already cover every subtask (a lost or crashed settle)."""
    rows = (await session.execute(settleable_stmt(limit))).all()
    return [TaskKey(r[0], r[1], r[2]) for r in rows]
