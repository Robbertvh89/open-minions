"""Task and subtask persistence for creation and splitting. SQL lives here only."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from minion_common.enums import SubtaskStatus, TaskStatus
from minion_common.schemas import ModelEntry, TaskCreate
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.db.models import Subtask, Task


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
    """Insert a SPLITTING task. Returns False if `(team, project, task_name)` already exists."""
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
        .on_conflict_do_nothing()
        .returning(Task.task_name)
    )
    return (await session.execute(stmt)).first() is not None


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


async def claim_splitting(session: AsyncSession) -> Task | None:
    """Lock one SPLITTING task (SKIP LOCKED) for the duration of the caller's transaction.

    Uses FOR NO KEY UPDATE: plain FOR UPDATE would block the subtask inserts (FK check takes
    FOR KEY SHARE on the task row) that the splitter issues from other transactions.
    """
    return (
        await session.execute(
            select(Task)
            .where(Task.status == TaskStatus.SPLITTING.value)
            .order_by(Task.created_at)
            .limit(1)
            .with_for_update(skip_locked=True, key_share=True)
        )
    ).scalar_one_or_none()


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


async def mark_split_done(session: AsyncSession, task: Task, total: int) -> None:
    await session.execute(
        update(Task)
        .where(
            Task.team == task.team,
            Task.project == task.project,
            Task.task_name == task.task_name,
        )
        .values(status=TaskStatus.PENDING.value, total_subtasks=total)
    )


async def mark_split_failed(session: AsyncSession, task: Task, error: str) -> None:
    await session.execute(
        update(Task)
        .where(
            Task.team == task.team,
            Task.project == task.project,
            Task.task_name == task.task_name,
        )
        .values(status=TaskStatus.FAILED.value, error=error, finished_at=datetime.now(UTC))
    )
