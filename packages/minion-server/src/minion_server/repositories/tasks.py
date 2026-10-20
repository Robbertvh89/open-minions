"""Task and subtask persistence for creation and splitting. SQL lives here only."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from minion_common.enums import SubtaskStatus, TaskStatus
from minion_common.schemas import ModelEntry, TaskCreate
from sqlalchemy import (
    ColumnElement,
    Select,
    and_,
    func,
    or_,
    select,
    tuple_,
    union_all,
    update,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.orm.util import AliasedClass

from minion_server.db.models import SUBTASK_KEY, Subtask, SubtaskAcquirement, Task, TaskCounter
from minion_server.repositories.completion import bump_cancelled, shard_sum

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


def _count_columns(t: type[Task] | AliasedClass[Task]) -> tuple[ColumnElement[int], ...]:
    """running, succeeded, failed, cancelled of task row `t`, correlated, in the same statement.

    Reads `minion_task_counter` (<= 16 shard rows) and the task's acquirement rows (bounded by
    executors x MINION_MAX_OPEN_ACQUIREMENTS across all tasks); never `minion_subtask`.
    """

    running = (
        select(func.count())
        .select_from(SubtaskAcquirement)
        .where(
            SubtaskAcquirement.team == t.team,
            SubtaskAcquirement.project == t.project,
            SubtaskAcquirement.task_name == t.task_name,
        )
        .scalar_subquery()
    )
    return (
        running,
        shard_sum(t, TaskCounter.succeeded),
        shard_sum(t, TaskCounter.failed),
        shard_sum(t, TaskCounter.cancelled),
    )


def derive_counts(
    total: int, running: int, succeeded: int, failed: int, cancelled: int
) -> dict[SubtaskStatus, int]:
    """Per-status counts; PENDING is what is left of `total`, clamped at 0.

    Exact once the split is done (total = number of rows). While SPLITTING, or if the split
    never finished, `total` is 0 and PENDING reads 0.
    """
    pending = max(0, total - running - succeeded - failed - cancelled)
    return {
        SubtaskStatus.PENDING: pending,
        SubtaskStatus.RUNNING: running,
        SubtaskStatus.SUCCEEDED: succeeded,
        SubtaskStatus.FAILED: failed,
        SubtaskStatus.CANCELLED: cancelled,
    }


async def get_task(
    session: AsyncSession, team: str, project: str, task_name: str
) -> tuple[Task, dict[SubtaskStatus, int]] | None:
    """The task and its per-status counts, in one statement (one snapshot)."""
    row = (
        await session.execute(
            select(Task, *_count_columns(Task)).where(
                Task.team == team, Task.project == project, Task.task_name == task_name
            )
        )
    ).one_or_none()
    if row is None:
        return None
    task = row[0]
    return task, derive_counts(task.total_subtasks, *(int(n) for n in row[1:]))


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


_CANCELLABLE = (TaskStatus.SPLITTING.value, TaskStatus.PENDING.value, TaskStatus.RUNNING.value)


async def cancel_task(
    session: AsyncSession, team: str, project: str, task_name: str
) -> TaskStatus | None:
    """Txn 1 of cancel (plan D4): flip the task to CANCELLED; return its resulting status.

    Returns CANCELLED both when this call cancelled it and when it already was (idempotent),
    another terminal status if the task already finished, None if it does not exist. Only the
    task row is locked here; subtasks are cancelled in a second transaction because acquire locks
    subtask -> task and holding both orders would deadlock.
    """
    key = (Task.team == team, Task.project == project, Task.task_name == task_name)
    cancelled = (
        await session.execute(
            update(Task)
            .where(*key, Task.status.in_(_CANCELLABLE))
            .values(status=TaskStatus.CANCELLED.value, finished_at=func.now(), next_split_at=None)
            .returning(Task.task_name)
        )
    ).first()
    if cancelled is not None:
        return TaskStatus.CANCELLED
    current = (await session.execute(select(Task.status).where(*key))).scalar_one_or_none()
    return None if current is None else TaskStatus(current)


CANCEL_BATCH = 10_000


def _cancel_stmt(team: str, project: str, task_name: str) -> Any:
    """`WITH c AS MATERIALIZED (locking batch) UPDATE ... FROM c WHERE <pk> = c.<pk>`.

    The CTE is evaluated exactly once. The former `subtask_id IN (SELECT ... FOR UPDATE SKIP
    LOCKED LIMIT n)` was planned as a semi join whose subquery ran once per outer row (quadratic:
    ~60 s for 25k rows before the first ANALYZE, when row estimates are 1). Joining on the full
    primary key does not depend on statistics. PENDING is re-checked in the UPDATE.
    """
    c = (
        select(*(getattr(Subtask, k) for k in SUBTASK_KEY))
        .where(
            Subtask.team == team,
            Subtask.project == project,
            Subtask.task_name == task_name,
            Subtask.status == SubtaskStatus.PENDING.value,
        )
        .order_by(Subtask.subtask_id)
        .limit(CANCEL_BATCH)
        .with_for_update(skip_locked=True)
        .cte("c")
        .prefix_with("MATERIALIZED")
    )
    return (
        update(Subtask)
        .where(
            *(getattr(Subtask, k) == c.c[k] for k in SUBTASK_KEY),
            Subtask.status == SubtaskStatus.PENDING.value,
        )
        .values(status=SubtaskStatus.CANCELLED.value)
        .returning(Subtask.subtask_id)
    )


async def cancel_pending_subtasks(
    session: AsyncSession, team: str, project: str, task_name: str
) -> int:
    """Txn 2 of cancel: one batch (<= CANCEL_BATCH rows) PENDING -> CANCELLED; RUNNING keep owner.

    The caller loops (one transaction per call) until this returns 0 so no single transaction
    holds an unbounded number of row locks.
    """
    result = await session.execute(_cancel_stmt(team, project, task_name))
    cancelled_ids = [int(r[0]) for r in result.all()]
    # Shard locks are the last row locks of this txn, sorted (ARCHITECTURE §4).
    await bump_cancelled(session, ((team, project, task_name, i) for i in cancelled_ids))
    return len(cancelled_ids)


async def sweep_cancelled(
    session: AsyncSession, window_s: float = 3600, batch: int = CANCEL_BATCH
) -> int:
    """One batch (<= `batch` rows) of PENDING subtasks under tasks cancelled in the last `window_s`.

    A requeue that read the task status just before cancel committed can leave one behind. They
    are never acquired (candidates and `claim` both require a PENDING/RUNNING task); this
    keeps the counts consistent. Locks subtask rows only (SKIP LOCKED, bounded), so it cannot
    deadlock with acquire (subtask -> task). The caller loops, one transaction per call, until
    fewer than `batch` rows come back. Stragglers older than the window are healed only by
    re-cancelling.
    """
    recent = (
        select(Task.team, Task.project, Task.task_name)
        .where(
            Task.status == TaskStatus.CANCELLED.value,
            Task.finished_at > func.now() - func.make_interval(0, 0, 0, 0, 0, 0, window_s),
        )
        .subquery()
    )
    ids = (
        select(Subtask.team, Subtask.project, Subtask.task_name, Subtask.subtask_id)
        .where(
            Subtask.status == SubtaskStatus.PENDING.value,
            tuple_(Subtask.team, Subtask.project, Subtask.task_name).in_(
                select(recent.c.team, recent.c.project, recent.c.task_name)
            ),
        )
        .order_by(Subtask.team, Subtask.project, Subtask.task_name, Subtask.subtask_id)
        .limit(batch)
        .with_for_update(skip_locked=True)
        .cte("batch")
        .prefix_with("MATERIALIZED")  # evaluate LIMIT/SKIP LOCKED once, not per outer row
    )
    result = await session.execute(
        update(Subtask)
        .where(
            *(getattr(Subtask, k) == ids.c[k] for k in SUBTASK_KEY),
            Subtask.status == SubtaskStatus.PENDING.value,
        )
        .values(status=SubtaskStatus.CANCELLED.value)
        .returning(Subtask.team, Subtask.project, Subtask.task_name, Subtask.subtask_id)
    )
    rows = [(str(r[0]), str(r[1]), str(r[2]), int(r[3])) for r in result.all()]
    await bump_cancelled(session, rows)
    return len(rows)


def _after_predicate(team: str, after: tuple[datetime, str, str, str]) -> ColumnElement[bool]:
    """`(created_at, team, project, task_name) < after` specialised to one fixed `team`.

    Written so each per-team branch is a plain range on the (created_at, project, task_name)
    suffix of ix_minion_task_team_created instead of a mixed-column row comparison. For other
    teams the team ordering is decided by the database itself (`Task.team < cursor_team` uses
    the column's collation, the same as ORDER BY), never by Python string comparison.
    """
    created, a_team, a_project, a_name = after
    if team == a_team:
        return tuple_(Task.created_at, Task.project, Task.task_name) < tuple_(
            created, a_project, a_name
        )
    return and_(Task.created_at <= created, or_(Task.created_at < created, Task.team < a_team))


def _list_tasks_stmt(
    teams: Sequence[str],
    project: str | None,
    status: TaskStatus | None,
    after: tuple[datetime, str, str, str] | None,
    limit: int,
) -> Select[Task]:
    """The page statement: one index-served capped branch per team, merged and re-sorted."""
    # One branch per team (ix_minion_task_team_created), each capped at limit + 1: cost is
    # O(teams * limit), not O(all tasks of the caller's teams).
    uniq = list(dict.fromkeys(teams))
    branches = []
    for team in uniq:
        q = select(Task.team, Task.project, Task.task_name, Task.created_at).where(
            Task.team == team
        )
        if project is not None:
            q = q.where(Task.project == project)
        if status is not None:
            q = q.where(Task.status == status.value)
        if after is not None:
            q = q.where(_after_predicate(team, after))
        q = q.order_by(Task.created_at.desc(), Task.project.desc(), Task.task_name.desc())
        branches.append(select(q.limit(limit + 1).subquery()))
    keys = branches[0] if len(branches) == 1 else union_all(*branches)
    keys_sq = keys.subquery("keys")
    return (
        select(Task)
        .join(
            keys_sq,
            and_(
                Task.team == keys_sq.c.team,
                Task.project == keys_sq.c.project,
                Task.task_name == keys_sq.c.task_name,
            ),
        )
        .order_by(
            Task.created_at.desc(), Task.team.desc(), Task.project.desc(), Task.task_name.desc()
        )
        .limit(limit + 1)
    )


async def list_tasks(
    session: AsyncSession,
    *,
    teams: Sequence[str],
    project: str | None,
    status: TaskStatus | None,
    after: tuple[datetime, str, str, str] | None,
    limit: int,
) -> tuple[list[tuple[Task, dict[SubtaskStatus, int]]], bool]:
    """Keyset page of tasks in `teams`, newest first: `(created_at, team, project, task_name)` DESC.

    Fetches `limit + 1` rows to learn whether another page exists, with the counts
    (counters + acquirement rows, never `minion_subtask`) in the same statement.
    Returns `(items, has_more)`.
    """
    if not teams:
        return [], False
    page_sq = _list_tasks_stmt(teams, project, status, after, limit).subquery("page")
    t = aliased(Task, page_sq)
    stmt = select(t, *_count_columns(t)).order_by(
        t.created_at.desc(), t.team.desc(), t.project.desc(), t.task_name.desc()
    )
    rows = (await session.execute(stmt)).all()
    has_more = len(rows) > limit
    return [
        (r[0], derive_counts(r[0].total_subtasks, *(int(n) for n in r[1:]))) for r in rows[:limit]
    ], has_more


async def task_exists(session: AsyncSession, team: str, project: str, task_name: str) -> bool:
    return (
        await session.execute(
            select(Task.task_name).where(
                Task.team == team, Task.project == project, Task.task_name == task_name
            )
        )
    ).first() is not None


async def list_subtasks(
    session: AsyncSession,
    team: str,
    project: str,
    task_name: str,
    *,
    status: SubtaskStatus | None,
    after_id: int | None,
    limit: int,
) -> tuple[list[Subtask], bool]:
    """Keyset page of one task's subtasks ordered by `subtask_id` (primary key suffix)."""
    stmt = select(Subtask).where(
        Subtask.team == team, Subtask.project == project, Subtask.task_name == task_name
    )
    if status is not None:
        stmt = stmt.where(Subtask.status == status.value)
    if after_id is not None:
        stmt = stmt.where(Subtask.subtask_id > after_id)
    rows = list(
        (await session.execute(stmt.order_by(Subtask.subtask_id).limit(limit + 1))).scalars()
    )
    return rows[:limit], len(rows) > limit
