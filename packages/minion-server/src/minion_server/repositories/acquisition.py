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

from minion_common.enums import Engine, ExecutorStatus, SubtaskStatus, TaskStatus
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
    tuple_,
    update,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.db.models import (
    Executor,
    ExecutorHeartbeat,
    Subtask,
    SubtaskAcquirement,
    Task,
)
from minion_server.repositories.executors import token_match

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


def status_literal(status: TaskStatus | SubtaskStatus) -> ColumnElement[str]:
    """A SQL string literal (never a bind param): asyncpg's generic plans cannot prove a bound
    status matches a partial-index predicate, a literal always can. Values are enum constants."""
    value = status.value
    if not re.fullmatch(r"[A-Z_]+", value):
        raise ValueError(f"unsafe status literal: {value!r}")
    return literal_column("'" + value + "'")


def _pending_probe(key: Any, limit: int, *cols: ColumnElement[Any] | Any) -> Select[Any]:
    """The first `limit` PENDING subtasks of the task `key` (anything with team, project,
    task_name columns), ordered by subtask_id. The ORDER BY is load-bearing: without it the
    planner may seq-scan the subtasks or flatten an EXISTS into a semi-join that drives the walk
    from `minion_subtask`; with it a seq scan would need a full sort and is never chosen."""
    return (
        select(*cols)
        .where(
            Subtask.team == key.team,
            Subtask.project == key.project,
            Subtask.task_name == key.task_name,
            Subtask.status == status_literal(SubtaskStatus.PENDING),
        )
        .order_by(Subtask.subtask_id)
        .limit(literal_column(str(int(limit))))
    )


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

    A `WITH RECURSIVE` ordered walk (TASK-54, ADR-0021). Each recursion step is a `LIMIT 1`
    range scan on `ix_minion_task_live_queue ((-priority), created_at, team, project,
    task_name) WHERE live` that starts after a row-compare cursor, so no plan can read every live
    task and sort them. A step returns the next eligible live task that still has a PENDING
    subtask and adds `min(limit, its pending count)` to the running total `got`; the recursion
    stops once `got >= limit` or no such task is left. The final SELECT takes each visited
    task's first `limit` PENDING subtasks (LATERAL, ordered by subtask_id) and re-sorts.

    Order equivalence with the single-join form `ORDER BY priority DESC, created_at, team,
    project, task_name, subtask_id LIMIT k`: the task key is the primary key, so the order is
    total and the rows of a task are contiguous and ordered by subtask_id. The walk visits tasks
    in that order and stops only once the visited tasks hold >= k rows, so they contain the
    global first k rows; the outer ORDER BY + LIMIT yields exactly that list.

    Every LIMIT is a literal (a generic plan would otherwise assume 10 % of the rows). Cost:
    O(contributing tasks x 3 probes + skipped live tasks); filtered tasks cost a heap filter,
    drained tasks one probe each. The read stays unlocked; the acquirement INSERT remains the
    only arbiter (invariant 2).
    """
    if not 1 <= limit <= MAX_WINDOW:
        raise ValueError(f"limit must be in 1..{MAX_WINDOW}, got {limit}")
    lim: ColumnElement[Any] = literal_column(str(int(limit)))
    # Sentinel row: one plan shape for every step; sorts before every real task. It carries
    # NULL model_id so the final SELECT can drop it.
    walk = select(
        literal_column("-2147483647::integer").label("nprio"),
        literal_column("'-infinity'::timestamptz").label("created_at"),
        literal_column("''::varchar").label("team"),
        literal_column("''::varchar").label("project"),
        literal_column("''::varchar").label("task_name"),
        literal_column("NULL::varchar").label("model_id"),
        literal_column("NULL::text").label("output_uri"),
        literal_column("NULL::jsonb").label("params"),
        literal_column("NULL::integer").label("priority"),
        literal_column("0::bigint").label("got"),
    ).cte("walk", recursive=True)
    nprio = -Task.priority
    has_pending = _pending_probe(Task, 1, Subtask.subtask_id).scalar_subquery().is_not(None)
    # .correlate(Task): without it the count becomes an uncorrelated cartesian product.
    pending_count = (
        select(func.count())
        .select_from(_pending_probe(Task, limit, literal_column("1")).correlate(Task).subquery("c"))
        .scalar_subquery()
    )
    step = (
        select(
            nprio.label("nprio"),
            Task.created_at,
            Task.team,
            Task.project,
            Task.task_name,
            Task.model_id,
            Task.output_uri,
            Task.params,
            Task.priority,
            (walk.c.got + pending_count).label("got"),
        )
        .where(
            Task.status.in_(
                [status_literal(TaskStatus.PENDING), status_literal(TaskStatus.RUNNING)]
            ),
            Task.model_id.in_(list(model_ids)),  # removed models must not occupy the head
            Task.engine.in_([e.value for e in engines]),
            Task.model_memory_gb <= Decimal(str(memory_gb)),
            (Task.allowed_pool.is_(None)) | (Task.allowed_pool == pool),
            has_pending,
            tuple_(nprio, Task.created_at, Task.team, Task.project, Task.task_name)
            > tuple_(
                walk.c.nprio, walk.c.created_at, walk.c.team, walk.c.project, walk.c.task_name
            ),
        )
        .order_by(nprio, Task.created_at, Task.team, Task.project, Task.task_name)
        .limit(literal_column("1"))
    )
    if preference is not None:
        for col, value in zip((Task.team, Task.project, Task.task_name), preference, strict=True):
            if value is not None:
                step = step.where(col == value)
    nx = step.lateral("nx")
    walk = walk.union_all(select(*nx.c).select_from(walk).join(nx, true()).where(walk.c.got < lim))
    s = _pending_probe(
        walk.c,
        limit,
        Subtask.subtask_id,
        Subtask.input_object_key,
        Subtask.byte_start,
        Subtask.byte_end,
        Subtask.record_count,
    ).lateral("s")
    cols: list[ColumnElement[Any]] = [
        walk.c.team,
        walk.c.project,
        walk.c.task_name,
        s.c.subtask_id,
        walk.c.model_id,
        s.c.input_object_key,
        s.c.byte_start,
        s.c.byte_end,
        s.c.record_count,
        walk.c.output_uri,
        walk.c.params,
    ]
    if with_priority:
        cols.append(walk.c.priority)
    stmt = (
        select(*cols)
        .select_from(walk)
        .join(s, true())
        .where(walk.c.model_id.is_not(None))
        .order_by(
            walk.c.nprio,
            walk.c.created_at,
            walk.c.team,
            walk.c.project,
            walk.c.task_name,
            s.c.subtask_id,
        )
        .limit(lim)
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


QUERY_CANCELED = "57014"  # PostgreSQL SQLSTATE: statement_timeout (or a cancel) fired


async def set_statement_timeout(session: AsyncSession, ms: int) -> None:
    """`SET LOCAL statement_timeout` for the current transaction (a no-op after commit/rollback)."""
    await session.execute(select(func.set_config("statement_timeout", str(ms), True)))


async def lock_executor_for_claim(
    session: AsyncSession, executor_id: uuid.UUID, token_hash: str
) -> bool:
    """`SELECT ... FOR NO KEY UPDATE` on the caller's own executor row (ADR-0019): serialises this
    executor's claim rounds across workers so the open-acquirement count below cannot be raced.
    False = the row is missing, REVOKED or no longer carries `token_hash` (rotated away; required,
    never skipped): the caller answers 401.

    Deadlock-freedom depends on complete / release / reclaim NEVER writing `minion_executor`: the
    claim takes executor row -> new acquirement -> subtask -> heartbeat, owner ops take
    acquirement -> heartbeat -> subtask -> counter -> executor row (heartbeat, presence). If an
    owner op that holds an acquirement/subtask lock ever started to write the executor row, revisit
    ARCHITECTURE §4 first. NO KEY UPDATE does not conflict with the FK KEY SHARE locks."""
    status = (
        await session.execute(
            select(Executor.status)
            .where(Executor.executor_id == executor_id, *token_match(token_hash))
            .with_for_update(key_share=True)
        )
    ).scalar_one_or_none()
    return status is not None and status != ExecutorStatus.REVOKED.value


async def count_open_acquirements(session: AsyncSession, executor_id: uuid.UUID) -> int:
    """Acquirements this executor holds. A separate statement after the lock: in READ COMMITTED it
    gets a fresh snapshot and sees a claim that committed while we waited for the lock."""
    return int(
        (
            await session.execute(
                select(func.count())
                .select_from(SubtaskAcquirement)
                .where(SubtaskAcquirement.executor_id == executor_id)
            )
        ).scalar_one()
    )


def is_query_canceled(exc: BaseException) -> bool:
    """True for a statement cancelled by `statement_timeout` (asyncpg QueryCanceledError, wrapped
    by SQLAlchemy as a DBAPIError)."""
    if not isinstance(exc, DBAPIError):
        return False
    orig: Any = exc.orig
    for obj in (orig, getattr(orig, "__cause__", None)):
        if getattr(obj, "sqlstate", None) == QUERY_CANCELED:
            return True
        if getattr(obj, "pgcode", None) == QUERY_CANCELED:
            return True
    return False


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
    live = [status_literal(TaskStatus.PENDING), status_literal(TaskStatus.RUNNING)]
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
            .where(Subtask.status == status_literal(SubtaskStatus.PENDING), Task.status.in_(live)),
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
