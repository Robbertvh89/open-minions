"""Watchdog reclaim persistence. SQL lives here only.

Reclaim shares the lock order of every owner op: acquirement -> heartbeat -> subtask -> task
(ARCHITECTURE §4), so it cannot deadlock against complete / fail / release / heartbeat.
"""

import json
import uuid
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from minion_common.enums import AttemptOutcome, SubtaskStatus, TaskStatus
from sqlalchemy import (
    Select,
    false,
    func,
    literal,
    select,
    tuple_,
    update,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.db.models import (
    ExecutorHeartbeat,
    Subtask,
    SubtaskAcquirement,
    SubtaskAttempt,
    Task,
)
from minion_server.repositories.completion import (
    Owned,
    attempt_row,
    budget_attempt,
    bump_counters,
    counter_shard,
    subtask_values,
    unacked_expiry,
)


def _cutoff(timeout_s: float) -> Any:
    return func.now() - func.make_interval(0, 0, 0, 0, 0, 0, timeout_s)


def stale_stmt(
    timeout_s: float, limit: int, exclude: Sequence[uuid.UUID] = ()
) -> Select[uuid.UUID]:
    """The stale-heartbeat scan; served by the `last_seen_at` index.

    `exclude` (quarantined ids) is filtered in SQL so the LIMIT fills with eligible rows."""
    stmt = select(ExecutorHeartbeat.acquirement_id).where(
        ExecutorHeartbeat.last_seen_at < _cutoff(timeout_s)
    )
    if exclude:
        stmt = stmt.where(ExecutorHeartbeat.acquirement_id.not_in(list(exclude)))
    return stmt.order_by(ExecutorHeartbeat.last_seen_at).limit(limit)


async def stale_ids(
    session: AsyncSession, timeout_s: float, limit: int, exclude: Sequence[uuid.UUID] = ()
) -> list[uuid.UUID]:
    """Unlocked scan (uses the `last_seen_at` index); each hit is re-checked under its lock."""
    rows = await session.execute(stale_stmt(timeout_s, limit, exclude))
    return list(rows.scalars())


def over_lease_stmt(
    max_lease_s: float, limit: int, exclude: Sequence[uuid.UUID] = ()
) -> Select[uuid.UUID]:
    """The max-lease scan; served by `ix_minion_subtask_acquirement_acquired_at` (0008)."""
    stmt = select(SubtaskAcquirement.acquirement_id).where(
        SubtaskAcquirement.acquired_at < _cutoff(max_lease_s)
    )
    if exclude:
        stmt = stmt.where(SubtaskAcquirement.acquirement_id.not_in(list(exclude)))
    return stmt.order_by(SubtaskAcquirement.acquired_at).limit(limit)


async def over_lease_ids(
    session: AsyncSession, max_lease_s: float, limit: int, exclude: Sequence[uuid.UUID] = ()
) -> list[uuid.UUID]:
    """Acquirements older than the max lease, however fresh their heartbeat (unlocked scan).

    Uses `ix_minion_subtask_acquirement_acquired_at` (range scan on `acquired_at`), so cost
    follows the number of expired rows, not the table size.
    """
    rows = await session.execute(over_lease_stmt(max_lease_s, limit, exclude))
    return list(rows.scalars())


# --- batched reclaim (TASK-35): one transaction per chunk, a handful of statements per chunk ----


@dataclass(frozen=True)
class ReclaimRow:
    """One locked acquirement with everything `decide` needs, read in a single statement."""

    owned: Owned
    stale: bool  # heartbeat older than the timeout (a missing heartbeat row is not stale)
    never_acked: bool  # no beat after the claim (ADR-0014)
    over_lease: bool
    budget_used: int  # earlier FAILED + EXPIRED attempts that consume budget
    unacked_used: int  # earlier free NOT_ACKNOWLEDGED expiries


@dataclass(frozen=True)
class Finish:
    """What to write for one reclaimed acquirement (same meaning as `finish_attempt`'s args)."""

    owned: Owned
    outcome: AttemptOutcome
    subtask_status: SubtaskStatus
    error: dict[str, Any] | None = None
    attempt_error: dict[str, Any] | None = None
    cap_reached: bool = False  # free NOT_ACKNOWLEDGED budget used up: logged after the commit


async def lock_batch(session: AsyncSession, ids: Sequence[uuid.UUID]) -> list[uuid.UUID]:
    """Lock the acquirements that exist and are not held by an owner op / another watchdog.

    Sorted + SKIP LOCKED: never waits, so two replicas cannot deadlock on each other.
    """
    rows = await session.execute(
        select(SubtaskAcquirement.acquirement_id)
        .where(SubtaskAcquirement.acquirement_id.in_(list(ids)))
        .order_by(SubtaskAcquirement.acquirement_id)
        .with_for_update(skip_locked=True)
    )
    return list(rows.scalars())


async def classify_locked(
    session: AsyncSession,
    ids: Sequence[uuid.UUID],
    *,
    timeout_s: float,
    max_lease_s: float | None,
) -> list[ReclaimRow]:
    """Re-check the locked acquirements in a FRESH statement (READ COMMITTED snapshot).

    A beat committed before our lock is visible here; a beat after the lock is impossible (beat
    takes the acquirement lock first).
    """
    a, s, t, h, at = (
        SubtaskAcquirement,
        Subtask,
        Task,
        ExecutorHeartbeat,
        SubtaskAttempt,
    )
    same_subtask = (
        (at.team == a.team)
        & (at.project == a.project)
        & (at.task_name == a.task_name)
        & (at.subtask_id == a.subtask_id)
    )
    budget = select(func.count()).where(same_subtask, budget_attempt()).scalar_subquery()
    unacked = select(func.count()).where(same_subtask, unacked_expiry()).scalar_subquery()
    over_lease = a.acquired_at < _cutoff(max_lease_s) if max_lease_s is not None else literal(False)
    stmt = (
        select(
            a.acquirement_id,
            a.team,
            a.project,
            a.task_name,
            a.subtask_id,
            a.executor_id,
            a.attempt,
            a.acquired_at,
            s.record_count,
            s.max_attempts,
            t.output_uri,
            t.status,
            func.coalesce(h.last_seen_at < _cutoff(timeout_s), false()).label("stale"),
            func.coalesce(h.last_seen_at == a.acquired_at, false()).label("never_acked"),
            over_lease.label("over_lease"),
            budget.label("budget_used"),
            unacked.label("unacked_used"),
        )
        .select_from(a)
        .join(
            s,
            (s.team == a.team)
            & (s.project == a.project)
            & (s.task_name == a.task_name)
            & (s.subtask_id == a.subtask_id),
        )
        .join(t, (t.team == a.team) & (t.project == a.project) & (t.task_name == a.task_name))
        .outerjoin(h, h.acquirement_id == a.acquirement_id)
        .where(a.acquirement_id.in_(list(ids)))
        .order_by(a.acquirement_id)
    )
    return [
        ReclaimRow(
            owned=Owned(
                acquirement_id=r[0],
                team=r[1],
                project=r[2],
                task_name=r[3],
                subtask_id=r[4],
                executor_id=r[5],
                attempt=r[6],
                acquired_at=r[7],
                record_count=r[8],
                max_attempts=r[9],
                task_output_uri=r[10],
                task_status=TaskStatus(r[11]),
            ),
            stale=bool(r[12]),
            never_acked=bool(r[13]),
            over_lease=bool(r[14]),
            budget_used=int(r[15]),
            unacked_used=int(r[16]),
        )
        for r in (await session.execute(stmt)).all()
    ]


async def finish_reclaimed(session: AsyncSession, items: Sequence[Finish]) -> None:
    """Write a chunk: one INSERT, one DELETE, one UPDATE per distinct outcome, counters per shard.

    Lock order as for a single row: acquirement (held) -> heartbeat (cascade) -> subtask ->
    counter shard; shards are bumped in sorted order. Caller holds every acquirement lock.
    """
    if not items:
        return
    await session.execute(
        pg_insert(SubtaskAttempt).values(
            [attempt_row(f.owned, f.outcome, f.error, f.attempt_error) for f in items]
        )
    )
    await session.execute(
        SubtaskAcquirement.__table__.delete().where(  # type: ignore[attr-defined]
            SubtaskAcquirement.acquirement_id.in_([f.owned.acquirement_id for f in items])
        )
    )
    groups: dict[tuple[str, str], list[Finish]] = defaultdict(list)
    for f in items:
        groups[(f.subtask_status.value, json.dumps(f.error, sort_keys=True))].append(f)
    for _gk, fs in sorted(groups.items()):
        await session.execute(
            update(Subtask)
            .where(
                tuple_(Subtask.team, Subtask.project, Subtask.task_name, Subtask.subtask_id).in_(
                    [
                        (f.owned.team, f.owned.project, f.owned.task_name, f.owned.subtask_id)
                        for f in fs
                    ]
                )
            )
            .values(**subtask_values(fs[0].subtask_status, fs[0].error))
        )
    shards: dict[tuple[str, str, str, int], list[Owned]] = defaultdict(list)
    for f in items:
        if f.subtask_status == SubtaskStatus.FAILED:
            o = f.owned
            shards[(o.team, o.project, o.task_name, counter_shard(o.subtask_id))].append(o)
    for _key, os_ in sorted(shards.items()):
        await bump_counters(session, os_[0], succeeded=0, failed=len(os_))
