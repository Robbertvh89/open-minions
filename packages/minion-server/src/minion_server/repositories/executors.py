"""Executor persistence. SQL lives here only."""

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from minion_common.enums import ExecutorStatus
from sqlalchemy import Numeric, Select, and_, case, func, select, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.db.models import (
    Executor,
    ExecutorHeartbeat,
    ExecutorPreference,
    SubtaskAcquirement,
)


@dataclass(frozen=True)
class ExecutorIdentity:
    executor_id: uuid.UUID
    owner: str
    pool: str
    status: ExecutorStatus
    token_expires_at: datetime | None
    token_hash: str


def token_match(token_hash: str) -> list[Any]:
    """Extra WHERE terms: the row must still carry the token the caller authenticated with (a
    rotation inside the auth-cache TTL changes it). Required: there is no unchecked variant."""
    return [Executor.token_hash == token_hash]


async def list_active_identities(session: AsyncSession, limit: int) -> list[ExecutorIdentity]:
    """Non-revoked, non-expired executors (token-cache preload at startup)."""
    rows = (
        await session.execute(
            select(
                Executor.executor_id,
                Executor.owner,
                Executor.pool,
                Executor.status,
                Executor.token_expires_at,
                Executor.token_hash,
            )
            .where(
                Executor.status != ExecutorStatus.REVOKED.value,
                (Executor.token_expires_at.is_(None)) | (Executor.token_expires_at > func.now()),
            )
            .order_by(Executor.last_seen_at.desc().nulls_last())
            .limit(limit)
        )
    ).all()
    return [ExecutorIdentity(r[0], r[1], r[2], ExecutorStatus(r[3]), r[4], r[5]) for r in rows]


async def get_by_token_hash(session: AsyncSession, token_hash: str) -> ExecutorIdentity | None:
    row = (
        await session.execute(
            select(
                Executor.executor_id,
                Executor.owner,
                Executor.pool,
                Executor.status,
                Executor.token_expires_at,
                Executor.token_hash,
            ).where(Executor.token_hash == token_hash)
        )
    ).one_or_none()
    if row is None:
        return None
    return ExecutorIdentity(row[0], row[1], row[2], ExecutorStatus(row[3]), row[4], row[5])


async def insert_unregistered(
    session: AsyncSession,
    *,
    token_hash: str,
    owner: str,
    pool: str,
    token_expires_at: datetime,
) -> uuid.UUID:
    executor_id = uuid.uuid4()
    session.add(
        Executor(
            executor_id=executor_id,
            token_hash=token_hash,
            hostname="(unregistered)",
            owner=owner,
            pool=pool,
            token_expires_at=token_expires_at,
        )
    )
    await session.flush()
    return executor_id


async def revoke(session: AsyncSession, executor_id: uuid.UUID) -> bool:
    """Mark the executor REVOKED; returns False if it does not exist."""
    result = await session.execute(
        update(Executor)
        .where(Executor.executor_id == executor_id)
        .values(status=ExecutorStatus.REVOKED.value)
    )
    return bool(result.rowcount)  # type: ignore[attr-defined]


async def register(
    session: AsyncSession,
    executor_id: uuid.UUID,
    *,
    hostname: str,
    chip: str,
    total_memory_gb: float,
    os_version: str,
    engines: list[str],
    profile: str,
    budget: dict[str, Any],
    token_hash: str,
) -> bool:
    """Record host capabilities. A REVOKED executor is never touched (cannot un-revoke itself)."""
    result = await session.execute(
        update(Executor)
        .where(
            Executor.executor_id == executor_id,
            Executor.status != ExecutorStatus.REVOKED.value,
            *token_match(token_hash),
        )
        .values(
            hostname=hostname,
            chip=chip,
            total_memory_gb=total_memory_gb,
            os_version=os_version,
            engines=engines,
            profile=profile,
            budget=budget,
            last_seen_at=func.now(),
        )
    )
    return bool(result.rowcount)  # type: ignore[attr-defined]


async def touch(
    session: AsyncSession,
    executor_id: uuid.UUID,
    *,
    engines: list[str],
    profile: str,
    budget: dict[str, Any],
    token_hash: str,
) -> bool:
    """Acquire-time refresh of the reported budget and liveness. False = no live executor row
    with this id and token: revoked or rotated away, the caller must refuse."""
    result = await session.execute(
        update(Executor)
        .where(
            Executor.executor_id == executor_id,
            Executor.status != ExecutorStatus.REVOKED.value,
            *token_match(token_hash),
        )
        .values(
            engines=engines,
            profile=profile,
            budget=budget,
            paused_reason=None,  # an acquiring executor is by definition not paused
            last_seen_at=func.now(),
        )
    )
    return bool(result.rowcount)  # type: ignore[attr-defined]


async def report_state(
    session: AsyncSession,
    executor_id: uuid.UUID,
    *,
    profile: str | None,
    budget: dict[str, Any] | None,
    paused_reason: str | None,
    token_hash: str,
) -> bool:
    """Heartbeat/presence-time refresh of liveness and the reported budget/pause state."""
    values: dict[str, Any] = {"last_seen_at": func.now(), "paused_reason": paused_reason}
    if profile is not None:
        values["profile"] = profile
    if budget is not None:
        values["budget"] = budget
    result = await session.execute(
        update(Executor)
        .where(
            Executor.executor_id == executor_id,
            Executor.status != ExecutorStatus.REVOKED.value,
            *token_match(token_hash),
        )
        .values(**values)
    )
    return bool(result.rowcount)  # type: ignore[attr-defined]


async def set_preference(
    session: AsyncSession,
    executor_id: uuid.UUID,
    *,
    team: str | None,
    project: str | None,
    task_name: str | None,
) -> None:
    stmt = pg_insert(ExecutorPreference).values(
        executor_id=executor_id, team=team, project=project, task_name=task_name
    )
    await session.execute(
        stmt.on_conflict_do_update(
            index_elements=[ExecutorPreference.executor_id],
            set_={
                "team": stmt.excluded.team,
                "project": stmt.excluded.project,
                "task_name": stmt.excluded.task_name,
                "updated_at": func.now(),
            },
        )
    )


async def get_preference(
    session: AsyncSession, executor_id: uuid.UUID
) -> tuple[str | None, str | None, str | None] | None:
    """`(team, project, task_name)` with None wildcards, or None if no (non-empty) preference."""
    row = (
        await session.execute(
            select(
                ExecutorPreference.team, ExecutorPreference.project, ExecutorPreference.task_name
            ).where(ExecutorPreference.executor_id == executor_id)
        )
    ).one_or_none()
    if row is None or all(v is None for v in row):
        return None
    return (row[0], row[1], row[2])


@dataclass(frozen=True)
class ExecutorSummary:
    """Operator listing row. Deliberately has no token hash."""

    executor_id: uuid.UUID
    owner: str
    pool: str
    hostname: str
    status: ExecutorStatus
    last_seen_at: datetime | None
    token_expires_at: datetime | None


async def list_all(session: AsyncSession) -> list[ExecutorSummary]:
    rows = await session.execute(
        select(
            Executor.executor_id,
            Executor.owner,
            Executor.pool,
            Executor.hostname,
            Executor.status,
            Executor.last_seen_at,
            Executor.token_expires_at,
        ).order_by(Executor.registered_at, Executor.executor_id)
    )
    return [ExecutorSummary(r[0], r[1], r[2], r[3], ExecutorStatus(r[4]), r[5], r[6]) for r in rows]


async def rotate_token(
    session: AsyncSession,
    executor_id: uuid.UUID,
    *,
    token_hash: str,
    token_expires_at: datetime,
) -> bool:
    """Replace the token hash of a non-revoked executor (same id); False if none matched."""
    result = await session.execute(
        update(Executor)
        .where(
            Executor.executor_id == executor_id,
            Executor.status != ExecutorStatus.REVOKED.value,
        )
        .values(token_hash=token_hash, token_expires_at=token_expires_at)
    )
    return bool(result.rowcount)  # type: ignore[attr-defined]


@dataclass(frozen=True)
class FleetRow:
    executor: Executor
    busy: bool
    last_heartbeat_at: datetime | None
    online: bool


def _fleet_exprs(idle_online_s: float, busy_online_s: float) -> tuple[Any, Any, Any]:
    """`(heartbeat subquery, busy expr, online expr)` shared by the page and the summary."""
    hb = (
        select(
            SubtaskAcquirement.executor_id.label("executor_id"),
            func.max(ExecutorHeartbeat.last_seen_at).label("last_hb"),
        )
        .join(
            ExecutorHeartbeat,
            ExecutorHeartbeat.acquirement_id == SubtaskAcquirement.acquirement_id,
        )
        .group_by(SubtaskAcquirement.executor_id)
        .subquery()
    )
    busy = hb.c.last_hb.is_not(None)
    seen = func.greatest(Executor.last_seen_at, hb.c.last_hb)
    online = and_(
        Executor.status == ExecutorStatus.ACTIVE.value,
        seen.is_not(None),
        seen
        >= func.now()
        - case(
            (busy, func.make_interval(0, 0, 0, 0, 0, 0, busy_online_s)),
            else_=func.make_interval(0, 0, 0, 0, 0, 0, idle_online_s),
        ),
    )
    return hb, busy, online


async def list_fleet(
    session: AsyncSession,
    *,
    idle_online_s: float,
    busy_online_s: float,
    status: ExecutorStatus | None = None,
    pool: str | None = None,
    after: tuple[datetime, uuid.UUID] | None = None,
    limit: int = 200,
) -> tuple[list[FleetRow], bool]:
    """Keyset page (`(registered_at, executor_id)`, `limit + 1` probe) of executors, flags derived.

    See ARCHITECTURE §7.

    busy: the executor owns an acquirement that still has a heartbeat row (the row exists exactly
    while the subtask is RUNNING under it). online: ACTIVE and heard from within `busy_online_s`
    (heartbeat) when busy, else within `idle_online_s` (acquire poll / presence ping). One query;
    time is evaluated in the database (`now()`), so it cannot skew against `last_seen_at`.
    """
    hb, busy, online = _fleet_exprs(idle_online_s, busy_online_s)
    stmt = select(Executor, busy, hb.c.last_hb, online).outerjoin(
        hb, hb.c.executor_id == Executor.executor_id
    )
    if status is not None:
        stmt = stmt.where(Executor.status == status.value)
    if pool is not None:
        stmt = stmt.where(Executor.pool == pool)
    if after is not None:
        stmt = stmt.where(tuple_(Executor.registered_at, Executor.executor_id) > tuple_(*after))
    rows = (
        await session.execute(
            stmt.order_by(Executor.registered_at, Executor.executor_id).limit(limit + 1)
        )
    ).all()
    out = [FleetRow(r[0], bool(r[1]), r[2], bool(r[3])) for r in rows]
    return out[:limit], len(out) > limit


@dataclass(frozen=True)
class FleetGroup:
    online: bool
    profile: str | None
    paused_reason: str | None
    count: int
    memory_gb: float


def fleet_summary_stmt(idle_online_s: float, busy_online_s: float) -> Select[Any]:
    """One GROUP BY over the whole fleet, `online` defined exactly as in `list_fleet`.

    `memory_gb` sums `budget->>'memory_gb'` only where it is a JSON number in (0, 1024]; the
    stored budget is executor-supplied, so anything else is ignored rather than cast.
    """
    hb, _busy, online = _fleet_exprs(idle_online_s, busy_online_s)
    raw = Executor.budget["memory_gb"]
    num = raw.as_string().cast(Numeric)
    # Nested CASE: Postgres may evaluate AND operands in any order, a nested THEN only after the
    # type guard passed, so the cast never sees a non-number.
    mem = case(
        (
            func.jsonb_typeof(raw) == "number",
            case((and_(num > 0, num <= 1024), num), else_=0),
        ),
        else_=0,
    )
    on = online.label("online")
    return (
        select(
            on,
            Executor.profile,
            Executor.paused_reason,
            func.count().label("n"),
            func.coalesce(func.sum(mem), 0).label("gb"),
        )
        .select_from(Executor)
        .outerjoin(hb, hb.c.executor_id == Executor.executor_id)
        .group_by(on, Executor.profile, Executor.paused_reason)
    )


async def fleet_summary(
    session: AsyncSession, *, idle_online_s: float, busy_online_s: float
) -> list[FleetGroup]:
    rows = (await session.execute(fleet_summary_stmt(idle_online_s, busy_online_s))).all()
    return [
        FleetGroup(
            bool(r[0]),
            r[1].value if r[1] is not None and hasattr(r[1], "value") else r[1],
            r[2],
            int(r[3]),
            float(r[4]),
        )
        for r in rows
    ]
