"""Operator-only fleet view (SPEC §6.1, S-11: it reveals when owners are active)."""

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query, Request
from minion_common.enums import ExecutorStatus
from minion_common.schemas import SLUG_PATTERN, Budget, ExecutorOut, ExecutorPage
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.auth import User, require_operator
from minion_server.cursor import (
    ExecutorCursor,
    InvalidCursorError,
    decode_executor_cursor,
    encode_cursor,
)
from minion_server.db.session import get_session
from minion_server.errors import api_error
from minion_server.repositories import executors as executors_repo
from minion_server.settings import ServerSettings

MAX_PAGE = 500

router = APIRouter(prefix="/api/v1/executors", tags=["fleet"])


@router.get("")
async def list_executors(
    request: Request,
    _op: Annotated[User, Depends(require_operator)],
    session: Annotated[AsyncSession, Depends(get_session)],
    status: ExecutorStatus | None = None,
    pool: Annotated[str | None, Query(pattern=SLUG_PATTERN)] = None,
    cursor: str | None = None,
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE)] = 200,
) -> ExecutorPage:
    settings: ServerSettings = request.app.state.settings
    return await list_fleet_page(session, settings, status, pool, cursor, limit)


def _budget(raw: dict[str, Any] | None) -> Budget | None:
    """The stored budget is executor-supplied JSON: an unreadable one is shown as absent."""
    if raw is None:
        return None
    try:
        return Budget.model_validate(raw)
    except ValidationError:
        return None


async def list_fleet_page(
    session: AsyncSession,
    settings: ServerSettings,
    status: ExecutorStatus | None,
    pool: str | None,
    cursor: str | None,
    limit: int,
) -> ExecutorPage:
    """Shared by the REST endpoint and the web UI; the caller enforces operator-only access."""
    after = None
    if cursor is not None:
        try:
            ec = decode_executor_cursor(cursor)
            after = (ec.registered_at, ec.executor_id)
        except InvalidCursorError as exc:
            raise api_error(422, "INVALID_CURSOR", "invalid cursor") from exc
    async with session.begin():
        rows, more = await executors_repo.list_fleet(
            session,
            idle_online_s=settings.executor_online_idle_s,
            busy_online_s=settings.executor_online_busy_s,
            status=status,
            pool=pool,
            after=after,
            limit=limit,
        )
    items = [
        ExecutorOut(
            executor_id=r.executor.executor_id,
            owner=r.executor.owner,
            pool=r.executor.pool,
            hostname=r.executor.hostname,
            chip=r.executor.chip,
            total_memory_gb=float(r.executor.total_memory_gb)
            if r.executor.total_memory_gb is not None
            else None,
            os_version=r.executor.os_version,
            engines=list(r.executor.engines),
            status=r.executor.status,
            profile=r.executor.profile,
            budget=_budget(r.executor.budget),
            paused_reason=r.executor.paused_reason,
            registered_at=r.executor.registered_at,
            last_seen_at=r.executor.last_seen_at,
            last_heartbeat_at=r.last_heartbeat_at,
            online=r.online,
            busy=r.busy,
        )
        for r in rows
    ]
    nxt = (
        encode_cursor(
            ExecutorCursor(
                registered_at=rows[-1].executor.registered_at,
                executor_id=rows[-1].executor.executor_id,
            )
        )
        if more
        else None
    )
    return ExecutorPage(items=items, next_cursor=nxt)
