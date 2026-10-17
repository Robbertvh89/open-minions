"""Operator-only fleet view (SPEC §6.1, S-11: it reveals when owners are active)."""

from typing import Annotated

from fastapi import APIRouter, Depends, Request
from minion_common.schemas import ExecutorOut
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.auth import User, require_operator
from minion_server.db.session import get_session
from minion_server.repositories import executors as executors_repo
from minion_server.settings import ServerSettings

router = APIRouter(prefix="/api/v1/executors", tags=["fleet"])


@router.get("")
async def list_executors(
    request: Request,
    _op: Annotated[User, Depends(require_operator)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> list[ExecutorOut]:
    settings: ServerSettings = request.app.state.settings
    async with session.begin():
        rows = await executors_repo.list_fleet(
            session,
            idle_online_s=settings.executor_online_idle_s,
            busy_online_s=settings.executor_online_busy_s,
        )
    return [
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
            paused_reason=r.executor.paused_reason,
            registered_at=r.executor.registered_at,
            last_seen_at=r.executor.last_seen_at,
            last_heartbeat_at=r.last_heartbeat_at,
            online=r.online,
            busy=r.busy,
        )
        for r in rows
    ]
