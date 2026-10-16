"""Executor presence ping (SPEC FR-55): refresh liveness, budget and pause state."""

from minion_common.schemas import PresenceRequest
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.auth import ExecutorPrincipal
from minion_server.errors import api_error
from minion_server.repositories import executors as executors_repo


async def report(session: AsyncSession, principal: ExecutorPrincipal, req: PresenceRequest) -> None:
    """Mutates only the caller's own executor row (never a subtask), so no acquirement id."""
    async with session.begin():
        ok = await executors_repo.report_state(
            session,
            principal.executor_id,
            profile=req.budget.profile.value,
            budget=req.budget.model_dump(mode="json"),
            paused_reason=req.paused_reason,
        )
    if not ok:  # revoked between auth and update
        raise api_error(401, "UNAUTHORIZED", "invalid or missing credentials")
