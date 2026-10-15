"""Heartbeat (SPEC §6.2, plan D2): owner-only liveness + refreshed output URL."""

import uuid
from datetime import UTC, datetime, timedelta

from minion_common.enums import TaskStatus
from minion_common.schemas import HeartbeatRequest, HeartbeatResponse
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.auth import ExecutorPrincipal
from minion_server.errors import api_error
from minion_server.repositories import completion as repo
from minion_server.repositories import executors as executors_repo
from minion_server.repositories import heartbeats as heartbeats_repo
from minion_server.services.acquisition import attempt_key
from minion_server.services.completion import stale_error
from minion_server.settings import ServerSettings
from minion_server.storage import MAX_PRESIGN_TTL_S, S3Storage
from minion_server.uris import parse_s3_uri


async def heartbeat(
    session: AsyncSession,
    principal: ExecutorPrincipal,
    acquirement_id: uuid.UUID,
    req: HeartbeatRequest,
    *,
    settings: ServerSettings,
    storage: S3Storage,
) -> HeartbeatResponse:
    async with session.begin():
        o = await repo.lock_owned(session, acquirement_id, principal.executor_id)
        if o is None:
            raise stale_error()
        # Reject (422), not clamp: a total above the subtask's record_count is a client bug.
        if req.progress is not None and req.progress.records_total > o.record_count:
            raise api_error(
                422, "INVALID_PROGRESS", "records_total exceeds the subtask's record_count"
            )
        await heartbeats_repo.beat(
            session,
            acquirement_id,
            req.progress.model_dump(mode="json") if req.progress else None,
        )
        await executors_repo.report_state(
            session,
            principal.executor_id,
            profile=req.budget.profile.value if req.budget else None,
            budget=req.budget.model_dump(mode="json") if req.budget else None,
            paused_reason=req.paused_reason,
        )
    # Presign outside the transaction: pure local signing, no lock needed.
    out = parse_s3_uri(o.task_output_uri, bucket=settings.s3_bucket, prefixes=("",))
    ttl = min(settings.presign_ttl_s, MAX_PRESIGN_TTL_S)
    url = await storage.presign_put(attempt_key(out.key, o.subtask_id, o.attempt), ttl)
    return HeartbeatResponse(
        cancel=o.task_status == TaskStatus.CANCELLED,
        output_url=url,
        expires_at=datetime.now(UTC) + timedelta(seconds=ttl),
    )
