"""Executor-facing endpoints (SPEC §6.2): register, preference, acquire."""

import asyncio
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from minion_common.schemas import (
    AcquireRequest,
    Assignment,
    CompleteRequest,
    FailRequest,
    HeartbeatRequest,
    HeartbeatResponse,
    ModelEntry,
    PreferenceRequest,
    PresenceRequest,
    RegisterRequest,
    RegisterResponse,
    ReleaseRequest,
)
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.auth import ExecutorPrincipal, current_executor
from minion_server.db.session import get_session
from minion_server.errors import api_error, busy_error
from minion_server.ratelimit import retry_after_header
from minion_server.repositories import executors as executors_repo
from minion_server.services import acquisition, completion
from minion_server.services import heartbeat as heartbeat_service
from minion_server.services import presence as presence_service
from minion_server.settings import ServerSettings
from minion_server.storage import S3Storage

router = APIRouter(prefix="/api/v1/executor", tags=["executor"])


def _settings(request: Request) -> ServerSettings:
    s: ServerSettings = request.app.state.settings
    return s


def _storage(request: Request) -> S3Storage:
    s: S3Storage = request.app.state.storage
    return s


def _models(request: Request) -> dict[str, ModelEntry]:
    m: dict[str, ModelEntry] = request.app.state.models
    return m


def _limited(op: str) -> Callable[[Request, ExecutorPrincipal], Awaitable[None]]:
    """Dependency: 429 + Retry-After when this executor called `op` too soon. Declared after
    `current_executor` (unauthenticated callers never touch the counters) and before any DB
    work. `async` on purpose: a sync dependency would run in the
    threadpool. It only sheds load: ownership is still decided by the acquirement INSERT
    (invariant 2)."""

    async def check(
        request: Request, principal: Annotated[ExecutorPrincipal, Depends(current_executor)]
    ) -> None:
        limiter = getattr(request.app.state, "limiter", None)
        wait = limiter.check(principal.executor_id, op) if limiter is not None else None
        if wait is not None:
            raise HTTPException(
                status_code=429,
                detail={"code": "TOO_MANY_REQUESTS", "message": f"{op} called too fast"},
                headers={"Retry-After": retry_after_header(wait)},
            )

    return check


@router.post("/register", dependencies=[Depends(_limited("register"))])
async def register(
    body: RegisterRequest,
    principal: Annotated[ExecutorPrincipal, Depends(current_executor)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> RegisterResponse:
    async with session.begin():
        ok = await executors_repo.register(
            session,
            principal.executor_id,
            hostname=body.hostname,
            chip=body.chip,
            total_memory_gb=body.total_memory_gb,
            os_version=body.os_version,
            engines=[e.value for e in body.engines],
            profile=body.profile.value,
            budget=body.budget.model_dump(mode="json"),
        )
    if not ok:  # revoked between auth and update
        raise api_error(401, "UNAUTHORIZED", "invalid or missing credentials")
    return RegisterResponse(executor_id=principal.executor_id)


@router.put("/preference", status_code=204, dependencies=[Depends(_limited("preference"))])
async def set_preference(
    body: PreferenceRequest,
    principal: Annotated[ExecutorPrincipal, Depends(current_executor)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> Response:
    async with session.begin():
        await executors_repo.set_preference(
            session,
            principal.executor_id,
            team=body.team,
            project=body.project,
            task_name=body.task_name,
        )
    return Response(status_code=204)


@router.post("/presence", status_code=204, dependencies=[Depends(_limited("presence"))])
async def presence(
    body: PresenceRequest,
    principal: Annotated[ExecutorPrincipal, Depends(current_executor)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> Response:
    await presence_service.report(session, principal, body)
    return Response(status_code=204)


@router.post(
    "/acquire",
    response_model=Assignment,
    responses={204: {"description": "No work"}},
    dependencies=[Depends(_limited("acquire"))],
)
async def acquire(
    request: Request,
    body: AcquireRequest,
    principal: Annotated[ExecutorPrincipal, Depends(current_executor)],
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[ServerSettings, Depends(_settings)],
    storage: Annotated[S3Storage, Depends(_storage)],
    models: Annotated[dict[str, ModelEntry], Depends(_models)],
) -> Response:
    started = time.perf_counter()
    deadline = time.monotonic() + settings.acquire_deadline_s
    metrics = getattr(request.app.state, "metrics", None)
    slots: asyncio.Semaphore | None = getattr(request.app.state, "acquire_slots", None)
    if slots is not None:
        # Non-blocking: when full, shed before any candidate work so owner operations
        # (heartbeat / complete / release) keep their connections.
        if slots.locked():
            if metrics is not None:
                metrics.acquires.labels(outcome="shed").inc()
            raise busy_error("SERVER_BUSY", "too many acquires in flight, retry later")
        await slots.acquire()  # not locked: returns without suspending
    outcome = "error"  # one of a few fixed values; a raised acquire is timed too
    try:
        assignment = await acquisition.acquire(
            session,
            principal,
            body,
            settings=settings,
            storage=storage,
            models=models,
            deadline=deadline,
        )
        outcome = "empty" if assignment is None else "assigned"
    finally:
        if slots is not None:
            slots.release()
        if metrics is not None:
            metrics.observe_acquire(outcome, time.perf_counter() - started)
    if metrics is not None:
        metrics.acquires.labels(outcome=outcome).inc()
    if assignment is None:
        return Response(status_code=204)
    return Response(content=assignment.model_dump_json(), media_type="application/json")


@router.post("/subtasks/{acquirement_id}/heartbeat")
async def heartbeat(
    acquirement_id: uuid.UUID,
    body: HeartbeatRequest,
    principal: Annotated[ExecutorPrincipal, Depends(current_executor)],
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[ServerSettings, Depends(_settings)],
    storage: Annotated[S3Storage, Depends(_storage)],
) -> HeartbeatResponse:
    return await heartbeat_service.heartbeat(
        session, principal, acquirement_id, body, settings=settings, storage=storage
    )


@router.post("/subtasks/{acquirement_id}/complete", status_code=204)
async def complete(
    acquirement_id: uuid.UUID,
    body: CompleteRequest,
    principal: Annotated[ExecutorPrincipal, Depends(current_executor)],
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[ServerSettings, Depends(_settings)],
    storage: Annotated[S3Storage, Depends(_storage)],
) -> Response:
    await completion.complete(
        session, principal, acquirement_id, body, settings=settings, storage=storage
    )
    return Response(status_code=204)


@router.post("/subtasks/{acquirement_id}/fail", status_code=204)
async def fail(
    acquirement_id: uuid.UUID,
    body: FailRequest,
    principal: Annotated[ExecutorPrincipal, Depends(current_executor)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> Response:
    await completion.fail(session, principal, acquirement_id, body)
    return Response(status_code=204)


@router.post("/subtasks/{acquirement_id}/release", status_code=204)
async def release(
    acquirement_id: uuid.UUID,
    principal: Annotated[ExecutorPrincipal, Depends(current_executor)],
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[ServerSettings, Depends(_settings)],
    body: ReleaseRequest | None = None,  # optional: executors before TASK-6.5 send no body
) -> Response:
    await completion.release(
        session,
        principal,
        acquirement_id,
        settings=settings,
        code=body.code if body is not None else None,
    )
    return Response(status_code=204)
