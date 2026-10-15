"""Executor-facing endpoints (SPEC §6.2): register, preference, acquire."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Request, Response
from minion_common.schemas import (
    AcquireRequest,
    Assignment,
    CompleteRequest,
    FailRequest,
    ModelEntry,
    PreferenceRequest,
    RegisterRequest,
    RegisterResponse,
)
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.auth import ExecutorPrincipal, current_executor
from minion_server.db.session import get_session
from minion_server.errors import api_error
from minion_server.repositories import executors as executors_repo
from minion_server.services import acquisition, completion
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


@router.post("/register")
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


@router.put("/preference", status_code=204)
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


@router.post("/acquire", response_model=Assignment, responses={204: {"description": "No work"}})
async def acquire(
    body: AcquireRequest,
    principal: Annotated[ExecutorPrincipal, Depends(current_executor)],
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[ServerSettings, Depends(_settings)],
    storage: Annotated[S3Storage, Depends(_storage)],
    models: Annotated[dict[str, ModelEntry], Depends(_models)],
) -> Response:
    assignment = await acquisition.acquire(
        session, principal, body, settings=settings, storage=storage, models=models
    )
    if assignment is None:
        return Response(status_code=204)
    return Response(content=assignment.model_dump_json(), media_type="application/json")


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
) -> Response:
    await completion.release(session, principal, acquirement_id)
    return Response(status_code=204)
