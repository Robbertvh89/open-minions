"""User-facing task endpoints (SPEC §6). Create and cancel live in `services/tasks.py`."""

import re
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request
from minion_common.enums import SubtaskStatus, TaskStatus
from minion_common.schemas import (
    ERROR_CODE_PATTERN,
    SLUG_PATTERN,
    ModelEntry,
    SubtaskOut,
    SubtaskPage,
    TaskCreate,
    TaskOut,
    TaskPage,
)
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.auth import KeysConfig, User, current_user, request_keys_config
from minion_server.cursor import (
    InvalidCursorError,
    SubtaskCursor,
    decode_subtask_cursor,
    decode_task_cursor,
    encode_cursor,
    task_cursor_from,
)
from minion_server.db.models import Subtask
from minion_server.db.session import get_session
from minion_server.errors import api_error
from minion_server.repositories import tasks as tasks_repo
from minion_server.services import tasks as tasks_service
from minion_server.services.tasks import get_task as _get
from minion_server.services.tasks import slug_ok as _slug_ok
from minion_server.services.tasks import to_out
from minion_server.settings import ServerSettings
from minion_server.storage import S3Storage

router = APIRouter(prefix="/api/v1/tasks", tags=["tasks"])


def _settings(request: Request) -> ServerSettings:
    s: ServerSettings = request.app.state.settings
    return s


def _storage(request: Request) -> S3Storage:
    s: S3Storage = request.app.state.storage
    return s


def _models(request: Request) -> dict[str, ModelEntry]:
    m: dict[str, ModelEntry] = request.app.state.models
    return m


@router.post("", status_code=202)
async def create_task(
    body: TaskCreate,
    user: Annotated[User, Depends(current_user)],
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[ServerSettings, Depends(_settings)],
    storage: Annotated[S3Storage, Depends(_storage)],
    models: Annotated[dict[str, ModelEntry], Depends(_models)],
    keys_config: Annotated[KeysConfig, Depends(request_keys_config)],
) -> TaskOut:
    return await tasks_service.create_task(
        body, user, session, settings, storage, models, keys_config
    )


@router.post("/{team}/{project}/{task_name}/cancel")
async def cancel_task(
    team: str,
    project: str,
    task_name: str,
    user: Annotated[User, Depends(current_user)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> TaskOut:
    """Lazy two-phase cancel (plan D4). Idempotent: re-cancel returns 200; a finished task 409."""
    return await tasks_service.cancel_task(session, user, team, project, task_name)


MAX_PAGE = 100
_Limit = Annotated[int, Query(ge=1, le=MAX_PAGE)]


@router.get("")
async def list_tasks(
    user: Annotated[User, Depends(current_user)],
    session: Annotated[AsyncSession, Depends(get_session)],
    team: Annotated[str | None, Query(pattern=SLUG_PATTERN)] = None,
    project: Annotated[str | None, Query(pattern=SLUG_PATTERN)] = None,
    status: TaskStatus | None = None,
    cursor: str | None = None,
    limit: _Limit = 50,
) -> TaskPage:
    """Tasks of the caller's teams, newest first (keyset pagination)."""
    return await list_tasks_page(session, user, team, project, status, cursor, limit)


async def list_tasks_page(
    session: AsyncSession,
    user: User,
    team: str | None,
    project: str | None,
    status: TaskStatus | None,
    cursor: str | None,
    limit: int,
) -> TaskPage:
    """Shared by the REST endpoint and the web UI (raises `HTTPException` like a handler)."""
    if team is not None and team not in user.teams:
        raise api_error(403, "FORBIDDEN", "not a member of this team")
    after = None
    if cursor is not None:
        try:
            c = decode_task_cursor(cursor)
        except InvalidCursorError as exc:
            raise api_error(422, "INVALID_CURSOR", "invalid cursor") from exc
        after = (c.created_at, c.team, c.project, c.task_name)
    teams = (team,) if team is not None else tuple(user.teams)
    async with session.begin():
        rows, more = await tasks_repo.list_tasks(
            session, teams=teams, project=project, status=status, after=after, limit=limit
        )
    items = [to_out(t, counts) for t, counts in rows]
    last = rows[-1][0] if rows else None
    nxt = (
        task_cursor_from(last.created_at, last.team, last.project, last.task_name)
        if more and last is not None
        else None
    )
    return TaskPage(items=items, next_cursor=nxt)


@router.get("/{team}/{project}/{task_name}")
async def get_task(
    team: str,
    project: str,
    task_name: str,
    user: Annotated[User, Depends(current_user)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> TaskOut:
    return await get_task_for(session, user, team, project, task_name)


async def get_task_for(
    session: AsyncSession, user: User, team: str, project: str, task_name: str
) -> TaskOut:
    """Shared by the REST endpoint and the web UI (raises `HTTPException` like a handler)."""
    for part in (team, project, task_name):
        if not _slug_ok(part):
            raise api_error(404, "NOT_FOUND", "task not found")
    if team not in user.teams:
        raise api_error(403, "FORBIDDEN", "not a member of this team")
    return await _get(session, team, project, task_name)


@router.get("/{team}/{project}/{task_name}/subtasks")
async def list_subtasks(
    team: str,
    project: str,
    task_name: str,
    user: Annotated[User, Depends(current_user)],
    session: Annotated[AsyncSession, Depends(get_session)],
    status: SubtaskStatus | None = None,
    cursor: str | None = None,
    limit: _Limit = 100,
) -> SubtaskPage:
    return await list_subtasks_page(session, user, team, project, task_name, status, cursor, limit)


async def list_subtasks_page(
    session: AsyncSession,
    user: User,
    team: str,
    project: str,
    task_name: str,
    status: SubtaskStatus | None,
    cursor: str | None,
    limit: int,
) -> SubtaskPage:
    """Shared by the REST endpoint and the web UI (raises `HTTPException` like a handler)."""
    for part in (team, project, task_name):
        if not _slug_ok(part):
            raise api_error(404, "NOT_FOUND", "task not found")
    if team not in user.teams:
        raise api_error(403, "FORBIDDEN", "not a member of this team")
    after_id = None
    if cursor is not None:
        try:
            after_id = decode_subtask_cursor(cursor).subtask_id
        except InvalidCursorError as exc:
            raise api_error(422, "INVALID_CURSOR", "invalid cursor") from exc
    async with session.begin():
        if not await tasks_repo.task_exists(session, team, project, task_name):
            raise api_error(404, "NOT_FOUND", "task not found")
        rows, more = await tasks_repo.list_subtasks(
            session, team, project, task_name, status=status, after_id=after_id, limit=limit
        )
    return SubtaskPage(
        items=[_subtask_out(r) for r in rows],
        next_cursor=encode_cursor(SubtaskCursor(subtask_id=rows[-1].subtask_id)) if more else None,
    )


def _subtask_out(row: Subtask) -> SubtaskOut:
    code = row.error.get("code") if row.error else None
    ok = isinstance(code, str) and re.fullmatch(ERROR_CODE_PATTERN, code) is not None
    return SubtaskOut(
        subtask_id=row.subtask_id,
        status=row.status,
        attempt=row.attempt,
        max_attempts=row.max_attempts,
        record_count=row.record_count,
        error_code=code if ok else None,
        updated_at=row.updated_at,
    )
