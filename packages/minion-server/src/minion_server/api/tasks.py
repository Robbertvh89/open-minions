"""User-facing task endpoints (SPEC §6)."""

import re
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request
from minion_common.enums import Engine, SubtaskStatus, TaskStatus
from minion_common.schemas import (
    ERROR_CODE_PATTERN,
    SLUG_PATTERN,
    EmbeddingParams,
    ModelEntry,
    SubtaskOut,
    SubtaskPage,
    TaskCounts,
    TaskCreate,
    TaskOut,
    TaskPage,
    parse_params,
)
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.auth import KeysConfig, User, current_user, request_keys_config, resolve_pool
from minion_server.cursor import (
    InvalidCursorError,
    SubtaskCursor,
    decode_subtask_cursor,
    decode_task_cursor,
    encode_cursor,
    task_cursor_from,
)
from minion_server.db.models import Subtask, Task
from minion_server.db.session import get_session
from minion_server.errors import api_error
from minion_server.repositories import tasks as tasks_repo
from minion_server.settings import ServerSettings
from minion_server.storage import S3Storage
from minion_server.uris import InvalidUriError, parse_s3_uri

router = APIRouter(prefix="/api/v1/tasks", tags=["tasks"])

ATTEMPTS_SEGMENT = "_attempts"


def _as_prefix(uri: str) -> str:
    return uri if uri.endswith("/") else uri + "/"


def validate_uris(body: TaskCreate, settings: ServerSettings) -> tuple[str, str, str]:
    """Return `(canonical_input_uri, canonical_output_uri, input_key_prefix)`.

    Stored URIs are rebuilt as `s3://{bucket}/{key}` from the parsed key, so every spelling of
    the same prefix maps to one string (the output-prefix unique constraint relies on that).

    Keys must live under `{configured prefix}{team}/`; both URIs are treated as prefixes. Output
    may not touch the reserved `_attempts` namespace (attempt-scoped upload keys,
    ARCHITECTURE §3.3).
    """
    in_uri, out_uri = _as_prefix(body.input_uri), _as_prefix(body.output_uri)
    try:
        inp = parse_s3_uri(
            in_uri,
            bucket=settings.s3_bucket,
            prefixes=tuple(f"{p}{body.team}/" for p in settings.s3_input_prefixes),
        )
        out = parse_s3_uri(
            out_uri,
            bucket=settings.s3_bucket,
            prefixes=tuple(f"{p}{body.team}/" for p in settings.s3_output_prefixes),
        )
    except InvalidUriError as exc:
        raise api_error(422, "INVALID_URI", str(exc)) from exc
    for key in (inp.key, out.key):
        if ATTEMPTS_SEGMENT in key.split("/"):
            raise api_error(422, "INVALID_URI", f"{ATTEMPTS_SEGMENT} is a reserved path segment")
    bucket = settings.s3_bucket
    return f"s3://{bucket}/{inp.key}", f"s3://{bucket}/{out.key}", inp.key


def to_out(task: Task, counts: dict[SubtaskStatus, int]) -> TaskOut:
    return TaskOut(
        team=task.team,
        project=task.project,
        task_name=task.task_name,
        type=task.type,
        model=task.model_id,
        input_uri=task.input_uri,
        output_uri=task.output_uri,
        params=task.params,
        records_per_subtask=task.records_per_subtask,
        priority=task.priority,
        allowed_pool=task.allowed_pool,
        status=task.status,
        created_by=task.created_by,
        created_at=task.created_at,
        finished_at=task.finished_at,
        error=task.error,
        counts=TaskCounts(
            total=task.total_subtasks,
            pending=counts.get(SubtaskStatus.PENDING, 0),
            running=counts.get(SubtaskStatus.RUNNING, 0),
            succeeded=counts.get(SubtaskStatus.SUCCEEDED, 0),
            failed=counts.get(SubtaskStatus.FAILED, 0),
            cancelled=counts.get(SubtaskStatus.CANCELLED, 0),
        ),
    )


def _params_message(exc: ValidationError) -> str:
    """Field paths and reasons only; never echo submitted values."""
    parts = [f"{'.'.join(str(p) for p in e['loc']) or 'params'}: {e['msg']}" for e in exc.errors()]
    return "invalid params: " + "; ".join(parts)[:500]


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
    if body.team not in user.teams:
        raise api_error(403, "FORBIDDEN", "not a member of this team")
    pool = resolve_pool(keys_config.teams.get(body.team), body.allowed_pool)
    if pool != body.allowed_pool:
        body = body.model_copy(update={"allowed_pool": pool})
    model = models.get(body.model)
    if model is None:
        raise api_error(422, "UNKNOWN_MODEL", "model is not in the allow-list")
    if model.type != body.type:
        raise api_error(422, "MODEL_TYPE_MISMATCH", "model type does not match task type")
    try:
        params = parse_params(body.type, body.params)
    except ValidationError as exc:
        raise api_error(422, "INVALID_PARAMS", _params_message(exc)) from exc
    if (
        model.engine == Engine.OLLAMA
        and isinstance(params, EmbeddingParams)
        and not params.normalize
    ):
        # Ollama only returns L2-normalised vectors: no executor could honour this (ADR-0009).
        raise api_error(422, "INVALID_PARAMS", "model engine ollama requires normalize=true")
    in_uri, out_uri, in_key = validate_uris(body, settings)
    if not await storage.any_key(in_key):
        raise api_error(422, "EMPTY_INPUT", "no objects under the input prefix")
    try:
        async with session.begin():
            created = await tasks_repo.insert_task(
                session,
                body,
                model=model,
                input_uri=in_uri,
                output_uri=out_uri,
                created_by=user.name,
            )
            if not created:
                raise api_error(409, "TASK_EXISTS", "a task with this name already exists")
    except tasks_repo.OutputUriInUseError as exc:
        raise api_error(
            409, "OUTPUT_URI_IN_USE", "another task already writes to this output prefix"
        ) from exc
    return await _get(session, body.team, body.project, body.task_name)


async def _get(session: AsyncSession, team: str, project: str, task_name: str) -> TaskOut:
    async with session.begin():
        result = await tasks_repo.get_task(session, team, project, task_name)
    if result is None:
        raise api_error(404, "NOT_FOUND", "task not found")
    return to_out(*result)


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


@router.post("/{team}/{project}/{task_name}/cancel")
async def cancel_task(
    team: str,
    project: str,
    task_name: str,
    user: Annotated[User, Depends(current_user)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> TaskOut:
    """Lazy two-phase cancel (plan D4). Idempotent: re-cancel returns 200; a finished task 409."""
    for part in (team, project, task_name):
        if not _slug_ok(part):
            raise api_error(404, "NOT_FOUND", "task not found")
    if team not in user.teams:
        raise api_error(403, "FORBIDDEN", "not a member of this team")
    async with session.begin():
        status = await tasks_repo.cancel_task(session, team, project, task_name)
    if status is None:
        raise api_error(404, "NOT_FOUND", "task not found")
    if status != TaskStatus.CANCELLED:
        raise api_error(409, "TASK_FINISHED", "task already finished")
    # Separate transaction (D4); also heals a cancel whose second phase never ran.
    while True:  # batched: bounded row locks per transaction; also heals a re-cancel
        async with session.begin():
            n = await tasks_repo.cancel_pending_subtasks(session, team, project, task_name)
        if n < tasks_repo.CANCEL_BATCH:
            break
    return await _get(session, team, project, task_name)


def _slug_ok(value: str) -> bool:
    return re.fullmatch(SLUG_PATTERN, value) is not None
