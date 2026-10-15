"""User-facing task endpoints (SPEC §6)."""

import re
from typing import Annotated

from fastapi import APIRouter, Depends, Request
from minion_common.enums import SubtaskStatus, TaskStatus
from minion_common.schemas import SLUG_PATTERN, ModelEntry, TaskCounts, TaskCreate, TaskOut
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.auth import User, current_user
from minion_server.db.models import Task
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
) -> TaskOut:
    if body.team not in user.teams:
        raise api_error(403, "FORBIDDEN", "not a member of this team")
    model = models.get(body.model)
    if model is None:
        raise api_error(422, "UNKNOWN_MODEL", "model is not in the allow-list")
    if model.type != body.type:
        raise api_error(422, "MODEL_TYPE_MISMATCH", "model type does not match task type")
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
        if n == 0:
            break
    return await _get(session, team, project, task_name)


def _slug_ok(value: str) -> bool:
    return re.fullmatch(SLUG_PATTERN, value) is not None
