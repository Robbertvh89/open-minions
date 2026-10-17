"""Server-rendered web UI (ADR-0010): login, task list/detail, create, cancel, fleet.

Every route sits behind `require_enabled` (404 until `MINION_UI_SESSION_SECRET` is set). Pages
reuse the REST handlers' shared helpers, so team scoping and cursors behave identically. All
templates are autoescaped; executor- and splitter-supplied strings (owner, error, ...) are never
marked safe.
"""

import json
import re
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import quote, urlencode

import structlog
from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response
from jinja2 import Environment, FileSystemLoader, StrictUndefined
from minion_common.enums import ExecutorStatus, SubtaskStatus, TaskStatus, TaskType
from minion_common.schemas import SLUG_PATTERN, ExecutorOut, ModelEntry, TaskCreate
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.api import fleet as fleet_api
from minion_server.api import tasks as tasks_api
from minion_server.auth import KeysConfig, User, lookup_user, sha256_hex
from minion_server.db.session import get_session
from minion_server.errors import api_error
from minion_server.netutil import host_header_is_loopback
from minion_server.services import tasks as tasks_service
from minion_server.settings import ServerSettings
from minion_server.storage import S3Storage
from minion_server.ui import session as sess

log = structlog.get_logger()

_HERE = Path(__file__).parent
_STATIC = {"htmx.min.js": "text/javascript", "ui.css": "text/css"}
MAX_KEY_CHARS = 256  # bounds the hashing work of an anonymous login attempt
_PAGE_MAX = 100

_env = Environment(
    loader=FileSystemLoader(_HERE / "templates"),
    autoescape=True,  # never `|safe` a value that came from an executor
    undefined=StrictUndefined,
)


def _ts(value: datetime | None) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC") if value else "-"


_env.filters["ts"] = _ts


def render(template: str, http_status: int = 200, **ctx: Any) -> HTMLResponse:
    return HTMLResponse(_env.get_template(template).render(**ctx), status_code=http_status)


def error_page(status: int, message: str, user: User | None = None) -> HTMLResponse:
    return render("error.html", status, status=status, message=message, user=user, csrf=None)


async def require_enabled(request: Request) -> None:
    if getattr(request.app.state, "ui_secret", None) is None:
        raise HTTPException(404)  # same body as an unknown route
    settings = getattr(request.app.state, "settings", None)
    allowed = settings.ui_allowed_host_set if settings is not None else frozenset()
    if allowed and (request.headers.get("host") or "").lower() not in allowed:
        raise HTTPException(400, "host not allowed")


router = APIRouter(prefix="/ui", dependencies=[Depends(require_enabled)], include_in_schema=False)


def _secret(request: Request) -> bytes:
    s: bytes = request.app.state.ui_secret
    return s


def _settings(request: Request) -> ServerSettings:
    s: ServerSettings = request.app.state.settings
    return s


def _set_cookie(request: Request, response: Response, name: str, value: str, max_age: int) -> None:
    response.set_cookie(
        name,
        value,
        max_age=max_age,
        path=sess.COOKIE_PATH,
        httponly=True,
        samesite="strict",
        secure=not host_header_is_loopback(request.headers.get("host")),
    )


def _clear_cookie(response: Response, name: str) -> None:
    response.delete_cookie(name, path=sess.COOKIE_PATH, httponly=True, samesite="strict")


class UiSession:
    def __init__(self, user: User, csrf: str, keys_config: KeysConfig) -> None:
        self.user = user
        self.csrf = csrf
        self.keys_config = keys_config  # the snapshot this request authenticated against


def _user_for_kid(secret: bytes, cfg: KeysConfig, kid: str) -> User | None:
    """Resolve the cookie's key id: full constant-time scan, as `lookup_user` does."""
    found: User | None = None
    for stored, user in cfg.keys.items():
        if sess.tokens_equal(sess.key_id(secret, stored), kid):
            found = user
    return found


def _login_redirect(request: Request) -> HTTPException:
    headers = {"Location": "/ui/login"}
    status = 303
    if request.headers.get("hx-request"):
        # htmx would follow a 3xx inside the fragment; ask it to navigate instead
        headers = {"HX-Redirect": "/ui/login"}
        status = 401
    probe = Response()
    _clear_cookie(probe, sess.SESSION_COOKIE)
    headers["Set-Cookie"] = probe.headers["set-cookie"]
    return HTTPException(status, "login required", headers=headers)


async def ui_session(request: Request) -> UiSession:
    """The logged-in user, re-resolved against the live KeyStore on every request."""
    secret = _secret(request)
    now = datetime.now(UTC)
    raw = request.cookies.get(sess.SESSION_COOKIE)
    claims = (
        sess.verify_session(secret, raw, now, _settings(request).ui_session_ttl_s) if raw else None
    )
    if claims is None:
        raise _login_redirect(request)
    store = getattr(request.app.state, "api_keys", None)
    cfg: KeysConfig | None = store.config if store is not None else None
    user = _user_for_kid(secret, cfg, claims.kid) if cfg else None
    if cfg is None or user is None or (user.expires_at is not None and user.expires_at <= now):
        raise _login_redirect(request)  # key removed/expired or keys file invalid: fail closed
    return UiSession(user, sess.csrf_token(secret, claims), cfg)


Current = Annotated[UiSession, Depends(ui_session)]
DbSession = Annotated[AsyncSession, Depends(get_session)]


def _post_allowed(request: Request) -> bool:
    return sess.origin_ok(
        request.headers.get("origin"), request.headers.get("referer"), request.headers.get("host")
    )


# --- static -------------------------------------------------------------------------------------


@router.get("/static/{name}")
async def static_file(name: str) -> Response:
    media = _STATIC.get(name)  # allow-list: no path is ever built from user input
    if media is None:
        raise HTTPException(404)
    return FileResponse(_HERE / "static" / name, media_type=media)


# --- login / logout -----------------------------------------------------------------------------


@router.get("")
async def index() -> Response:
    return RedirectResponse("/ui/tasks", status_code=303)


def _login_form(request: Request, status: int = 200, error: str | None = None) -> HTMLResponse:
    secret = _secret(request)
    nonce = sess.new_login_nonce()
    response = render(
        "login.html",
        status,
        user=None,
        csrf=sess.login_csrf_token(secret, nonce),
        error=error,
    )
    _set_cookie(request, response, sess.LOGIN_COOKIE, nonce, sess.LOGIN_NONCE_TTL_S)
    return response


@router.get("/login")
async def login_page(request: Request) -> Response:
    return _login_form(request)


@router.post("/login")
async def login(
    request: Request,
    api_key: Annotated[str, Form()] = "",
    csrf: Annotated[str, Form()] = "",
) -> Response:
    secret = _secret(request)
    nonce = request.cookies.get(sess.LOGIN_COOKIE, "")
    if not nonce:
        log.info(
            "ui_login_nonce_missing", hint="cookies blocked, form expired or Secure/Path mismatch"
        )
    if (
        not _post_allowed(request)
        or not nonce
        or not sess.tokens_equal(csrf, sess.login_csrf_token(secret, nonce))
    ):
        return error_page(403, "Request rejected (CSRF check failed).")
    store = getattr(request.app.state, "api_keys", None)
    cfg = store.config if store is not None else None
    # No Form(max_length): FastAPI's 422 would echo the submitted key back.
    too_long = len(api_key) > MAX_KEY_CHARS
    user = lookup_user(cfg.keys, api_key) if cfg and cfg.keys and not too_long else None
    now = datetime.now(UTC)
    if user is None or (user.expires_at is not None and user.expires_at <= now):
        # one message for wrong, expired and removed keys; the key itself is never logged/echoed
        log.info("ui_login_failed", client=request.client.host if request.client else None)
        return _login_form(request, 401, "Invalid or expired API key.")
    ttl = _settings(request).ui_session_ttl_s
    response = RedirectResponse("/ui/tasks", status_code=303)
    _set_cookie(
        request, response, sess.SESSION_COOKIE,
        sess.issue_session(secret, sha256_hex(api_key), now, ttl), ttl,
    )  # fmt: skip
    _clear_cookie(response, sess.LOGIN_COOKIE)
    log.info("ui_login", user=user.name)
    return response


@router.post("/logout")
async def logout(request: Request, current: Current, csrf: Annotated[str, Form()] = "") -> Response:
    if not _post_allowed(request) or not sess.tokens_equal(csrf, current.csrf):
        return error_page(403, "Request rejected (CSRF check failed).", current.user)
    response = RedirectResponse("/ui/login", status_code=303)
    _clear_cookie(response, sess.SESSION_COOKIE)
    return response


# --- helpers ------------------------------------------------------------------------------------


def _message(exc: HTTPException) -> str:
    """API error `code: message` (both server-authored, never request values); autoescaped."""
    d = exc.detail
    if isinstance(d, dict) and isinstance(d.get("message"), str):
        code = d.get("code")
        return f"{code}: {d['message']}" if isinstance(code, str) else d["message"]
    return "Request failed."


def _opt(value: str | None) -> str | None:
    return value.strip() or None if value is not None else None


def _parse_limit(raw: str | None, default: int) -> int:
    if not raw:
        return default
    if not raw.isdecimal() or not 1 <= int(raw) <= _PAGE_MAX:
        raise HTTPException(422, {"message": f"limit must be 1..{_PAGE_MAX}"})
    return int(raw)


def _parse_enum[E](kind: Callable[[str], E], raw: str | None, what: str) -> E | None:
    if not raw:
        return None
    try:
        return kind(raw)
    except ValueError:
        raise HTTPException(422, {"message": f"unknown {what}"}) from None


def _query(**params: str | int | None) -> str:
    return urlencode({k: v for k, v in params.items() if v not in (None, "")})


# --- pages --------------------------------------------------------------------------------------


@router.get("/tasks")
async def task_list(
    request: Request,
    current: Current,
    session: DbSession,
    team: str | None = None,
    project: str | None = None,
    status: str | None = None,
    cursor: str | None = None,
    limit: str | None = None,
) -> Response:
    team, project, status, cursor = _opt(team), _opt(project), _opt(status), _opt(cursor)
    try:
        for slug in (team, project):
            if slug is not None and not re.fullmatch(SLUG_PATTERN, slug):
                raise HTTPException(422, {"message": "invalid team or project filter"})
        size = _parse_limit(limit, 50)
        status_enum = _parse_enum(TaskStatus, status, "status")
        page = await tasks_api.list_tasks_page(
            session, current.user, team, project, status_enum, cursor, size
        )
    except HTTPException as exc:
        return error_page(exc.status_code, _message(exc), current.user)
    next_query = (
        _query(team=team, project=project, status=status, limit=limit, cursor=page.next_cursor)
        if page.next_cursor
        else None
    )
    return render(
        "tasks.html",
        user=current.user,
        csrf=current.csrf,
        page=page,
        statuses=[s.value for s in TaskStatus],
        teams=sorted(current.user.teams),
        f_team=team or "",
        f_project=project or "",
        f_status=status or "",
        f_limit=limit or "",
        next_query=next_query,
    )


@router.get("/tasks/{team}/{project}/{task_name}")
async def task_detail(
    team: str,
    project: str,
    task_name: str,
    current: Current,
    session: DbSession,
    partial: str | None = None,
    status: str | None = None,
    cursor: str | None = None,
) -> Response:
    status, cursor = _opt(status), _opt(cursor)
    try:
        sub_status = _parse_enum(SubtaskStatus, status, "status")
        task = await tasks_api.get_task_for(session, current.user, team, project, task_name)
        subtasks = await tasks_api.list_subtasks_page(
            session, current.user, team, project, task_name, sub_status, cursor, 100
        )
    except HTTPException as exc:
        return error_page(exc.status_code, _message(exc), current.user)
    base = f"/ui/tasks/{team}/{project}/{task_name}"
    live_url = f"{base}?{_query(partial=1, status=status, cursor=cursor)}"
    ctx: dict[str, Any] = {
        "user": current.user,
        "csrf": current.csrf,
        "task": task,
        "subtasks": subtasks,
        "base": base,
        "live_url": live_url,
        "next_query": _query(status=status, cursor=subtasks.next_cursor)
        if subtasks.next_cursor
        else None,
        "f_status": status or "",
        "sub_statuses": [s.value for s in SubtaskStatus],
        "terminal": task.status
        in (TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.PARTIAL, TaskStatus.CANCELLED),
    }
    ctx["cancellable"] = not ctx["terminal"]
    return render("_task_live.html" if partial == "1" else "task.html", **ctx)


# --- create -------------------------------------------------------------------------------------

_CREATE_FIELDS = (
    "team", "project", "task_name", "type", "model", "input_uri", "output_uri", "params",
    "records_per_subtask", "priority", "allowed_pool",
)  # fmt: skip


def _models(request: Request) -> dict[str, ModelEntry]:
    m: dict[str, ModelEntry] = request.app.state.models
    return m


def _create_form(
    request: Request,
    current: UiSession,
    values: dict[str, str],
    http_status: int = 200,
    error: str | None = None,
) -> HTMLResponse:
    return render(
        "new_task.html",
        http_status,
        user=current.user,
        csrf=current.csrf,
        teams=sorted(current.user.teams),
        types=[t.value for t in TaskType],
        models=sorted((m.model_id, m.type.value) for m in _models(request).values()),
        v=values,
        error=error,
    )


@router.get("/tasks/new")
async def new_task_form(request: Request, current: Current) -> Response:
    return _create_form(
        request,
        current,
        dict.fromkeys(_CREATE_FIELDS, "")
        | {"records_per_subtask": "500", "priority": "0", "params": "{}"},
    )


def _build_create(values: dict[str, str]) -> TaskCreate:
    """The form's strings -> `TaskCreate`; errors name fields and reasons, never values."""
    params_text = values["params"].strip()
    try:
        params = json.loads(params_text) if params_text else {}
    except ValueError:
        raise api_error(422, "INVALID_PARAMS", "params is not valid JSON") from None
    if not isinstance(params, dict):
        raise api_error(422, "INVALID_PARAMS", "params must be a JSON object")
    raw: dict[str, object] = {
        k: v.strip() for k, v in values.items() if k not in ("params", "allowed_pool") and v.strip()
    }
    raw["params"] = params
    if values["allowed_pool"].strip():
        raw["allowed_pool"] = values["allowed_pool"].strip()
    try:
        return TaskCreate.model_validate(raw)
    except ValidationError as exc:
        parts = [f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors()]
        raise api_error(422, "INVALID_REQUEST", "; ".join(parts)[:500]) from None


@router.post("/tasks")
async def create_task(
    request: Request,
    current: Current,
    session: DbSession,
    csrf: Annotated[str, Form()] = "",
    team: Annotated[str, Form()] = "",
    project: Annotated[str, Form()] = "",
    task_name: Annotated[str, Form()] = "",
    type: Annotated[str, Form()] = "",
    model: Annotated[str, Form()] = "",
    input_uri: Annotated[str, Form()] = "",
    output_uri: Annotated[str, Form()] = "",
    params: Annotated[str, Form()] = "",
    records_per_subtask: Annotated[str, Form()] = "",
    priority: Annotated[str, Form()] = "",
    allowed_pool: Annotated[str, Form()] = "",
) -> Response:
    if not _post_allowed(request) or not sess.tokens_equal(csrf, current.csrf):
        return error_page(403, "Request rejected (CSRF check failed).", current.user)
    values = {
        "team": team, "project": project, "task_name": task_name, "type": type, "model": model,
        "input_uri": input_uri, "output_uri": output_uri, "params": params,
        "records_per_subtask": records_per_subtask, "priority": priority,
        "allowed_pool": allowed_pool,
    }  # fmt: skip
    storage: S3Storage = request.app.state.storage
    try:
        body = _build_create(values)
        out = await tasks_service.create_task(
            body,
            current.user,
            session,
            _settings(request),
            storage,
            _models(request),
            current.keys_config,
        )
    except HTTPException as exc:
        return _create_form(request, current, values, exc.status_code, _message(exc))
    log.info("ui_task_created", user=current.user.name, team=out.team, project=out.project)
    return RedirectResponse(_task_url(out.team, out.project, out.task_name), status_code=303)


def _task_url(team: str, project: str, task_name: str) -> str:
    return f"/ui/tasks/{quote(team)}/{quote(project)}/{quote(task_name)}"


# --- cancel -------------------------------------------------------------------------------------


@router.get("/tasks/{team}/{project}/{task_name}/cancel")
async def cancel_confirm(
    team: str, project: str, task_name: str, current: Current, session: DbSession
) -> Response:
    """Server-rendered confirm step (no JS under the CSP); the POST below does the work."""
    try:
        task = await tasks_api.get_task_for(session, current.user, team, project, task_name)
    except HTTPException as exc:
        return error_page(exc.status_code, _message(exc), current.user)
    return render(
        "cancel.html",
        user=current.user,
        csrf=current.csrf,
        task=task,
        back=_task_url(team, project, task_name),
    )


@router.post("/tasks/{team}/{project}/{task_name}/cancel")
async def cancel(
    team: str,
    project: str,
    task_name: str,
    request: Request,
    current: Current,
    session: DbSession,
    csrf: Annotated[str, Form()] = "",
) -> Response:
    if not _post_allowed(request) or not sess.tokens_equal(csrf, current.csrf):
        return error_page(403, "Request rejected (CSRF check failed).", current.user)
    try:
        await tasks_service.cancel_task(session, current.user, team, project, task_name)
    except HTTPException as exc:
        return error_page(exc.status_code, _message(exc), current.user)
    log.info("ui_task_cancelled", user=current.user.name, team=team, project=project)
    return RedirectResponse(_task_url(team, project, task_name), status_code=303)


# --- fleet (operator only, S-11) ----------------------------------------------------------------

_SUMMARY_PAGE = 500
_SUMMARY_MAX_PAGES = 20  # 10 000 executors; beyond that the summary says it is partial


def _fleet_summary(
    rows: list[ExecutorOut],
) -> tuple[list[tuple[str, int, float]], list[tuple[str, int]]]:
    """Online executors only: (profile, count, Σ budget GB) and (paused_reason, count)."""
    by_profile: dict[str, list[float]] = {}
    paused: dict[str, int] = {}
    for e in rows:
        if not e.online:
            continue
        key = e.profile.value if e.profile else "unknown"
        gb = e.budget.memory_gb if e.budget else 0.0
        count, total = by_profile.setdefault(key, [0, 0.0])
        by_profile[key] = [count + 1, total + gb]
        if e.paused_reason:
            paused[e.paused_reason] = paused.get(e.paused_reason, 0) + 1
    profiles = [(k, int(v[0]), v[1]) for k, v in sorted(by_profile.items())]
    return profiles, sorted(paused.items())


@router.get("/fleet")
async def fleet(
    request: Request,
    current: Current,
    session: DbSession,
    status: str | None = None,
    pool: str | None = None,
    cursor: str | None = None,
    limit: str | None = None,
) -> Response:
    if not current.user.operator:
        return error_page(403, "OPERATOR_REQUIRED: operator key required", current.user)
    status, pool, cursor = _opt(status), _opt(pool), _opt(cursor)
    settings = _settings(request)
    try:
        if pool is not None and not re.fullmatch(SLUG_PATTERN, pool):
            raise HTTPException(422, {"message": "invalid pool filter"})
        size = _parse_limit(limit, 50)
        status_enum = _parse_enum(ExecutorStatus, status, "status")
        page = await fleet_api.list_fleet_page(session, settings, status_enum, pool, cursor, size)
        everyone: list[ExecutorOut] = []
        nxt: str | None = None
        for _ in range(_SUMMARY_MAX_PAGES):
            chunk = await fleet_api.list_fleet_page(
                session, settings, None, None, nxt, _SUMMARY_PAGE
            )
            everyone.extend(chunk.items)
            nxt = chunk.next_cursor
            if nxt is None:
                break
    except HTTPException as exc:
        return error_page(exc.status_code, _message(exc), current.user)
    profiles, paused = _fleet_summary(everyone)
    return render(
        "fleet.html",
        user=current.user,
        csrf=current.csrf,
        page=page,
        statuses=[s.value for s in ExecutorStatus],
        f_status=status or "",
        f_pool=pool or "",
        profiles=profiles,
        paused=paused,
        online=sum(1 for e in everyone if e.online),
        total=len(everyone),
        partial=nxt is not None,
        next_query=_query(status=status, pool=pool, limit=limit, cursor=page.next_cursor)
        if page.next_cursor
        else None,
    )
