"""Typed HTTP clients for the server REST API (SPEC §6).

Executors use :class:`ExecutorClient` only — they never touch PostgreSQL (invariant 1).
"""

from __future__ import annotations

import re
import uuid
from types import TracebackType
from typing import Self
from urllib.parse import quote, urlsplit

import httpx
from pydantic import BaseModel, SecretStr

from minion_common.schemas import (
    SLUG_PATTERN,
    AcquireRequest,
    Assignment,
    Budget,
    CompleteRequest,
    ErrorBody,
    FailRequest,
    HeartbeatRequest,
    HeartbeatResponse,
    PreferenceRequest,
    PresenceRequest,
    RegisterRequest,
    RegisterResponse,
    TaskCreate,
    TaskOut,
)

API_PREFIX = "/api/v1"
LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
DEFAULT_TIMEOUT_S = 30.0


class ApiError(Exception):
    """Non-success response from the server. Never carries request payloads."""

    def __init__(
        self, status_code: int, code: str, message: str, *, retry_after: float | None = None
    ) -> None:
        super().__init__(f"{status_code} {code}: {message}")
        self.status_code = status_code
        self.code = code
        self.message = message
        self.retry_after = retry_after  # seconds, from a numeric `Retry-After` header


class StaleAcquirementError(ApiError):
    """409: the acquirement is no longer current; the caller must discard its work."""


def require_secure_url(url: str) -> str:
    """Return `url` if it is https, or http to a loopback host; raise ValueError otherwise."""
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    if parts.scheme == "https" and host:
        return url
    if parts.scheme == "http" and host in LOCAL_HOSTS:
        return url
    shown = f"{parts.scheme}://{parts.hostname or ''}"
    raise ValueError(f"insecure or invalid server URL {shown!r}: https required (except localhost)")


def _error_from(resp: httpx.Response) -> ApiError:
    code, message = "HTTP_ERROR", resp.reason_phrase
    try:
        body = resp.json()
        detail = body.get("detail", body) if isinstance(body, dict) else None
        if isinstance(detail, dict):
            parsed = ErrorBody.model_validate(detail)
            code, message = parsed.code, parsed.message
        elif isinstance(detail, str):
            message = detail
    except ValueError:
        pass
    stale = resp.status_code == 409 and code == "STALE_ACQUIREMENT"
    cls = StaleAcquirementError if stale else ApiError
    retry_after: float | None = None
    raw = resp.headers.get("Retry-After")
    if raw is not None and re.fullmatch(r"[0-9]{1,6}", raw.strip()):  # ASCII delta-seconds only
        retry_after = float(raw.strip())
    return cls(resp.status_code, code, message, retry_after=retry_after)


class _BaseClient:
    def __init__(
        self,
        base_url: str,
        token: SecretStr,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout: float = DEFAULT_TIMEOUT_S,
    ) -> None:
        self._http = httpx.AsyncClient(
            base_url=require_secure_url(base_url).rstrip("/") + API_PREFIX,
            headers={"Authorization": f"Bearer {token.get_secret_value()}"},
            transport=transport,
            timeout=timeout,
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def _send(self, method: str, path: str, body: BaseModel | None = None) -> httpx.Response:
        if body is None:
            resp = await self._http.request(method, path)
        else:
            resp = await self._http.request(
                method,
                path,
                content=body.model_dump_json(),
                headers={"Content-Type": "application/json"},
            )
        if resp.status_code >= 400:
            raise _error_from(resp)
        return resp


class ExecutorClient(_BaseClient):
    async def register(self, req: RegisterRequest) -> RegisterResponse:
        resp = await self._send("POST", "/executor/register", req)
        return RegisterResponse.model_validate_json(resp.content)

    async def set_preference(self, pref: PreferenceRequest) -> None:
        await self._send("PUT", "/executor/preference", pref)

    async def acquire(self, req: AcquireRequest) -> Assignment | None:
        resp = await self._send("POST", "/executor/acquire", req)
        if resp.status_code == 204:
            return None
        return Assignment.model_validate_json(resp.content)

    async def presence(self, req: PresenceRequest) -> None:
        await self._send("POST", "/executor/presence", req)

    async def complete(self, acquirement_id: uuid.UUID, req: CompleteRequest) -> None:
        await self._send("POST", f"/executor/subtasks/{acquirement_id}/complete", req)

    async def fail(self, acquirement_id: uuid.UUID, req: FailRequest) -> None:
        await self._send("POST", f"/executor/subtasks/{acquirement_id}/fail", req)

    async def heartbeat(
        self, acquirement_id: uuid.UUID, req: HeartbeatRequest
    ) -> HeartbeatResponse:
        resp = await self._send("POST", f"/executor/subtasks/{acquirement_id}/heartbeat", req)
        return HeartbeatResponse.model_validate_json(resp.content)

    async def release(self, acquirement_id: uuid.UUID) -> None:
        await self._send("POST", f"/executor/subtasks/{acquirement_id}/release")


class UserClient(_BaseClient):
    async def create_task(self, req: TaskCreate) -> TaskOut:
        resp = await self._send("POST", "/tasks", req)
        return TaskOut.model_validate_json(resp.content)

    @staticmethod
    def _task_path(team: str, project: str, task_name: str) -> str:
        segments = []
        for seg in (team, project, task_name):
            if not re.fullmatch(SLUG_PATTERN, seg):
                raise ValueError(f"invalid path segment {seg!r}")
            segments.append(quote(seg, safe=""))
        return "/tasks/" + "/".join(segments)

    async def get_task(self, team: str, project: str, task_name: str) -> TaskOut:
        resp = await self._send("GET", self._task_path(team, project, task_name))
        return TaskOut.model_validate_json(resp.content)

    async def cancel_task(self, team: str, project: str, task_name: str) -> TaskOut:
        resp = await self._send("POST", self._task_path(team, project, task_name) + "/cancel")
        return TaskOut.model_validate_json(resp.content)


__all__ = [
    "ApiError",
    "Budget",
    "ExecutorClient",
    "StaleAcquirementError",
    "UserClient",
    "require_secure_url",
]
