"""Authentication dependencies: user API keys (file, plan D1) and executor tokens (DB, plan D2).

Secrets are never logged. Lookups hash with SHA-256 and compare with `hmac.compare_digest`.
"""

import hashlib
import hmac
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import yaml
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from minion_common.enums import ExecutorStatus
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.errors import api_error
from minion_server.repositories import executors as executors_repo

_bearer = HTTPBearer(auto_error=False)


def sha256_hex(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def _unauthorized() -> HTTPException:
    return api_error(401, "UNAUTHORIZED", "invalid or missing credentials")


@dataclass(frozen=True)
class User:
    name: str
    teams: tuple[str, ...]


@dataclass(frozen=True)
class ExecutorPrincipal:
    executor_id: uuid.UUID
    owner: str
    pool: str


def load_api_keys(path: str | Path) -> dict[str, User]:
    """Map sha256 hex -> User from a YAML `keys: [{name, sha256, teams}]` file."""
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    keys: dict[str, User] = {}
    for item in raw.get("keys", []):
        keys[str(item["sha256"]).lower()] = User(str(item["name"]), tuple(item.get("teams", [])))
    return keys


def lookup_user(keys: dict[str, User], presented: str) -> User | None:
    digest = sha256_hex(presented)
    found: User | None = None
    for stored, user in keys.items():  # full scan: no early exit on match
        if hmac.compare_digest(stored, digest):
            found = user
    return found


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.sessionmaker() as session:
        yield session


async def current_user(
    request: Request,
    creds: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> User:
    if creds is None:
        raise _unauthorized()
    path = request.app.state.settings.api_keys_file
    if not path:
        raise _unauthorized()
    keys: dict[str, User] | None = getattr(request.app.state, "api_keys", None)
    if keys is None:  # read each request when not cached so key rotation needs no restart
        keys = load_api_keys(path)
    user = lookup_user(keys, creds.credentials)
    if user is None:
        raise _unauthorized()
    return user


async def current_executor(
    creds: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> ExecutorPrincipal:
    if creds is None:
        raise _unauthorized()
    digest = sha256_hex(creds.credentials)
    ident = await executors_repo.get_by_token_hash(session, digest)
    # Hash lookup is by unique index; compare_digest guards the final equality check.
    if ident is None or not hmac.compare_digest(ident.token_hash, digest):
        raise _unauthorized()
    if ident.status is ExecutorStatus.REVOKED:
        raise _unauthorized()
    if ident.token_expires_at is not None and ident.token_expires_at <= datetime.now(UTC):
        raise _unauthorized()
    return ExecutorPrincipal(ident.executor_id, ident.owner, ident.pool)
