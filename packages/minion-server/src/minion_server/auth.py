"""Authentication dependencies: user API keys (file, plan D1) and executor tokens (DB, plan D2).

Secrets are never logged. Lookups hash with SHA-256 and compare with `hmac.compare_digest`.
"""

import hashlib
import hmac
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import yaml
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from minion_common.enums import ExecutorStatus
from minion_common.schemas import SLUG_PATTERN
from pydantic import BaseModel, ConfigDict, Field, StringConstraints

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


class _KeyEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=100)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    teams: list[Annotated[str, StringConstraints(pattern=SLUG_PATTERN)]]


class _KeysFile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    keys: list[_KeyEntry] = []


def load_api_keys(path: str | Path) -> dict[str, User]:
    """Map sha256 hex -> User from a strictly validated YAML `keys: [{name, sha256, teams}]`.

    Called once at startup; raises on a malformed file or duplicate names/hashes.
    """
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    parsed = _KeysFile.model_validate(raw)
    keys: dict[str, User] = {}
    names: set[str] = set()
    for entry in parsed.keys:
        if entry.sha256 in keys:
            raise ValueError("duplicate api key hash in keys file")
        if entry.name in names:
            raise ValueError(f"duplicate api key name {entry.name!r} in keys file")
        names.add(entry.name)
        keys[entry.sha256] = User(entry.name, tuple(entry.teams))
    return keys


def lookup_user(keys: dict[str, User], presented: str) -> User | None:
    digest = sha256_hex(presented)
    found: User | None = None
    for stored, user in keys.items():  # full scan: no early exit on match
        if hmac.compare_digest(stored, digest):
            found = user
    return found


async def current_user(
    request: Request,
    creds: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> User:
    if creds is None:
        raise _unauthorized()
    keys: dict[str, User] | None = getattr(request.app.state, "api_keys", None)
    if not keys:  # no file configured / empty: fail closed
        raise _unauthorized()
    user = lookup_user(keys, creds.credentials)
    if user is None:
        raise _unauthorized()
    return user


async def current_executor(
    request: Request,
    creds: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> ExecutorPrincipal:
    if creds is None:
        raise _unauthorized()
    digest = sha256_hex(creds.credentials)
    # Own short-lived session: closed before the handler's session starts its transaction.
    async with request.app.state.sessionmaker() as session:
        ident = await executors_repo.get_by_token_hash(session, digest)
    # Hash lookup is by unique index; compare_digest guards the final equality check.
    if ident is None or not hmac.compare_digest(ident.token_hash, digest):
        raise _unauthorized()
    if ident.status is ExecutorStatus.REVOKED:
        raise _unauthorized()
    if ident.token_expires_at is not None and ident.token_expires_at <= datetime.now(UTC):
        raise _unauthorized()
    return ExecutorPrincipal(ident.executor_id, ident.owner, ident.pool)
