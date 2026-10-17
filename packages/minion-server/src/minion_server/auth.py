"""Authentication dependencies: user API keys (file, plan D1) and executor tokens (DB, plan D2).

Secrets are never logged. Lookups hash with SHA-256 and compare with `hmac.compare_digest`.
"""

import hashlib
import hmac
import os
import stat
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Self

import structlog
import yaml
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from minion_common.enums import ExecutorStatus
from minion_common.schemas import SLUG_PATTERN
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StringConstraints,
    ValidationError,
    model_validator,
)

from minion_server.errors import api_error
from minion_server.repositories import executors as executors_repo

_bearer = HTTPBearer(auto_error=False)
log = structlog.get_logger()


def sha256_hex(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def _unauthorized() -> HTTPException:
    return api_error(401, "UNAUTHORIZED", "invalid or missing credentials")


@dataclass(frozen=True)
class User:
    name: str
    teams: tuple[str, ...]
    operator: bool = False
    expires_at: datetime | None = None


@dataclass(frozen=True)
class ExecutorPrincipal:
    executor_id: uuid.UUID
    owner: str
    pool: str


@dataclass(frozen=True)
class TeamPolicy:
    """Pool policy of one team (SPEC S-1). `allowed_pools` None = any pool is permitted."""

    default_pool: str | None = None
    allowed_pools: frozenset[str] | None = None


_Slug = Annotated[str, StringConstraints(pattern=SLUG_PATTERN)]
_Pool = Annotated[str, StringConstraints(min_length=1, max_length=64)]


class _KeyEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=100)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    teams: list[_Slug]
    operator: StrictBool = False
    expires_at: AwareDatetime | None = None


class _TeamEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    default_pool: _Pool | None = None
    allowed_pools: list[_Pool] | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _default_allowed(self) -> Self:
        if (
            self.default_pool is not None
            and self.allowed_pools is not None
            and self.default_pool not in self.allowed_pools
        ):
            raise ValueError("default_pool must be one of allowed_pools")
        return self


class _KeysFile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    keys: list[_KeyEntry] = []
    teams: dict[_Slug, _TeamEntry] = {}


@dataclass(frozen=True)
class KeysConfig:
    keys: dict[str, User]
    teams: dict[str, TeamPolicy]


MAX_KEYS_FILE_BYTES = 1 << 20
_FIELD_NAMES = frozenset(
    {"keys", "teams", "name", "sha256", "operator", "expires_at", "default_pool", "allowed_pools"}
)
_YamlLoader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)


class KeysFileUnsafeError(Exception):
    """The keys file (or its directory chain) is writable by someone other than root/this user."""


def _check_parents(path: Path) -> None:
    """Refuse a target whose directory chain is group/world-writable (unless sticky and root-owned).

    Docker/K8s secret mounts are root-owned and not writable by others, so they pass.
    """
    for directory in Path(os.path.realpath(path)).parents:
        st = directory.stat()
        if st.st_mode & 0o022 and not (st.st_mode & stat.S_ISVTX and st.st_uid == 0):
            raise KeysFileUnsafeError("api keys file directory is group/world-writable")


def _read_file(path: str | Path) -> bytes:
    """Read through one file descriptor after checking ownership and permissions.

    Symlinked files are allowed (secret mounts use them); only regular files are read.
    """
    _check_parents(Path(path))
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode):
            raise ValueError("api keys file is not a regular file")
        if st.st_uid not in (0, os.geteuid()):
            raise KeysFileUnsafeError("api keys file is not owned by root or the server user")
        if st.st_mode & 0o022:
            raise KeysFileUnsafeError("api keys file is group/world-writable")
        if st.st_size > MAX_KEYS_FILE_BYTES:
            raise ValueError("api keys file too large")
        with os.fdopen(fd, "rb", closefd=False) as fh:
            data = fh.read(MAX_KEYS_FILE_BYTES + 1)
    finally:
        os.close(fd)
    if len(data) > MAX_KEYS_FILE_BYTES:
        raise ValueError("api keys file too large")
    return data


def parse_keys_config(text: str) -> KeysConfig:
    raw = yaml.load(text, Loader=_YamlLoader) or {}  # noqa: S506  (safe loader)
    parsed = _KeysFile.model_validate(raw)
    keys: dict[str, User] = {}
    names: set[str] = set()
    for entry in parsed.keys:
        if entry.sha256 in keys:
            raise ValueError("duplicate api key hash in keys file")
        if entry.name in names:
            raise ValueError(f"duplicate api key name {entry.name!r} in keys file")
        names.add(entry.name)
        keys[entry.sha256] = User(entry.name, tuple(entry.teams), entry.operator, entry.expires_at)
    teams = {
        slug: TeamPolicy(
            t.default_pool, frozenset(t.allowed_pools) if t.allowed_pools is not None else None
        )
        for slug, t in parsed.teams.items()
    }
    member_teams = {t for u in keys.values() for t in u.teams}
    orphans = sorted(set(teams) - member_teams)
    if orphans:
        log.warning("api_keys_policy_for_unused_teams", count=len(orphans), teams=orphans[:20])
    return KeysConfig(keys, teams)


def load_keys_config(path: str | Path) -> KeysConfig:
    return parse_keys_config(_read_file(path).decode("utf-8"))


def load_api_keys(path: str | Path) -> dict[str, User]:
    """Map sha256 hex -> User from a strictly validated keys file (raises when malformed)."""
    return load_keys_config(path).keys


def _safe_detail(exc: Exception) -> str:
    """Error summary without input values or user-chosen keys (which could echo secrets)."""
    if isinstance(exc, ValidationError):

        def loc(parts: tuple[int | str, ...]) -> str:
            kept = [str(p) for p in parts if isinstance(p, int) or p in _FIELD_NAMES]
            return ".".join(kept) or "file"

        return "; ".join(f"{loc(e['loc'])}: {e['type']}" for e in exc.errors())[:500]
    if isinstance(exc, yaml.YAMLError):
        return "yaml syntax error"
    if isinstance(exc, KeysFileUnsafeError):
        return str(exc)
    if isinstance(exc, ValueError) and not isinstance(exc, UnicodeDecodeError):
        return str(exc)[:200]
    return type(exc).__name__


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class KeyStore:
    """Live view of the keys file (ADR-0007 amendment).

    Every `reload_s` seconds the next request reads the file (<= 1 MiB, local storage) and
    compares its sha256; changed content triggers a re-parse. A file that fails validation,
    vanishes, is unreadable or unsafe after startup fails closed: no key authenticates until a
    valid file is back. Content failures are remembered by hash (not re-parsed until the file
    changes); read failures (I/O, permissions) are retried every interval. The first load raises.
    The file must be replaced atomically (write temp, then rename); a result is discarded when
    the content changed while it was being parsed.
    """

    def __init__(
        self,
        path: str | Path,
        reload_s: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._path = Path(path)
        self._reload_s = reload_s
        self._clock = clock
        data = _read_file(self._path)
        self._digest: str | None = _digest(data)
        self._config: KeysConfig | None = parse_keys_config(data.decode("utf-8"))
        self._last_error: str | None = None
        self._next_check = clock() + reload_s

    def _fail(self, exc: Exception, digest: str | None) -> None:
        detail = _safe_detail(exc)
        if self._config is not None or self._last_error != detail:
            log.error("api_keys_reload_failed", path=str(self._path), error=detail)
        self._last_error = detail
        self._config = None
        self._digest = digest

    def _refresh(self) -> None:
        now = self._clock()
        if now < self._next_check:
            return
        self._next_check = now + self._reload_s
        try:
            data = _read_file(self._path)
        except Exception as exc:  # I/O, permission, size: transient; retry next interval
            self._fail(exc, None)
            return
        digest = _digest(data)
        if digest == self._digest:
            return  # unchanged (if invalid: stays closed, already logged)
        try:
            config = parse_keys_config(data.decode("utf-8"))
        except Exception as exc:  # content failure: remember the hash, fail closed
            self._fail(exc, digest)
            return
        try:
            again = _digest(_read_file(self._path))
        except Exception as exc:
            self._fail(exc, None)
            return
        if again != digest:
            return  # changed while parsing: discard, re-evaluate next interval
        self._digest = digest
        self._config = config
        self._last_error = None
        log.info("api_keys_reloaded", path=str(self._path), keys=len(config.keys))

    @property
    def config(self) -> KeysConfig | None:
        self._refresh()
        return self._config

    def team_policy(self, team: str) -> TeamPolicy | None:
        cfg = self.config
        return cfg.teams.get(team) if cfg else None


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
    store: KeyStore | None = getattr(request.app.state, "api_keys", None)
    cfg = store.config if store is not None else None
    if not cfg or not cfg.keys:  # no file configured / empty / invalid: fail closed
        raise _unauthorized()
    user = lookup_user(cfg.keys, creds.credentials)
    if user is None:
        raise _unauthorized()
    if user.expires_at is not None and user.expires_at <= datetime.now(UTC):
        raise _unauthorized()
    request.state.keys_config = cfg  # one snapshot per request (no second reload in handlers)
    return user


async def request_keys_config(
    request: Request, _user: Annotated[User, Depends(current_user)]
) -> KeysConfig:
    """The KeysConfig snapshot taken during authentication of this request."""
    cfg: KeysConfig | None = getattr(request.state, "keys_config", None)
    if cfg is None:
        raise _unauthorized()
    return cfg


async def require_operator(user: Annotated[User, Depends(current_user)]) -> User:
    if not user.operator:
        raise api_error(403, "OPERATOR_REQUIRED", "operator key required")
    return user


def resolve_pool(policy: TeamPolicy | None, requested: str | None) -> str | None:
    """Apply a team's pool policy to a task's `allowed_pool` (SPEC S-1)."""
    if policy is None:
        return requested
    if requested is None:
        if policy.default_pool is not None:
            return policy.default_pool
        if policy.allowed_pools is not None:
            raise api_error(422, "POOL_REQUIRED", "this team requires an explicit allowed_pool")
        return None
    if policy.allowed_pools is not None and requested not in policy.allowed_pools:
        raise api_error(403, "POOL_NOT_ALLOWED", "pool is not allowed for this team")
    return requested


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
