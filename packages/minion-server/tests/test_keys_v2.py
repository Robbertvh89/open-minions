"""Keys file v2: live reload, fail-closed, expiry, operator role, pool policy (TASK-6.1)."""

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient
from minion_server.auth import (
    KeyStore,
    TeamPolicy,
    User,
    current_user,
    parse_keys_config,
    require_operator,
    resolve_pool,
)
from pydantic import ValidationError
from structlog.testing import capture_logs

K1, K2 = "key-one-secret", "key-two-secret"


def _h(k: str) -> str:
    return hashlib.sha256(k.encode()).hexdigest()


def _write(path: Path, body: str, bump: int = 0) -> None:
    import os

    path.write_text(body)
    st = path.stat()
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + bump))


def _entry(name: str, key: str, extra: str = "") -> str:
    return f"  - {{name: {name}, sha256: {_h(key)}, teams: [t1]{extra}}}\n"


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def _app(store: KeyStore) -> FastAPI:
    app = FastAPI()
    app.state.api_keys = store

    @app.get("/u")
    async def u(user: User = Depends(current_user)) -> dict[str, str]:  # noqa: B008
        return {"name": user.name}

    @app.get("/op")
    async def op(user: User = Depends(require_operator)) -> dict[str, str]:  # noqa: B008
        return {"name": user.name}

    return app


async def _status(app: FastAPI, key: str, path: str = "/u") -> int:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        return (await c.get(path, headers={"Authorization": f"Bearer {key}"})).status_code


async def test_removed_key_rejected_after_reload_interval(tmp_path: Path) -> None:
    f = tmp_path / "k.yaml"
    f.write_text("keys:\n" + _entry("a", K1) + _entry("b", K2))
    clock = Clock()
    app = _app(KeyStore(f, reload_s=5, clock=clock))
    assert await _status(app, K1) == 200
    _write(f, "keys:\n" + _entry("b", K2), bump=1_000_000)
    clock.t = 4.9
    assert await _status(app, K1) == 200  # not checked yet
    clock.t = 6.0  # reload_s + 1
    assert await _status(app, K1) == 401
    assert await _status(app, K2) == 200


async def test_malformed_rewrite_fails_closed_and_recovers(tmp_path: Path) -> None:
    f = tmp_path / "k.yaml"
    good = "keys:\n" + _entry("a", K1)
    f.write_text(good)
    clock = Clock()
    app = _app(KeyStore(f, reload_s=5, clock=clock))
    assert await _status(app, K1) == 200
    _write(f, f"keys:\n  - {{name: a, sha256: {_h(K1)}, teams: [Bad Team]}}\n", bump=1)
    clock.t = 6
    with capture_logs() as logs:
        assert await _status(app, K1) == 401
        assert await _status(app, K2) == 401
    errors = [e for e in logs if e["log_level"] == "error"]
    assert errors and errors[0]["event"] == "api_keys_reload_failed"
    assert K1 not in str(errors) and _h(K1) not in str(errors)
    clock.t = 12
    assert await _status(app, K1) == 401  # unchanged invalid file: still closed
    _write(f, good, bump=2)
    clock.t = 18
    assert await _status(app, K1) == 200


async def test_garbage_yaml_and_missing_file_fail_closed(tmp_path: Path) -> None:
    f = tmp_path / "k.yaml"
    f.write_text("keys:\n" + _entry("a", K1))
    clock = Clock()
    app = _app(KeyStore(f, reload_s=1, clock=clock))
    _write(f, "keys: [unclosed\n", bump=1)
    clock.t = 2
    assert await _status(app, K1) == 401
    f.unlink()
    clock.t = 4
    assert await _status(app, K1) == 401
    f.write_text("keys:\n" + _entry("a", K1))
    clock.t = 6
    assert await _status(app, K1) == 200


def test_first_load_failure_raises(tmp_path: Path) -> None:
    f = tmp_path / "k.yaml"
    f.write_text("keys: nope\n")
    with pytest.raises(ValidationError):
        KeyStore(f)
    with pytest.raises(OSError):
        KeyStore(tmp_path / "missing.yaml")


def test_non_regular_file_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="regular"):
        KeyStore(tmp_path)  # a directory


async def test_symlinked_file_is_read(tmp_path: Path) -> None:
    real = tmp_path / "real.yaml"
    real.write_text("keys:\n" + _entry("a", K1))
    link = tmp_path / "link.yaml"
    link.symlink_to(real)
    assert await _status(_app(KeyStore(link)), K1) == 200


async def test_expired_key_rejected(tmp_path: Path) -> None:
    f = tmp_path / "k.yaml"
    past = (datetime.now(UTC) - timedelta(seconds=5)).isoformat()
    future = (datetime.now(UTC) + timedelta(days=1)).isoformat()
    f.write_text(
        "keys:\n"
        + _entry("old", K1, f", expires_at: '{past}'")
        + _entry("new", K2, f", expires_at: '{future}'")
    )
    app = _app(KeyStore(f))
    assert await _status(app, K1) == 401
    assert await _status(app, K2) == 200


def test_naive_expiry_rejected() -> None:
    with pytest.raises(ValidationError):
        parse_keys_config("keys:\n" + _entry("a", K1, ", expires_at: '2030-01-01T00:00:00'"))


async def test_require_operator(tmp_path: Path) -> None:
    f = tmp_path / "k.yaml"
    f.write_text("keys:\n" + _entry("op", K1, ", operator: true") + _entry("user", K2))
    app = _app(KeyStore(f))
    assert await _status(app, K1, "/op") == 200
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.get("/op", headers={"Authorization": f"Bearer {K2}"})
        assert r.status_code == 403
        assert r.json()["detail"]["code"] == "OPERATOR_REQUIRED"
    assert await _status(app, "nope", "/op") == 401


def test_v1_file_parses_without_policies() -> None:
    cfg = parse_keys_config("keys:\n" + _entry("a", K1))
    assert cfg.teams == {}
    user = cfg.keys[_h(K1)]
    assert (user.operator, user.expires_at) == (False, None)


@pytest.mark.parametrize(
    "teams",
    [
        "t1: {default_pool: a, allowed_pools: [b]}",
        "t1: {allowed_pools: []}",
        "t1: {bogus: 1}",
        "Bad Team: {default_pool: a}",
    ],
)
def test_bad_team_policies_rejected(teams: str) -> None:
    with pytest.raises(ValidationError):
        parse_keys_config(f"teams:\n  {teams}\n")


def test_resolve_pool() -> None:
    both = TeamPolicy("a", frozenset({"a", "b"}))
    only_allowed = TeamPolicy(None, frozenset({"a"}))
    only_default = TeamPolicy("a", None)
    assert resolve_pool(None, None) is None
    assert resolve_pool(None, "x") == "x"
    assert resolve_pool(both, None) == "a"
    assert resolve_pool(both, "b") == "b"
    assert resolve_pool(only_default, "z") == "z"
    assert resolve_pool(only_default, None) == "a"
    for policy, req, status, code in [
        (both, "c", 403, "POOL_NOT_ALLOWED"),
        (only_allowed, None, 422, "POOL_REQUIRED"),
    ]:
        with pytest.raises(HTTPException) as ei:
            resolve_pool(policy, req)
        assert ei.value.status_code == status
        assert ei.value.detail["code"] == code  # type: ignore[index]


def test_store_team_policy_follows_reload(tmp_path: Path) -> None:
    f = tmp_path / "k.yaml"
    f.write_text("keys:\n" + _entry("a", K1) + "teams:\n  t1: {default_pool: p1}\n")
    clock = Clock()
    store = KeyStore(f, reload_s=1, clock=clock)
    pol = store.team_policy("t1")
    assert pol is not None and pol.default_pool == "p1"
    assert store.team_policy("other") is None
    _write(f, "keys:\n" + _entry("a", K1), bump=5)
    clock.t = 2
    assert store.team_policy("t1") is None
