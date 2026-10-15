import hashlib
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from httpx import ASGITransport, AsyncClient
from minion_common.enums import ExecutorStatus
from minion_server.admin import app as admin_app
from minion_server.admin import issue_executor_token
from minion_server.auth import (
    ExecutorPrincipal,
    User,
    current_executor,
    current_user,
    load_api_keys,
)
from minion_server.db.models import Executor
from minion_server.db.session import create_sessionmaker, get_session
from minion_server.settings import ServerSettings
from pydantic import SecretStr, ValidationError
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from typer.testing import CliRunner

KEY = "k-secret-123"


def _keys_file(tmp_path: Path) -> Path:
    f = tmp_path / "keys.yaml"
    digest = hashlib.sha256(KEY.encode()).hexdigest()
    f.write_text(f"keys:\n  - name: alice\n    sha256: {digest}\n    teams: [t1]\n")
    return f


def _app(engine: AsyncEngine | None, keys: Path | None) -> FastAPI:
    app = FastAPI()
    app.state.settings = ServerSettings(
        database_url=SecretStr("postgresql+asyncpg://x@h/db"),
        s3_access_key=SecretStr("ak"),
        s3_secret_key=SecretStr("sk"),
        api_keys_file=str(keys) if keys else None,
    )
    app.state.api_keys = load_api_keys(keys) if keys else None
    if engine is not None:
        app.state.sessionmaker = create_sessionmaker(engine)

    @app.get("/u")
    async def u(user: User = Depends(current_user)) -> dict[str, object]:  # noqa: B008
        return {"name": user.name, "teams": list(user.teams)}

    @app.get("/e")
    async def e(p: ExecutorPrincipal = Depends(current_executor)) -> dict[str, str]:  # noqa: B008
        return {"id": str(p.executor_id), "pool": p.pool}

    @app.get("/begin")
    async def begin(
        p: ExecutorPrincipal = Depends(current_executor),  # noqa: B008
        session: AsyncSession = Depends(get_session),  # noqa: B008
    ) -> dict[str, str]:
        async with session.begin():  # fails if auth left a transaction open on this session
            pass
        return {"id": str(p.executor_id)}

    return app


@pytest.fixture
async def user_client(tmp_path: Path) -> AsyncIterator[AsyncClient]:
    app = _app(None, _keys_file(tmp_path))
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        yield c


async def test_api_key_valid(user_client: AsyncClient) -> None:
    r = await user_client.get("/u", headers={"Authorization": f"Bearer {KEY}"})
    assert r.status_code == 200
    assert r.json() == {"name": "alice", "teams": ["t1"]}


@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer nope"}])
async def test_api_key_invalid(user_client: AsyncClient, headers: dict[str, str]) -> None:
    r = await user_client.get("/u", headers=headers)
    assert r.status_code == 401
    assert r.json()["detail"]["code"] == "UNAUTHORIZED"


async def test_api_key_no_file_configured() -> None:
    app = _app(None, None)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.get("/u", headers={"Authorization": f"Bearer {KEY}"})
    assert r.status_code == 401


@pytest.mark.integration
async def test_executor_tokens(engine: AsyncEngine) -> None:
    sm = create_sessionmaker(engine)
    eid, token = await issue_executor_token(sm, owner="bob", pool="p1", ttl_days=1)
    app = _app(engine, None)

    async def call(tok: str | None) -> int:
        h = {"Authorization": f"Bearer {tok}"} if tok else {}
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            return (await c.get("/e", headers=h)).status_code

    assert await call(token) == 200
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.get("/begin", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 200, r.text
    assert await call("wrong") == 401
    assert await call(None) == 401

    async with sm() as s, s.begin():
        row = (await s.execute(select(Executor).where(Executor.executor_id == eid))).scalar_one()
        assert row.token_hash == hashlib.sha256(token.encode()).hexdigest()
        assert token not in row.token_hash
        assert (row.hostname, row.owner, row.pool) == ("(unregistered)", "bob", "p1")
        await s.execute(
            update(Executor)
            .where(Executor.executor_id == eid)
            .values(token_expires_at=datetime.now(UTC) - timedelta(seconds=1))
        )
    assert await call(token) == 401  # expired

    async with sm() as s, s.begin():
        await s.execute(
            update(Executor)
            .where(Executor.executor_id == eid)
            .values(token_expires_at=None, status=ExecutorStatus.REVOKED.value)
        )
    assert await call(token) == 401  # revoked

    async with sm() as s, s.begin():
        await s.execute(
            update(Executor)
            .where(Executor.executor_id == eid)
            .values(status=ExecutorStatus.ACTIVE.value)
        )
    assert await call(token) == 200  # no expiry


def test_generate_api_key_cli(tmp_path: Path) -> None:
    r = CliRunner().invoke(admin_app, ["generate-api-key", "--name", "dave", "--teams", "t1,t2"])
    assert r.exit_code == 0, r.output
    key = next(ln.split(": ", 1)[1] for ln in r.stdout.splitlines() if ln.startswith("api key"))
    assert len(key) >= 32
    digest = hashlib.sha256(key.encode()).hexdigest()
    assert f"sha256: {digest}" in r.stdout
    assert "teams: [t1, t2]" in r.stdout
    assert r.stdout.count(key) == 1  # printed once; only the hash is in the YAML entry
    f = tmp_path / "k.yaml"
    f.write_text("keys:\n" + "\n".join(ln for ln in r.stdout.splitlines() if ln.startswith(" ")))
    assert load_api_keys(f)[digest] == User("dave", ("t1", "t2"))


def test_generate_api_key_bad_teams() -> None:
    r = CliRunner().invoke(admin_app, ["generate-api-key", "--name", "x", "--teams", "Bad Team"])
    assert r.exit_code != 0


def test_ttl_days_capped() -> None:
    r = CliRunner().invoke(admin_app, ["issue-executor-token", "--owner", "o", "--ttl-days", "366"])
    assert r.exit_code != 0


H1 = "a" * 64
H2 = "b" * 64


def _two(a: tuple[str, str], b: tuple[str, str]) -> str:
    return "".join(
        f"{x}- {{name: {n}, sha256: {h}, teams: [t]}}\n"
        for x, (n, h) in (("keys:\n  ", a), ("  ", b))
    )


@pytest.mark.parametrize(
    "body",
    [
        _two(("a", H1), ("b", H1)),
        _two(("a", H1), ("a", H2)),
        "keys:\n  - {name: a, sha256: XYZ, teams: [t]}\n",
        f"keys:\n  - {{name: a, sha256: {H1.upper()}, teams: [t]}}\n",
        f"keys:\n  - {{name: a, sha256: {H1}, teams: [Bad Team]}}\n",
        f"keys:\n  - {{name: a, sha256: {H1}}}\n",
        f"keys:\n  - {{name: a, sha256: {H1}, teams: [t], extra: 1}}\n",
    ],
)
def test_api_keys_file_strict(tmp_path: Path, body: str) -> None:
    f = tmp_path / "k.yaml"
    f.write_text(body)
    with pytest.raises((ValueError, ValidationError)):
        load_api_keys(f)


def test_bad_keys_file_fails_startup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi.testclient import TestClient
    from minion_server.app import create_app

    f = tmp_path / "k.yaml"
    f.write_text("keys:\n  - {name: a, sha256: nope, teams: [t]}\n")
    cfg = ServerSettings(
        database_url=SecretStr("postgresql+asyncpg://x@h/db"),
        s3_access_key=SecretStr("ak"),
        s3_secret_key=SecretStr("sk"),
        api_keys_file=str(f),
    )
    with pytest.raises(ValidationError), TestClient(create_app(cfg)):
        pass


@pytest.mark.integration
def test_issue_token_cli(migrated_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_DATABASE_URL", migrated_url)
    monkeypatch.setenv("MINION_S3_ACCESS_KEY", "ak")
    monkeypatch.setenv("MINION_S3_SECRET_KEY", "sk")
    r = CliRunner().invoke(
        admin_app, ["issue-executor-token", "--owner", "carol", "--pool", "px", "--ttl-days", "7"]
    )
    assert r.exit_code == 0, r.output
    token = next(
        line.split(": ", 1)[1] for line in r.stdout.splitlines() if line.startswith("token")
    )
    assert len(token) >= 32


@pytest.mark.integration
def test_revoke_cli(migrated_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import asyncio
    from collections.abc import Awaitable, Callable
    from typing import Any

    from sqlalchemy.ext.asyncio import create_async_engine

    async def with_sm(fn: Callable[[Any], Awaitable[Any]]) -> Any:
        eng = create_async_engine(migrated_url)
        try:
            return await fn(create_sessionmaker(eng))
        finally:
            await eng.dispose()

    async def issue(sm: Any) -> str:
        return (await issue_executor_token(sm, owner="z", pool="p", ttl_days=1))[0]

    async def status(sm: Any) -> str:
        async with sm() as s:
            q = select(Executor.status).where(Executor.executor_id == uuid.UUID(eid))
            return str((await s.execute(q)).scalar_one())

    eid = asyncio.run(with_sm(issue))
    monkeypatch.setenv("MINION_DATABASE_URL", migrated_url)
    monkeypatch.setenv("MINION_S3_ACCESS_KEY", "ak")
    monkeypatch.setenv("MINION_S3_SECRET_KEY", "sk")
    r = CliRunner().invoke(admin_app, ["revoke-executor-token", "--executor-id", eid])
    assert r.exit_code == 0, r.output
    assert asyncio.run(with_sm(status)) == ExecutorStatus.REVOKED.value
    r = CliRunner().invoke(admin_app, ["revoke-executor-token", "--executor-id", str(uuid.uuid4())])
    assert r.exit_code == 1
