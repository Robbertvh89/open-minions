import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from httpx import ASGITransport, AsyncClient
from minion_common.enums import ExecutorStatus
from minion_server.admin import app as admin_app
from minion_server.admin import issue_executor_token
from minion_server.auth import ExecutorPrincipal, User, current_executor, current_user
from minion_server.db.models import Executor
from minion_server.db.session import create_sessionmaker
from minion_server.settings import ServerSettings
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncEngine
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
        database_url="postgresql+asyncpg://x@h/db", api_keys_file=str(keys) if keys else None
    )
    if engine is not None:
        app.state.sessionmaker = create_sessionmaker(engine)

    @app.get("/u")
    async def u(user: User = Depends(current_user)) -> dict[str, object]:  # noqa: B008
        return {"name": user.name, "teams": list(user.teams)}

    @app.get("/e")
    async def e(p: ExecutorPrincipal = Depends(current_executor)) -> dict[str, str]:  # noqa: B008
        return {"id": str(p.executor_id), "pool": p.pool}

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


def test_hash_api_key_cli() -> None:
    r = CliRunner().invoke(admin_app, ["hash-api-key", KEY])
    assert r.exit_code == 0
    assert r.stdout.strip() == hashlib.sha256(KEY.encode()).hexdigest()


@pytest.mark.integration
def test_issue_token_cli(migrated_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_DATABASE_URL", migrated_url)
    r = CliRunner().invoke(
        admin_app, ["issue-executor-token", "--owner", "carol", "--pool", "px", "--ttl-days", "7"]
    )
    assert r.exit_code == 0, r.output
    token = next(
        line.split(": ", 1)[1] for line in r.stdout.splitlines() if line.startswith("token")
    )
    assert len(token) >= 32
