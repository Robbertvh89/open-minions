"""Overload shedding (TASK-7.4.4): acquire deadline before the claim, pool timeout -> 503,
acquire concurrency slots, owner-operation headroom."""

import asyncio
import re
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from minion_server.app import create_app
from minion_server.repositories import executors as executors_repo
from pydantic import SecretStr
from sqlalchemy import event, text

from conftest import S3Env, _auth, _settings, make_executors, seed_task

BASE = "/api/v1/executor"
BUDGET: dict[str, Any] = {
    "profile": "balanced",
    "memory_gb": 8.0,
    "gpu_duty_cycle": 0.5,
    "max_batch_size": 8,
}
ACQ = {"engines": ["mlx"], "budget": BUDGET}

Make = Callable[..., Awaitable[tuple[AsyncClient, Any]]]


@pytest.fixture
async def make(fresh_url: str, s3: S3Env) -> AsyncIterator[Make]:
    stack: list[Any] = []

    async def build(**over: Any) -> tuple[AsyncClient, Any]:
        cfg = _settings(fresh_url).model_copy(
            update={
                "s3_endpoint_url": s3.endpoint_url,
                "public_s3_endpoint_url": s3.endpoint_url,
                "s3_access_key": SecretStr(s3.access_key),
                "s3_secret_key": SecretStr(s3.secret_key),
                "s3_bucket": s3.bucket,
                **over,
            }
        )
        app = create_app(cfg)
        lifespan = app.router.lifespan_context(app)
        await lifespan.__aenter__()
        c = AsyncClient(transport=ASGITransport(app=app), base_url="http://t")
        stack.append((lifespan, c))
        return c, app

    yield build
    for lifespan, c in reversed(stack):
        await c.aclose()
        await lifespan.__aexit__(None, None, None)


async def _acquirements(app: Any) -> int:
    async with app.state.sessionmaker() as s:
        return int(
            (await s.execute(text("SELECT count(*) FROM minion_subtask_acquirement"))).scalar_one()
        )


def _assert_shed(r: Any, code: str) -> None:
    assert r.status_code == 503, r.text
    assert r.json()["detail"]["code"] == code
    assert re.fullmatch(r"[1-9]\d*", r.headers["Retry-After"])


def _slow_preference(monkeypatch: pytest.MonkeyPatch, seconds: float) -> None:
    real = executors_repo.get_preference

    async def slow(*a: Any, **kw: Any) -> Any:
        await asyncio.sleep(seconds)
        return await real(*a, **kw)

    monkeypatch.setattr(executors_repo, "get_preference", slow)


@pytest.mark.integration
async def test_acquire_past_deadline_commits_no_acquirement(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    c, app = await make(acquire_deadline_s=1.0)
    await seed_task(app.state.sessionmaker, "dl", 3, team="dl")
    [(_, tok)] = await make_executors(app.state.sessionmaker, 1)
    _slow_preference(monkeypatch, 1.2)  # work before the claim INSERT eats the deadline
    r = await c.post(f"{BASE}/acquire", json=ACQ, headers=_auth(tok))
    _assert_shed(r, "ACQUIRE_DEADLINE")
    assert await _acquirements(app) == 0


@pytest.mark.integration
async def test_client_gives_up_before_the_server_finishes_leaves_no_ghost_acquirement(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    c, app = await make(acquire_deadline_s=1.0)
    await seed_task(app.state.sessionmaker, "gh", 3, team="gh")
    [(_, tok)] = await make_executors(app.state.sessionmaker, 1)
    _slow_preference(monkeypatch, 1.2)
    server_side = asyncio.ensure_future(c.post(f"{BASE}/acquire", json=ACQ, headers=_auth(tok)))
    with pytest.raises(TimeoutError):  # the client's own (shorter) timeout fires
        await asyncio.wait_for(asyncio.shield(server_side), 0.3)
    r = await server_side  # the server keeps working after the client left
    _assert_shed(r, "ACQUIRE_DEADLINE")
    assert await _acquirements(app) == 0


@pytest.mark.integration
async def test_pool_exhaustion_is_503_with_retry_after_never_500(make: Make) -> None:
    c, app = await make(db_pool_size=1, db_max_overflow=0, db_pool_timeout_s=0.5)
    [(_, tok)] = await make_executors(app.state.sessionmaker, 1)
    async with app.state.engine.connect() as held:  # the only connection
        await held.execute(text("SELECT 1"))
        for path, body in [("acquire", ACQ), ("presence", {"budget": BUDGET})]:
            r = await c.post(f"{BASE}/{path}", json=body, headers=_auth(tok))
            _assert_shed(r, "SERVER_BUSY")
    r = await c.post(f"{BASE}/presence", json={"budget": BUDGET}, headers=_auth(tok))
    assert r.status_code == 204  # recovered once the connection is back


@pytest.mark.integration
async def test_full_acquire_slots_shed_before_any_candidate_work(make: Make) -> None:
    c, app = await make(acquire_concurrency=1)
    sm = app.state.sessionmaker
    await seed_task(sm, "sem", 3, team="sem")
    [(_, tok)] = await make_executors(sm, 1)
    seen: list[str] = []

    def record(conn: Any, cur: Any, statement: str, *a: Any) -> None:
        seen.append(statement)

    await app.state.acquire_slots.acquire()  # another acquire is in flight
    event.listen(app.state.engine.sync_engine, "before_cursor_execute", record)
    # auth runs before the shed (a token lookup); mark where the acquire work would start
    seen.clear()
    r = await c.post(f"{BASE}/acquire", json=ACQ, headers=_auth(tok))
    _assert_shed(r, "SERVER_BUSY")
    assert not [
        q
        for q in seen
        if "minion_subtask" in q or ("minion_executor " in q.split("WHERE")[0] and "UPDATE" in q)
    ]
    assert await _acquirements(app) == 0
    app.state.acquire_slots.release()
    r = await c.post(f"{BASE}/acquire", json=ACQ, headers=_auth(tok))
    assert r.status_code == 200, r.text
    assert app.state.acquire_slots._value == 1  # the slot is returned after the call


@pytest.mark.integration
async def test_heartbeat_succeeds_while_acquires_are_shed(make: Make) -> None:
    c, app = await make(acquire_concurrency=1)
    sm = app.state.sessionmaker
    await seed_task(sm, "hbs", 3, team="hbs")
    [(_, tok)] = await make_executors(sm, 1)
    got = await c.post(f"{BASE}/acquire", json=ACQ, headers=_auth(tok))
    assert got.status_code == 200
    acq = got.json()["acquirement_id"]
    await app.state.acquire_slots.acquire()  # saturate
    _assert_shed(await c.post(f"{BASE}/acquire", json=ACQ, headers=_auth(tok)), "SERVER_BUSY")
    r = await c.post(f"{BASE}/subtasks/{acq}/heartbeat", json={}, headers=_auth(tok))
    assert r.status_code == 200, r.text
    app.state.acquire_slots.release()
