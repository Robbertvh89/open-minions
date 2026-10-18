"""Executor auth under a bearer flood and revocation bounds (TASK-32, ADR-0018)."""

import asyncio
import hashlib
import secrets
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from minion_server.admin import rotate_executor_token
from minion_server.app import create_app
from minion_server.repositories import executors as executors_repo
from minion_server.tokencache import ExecutorTokenCache
from pydantic import SecretStr
from sqlalchemy import text

from conftest import S3Env, _auth, _settings, make_executors, seed_task

BASE = "/api/v1/executor"
BUDGET: dict[str, Any] = {
    "profile": "balanced",
    "memory_gb": 8.0,
    "gpu_duty_cycle": 0.5,
    "max_batch_size": 8,
}
ACQ = {"engines": ["mlx"], "budget": BUDGET, "output_upload": "post"}

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


class Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


@pytest.mark.integration
@pytest.mark.parametrize("ttl", [0.0, 60.0])
async def test_bearer_flood_never_blocks_owner_ops(
    make: Make, monkeypatch: pytest.MonkeyPatch, ttl: float
) -> None:
    c, app = await make(
        db_pool_size=2,
        db_max_overflow=0,
        db_pool_timeout_s=0.5,
        acquire_concurrency=1,
        db_auth_pool_size=1,
        auth_lookup_timeout_s=0.2,
        executor_auth_cache_ttl_s=ttl,
    )
    sm = app.state.sessionmaker
    await seed_task(sm, "fl", 3, team="fl")
    [(_, tok)] = await make_executors(sm, 1)
    got = await c.post(f"{BASE}/acquire", json=ACQ, headers=_auth(tok))
    assert got.status_code == 200, got.text
    acq = got.json()["acquirement_id"]

    known = _digest(tok)
    real = executors_repo.get_by_token_hash
    flood_binds: list[Any] = []
    peak = 0

    async def slow_for_unknown(session: Any, token_hash: str) -> Any:
        nonlocal peak
        if token_hash == known:
            return await real(session, token_hash)
        flood_binds.append(session.bind)
        out = await real(session, token_hash)  # holds the checked-out connection from here
        peak = max(peak, app.state.auth_engine.pool.checkedout())
        await asyncio.sleep(1.0)
        return out

    monkeypatch.setattr(executors_repo, "get_by_token_hash", slow_for_unknown)

    async def flood_one(i: int) -> Any:
        path = "acquire" if i % 2 else f"subtasks/{acq}/heartbeat"
        body = ACQ if i % 2 else {}
        return await c.post(f"{BASE}/{path}", json=body, headers=_auth(secrets.token_urlsafe(32)))

    async def timed(path: str, body: Any) -> tuple[Any, float]:
        t0 = time.monotonic()
        r = await c.post(f"{BASE}/{path}", json=body, headers=_auth(tok))
        return r, time.monotonic() - t0

    flood = asyncio.gather(*(flood_one(i) for i in range(50)))
    await asyncio.sleep(0.1)  # the flood is in flight
    hb, hb_s = await timed(f"subtasks/{acq}/heartbeat", {})
    rel, rel_s = await timed(f"subtasks/{acq}/release", {"code": "TEST"})
    responses = await flood

    assert hb.status_code == 200 and hb_s < 0.5, (hb.text, hb_s)
    assert rel.status_code == 204 and rel_s < 0.5, (rel.text, rel_s)
    codes = {r.status_code for r in responses}
    assert codes <= {401, 503}, codes
    assert 503 in codes  # one auth connection, 50 slow lookups: the lane sheds
    for r in responses:
        if r.status_code == 503:
            assert r.json()["detail"]["code"] == "SERVER_BUSY"
            assert 1 <= int(r.headers["Retry-After"]) <= 3
    assert flood_binds, "flood lookups ran"
    assert all(b is app.state.auth_engine for b in flood_binds)  # none on the main engine
    assert peak <= 1


@pytest.mark.integration
async def test_revocation_takes_effect_within_the_ttl(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    c, app = await make(executor_auth_cache_ttl_s=5.0)
    clock = Clock()
    app.state.executor_tokens = ExecutorTokenCache(5.0, 100, clock=clock)
    sm = app.state.sessionmaker
    [(p, tok)] = await make_executors(sm, 1)
    pres = {"budget": BUDGET}
    assert (await c.post(f"{BASE}/presence", json=pres, headers=_auth(tok))).status_code == 204
    async with sm() as s, s.begin():
        assert await executors_repo.revoke(s, p.executor_id)
    clock.t += 4.0  # inside the documented bound: the cached identity still passes
    assert (await c.post(f"{BASE}/presence", json=pres, headers=_auth(tok))).status_code in (
        204,
        401,  # presence filters on the DB status itself (already immediate)
    )
    r = await c.post(
        f"{BASE}/subtasks/00000000-0000-0000-0000-000000000000/heartbeat",
        json={},
        headers=_auth(tok),
    )
    assert r.status_code != 401  # cache hit: authenticated (then 404/409 from the handler)
    clock.t += 1.5  # past the TTL
    r = await c.post(
        f"{BASE}/subtasks/00000000-0000-0000-0000-000000000000/heartbeat",
        json={},
        headers=_auth(tok),
    )
    assert r.status_code == 401
    calls: list[str] = []
    real = executors_repo.get_by_token_hash

    async def spy(session: Any, token_hash: str) -> Any:
        calls.append(token_hash)
        return await real(session, token_hash)

    monkeypatch.setattr(executors_repo, "get_by_token_hash", spy)
    r = await c.post(f"{BASE}/presence", json=pres, headers=_auth(tok))
    assert r.status_code == 401
    assert calls == []  # negative hit: no DB call


@pytest.mark.integration
async def test_rotation_old_token_dies_within_ttl_new_works_at_once(make: Make) -> None:
    c, app = await make(executor_auth_cache_ttl_s=5.0)
    clock = Clock()
    app.state.executor_tokens = ExecutorTokenCache(5.0, 100, clock=clock)
    sm = app.state.sessionmaker
    [(p, old)] = await make_executors(sm, 1)
    hb = f"{BASE}/subtasks/00000000-0000-0000-0000-000000000000/heartbeat"
    assert (await c.post(hb, json={}, headers=_auth(old))).status_code != 401
    async with sm() as s, s.begin():
        new = await rotate_executor_token(s, p.executor_id, ttl_days=1)
    assert new is not None
    assert (await c.post(hb, json={}, headers=_auth(new))).status_code != 401  # unknown lane
    clock.t += 5.1
    assert (await c.post(hb, json={}, headers=_auth(old))).status_code == 401


@pytest.mark.integration
async def test_token_expiry_is_checked_on_every_cache_hit(make: Make) -> None:
    c, app = await make(executor_auth_cache_ttl_s=60.0)
    sm = app.state.sessionmaker
    [(p, tok)] = await make_executors(sm, 1)
    hb = f"{BASE}/subtasks/00000000-0000-0000-0000-000000000000/heartbeat"
    assert (await c.post(hb, json={}, headers=_auth(tok))).status_code != 401  # cached
    async with sm() as s, s.begin():
        await s.execute(
            text("UPDATE minion_executor SET token_expires_at = :t WHERE executor_id = :i"),
            {"t": datetime.now(UTC) + timedelta(seconds=0.3), "i": p.executor_id},
        )
    # the cached entry still has the old expiry; refresh it through a stale revalidation
    app.state.executor_tokens = ExecutorTokenCache(60.0, 100)
    assert (await c.post(hb, json={}, headers=_auth(tok))).status_code != 401  # re-read, cached
    await asyncio.sleep(0.5)  # no clock change: expiry passes while cached
    assert (await c.post(hb, json={}, headers=_auth(tok))).status_code == 401
