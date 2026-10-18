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
    main_peak = 0
    in_flight = asyncio.Event()

    async def slow_for_unknown(session: Any, token_hash: str) -> Any:
        nonlocal peak, main_peak
        if token_hash == known:
            return await real(session, token_hash)
        flood_binds.append(session.bind)
        out = await real(session, token_hash)  # holds the checked-out connection from here
        peak = max(peak, app.state.auth_engine.pool.checkedout())
        main_peak = max(main_peak, app.state.engine.pool.checkedout())
        in_flight.set()
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
    await asyncio.wait_for(in_flight.wait(), 5)  # the flood is in flight
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
    assert peak <= 1  # the auth lane never exceeds its pool
    assert main_peak <= 1  # the flood held no main-pool connection (a legit op may hold one)


NO_ACQ = "00000000-0000-0000-0000-000000000000"
HB = f"{BASE}/subtasks/{NO_ACQ}/heartbeat"


async def _status(c: AsyncClient, path: str, tok: str, body: Any) -> int:
    return (await c.post(path, json=body, headers=_auth(tok))).status_code


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
    assert await _status(c, f"{BASE}/presence", tok, pres) == 204
    async with sm() as s, s.begin():
        assert await executors_repo.revoke(s, p.executor_id)
    clock.t += 4.0  # inside the documented bound: the cached identity still authenticates
    assert await _status(c, HB, tok, {}) == 409  # owner ops authenticate; no such acquirement
    # ... but presence (a DB write that matches the live row) is refused at once
    assert await _status(c, f"{BASE}/presence", tok, pres) == 401
    clock.t += 1.5  # past the TTL
    assert await _status(c, HB, tok, {}) == 401
    calls: list[str] = []
    real = executors_repo.get_by_token_hash

    async def spy(session: Any, token_hash: str) -> Any:
        calls.append(token_hash)
        return await real(session, token_hash)

    monkeypatch.setattr(executors_repo, "get_by_token_hash", spy)
    assert await _status(c, f"{BASE}/presence", tok, pres) == 401
    assert calls == []  # negative hit: no DB call


@pytest.mark.integration
async def test_revoked_token_cannot_acquire_or_heartbeat_inside_the_ttl(make: Make) -> None:
    c, app = await make(executor_auth_cache_ttl_s=60.0)
    sm = app.state.sessionmaker
    await seed_task(sm, "rv", 3, team="rv")
    [(p, tok)] = await make_executors(sm, 1)
    got = await c.post(f"{BASE}/acquire", json=ACQ, headers=_auth(tok))
    assert got.status_code == 200
    acq = got.json()["acquirement_id"]
    async with sm() as s, s.begin():
        assert await executors_repo.revoke(s, p.executor_id)
    # still inside the TTL: the cache would admit the token, the mutation refuses it at once
    assert await _status(c, f"{BASE}/acquire", tok, ACQ) == 401
    async with sm() as s:
        n = (await s.execute(text("SELECT count(*) FROM minion_subtask_acquirement"))).scalar_one()
    assert n == 1  # nothing was claimed by the revoked token
    assert await _status(c, f"{BASE}/subtasks/{acq}/heartbeat", tok, {}) == 401


@pytest.mark.integration
async def test_rotation_old_token_cannot_acquire_new_works_at_once(make: Make) -> None:
    c, app = await make(executor_auth_cache_ttl_s=5.0)
    clock = Clock()
    app.state.executor_tokens = ExecutorTokenCache(5.0, 100, clock=clock)
    sm = app.state.sessionmaker
    await seed_task(sm, "ro", 3, team="ro")
    [(p, old)] = await make_executors(sm, 1)
    assert await _status(c, f"{BASE}/acquire", old, ACQ) == 200
    async with sm() as s, s.begin():
        new = await rotate_executor_token(s, p.executor_id, ttl_days=1)
    assert new is not None
    assert await _status(c, HB, new, {}) == 409  # unknown lane, authenticated
    assert await _status(c, f"{BASE}/acquire", old, ACQ) == 401  # cached, but the UPDATE misses
    clock.t += 5.1
    assert await _status(c, HB, old, {}) == 401


@pytest.mark.integration
async def test_token_expiry_is_checked_on_every_cache_hit(make: Make) -> None:
    c, app = await make(executor_auth_cache_ttl_s=60.0)
    sm = app.state.sessionmaker
    [(p, tok)] = await make_executors(sm, 1)
    assert await _status(c, HB, tok, {}) == 409  # cached
    async with sm() as s, s.begin():
        await s.execute(
            text("UPDATE minion_executor SET token_expires_at = :t WHERE executor_id = :i"),
            {"t": datetime.now(UTC) + timedelta(seconds=0.3), "i": p.executor_id},
        )
    app.state.executor_tokens = ExecutorTokenCache(60.0, 100)  # re-read the new expiry
    assert await _status(c, HB, tok, {}) == 409
    await asyncio.sleep(0.5)  # no clock change: expiry passes while cached
    assert await _status(c, HB, tok, {}) == 401


@pytest.mark.integration
async def test_preloaded_known_executor_bypasses_the_auth_lane_after_restart(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, first = await make()
    [(_, tok)] = await make_executors(first.state.sessionmaker, 1)
    c, app = await make(
        executor_auth_cache_ttl_s=60.0, db_auth_pool_size=1, auth_lookup_timeout_s=0.2
    )  # a fresh process: its cache starts from the preload
    assert app.state.executor_tokens.lookup(_digest(tok)).state.value == "stale"
    real = executors_repo.get_by_token_hash
    binds: dict[str, Any] = {}
    flooding = asyncio.Event()

    async def slow_for_unknown(session: Any, token_hash: str) -> Any:
        if token_hash != _digest(tok):
            await real(session, token_hash)
            flooding.set()
            await asyncio.sleep(1.0)  # holds the only auth connection
        else:
            binds["known"] = session.bind
        return await real(session, token_hash)

    monkeypatch.setattr(executors_repo, "get_by_token_hash", slow_for_unknown)
    flood = asyncio.gather(
        *(c.post(HB, json={}, headers=_auth(secrets.token_urlsafe(32))) for _ in range(20))
    )
    await asyncio.wait_for(flooding.wait(), 5)
    t0 = time.monotonic()
    assert await _status(c, HB, tok, {}) == 409
    assert time.monotonic() - t0 < 0.5
    assert binds["known"] is app.state.engine  # revalidated on the main pool, not the lane
    assert {r.status_code for r in await flood} <= {401, 503}
