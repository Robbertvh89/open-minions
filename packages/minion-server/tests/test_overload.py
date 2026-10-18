"""Overload shedding (TASK-7.4.4): acquire deadline before the claim, pool timeout -> 503,
acquire concurrency slots, owner-operation headroom."""

import asyncio
import re
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from minion_server.app import create_app
from minion_server.repositories import acquisition as acquisition_repo
from minion_server.repositories import executors as executors_repo
from pydantic import SecretStr
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession

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


def _assert_shed(r: Any, code: str, retry_after: tuple[int, int] = (1, 3)) -> None:
    assert r.status_code == 503, r.text
    assert r.json()["detail"]["code"] == code
    assert re.fullmatch(r"[1-9]\d*", r.headers["Retry-After"])
    assert retry_after[0] <= int(r.headers["Retry-After"]) <= retry_after[1]


def _assert_slots_free(app: Any, limit: int = 1) -> None:
    """Every acquire slot and per-executor counter is back (no leak on any exit path)."""
    assert not app.state.acquire_slots.locked()
    assert app.state.acquire_slots._value == limit
    assert app.state.acquire_inflight == {}


def _shed_count(app: Any) -> float:
    return float(app.state.metrics.acquires.labels(outcome="shed")._value.get())


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
    c, app = await make(acquire_deadline_s=1.0, acquire_concurrency=1, metrics_enabled=True)
    await seed_task(app.state.sessionmaker, "dl", 3, team="dl")
    [(_, tok)] = await make_executors(app.state.sessionmaker, 1)
    # The pool's last connection is busy past the deadline: the claim round's connection() call
    # returns late. Simulated at the call (a pool wait is the same await), after the preference
    # read so only the claim round is delayed.
    real_pref = executors_repo.get_preference
    real_conn = AsyncSession.connection
    after_pref = False

    async def pref(*a: Any, **kw: Any) -> Any:
        nonlocal after_pref
        out = await real_pref(*a, **kw)
        after_pref = True
        return out

    async def late_connection(self: Any, *a: Any, **kw: Any) -> Any:
        if after_pref:
            await asyncio.sleep(1.3)  # held past the 1 s deadline
        return await real_conn(self, *a, **kw)

    monkeypatch.setattr(executors_repo, "get_preference", pref)
    monkeypatch.setattr(AsyncSession, "connection", late_connection)
    r = await c.post(f"{BASE}/acquire", json=ACQ, headers=_auth(tok))
    # session.connection() returned late (after the holder released): the check fires before claim
    _assert_shed(r, "ACQUIRE_DEADLINE")
    assert await _acquirements(app) == 0
    _assert_slots_free(app)
    assert _shed_count(app) == 1


@pytest.mark.integration
async def test_slow_claim_statement_is_cancelled_at_the_deadline(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    c, app = await make(acquire_deadline_s=1.0, acquire_concurrency=1)
    await seed_task(app.state.sessionmaker, "sc", 3, team="sc")
    [(_, tok)] = await make_executors(app.state.sessionmaker, 1)
    real = acquisition_repo.claim

    async def slow_claim(session: Any, *a: Any, **kw: Any) -> Any:
        await session.execute(text("SELECT pg_sleep(5)"))  # a claim stuck behind a lock
        return await real(session, *a, **kw)

    monkeypatch.setattr(acquisition_repo, "claim", slow_claim)
    t0 = time.monotonic()
    r = await c.post(f"{BASE}/acquire", json=ACQ, headers=_auth(tok))
    assert time.monotonic() - t0 < 3.0  # cancelled by statement_timeout, not run to the end
    _assert_shed(r, "ACQUIRE_DEADLINE")
    assert await _acquirements(app) == 0
    _assert_slots_free(app)
    monkeypatch.setattr(acquisition_repo, "claim", real)
    ok = await c.post(f"{BASE}/acquire", json=ACQ, headers=_auth(tok))
    assert ok.status_code == 200  # the connection is clean again (timeout was LOCAL)


@pytest.mark.integration
async def test_deadline_passing_after_the_claim_rolls_back_the_acquirement(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    c, app = await make(acquire_deadline_s=1.0, acquire_concurrency=1)
    await seed_task(app.state.sessionmaker, "al", 3, team="al")
    [(_, tok)] = await make_executors(app.state.sessionmaker, 1)
    real = acquisition_repo.claim

    async def claim_then_stall(session: Any, *a: Any, **kw: Any) -> Any:
        out = await real(session, *a, **kw)  # the INSERT has run inside the open transaction
        await asyncio.sleep(1.3)  # work after the claim outlives the deadline
        return out

    monkeypatch.setattr(acquisition_repo, "claim", claim_then_stall)
    r = await c.post(f"{BASE}/acquire", json=ACQ, headers=_auth(tok))
    _assert_shed(r, "ACQUIRE_DEADLINE")
    assert await _acquirements(app) == 0
    _assert_slots_free(app)


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
    c, app = await make(
        db_pool_size=1, db_max_overflow=0, db_pool_timeout_s=0.5, acquire_concurrency=1
    )
    [(_, tok)] = await make_executors(app.state.sessionmaker, 1)
    async with app.state.engine.connect() as held:  # the only connection
        await held.execute(text("SELECT 1"))
        for path, body in [("acquire", ACQ), ("presence", {"budget": BUDGET})]:
            r = await c.post(f"{BASE}/{path}", json=body, headers=_auth(tok))
            _assert_shed(r, "SERVER_BUSY", (2, 10))
    _assert_slots_free(app)
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


@pytest.mark.integration
async def test_cancelled_acquire_releases_its_slot(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    c, app = await make(acquire_concurrency=1)
    [(_, tok)] = await make_executors(app.state.sessionmaker, 1)
    _slow_preference(monkeypatch, 5.0)
    task = asyncio.ensure_future(c.post(f"{BASE}/acquire", json=ACQ, headers=_auth(tok)))
    for _ in range(100):  # wait until the acquire holds the slot
        if app.state.acquire_slots.locked():
            break
        await asyncio.sleep(0.02)
    assert app.state.acquire_slots.locked()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    _assert_slots_free(app)


@pytest.mark.integration
async def test_one_executor_cannot_hold_more_than_two_acquire_slots(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    c, app = await make(acquire_concurrency=8, acquire_min_interval_s=0.0)
    sm = app.state.sessionmaker
    await seed_task(sm, "cap", 6, team="cap")
    [(_, tok), (_, tok2)] = await make_executors(sm, 2)
    _slow_preference(monkeypatch, 0.6)
    rs = await asyncio.gather(
        *[c.post(f"{BASE}/acquire", json=ACQ, headers=_auth(tok)) for _ in range(4)]
    )
    codes = sorted(r.status_code for r in rs)
    assert codes.count(429) == 2 and codes.count(200) == 2, codes
    assert all(r.headers["Retry-After"] == "1" for r in rs if r.status_code == 429)
    _assert_slots_free(app, 8)
    # another executor is not affected by the first one's cap
    r = await c.post(f"{BASE}/acquire", json=ACQ, headers=_auth(tok2))
    assert r.status_code == 200
