"""Executor endpoint rate limits (TASK-6.2, NF-5): limiter unit tests, integration over HTTP."""

import asyncio
import re
import threading
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from minion_server.app import create_app
from minion_server.ratelimit import MinIntervalLimiter, retry_after_header
from pydantic import SecretStr
from sqlalchemy import text

from conftest import S3Env, _auth, _req, _settings, make_executors, seed_task

BASE = "/api/v1/executor"
BUDGET: dict[str, Any] = {
    "profile": "light",
    "memory_gb": 4.0,
    "gpu_duty_cycle": 0.3,
    "max_batch_size": 4,
    "cpu_priority": "background",
}
REGISTER: dict[str, Any] = {
    "hostname": "h",
    "chip": "M1",
    "total_memory_gb": 16.0,
    "os_version": "15.0",
    "engines": ["mlx"],
    "profile": "light",
    "budget": BUDGET,
}


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_limiter_admits_after_interval_and_reports_remaining() -> None:
    clock, a = Clock(), uuid.uuid4()
    lim = MinIntervalLimiter({"acquire": 2.0}, clock=clock)
    assert lim.check(a, "acquire") is None
    clock.now += 0.5
    wait = lim.check(a, "acquire")
    assert wait == pytest.approx(1.5)
    clock.now += 1.5  # a rejected call did not extend the window
    assert lim.check(a, "acquire") is None


def test_limiter_keys_are_per_executor_and_op() -> None:
    clock, a, b = Clock(), uuid.uuid4(), uuid.uuid4()
    lim = MinIntervalLimiter({"acquire": 5.0, "presence": 5.0}, clock=clock)
    assert lim.check(a, "acquire") is None
    assert lim.check(b, "acquire") is None
    assert lim.check(a, "presence") is None
    assert lim.check(a, "acquire") is not None


def test_limiter_zero_or_unknown_op_disables() -> None:
    a = uuid.uuid4()
    lim = MinIntervalLimiter({"acquire": 0.0})
    for _ in range(3):
        assert lim.check(a, "acquire") is None
        assert lim.check(a, "other") is None


def test_limiter_is_bounded_lru() -> None:
    clock = Clock()
    lim = MinIntervalLimiter({"x": 100.0}, clock=clock, max_entries=3)
    ids = [uuid.uuid4() for _ in range(4)]
    for i in ids:
        assert lim.check(i, "x") is None
    assert len(lim._last) == 3
    assert lim.check(ids[0], "x") is None  # evicted: admitted again (soft state, bounded memory)
    assert lim.check(ids[3], "x") is not None  # recent entries are still limited


def test_limiter_threaded_admits_exactly_one_per_key_per_interval() -> None:
    lim = MinIntervalLimiter({"acquire": 3600.0})
    keys = [uuid.uuid4() for _ in range(5)]
    admitted: list[uuid.UUID] = []
    errors: list[BaseException] = []
    barrier = threading.Barrier(40)

    def work(i: int) -> None:
        try:
            barrier.wait()
            for k in keys:
                if lim.check(k, "acquire") is None:
                    admitted.append(k)
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=work, args=(i,)) for i in range(40)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert sorted(admitted) == sorted(keys)


def test_limiter_threaded_eviction_churn_has_no_errors() -> None:
    lim = MinIntervalLimiter({"x": 0.0001}, max_entries=8)
    errors: list[BaseException] = []

    def work() -> None:
        try:
            for _ in range(3000):
                lim.check(uuid.uuid4(), "x")
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=work) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert len(lim._last) <= 8


def test_retry_after_is_whole_seconds_at_least_one() -> None:
    assert retry_after_header(0.01) == "1"
    assert retry_after_header(1.2) == "2"
    assert re.fullmatch(r"[A-Z0-9_]{1,64}", "TOO_MANY_REQUESTS")


@pytest.fixture
async def limited(fresh_url: str, s3: S3Env) -> AsyncIterator[tuple[AsyncClient, Any]]:
    cfg = _settings(fresh_url).model_copy(
        update={
            "s3_endpoint_url": s3.endpoint_url,
            "public_s3_endpoint_url": s3.endpoint_url,
            "s3_access_key": SecretStr(s3.access_key),
            "s3_secret_key": SecretStr(s3.secret_key),
            "s3_bucket": s3.bucket,
            "register_min_interval_s": 3600.0,
            "acquire_min_interval_s": 3600.0,
            "presence_min_interval_s": 3600.0,
        }
    )
    app = create_app(cfg)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
    ):
        yield c, app


async def _hb_count(sm: Any, eid: uuid.UUID) -> Any:
    async with sm() as s:
        return (
            await s.execute(
                text("SELECT hostname, last_seen_at FROM minion_executor WHERE executor_id = :e"),
                {"e": eid},
            )
        ).one()


def _assert_limited(r: Any) -> None:
    assert r.status_code == 429, r.text
    assert r.json()["detail"]["code"] == "TOO_MANY_REQUESTS"
    assert re.fullmatch(r"[A-Z0-9_]{1,64}", r.json()["detail"]["code"])
    assert int(r.headers["Retry-After"]) >= 1


@pytest.mark.integration
async def test_presence_too_fast_is_429_without_db_write(limited: tuple[AsyncClient, Any]) -> None:
    c, app = limited
    sm = app.state.sessionmaker
    [(p, token)] = await make_executors(sm, 1)
    r = await c.post(f"{BASE}/presence", json={"budget": BUDGET}, headers=_auth(token))
    assert r.status_code == 204
    before = await _hb_count(sm, p.executor_id)
    r = await c.post(
        f"{BASE}/presence",
        json={"budget": BUDGET, "paused_reason": "on_battery"},
        headers=_auth(token),
    )
    _assert_limited(r)
    async with sm() as s:
        reason = (
            await s.execute(
                text("SELECT paused_reason FROM minion_executor WHERE executor_id = :e"),
                {"e": p.executor_id},
            )
        ).scalar_one()
    assert reason is None  # the rejected ping wrote nothing
    assert (await _hb_count(sm, p.executor_id)).last_seen_at == before.last_seen_at


@pytest.mark.integration
async def test_register_too_fast_is_429_without_db_write(limited: tuple[AsyncClient, Any]) -> None:
    c, app = limited
    sm = app.state.sessionmaker
    [(p, token)] = await make_executors(sm, 1)
    first = await c.post(f"{BASE}/register", json=REGISTER, headers=_auth(token))
    assert first.status_code == 200
    r = await c.post(
        f"{BASE}/register", json={**REGISTER, "hostname": "other"}, headers=_auth(token)
    )
    _assert_limited(r)
    assert (await _hb_count(sm, p.executor_id)).hostname == "h"


@pytest.mark.integration
async def test_acquire_too_fast_is_429_and_does_not_acquire(
    limited: tuple[AsyncClient, Any],
) -> None:
    c, app = limited
    sm = app.state.sessionmaker
    await seed_task(sm, "rl", 2)
    [(p, token)] = await make_executors(sm, 1)
    body = _req().model_dump(mode="json")
    assert (await c.post(f"{BASE}/acquire", json=body, headers=_auth(token))).status_code == 200
    _assert_limited(await c.post(f"{BASE}/acquire", json=body, headers=_auth(token)))
    async with sm() as s:
        n = (
            await s.execute(
                text("SELECT count(*) FROM minion_subtask_acquirement WHERE executor_id = :e"),
                {"e": p.executor_id},
            )
        ).scalar_one()
    assert n == 1


@pytest.mark.integration
async def test_concurrent_acquires_with_one_token_admit_exactly_one(
    limited: tuple[AsyncClient, Any],
) -> None:
    c, app = limited
    sm = app.state.sessionmaker
    await seed_task(sm, "rlc", 20)
    [(p, token)] = await make_executors(sm, 1)
    body = _req().model_dump(mode="json")
    rs = await asyncio.gather(
        *(c.post(f"{BASE}/acquire", json=body, headers=_auth(token)) for _ in range(20))
    )
    codes = sorted(r.status_code for r in rs)
    assert codes == [200] + [429] * 19
    async with sm() as s:
        n = (
            await s.execute(
                text("SELECT count(*) FROM minion_subtask_acquirement WHERE executor_id = :e"),
                {"e": p.executor_id},
            )
        ).scalar_one()
    assert n == 1


@pytest.mark.integration
async def test_preference_too_fast_is_429(limited: tuple[AsyncClient, Any]) -> None:
    c, app = limited
    app.state.limiter._intervals["preference"] = 3600.0
    [(_, token)] = await make_executors(app.state.sessionmaker, 1)
    body = {"team": "t"}
    r = await c.put(f"{BASE}/preference", json=body, headers=_auth(token))
    assert r.status_code == 204, r.text
    _assert_limited(await c.put(f"{BASE}/preference", json=body, headers=_auth(token)))


@pytest.mark.integration
async def test_limits_are_per_executor_and_unauthenticated_is_401(
    limited: tuple[AsyncClient, Any],
) -> None:
    c, app = limited
    execs = await make_executors(app.state.sessionmaker, 2)
    body = {"budget": BUDGET}
    assert (await c.post(f"{BASE}/presence", json=body)).status_code == 401
    assert (await c.post(f"{BASE}/presence", json=body)).status_code == 401  # never counted
    for _, token in execs:
        r = await c.post(f"{BASE}/presence", json=body, headers=_auth(token))
        assert r.status_code == 204
