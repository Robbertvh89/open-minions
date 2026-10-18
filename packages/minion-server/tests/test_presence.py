"""Presence endpoint (TASK-5.1, SPEC FR-55): one test file, one DB per test via fixtures."""

import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from minion_common.enums import PausedReason
from minion_common.schemas import AcquireRequest, Budget, HeartbeatRequest, PresenceRequest
from minion_server.app import create_app
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from conftest import S3Env, _auth, _settings, make_executors, seed_task

BASE = "/api/v1/executor"
BUDGET: dict[str, Any] = {
    "profile": "light",
    "memory_gb": 4.0,
    "gpu_duty_cycle": 0.3,
    "max_batch_size": 4,
    "cpu_priority": "background",
}


@pytest.fixture
async def http(migrated_url: str, s3: S3Env) -> AsyncIterator[tuple[AsyncClient, Any]]:
    cfg = _settings(migrated_url).model_copy(
        update={
            "s3_endpoint_url": s3.endpoint_url,
            "public_s3_endpoint_url": s3.endpoint_url,
            "s3_access_key": SecretStr(s3.access_key),
            "s3_secret_key": SecretStr(s3.secret_key),
            "s3_bucket": s3.bucket,
        }
    )
    app = create_app(cfg)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
    ):
        yield c, app


async def _row(sm: async_sessionmaker[AsyncSession], eid: uuid.UUID) -> Any:
    async with sm() as s:
        return (
            await s.execute(
                text(
                    "SELECT profile, budget, paused_reason, last_seen_at "
                    "FROM minion_executor WHERE executor_id = :e"
                ),
                {"e": eid},
            )
        ).one()


@pytest.mark.integration
async def test_presence_updates_row(http: tuple[AsyncClient, Any]) -> None:
    c, app = http
    sm = app.state.sessionmaker
    [(p, token)] = await make_executors(sm, 1)
    before = await _row(sm, p.executor_id)
    r = await c.post(
        f"{BASE}/presence",
        json={"budget": BUDGET, "paused_reason": "on_battery"},
        headers=_auth(token),
    )
    assert r.status_code == 204, r.text
    after = await _row(sm, p.executor_id)
    assert after.profile == "light"
    assert after.budget == BUDGET
    assert after.paused_reason == "on_battery"
    assert before.last_seen_at is None or after.last_seen_at > before.last_seen_at


@pytest.mark.integration
async def test_presence_rejects_bad_reason_and_unknown_fields(
    http: tuple[AsyncClient, Any],
) -> None:
    c, app = http
    [(_, token)] = await make_executors(app.state.sessionmaker, 1)
    for body in (
        {"budget": BUDGET, "paused_reason": "Bad Reason!"},
        {"budget": BUDGET, "paused_reason": "x" * 65},
        {"budget": BUDGET, "extra": 1},
        {"budget": {**BUDGET, "cpu_priority": "turbo"}},
        {},
    ):
        r = await c.post(f"{BASE}/presence", json=body, headers=_auth(token))
        assert r.status_code == 422, body


@pytest.mark.integration
async def test_presence_requires_valid_token(http: tuple[AsyncClient, Any]) -> None:
    c, app = http
    sm = app.state.sessionmaker
    [(p, token)] = await make_executors(sm, 1)
    body = {"budget": BUDGET}
    assert (await c.post(f"{BASE}/presence", json=body)).status_code == 401
    assert (await c.post(f"{BASE}/presence", json=body, headers=_auth("nope"))).status_code == 401
    async with sm() as s, s.begin():
        await s.execute(
            text("UPDATE minion_executor SET status = 'REVOKED' WHERE executor_id = :e"),
            {"e": p.executor_id},
        )
    assert (await c.post(f"{BASE}/presence", json=body, headers=_auth(token))).status_code == 401


@pytest.mark.integration
async def test_acquire_clears_paused_reason_and_stores_cpu_priority(
    http: tuple[AsyncClient, Any],
) -> None:
    c, app = http
    sm = app.state.sessionmaker
    [(p, token)] = await make_executors(sm, 1)
    r = await c.post(
        f"{BASE}/presence",
        json={"budget": BUDGET, "paused_reason": "thermal"},
        headers=_auth(token),
    )
    assert r.status_code == 204
    assert (await _row(sm, p.executor_id)).paused_reason == "thermal"
    await seed_task(sm, "presence-clear", 1, team="presence")
    req = AcquireRequest(
        engines=["mlx"], budget=Budget.model_validate(BUDGET), output_upload="post"
    )
    r = await c.post(f"{BASE}/acquire", json=req.model_dump(mode="json"), headers=_auth(token))
    assert r.status_code in (200, 204), r.text
    row = await _row(sm, p.executor_id)
    assert row.paused_reason is None
    assert row.budget["cpu_priority"] == "background"


@pytest.mark.integration
async def test_acquire_without_cpu_priority_still_accepted(http: tuple[AsyncClient, Any]) -> None:
    c, app = http
    [(_, token)] = await make_executors(app.state.sessionmaker, 1)
    old = {k: v for k, v in BUDGET.items() if k != "cpu_priority"}
    r = await c.post(
        f"{BASE}/acquire",
        json={"engines": ["mlx"], "budget": old, "output_upload": "post"},
        headers=_auth(token),
    )
    assert r.status_code in (200, 204), r.text


@pytest.mark.integration
async def test_presence_cannot_target_another_executor(http: tuple[AsyncClient, Any]) -> None:
    c, app = http
    sm = app.state.sessionmaker
    [(a, tok_a), (b, _)] = await make_executors(sm, 2)
    before = await _row(sm, b.executor_id)
    r = await c.post(
        f"{BASE}/presence",
        json={"budget": BUDGET, "executor_id": str(b.executor_id)},
        headers=_auth(tok_a),
    )
    assert r.status_code == 422
    r = await c.post(
        f"{BASE}/presence",
        json={"budget": BUDGET, "paused_reason": "thermal"},
        headers=_auth(tok_a),
    )
    assert r.status_code == 204
    assert (await _row(sm, a.executor_id)).paused_reason == "thermal"
    after = await _row(sm, b.executor_id)
    assert tuple(after) == tuple(before)


def test_every_paused_reason_matches_wire_pattern() -> None:
    for reason in PausedReason:
        PresenceRequest(budget=Budget.model_validate(BUDGET), paused_reason=reason.value)
        HeartbeatRequest(paused_reason=reason.value)
