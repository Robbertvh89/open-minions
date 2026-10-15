"""Heartbeat endpoint (TASK-3.1 S2). Each test uses its own team for isolation."""

import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from minion_common.schemas import HeartbeatResponse
from minion_server.app import create_app
from minion_server.repositories import executors as executors_repo
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from conftest import S3Env, _auth, _req, _settings, make_executors, seed_task

BASE = "/api/v1/executor"
HB_SQL = "SELECT {} FROM minion_executor_heartbeat WHERE acquirement_id = :a"
BUDGET = {"profile": "light", "memory_gb": 4.0, "gpu_duty_cycle": 0.25, "max_batch_size": 4}


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


async def _own(
    c: AsyncClient, sm: async_sessionmaker[AsyncSession], team: str, token: str, eid: uuid.UUID
) -> dict[str, Any]:
    async with sm() as s, s.begin():
        await executors_repo.set_preference(s, eid, team=team, project=None, task_name=None)
    r = await c.post(f"{BASE}/acquire", json=_req().model_dump(mode="json"), headers=_auth(token))
    assert r.status_code == 200, r.text
    a: dict[str, Any] = r.json()
    return a


async def _one(sm: async_sessionmaker[AsyncSession], sql: str, **p: Any) -> Any:
    async with sm() as s:
        return (await s.execute(text(sql), p)).one()


@pytest.mark.integration
async def test_heartbeat_updates_state_and_refreshes_url(
    http: tuple[AsyncClient, Any], s3: S3Env
) -> None:
    c, app = http
    sm = app.state.sessionmaker
    await seed_task(sm, "hb1", 1, team="hb1")
    ((p, tok),) = await make_executors(sm, 1)
    a = await _own(c, sm, "hb1", tok, p.executor_id)
    async with sm() as s, s.begin():
        await s.execute(
            text(
                "UPDATE minion_executor_heartbeat SET last_seen_at = now() - interval '1 hour'"
                " WHERE acquirement_id = :a"
            ),
            {"a": a["acquirement_id"]},
        )
    old = (await _one(sm, HB_SQL.format("last_seen_at"), a=a["acquirement_id"]))[0]

    body = {
        "progress": {"records_done": 4, "records_total": 10},
        "budget": BUDGET,
        "paused_reason": "on_battery",
    }
    r = await c.post(
        f"{BASE}/subtasks/{a['acquirement_id']}/heartbeat", json=body, headers=_auth(tok)
    )
    assert r.status_code == 200, r.text
    out = HeartbeatResponse.model_validate(r.json())
    assert out.cancel is False
    assert out.expires_at > datetime.now(UTC)

    row = await _one(sm, HB_SQL.format("last_seen_at, progress"), a=a["acquirement_id"])
    assert row[0] > old
    assert row[1] == {"records_done": 4, "records_total": 10}
    ex = await _one(
        sm,
        "SELECT profile, budget->>'memory_gb', paused_reason FROM minion_executor"
        " WHERE executor_id=:e",
        e=p.executor_id,
    )
    assert tuple(ex) == ("light", "4.0", "on_battery")

    # a beat without progress keeps the stored progress
    r = await c.post(
        f"{BASE}/subtasks/{a['acquirement_id']}/heartbeat", json={}, headers=_auth(tok)
    )
    assert r.status_code == 200
    row = await _one(sm, HB_SQL.format("progress"), a=a["acquirement_id"])
    assert row[0] == {"records_done": 4, "records_total": 10}

    # the refreshed URL is scoped to the attempt key and accepts a PUT
    key = f"outputs/hb1/hb1/_attempts/{a['subtask_id']:06d}/{a['attempt']}.jsonl"
    assert key in out.output_url
    async with httpx.AsyncClient() as raw:
        put = await raw.put(out.output_url, content=b"x\n")
    assert put.status_code == 200
    assert s3.client.get_object(Bucket=s3.bucket, Key=key)["Body"].read() == b"x\n"


@pytest.mark.integration
async def test_heartbeat_stale_and_foreign_identical_409(http: tuple[AsyncClient, Any]) -> None:
    c, app = http
    sm = app.state.sessionmaker
    await seed_task(sm, "hb2", 1, team="hb2")
    (p, tok), (_, other) = await make_executors(sm, 2)
    a = await _own(c, sm, "hb2", tok, p.executor_id)

    def beat(acq: object, token: str) -> Any:
        return c.post(f"{BASE}/subtasks/{acq}/heartbeat", json={}, headers=_auth(token))

    bodies = []
    for acq, token in [(a["acquirement_id"], other), (uuid.uuid4(), tok)]:
        r = await beat(acq, token)
        assert (r.status_code, r.json()["detail"]["code"]) == (409, "STALE_ACQUIREMENT")
        bodies.append(r.json())
    # after release the owner's own id is stale too, with the same body
    rel = await c.post(f"{BASE}/subtasks/{a['acquirement_id']}/release", headers=_auth(tok))
    assert rel.status_code == 204
    r = await beat(a["acquirement_id"], tok)
    assert r.status_code == 409
    bodies.append(r.json())
    assert bodies[0] == bodies[1] == bodies[2]
    assert (
        await c.post(f"{BASE}/subtasks/{uuid.uuid4()}/heartbeat", json={"x": 1}, headers=_auth(tok))
    ).status_code == 422


@pytest.mark.integration
async def test_heartbeat_cancel_flag(http: tuple[AsyncClient, Any]) -> None:
    c, app = http
    sm = app.state.sessionmaker
    await seed_task(sm, "hb3", 1, team="hb3")
    ((p, tok),) = await make_executors(sm, 1)
    a = await _own(c, sm, "hb3", tok, p.executor_id)
    url = f"{BASE}/subtasks/{a['acquirement_id']}/heartbeat"
    assert (await c.post(url, json={}, headers=_auth(tok))).json()["cancel"] is False
    async with sm() as s, s.begin():
        await s.execute(text("UPDATE minion_task SET status='CANCELLED' WHERE task_name='hb3'"))
    assert (await c.post(url, json={}, headers=_auth(tok))).json()["cancel"] is True
