"""Prometheus /metrics (FR-41): opt-in, route templates only, no ids or names (TASK-6.5)."""

import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from minion_server.app import create_app
from minion_server.settings import ServerSettings
from pydantic import SecretStr

from conftest import _auth, _req, _settings, make_executors, seed_task

DEAD_DB = "postgresql+asyncpg://u:p@127.0.0.1:1/none"  # engines connect lazily: never reached


async def _client(cfg: ServerSettings) -> AsyncIterator[AsyncClient]:
    app = create_app(cfg)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
    ):
        yield c


@pytest.fixture
async def on() -> AsyncIterator[AsyncClient]:
    async for c in _client(_settings(DEAD_DB).model_copy(update={"metrics_enabled": True})):
        yield c


async def test_metrics_is_off_by_default() -> None:
    assert (
        ServerSettings(
            database_url=SecretStr("x"), s3_access_key=SecretStr("a"), s3_secret_key=SecretStr("b")
        ).metrics_enabled
        is False
    )
    async for c in _client(_settings(DEAD_DB)):
        assert (await c.get("/metrics")).status_code == 404
        assert (await c.get("/healthz")).status_code == 200


async def test_counts_route_templates_not_urls(on: AsyncClient) -> None:
    await on.get("/healthz")
    await on.get("/healthz")
    secret = uuid.uuid4()
    await on.post(f"/api/v1/executor/subtasks/{secret}/release")  # no auth -> 401
    await on.get("/api/v1/tasks/secret-team/secret-proj/secret-task")  # 401, templated
    await on.get("/no/such/path/with-an-id-123")
    await on.request("BREW", "/healthz")
    r = await on.get("/metrics")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/plain")
    body = r.text
    assert 'minion_http_requests_total{method="GET",route="/healthz",status="200"} 2.0' in body
    assert 'route="/api/v1/executor/subtasks/{acquirement_id}/release"' in body
    assert 'route="/api/v1/tasks/{team}/{project}/{task_name}"' in body
    assert 'route="unmatched"' in body and 'method="OTHER"' in body
    for leak in (str(secret), "secret-team", "secret-proj", "secret-task", "123", "Bearer"):
        assert leak not in body, leak


async def test_token_protects_metrics() -> None:
    cfg = _settings(DEAD_DB).model_copy(
        update={"metrics_enabled": True, "metrics_token": SecretStr("scrape-me")}
    )
    async for c in _client(cfg):
        assert (await c.get("/metrics")).status_code == 401
        bad = await c.get("/metrics", headers={"Authorization": "Bearer nope"})
        assert bad.status_code == 401
        ok = await c.get("/metrics", headers=_auth("scrape-me"))
        assert ok.status_code == 200
        assert "scrape-me" not in ok.text


@pytest.mark.integration
async def test_acquire_outcome_counter(fresh_url: str) -> None:
    cfg = _settings(fresh_url).model_copy(update={"metrics_enabled": True})
    app = create_app(cfg)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
    ):
        sm = app.state.sessionmaker
        ((p, tok),) = await make_executors(sm, 1, pool="metrics-pool")
        body = _req().model_dump(mode="json")
        # empty database -> no work
        r = await c.post("/api/v1/executor/acquire", json=body, headers=_auth(tok))
        assert r.status_code == 204
        await seed_task(sm, "m1", 1, team="metrics", pool="metrics-pool")
        r = await c.post("/api/v1/executor/acquire", json=body, headers=_auth(tok))
        assert r.status_code == 200
        text: Any = (await c.get("/metrics")).text
    assert 'minion_acquire_total{outcome="empty"} 1.0' in text
    assert 'minion_acquire_total{outcome="assigned"} 1.0' in text
    assert str(p.executor_id) not in text and "metrics-pool" not in text
