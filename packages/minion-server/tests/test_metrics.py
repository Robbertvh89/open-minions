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

ALL_IF = "0.0.0.0"  # noqa: S104 - a config value under test, nothing binds to it
TOKEN = "scrape-me-" + "x" * 32
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
    await on.get("/no/such/path/with-an-id-7f3e9a")
    await on.request("BREW", "/healthz")
    r = await on.get("/metrics")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/plain")
    body = r.text
    assert 'minion_http_requests_total{method="GET",route="/healthz",status="200"} 2.0' in body
    assert 'route="/api/v1/executor/subtasks/{acquirement_id}/release"' in body
    assert 'route="/api/v1/tasks/{team}/{project}/{task_name}"' in body
    assert 'route="unmatched"' in body and 'method="OTHER"' in body
    for leak in (
        str(secret),
        "secret-team",
        "secret-proj",
        "secret-task",
        "with-an-id-7f3e9a",
        "Bearer",
    ):
        assert leak not in body, leak


async def test_token_protects_metrics() -> None:
    cfg = _settings(DEAD_DB).model_copy(
        update={"metrics_enabled": True, "metrics_token": SecretStr(TOKEN)}
    )
    async for c in _client(cfg):
        assert (await c.get("/metrics")).status_code == 401
        bad = await c.get("/metrics", headers={"Authorization": "Bearer nope"})
        assert bad.status_code == 401
        ok = await c.get("/metrics", headers=_auth(TOKEN))
        assert ok.status_code == 200
        assert TOKEN not in ok.text


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
    assert 'minion_acquire_seconds_count{outcome="empty"} 1.0' in text
    assert 'minion_acquire_seconds_count{outcome="assigned"} 1.0' in text
    assert 'minion_acquire_seconds_bucket{le="+Inf",outcome="assigned"} 1.0' in text
    assert str(p.executor_id) not in text and "metrics-pool" not in text


@pytest.mark.integration
async def test_exhausted_acquire_counts_as_exhausted_not_empty(
    fresh_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from minion_server.services import acquisition

    async def exhausted(*_a: Any, trace: Any = None, **_k: Any) -> None:
        trace.exhausted = True  # as the service does when every round lost the race
        return None

    monkeypatch.setattr(acquisition, "acquire", exhausted)
    cfg = _settings(fresh_url).model_copy(update={"metrics_enabled": True})
    app = create_app(cfg)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
    ):
        ((_p, tok),) = await make_executors(app.state.sessionmaker, 1, pool="metrics-pool")
        body = _req().model_dump(mode="json")
        r = await c.post("/api/v1/executor/acquire", json=body, headers=_auth(tok))
        assert r.status_code == 204
        text: Any = (await c.get("/metrics")).text
    assert 'minion_acquire_total{outcome="exhausted"} 1.0' in text
    assert 'minion_acquire_total{outcome="empty"}' not in text
    assert 'minion_acquire_seconds_count{outcome="exhausted"} 1.0' in text


@pytest.mark.integration
async def test_acquire_error_outcome_is_timed(
    fresh_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from minion_server.services import acquisition

    async def boom(*_a: Any, **_k: Any) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(acquisition, "acquire", boom)
    cfg = _settings(fresh_url).model_copy(update={"metrics_enabled": True})
    app = create_app(cfg)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(
            transport=ASGITransport(app=app, raise_app_exceptions=False), base_url="http://t"
        ) as c,
    ):
        ((_p, tok),) = await make_executors(app.state.sessionmaker, 1, pool="metrics-pool")
        body = _req().model_dump(mode="json")
        r = await c.post("/api/v1/executor/acquire", json=body, headers=_auth(tok))
        assert r.status_code == 500
        text: Any = (await c.get("/metrics")).text
    assert 'minion_acquire_seconds_count{outcome="error"} 1.0' in text
    assert 'minion_acquire_total{outcome="error"}' not in text


def _cfg(**kw: Any) -> ServerSettings:
    return ServerSettings(
        database_url=SecretStr("x"),
        s3_access_key=SecretStr("a"),
        s3_secret_key=SecretStr("b"),
        **kw,
    )


def test_metrics_token_empty_is_unset_and_short_is_rejected() -> None:
    assert _cfg(metrics_token="").metrics_token is None
    assert _cfg(metrics_token=TOKEN).metrics_token is not None
    with pytest.raises(ValueError, match="at least 32"):
        _cfg(metrics_token="short")
    padded = _cfg(metrics_token=f"  {TOKEN}\n").metrics_token
    assert padded is not None
    assert padded.get_secret_value() == TOKEN
    assert _cfg(metrics_token="  \n").metrics_token is None


async def test_warns_when_metrics_are_open_on_a_non_loopback_host(
    capsys: pytest.CaptureFixture[str],
) -> None:
    for host, expect in ((ALL_IF, True), ("127.0.0.1", False), ("localhost", False)):
        cfg = _settings(DEAD_DB).model_copy(update={"metrics_enabled": True, "host": host})
        async for _ in _client(cfg):
            pass
        assert ("metrics_unauthenticated" in capsys.readouterr().out) is expect, host
    cfg = _settings(DEAD_DB).model_copy(
        update={"metrics_enabled": True, "host": ALL_IF, "metrics_token": SecretStr(TOKEN)}
    )
    async for _ in _client(cfg):
        pass
    assert "metrics_unauthenticated" not in capsys.readouterr().out


async def test_warns_when_online_thresholds_differ_from_the_dashboards(
    capsys: pytest.CaptureFixture[str],
) -> None:
    async for _ in _client(_settings(DEAD_DB)):
        pass
    assert "executor_online_thresholds" not in capsys.readouterr().out
    cfg = _settings(DEAD_DB).model_copy(update={"executor_online_busy_s": 300.0})
    async for _ in _client(cfg):
        pass
    assert "executor_online_thresholds" in capsys.readouterr().out


@pytest.mark.integration
async def test_executor_auth_outcomes(fresh_url: str) -> None:
    cfg = _settings(fresh_url).model_copy(
        update={"metrics_enabled": True, "executor_auth_cache_ttl_s": 60.0}
    )
    app = create_app(cfg)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
    ):
        ((_, tok),) = await make_executors(app.state.sessionmaker, 1)
        url = "/api/v1/executor/acquire"
        body = _req().model_dump(mode="json")
        for t in (tok, tok, "garbage", "garbage"):
            await c.post(url, json=body, headers=_auth(t))
        text = (await c.get("/metrics")).text
    assert 'minion_executor_auth_total{outcome="db_valid"} 1.0' in text
    assert 'minion_executor_auth_total{outcome="cache_hit"} 1.0' in text
    assert 'minion_executor_auth_total{outcome="db_invalid"} 1.0' in text
    assert 'minion_executor_auth_total{outcome="negative_hit"} 1.0' in text
    assert "garbage" not in text
