import os
from pathlib import Path
from typing import Any

import httpx
import pytest
import structlog.testing
from fastapi import FastAPI
from minion_server import app as app_module
from minion_server.app import create_app


async def test_healthz() -> None:
    transport = httpx.ASGITransport(app=create_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def _main_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_DATABASE_URL", "postgresql+asyncpg://x@h/db")
    monkeypatch.setenv("MINION_S3_ACCESS_KEY", "ak")
    monkeypatch.setenv("MINION_S3_SECRET_KEY", "sk")
    monkeypatch.chdir("/")  # no .env file


def test_main_single_worker_runs_the_app_in_process(monkeypatch: pytest.MonkeyPatch) -> None:
    import uvicorn

    _main_env(monkeypatch)
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    monkeypatch.setattr(uvicorn, "run", lambda *a, **kw: calls.append((a, kw)))
    monkeypatch.delenv("PROMETHEUS_MULTIPROC_DIR", raising=False)
    app_module.main()
    [(args, kwargs)] = calls
    assert isinstance(args[0], FastAPI)
    assert "workers" not in kwargs
    assert (kwargs["host"], kwargs["port"]) == ("127.0.0.1", 8080)
    assert "PROMETHEUS_MULTIPROC_DIR" not in os.environ


def test_main_multi_worker_uses_the_factory_string_and_prepares_metrics(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import uvicorn

    _main_env(monkeypatch)
    d = tmp_path / "m"
    d.mkdir()
    (d / "counter_1.db").write_bytes(b"stale")
    monkeypatch.setenv("MINION_SERVER_WORKERS", "4")
    monkeypatch.setenv("MINION_METRICS_ENABLED", "true")
    monkeypatch.setenv("MINION_METRICS_MULTIPROC_DIR", str(d))
    monkeypatch.delenv("PROMETHEUS_MULTIPROC_DIR", raising=False)
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    monkeypatch.setattr(uvicorn, "run", lambda *a, **kw: calls.append((a, kw)))
    try:
        app_module.main()
        assert os.environ["PROMETHEUS_MULTIPROC_DIR"] == str(d)
    finally:
        os.environ.pop("PROMETHEUS_MULTIPROC_DIR", None)
    [(args, kwargs)] = calls
    assert args == ("minion_server.app:create_app",)
    assert kwargs["factory"] is True
    assert kwargs["workers"] == 4
    assert list(d.iterdir()) == []


def test_main_warns_when_the_connection_budget_is_exceeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import uvicorn

    _main_env(monkeypatch)
    monkeypatch.setenv("MINION_SERVER_WORKERS", "8")  # 8 x 20 + 2 > 100
    monkeypatch.setattr(uvicorn, "run", lambda *a, **kw: None)
    with structlog.testing.capture_logs() as logs:
        app_module.main()
    [w] = [e for e in logs if e["event"] == "db_connection_budget_exceeded"]
    assert w["budget"] == 162
    assert w["db_max_connections"] == 100
    monkeypatch.setenv("MINION_SERVER_WORKERS", "4")  # 4 x 20 + 2 = 82
    with structlog.testing.capture_logs() as logs:
        app_module.main()
    assert not [e for e in logs if e["event"] == "db_connection_budget_exceeded"]
