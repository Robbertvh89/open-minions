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
    d.mkdir(mode=0o700)
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
    assert w["limit"] == 93
    monkeypatch.setenv("MINION_SERVER_WORKERS", "4")  # 4 x 20 + 2 = 82 < 93
    with structlog.testing.capture_logs() as logs:
        app_module.main()
    assert not [e for e in logs if e["event"] == "db_connection_budget_exceeded"]
    monkeypatch.setenv("MINION_DB_MAX_CONNECTIONS", "89")  # budget 82 >= 89 - 7: warns (>=)
    with structlog.testing.capture_logs() as logs:
        app_module.main()
    assert [e for e in logs if e["event"] == "db_connection_budget_exceeded"]
    monkeypatch.setenv("MINION_DB_MAX_CONNECTIONS", "90")  # 82 < 83
    with structlog.testing.capture_logs() as logs:
        app_module.main()
    assert not [e for e in logs if e["event"] == "db_connection_budget_exceeded"]


def test_main_multi_worker_default_metrics_dir_is_private_and_removed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import uvicorn

    _main_env(monkeypatch)
    monkeypatch.setenv("MINION_SERVER_WORKERS", "2")
    monkeypatch.setenv("MINION_METRICS_ENABLED", "true")
    monkeypatch.delenv("MINION_METRICS_MULTIPROC_DIR", raising=False)
    monkeypatch.delenv("PROMETHEUS_MULTIPROC_DIR", raising=False)
    seen: list[tuple[str, int]] = []

    def fake_run(*a: Any, **kw: Any) -> None:
        d = os.environ["PROMETHEUS_MULTIPROC_DIR"]
        seen.append((d, Path(d).stat().st_mode & 0o777))

    monkeypatch.setattr(uvicorn, "run", fake_run)
    try:
        app_module.main()
    finally:
        os.environ.pop("PROMETHEUS_MULTIPROC_DIR", None)
    [(d, mode)] = seen
    assert mode == 0o700
    assert "minion-metrics-" in d
    assert not Path(d).exists()


def test_main_refuses_an_unsafe_operator_metrics_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import uvicorn

    _main_env(monkeypatch)
    d = tmp_path / "m"
    d.mkdir()
    d.chmod(0o755)
    monkeypatch.setenv("MINION_SERVER_WORKERS", "2")
    monkeypatch.setenv("MINION_METRICS_ENABLED", "true")
    monkeypatch.setenv("MINION_METRICS_MULTIPROC_DIR", str(d))
    monkeypatch.setattr(uvicorn, "run", lambda *a, **kw: pytest.fail("must not start"))
    with pytest.raises(SystemExit) as exc:
        app_module.main()
    assert exc.value.code == 2


def test_main_single_worker_refuses_an_unsafe_inherited_multiproc_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import uvicorn

    _main_env(monkeypatch)
    d = tmp_path / "shared"
    d.mkdir()
    d.chmod(0o777)
    monkeypatch.setenv("PROMETHEUS_MULTIPROC_DIR", str(d))
    monkeypatch.setattr(uvicorn, "run", lambda *a, **kw: pytest.fail("must not start"))
    with pytest.raises(SystemExit) as exc:
        app_module.main()
    assert exc.value.code == 2
