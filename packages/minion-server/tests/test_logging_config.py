import asyncio
import json
import logging
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import pytest
import structlog
import uvicorn
from fastapi import FastAPI
from minion_server import app as app_module
from minion_server.app import create_app
from minion_server.logging_config import configure_logging
from minion_server.settings import ServerSettings
from pydantic import SecretStr

BEARER = "Bearer s3cr3t-token-value"
SIGNATURE = "X-Amz-Signature=deadbeefcafe"
PAYLOAD = "super-secret-prompt-text"
LOCAL_SECRET = "local-var-s3cr3t"


def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")
    monkeypatch.setenv("MINION_S3_ACCESS_KEY", "ak")
    monkeypatch.setenv("MINION_S3_SECRET_KEY", "sk")
    monkeypatch.setenv("MINION_SERVER_WORKERS", "1")
    monkeypatch.chdir("/")


def _lines(capsys: pytest.CaptureFixture[str]) -> list[dict[str, object]]:
    out = capsys.readouterr().out
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def test_structlog_line_is_json_with_utc_timestamp_and_level(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from datetime import datetime

    configure_logging()
    structlog.get_logger().warning("metrics_unauthenticated", host="10.0.0.1")
    [rec] = _lines(capsys)
    assert rec["event"] == "metrics_unauthenticated"
    assert rec["level"] == "warning"
    off = datetime.fromisoformat(str(rec["timestamp"])).utcoffset()
    assert off is not None and off.total_seconds() == 0


def test_stdlib_loggers_such_as_uvicorn_are_json_too(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging()
    logging.getLogger("uvicorn.error").error("boom")
    [rec] = _lines(capsys)
    assert (rec["event"], rec["level"]) == ("boom", "error")
    assert "timestamp" in rec


def test_configure_is_idempotent(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging()
    configure_logging()
    structlog.get_logger().info("once")
    assert len(_lines(capsys)) == 1
    access = logging.getLogger("uvicorn.access")
    assert len([f for f in access.filters if type(f).__name__ == "_StripQueryFilter"]) == 1


def test_tracebacks_are_text_and_never_include_locals(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging()

    def boom() -> None:
        authorization = LOCAL_SECRET  # noqa: F841  (a frame local that must not be rendered)
        raise ValueError("kaboom")

    try:
        boom()
    except ValueError:
        logging.getLogger("uvicorn.error").exception("Exception in ASGI application")
        structlog.get_logger().exception("struct_failure")
    recs = _lines(capsys)
    assert len(recs) == 2
    for rec in recs:
        text = json.dumps(rec)
        assert "Traceback (most recent call last)" in str(rec["exception"])
        assert "ValueError: kaboom" in str(rec["exception"])
        assert "traceback object" not in text
        assert LOCAL_SECRET not in text


def test_httpx_and_httpcore_do_not_log_urls(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging()
    for name in ("httpx", "httpcore"):
        assert logging.getLogger(name).getEffectiveLevel() >= logging.WARNING
        logging.getLogger(name).info("HTTP Request: GET https://s3/x?%s", SIGNATURE)
    assert _lines(capsys) == []


@asynccontextmanager
async def _serve(app: FastAPI) -> AsyncIterator[str]:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))  # keep the bound socket: no port race
    port = int(sock.getsockname()[1])
    config = uvicorn.Config(app, log_config=None, access_log=True, lifespan="off")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        async with asyncio.timeout(10):
            while not server.started and not task.done():  # noqa: ASYNC110
                await asyncio.sleep(0.01)
        assert server.started, "uvicorn did not start"
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(task, 10)
        finally:
            sock.close()


def test_uvicorn_default_log_config_does_not_hide_lines(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A prior uvicorn.Config (default LOGGING_CONFIG) must not keep uvicorn loggers off root."""
    uvicorn.Config(FastAPI(), log_level="warning")
    configure_logging()
    logging.getLogger("uvicorn.access").info(
        '%s - "%s %s HTTP/%s" %d', "1.2.3.4", "GET", "/x?a=b", "1.1", 200
    )
    logging.getLogger("uvicorn.error").info("started")
    recs = _lines(capsys)
    assert [r["event"] for r in recs] == ['1.2.3.4 - "GET /x HTTP/1.1" 200', "started"]


async def test_real_requests_log_json_without_secrets(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """401 and 500 paths through a real uvicorn: valid JSON, traceback kept, nothing sensitive."""
    from minion_server.tokencache import ExecutorTokenCache

    configure_logging()
    cfg = ServerSettings(
        database_url=SecretStr("postgresql+asyncpg://x@h/db"),
        s3_access_key=SecretStr("ak"),
        s3_secret_key=SecretStr("sk"),
    )
    app = create_app(cfg)
    app.state.settings = cfg
    app.state.api_keys = None
    app.state.executor_tokens = ExecutorTokenCache(0.0, 10)

    @app.post("/boom")
    async def boom() -> None:
        authorization = BEARER  # noqa: F841
        raise RuntimeError("handler exploded")

    headers = {"Authorization": BEARER}
    async with _serve(app) as base, httpx.AsyncClient(base_url=base) as client:
        r401 = await client.get(f"/api/v1/executors?{SIGNATURE}", headers=headers)
        r500 = await client.post(f"/boom?{SIGNATURE}", headers=headers, content=PAYLOAD.encode())
    assert r401.status_code == 401
    assert r500.status_code == 500
    out = capsys.readouterr().out
    recs = [json.loads(line) for line in out.splitlines() if line.strip()]
    assert recs, "no log lines emitted"
    assert all("timestamp" in r and "level" in r for r in recs)
    errors = [r for r in recs if "exception" in r]
    assert any("RuntimeError: handler exploded" in str(r["exception"]) for r in errors)
    access = [r for r in recs if "/boom" in str(r["event"])]
    assert access and all("?" not in str(r["event"]) for r in access)
    low = out.lower()
    for secret in (BEARER, "s3cr3t-token-value", SIGNATURE, "deadbeefcafe", PAYLOAD):
        assert secret not in out
    assert "authorization" not in low


def test_main_disables_access_log_by_default_and_keeps_json_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _env(monkeypatch)
    seen: dict[str, object] = {}
    monkeypatch.setattr(uvicorn, "run", lambda *a, **kw: seen.update(kw))
    monkeypatch.setattr(app_module, "configure_logging", lambda **_: None)
    app_module.main()
    assert seen["access_log"] is False
    assert seen["log_config"] is None
    monkeypatch.setenv("MINION_SERVER_ACCESS_LOG", "true")
    app_module.main()
    assert seen["access_log"] is True


@pytest.mark.parametrize("access_log", [False, True])
def test_worker_startup_sequence_honours_access_log_setting(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], access_log: bool
) -> None:
    """Replays what a uvicorn worker does: Config.configure_logging() then load() -> factory."""
    _env(monkeypatch)
    monkeypatch.setenv("MINION_SERVER_ACCESS_LOG", "true" if access_log else "false")
    config = uvicorn.Config(
        "minion_server.app:create_app_from_env",
        factory=True,
        log_config=None,
        access_log=access_log,
    )
    config.configure_logging()
    config.load()
    access = logging.getLogger("uvicorn.access")
    assert access.hasHandlers() is access_log
    if access_log:
        access.info('%s - "%s %s HTTP/%s" %d', "1.2.3.4", "GET", "/x", "1.1", 200)
        assert [r["event"] for r in _lines(capsys)] == ['1.2.3.4 - "GET /x HTTP/1.1" 200']
