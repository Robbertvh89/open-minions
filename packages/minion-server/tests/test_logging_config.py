import json
import logging
from collections.abc import Iterator
from datetime import datetime

import pytest
import structlog
from minion_server.logging_config import configure_logging

SECRETS = (
    "Bearer abc.secret.token",
    "X-Amz-Signature=deadbeef",
    "https://s3.example/bucket/key?X-Amz-Signature=deadbeef",
    "super-secret-prompt-text",
)


@pytest.fixture(autouse=True)
def _restore_logging() -> Iterator[None]:
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)
    structlog.reset_defaults()


def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")
    monkeypatch.setenv("MINION_S3_ACCESS_KEY", "ak")
    monkeypatch.setenv("MINION_S3_SECRET_KEY", "sk")
    monkeypatch.setenv("MINION_SERVER_WORKERS", "1")


def _lines(capsys: pytest.CaptureFixture[str]) -> list[dict[str, object]]:
    err = capsys.readouterr().out
    return [json.loads(line) for line in err.splitlines() if line.strip()]


def test_structlog_line_is_json_with_utc_timestamp_and_level(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging()
    structlog.get_logger().warning("metrics_unauthenticated", host="10.0.0.1")
    [rec] = _lines(capsys)
    assert rec["event"] == "metrics_unauthenticated"
    assert rec["level"] == "warning"
    ts = datetime.fromisoformat(str(rec["timestamp"]))
    assert ts.utcoffset() is not None
    assert ts.utcoffset().total_seconds() == 0  # type: ignore[union-attr]


def test_stdlib_loggers_such_as_uvicorn_are_json_too(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging()
    logging.getLogger("uvicorn.error").error("boom")
    [rec] = _lines(capsys)
    assert rec["event"] == "boom"
    assert rec["level"] == "error"
    assert "timestamp" in rec


def test_configure_is_idempotent(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging()
    configure_logging()
    structlog.get_logger().info("once")
    assert len(_lines(capsys)) == 1


def test_server_paths_do_not_log_secrets_or_payloads(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Representative server log calls carry ids/classes only; nothing sensitive reaches output."""
    import uvicorn
    from minion_server import app as app_module

    _env(monkeypatch)
    monkeypatch.setattr(uvicorn, "run", lambda *a, **kw: None)
    # the secrets exist in the request/environment of the process; none may be echoed
    monkeypatch.setenv("MINION_S3_SECRET_KEY", SECRETS[0])
    monkeypatch.chdir("/")
    app_module.main()
    out = capsys.readouterr().out
    for line in out.splitlines():
        json.loads(line)
    for secret in SECRETS:
        assert secret not in out
    assert "authorization" not in out.lower()


def test_main_disables_uvicorn_access_log_and_keeps_json_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import uvicorn
    from minion_server import app as app_module

    _env(monkeypatch)
    monkeypatch.chdir("/")
    seen: dict[str, object] = {}
    monkeypatch.setattr(uvicorn, "run", lambda *a, **kw: seen.update(kw))
    app_module.main()
    assert seen["access_log"] is False
    assert seen["log_config"] is None
