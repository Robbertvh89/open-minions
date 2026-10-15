"""Watchdog settings, loop and process (TASK-3.2 S5). Integration tests use a fresh DB."""

import asyncio
import json
import os
import signal
import sys
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from minion_server.app import create_app
from minion_server.services import reclaim as reclaim_service
from minion_server.settings import ServerSettings
from minion_watchdog import main as wd
from minion_watchdog.settings import WatchdogSettings
from pydantic import SecretStr, ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from conftest import TEST_MODELS_FILE, S3Env, _auth, _req, make_executors, seed_task

Sm = async_sessionmaker[AsyncSession]
EXPIRED_SQL = "SELECT count(*) FROM minion_subtask_attempt WHERE outcome='EXPIRED'"
URL = "postgresql+asyncpg://u:p@h/db"


def _cfg(url: str = URL, **kw: Any) -> WatchdogSettings:
    return WatchdogSettings(database_url=SecretStr(url), **kw)


# --- unit ----------------------------------------------------------------------------------------


def test_defaults(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)  # no .env
    cfg = _cfg()
    assert (cfg.heartbeat_timeout_s, cfg.watchdog_interval_s, cfg.watchdog_batch_size) == (
        900.0,
        60.0,
        500,
    )
    assert cfg.max_lease_s == 6 * 3600 and cfg.db_pool_size == 2


def test_database_url_required(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("MINION_DATABASE_URL", raising=False)
    with pytest.raises(ValidationError):
        WatchdogSettings()


@pytest.mark.parametrize(
    "kw",
    [
        {"heartbeat_timeout_s": 0},
        {"watchdog_interval_s": 0},
        {"watchdog_batch_size": 0},
        {"db_pool_size": 0},
        {"heartbeat_timeout_s": 900, "max_lease_s": 900},  # lease must exceed the timeout
    ],
)
def test_invalid_settings(kw: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        _cfg(**kw)


def test_backoff_doubles_and_caps() -> None:
    assert wd.backoff_s(1.0, 1, 300) == 2.0
    assert wd.backoff_s(1.0, 3, 300) == 8.0
    assert wd.backoff_s(1.0, 50, 300) == 300.0
    assert wd.backoff_s(600.0, 1, 300) == 600.0  # never below the interval


# --- integration ---------------------------------------------------------------------------------


@pytest.fixture
async def env(fresh_url: str, s3: S3Env) -> AsyncIterator[tuple[AsyncClient, Any, str]]:
    cfg = ServerSettings(
        database_url=SecretStr(fresh_url),
        s3_endpoint_url=s3.endpoint_url,
        s3_access_key=SecretStr(s3.access_key),
        s3_secret_key=SecretStr(s3.secret_key),
        s3_bucket=s3.bucket,
        models_file=TEST_MODELS_FILE,
        splitter_poll_s=3600,
        heartbeat_min_interval_s=0,
    )
    app = create_app(cfg)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
    ):
        yield c, app, fresh_url


async def _acquire(c: AsyncClient, token: str) -> dict[str, Any]:
    r = await c.post(
        "/api/v1/executor/acquire", json=_req().model_dump(mode="json"), headers=_auth(token)
    )
    assert r.status_code == 200, r.text
    a: dict[str, Any] = r.json()
    return a


async def _scalar(sm: Sm, sql: str) -> Any:
    async with sm() as s:
        return (await s.execute(text(sql))).scalar_one()


async def _backdate_beats(sm: Sm) -> None:
    async with sm() as s, s.begin():
        await s.execute(
            text("UPDATE minion_executor_heartbeat SET last_seen_at = now() - interval '1 hour'")
        )


async def _wait_for(sm: Sm, sql: str, want: Any, timeout_s: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if await _scalar(sm, sql) == want:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"{sql!r} never became {want!r}")


@pytest.mark.integration
async def test_run_watchdog_reclaims_sweeps_and_stops(env: tuple[AsyncClient, Any, str]) -> None:
    c, app, url = env
    sm: Sm = app.state.sessionmaker
    await seed_task(sm, "wd", 3)
    await seed_task(sm, "swept", 2)
    [(_, tok)] = await make_executors(sm, 1)
    await _acquire(c, tok)
    await _backdate_beats(sm)
    async with sm() as s, s.begin():  # a cancelled task with a straggler PENDING subtask
        await s.execute(
            text(
                "UPDATE minion_task SET status='CANCELLED', finished_at=now() "
                "WHERE task_name='swept'"
            )
        )
    cfg = _cfg(url, watchdog_interval_s=0.05, heartbeat_timeout_s=0.5, max_lease_s=3600)
    stop = asyncio.Event()
    task = asyncio.create_task(wd.run_watchdog(sm, cfg, stop))
    try:
        await _wait_for(sm, EXPIRED_SQL, 1)
        await _wait_for(sm, "SELECT count(*) FROM minion_subtask WHERE status='CANCELLED'", 2)
        assert await _scalar(sm, "SELECT count(*) FROM minion_subtask_acquirement") == 0
    finally:
        t0 = time.monotonic()
        stop.set()
        await asyncio.wait_for(task, 1.0)
    assert time.monotonic() - t0 < 1.0


@pytest.mark.integration
async def test_tick_error_does_not_kill_loop(
    env: tuple[AsyncClient, Any, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    _, app, url = env
    sm: Sm = app.state.sessionmaker
    real = reclaim_service.reclaim_stale
    calls = {"n": 0}

    async def flaky(*a: Any, **kw: Any) -> Any:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ConnectionError("db down")
        return await real(*a, **kw)

    monkeypatch.setattr(wd, "reclaim_stale", flaky)
    stop = asyncio.Event()
    task = asyncio.create_task(wd.run_watchdog(sm, _cfg(url, watchdog_interval_s=0.05), stop))
    deadline = time.monotonic() + 5
    while calls["n"] < 3 and time.monotonic() < deadline:  # noqa: ASYNC110
        await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, 1.0)
    assert calls["n"] >= 3  # failed once, backed off, then kept ticking


@pytest.mark.integration
async def test_watchdog_process_reclaims_and_exits_on_sigterm(
    env: tuple[AsyncClient, Any, str], tmp_path: Path
) -> None:
    c, app, url = env
    sm: Sm = app.state.sessionmaker
    await seed_task(sm, "proc", 1)
    [(_, tok)] = await make_executors(sm, 1)
    await _acquire(c, tok)
    await _backdate_beats(sm)
    penv = {
        **os.environ,
        "MINION_DATABASE_URL": url,
        "MINION_WATCHDOG_INTERVAL_S": "0.2",
        "MINION_HEARTBEAT_TIMEOUT_S": "900",
    }
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "from minion_watchdog.main import main; main()",
        env=penv,
        cwd=tmp_path,  # no .env picked up
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    try:
        await _wait_for(sm, EXPIRED_SQL, 1, 20)
        assert await _scalar(sm, "SELECT status FROM minion_subtask") == "PENDING"
        proc.send_signal(signal.SIGTERM)
        raw, _ = await asyncio.wait_for(proc.communicate(), 5)
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
    out = raw.decode()
    assert proc.returncode == 0, out
    events = [json.loads(line)["event"] for line in out.splitlines() if line.startswith("{")]
    assert "watchdog_started" in events and "watchdog_stopped" in events
    assert "u:p" not in out and "token" not in out.lower()


@pytest.mark.integration
async def test_tick_repeats_while_batches_are_full(env: tuple[AsyncClient, Any, str]) -> None:
    c, app, url = env
    sm: Sm = app.state.sessionmaker
    await seed_task(sm, "many", 5)
    execs = await make_executors(sm, 5)
    for _, tok in execs:
        await _acquire(c, tok)
    await _backdate_beats(sm)
    cfg = _cfg(url, watchdog_batch_size=2, heartbeat_timeout_s=900)
    await wd.tick(sm, cfg, asyncio.Event())
    assert await _scalar(sm, EXPIRED_SQL) == 5  # 2 + 2 + 1 in one tick
    assert await _scalar(sm, "SELECT count(*) FROM minion_subtask_acquirement") == 0


@pytest.mark.integration
async def test_tick_round_cap(
    env: tuple[AsyncClient, Any, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    _, app, url = env
    sm: Sm = app.state.sessionmaker
    calls = {"n": 0}

    async def always_full(*a: Any, **kw: Any) -> Any:
        calls["n"] += 1
        return {reclaim_service.ReclaimResult.EXPIRED_REQUEUED: 2}

    monkeypatch.setattr(wd, "reclaim_stale", always_full)
    monkeypatch.setattr(wd, "MAX_ROUNDS_PER_TICK", 4)
    await wd.tick(sm, _cfg(url, watchdog_batch_size=2), asyncio.Event())
    assert calls["n"] == 4


@pytest.mark.integration
async def test_non_expired_results_do_not_repeat(
    env: tuple[AsyncClient, Any, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    _, app, url = env
    sm: Sm = app.state.sessionmaker
    calls = {"n": 0}

    async def skipped(*a: Any, **kw: Any) -> Any:
        calls["n"] += 1
        return {reclaim_service.ReclaimResult.SKIPPED: 5}

    monkeypatch.setattr(wd, "reclaim_stale", skipped)
    await wd.tick(sm, _cfg(url, watchdog_batch_size=2), asyncio.Event())
    assert calls["n"] == 1


async def test_stop_interrupts_hanging_tick(monkeypatch: pytest.MonkeyPatch) -> None:
    started = asyncio.Event()
    cancelled = {"v": False}

    async def hang(*a: Any, **kw: Any) -> Any:
        started.set()
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled["v"] = True
            raise

    monkeypatch.setattr(wd, "reclaim_stale", hang)
    stop = asyncio.Event()
    sm: Any = None  # never used: the stub replaces every DB call
    task = asyncio.create_task(wd.run_watchdog(sm, _cfg(), stop))
    await asyncio.wait_for(started.wait(), 2)
    t0 = time.monotonic()
    stop.set()
    await asyncio.wait_for(task, 1.0)
    assert time.monotonic() - t0 < 0.5 and cancelled["v"]


def test_command_timeout_setting() -> None:
    assert _cfg().db_command_timeout_s == 30.0
    with pytest.raises(ValidationError):
        _cfg(db_command_timeout_s=0)
