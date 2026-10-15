import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from minion_common.enums import Engine
from minion_common.schemas import ModelEntry
from minion_server.app import create_app
from minion_server.auth import ExecutorPrincipal
from minion_server.db.session import create_engine, create_sessionmaker
from minion_server.repositories import executors as executors_repo
from minion_server.services.acquisition import acquire, attempt_key
from minion_server.settings import ServerSettings
from minion_server.storage import S3Storage
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from conftest import _auth, _budget, _req, _settings, make_executors, seed_task

N = 60
MODELS = {
    "fake-embed": ModelEntry(
        model_id="fake-embed",
        type="embedding",
        engine=Engine.MLX,
        ref="o/m",
        revision="a" * 40,
        memory_gb=0.1,
    )
}


@pytest.fixture
async def big_engine(fresh_url: str) -> AsyncIterator[AsyncEngine]:
    eng = create_engine(SecretStr(fresh_url), pool_size=N + 5, max_overflow=0)
    yield eng
    await eng.dispose()


@pytest.fixture
async def sm(big_engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    # Isolation: every test gets its own empty database (see `fresh_url`).
    return create_sessionmaker(big_engine)


async def _acquire(
    sm: async_sessionmaker[AsyncSession], p: ExecutorPrincipal, cfg: ServerSettings, **kw: Any
) -> Any:
    async with sm() as s:
        return await acquire(
            s, p, kw.pop("req", _req()), settings=cfg, storage=S3Storage(cfg), models=MODELS, **kw
        )


async def _counts(sm: async_sessionmaker[AsyncSession], name: str) -> dict[str, Any]:
    async with sm() as s:
        rows = (
            await s.execute(
                text(
                    "SELECT count(*), count(DISTINCT subtask_id), count(DISTINCT acquirement_id),"
                    " min(attempt), max(attempt) FROM minion_subtask_acquirement"
                    " WHERE task_name=:n"
                ),
                {"n": name},
            )
        ).one()
        hb = (
            await s.execute(
                text(
                    "SELECT count(*) FROM minion_executor_heartbeat h JOIN"
                    " minion_subtask_acquirement a USING (acquirement_id) WHERE a.task_name=:n"
                ),
                {"n": name},
            )
        ).scalar_one()
        running = (
            await s.execute(
                text(
                    "SELECT count(*), min(attempt), max(attempt) FROM minion_subtask"
                    " WHERE task_name=:n AND status='RUNNING'"
                ),
                {"n": name},
            )
        ).one()
        task = (
            await s.execute(text("SELECT status FROM minion_task WHERE task_name=:n"), {"n": name})
        ).scalar_one()
    return {
        "rows": rows[0],
        "subtasks": rows[1],
        "ids": rows[2],
        "attempts": (rows[3], rows[4]),
        "heartbeats": hb,
        "running": tuple(running),
        "task": task,
    }


def test_attempt_key() -> None:
    assert attempt_key("outputs/t/r/", 7, 2) == "outputs/t/r/_attempts/000007/2.jsonl"


# --- service-level races (AC#1) --------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.parametrize(("name", "n_sub"), [("hot", 1), ("twenty", 20)])
async def test_service_race_unique_owners(
    sm: async_sessionmaker[AsyncSession], fresh_url: str, name: str, n_sub: int
) -> None:
    cfg = _settings(fresh_url)
    await seed_task(sm, f"svc-{name}", n_sub)
    execs = await make_executors(sm, N)
    results = await asyncio.gather(*(_acquire(sm, p, cfg) for p, _ in execs))
    winners = [r for r in results if r is not None]
    assert len(winners) == min(n_sub, N)
    assert len({w.acquirement_id for w in winners}) == len(winners)
    assert len({w.subtask_id for w in winners}) == len(winners)
    assert all(w.attempt == 1 for w in winners)
    c = await _counts(sm, f"svc-{name}")
    assert (c["rows"], c["subtasks"], c["ids"]) == (len(winners),) * 3
    assert c["attempts"] == (1, 1)
    assert c["heartbeats"] == len(winners)
    assert c["running"] == (len(winners), 1, 1)
    assert c["task"] == "RUNNING"


# --- HTTP-level race -------------------------------------------------------------------------


@pytest.fixture
async def http(fresh_url: str) -> AsyncIterator[tuple[AsyncClient, Any]]:
    cfg = _settings(fresh_url)
    app = create_app(cfg)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
    ):
        yield c, app


@pytest.mark.integration
@pytest.mark.parametrize(("name", "n_sub"), [("hot", 1), ("twenty", 20)])
async def test_http_race_unique_owners(
    http: tuple[AsyncClient, Any], name: str, n_sub: int
) -> None:
    c, app = http
    sm = app.state.sessionmaker
    await seed_task(sm, f"http-{name}", n_sub)
    execs = await make_executors(sm, N)
    body = _req().model_dump(mode="json")
    resps = await asyncio.gather(
        *(c.post("/api/v1/executor/acquire", json=body, headers=_auth(t)) for _, t in execs)
    )
    codes = [r.status_code for r in resps]
    assert set(codes) <= {200, 204}, codes
    won = [r.json() for r in resps if r.status_code == 200]
    assert len(won) == min(n_sub, N)
    assert len({w["acquirement_id"] for w in won}) == len(won)
    assert len({w["subtask_id"] for w in won}) == len(won)
    assert all(w["attempt"] == 1 for w in won)
    cnt = await _counts(sm, f"http-{name}")
    assert (cnt["rows"], cnt["subtasks"], cnt["ids"]) == (len(won),) * 3
    assert codes.count(204) == N - len(won)


@pytest.mark.integration
async def test_http_assignment_shape_and_204(http: tuple[AsyncClient, Any]) -> None:
    c, app = http
    sm = app.state.sessionmaker
    ((_, token),) = await make_executors(sm, 1)
    body = _req().model_dump(mode="json")
    empty = await c.post("/api/v1/executor/acquire", json=body, headers=_auth(token))
    assert empty.status_code == 204 and empty.content == b""
    await seed_task(sm, "shape", 4)
    r = await c.post("/api/v1/executor/acquire", json=body, headers=_auth(token))
    assert r.status_code == 200
    a = r.json()
    assert a["attempt"] == 1 and a["model"]["model_id"] == "fake-embed"
    assert a["model"]["revision"] == "a" * 40
    assert "inputs/acq/shape/a.jsonl" in a["input_url"] and "X-Amz-Signature" in a["input_url"]
    sid = a["subtask_id"]
    assert f"outputs/acq/shape/_attempts/{sid:06d}/1.jsonl" in a["output_url"]
    assert (a["byte_start"], a["byte_end"]) == (sid * 100, sid * 100 + 100)
    assert (
        await c.post("/api/v1/executor/acquire", json={}, headers=_auth(token))
    ).status_code == 422
    assert (await c.post("/api/v1/executor/acquire", json=body)).status_code == 401


# --- ordering and filters --------------------------------------------------------------------


@pytest.mark.integration
async def test_preference_before_fifo(sm: async_sessionmaker[AsyncSession], fresh_url: str) -> None:
    cfg = _settings(fresh_url)
    await seed_task(sm, "fifo-old", 2)  # older: global FIFO would serve this first
    await seed_task(sm, "fifo-pref", 2)
    ((p, _),) = await make_executors(sm, 1)
    first = await _acquire(sm, p, cfg, shuffle_head=1)
    assert first.task_name == "fifo-old" and first.subtask_id == 0  # no preference: FIFO
    async with sm() as s, s.begin():
        await executors_repo.set_preference(
            s, p.executor_id, team=None, project=None, task_name="fifo-pref"
        )
    for expected in (0, 1):
        got = await _acquire(sm, p, cfg, shuffle_head=1)
        assert (got.task_name, got.subtask_id) == ("fifo-pref", expected)
    # preferred task exhausted -> falls back to global FIFO
    fallback = await _acquire(sm, p, cfg, shuffle_head=1)
    assert (fallback.task_name, fallback.subtask_id) == ("fifo-old", 1)
    # clearing the preference (all None) restores plain FIFO
    async with sm() as s, s.begin():
        await executors_repo.set_preference(
            s, p.executor_id, team=None, project=None, task_name=None
        )
        assert await executors_repo.get_preference(s, p.executor_id) is None


@pytest.mark.integration
async def test_priority_orders_before_age(
    sm: async_sessionmaker[AsyncSession], fresh_url: str
) -> None:
    cfg = _settings(fresh_url)
    await seed_task(sm, "prio-low", 1)
    await seed_task(sm, "prio-high", 1, priority=5)
    ((p, _),) = await make_executors(sm, 1)
    got = await _acquire(sm, p, cfg, shuffle_head=1)
    assert got.task_name == "prio-high"


@pytest.mark.integration
async def test_filters(sm: async_sessionmaker[AsyncSession], fresh_url: str) -> None:
    cfg = _settings(fresh_url)
    await seed_task(sm, "f-ollama", 1, engine=Engine.OLLAMA)
    await seed_task(sm, "f-big", 1, memory_gb=16.0)
    await seed_task(sm, "f-pool", 1, pool="gold")
    await seed_task(sm, "f-split", 1, status="SPLITTING")
    ((plain, _),) = await make_executors(sm, 1, pool="default")
    ((gold, _),) = await make_executors(sm, 1, pool="gold")

    assert await _acquire(sm, plain, cfg) is None  # mlx only, 8 GB, wrong pool, no splitting
    big = await _acquire(sm, plain, cfg, req=_req(mem=32.0))
    assert big.task_name == "f-big"  # memory admission passes at 32 GB
    olla = await _acquire(sm, plain, cfg, req=_req(engines=(Engine.OLLAMA,)))
    assert olla.task_name == "f-ollama"
    assert (await _acquire(sm, gold, cfg)).task_name == "f-pool"
    assert await _acquire(sm, gold, cfg) is None


# --- register / preference over HTTP ---------------------------------------------------------


def _register_body() -> dict[str, Any]:
    return {
        "hostname": "mbp",
        "chip": "M3 Pro",
        "total_memory_gb": 36,
        "os_version": "15.1",
        "engines": ["mlx"],
        "profile": "balanced",
        "budget": _budget().model_dump(mode="json"),
    }


@pytest.mark.integration
async def test_register_and_preference(http: tuple[AsyncClient, Any]) -> None:
    c, app = http
    sm = app.state.sessionmaker
    ((p, token),) = await make_executors(sm, 1)
    r = await c.post("/api/v1/executor/register", json=_register_body(), headers=_auth(token))
    assert r.status_code == 200 and r.json()["executor_id"] == str(p.executor_id)
    async with sm() as s:
        row = (
            await s.execute(
                text(
                    "SELECT hostname, chip, status, profile FROM minion_executor"
                    " WHERE executor_id=:i"
                ),
                {"i": p.executor_id},
            )
        ).one()
    assert tuple(row) == ("mbp", "M3 Pro", "ACTIVE", "balanced")
    pref = await c.put("/api/v1/executor/preference", json={"team": "acq"}, headers=_auth(token))
    assert pref.status_code == 204
    async with sm() as s:
        assert await executors_repo.get_preference(s, p.executor_id) == ("acq", None, None)
    bad = await c.put(
        "/api/v1/executor/preference", json={"team": "Bad Slug"}, headers=_auth(token)
    )
    assert bad.status_code == 422


@pytest.mark.integration
async def test_revoked_executor_cannot_register(http: tuple[AsyncClient, Any]) -> None:
    c, app = http
    sm = app.state.sessionmaker
    ((p, token),) = await make_executors(sm, 1)
    async with sm() as s, s.begin():
        assert await executors_repo.revoke(s, p.executor_id)
    for method, path, body in [
        ("POST", "/api/v1/executor/register", _register_body()),
        ("PUT", "/api/v1/executor/preference", {"team": "acq"}),
        ("POST", "/api/v1/executor/acquire", _req().model_dump(mode="json")),
    ]:
        r = await c.request(method, path, json=body, headers=_auth(token))
        assert r.status_code == 401, path
    # Even if auth were bypassed (revoked between auth and update), the repo never un-revokes.
    async with sm() as s, s.begin():
        ok = await executors_repo.register(
            s,
            p.executor_id,
            hostname="evil",
            chip="x",
            total_memory_gb=1,
            os_version="1",
            engines=["mlx"],
            profile="balanced",
            budget={},
        )
    assert ok is False
    async with sm() as s:
        row = (
            await s.execute(
                text("SELECT status, hostname FROM minion_executor WHERE executor_id=:i"),
                {"i": p.executor_id},
            )
        ).one()
    assert tuple(row) == ("REVOKED", "(unregistered)")


@pytest.mark.integration
async def test_removed_model_does_not_starve_valid_work(
    sm: async_sessionmaker[AsyncSession], fresh_url: str
) -> None:
    """16+ head candidates whose model left the allow-list must not hide lower-priority work."""
    cfg = _settings(fresh_url)
    await seed_task(sm, "gone", 20, model_id="removed-model", priority=10)  # > CANDIDATE_LIMIT
    await seed_task(sm, "valid", 2, priority=0)
    ((p, _),) = await make_executors(sm, 1)
    got = await _acquire(sm, p, cfg)
    assert got is not None and got.task_name == "valid"
    async with sm() as s:
        gone_rows = (
            await s.execute(
                text("SELECT count(*) FROM minion_subtask_acquirement WHERE task_name='gone'")
            )
        ).scalar_one()
    assert gone_rows == 0


@pytest.mark.integration
async def test_stale_candidate_cannot_reacquire_finished_subtask(
    sm: async_sessionmaker[AsyncSession],
) -> None:
    """Deterministic D5 re-check: a candidate read before another owner finished is rejected."""
    from minion_server.repositories import acquisition as repo

    await seed_task(sm, "stale", 2)
    (a, _), (b, _) = await make_executors(sm, 2)

    async def read() -> list[repo.Candidate]:
        async with sm() as s, s.begin():
            return await repo.candidates(
                s,
                engines=[Engine.MLX],
                model_ids=["fake-embed"],
                memory_gb=8,
                pool="default",
                preference=None,
            )

    stale = await read()
    c0, c1 = stale[0], stale[1]
    async with sm() as s, s.begin():
        won = await repo.try_acquire(s, c0, a.executor_id)
        assert won is not None
    # owner finishes (what complete does): acquirement dropped, subtask terminal
    async with sm() as s, s.begin():
        await s.execute(text("DELETE FROM minion_subtask_acquirement WHERE subtask_id=0"))
        await s.execute(
            text(
                "UPDATE minion_subtask SET status='SUCCEEDED'"
                " WHERE subtask_id=0 AND task_name='stale'"
            )
        )
    async with sm() as s, s.begin():
        assert await repo.try_acquire(s, c0, b.executor_id) is None
    # terminal failure likewise
    async with sm() as s, s.begin():
        assert await repo.try_acquire(s, c1, a.executor_id) is not None
        await s.execute(text("DELETE FROM minion_subtask_acquirement WHERE subtask_id=1"))
        await s.execute(
            text(
                "UPDATE minion_subtask SET status='FAILED' WHERE subtask_id=1 AND task_name='stale'"
            )
        )
    async with sm() as s, s.begin():
        assert await repo.try_acquire(s, c1, b.executor_id) is None
    async with sm() as s:
        n = (
            await s.execute(
                text("SELECT count(*) FROM minion_subtask_acquirement WHERE task_name='stale'")
            )
        ).scalar_one()
    assert n == 0  # the rejected acquires left no acquirement rows behind
