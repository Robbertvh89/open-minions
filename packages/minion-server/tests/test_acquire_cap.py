"""Per-executor cap on open acquirements (TASK-33, ADR-0019)."""

import asyncio
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, cast

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient
from minion_common.enums import Engine
from minion_common.schemas import ModelEntry
from minion_server.app import create_app
from minion_server.auth import ExecutorPrincipal
from minion_server.db.session import create_engine, create_sessionmaker
from minion_server.repositories import acquisition as acq_repo
from minion_server.repositories import executors as executors_repo
from minion_server.services.acquisition import acquire
from minion_server.services.reclaim import reclaim_one
from minion_server.storage import S3Storage
from pydantic import SecretStr
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from conftest import S3Env, _auth, _req, _settings, make_executors, seed_task

BASE = "/api/v1/executor"
BUDGET: dict[str, Any] = {
    "profile": "balanced",
    "memory_gb": 8.0,
    "gpu_duty_cycle": 0.5,
    "max_batch_size": 8,
}
ACQ = {"engines": ["mlx"], "budget": BUDGET, "output_upload": "post"}
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
Make = Callable[..., Awaitable[tuple[AsyncClient, Any]]]

pytestmark = pytest.mark.integration


@pytest.fixture
async def engine(fresh_url: str) -> AsyncIterator[AsyncEngine]:
    eng = create_engine(SecretStr(fresh_url), pool_size=30, max_overflow=0, pool_timeout=60)
    yield eng
    await eng.dispose()


@pytest.fixture
async def sm(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return create_sessionmaker(engine)


@pytest.fixture
async def make(fresh_url: str, s3: S3Env) -> AsyncIterator[Make]:
    stack: list[Any] = []

    async def build(**over: Any) -> tuple[AsyncClient, Any]:
        cfg = _settings(fresh_url).model_copy(
            update={
                "s3_endpoint_url": s3.endpoint_url,
                "public_s3_endpoint_url": s3.endpoint_url,
                "s3_access_key": SecretStr(s3.access_key),
                "s3_secret_key": SecretStr(s3.secret_key),
                "s3_bucket": s3.bucket,
                **over,
            }
        )
        app = create_app(cfg)
        lifespan = app.router.lifespan_context(app)
        await lifespan.__aenter__()
        c = AsyncClient(transport=ASGITransport(app=app), base_url="http://t")
        stack.append((lifespan, c))
        return c, app

    yield build
    for lifespan, c in reversed(stack):
        await c.aclose()
        await lifespan.__aexit__(None, None, None)


async def _held(sm: async_sessionmaker[AsyncSession], eid: uuid.UUID | str | None = None) -> int:
    q = "SELECT count(*) FROM minion_subtask_acquirement"
    async with sm() as s:
        if eid is None:
            return int((await s.execute(text(q))).scalar_one())
        res = await s.execute(text(q + " WHERE executor_id = CAST(:e AS uuid)"), {"e": str(eid)})
        return int(res.scalar_one())


async def _try(
    sm: async_sessionmaker[AsyncSession], p: ExecutorPrincipal, cfg: Any
) -> Any | HTTPException:
    async with sm() as s:
        try:
            return await acquire(
                s, p, _req(), settings=cfg, storage=S3Storage(cfg), models=MODELS, window=4
            )
        except HTTPException as exc:
            return exc


def _is_cap(x: Any) -> bool:
    return (
        isinstance(x, HTTPException)
        and x.status_code == 429
        and cast(dict[str, str], x.detail)["code"] == "TOO_MANY_ACQUIREMENTS"
        and x.headers is not None
        and x.headers["Retry-After"] == "30"
    )


async def test_concurrent_acquires_of_one_executor_never_exceed_the_cap(
    sm: async_sessionmaker[AsyncSession], fresh_url: str
) -> None:
    cfg = _settings(fresh_url).model_copy(update={"max_open_acquirements": 3})
    [(p, _)] = await make_executors(sm, 1)
    for i in range(10):
        await seed_task(sm, f"cap{i}", 20, team=f"cap{i}")
        async with sm() as s, s.begin():  # only this iteration's work is claimable
            await s.execute(
                text("UPDATE minion_task SET status='PENDING' WHERE task_name = :n"),
                {"n": f"cap{i}"},
            )
        res = await asyncio.gather(*[_try(sm, p, cfg) for _ in range(20)])
        assigned = [r for r in res if not isinstance(r, HTTPException)]
        assert len(assigned) == 3, [type(r) for r in res]
        assert sum(_is_cap(r) for r in res) == 17
        assert await _held(sm, p.executor_id) == 3
        async with sm() as s, s.begin():  # clean up for the next iteration
            await s.execute(text("DELETE FROM minion_executor_heartbeat"))
            await s.execute(text("DELETE FROM minion_subtask_acquirement"))
            await s.execute(text("DELETE FROM minion_task WHERE task_name = :n"), {"n": f"cap{i}"})


async def test_without_the_executor_lock_the_cap_overshoots(
    sm: async_sessionmaker[AsyncSession], fresh_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Documents why the lock exists: the count alone is a check-then-act race (best effort)."""
    cfg = _settings(fresh_url).model_copy(update={"max_open_acquirements": 3})
    [(p, _)] = await make_executors(sm, 1)
    await seed_task(sm, "nolock", 40, team="nolock")

    async def no_lock(session: AsyncSession, executor_id: uuid.UUID) -> bool:
        return True

    monkeypatch.setattr(acq_repo, "lock_executor_for_claim", no_lock)
    over = 0
    for _ in range(5):
        await asyncio.gather(*[_try(sm, p, cfg) for _ in range(20)])
        over = max(over, await _held(sm, p.executor_id))
        if over > 3:
            break
    if over <= 3:
        pytest.skip("the race did not show up in this run")
    assert over > 3


async def test_three_workers_share_one_cap(make: Make, monkeypatch: pytest.MonkeyPatch) -> None:
    apps = [await make(max_open_acquirements=2, acquire_concurrency=8) for _ in range(3)]
    sm = apps[0][1].state.sessionmaker
    await seed_task(sm, "mw", 12, team="mw")
    [(p, tok)] = await make_executors(sm, 1)
    real = executors_repo.get_preference

    async def slow(*a: Any, **kw: Any) -> Any:
        await asyncio.sleep(0.4)  # make the 6 acquires overlap
        return await real(*a, **kw)

    monkeypatch.setattr(executors_repo, "get_preference", slow)
    rs = await asyncio.gather(
        *[
            c.post(f"{BASE}/acquire", json=ACQ, headers=_auth(tok))
            for c, _ in apps
            for _ in range(2)
        ]
    )
    codes = [r.status_code for r in rs]
    assert codes.count(200) == 2 and codes.count(429) == 4, codes
    assert await _held(sm, p.executor_id) == 2


async def test_capped_acquire_reads_no_candidates_and_commits_nothing(make: Make) -> None:
    c, app = await make(max_open_acquirements=1, metrics_enabled=True)
    sm = app.state.sessionmaker
    await seed_task(sm, "nc", 5, team="nc")
    [(_, tok)] = await make_executors(sm, 1)
    assert (await c.post(f"{BASE}/acquire", json=ACQ, headers=_auth(tok))).status_code == 200
    seen: list[str] = []

    def record(conn: Any, cur: Any, statement: str, *a: Any) -> None:
        seen.append(statement)

    event.listen(app.state.engine.sync_engine, "before_cursor_execute", record)
    r = await c.post(f"{BASE}/acquire", json=ACQ, headers=_auth(tok))
    assert r.status_code == 429, r.text
    assert r.json()["detail"]["code"] == "TOO_MANY_ACQUIREMENTS"
    assert r.headers["Retry-After"] == "30"
    assert not [q for q in seen if "FROM minion_subtask " in q or "INSERT INTO minion_subtask" in q]
    assert any("FOR NO KEY UPDATE" in q for q in seen)
    assert await _held(sm) == 1
    assert float(app.state.metrics.acquires.labels(outcome="capped")._value.get()) == 1.0


async def test_freed_slot_and_other_executors(make: Make) -> None:
    c, app = await make(max_open_acquirements=2)
    sm = app.state.sessionmaker
    await seed_task(sm, "fr", 10, team="fr")
    [(_, tok), (_, tok2)] = await make_executors(sm, 2)

    async def acq(t: str) -> Any:
        return await c.post(f"{BASE}/acquire", json=ACQ, headers=_auth(t))

    a = await acq(tok)
    b = await acq(tok)
    assert a.status_code == b.status_code == 200
    assert (await acq(tok)).status_code == 429
    assert (await acq(tok2)).status_code == 200  # another executor is unaffected
    rel = await c.post(f"{BASE}/subtasks/{a.json()['acquirement_id']}/release", headers=_auth(tok))
    assert rel.status_code == 204, rel.text
    assert (await acq(tok)).status_code == 200  # a slot freed by release
    assert (await acq(tok)).status_code == 429
    # a watchdog reclaim of an unacknowledged acquirement frees a slot too
    async with sm() as s, s.begin():
        await s.execute(
            text("UPDATE minion_executor_heartbeat SET last_seen_at = now() - interval '1 day'")
        )
    async with sm() as s, s.begin():
        await reclaim_one(s, uuid.UUID(b.json()["acquirement_id"]), timeout_s=900)
    assert (await acq(tok)).status_code == 200


async def test_revoked_executor_gets_401_on_acquire_despite_warm_cache(make: Make) -> None:
    c, app = await make(executor_auth_cache_ttl_s=60)
    sm = app.state.sessionmaker
    await seed_task(sm, "rv", 3, team="rv")
    [(p, tok)] = await make_executors(sm, 1)
    assert (await c.post(f"{BASE}/acquire", json=ACQ, headers=_auth(tok))).status_code == 200
    async with sm() as s, s.begin():
        assert await executors_repo.revoke(s, p.executor_id)
    r = await c.post(f"{BASE}/acquire", json=ACQ, headers=_auth(tok))
    assert r.status_code == 401
    assert await _held(sm, p.executor_id) == 1


async def test_lock_helper_refuses_missing_and_revoked(
    sm: async_sessionmaker[AsyncSession],
) -> None:
    [(p, _)] = await make_executors(sm, 1)
    async with sm() as s, s.begin():
        assert await acq_repo.lock_executor_for_claim(s, p.executor_id)
        assert not await acq_repo.lock_executor_for_claim(s, uuid.uuid4())
    async with sm() as s, s.begin():
        assert await executors_repo.revoke(s, p.executor_id)
    async with sm() as s, s.begin():
        assert not await acq_repo.lock_executor_for_claim(s, p.executor_id)


async def test_lock_wait_past_the_deadline_answers_503(
    make: Make, monkeypatch: pytest.MonkeyPatch
) -> None:
    c, app = await make(acquire_deadline_s=1.0, metrics_enabled=True)
    sm = app.state.sessionmaker
    await seed_task(sm, "lw", 3, team="lw")
    [(p, tok)] = await make_executors(sm, 1)
    real = acq_repo.set_statement_timeout
    holder = sm()

    async def hold_then_set(*a: Any, **kw: Any) -> Any:
        # the touch txn has committed; another txn now holds the executor row before the claim
        await holder.begin()
        await holder.execute(
            text("SELECT 1 FROM minion_executor WHERE executor_id = CAST(:e AS uuid) FOR UPDATE"),
            {"e": str(p.executor_id)},
        )
        return await real(*a, **kw)

    monkeypatch.setattr(acq_repo, "set_statement_timeout", hold_then_set)
    try:
        r = await c.post(f"{BASE}/acquire", json=ACQ, headers=_auth(tok))
    finally:
        await holder.rollback()
        await holder.close()
    assert r.status_code == 503, r.text
    assert r.json()["detail"]["code"] == "ACQUIRE_DEADLINE"
    assert await _held(sm) == 0


async def test_open_acquirement_count_uses_the_executor_index(fresh_url: str) -> None:
    eng = create_async_engine(fresh_url, isolation_level="AUTOCOMMIT")
    seed = (
        "INSERT INTO minion_subtask_acquirement"
        " (acquirement_id, team, project, task_name, subtask_id, executor_id, attempt)"
        " SELECT gen_random_uuid(), 't', 'p', 'n', g, gen_random_uuid(), 1"
        " FROM generate_series(1, 20000) AS g"
    )
    try:
        async with eng.connect() as conn:
            await conn.execute(text("SET session_replication_role = replica"))
            await conn.execute(text(seed))
            await conn.execute(text("RESET session_replication_role"))
            await conn.execute(text("ANALYZE minion_subtask_acquirement"))
            eid = (
                await conn.execute(
                    text("SELECT executor_id FROM minion_subtask_acquirement LIMIT 1")
                )
            ).scalar_one()
            plan = "\n".join(
                r[0]
                for r in await conn.execute(
                    text(
                        "EXPLAIN SELECT count(*) FROM minion_subtask_acquirement"  # noqa: S608
                        f" WHERE executor_id = '{eid}'"
                    )
                )
            )
    finally:
        await eng.dispose()
    assert "ix_minion_subtask_acquirement_executor_id" in plan, plan
