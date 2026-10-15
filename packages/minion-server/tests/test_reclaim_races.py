"""Watchdog reclaim, retry budget and release cap (TASK-3.1 S3, AC#1 complete-vs-reclaim race).

Every test runs on its own empty database (`fresh_url`). Heartbeats are backdated by an hour and
the reclaim timeout is 900 s, so only deliberately backdated acquirements are stale.
"""

import asyncio
import hashlib
import random
from collections import Counter
from collections.abc import AsyncIterator
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from minion_common.enums import SubtaskStatus
from minion_server.app import create_app
from minion_server.services.completion import next_status
from minion_server.services.reclaim import ReclaimResult, reclaim_one, reclaim_stale
from minion_server.settings import ServerSettings
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from conftest import TEST_MODELS_FILE, S3Env, _auth, _req, make_executors, seed_task

BASE = "/api/v1/executor"
DATA = b'{"id":"a"}\n{"id":"b"}\n'
TIMEOUT = 900.0
FAIL = {"retryable": True, "code": "OOM", "message": "boom"}
Sm = async_sessionmaker[AsyncSession]


def _cfg(url: str, s3: S3Env, **extra: Any) -> ServerSettings:
    return ServerSettings(
        database_url=SecretStr(url),
        s3_endpoint_url=s3.endpoint_url,
        s3_access_key=SecretStr(s3.access_key),
        s3_secret_key=SecretStr(s3.secret_key),
        s3_bucket=s3.bucket,
        s3_region=s3.region,
        models_file=TEST_MODELS_FILE,
        splitter_poll_s=3600,
        db_pool_size=60,
        **extra,
    )


@pytest.fixture
async def env(fresh_url: str, s3: S3Env) -> AsyncIterator[tuple[AsyncClient, Any]]:
    app = create_app(_cfg(fresh_url, s3))
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
    ):
        yield c, app


@pytest.fixture
async def env_cap2(fresh_url: str, s3: S3Env) -> AsyncIterator[tuple[AsyncClient, Any]]:
    app = create_app(_cfg(fresh_url, s3, release_limit=2))
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
    ):
        yield c, app


def _put_output(s3: S3Env, team: str, a: dict[str, Any]) -> str:
    base = f"outputs/{team}/{a['task_name']}"
    key = f"{base}/_attempts/{a['subtask_id']:06d}/{a['attempt']}.jsonl"
    s3.client.put_object(Bucket=s3.bucket, Key=key, Body=DATA)
    return f"{base}/{a['subtask_id']:06d}.jsonl"


def _exists(s3: S3Env, key: str) -> bool:
    return bool(s3.client.list_objects_v2(Bucket=s3.bucket, Prefix=key).get("Contents"))


CBODY = {
    "record_count": 10,
    "byte_size": len(DATA),
    "sha256": hashlib.sha256(DATA).hexdigest(),
}


async def _acquire(c: AsyncClient, token: str) -> dict[str, Any]:
    r = await c.post(f"{BASE}/acquire", json=_req().model_dump(mode="json"), headers=_auth(token))
    assert r.status_code == 200, r.text
    a: dict[str, Any] = r.json()
    return a


async def _acquire_all(c: AsyncClient, execs: list[Any]) -> list[dict[str, Any]]:
    return list(await asyncio.gather(*(_acquire(c, t) for _, t in execs)))


async def _rows(sm: Sm, sql: str, **p: Any) -> list[tuple[Any, ...]]:
    async with sm() as s:
        return [tuple(r) for r in (await s.execute(text(sql), p)).all()]


async def _backdate(sm: Sm, acq_ids: list[str] | None = None) -> None:
    sql = "UPDATE minion_executor_heartbeat SET last_seen_at = now() - interval '1 hour'"
    p: dict[str, Any] = {}
    if acq_ids is not None:
        sql += " WHERE acquirement_id = ANY(CAST(:ids AS uuid[]))"
        p["ids"] = acq_ids
    async with sm() as s, s.begin():
        await s.execute(text(sql), p)


async def _reclaim(sm: Sm, acq_id: str | Any) -> ReclaimResult:
    import uuid

    async with sm() as s, s.begin():
        return await reclaim_one(s, uuid.UUID(str(acq_id)), timeout_s=TIMEOUT)


def _call(c: AsyncClient, a: dict[str, Any], tok: str, op: str) -> Any:
    body = {"complete": CBODY, "fail": FAIL, "release": None, "heartbeat": {}}[op]
    return c.post(f"{BASE}/subtasks/{a['acquirement_id']}/{op}", json=body, headers=_auth(tok))


# --- pure ---------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("used", "mx", "retryable", "cancelled", "want"),
    [
        (1, 3, True, False, SubtaskStatus.PENDING),
        (2, 3, True, False, SubtaskStatus.PENDING),
        (3, 3, True, False, SubtaskStatus.FAILED),
        (1, 3, False, False, SubtaskStatus.FAILED),
        (1, 3, True, True, SubtaskStatus.CANCELLED),
        (3, 3, False, True, SubtaskStatus.CANCELLED),
    ],
)
def test_next_status(
    used: int, mx: int, retryable: bool, cancelled: bool, want: SubtaskStatus
) -> None:
    assert next_status(used, mx, retryable=retryable, task_cancelled=cancelled) == want


# --- AC#1: complete vs reclaim ------------------------------------------------------------------


@pytest.mark.integration
async def test_complete_vs_reclaim_randomized(env: tuple[AsyncClient, Any], s3: S3Env) -> None:
    c, app = env
    sm: Sm = app.state.sessionmaker
    rng = random.Random(1234)  # noqa: S311
    n, rounds = 16, 4
    complete_wins = 0
    execs = await make_executors(sm, n)
    for rnd in range(rounds):
        team, name = f"rr{rnd}", f"rr{rnd}"
        await seed_task(sm, name, n, team=team)
        owned = await _acquire_all(c, execs)
        dsts = {a["subtask_id"]: _put_output(s3, team, a) for a in owned}
        await _backdate(sm)

        async def complete(a: dict[str, Any], tok: str) -> int:
            await asyncio.sleep(rng.random() * 0.01)
            return int((await _call(c, a, tok, "complete")).status_code)

        async def reclaim(a: dict[str, Any]) -> ReclaimResult:
            await asyncio.sleep(rng.random() * 0.06)
            return await _reclaim(sm, a["acquirement_id"])

        results = await asyncio.gather(
            *(
                asyncio.gather(complete(a, t), reclaim(a))
                for a, (_, t) in zip(owned, execs, strict=True)
            )
        )
        await reclaim_stale(sm, timeout_s=TIMEOUT, batch_size=100)  # mop-up must be a no-op

        wins = 0
        for a, (code, rec) in zip(owned, results, strict=True):
            sid = a["subtask_id"]
            ((status, outuri),) = await _rows(
                sm,
                "SELECT status, output_uri FROM minion_subtask"
                " WHERE task_name=:n AND subtask_id=:i",
                n=name,
                i=sid,
            )
            att = await _rows(
                sm,
                "SELECT outcome FROM minion_subtask_attempt WHERE task_name=:n AND subtask_id=:i",
                n=name,
                i=sid,
            )
            acq = await _rows(
                sm,
                "SELECT count(*) FROM minion_subtask_acquirement"
                " WHERE task_name=:n AND subtask_id=:i",
                n=name,
                i=sid,
            )
            assert len(att) == 1 and acq == [(0,)]
            if code == 204:
                wins += 1
                assert rec == ReclaimResult.SKIPPED  # the acquirement row is already gone
                assert (status, att[0][0]) == ("SUCCEEDED", "SUCCEEDED")
                assert outuri is not None and outuri.endswith(dsts[sid])
                assert _exists(s3, dsts[sid])
            else:
                assert code == 409, code
                assert rec == ReclaimResult.EXPIRED_REQUEUED
                assert (status, att[0][0], outuri) == ("PENDING", "EXPIRED", None)
                assert not _exists(s3, dsts[sid])  # nothing promoted
        ((succ, failed, tstatus),) = await _rows(
            sm,
            "SELECT succeeded_subtasks, failed_subtasks, status FROM minion_task"
            " WHERE task_name=:n",
            n=name,
        )
        complete_wins += wins
        assert (succ, failed) == (wins, 0)
        assert tstatus == ("SUCCEEDED" if wins == n else "RUNNING")
        # drain: finish this round's leftovers so the next round starts from an empty queue
        await _cancel_pending(sm, name)
    # both orderings must have been exercised, else the race is not actually being tested
    assert 0 < complete_wins < n * rounds, complete_wins


async def _cancel_pending(sm: Sm, name: str) -> None:
    async with sm() as s, s.begin():
        await s.execute(
            text(
                "UPDATE minion_subtask SET status='CANCELLED' WHERE task_name=:n"
                " AND status='PENDING'"
            ),
            {"n": name},
        )
        await s.execute(
            text("UPDATE minion_task SET status='CANCELLED' WHERE task_name=:n"), {"n": name}
        )


@pytest.mark.integration
async def test_complete_first_reclaim_skipped(
    env: tuple[AsyncClient, Any], s3: S3Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    c, app = env
    sm: Sm = app.state.sessionmaker
    await seed_task(sm, "cf", 1, team="cf")
    ((_, tok),) = await make_executors(sm, 1)
    a = await _acquire(c, tok)
    dst = _put_output(s3, "cf", a)
    await _backdate(sm)

    storage = app.state.storage
    real_copy = storage.copy
    entered, go = asyncio.Event(), asyncio.Event()

    async def slow_copy(*args: Any, **kw: Any) -> Any:
        entered.set()
        await go.wait()
        return await real_copy(*args, **kw)

    monkeypatch.setattr(storage, "copy", slow_copy)
    task = asyncio.create_task(_call(c, a, tok, "complete"))
    await asyncio.wait_for(entered.wait(), 10)
    # complete holds the acquirement lock: the watchdog skips instead of blocking
    assert await _reclaim(sm, a["acquirement_id"]) == ReclaimResult.SKIPPED
    go.set()
    assert (await task).status_code == 204
    assert await _reclaim(sm, a["acquirement_id"]) == ReclaimResult.SKIPPED  # gone
    assert not await reclaim_stale(sm, timeout_s=TIMEOUT, batch_size=10)
    assert _exists(s3, dst)
    assert await _rows(sm, "SELECT outcome FROM minion_subtask_attempt") == [("SUCCEEDED",)]


async def _wait_lock_waiter(sm: Sm, deadline_s: float = 10.0) -> None:
    loop = asyncio.get_running_loop()
    end = loop.time() + deadline_s
    while loop.time() < end:
        n = await _rows(sm, "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'")
        if n[0][0] > 0:
            return
        await asyncio.sleep(0.02)
    raise AssertionError("no backend ever waited on a lock")


@pytest.mark.integration
async def test_reclaim_first_complete_rejected(env: tuple[AsyncClient, Any], s3: S3Env) -> None:
    c, app = env
    sm: Sm = app.state.sessionmaker
    await seed_task(sm, "rf", 1, team="rf")
    ((_, tok),) = await make_executors(sm, 1)
    a = await _acquire(c, tok)
    dst = _put_output(s3, "rf", a)
    await _backdate(sm)

    import uuid

    async with sm() as s, s.begin():
        res = await reclaim_one(s, uuid.UUID(a["acquirement_id"]), timeout_s=TIMEOUT)
        assert res == ReclaimResult.EXPIRED_REQUEUED
        task = asyncio.create_task(_call(c, a, tok, "complete"))
        await _wait_lock_waiter(sm)  # complete is blocked on the acquirement row lock
        assert not task.done()
    r = await asyncio.wait_for(task, 10)
    assert r.status_code == 409 and r.json()["detail"]["code"] == "STALE_ACQUIREMENT"
    assert not _exists(s3, dst)
    assert await _rows(sm, "SELECT outcome FROM minion_subtask_attempt") == [("EXPIRED",)]
    assert await _rows(sm, "SELECT status FROM minion_subtask") == [("PENDING",)]


@pytest.mark.integration
async def test_heartbeat_vs_reclaim(env: tuple[AsyncClient, Any]) -> None:
    c, app = env
    sm: Sm = app.state.sessionmaker
    n = 16
    await seed_task(sm, "hbr", n, team="hbr")
    execs = await make_executors(sm, n)
    owned = await _acquire_all(c, execs)
    await _backdate(sm)
    rng = random.Random(7)  # noqa: S311

    async def hb(a: dict[str, Any], tok: str) -> int:
        await asyncio.sleep(rng.random() * 0.02)
        return int((await _call(c, a, tok, "heartbeat")).status_code)

    async def rec(a: dict[str, Any]) -> ReclaimResult:
        await asyncio.sleep(rng.random() * 0.02)
        return await _reclaim(sm, a["acquirement_id"])

    out = await asyncio.gather(
        *(asyncio.gather(hb(a, t), rec(a)) for a, (_, t) in zip(owned, execs, strict=True))
    )
    kinds: Counter[str] = Counter()
    for code, res in out:
        if code == 200:  # the beat won: it must have kept the lease
            assert res in (ReclaimResult.NOT_STALE, ReclaimResult.SKIPPED)
        else:
            assert code == 409 and res == ReclaimResult.EXPIRED_REQUEUED
        kinds[str(code)] += 1
    expired = kinds["409"]
    assert await _rows(sm, "SELECT count(*) FROM minion_subtask_acquirement") == [(n - expired,)]
    assert await _rows(sm, "SELECT count(*) FROM minion_subtask_attempt") == [(expired,)]
    # survivors beat just now, so a sweep leaves them alone
    assert not await reclaim_stale(sm, timeout_s=TIMEOUT, batch_size=100)
    assert await _rows(sm, "SELECT count(*) FROM minion_subtask_acquirement") == [(n - expired,)]


@pytest.mark.integration
async def test_two_watchdogs_reclaim_each_once(env: tuple[AsyncClient, Any]) -> None:
    c, app = env
    sm: Sm = app.state.sessionmaker
    n = 30
    await seed_task(sm, "tw", n, team="tw")
    execs = await make_executors(sm, n)
    await _acquire_all(c, execs)
    await _backdate(sm)

    r1, r2 = await asyncio.gather(
        reclaim_stale(sm, timeout_s=TIMEOUT, batch_size=100),
        reclaim_stale(sm, timeout_s=TIMEOUT, batch_size=100),
    )
    total = r1 + r2
    assert total[ReclaimResult.EXPIRED_REQUEUED] == n  # each acquirement reclaimed exactly once
    expired_sql = "SELECT count(*) FROM minion_subtask_attempt WHERE outcome='EXPIRED'"
    assert await _rows(sm, expired_sql) == [(n,)]
    assert await _rows(sm, "SELECT count(DISTINCT acquirement_id) FROM minion_subtask_attempt") == [
        (n,)
    ]
    assert await _rows(sm, "SELECT count(*) FROM minion_subtask WHERE status='PENDING'") == [(n,)]
    assert await _rows(sm, "SELECT count(*) FROM minion_executor_heartbeat") == [(0,)]


# --- zombie owner -------------------------------------------------------------------------------


@pytest.mark.integration
async def test_zombie_owner_rejected_after_reclaim(env: tuple[AsyncClient, Any], s3: S3Env) -> None:
    c, app = env
    sm: Sm = app.state.sessionmaker
    await seed_task(sm, "zb", 1, team="zb")
    (_, ta), (_, tb) = await make_executors(sm, 2)
    a = await _acquire(c, ta)
    assert a["attempt"] == 1
    await _backdate(sm)
    assert (await reclaim_stale(sm, timeout_s=TIMEOUT, batch_size=10))[
        ReclaimResult.EXPIRED_REQUEUED
    ] == 1

    # the zombie wakes up: every owner endpoint answers the same 409
    _put_output(s3, "zb", a)  # late PUT to its own attempt key
    bodies = []
    for op in ("heartbeat", "complete", "fail", "release"):
        r = await _call(c, a, ta, op)
        assert r.status_code == 409, (op, r.text)
        bodies.append(r.json())
    assert all(b == bodies[0] and b["detail"]["code"] == "STALE_ACQUIREMENT" for b in bodies)
    assert not _exists(s3, "outputs/zb/zb/000000.jsonl")

    b = await _acquire(c, tb)  # re-run elsewhere with a fresh attempt number
    assert b["attempt"] == 2 and b["acquirement_id"] != a["acquirement_id"]
    _put_output(s3, "zb", b)
    assert (await _call(c, b, tb, "complete")).status_code == 204
    assert await _rows(
        sm, "SELECT attempt, outcome FROM minion_subtask_attempt ORDER BY attempt"
    ) == [(1, "EXPIRED"), (2, "SUCCEEDED")]
    assert await _rows(sm, "SELECT status FROM minion_task") == [("SUCCEEDED",)]


# --- budget and release cap ---------------------------------------------------------------------


@pytest.mark.integration
async def test_budget_counts_failed_and_expired_not_released(
    env: tuple[AsyncClient, Any],
) -> None:
    c, app = env
    sm: Sm = app.state.sessionmaker
    await seed_task(sm, "bud", 1, team="bud")  # max_attempts defaults to 3
    ((_, tok),) = await make_executors(sm, 1)

    a = await _acquire(c, tok)
    assert (await _call(c, a, tok, "fail")).status_code == 204  # budget 1
    a = await _acquire(c, tok)
    assert (await _call(c, a, tok, "release")).status_code == 204  # free
    a = await _acquire(c, tok)
    await _backdate(sm)
    assert await _reclaim(sm, a["acquirement_id"]) == ReclaimResult.EXPIRED_REQUEUED  # budget 2
    assert await _rows(sm, "SELECT status FROM minion_subtask") == [("PENDING",)]
    a = await _acquire(c, tok)
    assert a["attempt"] == 4  # reclaim never bumps; every acquire does
    await _backdate(sm)
    assert await _reclaim(sm, a["acquirement_id"]) == ReclaimResult.EXPIRED_FAILED  # budget 3

    assert await _rows(sm, "SELECT status FROM minion_subtask") == [("FAILED",)]
    assert await _rows(sm, "SELECT outcome FROM minion_subtask_attempt ORDER BY attempt") == [
        ("FAILED",),
        ("RELEASED",),
        ("EXPIRED",),
        ("EXPIRED",),
    ]
    ((status, failed, fin),) = await _rows(
        sm, "SELECT status, failed_subtasks, finished_at IS NOT NULL FROM minion_task"
    )
    assert (status, failed, fin) == ("FAILED", 1, True)
    assert await _rows(sm, "SELECT count(*) FROM minion_subtask_acquirement") == [(0,)]


@pytest.mark.integration
async def test_release_cap_turns_into_failed_attempt(env_cap2: tuple[AsyncClient, Any]) -> None:
    c, app = env_cap2  # release_limit=2
    sm: Sm = app.state.sessionmaker
    await seed_task(sm, "cap", 1, team="cap")
    ((_, tok),) = await make_executors(sm, 1)
    for _ in range(2):  # free releases
        a = await _acquire(c, tok)
        assert (await _call(c, a, tok, "release")).status_code == 204
    outcomes = []
    for _ in range(3):  # beyond the cap: FAILED/RELEASE_LIMIT, budget 1..3 -> terminal
        a = await _acquire(c, tok)
        assert (await _call(c, a, tok, "release")).status_code == 204  # response unchanged
        outcomes.append(await _rows(sm, "SELECT status FROM minion_subtask"))
    assert outcomes == [[("PENDING",)], [("PENDING",)], [("FAILED",)]]
    rows = await _rows(
        sm,
        "SELECT outcome, error->>'code' FROM minion_subtask_attempt ORDER BY attempt",
    )
    assert rows == [("RELEASED", None)] * 2 + [("FAILED", "RELEASE_LIMIT")] * 3
    assert await _rows(sm, "SELECT status FROM minion_task") == [("FAILED",)]


# --- cancelled task paths -----------------------------------------------------------------------


async def _cancel_task(sm: Sm, name: str) -> None:
    async with sm() as s, s.begin():
        await s.execute(
            text("UPDATE minion_task SET status='CANCELLED' WHERE task_name=:n"), {"n": name}
        )


@pytest.mark.integration
async def test_cancelled_task_release_fail_reclaim(env: tuple[AsyncClient, Any]) -> None:
    c, app = env
    sm: Sm = app.state.sessionmaker
    await seed_task(sm, "cx", 3, team="cx")
    execs = await make_executors(sm, 3)
    ra, fa, xa = await _acquire_all(c, execs)
    await _cancel_task(sm, "cx")

    assert (await _call(c, ra, execs[0][1], "release")).status_code == 204
    assert (await _call(c, fa, execs[1][1], "fail")).status_code == 204
    await _backdate(sm)
    assert await _reclaim(sm, xa["acquirement_id"]) == ReclaimResult.EXPIRED_CANCELLED

    rows = await _rows(sm, "SELECT subtask_id, status FROM minion_subtask ORDER BY subtask_id")
    assert {st for _, st in rows} == {"CANCELLED"}
    outcomes = await _rows(sm, "SELECT outcome FROM minion_subtask_attempt ORDER BY outcome")
    assert outcomes == [("CANCELLED",), ("EXPIRED",), ("FAILED",)]
    assert await _rows(sm, "SELECT failed_subtasks FROM minion_task") == [(0,)]


# --- reclaim robustness -------------------------------------------------------------------------


@pytest.mark.integration
async def test_reclaim_one_not_stale_after_heartbeat(env: tuple[AsyncClient, Any]) -> None:
    c, app = env
    sm: Sm = app.state.sessionmaker
    await seed_task(sm, "ns", 1, team="ns")
    ((_, tok),) = await make_executors(sm, 1)
    a = await _acquire(c, tok)
    await _backdate(sm)
    assert (await _call(c, a, tok, "heartbeat")).status_code == 200
    assert await _reclaim(sm, a["acquirement_id"]) == ReclaimResult.NOT_STALE
    assert await _rows(sm, "SELECT count(*) FROM minion_subtask_acquirement") == [(1,)]
    assert await _rows(sm, "SELECT count(*) FROM minion_subtask_attempt") == [(0,)]


@pytest.mark.integration
async def test_reclaim_stale_continues_after_failing_id(
    env: tuple[AsyncClient, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    import minion_server.services.reclaim as mod

    c, app = env
    sm: Sm = app.state.sessionmaker
    n = 4
    await seed_task(sm, "bad", n, team="bad")
    execs = await make_executors(sm, n)
    owned = await _acquire_all(c, execs)
    await _backdate(sm)
    poisoned = owned[0]["acquirement_id"]
    real = mod.reclaim_one

    async def flaky(session: AsyncSession, aid: Any, *, timeout_s: float) -> ReclaimResult:
        if str(aid) == poisoned:
            raise RuntimeError("boom")
        return await real(session, aid, timeout_s=timeout_s)

    monkeypatch.setattr(mod, "reclaim_one", flaky)
    res = await reclaim_stale(sm, timeout_s=TIMEOUT, batch_size=100)
    assert res[ReclaimResult.EXPIRED_REQUEUED] == n - 1
    assert await _rows(sm, "SELECT count(*) FROM minion_subtask_acquirement") == [(1,)]
