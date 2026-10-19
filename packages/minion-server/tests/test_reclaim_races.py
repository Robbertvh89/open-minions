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
from minion_server.services.reclaim import (
    ReclaimResult,
    reclaim_chunk,
    reclaim_one,
    reclaim_stale,
    settle_stuck,
)
from minion_server.settings import ServerSettings
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
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
        db_pool_timeout_s=15.0,  # race tests queue many requests on the pool (default 5 s)
        **{
            "register_min_interval_s": 0.0,
            "acquire_min_interval_s": 0.0,
            "presence_min_interval_s": 0.0,
            **extra,
        },
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
    n, min_rounds, max_rounds = 16, 4, 16
    complete_wins = 0
    played = 0
    execs = await make_executors(sm, n)
    for rnd in range(max_rounds):
        # keep playing until both orderings were seen (runner speed shifts the odds)
        if rnd >= min_rounds and 0 < complete_wins < n * played:
            break
        played += 1
        team, name = f"rr{rnd}", f"rr{rnd}"
        await seed_task(sm, name, n, team=team)
        owned = await _acquire_all(c, execs)
        dsts = {a["subtask_id"]: _put_output(s3, team, a) for a in owned}
        await _backdate(sm)

        async def complete(a: dict[str, Any], tok: str) -> int:
            await asyncio.sleep(rng.random() * 0.01)
            return int((await _call(c, a, tok, "complete")).status_code)

        # alternate the reclaim timing so both orderings occur on fast and slow runners alike:
        # even rounds reclaim early (mostly before complete), odd rounds late (mostly after)
        early = rnd % 2 == 0

        async def reclaim(a: dict[str, Any], early: bool = early) -> ReclaimResult:
            await asyncio.sleep(rng.random() * 0.01 if early else 0.05 + rng.random() * 0.25)
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
            "SELECT succeeded_subtasks, failed_subtasks, status FROM minion_ro.task"
            " WHERE task_name=:n",
            n=name,
        )
        complete_wins += wins
        assert (succ, failed) == (wins, 0)
        assert tstatus == ("SUCCEEDED" if wins == n else "RUNNING")
        # drain: finish this round's leftovers so the next round starts from an empty queue
        await _cancel_pending(sm, name)
    # both orderings must have been exercised, else the race is not actually being tested
    assert 0 < complete_wins < n * played, (complete_wins, played)


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
    task_sql = "SELECT status, failed_subtasks, finished_at IS NOT NULL FROM minion_ro.task"
    # reclaim only bumps the counters; the watchdog's settle makes the task terminal (ADR-0015)
    assert await _rows(sm, task_sql) == [("RUNNING", 1, False)]
    assert await settle_stuck(sm) == 1
    assert await _rows(sm, task_sql) == [("FAILED", 1, True)]
    assert await settle_stuck(sm) == 0
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
    assert await _rows(sm, "SELECT failed_subtasks FROM minion_ro.task") == [(0,)]


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
    real = mod.reclaim_chunk

    async def flaky(session: AsyncSession, ids: Any, **kw: Any) -> Any:
        if any(str(i) == poisoned for i in ids):  # poisons the chunk and the 1-id retry
            raise RuntimeError("boom")
        return await real(session, ids, **kw)

    monkeypatch.setattr(mod, "reclaim_chunk", flaky)
    res = await reclaim_stale(sm, timeout_s=TIMEOUT, batch_size=100)
    assert res[ReclaimResult.EXPIRED_REQUEUED] == n - 1
    assert await _rows(sm, "SELECT count(*) FROM minion_subtask_acquirement") == [(1,)]


# --- unacknowledged expiry (TASK-7.4.5, ADR-0014) ------------------------------------------------

CAP = 2


async def _ghost_age(sm: Sm) -> None:
    """Age every acquirement and its claim-time heartbeat together: still never beaten."""
    async with sm() as s, s.begin():
        await s.execute(
            text("UPDATE minion_subtask_acquirement SET acquired_at = now() - interval '1 hour'")
        )
        await s.execute(
            text("UPDATE minion_executor_heartbeat SET last_seen_at = now() - interval '1 hour'")
        )


async def _reclaim_cap(sm: Sm, acq_id: str | Any, cap: int = CAP) -> ReclaimResult:
    import uuid

    async with sm() as s, s.begin():
        return await reclaim_one(s, uuid.UUID(str(acq_id)), timeout_s=TIMEOUT, unacked_free_cap=cap)


async def _max_attempts(sm: Sm, n: int) -> None:
    async with sm() as s, s.begin():
        await s.execute(text("UPDATE minion_subtask SET max_attempts = :n"), {"n": n})


async def _ghost_cycle(c: AsyncClient, sm: Sm, tok: str) -> ReclaimResult:
    a = await _acquire(c, tok)
    await _ghost_age(sm)
    return await _reclaim_cap(sm, a["acquirement_id"])


@pytest.mark.integration
async def test_never_beaten_reclaim_is_free(env: tuple[AsyncClient, Any]) -> None:
    c, app = env
    sm: Sm = app.state.sessionmaker
    await seed_task(sm, "ghost", 1, team="ghost")
    await _max_attempts(sm, 1)
    ((_, tok),) = await make_executors(sm, 1)
    assert await _ghost_cycle(c, sm, tok) == ReclaimResult.EXPIRED_REQUEUED
    assert await _rows(sm, "SELECT status, error FROM minion_subtask") == [("PENDING", None)]
    assert await _rows(sm, "SELECT outcome, error->>'code' FROM minion_subtask_attempt") == [
        ("EXPIRED", "NOT_ACKNOWLEDGED")
    ]
    assert await _rows(sm, "SELECT failed_subtasks FROM minion_ro.task") == [(0,)]


@pytest.mark.integration
async def test_beaten_reclaim_consumes_budget(env: tuple[AsyncClient, Any]) -> None:
    c, app = env
    sm: Sm = app.state.sessionmaker
    await seed_task(sm, "beat", 1, team="beat")
    await _max_attempts(sm, 1)
    ((_, tok),) = await make_executors(sm, 1)
    a = await _acquire(c, tok)
    assert (await _call(c, a, tok, "heartbeat")).status_code == 200  # the ack
    await _backdate(sm)  # silent afterwards
    assert await _reclaim_cap(sm, a["acquirement_id"]) == ReclaimResult.EXPIRED_FAILED
    assert await _rows(sm, "SELECT outcome, error->>'code' FROM minion_subtask_attempt") == [
        ("EXPIRED", "HEARTBEAT_TIMEOUT")
    ]


@pytest.mark.integration
async def test_first_beat_is_not_rate_limited_second_is(env: tuple[AsyncClient, Any]) -> None:
    c, app = env
    sm: Sm = app.state.sessionmaker
    await seed_task(sm, "ackrl", 1, team="ackrl")
    ((_, tok),) = await make_executors(sm, 1)
    a = await _acquire(c, tok)
    assert (await _call(c, a, tok, "heartbeat")).status_code == 200
    assert (await _call(c, a, tok, "heartbeat")).status_code == 429


@pytest.mark.integration
async def test_free_unacked_expiries_are_capped(env: tuple[AsyncClient, Any]) -> None:
    c, app = env
    sm: Sm = app.state.sessionmaker
    await seed_task(sm, "cap", 1, team="cap")
    await _max_attempts(sm, 1)
    ((_, tok),) = await make_executors(sm, 1)
    for _ in range(CAP):
        assert await _ghost_cycle(c, sm, tok) == ReclaimResult.EXPIRED_REQUEUED
    # over the cap it counts like any expiry: max_attempts=1 is spent
    assert await _ghost_cycle(c, sm, tok) == ReclaimResult.EXPIRED_FAILED
    assert await _rows(
        sm, "SELECT error->>'code' FROM minion_subtask_attempt ORDER BY attempt"
    ) == [("NOT_ACKNOWLEDGED",)] * CAP + [("HEARTBEAT_TIMEOUT",)]
    assert await _rows(sm, "SELECT status FROM minion_subtask") == [("FAILED",)]


@pytest.mark.integration
async def test_free_rows_do_not_count_toward_the_budget(env: tuple[AsyncClient, Any]) -> None:
    c, app = env
    sm: Sm = app.state.sessionmaker
    await seed_task(sm, "excl", 1, team="excl")
    await _max_attempts(sm, 2)
    ((_, tok),) = await make_executors(sm, 1)
    for _ in range(CAP):
        assert await _ghost_cycle(c, sm, tok) == ReclaimResult.EXPIRED_REQUEUED
    # over the cap: the first counted expiry sees 0 earlier counted attempts (1 of 2 used)
    assert await _ghost_cycle(c, sm, tok) == ReclaimResult.EXPIRED_REQUEUED
    assert await _rows(sm, "SELECT status FROM minion_subtask") == [("PENDING",)]
    assert await _ghost_cycle(c, sm, tok) == ReclaimResult.EXPIRED_FAILED  # 2 of 2


@pytest.mark.integration
async def test_ghost_cycle_then_retryable_fail_requeues(
    env: tuple[AsyncClient, Any],
) -> None:
    c, app = env
    sm: Sm = app.state.sessionmaker
    await seed_task(sm, "gfail", 1, team="gfail")
    await _max_attempts(sm, 2)
    ((_, tok),) = await make_executors(sm, 1)
    assert await _ghost_cycle(c, sm, tok) == ReclaimResult.EXPIRED_REQUEUED
    a = await _acquire(c, tok)
    assert (await _call(c, a, tok, "fail")).status_code == 204
    assert await _rows(sm, "SELECT status FROM minion_subtask") == [("PENDING",)]


@pytest.mark.integration
async def test_cap_zero_keeps_paper_semantics(env: tuple[AsyncClient, Any]) -> None:
    c, app = env
    sm: Sm = app.state.sessionmaker
    await seed_task(sm, "cap0", 1, team="cap0")
    await _max_attempts(sm, 1)
    ((_, tok),) = await make_executors(sm, 1)
    a = await _acquire(c, tok)
    await _ghost_age(sm)
    assert await _reclaim_cap(sm, a["acquirement_id"], cap=0) == ReclaimResult.EXPIRED_FAILED


@pytest.mark.integration
async def test_max_attempts_one_survives_a_ghost_acquire(
    env: tuple[AsyncClient, Any], s3: S3Env
) -> None:
    c, app = env
    sm: Sm = app.state.sessionmaker
    await seed_task(sm, "surv", 1, team="surv")
    await _max_attempts(sm, 1)
    ((_, tok),) = await make_executors(sm, 1)
    assert await _ghost_cycle(c, sm, tok) == ReclaimResult.EXPIRED_REQUEUED
    a = await _acquire(c, tok)
    assert a["attempt"] == 2
    _put_output(s3, "surv", a)
    assert (await _call(c, a, tok, "complete")).status_code == 204
    assert await _rows(sm, "SELECT status FROM minion_subtask") == [("SUCCEEDED",)]


@pytest.mark.integration
async def test_unacked_reclaim_of_cancelled_task_stays_cancelled(
    env: tuple[AsyncClient, Any],
) -> None:
    c, app = env
    sm: Sm = app.state.sessionmaker
    await seed_task(sm, "canc", 1, team="canc")
    ((_, tok),) = await make_executors(sm, 1)
    a = await _acquire(c, tok)
    async with sm() as s, s.begin():
        await s.execute(text("UPDATE minion_task SET status = 'CANCELLED'"))
    await _ghost_age(sm)
    assert await _reclaim_cap(sm, a["acquirement_id"]) == ReclaimResult.EXPIRED_CANCELLED


# --- chunked reclaim (TASK-35) ------------------------------------------------------------------


async def _acquire_many(
    c: AsyncClient, sm: Sm, n: int, name: str
) -> list[tuple[dict[str, Any], str]]:
    """n live acquirements (4 per executor, the open-acquirement cap)."""
    await seed_task(sm, name, n, team=name)
    execs = await make_executors(sm, (n + 3) // 4)
    out: list[tuple[dict[str, Any], str]] = []

    for _, tok in execs:  # sequential: concurrent claims fight over the acquire window
        for _ in range(4):
            if len(out) < n:
                out.append((await _acquire(c, tok), tok))
    assert len(out) == n
    return out


@pytest.mark.integration
async def test_classify_and_lock_batch(env: tuple[AsyncClient, Any]) -> None:
    import uuid

    from minion_server.repositories import reclaim as repo

    c, app = env
    sm: Sm = app.state.sessionmaker
    got = await _acquire_many(c, sm, 4, "cl")
    fresh, ghost, acked, leased = (uuid.UUID(a["acquirement_id"]) for a, _ in got)
    async with sm() as s, s.begin():
        # ghost: aged together with its claim-time beat (never acked); acked: only the beat aged
        for aid, both in ((ghost, True), (acked, False)):
            await s.execute(
                text(
                    "UPDATE minion_executor_heartbeat SET last_seen_at = now() - interval '1 hour'"
                    " WHERE acquirement_id = :a"
                ),
                {"a": aid},
            )
            if both:
                await s.execute(
                    text(
                        "UPDATE minion_subtask_acquirement"
                        " SET acquired_at = now() - interval '1 hour' WHERE acquirement_id = :a"
                    ),
                    {"a": aid},
                )
        # leased: fresh beat on top of an old claim
        await s.execute(
            text(
                "UPDATE minion_subtask_acquirement SET acquired_at = now() - interval '1 hour'"
                " WHERE acquirement_id = :a"
            ),
            {"a": leased},
        )
        await s.execute(
            text(
                "UPDATE minion_executor_heartbeat SET last_seen_at = now()"
                " WHERE acquirement_id = :a"
            ),
            {"a": leased},
        )
    ids = [fresh, ghost, acked, leased]
    async with sm() as s, s.begin():
        locked = await repo.lock_batch(s, ids)
        assert locked == sorted(ids)
        rows = {
            r.owned.acquirement_id: r
            for r in await repo.classify_locked(s, locked, timeout_s=TIMEOUT, max_lease_s=1800.0)
        }
    flags = {k: (r.stale, r.never_acked, r.over_lease) for k, r in rows.items()}
    assert flags[fresh] == (False, True, False)  # claim-time beat only, but fresh
    assert flags[ghost] == (True, True, True)
    assert flags[acked] == (True, False, False)
    assert flags[leased] == (False, False, True)
    assert all(r.budget_used == 0 and r.unacked_used == 0 for r in rows.values())


@pytest.mark.integration
async def test_lock_batch_skips_row_locked_elsewhere(env: tuple[AsyncClient, Any]) -> None:
    import uuid

    from minion_server.repositories import reclaim as repo

    c, app = env
    sm: Sm = app.state.sessionmaker
    got = await _acquire_many(c, sm, 3, "lk")
    ids = [uuid.UUID(a["acquirement_id"]) for a, _ in got]
    async with sm() as holder, holder.begin():
        await holder.execute(
            text("SELECT 1 FROM minion_subtask_acquirement WHERE acquirement_id = :a FOR UPDATE"),
            {"a": ids[1]},
        )
        async with sm() as s, s.begin():
            locked = await repo.lock_batch(s, ids)
            assert sorted(locked) == sorted([ids[0], ids[2]])
            res = await reclaim_chunk(s, ids, timeout_s=TIMEOUT)
        assert res[ids[1]] == ReclaimResult.SKIPPED


@pytest.mark.integration
async def test_chunk_beat_before_lock_is_not_stale(env: tuple[AsyncClient, Any]) -> None:
    import uuid

    c, app = env
    sm: Sm = app.state.sessionmaker
    got = await _acquire_many(c, sm, 3, "bb")
    await _backdate(sm)
    ids = [uuid.UUID(a["acquirement_id"]) for a, _ in got]
    # the unlocked scan saw all three stale; one beat commits before the chunk takes its lock
    assert (await _call(c, got[1][0], got[1][1], "heartbeat")).status_code == 200
    async with sm() as s, s.begin():
        res = await reclaim_chunk(s, ids, timeout_s=TIMEOUT)
    assert res[ids[1]] == ReclaimResult.NOT_STALE
    assert res[ids[0]] == res[ids[2]] == ReclaimResult.EXPIRED_REQUEUED
    assert await _rows(sm, "SELECT count(*) FROM minion_subtask_attempt") == [(2,)]
    assert await _rows(sm, "SELECT count(*) FROM minion_subtask_acquirement") == [(1,)]


@pytest.mark.integration
async def test_two_replicas_with_concurrent_beats_and_completes(
    env: tuple[AsyncClient, Any], s3: S3Env
) -> None:
    c, app = env
    sm: Sm = app.state.sessionmaker
    n = 200
    got = await _acquire_many(c, sm, n, "rep")
    outputs = {a["acquirement_id"]: _put_output(s3, "rep", a) for a, _ in got[:40]}
    assert outputs
    await _backdate(sm)

    async def beat(a: dict[str, Any], tok: str) -> int:
        return int((await _call(c, a, tok, "heartbeat")).status_code)

    async def complete(a: dict[str, Any], tok: str) -> int:
        return int((await _call(c, a, tok, "complete")).status_code)

    kw: dict[str, Any] = {"timeout_s": TIMEOUT, "batch_size": 500}
    results = await asyncio.gather(
        reclaim_stale(sm, **kw),
        reclaim_stale(sm, **kw),
        *(complete(a, t) for a, t in got[:20]),
        *(beat(a, t) for a, t in got[20:40]),
    )
    await reclaim_stale(sm, **kw)  # mop-up
    assert set(results[2:]) <= {200, 204, 409}, Counter(results[2:])  # no 500 / deadlock
    per_acq = await _rows(
        sm,
        "SELECT a.acquirement_id, (SELECT count(*) FROM minion_subtask_attempt t"
        "  WHERE t.acquirement_id = a.acquirement_id) FROM minion_subtask_acquirement a",
    )
    assert all(cnt == 0 for _, cnt in per_acq)  # live rows have no attempt (beaten survivors)
    attempts = await _rows(
        sm, "SELECT acquirement_id, count(*) FROM minion_subtask_attempt GROUP BY 1"
    )
    assert all(cnt == 1 for _, cnt in attempts)
    assert len(attempts) + len(per_acq) == n
    assert await _rows(sm, "SELECT count(*) FROM minion_subtask WHERE status = 'RUNNING'") == [
        (len(per_acq),)
    ]
    done = await _rows(
        sm,
        "SELECT count(*) FROM minion_subtask_attempt WHERE outcome = 'SUCCEEDED'",
    )
    assert done == [(sum(1 for r in results[2:22] if r == 204),)]
    # every other expiry requeued exactly once
    assert await _rows(
        sm, "SELECT count(*) FROM minion_subtask_attempt WHERE outcome = 'EXPIRED'"
    ) == await _rows(sm, "SELECT count(*) FROM minion_subtask WHERE status = 'PENDING'")


@pytest.mark.integration
async def test_chunked_reclaim_statement_count_is_bounded(env: tuple[AsyncClient, Any]) -> None:
    from sqlalchemy import event

    c, app = env
    sm: Sm = app.state.sessionmaker
    n = 250
    await _acquire_many(c, sm, n, "cnt")
    await _backdate(sm)
    seen: list[str] = []
    engine = sm.kw["bind"].sync_engine

    def count(conn: Any, cur: Any, stmt: str, *a: Any) -> None:
        seen.append(stmt)

    event.listen(engine, "before_cursor_execute", count)
    try:
        res = await reclaim_stale(sm, timeout_s=TIMEOUT, batch_size=1000)
    finally:
        event.remove(engine, "before_cursor_execute", count)
    assert res[ReclaimResult.EXPIRED_REQUEUED] == n
    chunks = 3  # RECLAIM_CHUNK = 100
    # scan + per chunk: lock, classify, INSERT, DELETE, UPDATE (+ shard bumps only for FAILED)
    assert len(seen) <= 1 + chunks * 6, len(seen)


def _record_chunks(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    import minion_server.services.reclaim as mod

    sizes: list[int] = []
    real = mod.reclaim_chunk

    async def spy(session: AsyncSession, ids: Any, **kw: Any) -> Any:
        sizes.append(len(ids))
        return await real(session, ids, **kw)

    monkeypatch.setattr(mod, "reclaim_chunk", spy)
    return sizes


@pytest.mark.integration
async def test_two_replicas_mixed_outcomes_counters_and_codes(
    env: tuple[AsyncClient, Any], s3: S3Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    c, app = env
    sm: Sm = app.state.sessionmaker
    n, extra = 120, 10
    got = await _acquire_many(c, sm, n + extra, "mx")
    victims, finishers = got[:n], got[n:]
    ghosts, acked = victims[:40], victims[40:]
    for a, tok in acked:  # a real beat after the claim: not a ghost
        assert (await _call(c, a, tok, "heartbeat")).status_code == 200
    for a, _ in finishers:
        _put_output(s3, "mx", a)
    ghost_ids = [a["acquirement_id"] for a, _ in ghosts]
    acked_ids = [a["acquirement_id"] for a, _ in acked]
    async with sm() as s, s.begin():
        await s.execute(
            text(
                "UPDATE minion_subtask_acquirement SET acquired_at = now() - interval '1 hour'"
                " WHERE acquirement_id = ANY(CAST(:g AS uuid[]))"
            ),
            {"g": ghost_ids},
        )  # ghost: claim-time beat aged together with the claim
        await s.execute(
            text(
                "UPDATE minion_executor_heartbeat h SET last_seen_at = a.acquired_at"
                " FROM minion_subtask_acquirement a WHERE a.acquirement_id = h.acquirement_id"
                " AND a.acquirement_id = ANY(CAST(:g AS uuid[]))"
            ),
            {"g": ghost_ids},
        )
        await s.execute(
            text(
                "UPDATE minion_executor_heartbeat SET last_seen_at = now() - interval '2 hours'"
                " WHERE acquirement_id = ANY(CAST(:g AS uuid[]))"
            ),
            {"g": acked_ids},
        )
        await s.execute(text("UPDATE minion_subtask SET max_attempts = 1 WHERE subtask_id % 2 = 0"))
    sizes = _record_chunks(monkeypatch)
    kw: dict[str, Any] = {"timeout_s": TIMEOUT, "batch_size": 500, "unacked_free_cap": 2}
    r1, r2, *codes = await asyncio.gather(
        reclaim_stale(sm, **kw),
        reclaim_stale(sm, **kw),
        *(_call(c, a, t, "complete") for a, t in finishers),
    )
    assert all(x.status_code == 204 for x in codes)
    total = r1 + r2
    assert sum(total.values()) - total[ReclaimResult.SKIPPED] == n, total  # no mop-up
    assert min(sizes) > 1, sizes  # the row-by-row fallback never ran
    subs = {
        sid: (st, code)
        for sid, st, code in await _rows(
            sm, "SELECT subtask_id, status, error->>'code' FROM minion_subtask"
        )
    }
    ghost_sids = {a["subtask_id"] for a, _ in ghosts}
    victim_sids = {a["subtask_id"] for a, _ in victims}
    for sid, (st, code) in subs.items():
        if sid in ghost_sids:
            assert (st, code) == ("PENDING", None)  # NOT_ACKNOWLEDGED lives on the history only
        elif sid in victim_sids:
            assert code == "HEARTBEAT_TIMEOUT"
            assert st == ("FAILED" if sid % 2 == 0 else "PENDING")
        else:
            assert st == "SUCCEEDED"
    want: dict[int, int] = {}
    for sid, (st, _) in subs.items():
        if st == "FAILED":
            want[sid % 16] = want.get(sid % 16, 0) + 1
    shards = dict(
        await _rows(sm, "SELECT shard, failed FROM minion_task_counter WHERE task_name = 'mx'")
    )
    assert {k: v for k, v in shards.items() if v} == want
    assert dict(
        await _rows(
            sm,
            "SELECT error->>'code', count(*) FROM minion_subtask_attempt"
            " WHERE outcome = 'EXPIRED' GROUP BY 1",
        )
    ) == {"NOT_ACKNOWLEDGED": 40, "HEARTBEAT_TIMEOUT": 80}


@pytest.mark.integration
async def test_partial_chunk_write_rolls_back_before_fallback(
    env: tuple[AsyncClient, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    from minion_server.repositories import reclaim as repo_mod

    c, app = env
    sm: Sm = app.state.sessionmaker
    n = 5
    await _acquire_many(c, sm, n, "pw")
    await _backdate(sm)
    real = repo_mod.finish_reclaimed
    calls = 0

    async def write_then_boom(session: AsyncSession, items: Any) -> None:
        nonlocal calls
        calls += 1
        await real(session, items)  # real writes in the open transaction ...
        if calls == 1:
            raise RuntimeError("boom after writes")  # ... then fail the chunk once

    monkeypatch.setattr(repo_mod, "finish_reclaimed", write_then_boom)
    res = await reclaim_stale(sm, timeout_s=TIMEOUT, batch_size=100)
    assert res[ReclaimResult.EXPIRED_REQUEUED] == n
    assert await _rows(sm, "SELECT count(*) FROM minion_subtask_acquirement") == [(0,)]
    assert await _rows(
        sm, "SELECT count(*), count(DISTINCT acquirement_id) FROM minion_subtask_attempt"
    ) == [(n, n)]


@pytest.mark.integration
async def test_failing_row_is_quarantined_for_next_cycles(
    env: tuple[AsyncClient, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    import minion_server.services.reclaim as mod

    c, app = env
    sm: Sm = app.state.sessionmaker
    got = await _acquire_many(c, sm, 4, "qq")
    await _backdate(sm)
    poisoned = got[0][0]["acquirement_id"]
    real = mod.reclaim_chunk
    seen: list[list[str]] = []

    async def flaky(session: AsyncSession, ids: Any, **kw: Any) -> Any:
        seen.append([str(i) for i in ids])
        if any(str(i) == poisoned for i in ids):
            raise RuntimeError("boom")
        return await real(session, ids, **kw)

    monkeypatch.setattr(mod, "reclaim_chunk", flaky)
    await reclaim_stale(sm, timeout_s=TIMEOUT, batch_size=100)
    assert len(seen) == 1 + 4  # the chunk, then each row alone
    seen.clear()
    res = await reclaim_stale(sm, timeout_s=TIMEOUT, batch_size=100)
    assert not res and seen == []  # quarantined: not retried (and nothing else is stale)


@pytest.mark.integration
async def test_quarantined_rows_do_not_hold_the_scan_slots(
    env: tuple[AsyncClient, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    import uuid

    import minion_server.services.reclaim as mod

    c, app = env
    sm: Sm = app.state.sessionmaker
    got = await _acquire_many(c, sm, 4, "qs")
    ids = [a["acquirement_id"] for a, _ in got]
    async with sm() as s, s.begin():  # the three oldest are quarantined, one newer is stale
        for i, aid in enumerate(ids):
            await s.execute(
                text(
                    "UPDATE minion_executor_heartbeat SET last_seen_at ="
                    " now() - make_interval(hours => 5 - :i) WHERE acquirement_id = :a"
                ),
                {"i": i, "a": aid},
            )
    for aid in ids[:3]:
        mod._QUARANTINE[uuid.UUID(aid)] = 10**12
    res = await reclaim_stale(sm, timeout_s=TIMEOUT, batch_size=3)  # LIMIT == quarantined count
    assert res[ReclaimResult.EXPIRED_REQUEUED] == 1
    assert await _rows(sm, "SELECT count(*) FROM minion_subtask_acquirement") == [(3,)]


@pytest.mark.integration
async def test_transient_errors_never_quarantine(
    env: tuple[AsyncClient, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    import minion_server.services.reclaim as mod

    c, app = env
    sm: Sm = app.state.sessionmaker
    await _acquire_many(c, sm, 3, "tr")
    await _backdate(sm)

    async def down(session: AsyncSession, ids: Any, **kw: Any) -> Any:
        raise OperationalError("SELECT 1", {}, Exception("connection lost"))

    monkeypatch.setattr(mod, "reclaim_chunk", down)
    await reclaim_stale(sm, timeout_s=TIMEOUT, batch_size=100)
    assert mod._QUARANTINE == {}


def _poison_one(mod: Any, poisoned: set[str], exc: Exception) -> Any:
    """Patch `reclaim_chunk` so any call containing a poisoned id raises `exc`."""
    real = mod.reclaim_chunk

    async def flaky(session: AsyncSession, ids: Any, **kw: Any) -> Any:
        if any(str(i) in poisoned for i in ids):
            raise exc
        return await real(session, ids, **kw)

    return flaky


@pytest.fixture(autouse=True)
def _clean_quarantine() -> Any:
    import minion_server.services.reclaim as mod

    mod.reset_quarantine()
    yield
    mod.reset_quarantine()


@pytest.mark.integration
async def test_quarantine_expires(
    env: tuple[AsyncClient, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    import time
    from types import SimpleNamespace

    import minion_server.services.reclaim as mod

    c, app = env
    sm: Sm = app.state.sessionmaker
    got = await _acquire_many(c, sm, 3, "ex")
    await _backdate(sm)
    poisoned = got[0][0]["acquirement_id"]
    monkeypatch.setattr(mod, "reclaim_chunk", _poison_one(mod, {poisoned}, RuntimeError("boom")))
    res = await reclaim_stale(sm, timeout_s=TIMEOUT, batch_size=100)
    assert res[ReclaimResult.EXPIRED_REQUEUED] == 2
    assert [str(k) for k in mod._QUARANTINE] == [poisoned]
    # time passes: the quarantine lapses and the row is reclaimed again
    clock = time.monotonic() + mod.QUARANTINE_S + 1
    monkeypatch.undo()  # unpoison
    monkeypatch.setattr(mod, "time", SimpleNamespace(monotonic=lambda: clock))
    res = await reclaim_stale(sm, timeout_s=TIMEOUT, batch_size=100)
    assert res[ReclaimResult.EXPIRED_REQUEUED] == 1
    assert mod._QUARANTINE == {}


def test_full_quarantine_evicts_soonest_expiring(monkeypatch: pytest.MonkeyPatch) -> None:
    import time
    import uuid

    import minion_server.services.reclaim as mod
    from structlog.testing import capture_logs

    monkeypatch.setattr(mod, "_QUARANTINE_MAX", 3)
    base = time.monotonic() + 10_000
    ids = [uuid.uuid4() for _ in range(3)]
    for i, k in enumerate([ids[1], ids[0], ids[2]]):  # ids[1] expires first
        mod._QUARANTINE[k] = base + i
    new = uuid.uuid4()
    with capture_logs() as logs:
        assert mod._quarantine_add(new, "RuntimeError") is True
    assert len(mod._QUARANTINE) == 3 and new in mod._QUARANTINE
    assert ids[1] not in mod._QUARANTINE
    assert mod._quarantine_evicted == 1
    full = [e for e in logs if e["event"] == "reclaim_quarantine_full"]
    assert full and full[0]["evicted_acquirement_id"] == str(ids[1]) and full[0]["max"] == 3
    # re-adding a present id never evicts
    with capture_logs():
        assert mod._quarantine_add(new, "RuntimeError") is False
    assert mod._quarantine_evicted == 1


@pytest.mark.integration
@pytest.mark.parametrize("exc", [RuntimeError("boom"), AttributeError("boom")])
async def test_systemic_chunk_failure_quarantines_nothing(
    env: tuple[AsyncClient, Any], monkeypatch: pytest.MonkeyPatch, exc: Exception
) -> None:
    import minion_server.services.reclaim as mod
    from structlog.testing import capture_logs

    c, app = env
    sm: Sm = app.state.sessionmaker
    got = await _acquire_many(c, sm, 3, "sy")
    await _backdate(sm)
    real = mod.reclaim_chunk

    async def broken(session: AsyncSession, ids: Any, **kw: Any) -> Any:
        raise exc

    monkeypatch.setattr(mod, "reclaim_chunk", broken)
    with capture_logs() as logs:
        await reclaim_stale(sm, timeout_s=TIMEOUT, batch_size=100)
    assert mod._QUARANTINE == {}
    assert await _rows(sm, "SELECT count(*) FROM minion_subtask_acquirement") == [(3,)]
    systemic = [e for e in logs if e["event"] == "reclaim_chunk_systemic"]
    assert len(systemic) == 1 and systemic[0]["failed"] == 3
    assert systemic[0]["error_type"] == type(exc).__name__
    # the code bug is fixed: the very next tick reclaims everything
    monkeypatch.setattr(mod, "reclaim_chunk", real)
    res = await reclaim_stale(sm, timeout_s=TIMEOUT, batch_size=100)
    assert res[ReclaimResult.EXPIRED_REQUEUED] == len(got)


@pytest.mark.integration
async def test_quarantine_log_has_no_payload(
    env: tuple[AsyncClient, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    import minion_server.services.reclaim as mod
    from structlog.testing import capture_logs

    c, app = env
    sm: Sm = app.state.sessionmaker
    got = await _acquire_many(c, sm, 3, "lg")
    await _backdate(sm)
    poisoned = got[0][0]["acquirement_id"]
    monkeypatch.setattr(mod, "reclaim_chunk", _poison_one(mod, {poisoned}, RuntimeError("SECRET")))
    with capture_logs() as logs:
        await reclaim_stale(sm, timeout_s=TIMEOUT, batch_size=100)
    assert "SECRET" not in repr(logs)
    q = [e for e in logs if e["event"] == "reclaim_quarantined"]
    assert len(q) == 1
    assert {k for k in q[0] if k not in ("event", "log_level")} == {
        "acquirement_id",
        "until",
        "error_type",
    }
    assert q[0]["acquirement_id"] == poisoned and q[0]["error_type"] == "RuntimeError"
    summary = [e for e in logs if e["event"] == "reclaim_quarantine"]
    assert len(summary) == 1 and summary[0]["size"] == 1 and summary[0]["added"] == 1


@pytest.mark.integration
async def test_lock_timeout_55p03_is_not_quarantined(
    env: tuple[AsyncClient, Any], fresh_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Real DB: a row lock held elsewhere + lock_timeout raises 55P03 -> not row-specific."""
    import minion_server.services.reclaim as mod
    from sqlalchemy.exc import DBAPIError
    from sqlalchemy.ext.asyncio import create_async_engine

    c, app = env
    sm: Sm = app.state.sessionmaker
    got = await _acquire_many(c, sm, 3, "lt")
    await _backdate(sm)
    victim = got[0][0]["acquirement_id"]
    (victim_subtask,) = (
        await _rows(
            sm,
            "SELECT subtask_id FROM minion_subtask_acquirement WHERE acquirement_id = :a",
            a=victim,
        )
    )[0]
    eng = create_async_engine(fresh_url, connect_args={"server_settings": {"lock_timeout": "300"}})
    sm2: Sm = async_sessionmaker(eng, expire_on_commit=False)
    seen: list[tuple[type[BaseException], Any]] = []
    real_rs = mod._row_specific

    def spy(exc: BaseException) -> bool:
        orig = exc.orig if isinstance(exc, DBAPIError) else None
        seen.append((type(exc), getattr(orig, "sqlstate", None)))
        return real_rs(exc)

    monkeypatch.setattr(mod, "_row_specific", spy)
    try:
        async with sm() as blocker:
            await blocker.execute(
                text("SELECT 1 FROM minion_subtask WHERE subtask_id = :s FOR UPDATE"),
                {"s": victim_subtask},
            )
            res = await reclaim_stale(sm2, timeout_s=TIMEOUT, batch_size=100)
            await blocker.rollback()
        assert res[ReclaimResult.EXPIRED_REQUEUED] == 2
        assert mod._QUARANTINE == {}
        assert await _rows(
            sm,
            "SELECT count(*) FROM minion_subtask_acquirement WHERE acquirement_id = :a",
            a=victim,
        ) == [(1,)]
        assert seen and all(t is not OperationalError and st == "55P03" for t, st in seen)
        assert all(issubclass(t, DBAPIError) for t, _ in seen)
        res = await reclaim_stale(sm2, timeout_s=TIMEOUT, batch_size=100)
        assert res[ReclaimResult.EXPIRED_REQUEUED] == 1
    finally:
        await eng.dispose()


class _PgError(Exception):
    def __init__(self, sqlstate: str) -> None:
        super().__init__(sqlstate)
        self.sqlstate = sqlstate


@pytest.mark.integration
@pytest.mark.parametrize(
    ("sqlstate", "quarantined"),
    [
        ("40P01", False),  # deadlock
        ("40001", False),  # serialization failure
        ("57014", False),  # statement_timeout
        ("55P03", False),  # lock_not_available
        ("53300", False),  # too_many_connections
        ("57P03", False),  # cannot_connect_now
        ("08006", False),  # connection failure
        ("23505", True),  # unique violation: about the row
        ("22P02", True),  # invalid text representation: about the row
    ],
)
async def test_sqlstate_classifies_quarantine(
    env: tuple[AsyncClient, Any], monkeypatch: pytest.MonkeyPatch, sqlstate: str, quarantined: bool
) -> None:
    import minion_server.services.reclaim as mod
    from sqlalchemy.exc import DBAPIError

    c, app = env
    sm: Sm = app.state.sessionmaker
    got = await _acquire_many(c, sm, 3, "ss")
    await _backdate(sm)
    poisoned = got[0][0]["acquirement_id"]
    monkeypatch.setattr(
        mod,
        "reclaim_chunk",
        _poison_one(mod, {poisoned}, DBAPIError("stmt", {}, _PgError(sqlstate))),
    )
    await reclaim_stale(sm, timeout_s=TIMEOUT, batch_size=100)
    assert len(mod._QUARANTINE) == (1 if quarantined else 0)
