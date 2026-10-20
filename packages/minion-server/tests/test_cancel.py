"""Task cancel + sweep (TASK-3.2 S4), max lease and heartbeat rate limit. Fresh DB per test."""

import asyncio
import hashlib
import threading
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from minion_common.schemas import ModelEntry, TaskCreate
from minion_server.app import create_app
from minion_server.repositories import tasks as tasks_repo
from minion_server.services.acquisition import acquire
from minion_server.services.reclaim import (
    ReclaimResult,
    reclaim_one,
    reclaim_stale,
    sweep_cancelled,
)
from minion_server.services.splitter import split_one
from minion_server.settings import ServerSettings
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from conftest import (
    TEST_MODELS_FILE,
    S3Env,
    _auth,
    _req,
    acquire_after_claim,
    assert_counts_match,
    make_executors,
    seed_task,
)

BASE = "/api/v1/executor"
TASKS = "/api/v1/tasks"
KEY = "k-cancel-secret"
OTHER_KEY = "k-cancel-other"
DATA = b'{"id":"a"}\n{"id":"b"}\n'
CBODY = {"record_count": 10, "byte_size": len(DATA), "sha256": hashlib.sha256(DATA).hexdigest()}
FAIL = {"retryable": True, "code": "OOM", "message": "boom"}
Sm = async_sessionmaker[AsyncSession]
USER = {"Authorization": f"Bearer {KEY}"}


def _cfg(url: str, s3: S3Env, keys: Path, **extra: Any) -> ServerSettings:
    return ServerSettings(
        database_url=SecretStr(url),
        s3_endpoint_url=s3.endpoint_url,
        s3_access_key=SecretStr(s3.access_key),
        s3_secret_key=SecretStr(s3.secret_key),
        s3_bucket=s3.bucket,
        s3_region=s3.region,
        api_keys_file=str(keys),
        models_file=TEST_MODELS_FILE,
        splitter_poll_s=3600,
        db_pool_size=40,
        **{
            "heartbeat_min_interval_s": 0.0,
            "register_min_interval_s": 0.0,
            "acquire_min_interval_s": 0.0,
            "presence_min_interval_s": 0.0,
            **extra,
        },
    )


def _sha(k: str) -> str:
    return hashlib.sha256(k.encode()).hexdigest()


def _keys(tmp_path: Path) -> Path:
    p = tmp_path / "keys.yaml"
    h = _sha
    p.write_text(
        f"keys:\n  - name: alice\n    sha256: {h(KEY)}\n    teams: [acq]\n"
        f"  - name: bob\n    sha256: {h(OTHER_KEY)}\n    teams: [elsewhere]\n"
    )
    return p


@pytest.fixture
async def env(fresh_url: str, s3: S3Env, tmp_path: Path) -> AsyncIterator[tuple[AsyncClient, Any]]:
    app = create_app(_cfg(fresh_url, s3, _keys(tmp_path)))
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
    ):
        yield c, app


def _cancel(c: AsyncClient, name: str, headers: dict[str, str] = USER, team: str = "acq") -> Any:
    return c.post(f"{TASKS}/{team}/p/{name}/cancel", headers=headers)


async def _acquire(c: AsyncClient, token: str) -> dict[str, Any]:
    r = await c.post(f"{BASE}/acquire", json=_req().model_dump(mode="json"), headers=_auth(token))
    assert r.status_code == 200, r.text
    a: dict[str, Any] = r.json()
    return a


def _call(c: AsyncClient, a: dict[str, Any], tok: str, op: str) -> Any:
    body = {"complete": CBODY, "fail": FAIL, "release": None, "heartbeat": {}}[op]
    return c.post(f"{BASE}/subtasks/{a['acquirement_id']}/{op}", json=body, headers=_auth(tok))


async def _rows(sm: Sm, sql: str, **p: Any) -> list[tuple[Any, ...]]:
    async with sm() as s:
        return [tuple(r) for r in (await s.execute(text(sql), p)).all()]


async def _task_state(sm: Sm, name: str) -> tuple[str, int, list[str]]:
    (status, failed), *_ = await _rows(
        sm, "SELECT status, failed_subtasks FROM minion_ro.task WHERE task_name=:n", n=name
    )
    subs = await _rows(
        sm, "SELECT status FROM minion_subtask WHERE task_name=:n ORDER BY subtask_id", n=name
    )
    return status, failed, [r[0] for r in subs]


_BACKDATE = {
    "heartbeat": "UPDATE minion_executor_heartbeat SET last_seen_at = now() - interval '1 hour'",
    "lease": "UPDATE minion_subtask_acquirement SET acquired_at = now() - interval '1 hour'",
}


async def _backdate(sm: Sm, which: str) -> None:
    async with sm() as s, s.begin():
        await s.execute(text(_BACKDATE[which]))


# --- responses and authz -------------------------------------------------------------------------


@pytest.mark.integration
async def test_cancel_responses(env: tuple[AsyncClient, Any]) -> None:
    c, app = env
    sm = app.state.sessionmaker
    await seed_task(sm, "r1", 2)
    await seed_task(sm, "done", 1, status="SUCCEEDED")
    r = await _cancel(c, "r1")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "CANCELLED" and body["counts"]["cancelled"] == 2
    again = await _cancel(c, "r1")  # idempotent
    assert again.status_code == 200 and again.json()["status"] == "CANCELLED"
    r = await _cancel(c, "done")
    assert r.status_code == 409 and r.json()["detail"]["code"] == "TASK_FINISHED"
    assert (await _task_state(sm, "done"))[0] == "SUCCEEDED"
    assert (await _cancel(c, "nope")).status_code == 404
    assert (await _cancel(c, "Bad Name!")).status_code == 404
    other = {"Authorization": f"Bearer {OTHER_KEY}"}
    assert (await _cancel(c, "r1", other)).status_code == 403
    assert (await _cancel(c, "r1", {})).status_code == 401
    assert (await _cancel(c, "r1", {"Authorization": "Bearer nope"})).status_code == 401


@pytest.mark.integration
async def test_cancel_pending_task_stops_acquisition(env: tuple[AsyncClient, Any]) -> None:
    c, app = env
    sm = app.state.sessionmaker
    await seed_task(sm, "p1", 3)
    [(_, tok)] = await make_executors(sm, 1)
    assert (await _cancel(c, "p1")).status_code == 200
    status, failed, subs = await _task_state(sm, "p1")
    assert (status, failed, subs) == ("CANCELLED", 0, ["CANCELLED"] * 3)
    r = await c.post(f"{BASE}/acquire", json=_req().model_dump(mode="json"), headers=_auth(tok))
    assert r.status_code == 204
    (fin,) = await _rows(sm, "SELECT finished_at IS NOT NULL FROM minion_task WHERE task_name='p1'")
    assert fin[0] is True


@pytest.mark.integration
async def test_cancel_splitting_task(env: tuple[AsyncClient, Any]) -> None:
    c, app = env
    sm = app.state.sessionmaker
    await seed_task(sm, "sp", 0, status="SPLITTING")
    assert (await _cancel(c, "sp")).status_code == 200
    (row,) = await _rows(
        sm, "SELECT status, next_split_at IS NULL FROM minion_task WHERE task_name='sp'"
    )
    assert tuple(row) == ("CANCELLED", True)  # the splitter's fenced lease ops no longer match


@pytest.mark.integration
async def test_cancel_during_split(
    env: tuple[AsyncClient, Any], s3: S3Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cancel while the splitter is mid-plan: it loses its lease and never overwrites CANCELLED."""
    c, app = env
    sm = app.state.sessionmaker
    storage = app.state.storage
    app.state.splitter_task.cancel()  # the test drives split_one itself
    await asyncio.gather(app.state.splitter_task, return_exceptions=True)
    data = b"".join(f'{{"id":"{i}"}}\n'.encode() for i in range(40))
    s3.client.put_object(Bucket=s3.bucket, Key="inputs/acq/dur/a.jsonl", Body=data)
    body = TaskCreate.model_validate(
        {
            "team": "acq",
            "project": "p",
            "task_name": "dur",
            "type": "embedding",
            "model": "fake-embed",
            "input_uri": f"s3://{s3.bucket}/inputs/acq/dur/",
            "output_uri": f"s3://{s3.bucket}/outputs/acq/dur/",
            "records_per_subtask": 5,
        }
    )
    model = ModelEntry(
        model_id="fake-embed",
        type="embedding",
        engine="mlx",
        ref="o/m",
        revision="a" * 40,
        memory_gb=0.1,
    )
    async with sm() as s, s.begin():
        assert await tasks_repo.insert_task(
            s,
            body,
            model=model,
            input_uri=body.input_uri,
            output_uri=body.output_uri,
            created_by="t",
        )
    release = threading.Event()
    cut = len(data) * 3 // 4

    def slow_iter(key: str, *, etag: str, size: int) -> Any:
        yield data[:cut]
        release.wait(10)  # blocks the splitter thread until the cancel has committed
        yield data[cut:]

    monkeypatch.setattr(storage, "iter_object", slow_iter)
    split = asyncio.create_task(split_one(sm, storage, batch_size=2))
    for _ in range(200):  # first batch(es) land while the rest of the input is "downloading"
        n = (await _rows(sm, "SELECT count(*) FROM minion_subtask WHERE task_name='dur'"))[0][0]
        if n >= 2:
            break
        await asyncio.sleep(0.05)
    assert n >= 2
    assert (await _cancel(c, "dur")).status_code == 200
    release.set()
    assert await asyncio.wait_for(split, 20) is True
    await sweep_cancelled(sm)
    status, _, subs = await _task_state(sm, "dur")
    assert status == "CANCELLED" and "PENDING" not in subs and subs  # no overwrite, no stragglers
    assert (await _rows(sm, "SELECT next_split_at IS NULL FROM minion_task"))[0][0] is True


# --- running owners ------------------------------------------------------------------------------


@pytest.mark.integration
async def test_running_owner_sees_cancel_and_releases(env: tuple[AsyncClient, Any]) -> None:
    c, app = env
    sm = app.state.sessionmaker
    await seed_task(sm, "run1", 3)
    [(_, tok)] = await make_executors(sm, 1)
    a = await _acquire(c, tok)
    hb = await _call(c, a, tok, "heartbeat")
    assert hb.status_code == 200 and hb.json()["cancel"] is False
    assert (await _cancel(c, "run1")).status_code == 200
    # RUNNING keeps its owner; only the two PENDING subtasks were cancelled.
    status, _, subs = await _task_state(sm, "run1")
    assert status == "CANCELLED" and sorted(subs) == ["CANCELLED", "CANCELLED", "RUNNING"]
    hb = await _call(c, a, tok, "heartbeat")
    assert hb.status_code == 200 and hb.json()["cancel"] is True
    assert (await _call(c, a, tok, "release")).status_code == 204
    status, failed, subs = await _task_state(sm, "run1")
    assert (status, failed, subs) == ("CANCELLED", 0, ["CANCELLED"] * 3)
    (att,) = await _rows(sm, "SELECT outcome FROM minion_subtask_attempt")
    assert att[0] == "CANCELLED"
    assert (await assert_counts_match(sm, "acq", "p", "run1"))["CANCELLED"] == 3


@pytest.mark.integration
async def test_fail_on_cancelled_task_is_cancelled_without_budget(
    env: tuple[AsyncClient, Any],
) -> None:
    c, app = env
    sm = app.state.sessionmaker
    await seed_task(sm, "f1", 1)
    [(_, tok)] = await make_executors(sm, 1)
    a = await _acquire(c, tok)
    await _cancel(c, "f1")
    assert (await _call(c, a, tok, "fail")).status_code == 204
    status, failed, subs = await _task_state(sm, "f1")
    assert (status, failed, subs) == ("CANCELLED", 0, ["CANCELLED"])
    (att,) = await _rows(sm, "SELECT outcome FROM minion_subtask_attempt")
    assert att[0] == "FAILED"
    assert (await assert_counts_match(sm, "acq", "p", "f1"))["CANCELLED"] == 1


@pytest.mark.integration
async def test_complete_after_cancel_is_accepted(env: tuple[AsyncClient, Any], s3: S3Env) -> None:
    c, app = env
    sm = app.state.sessionmaker
    await seed_task(sm, "c1", 1)
    [(_, tok)] = await make_executors(sm, 1)
    a = await _acquire(c, tok)
    await _cancel(c, "c1")
    base = f"outputs/acq/c1/_attempts/{a['subtask_id']:06d}/{a['attempt']}.jsonl"
    s3.client.put_object(Bucket=s3.bucket, Key=base, Body=DATA)
    assert (await _call(c, a, tok, "complete")).status_code == 204
    status, _, subs = await _task_state(sm, "c1")
    assert (status, subs) == ("CANCELLED", ["SUCCEEDED"])  # work kept, task stays CANCELLED
    assert (await assert_counts_match(sm, "acq", "p", "c1"))["SUCCEEDED"] == 1
    got = await c.get(f"{TASKS}/acq/p/c1", headers=USER)
    assert got.json()["status"] == "CANCELLED" and got.json()["counts"]["succeeded"] == 1


@pytest.mark.integration
async def test_reclaim_on_cancelled_task_is_cancelled(env: tuple[AsyncClient, Any]) -> None:
    c, app = env
    sm = app.state.sessionmaker
    await seed_task(sm, "rc", 1)
    [(_, tok)] = await make_executors(sm, 1)
    await _acquire(c, tok)
    await _cancel(c, "rc")
    await _backdate(sm, "heartbeat")
    res = await reclaim_stale(sm, timeout_s=900, batch_size=10)
    assert res == {ReclaimResult.EXPIRED_CANCELLED: 1}
    assert await _task_state(sm, "rc") == ("CANCELLED", 0, ["CANCELLED"])
    assert (await assert_counts_match(sm, "acq", "p", "rc"))["CANCELLED"] == 1
    (att,) = await _rows(sm, "SELECT outcome FROM minion_subtask_attempt")
    assert att[0] == "EXPIRED"


# --- sweep ---------------------------------------------------------------------------------------


@pytest.mark.integration
async def test_sweep_heals_pending_under_cancelled_task(env: tuple[AsyncClient, Any]) -> None:
    c, app = env
    sm = app.state.sessionmaker
    await seed_task(sm, "sw", 3)
    await seed_task(sm, "old", 2)
    await _cancel(c, "sw")
    await _cancel(c, "old")
    async with sm() as s, s.begin():  # simulate a requeue that raced the cancel
        await s.execute(text("UPDATE minion_subtask SET status='PENDING'"))
        await s.execute(
            text(
                "UPDATE minion_task SET finished_at = now() - interval '2 hours' "
                "WHERE task_name='old'"
            )
        )
    assert await sweep_cancelled(sm, window_s=3600) == 3  # 'old' is outside the window
    assert (await _task_state(sm, "sw"))[2] == ["CANCELLED"] * 3
    assert (await _task_state(sm, "old"))[2] == ["PENDING"] * 2
    assert await sweep_cancelled(sm, window_s=3600) == 0


@pytest.mark.integration
async def test_cancel_concurrent_with_acquires(env: tuple[AsyncClient, Any]) -> None:
    """No 5xx or deadlock. An acquire in flight when cancel commits may win (RUNNING subtask under
    a CANCELLED task, told to stop by heartbeat.cancel); everything else ends CANCELLED.

    Residual READ COMMITTED window: an acquire statement that started before the cancel commit
    cannot see it, so it may still win. That is accepted and handled by heartbeat cancel=true; the
    deterministic guarantee (no acquire starting after the commit) is the stale-candidate test."""
    c, app = env
    sm = app.state.sessionmaker
    await seed_task(sm, "cc", 20)
    execs = await make_executors(sm, 20)
    results = await asyncio.gather(
        _cancel(c, "cc"),
        *(
            c.post(f"{BASE}/acquire", json=_req().model_dump(mode="json"), headers=_auth(t))
            for _, t in execs
        ),
    )
    assert results[0].status_code == 200
    assert all(r.status_code in (200, 204) for r in results[1:])
    pairs = zip(results[1:], execs, strict=True)
    won = [(r.json(), t) for r, (_, t) in pairs if r.status_code == 200]
    await sweep_cancelled(sm)
    status, _, subs = await _task_state(sm, "cc")
    assert status == "CANCELLED" and "PENDING" not in subs
    assert subs.count("RUNNING") == len(won)
    for a, t in won:  # whatever stayed RUNNING learns of the cancel and releases cleanly
        assert (await _call(c, a, t, "heartbeat")).json()["cancel"] is True
        assert (await _call(c, a, t, "release")).status_code == 204
    assert (await _task_state(sm, "cc"))[2] == ["CANCELLED"] * 20
    assert (await assert_counts_match(sm, "acq", "p", "cc"))["CANCELLED"] == 20


@pytest.mark.integration
async def test_acquire_after_cancel_commit_gets_nothing(env: tuple[AsyncClient, Any]) -> None:
    """A cancel committed before the claim statement starts is seen by it: no acquirement."""
    c, app = env
    sm = app.state.sessionmaker
    await seed_task(sm, "sc", 2)
    [(ex, _)] = await make_executors(sm, 1)
    assert (await _cancel(c, "sc")).status_code == 200
    async with sm() as s:
        got = await acquire(
            s, ex, _req(), settings=app.state.settings, storage=app.state.storage,
            models=app.state.models,
        )  # fmt: skip
    assert got is None
    assert await _rows(sm, "SELECT count(*) FROM minion_subtask_acquirement") == [(0,)]
    assert (await _task_state(sm, "sc"))[2] == ["CANCELLED"] * 2


@pytest.mark.integration
@pytest.mark.parametrize("window", [1, 16])
async def test_acquire_update_guard_rolls_back(env: tuple[AsyncClient, Any], window: int) -> None:
    """Production path (acquire -> claim): the task is cancelled (committed by another session)
    after the claim INSERT won and before the re-check UPDATE. n_candidates > 0 and won None ->
    _LostRaceError -> rollback; the next round sees the cancelled task and returns None."""
    _, app = env
    sm = app.state.sessionmaker
    await seed_task(sm, "ug", 2)
    [(ex, _)] = await make_executors(sm, 1)

    async def cancel() -> None:
        async with sm() as s, s.begin():
            await s.execute(text("UPDATE minion_task SET status='CANCELLED' WHERE task_name='ug'"))

    got = await acquire_after_claim(
        sm, ex, cancel, settings=app.state.settings, storage=app.state.storage,
        models=app.state.models, window=window,
    )  # fmt: skip
    assert got is None
    assert await _rows(sm, "SELECT count(*) FROM minion_subtask_acquirement") == [(0,)]
    assert await _rows(sm, "SELECT count(*) FROM minion_executor_heartbeat") == [(0,)]
    status, _, subs = await _task_state(sm, "ug")
    assert status == "CANCELLED" and subs == ["PENDING"] * 2


# --- max lease -----------------------------------------------------------------------------------


@pytest.mark.integration
async def test_max_lease_reclaims_despite_fresh_heartbeat(env: tuple[AsyncClient, Any]) -> None:
    c, app = env
    sm = app.state.sessionmaker
    await seed_task(sm, "ml", 2)
    [(_, ta), (_, tb)] = await make_executors(sm, 2)
    a = await _acquire(c, ta)
    b = await _acquire(c, tb)
    await _backdate(sm, "lease")  # both old, beats fresh
    async with sm() as s, s.begin():  # b is within the lease
        await s.execute(
            text("UPDATE minion_subtask_acquirement SET acquired_at=now() WHERE acquirement_id=:i"),
            {"i": b["acquirement_id"]},
        )
    assert await reclaim_stale(sm, timeout_s=900, batch_size=10) == {}  # no lease: nothing stale
    res = await reclaim_stale(sm, timeout_s=900, batch_size=10, max_lease_s=1800)
    assert res == {ReclaimResult.EXPIRED_REQUEUED: 1}
    (att,) = await _rows(sm, "SELECT outcome, error->>'code' FROM minion_subtask_attempt")
    assert tuple(att) == ("EXPIRED", "MAX_LEASE_EXCEEDED")
    assert (await _call(c, a, ta, "heartbeat")).status_code == 409  # zombie owner
    assert (await _call(c, b, tb, "heartbeat")).status_code == 200
    # Re-check under the lock: a fresh acquirement is NOT reclaimed by id either.
    async with sm() as s, s.begin():
        r = await reclaim_one(s, uuid.UUID(b["acquirement_id"]), timeout_s=900, max_lease_s=1800)
    assert r == ReclaimResult.NOT_STALE


# --- heartbeat rate limit ------------------------------------------------------------------------


@pytest.mark.integration
async def test_heartbeat_rate_limit(fresh_url: str, s3: S3Env, tmp_path: Path) -> None:
    app = create_app(_cfg(fresh_url, s3, _keys(tmp_path), heartbeat_min_interval_s=3600))
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
    ):
        sm = app.state.sessionmaker
        await seed_task(sm, "rl", 1)
        [(_, tok)] = await make_executors(sm, 1)
        a = await _acquire(c, tok)
        # The claim's own heartbeat row is no beat: the first heartbeat (the ack) is never limited.
        assert (await _call(c, a, tok, "heartbeat")).status_code == 200
        before = await _rows(sm, "SELECT last_seen_at, progress FROM minion_executor_heartbeat")
        r = await c.post(
            f"{BASE}/subtasks/{a['acquirement_id']}/heartbeat",
            json={"progress": {"records_done": 1, "records_total": 2}},
            headers=_auth(tok),
        )
        assert r.status_code == 429
        assert r.json()["detail"]["code"] == "TOO_MANY_HEARTBEATS"
        assert int(r.headers["retry-after"]) >= 1
        after = await _rows(sm, "SELECT last_seen_at, progress FROM minion_executor_heartbeat")
        assert after == before and after[0][1] is None  # not refreshed, progress not stored
        # An aged row is accepted again.
        await _backdate(sm, "heartbeat")
        assert (await _call(c, a, tok, "heartbeat")).status_code == 200
        assert (await _call(c, a, tok, "heartbeat")).status_code == 429


@pytest.mark.integration
async def test_sweep_cancelled_batches(env: tuple[AsyncClient, Any]) -> None:
    c, app = env
    sm = app.state.sessionmaker
    await seed_task(sm, "sb", 5)
    await _cancel(c, "sb")
    async with sm() as s, s.begin():
        await s.execute(text("UPDATE minion_subtask SET status='PENDING'"))
    async with sm() as s, s.begin():
        assert await tasks_repo.sweep_cancelled(s, 3600, 2) == 2  # bounded per call
    assert await sweep_cancelled(sm, window_s=3600, batch=2) == 3  # loops 2,1
    assert (await _task_state(sm, "sb"))[2] == ["CANCELLED"] * 5
