"""TASK-16 AC#2: task counts (counters + acquirements) stay equal to the subtask aggregate
under concurrent acquire / complete / fail / release / reclaim / cancel.

Every check compares the API counts with a `minion_subtask` aggregate in one REPEATABLE READ
snapshot (`conftest.assert_counts_match`), so it is exact even in the middle of a storm.
Workers draw their actions from a seeded RNG; the interleaving is up to the event loop and
Postgres, and the assertions hold for every interleaving. Loops are bounded by deadlines.
"""

import asyncio
import hashlib
import random
from collections import Counter
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from minion_server.app import create_app
from minion_server.services.completion import derive_task_status
from minion_server.services.reclaim import reclaim_stale, sweep_cancelled
from minion_server.settings import ServerSettings
from minion_watchdog.main import tick
from minion_watchdog.settings import WatchdogSettings
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from conftest import (
    TEST_MODELS_FILE,
    S3Env,
    _auth,
    _req,
    assert_counts_match,
    make_executors,
    seed_task,
)

pytestmark = pytest.mark.integration

Sm = async_sessionmaker[AsyncSession]
BASE = "/api/v1/executor"
TEAM, PROJECT = "e2e", "p"
KEY = "k-counters-secret"
USER = {"Authorization": f"Bearer {KEY}"}
DATA = b'{"id":"a"}\n'
CBODY = {"record_count": 10, "byte_size": len(DATA), "sha256": hashlib.sha256(DATA).hexdigest()}
FAIL_RETRY = {"retryable": True, "code": "OOM", "message": "boom"}
FAIL_FATAL = {"retryable": False, "code": "BAD", "message": "boom"}
STORM_S = 6.0
WD = WatchdogSettings(database_url=SecretStr("postgresql+asyncpg://x/x"))


def _cfg(url: str, s3: S3Env, keys: Path) -> ServerSettings:
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
        db_pool_size=60,
        db_pool_timeout_s=15.0,
        register_min_interval_s=0,
        acquire_min_interval_s=0,
        presence_min_interval_s=0,
        heartbeat_min_interval_s=0,
    )


@pytest.fixture
async def env(fresh_url: str, s3: S3Env, tmp_path: Path) -> AsyncIterator[tuple[AsyncClient, Any]]:
    keys = tmp_path / "keys.yaml"
    digest = hashlib.sha256(KEY.encode()).hexdigest()
    keys.write_text(f"keys:\n  - name: alice\n    sha256: {digest}\n    teams: [{TEAM}]\n")
    app = create_app(_cfg(fresh_url, s3, keys))
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
    ):
        yield c, app


def _put_output(s3: S3Env, name: str, a: dict[str, Any]) -> None:
    key = f"outputs/{TEAM}/{name}/_attempts/{a['subtask_id']:06d}/{a['attempt']}.jsonl"
    s3.client.put_object(Bucket=s3.bucket, Key=key, Body=DATA)


async def _acquire(c: AsyncClient, tok: str) -> dict[str, Any] | None:
    r = await c.post(f"{BASE}/acquire", json=_req().model_dump(mode="json"), headers=_auth(tok))
    assert r.status_code in (200, 204, 429, 503), (r.status_code, r.text)
    if r.status_code != 200:
        return None
    a: dict[str, Any] = r.json()
    return a


def _op(c: AsyncClient, a: dict[str, Any], tok: str, op: str, body: Any = None) -> Any:
    return c.post(f"{BASE}/subtasks/{a['acquirement_id']}/{op}", json=body, headers=_auth(tok))


async def _backdate(sm: Sm, acquirement_id: str | None = None) -> None:
    sql = "UPDATE minion_executor_heartbeat SET last_seen_at = now() - interval '1 hour'"
    p: dict[str, Any] = {}
    if acquirement_id is not None:
        sql += " WHERE acquirement_id = CAST(:a AS uuid)"
        p = {"a": acquirement_id}
    async with sm() as s, s.begin():
        await s.execute(text(sql), p)


async def _scalar(sm: Sm, sql: str, **p: Any) -> Any:
    async with sm() as s:
        return (await s.execute(text(sql), p)).scalar_one()


class Storm:
    """Workers + reclaimer + checker against one task; `run` returns the action tally."""

    def __init__(
        self, c: AsyncClient, sm: Sm, s3: S3Env, name: str, execs: list[tuple[Any, str]]
    ) -> None:
        self.c, self.sm, self.s3, self.name, self.execs = c, sm, s3, name, execs
        self.stop = asyncio.Event()
        self.tally: Counter[str] = Counter()
        self.checks = 0

    async def worker(self, idx: int, tok: str) -> None:
        rng = random.Random(idx)  # noqa: S311  (test determinism of the action mix, not security)
        while not self.stop.is_set():
            a = await _acquire(self.c, tok)
            if a is None:
                await asyncio.sleep(0.02)  # backoff while the queue is empty / capped
                continue
            roll = rng.random()
            if roll < 0.50:
                await asyncio.to_thread(_put_output, self.s3, self.name, a)
                r = await _op(self.c, a, tok, "complete", CBODY)
                self.tally["complete"] += 1
            elif roll < 0.62:
                r = await _op(self.c, a, tok, "fail", FAIL_RETRY)
                self.tally["fail_retry"] += 1
            elif roll < 0.68:
                r = await _op(self.c, a, tok, "fail", FAIL_FATAL)
                self.tally["fail_fatal"] += 1
            elif roll < 0.84:
                r = await _op(self.c, a, tok, "release")
                self.tally["release"] += 1
            else:  # vanish: heartbeat backdated, acquirement left for the reclaimer
                await _backdate(self.sm, a["acquirement_id"])
                self.tally["vanish"] += 1
                continue
            # a live owner is never reclaimed (its heartbeat is fresh), so every op is accepted
            assert r.status_code == 204, (r.status_code, r.text)

    async def reclaimer(self) -> None:
        while not self.stop.is_set():
            await reclaim_stale(self.sm, timeout_s=900, batch_size=20)
            await asyncio.sleep(0.05)

    async def checker(self) -> None:
        while not self.stop.is_set():
            await assert_counts_match(self.sm, TEAM, PROJECT, self.name)
            self.checks += 1
            await asyncio.sleep(0.05)

    async def run(self, duration_s: float, mid: Any = None) -> Counter[str]:
        tasks = [asyncio.create_task(self.worker(i, t)) for i, (_, t) in enumerate(self.execs)]
        tasks += [asyncio.create_task(self.reclaimer()), asyncio.create_task(self.checker())]
        try:
            if mid is None:
                await asyncio.sleep(duration_s)
            else:
                await asyncio.sleep(duration_s / 2)
                await mid()
                await asyncio.sleep(duration_s / 2)
        finally:
            self.stop.set()
            results = await asyncio.gather(*tasks, return_exceptions=True)
        errs = [r for r in results if isinstance(r, BaseException)]
        assert not errs, errs[0]
        return self.tally


async def _quiesce(sm: Sm, name: str) -> None:
    """Reclaim every leftover acquirement (all heartbeats are backdated), bounded."""
    for _ in range(50):
        await _backdate(sm)
        await tick(sm, WD, asyncio.Event())
        if await _scalar(sm, "SELECT count(*) FROM minion_subtask_acquirement") == 0:
            return
    raise AssertionError("acquirements did not drain")


async def _drain(c: AsyncClient, sm: Sm, s3: S3Env, name: str, tok: str) -> None:
    """One executor completes whatever is still PENDING (bounded)."""
    for _ in range(2000):
        a = await _acquire(c, tok)
        if a is None:
            return
        await asyncio.to_thread(_put_output, s3, name, a)
        assert (await _op(c, a, tok, "complete", CBODY)).status_code == 204
    raise AssertionError("drain did not finish")


# --- storms ------------------------------------------------------------------------------------


async def test_counts_consistent_under_storm(env: tuple[AsyncClient, Any], s3: S3Env) -> None:
    c, app = env
    sm: Sm = app.state.sessionmaker
    name, n = "storm", 200
    await seed_task(sm, name, n, team=TEAM, project=PROJECT)
    async with sm() as s, s.begin():
        await s.execute(text("UPDATE minion_subtask SET max_attempts = 3"))
    execs = await make_executors(sm, 24)
    storm = Storm(c, sm, s3, name, execs)
    tally = await storm.run(STORM_S)
    assert storm.checks > 20, storm.checks
    assert sum(tally.values()) > 0 and tally["complete"] > 0, tally

    await assert_counts_match(sm, TEAM, PROJECT, name)
    await _quiesce(sm, name)
    await assert_counts_match(sm, TEAM, PROJECT, name)
    await _drain(c, sm, s3, name, execs[0][1])
    await _quiesce(sm, name)
    counts = await assert_counts_match(sm, TEAM, PROJECT, name)

    assert counts["RUNNING"] == 0 and counts["PENDING"] == 0
    assert counts["SUCCEEDED"] + counts["FAILED"] == n and counts["CANCELLED"] == 0
    got = (await c.get(f"/api/v1/tasks/{TEAM}/{PROJECT}/{name}", headers=USER)).json()
    api = got["counts"]
    assert api["total"] == n
    assert api["succeeded"] + api["failed"] == n
    expected = derive_task_status(n, api["succeeded"], api["failed"])
    assert expected is not None and got["status"] == expected.value


async def test_counts_consistent_with_cancel_mid_storm(
    env: tuple[AsyncClient, Any], s3: S3Env
) -> None:
    c, app = env
    sm: Sm = app.state.sessionmaker
    name, n = "cstorm", 200
    await seed_task(sm, name, n, team=TEAM, project=PROJECT)
    async with sm() as s, s.begin():
        await s.execute(text("UPDATE minion_subtask SET max_attempts = 3"))
    execs = await make_executors(sm, 24)
    held_execs = await make_executors(sm, 5)  # idle owners that keep their subtask over the cancel
    held = []
    for _, tok in held_execs:
        a = await _acquire(c, tok)
        assert a is not None
        held.append((a, tok))
    await assert_counts_match(sm, TEAM, PROJECT, name)

    async def cancel() -> None:
        r = await c.post(f"/api/v1/tasks/{TEAM}/{PROJECT}/{name}/cancel", headers=USER)
        assert r.status_code == 200, r.text

    storm = Storm(c, sm, s3, name, execs)
    tally = await storm.run(STORM_S, mid=cancel)
    assert storm.checks > 20 and tally["complete"] > 0, (storm.checks, tally)
    await assert_counts_match(sm, TEAM, PROJECT, name)

    # in-flight owners finish on the cancelled task: the cancelled branches of each owner op
    (a_ok, t_ok), (a_fail, t_fail), (a_rel, t_rel), (a_dead, _), (a_dead2, _) = held
    await asyncio.to_thread(_put_output, s3, name, a_ok)
    assert (await _op(c, a_ok, t_ok, "complete", CBODY)).status_code == 204
    await assert_counts_match(sm, TEAM, PROJECT, name)
    assert (await _op(c, a_fail, t_fail, "fail", FAIL_RETRY)).status_code == 204  # -> CANCELLED
    await assert_counts_match(sm, TEAM, PROJECT, name)
    assert (await _op(c, a_rel, t_rel, "release")).status_code == 204  # -> CANCELLED
    await assert_counts_match(sm, TEAM, PROJECT, name)
    # reclaim of cancelled work (decide -> CANCELLED)
    await _backdate(sm, a_dead["acquirement_id"])
    await reclaim_stale(sm, timeout_s=900, batch_size=50)
    await assert_counts_match(sm, TEAM, PROJECT, name)
    # a requeue that raced the cancel leaves a PENDING straggler: the sweep cancels it
    async with sm() as s, s.begin():
        await s.execute(
            text("DELETE FROM minion_subtask_acquirement WHERE acquirement_id = CAST(:a AS uuid)"),
            {"a": a_dead2["acquirement_id"]},
        )
        await s.execute(
            text("UPDATE minion_subtask SET status='PENDING' WHERE task_name=:n AND subtask_id=:i"),
            {"n": name, "i": a_dead2["subtask_id"]},
        )
    mid_counts = await assert_counts_match(sm, TEAM, PROJECT, name)
    assert mid_counts["PENDING"] >= 1
    assert await sweep_cancelled(sm) >= 1
    await _quiesce(sm, name)
    counts = await assert_counts_match(sm, TEAM, PROJECT, name)

    assert counts["PENDING"] == 0 and counts["RUNNING"] == 0
    assert counts["CANCELLED"] > 0
    assert counts["CANCELLED"] + counts["SUCCEEDED"] + counts["FAILED"] == n
    status = await _scalar(sm, "SELECT status FROM minion_task WHERE task_name=:n", n=name)
    assert status == "CANCELLED"


async def test_double_cancel_and_sweep_on_25k_subtasks(env: tuple[AsyncClient, Any]) -> None:
    """Two cancel loops plus the sweep bump the same 16 shards of a 3-batch task: no deadlock
    (40P01), every subtask counted exactly once."""
    c, app = env
    sm: Sm = app.state.sessionmaker
    name, n = "big", 25_000
    await seed_task(sm, name, 0, team=TEAM, project=PROJECT)
    async with sm() as s, s.begin():
        await s.execute(
            text(
                "INSERT INTO minion_subtask (team, project, task_name, subtask_id, "
                "input_object_key, byte_start, byte_end, first_record, record_count) "
                "SELECT :t, :p, :n, i, 'inputs/x', i * 100, i * 100 + 100, i * 10, 10 "
                "FROM generate_series(0, :last) AS i"
            ),
            {"t": TEAM, "p": PROJECT, "n": name, "last": n - 1},
        )
        await s.execute(
            text("UPDATE minion_task SET total_subtasks=:n WHERE task_name=:name"),
            {"n": n, "name": name},
        )
    async with sm() as s:  # what autovacuum does in production; without statistics the cancel
        await s.execute(text("ANALYZE minion_subtask"))  # UPDATE plan is pathological (minutes)
        await s.commit()
    assert (await assert_counts_match(sm, TEAM, PROJECT, name))["PENDING"] == n

    stop = asyncio.Event()
    sweeps = 0

    async def sweeper() -> None:
        nonlocal sweeps
        while not stop.is_set():
            await sweep_cancelled(sm)
            sweeps += 1
            await asyncio.sleep(0)

    sw = asyncio.create_task(sweeper())
    try:
        url = f"/api/v1/tasks/{TEAM}/{PROJECT}/{name}/cancel"
        r1, r2 = await asyncio.wait_for(
            asyncio.gather(c.post(url, headers=USER), c.post(url, headers=USER)), timeout=120
        )
    finally:
        stop.set()
        await sw
    assert (r1.status_code, r2.status_code) == (200, 200), (r1.text, r2.text)
    counts = await assert_counts_match(sm, TEAM, PROJECT, name)
    assert counts["CANCELLED"] == n and counts["PENDING"] == 0
    shard_sum = await _scalar(
        sm, "SELECT sum(cancelled) FROM minion_task_counter WHERE task_name=:n", n=name
    )
    assert shard_sum == n
