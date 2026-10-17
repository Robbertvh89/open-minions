"""Owner-op races, counter atomicity, concurrent splitters and the HTTP e2e path (TASK-2.2).

Every test runs on its own empty database (`fresh_url`) so no other test's rows interfere.
"""

import asyncio
import hashlib
import itertools
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from minion_common.client import ExecutorClient, StaleAcquirementError, UserClient
from minion_common.enums import Engine, TaskStatus
from minion_common.schemas import (
    CompleteRequest,
    FailRequest,
    RegisterRequest,
    TaskCreate,
)
from minion_server.app import create_app
from minion_server.db.models import Subtask
from minion_server.db.session import create_engine, create_sessionmaker
from minion_server.repositories import completion as completion_repo
from minion_server.repositories import tasks as tasks_repo
from minion_server.services.splitter import split_one
from minion_server.settings import ServerSettings
from minion_server.splitting import slice_jsonl
from minion_server.storage import S3Storage
from pydantic import SecretStr
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from conftest import TEST_MODELS_FILE, S3Env, _auth, _budget, _req, make_executors, seed_task

pytestmark = pytest.mark.integration

BASE = "/api/v1/executor"
DATA = b'{"id":"a"}\n{"id":"b"}\n'
FAIL = {"retryable": False, "code": "OOM", "message": "boom"}
USER_KEY = "k-race-secret"


def _cfg(url: str, s3: S3Env, keys: Path | None = None) -> ServerSettings:
    return ServerSettings(
        database_url=SecretStr(url),
        s3_endpoint_url=s3.endpoint_url,
        s3_access_key=SecretStr(s3.access_key),
        s3_secret_key=SecretStr(s3.secret_key),
        s3_bucket=s3.bucket,
        s3_region=s3.region,
        api_keys_file=str(keys) if keys else None,
        models_file=TEST_MODELS_FILE,
        splitter_poll_s=3600,  # tests drive split_one themselves
        db_pool_size=60,
        register_min_interval_s=0,  # tests acquire back-to-back (TASK-6.2 has its own tests)
        acquire_min_interval_s=0,
        presence_min_interval_s=0,
    )


@pytest.fixture
async def app_env(
    fresh_url: str, s3: S3Env, tmp_path: Path
) -> AsyncIterator[tuple[AsyncClient, Any, ASGITransport]]:
    keys = tmp_path / "keys.yaml"
    digest = hashlib.sha256(USER_KEY.encode()).hexdigest()
    keys.write_text(f"keys:\n  - name: alice\n    sha256: {digest}\n    teams: [e2e]\n")
    app = create_app(_cfg(fresh_url, s3, keys))
    transport = ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=transport, base_url="http://t") as c,
    ):
        yield c, app, transport


def _put_output(s3: S3Env, team: str, name: str, a: dict[str, Any], body: bytes = DATA) -> str:
    base = f"outputs/{team}/{name}"
    key = f"{base}/_attempts/{a['subtask_id']:06d}/{a['attempt']}.jsonl"
    s3.client.put_object(Bucket=s3.bucket, Key=key, Body=body)
    return f"{base}/{a['subtask_id']:06d}.jsonl"


def _cbody(body: bytes = DATA, records: int = 10) -> dict[str, Any]:
    return {
        "record_count": records,
        "byte_size": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
    }


def _exists(s3: S3Env, key: str) -> bool:
    return bool(s3.client.list_objects_v2(Bucket=s3.bucket, Prefix=key).get("Contents"))


async def _acquire_all(c: AsyncClient, execs: list[Any]) -> list[tuple[dict[str, Any], str]]:
    body = _req().model_dump(mode="json")
    resps = await asyncio.gather(
        *(c.post(f"{BASE}/acquire", json=body, headers=_auth(t)) for _, t in execs)
    )
    assert all(r.status_code == 200 for r in resps), [r.status_code for r in resps]
    return [(r.json(), t) for r, (_, t) in zip(resps, execs, strict=True)]


async def _rows(sm: async_sessionmaker[AsyncSession], sql: str, **p: Any) -> list[tuple[Any, ...]]:
    async with sm() as s:
        return [tuple(r) for r in (await s.execute(text(sql), p)).all()]


# --- 1. owner-op races on one acquirement ------------------------------------------------------


@pytest.mark.parametrize("rival", ["release", "fail"])
async def test_complete_vs_rival_exactly_one_wins(
    app_env: tuple[AsyncClient, Any, ASGITransport], s3: S3Env, rival: str
) -> None:
    c, app, _ = app_env
    sm = app.state.sessionmaker
    n = 12
    team, name = f"rc{rival[0]}", f"race-{rival}"
    await seed_task(sm, name, n, team=team)
    execs = await make_executors(sm, n)
    owned = await _acquire_all(c, execs)
    dsts = {a["subtask_id"]: _put_output(s3, team, name, a) for a, _ in owned}

    def call(a: dict[str, Any], tok: str, op: str) -> Any:
        body = {"complete": _cbody(), "fail": FAIL, "release": None}[op]
        return c.post(f"{BASE}/subtasks/{a['acquirement_id']}/{op}", json=body, headers=_auth(tok))

    pairs = await asyncio.gather(
        *(asyncio.gather(call(a, t, "complete"), call(a, t, rival)) for a, t in owned)
    )
    complete_wins = set()
    for (a, _), (rc, rr) in zip(owned, pairs, strict=True):
        codes = sorted((rc.status_code, rr.status_code))
        assert codes == [204, 409], codes  # exactly one wins, the loser is stale
        if rc.status_code == 204:
            complete_wins.add(a["subtask_id"])

    for a, _ in owned:
        sid = a["subtask_id"]
        won = sid in complete_wins
        ((status, outuri),) = await _rows(
            sm,
            "SELECT status, output_uri FROM minion_subtask WHERE task_name=:n AND subtask_id=:i",
            n=name,
            i=sid,
        )
        outcomes = await _rows(
            sm,
            "SELECT outcome FROM minion_subtask_attempt WHERE task_name=:n AND subtask_id=:i",
            n=name,
            i=sid,
        )
        acq = await _rows(
            sm,
            "SELECT count(*) FROM minion_subtask_acquirement WHERE task_name=:n AND subtask_id=:i",
            n=name,
            i=sid,
        )
        assert len(outcomes) == 1 and acq == [(0,)]  # exactly one attempt record, acquirement gone
        if won:
            assert (status, outcomes[0][0]) == ("SUCCEEDED", "SUCCEEDED")
            assert outuri.endswith(dsts[sid])
            assert s3.client.get_object(Bucket=s3.bucket, Key=dsts[sid])["Body"].read() == DATA
        else:
            expected = ("PENDING", "RELEASED") if rival == "release" else ("FAILED", "FAILED")
            assert (status, outcomes[0][0]) == expected
            assert outuri is None
            assert not _exists(s3, dsts[sid])  # a lost complete promoted nothing

    ((succ, failed, tstatus),) = await _rows(
        sm,
        "SELECT succeeded_subtasks, failed_subtasks, status FROM minion_task WHERE task_name=:n",
        n=name,
    )
    n_ok = len(complete_wins)
    assert succ == n_ok
    assert failed == (n - n_ok if rival == "fail" else 0)
    if rival == "fail":
        expected_task = {n: "SUCCEEDED", 0: "FAILED"}.get(n_ok, "PARTIAL")
        assert tstatus == expected_task
    else:
        assert tstatus == ("SUCCEEDED" if n_ok == n else "RUNNING")


async def test_double_complete_same_owner_is_single_promotion(
    app_env: tuple[AsyncClient, Any, ASGITransport], s3: S3Env
) -> None:
    c, app, _ = app_env
    sm = app.state.sessionmaker
    await seed_task(sm, "dbl", 1, team="dbl")
    ((p, tok),) = await make_executors(sm, 1)
    ((a, _),) = await _acquire_all(c, [(p, tok)])
    dst = _put_output(s3, "dbl", "dbl", a)
    url = f"{BASE}/subtasks/{a['acquirement_id']}/complete"
    resps = await asyncio.gather(
        *(c.post(url, json=_cbody(), headers=_auth(tok)) for _ in range(8))
    )
    assert [r.status_code for r in resps] == [204] * 8  # lost-response retries are no-ops
    assert s3.client.get_object(Bucket=s3.bucket, Key=dst)["Body"].read() == DATA
    listed = s3.client.list_objects_v2(Bucket=s3.bucket, Prefix="outputs/dbl/dbl/")
    assert sorted(o["Key"] for o in listed["Contents"]) == sorted(
        [dst, f"outputs/dbl/dbl/_attempts/000000/{a['attempt']}.jsonl"]
    )
    assert await _rows(sm, "SELECT count(*) FROM minion_subtask_attempt WHERE task_name='dbl'") == [
        (1,)
    ]
    assert await _rows(
        sm, "SELECT succeeded_subtasks, failed_subtasks, status FROM minion_task WHERE team='dbl'"
    ) == [(1, 0, "SUCCEEDED")]


# --- 2. counters under concurrent completion ---------------------------------------------------


@pytest.mark.parametrize(("n_ok", "n_bad"), [(30, 0), (14, 6)])
async def test_concurrent_completes_counters_exact_status_derived_once(
    app_env: tuple[AsyncClient, Any, ASGITransport],
    s3: S3Env,
    monkeypatch: pytest.MonkeyPatch,
    n_ok: int,
    n_bad: int,
) -> None:
    c, app, _ = app_env
    sm = app.state.sessionmaker
    n = n_ok + n_bad
    name = f"cnt-{n_ok}-{n_bad}"
    await seed_task(sm, name, n, team="cnt")
    owned = await _acquire_all(c, await make_executors(sm, n))
    for a, _ in owned:
        _put_output(s3, "cnt", name, a)

    terminal_calls: list[TaskStatus] = []
    real = completion_repo.set_task_terminal

    async def spy(session: AsyncSession, o: Any, status: TaskStatus) -> None:
        terminal_calls.append(status)
        await real(session, o, status)

    monkeypatch.setattr(completion_repo, "set_task_terminal", spy)

    def call(i: int, a: dict[str, Any], tok: str) -> Any:
        op, body = ("complete", _cbody()) if i < n_ok else ("fail", FAIL)
        return c.post(f"{BASE}/subtasks/{a['acquirement_id']}/{op}", json=body, headers=_auth(tok))

    resps = await asyncio.gather(*(call(i, a, t) for i, (a, t) in enumerate(owned)))
    assert [r.status_code for r in resps] == [204] * n
    expected = TaskStatus.SUCCEEDED if n_bad == 0 else TaskStatus.PARTIAL
    assert terminal_calls == [expected]  # derived exactly once, with the right value
    ((succ, failed, status, fin),) = await _rows(
        sm,
        "SELECT succeeded_subtasks, failed_subtasks, status, finished_at IS NOT NULL"
        " FROM minion_task WHERE task_name=:n",
        n=name,
    )
    assert (succ, failed, status, fin) == (n_ok, n_bad, expected.value, True)
    assert await _rows(
        sm,
        "SELECT status, count(*) FROM minion_subtask WHERE task_name=:n GROUP BY status"
        " ORDER BY status",
        n=name,
    ) == sorted([(s, k) for s, k in [("SUCCEEDED", n_ok), ("FAILED", n_bad)] if k])


# --- 3. concurrent splitters -------------------------------------------------------------------


async def test_concurrent_splitters_no_duplicates_full_coverage(fresh_url: str, s3: S3Env) -> None:
    cfg = _cfg(fresh_url, s3)
    storage = S3Storage(cfg)
    engine = create_engine(cfg.database_url, pool_size=20, max_overflow=0)
    sm = create_sessionmaker(engine)
    try:
        a = b"".join(f'{{"id":"a{i}","t":"{"x" * (i % 7)}"}}\n'.encode() for i in range(257))
        b = b"".join(f'{{"id":"b{i}"}}\n'.encode() for i in range(40)).rstrip(b"\n")
        s3.client.put_object(Bucket=s3.bucket, Key="inputs/spc/big/a.jsonl", Body=a)
        s3.client.put_object(Bucket=s3.bucket, Key="inputs/spc/big/b.jsonl", Body=b)
        body = TaskCreate.model_validate(
            {
                "team": "spc",
                "project": "p",
                "task_name": "big",
                "type": "embedding",
                "model": "fake-embed",
                "input_uri": "s3://minion/inputs/spc/big/",
                "output_uri": "s3://minion/outputs/spc/big/",
                "records_per_subtask": 10,
            }
        )
        from minion_common.schemas import ModelEntry

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

        async def loop() -> int:
            done = 0
            while await split_one(sm, storage, batch_size=7):
                done += 1
            return done

        claims = await asyncio.gather(*(loop() for _ in range(4)))
        assert sum(claims) == 1  # SKIP LOCKED: exactly one splitter claimed the task

        async with sm() as s:
            rows = (
                await s.execute(
                    select(
                        Subtask.subtask_id,
                        Subtask.input_object_key,
                        Subtask.byte_start,
                        Subtask.byte_end,
                        Subtask.first_record,
                        Subtask.record_count,
                    )
                    .where(Subtask.task_name == "big")
                    .order_by(Subtask.subtask_id)
                )
            ).all()
            total = (
                await s.execute(
                    text("SELECT status, total_subtasks FROM minion_task WHERE task_name='big'")
                )
            ).one()
            distinct = (
                await s.execute(
                    select(func.count(func.distinct(Subtask.subtask_id))).where(
                        Subtask.task_name == "big"
                    )
                )
            ).scalar_one()
        expected = [
            (key, s_.byte_start, s_.byte_end, s_.record_count)
            for key, data in (("inputs/spc/big/a.jsonl", a), ("inputs/spc/big/b.jsonl", b))
            for s_ in slice_jsonl([data], 0, 10)
        ]
        assert [r[0] for r in rows] == list(range(len(expected))) and distinct == len(expected)
        assert [(r[1], r[2], r[3], r[5]) for r in rows] == expected
        assert tuple(total) == ("PENDING", len(expected))
        assert [r[4] for r in rows] == [sum(e[3] for e in expected[:i]) for i in range(len(rows))]
        # per object: slices tile [0, size) contiguously, each ends just after a newline or EOF
        for key, data in (("inputs/spc/big/a.jsonl", a), ("inputs/spc/big/b.jsonl", b)):
            mine = [r for r in rows if r[1] == key]
            assert mine[0][2] == 0 and mine[-1][3] == len(data)
            for prev, nxt in itertools.pairwise(mine):
                assert prev[3] == nxt[2]
            for r in mine:
                assert r[3] == len(data) or data[r[3] - 1 : r[3]] == b"\n"
    finally:
        await engine.dispose()


# --- 4. end-to-end over HTTP with the shared clients -------------------------------------------


async def test_e2e_user_and_executor_clients(
    app_env: tuple[AsyncClient, Any, ASGITransport], s3: S3Env
) -> None:
    _, app, transport = app_env
    sm = app.state.sessionmaker
    data = b"".join(f'{{"id":{i}}}\n'.encode() for i in range(25))
    s3.client.put_object(Bucket=s3.bucket, Key="inputs/e2e/run/a.jsonl", Body=data)
    ((p, tok),) = await make_executors(sm, 1)

    async with (
        UserClient("http://localhost", SecretStr(USER_KEY), transport=transport) as user,
        ExecutorClient("http://localhost", SecretStr(tok), transport=transport) as ex,
    ):
        created = await user.create_task(
            TaskCreate.model_validate(
                {
                    "team": "e2e",
                    "project": "p",
                    "task_name": "run",
                    "type": "embedding",
                    "model": "fake-embed",
                    "input_uri": "s3://minion/inputs/e2e/run",
                    "output_uri": "s3://minion/outputs/e2e/run",
                    "records_per_subtask": 10,
                }
            )
        )
        assert created.status == TaskStatus.SPLITTING
        assert await split_one(sm, app.state.storage)  # drive the splitter deterministically
        got = await user.get_task("e2e", "p", "run")
        assert (got.status, got.counts.total) == (TaskStatus.PENDING, 3)

        reg = await ex.register(
            RegisterRequest.model_validate(
                {
                    "hostname": "mbp",
                    "chip": "M3",
                    "total_memory_gb": 32,
                    "os_version": "15",
                    "engines": ["fake"],
                    "profile": "balanced",
                    "budget": _budget().model_dump(mode="json"),
                }
            )
        )
        assert reg.executor_id == p.executor_id

        seen = set()
        async with httpx.AsyncClient() as raw:  # real HTTP to SeaweedFS via presigned URLs
            while (asg := await ex.acquire(_req(engines=(Engine.FAKE,)))) is not None:
                seen.add(asg.subtask_id)
                got_in = await raw.get(
                    asg.input_url, headers={"Range": f"bytes={asg.byte_start}-{asg.byte_end - 1}"}
                )
                assert got_in.status_code in (200, 206)
                chunk = got_in.content
                assert chunk == data[asg.byte_start : asg.byte_end]
                assert chunk.count(b"\n") == asg.record_count
                put = await raw.put(asg.output_url, content=chunk)
                assert put.status_code == 200
                await ex.complete(
                    asg.acquirement_id,
                    CompleteRequest(
                        record_count=asg.record_count,
                        byte_size=len(chunk),
                        sha256=hashlib.sha256(chunk).hexdigest(),
                    ),
                )
                with pytest.raises(StaleAcquirementError):  # ownership is gone after complete
                    await ex.fail(
                        asg.acquirement_id,
                        FailRequest(retryable=True, code="LATE", message="late"),
                    )
        assert seen == {0, 1, 2}
        final = await user.get_task("e2e", "p", "run")
        assert final.status == TaskStatus.SUCCEEDED
        assert (final.counts.total, final.counts.succeeded, final.counts.failed) == (3, 3, 0)
        assert final.finished_at is not None

    promoted = b""
    for i in range(3):
        promoted += s3.client.get_object(Bucket=s3.bucket, Key=f"outputs/e2e/run/{i:06d}.jsonl")[
            "Body"
        ].read()
    assert promoted == data  # outputs tile the input exactly, in order
    uris = await _rows(sm, "SELECT output_uri FROM minion_subtask WHERE team='e2e' ORDER BY 1")
    assert uris == [(f"s3://minion/outputs/e2e/run/{i:06d}.jsonl",) for i in range(3)]
