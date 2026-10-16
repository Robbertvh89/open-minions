import asyncio
import hashlib
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi import FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient
from minion_common.enums import Engine, TaskType
from minion_common.schemas import ModelEntry, TaskCreate
from minion_server.api.tasks import create_task, validate_uris
from minion_server.app import create_app
from minion_server.auth import User
from minion_server.db.models import Subtask
from minion_server.db.session import create_sessionmaker
from minion_server.repositories import tasks as tasks_repo
from minion_server.services.splitter import SplitLimits, split_one
from minion_server.settings import ServerSettings
from minion_server.splitting import slice_jsonl
from minion_server.storage import S3Storage
from pydantic import SecretStr
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine

from conftest import TEST_MODELS_FILE, S3Env

KEY = "k-tasks-secret"


def _settings(url: str, s3: S3Env | None = None, keys: Path | None = None) -> ServerSettings:
    return ServerSettings(
        database_url=SecretStr(url),
        s3_endpoint_url=s3.endpoint_url if s3 else "http://localhost:9000",
        s3_access_key=SecretStr(s3.access_key if s3 else "ak"),
        s3_secret_key=SecretStr(s3.secret_key if s3 else "sk"),
        s3_bucket=s3.bucket if s3 else "minion",
        s3_region=s3.region if s3 else "us-east-1",
        api_keys_file=str(keys) if keys else None,
        models_file=TEST_MODELS_FILE,
        splitter_poll_s=60,  # tests drive split_one directly
    )


def _body(**kw: Any) -> TaskCreate:
    base: dict[str, Any] = {
        "team": "t1",
        "project": "p",
        "task_name": "n",
        "type": "embedding",
        "model": "fake-embed",
        "input_uri": "s3://minion/inputs/t1/data",
        "output_uri": "s3://minion/outputs/t1/run1",
    }
    return TaskCreate.model_validate(base | kw)


# --- unit: engine-specific params -------------------------------------------------------------


async def _create_with(model: ModelEntry, params: dict[str, Any]) -> None:
    body = _body(model=model.model_id, params=params)
    await create_task(
        body,
        User(name="u", teams=("t1",)),
        session=cast(Any, None),  # the checks under test fail before any I/O
        settings=cast(Any, None),
        storage=cast(Any, None),
        models={model.model_id: model},
    )


def _embed_model(engine: Engine, ref: str, revision: str) -> ModelEntry:
    return ModelEntry(
        model_id="m1", type=TaskType.EMBEDDING, engine=engine, ref=ref,
        revision=revision, memory_gb=1,
    )  # fmt: skip


async def test_ollama_model_rejects_unnormalized_embeddings() -> None:
    model = _embed_model(Engine.OLLAMA, "nomic-embed-text:v1.5", "a" * 64)
    with pytest.raises(HTTPException) as ei:
        await _create_with(model, {"normalize": False})
    assert ei.value.status_code == 422
    assert ei.value.detail["code"] == "INVALID_PARAMS"  # type: ignore[index]


async def test_other_engines_do_not_get_the_ollama_normalize_rule() -> None:
    model = _embed_model(Engine.MLX, "o/m", "a" * 40)
    with pytest.raises(AttributeError):  # got past the params check: next step needs real storage
        await _create_with(model, {"normalize": False})


# --- unit: URI rules -------------------------------------------------------------------------


def test_validate_uris_normalizes_to_prefixes() -> None:
    cfg = _settings("postgresql+asyncpg://x@h/db")
    in_uri, out_uri, key = validate_uris(_body(), cfg)
    assert in_uri.endswith("/data/") and out_uri.endswith("/run1/")
    assert key == "inputs/t1/data/"


@pytest.mark.parametrize(
    ("field", "uri"),
    [
        ("input_uri", "s3://minion/inputs/other-team/data"),  # other team's prefix
        ("input_uri", "s3://minion/inputs/t1x/data"),  # sibling prefix
        ("input_uri", "s3://minion/outputs/t1/data"),  # wrong prefix list
        ("output_uri", "s3://minion/inputs/t1/run"),  # wrong prefix list
        ("output_uri", "s3://minion/outputs/other/run"),
        ("output_uri", "s3://minion/outputs/t1/_attempts/x"),
        ("output_uri", "s3://minion/outputs/t1/run/_attempts"),
        ("output_uri", "s3://other/outputs/t1/run"),
        ("input_uri", "s3://minion/inputs/t1/../t2/x"),
    ],
)
def test_validate_uris_rejects(field: str, uri: str) -> None:
    from fastapi import HTTPException

    cfg = _settings("postgresql+asyncpg://x@h/db")
    with pytest.raises(HTTPException) as exc:
        validate_uris(_body(**{field: uri}), cfg)
    assert exc.value.status_code == 422


# --- integration -----------------------------------------------------------------------------


def _put(s3: S3Env, key: str, data: bytes) -> None:
    s3.client.put_object(Bucket=s3.bucket, Key=key, Body=data)


def _jsonl(n: int, tag: str) -> bytes:
    return b"".join(f'{{"id":"{tag}{i}","text":"hello {i}"}}\n'.encode() for i in range(n))


@pytest.fixture
async def app_client(
    migrated_url: str, s3: S3Env, tmp_path: Path
) -> AsyncIterator[tuple[AsyncClient, FastAPI]]:
    keys = tmp_path / "keys.yaml"
    digest = hashlib.sha256(KEY.encode()).hexdigest()
    keys.write_text(f"keys:\n  - name: alice\n    sha256: {digest}\n    teams: [api1, api2]\n")
    app = create_app(_settings(migrated_url, s3, keys))
    async with app.router.lifespan_context(app):
        # Tests drive split_one themselves; a live background splitter would race for the lease.
        app.state.splitter_task.cancel()
        await asyncio.gather(app.state.splitter_task, return_exceptions=True)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://t") as c:
            c.headers["Authorization"] = f"Bearer {KEY}"
            yield c, app


def _payload(team: str, name: str, **kw: Any) -> dict[str, Any]:
    return {
        "team": team,
        "project": "p",
        "task_name": name,
        "type": "embedding",
        "model": "fake-embed",
        "input_uri": f"s3://minion/inputs/{team}/{name}",
        "output_uri": f"s3://minion/outputs/{team}/{name}",
        "records_per_subtask": 10,
    } | kw


@pytest.mark.integration
async def test_create_split_and_get(app_client: tuple[AsyncClient, FastAPI], s3: S3Env) -> None:
    c, app = app_client
    _put(s3, "inputs/api1/e2e/a.jsonl", _jsonl(25, "a"))
    _put(s3, "inputs/api1/e2e/b.jsonl", _jsonl(10, "b"))
    r = await c.post("/api/v1/tasks", json=_payload("api1", "e2e"))
    assert r.status_code == 202, r.text
    assert r.json()["status"] == "SPLITTING"

    while await split_one(app.state.sessionmaker, app.state.storage):
        pass
    got = (await c.get("/api/v1/tasks/api1/p/e2e")).json()
    assert got["status"] == "PENDING"
    assert got["counts"]["total"] == 4 == got["counts"]["pending"]  # 10+10+5, then 10

    dup = await c.post("/api/v1/tasks", json=_payload("api1", "e2e"))
    assert dup.status_code == 409
    assert dup.json()["detail"]["code"] == "TASK_EXISTS"


@pytest.mark.integration
async def test_create_params_allow_list(app_client: tuple[AsyncClient, FastAPI], s3: S3Env) -> None:
    c, _ = app_client
    _put(s3, "inputs/api1/prm/a.jsonl", _jsonl(1, "x"))
    for bad in ({"trust_remote_code": True}, {"normalize": "yes"}, {"temperature": 1}):
        r = await c.post("/api/v1/tasks", json=_payload("api1", "prm", params=bad))
        assert r.status_code == 422, bad
        assert r.json()["detail"]["code"] == "INVALID_PARAMS"
    # values are never echoed back
    r = await c.post(
        "/api/v1/tasks", json=_payload("api1", "prm", params={"secret-key": "hunter2"})
    )
    assert r.status_code == 422 and "hunter2" not in r.text
    ok = await c.post("/api/v1/tasks", json=_payload("api1", "prm", params={"normalize": False}))
    assert ok.status_code == 202, ok.text
    assert ok.json()["params"] == {"normalize": False}


@pytest.mark.integration
async def test_create_rejections(app_client: tuple[AsyncClient, FastAPI], s3: S3Env) -> None:
    c, _ = app_client
    _put(s3, "inputs/api1/rej/a.jsonl", _jsonl(1, "x"))
    assert (await c.post("/api/v1/tasks", json=_payload("zzz", "rej"))).status_code == 403
    r = await c.post("/api/v1/tasks", json=_payload("api1", "rej", model="nope"))
    assert r.status_code == 422
    r = await c.post("/api/v1/tasks", json=_payload("api1", "rej", type="completion"))
    assert r.status_code == 422  # fake-embed is an embedding model
    r = await c.post(
        "/api/v1/tasks",
        json=_payload("api1", "rej", output_uri="s3://minion/outputs/api1/_attempts/x"),
    )
    assert r.status_code == 422
    r = await c.post(
        "/api/v1/tasks", json=_payload("api1", "rej", input_uri="s3://minion/inputs/api2/rej")
    )
    assert r.status_code == 422  # team-scoped: api1 may not read api2's prefix
    r = await c.post(
        "/api/v1/tasks", json=_payload("api1", "rej", input_uri="s3://minion/inputs/api1/empty")
    )
    assert r.status_code == 422
    assert r.json()["detail"]["code"] == "EMPTY_INPUT"
    assert (await c.get("/api/v1/tasks/api1/p/missing")).status_code == 404
    assert (await c.get("/api/v1/tasks/other/p/rej")).status_code == 403
    assert (
        await c.post(
            "/api/v1/tasks", json=_payload("api1", "x"), headers={"Authorization": "Bearer bad"}
        )
    ).status_code == 401


@pytest.fixture
def storage(migrated_url: str, s3: S3Env) -> S3Storage:
    return S3Storage(_settings(migrated_url, s3))


async def _insert_task(engine: AsyncEngine, team: str, name: str, n: int = 10) -> None:
    sm = create_sessionmaker(engine)
    body = _body(
        team=team,
        task_name=name,
        input_uri=f"s3://minion/inputs/{team}/{name}/",
        output_uri=f"s3://minion/outputs/{team}/{name}/",
        records_per_subtask=n,
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


async def _subtasks(engine: AsyncEngine, name: str) -> list[tuple[Any, ...]]:
    sm = create_sessionmaker(engine)
    async with sm() as s:
        rows = await s.execute(
            select(
                Subtask.subtask_id,
                Subtask.input_object_key,
                Subtask.byte_start,
                Subtask.byte_end,
                Subtask.first_record,
                Subtask.record_count,
            )
            .where(Subtask.task_name == name)
            .order_by(Subtask.subtask_id)
        )
        return [tuple(r) for r in rows.all()]


@pytest.mark.integration
async def test_split_slices_match_pure_slicer(
    engine: AsyncEngine, storage: S3Storage, s3: S3Env
) -> None:
    data = _jsonl(23, "a") + b"\n\n"
    _put(s3, "inputs/sp1/exact/a.jsonl", data)
    _put(s3, "inputs/sp1/exact/b.jsonl", _jsonl(4, "b").rstrip(b"\n"))  # no trailing newline
    await _insert_task(engine, "sp1", "exact", 10)
    sm = create_sessionmaker(engine)
    while await split_one(sm, storage):
        pass
    rows = await _subtasks(engine, "exact")
    assert [r[0] for r in rows] == [0, 1, 2, 3]
    assert [r[5] for r in rows] == [10, 10, 3, 4]
    assert [r[4] for r in rows] == [0, 10, 20, 23]
    assert rows[3][1].endswith("b.jsonl")
    expected = list(slice_jsonl([data], 0, 10))
    assert [(r[2], r[3]) for r in rows[:3]] == [(s.byte_start, s.byte_end) for s in expected]
    # every slice parses as whole JSONL records
    for _, key, start, end, _, count in rows:
        body = s3.client.get_object(Bucket=s3.bucket, Key=key, Range=f"bytes={start}-{end - 1}")[
            "Body"
        ].read()
        assert len([ln for ln in body.split(b"\n") if ln.strip()]) == count
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text("SELECT status, total_subtasks FROM minion_task WHERE task_name='exact'")
            )
        ).one()
    assert tuple(row) == ("PENDING", 4)


@pytest.mark.integration
async def test_split_resumes_after_crash(
    engine: AsyncEngine, storage: S3Storage, s3: S3Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    _put(s3, "inputs/sp1/resume/a.jsonl", _jsonl(50, "r"))
    _put(s3, "inputs/sp1/clean/a.jsonl", _jsonl(50, "r"))
    await _insert_task(engine, "sp1", "resume", 5)
    await _insert_task(engine, "sp1", "clean", 5)
    sm = create_sessionmaker(engine)

    real = tasks_repo.insert_subtasks
    calls = 0

    async def flaky(session: Any, task: Any, rows: Any) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("crash mid-split")
        await real(session, task, rows)

    monkeypatch.setattr(tasks_repo, "insert_subtasks", flaky)
    lim = SplitLimits(backoff_base_s=0)  # retry immediately
    # claim picks the oldest SPLITTING task; "resume" was inserted first
    assert await split_one(sm, storage, lim, batch_size=3)  # crash is absorbed and retried
    partial = await _subtasks(engine, "resume")
    assert len(partial) == 3  # first batch committed, task still SPLITTING

    monkeypatch.setattr(tasks_repo, "insert_subtasks", real)
    while await split_one(sm, storage, lim, batch_size=3):
        pass
    resumed = await _subtasks(engine, "resume")
    clean = await _subtasks(engine, "clean")
    assert len(resumed) == 10

    def norm(rows: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
        return [(r[0], r[2], r[3], r[4], r[5]) for r in rows]

    assert norm(resumed) == norm(clean)
    assert resumed[:3] == partial


@pytest.mark.integration
async def test_empty_input_fails_task(engine: AsyncEngine, storage: S3Storage, s3: S3Env) -> None:
    _put(s3, "inputs/sp1/blank/a.jsonl", b"\n\n  \n")
    await _insert_task(engine, "sp1", "blank")
    sm = create_sessionmaker(engine)
    while await split_one(sm, storage):
        pass
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text("SELECT status, error, finished_at FROM minion_task WHERE task_name='blank'")
            )
        ).one()
    assert row[0] == "FAILED"
    assert "no records" in row[1]
    assert row[2] is not None


# --- splitter hardening ----------------------------------------------------------------------


async def _task_row(engine: AsyncEngine, name: str) -> tuple[Any, ...]:
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT status, error, split_attempts, total_subtasks FROM minion_task"
                    " WHERE task_name=:n"
                ),
                {"n": name},
            )
        ).one()
    return tuple(row)


@pytest.mark.integration
async def test_line_too_long_fails_task(engine: AsyncEngine, storage: S3Storage, s3: S3Env) -> None:
    _put(s3, "inputs/hd1/long/a.jsonl", b"x" * 5000)  # no newline at all, > cap
    await _insert_task(engine, "hd1", "long")
    sm = create_sessionmaker(engine)
    lim = SplitLimits(max_line_bytes=1024)
    while await split_one(sm, storage, lim):
        pass
    status, error, attempts, _ = await _task_row(engine, "long")
    assert status == "FAILED" and error.startswith("LINE_TOO_LONG") and attempts == 1


@pytest.mark.integration
@pytest.mark.parametrize(
    ("limits", "needle"),
    [
        (SplitLimits(max_objects=1), "objects"),
        (SplitLimits(max_input_bytes=100), "larger than"),
        (SplitLimits(max_subtasks=2), "subtasks"),
    ],
)
async def test_input_limits_fail_task(
    engine: AsyncEngine, storage: S3Storage, s3: S3Env, limits: SplitLimits, needle: str
) -> None:
    name = f"lim-{needle.split()[0]}"
    _put(s3, f"inputs/hd2/{name}/a.jsonl", _jsonl(10, "a"))
    _put(s3, f"inputs/hd2/{name}/b.jsonl", _jsonl(10, "b"))
    await _insert_task(engine, "hd2", name, 2)
    sm = create_sessionmaker(engine)
    while await split_one(sm, storage, limits):
        pass
    status, error, _, _ = await _task_row(engine, name)
    assert status == "FAILED" and needle in error


@pytest.mark.integration
async def test_bad_task_does_not_block_queue(
    engine: AsyncEngine, storage: S3Storage, s3: S3Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Task A's input is unreadable (transient error); the later task B is still split."""
    _put(s3, "inputs/hd3/bad/a.jsonl", _jsonl(5, "a"))
    _put(s3, "inputs/hd3/good/a.jsonl", _jsonl(5, "g"))
    await _insert_task(engine, "hd3", "bad", 2)
    await _insert_task(engine, "hd3", "good", 2)
    sm = create_sessionmaker(engine)

    real = S3Storage.iter_object

    def broken(self: S3Storage, key: str, chunk_size: int = 1 << 20, **kw: Any) -> Any:
        if "/bad/" in key:
            raise ConnectionError("simulated outage")
        return real(self, key, chunk_size, **kw)

    monkeypatch.setattr(S3Storage, "iter_object", broken)
    lim = SplitLimits(backoff_base_s=1000, max_attempts=3)
    assert await split_one(sm, storage, lim)  # A: fails, backs off
    assert (await _task_row(engine, "bad"))[:3] == ("SPLITTING", None, 1)
    assert await split_one(sm, storage, lim)  # B is next although A is older
    assert (await _task_row(engine, "good"))[0] == "PENDING"
    assert not await split_one(sm, storage, lim)  # A is backing off: nothing due

    lim0 = SplitLimits(backoff_base_s=0, max_attempts=3)
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE minion_task SET next_split_at=now() WHERE task_name='bad'"))
    while await split_one(sm, storage, lim0):
        pass
    status, error, attempts, _ = await _task_row(engine, "bad")
    assert (status, attempts) == ("FAILED", 3) and "3 attempts" in error
    assert "outage" not in error  # error text never echoes exception details


@pytest.mark.integration
async def test_deterministic_s3_error_fails_immediately(
    engine: AsyncEngine, storage: S3Storage, s3: S3Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    from botocore.exceptions import ClientError

    _put(s3, "inputs/hd4/denied/a.jsonl", _jsonl(2, "a"))
    await _insert_task(engine, "hd4", "denied")

    def denied(self: S3Storage, key: str, chunk_size: int = 1 << 20, **kw: Any) -> Any:
        raise ClientError({"Error": {"Code": "AccessDenied", "Message": "x"}}, "GetObject")

    monkeypatch.setattr(S3Storage, "iter_object", denied)
    sm = create_sessionmaker(engine)
    while await split_one(sm, storage):
        pass
    status, error, attempts, _ = await _task_row(engine, "denied")
    assert (status, attempts) == ("FAILED", 1) and "AccessDenied" in error


@pytest.mark.integration
async def test_resume_detects_changed_input(
    engine: AsyncEngine, storage: S3Storage, s3: S3Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    _put(s3, "inputs/hd5/chg/a.jsonl", _jsonl(20, "r"))
    _put(s3, "inputs/hd5/shrunk/a.jsonl", _jsonl(20, "r"))
    await _insert_task(engine, "hd5", "chg", 2)
    await _insert_task(engine, "hd5", "shrunk", 2)
    sm = create_sessionmaker(engine)
    real = tasks_repo.insert_subtasks
    state = {"fail": True}

    async def flaky(session: Any, task: Any, rows: Any) -> None:
        if state["fail"] and rows[0].subtask_id > 0:
            raise RuntimeError("crash mid-split")
        await real(session, task, rows)

    monkeypatch.setattr(tasks_repo, "insert_subtasks", flaky)
    lim = SplitLimits(backoff_base_s=0)
    assert await split_one(sm, storage, lim, batch_size=3)  # "chg": first batch only
    assert await split_one(sm, storage, lim, batch_size=3)  # "shrunk": first batch only
    state["fail"] = False
    _put(s3, "inputs/hd5/chg/a.jsonl", _jsonl(2, "DIFFERENT-PREFIX-LONGER") + _jsonl(18, "r"))
    _put(s3, "inputs/hd5/shrunk/a.jsonl", _jsonl(4, "r"))  # same prefix bytes, fewer records
    while await split_one(sm, storage, lim, batch_size=3):
        pass
    assert (await _task_row(engine, "chg"))[:2] == (
        "FAILED",
        "INPUT_CHANGED: input differs from a previous split attempt",
    )
    status, error, _, _ = await _task_row(engine, "shrunk")
    assert status == "FAILED" and error.startswith("INPUT_CHANGED")


@pytest.mark.integration
async def test_output_uri_collision_and_nesting(
    app_client: tuple[AsyncClient, FastAPI], s3: S3Env
) -> None:
    c, _ = app_client
    _put(s3, "inputs/api1/oc/a.jsonl", _jsonl(1, "x"))
    first = _payload(
        "api1",
        "oc1",
        input_uri="s3://minion/inputs/api1/oc",
        output_uri="s3://minion/outputs/api1/shared",
    )
    assert (await c.post("/api/v1/tasks", json=first)).status_code == 202
    other = first | {"task_name": "oc2", "output_uri": "s3://minion/outputs/api1/shared/"}
    r = await c.post("/api/v1/tasks", json=other)  # same normalized prefix
    assert r.status_code == 409 and r.json()["detail"]["code"] == "OUTPUT_URI_IN_USE"
    same_name = await c.post("/api/v1/tasks", json=first)
    assert same_name.json()["detail"]["code"] == "TASK_EXISTS"
    # nested prefixes cannot collide: keys are {prefix}NNNNNN.jsonl and {prefix}_attempts/...
    nested = first | {"task_name": "oc3", "output_uri": "s3://minion/outputs/api1/shared/inner"}
    assert (await c.post("/api/v1/tasks", json=nested)).status_code == 202
    bad = first | {"task_name": "oc4", "output_uri": "s3://minion/outputs/api1/shared/_attempts"}
    assert (await c.post("/api/v1/tasks", json=bad)).status_code == 422


def test_nested_output_prefixes_have_disjoint_keys() -> None:
    from minion_server.services.acquisition import attempt_key

    outer, inner = "outputs/t/a/", "outputs/t/a/b/"
    keys_outer = {f"{outer}{i:06d}.jsonl" for i in range(50)} | {
        attempt_key(outer, i, 1) for i in range(50)
    }
    keys_inner = {f"{inner}{i:06d}.jsonl" for i in range(50)} | {
        attempt_key(inner, i, 1) for i in range(50)
    }
    assert keys_outer.isdisjoint(keys_inner)  # holds because `_attempts` segments are rejected


@pytest.mark.integration
async def test_oversize_body_rejected_before_auth(app_client: tuple[AsyncClient, FastAPI]) -> None:
    c, _ = app_client
    big = b"{" + b" " * (2 << 20) + b"}"
    unauth = {"Authorization": "Bearer nope"}
    r = await c.post("/api/v1/tasks", content=big, headers=unauth)
    assert r.status_code == 413 and r.json()["detail"]["code"] == "PAYLOAD_TOO_LARGE"

    async def chunks() -> Any:  # chunked: no Content-Length
        for _ in range(4):
            yield b" " * (512 << 10)

    r = await c.post("/api/v1/tasks", content=chunks(), headers=unauth)
    assert r.status_code == 413
    ok = await c.post("/api/v1/tasks", json={}, headers=unauth)
    assert ok.status_code == 401  # small bodies still reach auth


@pytest.fixture
async def fresh_engine(fresh_url: str) -> AsyncIterator[AsyncEngine]:
    """Empty database: claims cannot pick up other tests' SPLITTING tasks."""
    from sqlalchemy.ext.asyncio import create_async_engine

    eng = create_async_engine(fresh_url)
    yield eng
    await eng.dispose()


EXPIRE = text("UPDATE minion_task SET next_split_at=now() WHERE task_name=:n")
BUMP = text("UPDATE minion_task SET split_attempts=split_attempts+1 WHERE task_name='lost'")


# --- TASK-2.2 final fixes: canonical URIs, lease fencing, input guards ---------------------------


@pytest.mark.integration
async def test_uppercase_scheme_rejected_and_uris_canonical(
    app_client: tuple[AsyncClient, FastAPI], s3: S3Env
) -> None:
    c, _ = app_client
    _put(s3, "inputs/api1/canon/a.jsonl", _jsonl(1, "x"))
    first = _payload(
        "api1",
        "cn1",
        input_uri="s3://minion/inputs/api1/canon",
        output_uri="s3://minion/outputs/api1/canon",
    )
    r = await c.post("/api/v1/tasks", json=first)
    assert r.status_code == 202
    assert r.json()["output_uri"] == "s3://minion/outputs/api1/canon/"
    assert r.json()["input_uri"] == "s3://minion/inputs/api1/canon/"
    dup = first | {"task_name": "cn2", "output_uri": "S3://minion/outputs/api1/canon/"}
    r = await c.post("/api/v1/tasks", json=dup)
    assert r.status_code == 422 and r.json()["detail"]["code"] == "INVALID_URI"
    r = await c.post(
        "/api/v1/tasks",
        json=first | {"task_name": "cn3", "input_uri": "S3://minion/inputs/api1/canon"},
    )
    assert r.status_code == 422


@pytest.mark.integration
async def test_stale_split_holder_is_fenced(fresh_engine: AsyncEngine) -> None:
    await _insert_task(fresh_engine, "fz1", "fence")
    sm = create_sessionmaker(fresh_engine)
    async with sm() as s, s.begin():
        stale = await tasks_repo.claim_splitting(s, 900, 5)
    assert stale is not None and stale.split_attempts == 1
    async with fresh_engine.begin() as conn:  # lease "expires"
        await conn.execute(EXPIRE, {"n": "fence"})
    async with sm() as s, s.begin():
        fresh = await tasks_repo.claim_splitting(s, 900, 5)
    assert fresh is not None and fresh.split_attempts == 2
    async with sm() as s, s.begin():
        assert not await tasks_repo.renew_split_lease(s, stale, 900)
        assert not await tasks_repo.mark_split_done(s, stale, 3)
        assert not await tasks_repo.mark_split_failed(s, stale, "x")
        assert not await tasks_repo.schedule_split_retry(s, stale, 0)
    assert (await _task_row(fresh_engine, "fence"))[:3] == ("SPLITTING", None, 2)
    async with sm() as s, s.begin():
        assert await tasks_repo.renew_split_lease(s, fresh, 900)
        assert await tasks_repo.mark_split_done(s, fresh, 3)
    assert (await _task_row(fresh_engine, "fence"))[0] == "PENDING"


@pytest.mark.integration
async def test_renewal_keeps_lease_across_slow_split(
    fresh_engine: AsyncEngine, storage: S3Storage, s3: S3Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    _put(s3, "inputs/fz2/slow/a.jsonl", _jsonl(8, "s"))
    await _insert_task(fresh_engine, "fz2", "slow", 1)  # 8 subtasks, batch_size=1 -> 8 batches
    sm = create_sessionmaker(fresh_engine)
    real = tasks_repo.insert_subtasks

    async def slow(session: Any, task: Any, rows: Any) -> None:
        await asyncio.sleep(0.4)
        await real(session, task, rows)

    monkeypatch.setattr(tasks_repo, "insert_subtasks", slow)
    lim = SplitLimits(lease_s=1.5)  # total split takes ~3.2s, well past the lease
    stolen = 0
    stop = asyncio.Event()

    async def rival() -> None:
        nonlocal stolen
        while not stop.is_set():
            async with sm() as s, s.begin():
                if await tasks_repo.claim_splitting(s, 900, 5) is not None:
                    stolen += 1
            await asyncio.sleep(0.1)

    splitting = asyncio.create_task(split_one(sm, storage, lim, batch_size=1))
    await asyncio.sleep(0.3)  # the splitter holds the claim; now a rival starts polling
    rt = asyncio.create_task(rival())
    try:
        assert await splitting
    finally:
        stop.set()
        await rt
    assert stolen == 0
    assert (await _task_row(fresh_engine, "slow"))[0] == "PENDING"


@pytest.mark.integration
async def test_lost_lease_aborts_quietly(
    fresh_engine: AsyncEngine, storage: S3Storage, s3: S3Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    _put(s3, "inputs/fz3/lost/a.jsonl", _jsonl(6, "s"))
    await _insert_task(fresh_engine, "fz3", "lost", 1)
    sm = create_sessionmaker(fresh_engine)
    real = tasks_repo.renew_split_lease
    n = 0

    async def steal(session: Any, task: Any, lease_s: float) -> bool:
        nonlocal n
        n += 1
        if n == 3:  # someone else claims between batches (before our third renewal)
            async with sm() as other, other.begin():
                await other.execute(BUMP)
        return await real(session, task, lease_s)

    monkeypatch.setattr(tasks_repo, "renew_split_lease", steal)
    assert await split_one(sm, storage, SplitLimits(), batch_size=1)
    status, error, attempts, _ = await _task_row(fresh_engine, "lost")
    assert (status, error, attempts) == ("SPLITTING", None, 2)  # untouched by the old holder
    assert n == 3 and len(await _subtasks(fresh_engine, "lost")) == 2  # stopped at renewal 3


@pytest.mark.integration
async def test_split_wall_clock_budget_backs_off(
    fresh_engine: AsyncEngine, storage: S3Storage, s3: S3Env
) -> None:
    _put(s3, "inputs/fz4/budget/a.jsonl", _jsonl(6, "s"))
    await _insert_task(fresh_engine, "fz4", "budget", 1)
    sm = create_sessionmaker(fresh_engine)
    assert await split_one(
        sm, storage, SplitLimits(max_seconds=-1, backoff_base_s=1000), batch_size=1
    )
    status, error, attempts, _ = await _task_row(fresh_engine, "budget")
    assert (status, error, attempts) == ("SPLITTING", None, 1)
    assert not await split_one(sm, storage)  # backing off


@pytest.mark.integration
async def test_claim_fails_exhausted_tasks_crash_loop(fresh_engine: AsyncEngine) -> None:
    await _insert_task(fresh_engine, "fz5", "loop")
    sm = create_sessionmaker(fresh_engine)
    for expected in (1, 2):  # claims whose holder crashed before any retry/fail bookkeeping
        async with sm() as s, s.begin():
            t = await tasks_repo.claim_splitting(s, 900, 2)
        assert t is not None and t.split_attempts == expected
        async with fresh_engine.begin() as conn:
            await conn.execute(EXPIRE, {"n": "loop"})
    async with sm() as s, s.begin():
        assert await tasks_repo.claim_splitting(s, 900, 2) is None
    status, error, attempts, _ = await _task_row(fresh_engine, "loop")
    assert (status, error, attempts) == ("FAILED", "SPLIT_ATTEMPTS_EXHAUSTED", 2)


@pytest.mark.integration
async def test_object_replaced_after_listing_is_input_changed(
    fresh_engine: AsyncEngine, storage: S3Storage, s3: S3Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    key = "inputs/fz6/swap/a.jsonl"
    _put(s3, key, _jsonl(10, "o"))
    await _insert_task(fresh_engine, "fz6", "swap", 2)
    sm = create_sessionmaker(fresh_engine)
    real = S3Storage.list_objects

    async def list_then_swap(self: S3Storage, *a: Any, **kw: Any) -> Any:
        out = await real(self, *a, **kw)
        _put(s3, key, _jsonl(10, "o") + _jsonl(50, "grown"))  # replaced after listing
        return out

    monkeypatch.setattr(S3Storage, "list_objects", list_then_swap)
    assert await split_one(sm, storage, batch_size=2)
    status, error, attempts, _ = await _task_row(fresh_engine, "swap")
    assert status == "FAILED" and error.startswith("INPUT_CHANGED") and attempts == 1


@pytest.mark.integration
async def test_iter_object_guards(storage: S3Storage, s3: S3Env) -> None:
    from minion_server.storage import PreconditionFailedError

    data = _jsonl(5, "g")
    _put(s3, "inputs/fz7/g.jsonl", data)
    ((_, size, etag),) = await storage.list_objects("inputs/fz7/")
    assert b"".join(storage.iter_object("inputs/fz7/g.jsonl", etag=etag, size=size)) == data
    with pytest.raises(PreconditionFailedError):  # grew beyond the listed size
        b"".join(storage.iter_object("inputs/fz7/g.jsonl", chunk_size=8, size=size - 1))
    with pytest.raises(PreconditionFailedError):  # shorter than listed
        b"".join(storage.iter_object("inputs/fz7/g.jsonl", size=size + 1))
    with pytest.raises(PreconditionFailedError):  # If-Match with a stale ETag
        b"".join(storage.iter_object("inputs/fz7/g.jsonl", etag='"deadbeef"'))


@pytest.mark.integration
async def test_insert_readback_detects_mismatch(
    fresh_engine: AsyncEngine, storage: S3Storage, s3: S3Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    _put(s3, "inputs/fz8/rb/a.jsonl", _jsonl(6, "r"))
    await _insert_task(fresh_engine, "fz8", "rb", 2)
    sm = create_sessionmaker(fresh_engine)

    real = tasks_repo.insert_subtasks

    async def partial(session: Any, task: Any, rows: Any) -> None:
        await real(session, task, rows[:-1])

    monkeypatch.setattr(tasks_repo, "insert_subtasks", partial)
    assert await split_one(sm, storage, batch_size=2)
    status, error, _, _ = await _task_row(fresh_engine, "rb")
    assert status == "FAILED" and error.startswith("INPUT_CHANGED")
