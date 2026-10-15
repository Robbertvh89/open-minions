import asyncio
import hashlib
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from minion_common.schemas import TaskCreate
from minion_server.api.tasks import validate_uris
from minion_server.app import create_app
from minion_server.db.models import Subtask
from minion_server.db.session import create_sessionmaker
from minion_server.repositories import tasks as tasks_repo
from minion_server.services.splitter import split_one
from minion_server.settings import ServerSettings
from minion_server.splitting import slice_jsonl
from minion_server.storage import S3Storage
from pydantic import SecretStr
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine

from conftest import S3Env

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
        models_file=str(Path(__file__).resolve().parents[3] / "deploy" / "models.yaml"),
        splitter_poll_s=0.05,
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
    c, _ = app_client
    _put(s3, "inputs/api1/e2e/a.jsonl", _jsonl(25, "a"))
    _put(s3, "inputs/api1/e2e/b.jsonl", _jsonl(10, "b"))
    r = await c.post("/api/v1/tasks", json=_payload("api1", "e2e"))
    assert r.status_code == 202, r.text
    assert r.json()["status"] == "SPLITTING"

    for _ in range(100):
        got = (await c.get("/api/v1/tasks/api1/p/e2e")).json()
        if got["status"] != "SPLITTING":
            break
        await asyncio.sleep(0.05)
    assert got["status"] == "PENDING"
    assert got["counts"]["total"] == 4 == got["counts"]["pending"]  # 10+10+5, then 10

    dup = await c.post("/api/v1/tasks", json=_payload("api1", "e2e"))
    assert dup.status_code == 409
    assert dup.json()["detail"]["code"] == "TASK_EXISTS"


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
        model_id="fake-embed", type="embedding", engine="mlx", ref="f", memory_gb=0.1
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
    # claim picks the oldest SPLITTING task; "resume" was inserted first
    with pytest.raises(RuntimeError):
        await split_one(sm, storage, batch_size=3)
    partial = await _subtasks(engine, "resume")
    assert len(partial) == 3  # first batch committed, task still SPLITTING

    monkeypatch.setattr(tasks_repo, "insert_subtasks", real)
    while await split_one(sm, storage, batch_size=3):
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
