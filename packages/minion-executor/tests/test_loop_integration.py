"""Executor loop against the real ASGI app + SeaweedFS (one subtask, fake runner in a worker)."""

import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport
from minion_common.client import ExecutorClient
from minion_common.enums import Engine
from minion_common.schemas import Budget
from minion_executor.loop import ExecutorLoop
from minion_executor.runners.fake import fake_vector
from minion_executor.worker.process import WorkerHandle
from minion_server.app import create_app
from pydantic import SecretStr
from sqlalchemy import text

from conftest import S3Env, _settings, make_executors, seed_task

INPUT = b"".join(f'{{"id":"r{i}","text":"hello {i}"}}\n'.encode() for i in range(4))


def _leftover(p: Path) -> list[Path]:
    return list(p.iterdir())


@pytest.fixture
async def app(migrated_url: str, s3: S3Env) -> AsyncIterator[Any]:
    cfg = _settings(migrated_url).model_copy(
        update={
            "s3_endpoint_url": s3.endpoint_url,
            "public_s3_endpoint_url": s3.endpoint_url,
            "s3_access_key": SecretStr(s3.access_key),
            "s3_secret_key": SecretStr(s3.secret_key),
            "s3_bucket": s3.bucket,
        }
    )
    application = create_app(cfg)
    async with application.router.lifespan_context(application):
        yield application


@pytest.mark.integration
async def test_loop_runs_one_subtask_end_to_end(app: Any, s3: S3Env, tmp_path: Path) -> None:
    sm = app.state.sessionmaker
    await seed_task(sm, "loop1", 1, team="exl1", engine=Engine.FAKE)
    key = "inputs/exl1/loop1/a.jsonl"
    s3.client.put_object(Bucket=s3.bucket, Key=key, Body=INPUT)
    async with sm() as s, s.begin():
        await s.execute(
            text(
                "UPDATE minion_subtask SET byte_start=0, byte_end=:e, record_count=4"
                " WHERE task_name='loop1'"
            ),
            {"e": len(INPUT)},
        )
    [(_p, token)] = await make_executors(sm, 1)
    budget = Budget.model_validate(
        {"profile": "balanced", "memory_gb": 8, "gpu_duty_cycle": 0.5, "max_batch_size": 3}
    )
    worker = WorkerHandle([Engine.FAKE])
    async with ExecutorClient(
        "http://localhost", SecretStr(token), transport=ASGITransport(app=app)
    ) as client:
        loop = ExecutorLoop(
            client, worker, budget, [Engine.FAKE], tmp_root=tmp_path, allow_http_loopback=True
        )
        assert await loop.run(max_subtasks=1) == 1

    out = s3.client.get_object(Bucket=s3.bucket, Key="outputs/exl1/loop1/000000.jsonl")
    rows = [json.loads(line) for line in out["Body"].read().splitlines()]
    assert [r["id"] for r in rows] == ["r0", "r1", "r2", "r3"]
    assert rows[1]["embedding"] == fake_vector("hello 1")
    async with sm() as s:
        st = (
            await s.execute(
                text(
                    "SELECT s.status, t.status,"
                    " (SELECT count(*) FROM minion_subtask_acquirement WHERE task_name='loop1')"
                    " FROM minion_subtask s JOIN minion_task t USING (team, project, task_name)"
                    " WHERE s.task_name='loop1'"
                )
            )
        ).one()
    assert tuple(st) == ("SUCCEEDED", "SUCCEEDED", 0)
    assert not _leftover(tmp_path)
