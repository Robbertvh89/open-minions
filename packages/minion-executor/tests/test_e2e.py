"""Process-level e2e (TASK-2.3 AC #1/#2): real uvicorn, Postgres, SeaweedFS, fake runner."""

import asyncio
import hashlib
import json
import os
import signal
import socket
import sys
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import uvicorn
from minion_common.client import ExecutorClient, UserClient
from minion_common.enums import Engine
from minion_common.schemas import Budget, TaskCreate
from minion_executor.loop import ExecutorLoop
from minion_executor.worker.process import WorkerHandle
from minion_server.admin import issue_executor_token
from minion_server.app import create_app
from pydantic import SecretStr
from sqlalchemy import text

from conftest import S3Env, _settings

pytestmark = pytest.mark.integration

API_KEY = "e2e-user-key"
TEAM = "e2e"
FAST = (0.02, 0.1)
BUDGET = Budget.model_validate(
    {"profile": "balanced", "memory_gb": 8, "gpu_duty_cycle": 0.5, "max_batch_size": 7}
)


@dataclass
class Live:
    url: str
    sm: Any
    s3: S3Env
    api_key: str = API_KEY

    async def token(self, owner: str) -> str:
        return (await issue_executor_token(self.sm, owner=owner, pool="default", ttl_days=1))[1]

    def put_input(self, name: str, obj: str, body: bytes) -> None:
        self.s3.client.put_object(
            Bucket=self.s3.bucket, Key=f"inputs/{TEAM}/{name}/{obj}", Body=body
        )

    async def submit(self, name: str, rps: int) -> None:
        async with UserClient(self.url, SecretStr(self.api_key)) as uc:
            await uc.create_task(
                TaskCreate.model_validate(
                    {
                        "team": TEAM,
                        "project": "p",
                        "task_name": name,
                        "type": "embedding",
                        "model": "fake-embed",
                        "input_uri": f"s3://{self.s3.bucket}/inputs/{TEAM}/{name}/",
                        "output_uri": f"s3://{self.s3.bucket}/outputs/{TEAM}/{name}/",
                        "records_per_subtask": rps,
                    }
                )
            )

    async def task_status(self, name: str) -> str:
        async with UserClient(self.url, SecretStr(self.api_key)) as uc:
            return str((await uc.get_task(TEAM, "p", name)).status)

    async def rows(self, sql: str, **p: Any) -> list[Any]:
        async with self.sm() as s:
            return list((await s.execute(text(sql), p)).all())


async def eventually(cond: Callable[[], Awaitable[bool]], limit_s: float = 60.0) -> None:
    deadline = time.monotonic() + limit_s
    while not await cond():
        if time.monotonic() > deadline:
            raise TimeoutError("condition not met")
        await asyncio.sleep(0.05)


@pytest.fixture
async def live(fresh_url: str, s3: S3Env, tmp_path: Path) -> AsyncIterator[Live]:
    keys = tmp_path / "keys.yaml"
    digest = hashlib.sha256(API_KEY.encode()).hexdigest()
    keys.write_text(f"keys:\n  - name: u\n    sha256: {digest}\n    teams: [{TEAM}]\n")
    cfg = _settings(fresh_url).model_copy(
        update={
            "s3_endpoint_url": s3.endpoint_url,
            "s3_access_key": SecretStr(s3.access_key),
            "s3_secret_key": SecretStr(s3.secret_key),
            "s3_bucket": s3.bucket,
            "api_keys_file": str(keys),
            "splitter_poll_s": 0.1,
        }
    )
    app = create_app(cfg)
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level="warning", lifespan="on"))
    serve = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        await eventually(lambda: _started(server))
        yield Live(f"http://127.0.0.1:{port}", app.state.sessionmaker, s3)
    finally:
        server.should_exit = True
        await asyncio.wait_for(serve, 30)
        sock.close()


async def _started(server: uvicorn.Server) -> bool:
    return server.started


def _records(prefix: str, n: int) -> bytes:
    return b"".join(f'{{"id":"{prefix}{i}","text":"t {prefix}{i}"}}\n'.encode() for i in range(n))


def _final_keys(live: Live, name: str) -> list[str]:
    pfx = f"outputs/{TEAM}/{name}/"
    resp = live.s3.client.list_objects_v2(Bucket=live.s3.bucket, Prefix=pfx)
    return sorted(o["Key"] for o in resp.get("Contents", []) if "_attempts" not in o["Key"])


def _out_rows(live: Live, name: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for k in _final_keys(live, name):
        body = live.s3.client.get_object(Bucket=live.s3.bucket, Key=k)["Body"].read()
        out.extend(json.loads(line) for line in body.splitlines())
    return out


async def _run_loops(live: Live, workers: list[WorkerHandle], name: str, tmp: Path) -> list[int]:
    tokens = [await live.token(f"o{i}") for i in range(len(workers))]
    clients = [ExecutorClient(live.url, SecretStr(t)) for t in tokens]
    loops = [
        ExecutorLoop(
            c, w, BUDGET, [Engine.FAKE], backoff=FAST, tmp_root=tmp, allow_http_loopback=True
        )
        for c, w in zip(clients, workers, strict=True)
    ]
    tasks = [asyncio.create_task(lp.run()) for lp in loops]
    try:

        async def done() -> bool:
            return await live.task_status(name) in ("SUCCEEDED", "FAILED", "PARTIAL")

        await eventually(done)
    finally:
        for lp in loops:
            lp.stop()
        res = await asyncio.wait_for(asyncio.gather(*tasks), 30)
        for c in clients:
            await c.aclose()
    return list(res)


async def test_two_executors_complete_task_exactly_once(live: Live, tmp_path: Path) -> None:
    """AC #1: multi-object JSONL -> splitter -> 2 concurrent loops -> SUCCEEDED, 1:1 outputs."""
    live.put_input("two", "a.jsonl", _records("a", 130))
    live.put_input("two", "b.jsonl", _records("b", 107))
    await live.submit("two", rps=25)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    counts = await _run_loops(live, [WorkerHandle("fake"), WorkerHandle("fake")], "two", scratch)

    assert await live.task_status("two") == "SUCCEEDED"
    n_sub = (await live.rows("SELECT count(*) FROM minion_subtask WHERE task_name='two'"))[0][0]
    assert n_sub == 11  # ceil(130/25) + ceil(107/25)
    assert sum(counts) >= n_sub
    assert _final_keys(live, "two") == [f"outputs/{TEAM}/two/{i:06d}.jsonl" for i in range(n_sub)]
    ids = [r["id"] for r in _out_rows(live, "two")]
    expected = [f"a{i}" for i in range(130)] + [f"b{i}" for i in range(107)]
    assert sorted(ids) == sorted(expected)
    assert len(set(ids)) == len(ids)
    per = await live.rows(
        "SELECT subtask_id, count(*) FILTER (WHERE outcome='SUCCEEDED') FROM minion_subtask_attempt"
        " WHERE task_name='two' GROUP BY subtask_id"
    )
    assert len(per) == n_sub
    assert all(c == 1 for _, c in per)
    assert (await live.rows("SELECT count(*) FROM minion_subtask_acquirement"))[0][0] == 0
    assert not await asyncio.to_thread(lambda: list(scratch.iterdir()))


class CrashOnceWorker(WorkerHandle):
    """SIGKILLs its own worker subprocess during the first batch it runs."""

    def __init__(self) -> None:
        super().__init__("fake")
        self.killed_pid: int | None = None

    def run(self, batch: Any) -> Any:
        if self.killed_pid is None and self.pid is not None:
            self.killed_pid = self.pid
            os.kill(self.pid, signal.SIGKILL)
        return super().run(batch)


async def test_worker_sigkill_is_retried_to_success(live: Live, tmp_path: Path) -> None:
    """AC #2(a): kill -9 of the worker -> retryable FAILED attempt, re-acquired, SUCCEEDED."""
    live.put_input("crash", "a.jsonl", _records("c", 20))
    await live.submit("crash", rps=10)
    worker = CrashOnceWorker()
    await _run_loops(live, [worker], "crash", tmp_path)

    assert worker.killed_pid is not None
    assert await live.task_status("crash") == "SUCCEEDED"
    outcomes = await live.rows(
        "SELECT subtask_id, attempt, outcome FROM minion_subtask_attempt"
        " WHERE task_name='crash' ORDER BY subtask_id, attempt"
    )
    failed = [o for o in outcomes if o[2] == "FAILED"]
    assert len(failed) == 1
    sid = failed[0][0]
    assert [o[2] for o in outcomes if o[0] == sid] == ["FAILED", "SUCCEEDED"]
    err = (await live.rows("SELECT error FROM minion_subtask_attempt WHERE outcome='FAILED'"))[0][0]
    assert "WORKER_CRASHED" in json.dumps(err)
    ids = sorted(r["id"] for r in _out_rows(live, "crash"))
    assert ids == sorted(f"c{i}" for i in range(20))


async def test_sigterm_releases_inflight_subtask(live: Live, tmp_path: Path) -> None:
    """AC #2(b): SIGTERM to `minion-executor run` -> exit 0, RELEASED, subtask PENDING again."""
    body = b'{"id":"slow0","text":"x","sleep_s":60}\n' + _records("s", 2)
    live.put_input("term", "a.jsonl", body)
    await live.submit("term", rps=10)
    token = await live.token("sigterm")
    env = {
        **os.environ,
        "MINION_EXECUTOR_TOKEN": token,
        "MINION_EXECUTOR_DEV": "1",
        "MINION_EXECUTOR_TEST_HOOKS": "1",
    }
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "from minion_executor.main import app; app()",
        "run",
        "--server",
        live.url,
        "--profile",
        "balanced",
        "--runner",
        "fake",
        env=env,
        cwd=str(tmp_path),
    )
    try:

        async def inflight() -> bool:
            r = await live.rows("SELECT count(*) FROM minion_subtask_acquirement")
            return bool(r[0][0] == 1)

        await eventually(inflight)
        await asyncio.sleep(0.5)  # let the worker start sleeping; release works either way
        proc.send_signal(signal.SIGTERM)
        rc = await asyncio.wait_for(proc.wait(), 15)
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
    assert rc == 0
    att = await live.rows("SELECT outcome FROM minion_subtask_attempt WHERE task_name='term'")
    assert [a[0] for a in att] == ["RELEASED"]
    sub = (await live.rows("SELECT status FROM minion_subtask WHERE task_name='term'"))[0][0]
    assert sub == "PENDING"
    assert (await live.rows("SELECT count(*) FROM minion_subtask_acquirement"))[0][0] == 0
