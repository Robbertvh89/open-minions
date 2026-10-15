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
from minion_server.services.reclaim import reclaim_stale
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


# --- fault tolerance (TASK-3.3 S7). Heartbeat interval floor is 1 s, so beats are 1 s. ---

HB_S = 1.0  # executor heartbeat interval (floor)
TIMEOUT_S = 4.0  # server heartbeat timeout (>= 2x the interval, as documented)
SLOW = b'{"id":"slow0","text":"x","sleep_s":60}\n'


def _spawn_cmd(*args: str) -> tuple[str, ...]:
    return (sys.executable, "-c", "from minion_executor.main import app; app()", *args)


async def _inflight(lv: Live, n: int = 1) -> bool:
    return bool((await lv.rows("SELECT count(*) FROM minion_subtask_acquirement"))[0][0] == n)


async def _spawn_executor(lv: Live, tmp: Path, owner: str) -> asyncio.subprocess.Process:
    env = {
        **os.environ,
        "MINION_EXECUTOR_TOKEN": await lv.token(owner),
        "MINION_EXECUTOR_DEV": "1",
        "MINION_EXECUTOR_TEST_HOOKS": "1",
        "MINION_EXECUTOR_HEARTBEAT_INTERVAL_S": str(HB_S),
        "MINION_HEARTBEAT_TIMEOUT_S": str(TIMEOUT_S),
    }
    return await asyncio.create_subprocess_exec(
        *_spawn_cmd("run", "--server", lv.url, "--runner", "fake"), env=env, cwd=str(tmp)
    )


async def _spawn_watchdog(db_url: str, tmp: Path) -> asyncio.subprocess.Process:
    env = {
        **os.environ,
        "MINION_DATABASE_URL": db_url,
        "MINION_HEARTBEAT_TIMEOUT_S": str(TIMEOUT_S),
        "MINION_WATCHDOG_INTERVAL_S": "0.5",
    }
    return await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "from minion_watchdog.main import main; main()",
        env=env,
        cwd=str(tmp),
    )


async def _reap(proc: asyncio.subprocess.Process) -> None:
    if proc.returncode is None:
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), 15)
        except TimeoutError:
            proc.kill()
            await proc.wait()


def _in_process_loop(
    lv: Live, token: str, worker: WorkerHandle, tmp: Path
) -> tuple[ExecutorClient, ExecutorLoop]:
    client = ExecutorClient(lv.url, SecretStr(token))
    loop = ExecutorLoop(
        client,
        worker,
        BUDGET,
        [Engine.FAKE],
        backoff=FAST,
        tmp_root=tmp,
        allow_http_loopback=True,
        heartbeat_interval_s=HB_S,
    )
    return client, loop


async def test_killed_executor_subtask_is_reclaimed_and_rerun(
    live: Live, fresh_url: str, tmp_path: Path
) -> None:
    """AC #2: SIGKILL of executor A -> watchdog EXPIRES it -> B re-runs it, exactly once."""
    live.put_input("kill", "a.jsonl", SLOW + _records("k", 4))
    await live.submit("kill", rps=10)
    a = await _spawn_executor(live, tmp_path, "exec-a")
    wd = await _spawn_watchdog(fresh_url, tmp_path)
    b_client: ExecutorClient | None = None
    b_task: asyncio.Task[int] | None = None
    b_loop: ExecutorLoop | None = None
    try:
        await eventually(lambda: _inflight(live))
        a.send_signal(signal.SIGKILL)  # no release, no complete: only the watchdog can recover
        assert await asyncio.wait_for(a.wait(), 10) == -signal.SIGKILL

        async def expired() -> bool:
            rows = await live.rows("SELECT outcome FROM minion_subtask_attempt")
            return [r[0] for r in rows] == ["EXPIRED"]

        await eventually(expired, 30)
        b_client, b_loop = _in_process_loop(
            live, await live.token("exec-b"), WorkerHandle("fake"), tmp_path
        )
        b_task = asyncio.create_task(b_loop.run())

        async def done() -> bool:
            return await live.task_status("kill") == "SUCCEEDED"

        await eventually(done, 60)
    finally:
        if b_loop is not None and b_task is not None:
            b_loop.stop()
            await asyncio.wait_for(b_task, 30)
        if b_client is not None:
            await b_client.aclose()
        await _reap(a)
        await _reap(wd)

    att = await live.rows(
        "SELECT attempt, outcome, error->>'code' FROM minion_subtask_attempt ORDER BY attempt"
    )
    assert [(r[0], r[1]) for r in att] == [(1, "EXPIRED"), (2, "SUCCEEDED")]
    assert att[0][2] == "HEARTBEAT_TIMEOUT"
    ids = sorted(r["id"] for r in _out_rows(live, "kill"))
    assert ids == sorted(["slow0", *(f"k{i}" for i in range(4))])
    assert _final_keys(live, "kill") == ["outputs/e2e/kill/000000.jsonl"]
    assert (await live.rows("SELECT count(*) FROM minion_subtask_acquirement"))[0][0] == 0


async def test_cancel_interrupts_worker_and_executor_keeps_running(
    live: Live, tmp_path: Path
) -> None:
    """Cancel -> next beat sees cancel=true -> worker killed, release CANCELLED, loop idles."""
    live.put_input("cxl", "a.jsonl", SLOW + _records("c", 4))
    await live.submit("cxl", rps=10)
    worker = WorkerHandle("fake", test_hooks=True)
    client, loop = _in_process_loop(live, await live.token("exec-c"), worker, tmp_path)
    run = asyncio.create_task(loop.run())
    try:
        await eventually(lambda: _inflight(live))
        pid = worker.pid
        assert pid is not None
        t0 = time.monotonic()
        async with UserClient(live.url, SecretStr(live.api_key)) as uc:
            await uc.cancel_task("e2e", "p", "cxl")

        async def released() -> bool:
            return await _inflight(live, 0)

        await eventually(released, 30)
        assert time.monotonic() - t0 < 5 * HB_S  # ~1 heartbeat, generous for slow CI
        assert not run.done()  # executor idles instead of crashing
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)  # the worker (and its model memory) is gone
    finally:
        loop.stop()
        await asyncio.wait_for(run, 30)
        await client.aclose()

    assert await live.task_status("cxl") == "CANCELLED"
    att = await live.rows("SELECT outcome FROM minion_subtask_attempt WHERE task_name='cxl'")
    assert [r[0] for r in att] == ["CANCELLED"]
    sub = await live.rows("SELECT status FROM minion_subtask WHERE task_name='cxl'")
    assert [r[0] for r in sub] == ["CANCELLED"]
    assert _final_keys(live, "cxl") == []


async def test_stale_acquirement_409_stops_work_sends_nothing_and_continues(
    live: Live, tmp_path: Path
) -> None:
    """Forced reclaim -> next beat gets 409 -> worker killed, nothing reported, keeps polling."""
    live.put_input("stale", "a.jsonl", SLOW + _records("s", 4))
    await live.submit("stale", rps=10)
    worker = WorkerHandle("fake", test_hooks=True)
    client, loop = _in_process_loop(live, await live.token("exec-s"), worker, tmp_path)
    run = asyncio.create_task(loop.run())
    try:
        await eventually(lambda: _inflight(live))
        first = (await live.rows("SELECT acquirement_id FROM minion_subtask_acquirement"))[0][0]
        pid = worker.pid
        assert pid is not None
        async with live.sm() as s, s.begin():
            await s.execute(
                text(
                    "UPDATE minion_executor_heartbeat SET last_seen_at = now() - interval '1 hour'"
                )
            )
        await reclaim_stale(live.sm, timeout_s=TIMEOUT_S, batch_size=10)
        assert [r[0] for r in await live.rows("SELECT outcome FROM minion_subtask_attempt")] == [
            "EXPIRED"
        ]

        async def reacquired() -> bool:
            rows = await live.rows("SELECT acquirement_id FROM minion_subtask_acquirement")
            return len(rows) == 1 and rows[0][0] != first

        await eventually(reacquired, 30)  # 409 -> discarded -> polls again -> attempt 2
        assert not run.done()
    finally:
        loop.stop()
        await asyncio.wait_for(run, 30)
        await client.aclose()

    att = await live.rows("SELECT attempt, outcome FROM minion_subtask_attempt ORDER BY attempt")
    # attempt 1 stays EXPIRED (the stale owner reported nothing); attempt 2 released by shutdown
    assert [(r[0], r[1]) for r in att] == [(1, "EXPIRED"), (2, "RELEASED")]
    assert _final_keys(live, "stale") == []
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
