"""Control-loop branches with a fake server client, fake S3 (MockTransport) and fake worker."""

import asyncio
import hashlib
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
from minion_common.client import ApiError, StaleAcquirementError
from minion_common.enums import Engine
from minion_common.schemas import (
    AcquireRequest,
    Assignment,
    Budget,
    CompleteRequest,
    FailRequest,
    ModelEntry,
)
from minion_executor.loop import ExecutorLoop
from minion_executor.runners.base import Record, RunnerError
from minion_executor.worker.process import WorkerCrashed

BUDGET = Budget.model_validate(
    {"profile": "balanced", "memory_gb": 8, "gpu_duty_cycle": 0.5, "max_batch_size": 2}
)
MODEL = {"model_id": "m", "type": "embedding", "engine": "mlx", "ref": "r", "memory_gb": 0.1}
INPUT = b'{"id":"a","text":"x"}\n{"id":"b","text":"y"}\n{"id":"c","text":"z"}\n'


def assignment(count: int = 3) -> Assignment:
    return Assignment.model_validate(
        {
            "acquirement_id": str(uuid.uuid4()),
            "attempt": 1,
            "team": "t",
            "project": "p",
            "task_name": "n",
            "subtask_id": 0,
            "type": "embedding",
            "model": MODEL,
            "input_url": "https://s3.test/in",
            "byte_start": 0,
            "byte_end": len(INPUT),
            "record_count": count,
            "output_url": "https://s3.test/out",
            "expires_at": datetime.now(UTC).isoformat(),
        }
    )


class FakeClient:
    def __init__(self, items: list[Assignment | Exception | None]) -> None:
        self.items = items
        self.calls: list[tuple[str, Any]] = []
        self.complete_exc: Exception | None = None
        self.on_empty: Any = None

    async def acquire(self, req: AcquireRequest) -> Assignment | None:
        self.calls.append(("acquire", None))
        if not self.items:
            if self.on_empty:
                self.on_empty()
            return None
        item = self.items.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    async def complete(self, aid: uuid.UUID, req: CompleteRequest) -> None:
        self.calls.append(("complete", req))
        if self.complete_exc:
            raise self.complete_exc

    async def fail(self, aid: uuid.UUID, req: FailRequest) -> None:
        self.calls.append(("fail", req))

    async def release(self, aid: uuid.UUID) -> None:
        self.calls.append(("release", aid))

    def kinds(self) -> list[str]:
        return [k for k, _ in self.calls]


class FakeWorker:
    def __init__(self, exc: Exception | None = None, block: bool = False) -> None:
        self.exc, self.block = exc, block
        self.batches: list[int] = []
        self.killed = False
        self.closed = False

    def ensure_model(self, model: ModelEntry, budget: Budget) -> None:
        pass

    def run(self, batch: list[Record]) -> list[Record]:
        if self.block:
            import time

            while not self.killed:
                time.sleep(0.01)
            raise WorkerCrashed("killed")
        if self.exc:
            raise self.exc
        self.batches.append(len(batch))
        return [{"id": r["id"], "embedding": [1.0]} for r in batch]

    def kill(self) -> None:
        self.killed = True

    def close(self) -> None:
        self.closed = True


class FakeS3:
    def __init__(self, body: bytes = INPUT) -> None:
        self.body = body
        self.puts: list[bytes] = []
        self.ranges: list[str] = []

    def handler(self, req: httpx.Request) -> httpx.Response:
        if req.method == "GET":
            self.ranges.append(req.headers["Range"])
            return httpx.Response(206, content=self.body)
        self.puts.append(req.read())
        return httpx.Response(200)


def leftover(p: Path) -> list[Path]:
    return list(p.iterdir())


async def _no_sleep(_: float) -> None:
    return None


def make_loop(
    client: FakeClient, worker: FakeWorker, s3: FakeS3, tmp_path: Path, sleeps: list[float]
) -> ExecutorLoop:
    async def rec(d: float) -> None:
        sleeps.append(d)

    http = httpx.AsyncClient(transport=httpx.MockTransport(s3.handler))
    return ExecutorLoop(
        client,  # type: ignore[arg-type]
        worker,
        BUDGET,
        [Engine.MLX],
        http=http,
        sleep=rec,
        tmp_root=tmp_path,
    )


async def test_no_work_backs_off_with_jitter(tmp_path: Path) -> None:
    client, sleeps = FakeClient([]), list[float]()
    loop = make_loop(client, FakeWorker(), FakeS3(), tmp_path, sleeps)
    n = 0

    def stop_after() -> None:
        nonlocal n
        n += 1
        if n == 3:
            loop.stop()

    client.on_empty = stop_after
    assert await loop.run() == 0
    assert len(sleeps) == 3
    assert all(5.0 <= s <= 30.0 for s in sleeps)
    assert sleeps[2] >= 15.0  # third consecutive empty: capped at 30, jitter >= 0.5


async def test_transient_acquire_error_backs_off(tmp_path: Path) -> None:
    client = FakeClient([httpx.ConnectError("boom"), ApiError(503, "X", "down")])
    sleeps: list[float] = []
    loop = make_loop(client, FakeWorker(), FakeS3(), tmp_path, sleeps)
    client.on_empty = loop.stop
    await loop.run()
    assert len(sleeps) == 3  # two errors + final empty


async def test_auth_error_is_fatal(tmp_path: Path) -> None:
    client = FakeClient([ApiError(401, "UNAUTHORIZED", "no")])
    loop = make_loop(client, FakeWorker(), FakeS3(), tmp_path, [])
    with pytest.raises(ApiError):
        await loop.run()


async def test_success_uploads_and_completes(tmp_path: Path) -> None:
    client, s3, worker = FakeClient([assignment()]), FakeS3(), FakeWorker()
    loop = make_loop(client, worker, s3, tmp_path, [])
    assert await loop.run(max_subtasks=1) == 1
    assert s3.ranges == [f"bytes=0-{len(INPUT) - 1}"]
    assert worker.batches == [2, 1]
    assert len(s3.puts) == 1
    kind, req = client.calls[-1]
    assert kind == "complete"
    assert req.record_count == 3
    assert req.byte_size == len(s3.puts[0])
    assert req.sha256 == hashlib.sha256(s3.puts[0]).hexdigest()
    assert [json.loads(x)["id"] for x in s3.puts[0].splitlines()] == ["a", "b", "c"]
    assert worker.closed
    assert not leftover(tmp_path)  # private temp dir and files removed


async def test_stale_on_complete_is_discarded(tmp_path: Path) -> None:
    client = FakeClient([assignment(), None])
    client.complete_exc = StaleAcquirementError(409, "STALE_ACQUIREMENT", "stale")
    loop = make_loop(client, FakeWorker(), FakeS3(), tmp_path, [])
    client.on_empty = loop.stop
    assert await loop.run() == 0
    assert "fail" not in client.kinds()
    assert client.kinds().count("acquire") >= 2  # kept going


@pytest.mark.parametrize(
    ("body", "count"),
    [(b'{"id":"a"}\nnot json\n{"id":"c"}\n', 3), (INPUT, 5), (b'[1]\n{"id":"b"}\n{"id":"c"}\n', 3)],
)
async def test_bad_input_non_retryable(tmp_path: Path, body: bytes, count: int) -> None:
    client, s3 = FakeClient([assignment(count)]), FakeS3(body)
    loop = make_loop(client, FakeWorker(), s3, tmp_path, [])
    client.on_empty = loop.stop
    await loop.run()
    kind, req = next(c for c in client.calls if c[0] == "fail")
    assert kind == "fail"
    assert (req.retryable, req.code) == (False, "BAD_INPUT")
    assert "not json" not in req.message
    assert not s3.puts


async def test_worker_crash_is_retryable_fail(tmp_path: Path) -> None:
    client = FakeClient([assignment()])
    loop = make_loop(client, FakeWorker(WorkerCrashed("x")), FakeS3(), tmp_path, [])
    client.on_empty = loop.stop
    await loop.run()
    req = next(r for k, r in client.calls if k == "fail")
    assert (req.retryable, req.code) == (True, "WORKER_CRASHED")


async def test_runner_error_is_retryable_fail(tmp_path: Path) -> None:
    client = FakeClient([assignment()])
    loop = make_loop(client, FakeWorker(RunnerError("oom", "secret")), FakeS3(), tmp_path, [])
    client.on_empty = loop.stop
    await loop.run()
    req = next(r for k, r in client.calls if k == "fail")
    assert (req.retryable, req.code) == (True, "oom")
    assert "secret" not in req.message


async def test_stop_releases_current_subtask(tmp_path: Path) -> None:
    client, worker = FakeClient([assignment()]), FakeWorker(block=True)
    loop = make_loop(client, worker, FakeS3(), tmp_path, [])
    task = asyncio.create_task(loop.run())
    for _ in range(200):
        await asyncio.sleep(0.01)
        if "acquire" in client.kinds():
            break
    await asyncio.sleep(0.2)
    loop.stop()
    assert await asyncio.wait_for(task, 5) == 0
    assert client.kinds() == ["acquire", "release"]
    assert worker.killed and worker.closed
    assert not leftover(tmp_path)
