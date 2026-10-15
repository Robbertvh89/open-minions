"""Control-loop branches with a fake server client, fake S3 (MockTransport) and fake worker."""

import asyncio
import hashlib
import json
import os
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
from minion_executor.loop import ExecutorLoop, cleanup_stale_tmp
from minion_executor.runners.base import Record, RunnerError
from minion_executor.worker.process import WorkerCrashed

BUDGET = Budget.model_validate(
    {"profile": "balanced", "memory_gb": 8, "gpu_duty_cycle": 0.5, "max_batch_size": 2}
)
MODEL = {"model_id": "m", "type": "embedding", "engine": "mlx", "ref": "r", "memory_gb": 0.1}
INPUT = b'{"id":"a","text":"x"}\n{"id":"b","text":"y"}\n{"id":"c","text":"z"}\n'


def assignment(
    count: int = 3, size: int = len(INPUT), url: str = "https://s3.test/in"
) -> Assignment:
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
            "input_url": url,
            "byte_start": 0,
            "byte_end": size,
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
        self.complete_excs: list[Exception] = []
        self.register_excs: list[Exception] = []
        self.on_empty: Any = None

    async def register(self, req: Any) -> None:
        self.calls.append(("register", None))
        if self.register_excs:
            raise self.register_excs.pop(0)

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
        if self.complete_excs:
            raise self.complete_excs.pop(0)
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
    def __init__(self, body: bytes = INPUT, get_status: int = 206, put_status: int = 200) -> None:
        self.body = body
        self.get_status, self.put_status = get_status, put_status
        self.puts: list[bytes] = []
        self.ranges: list[str] = []

    def handler(self, req: httpx.Request) -> httpx.Response:
        if req.method == "GET":
            self.ranges.append(req.headers["Range"])
            return httpx.Response(self.get_status, content=self.body)
        self.puts.append(req.read())
        return httpx.Response(self.put_status)


def leftover(p: Path) -> list[Path]:
    return list(p.iterdir())


async def _no_sleep(_: float) -> None:
    return None


def make_loop(
    client: FakeClient,
    worker: FakeWorker,
    s3: FakeS3,
    tmp_path: Path,
    sleeps: list[float],
    **kw: Any,
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
        **kw,
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
    client, s3 = FakeClient([assignment(count, len(body))]), FakeS3(body)
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


def _fail_req(client: FakeClient) -> FailRequest:
    req = next(r for k, r in client.calls if k == "fail")
    assert isinstance(req, FailRequest)
    return req


async def test_complete_retried_on_transport_error_then_succeeds(tmp_path: Path) -> None:
    client, sleeps = FakeClient([assignment()]), list[float]()
    client.complete_excs = [httpx.ConnectError("x"), ApiError(503, "X", "down")]
    loop = make_loop(client, FakeWorker(), FakeS3(), tmp_path, sleeps)
    assert await loop.run(max_subtasks=1) == 1
    assert client.kinds().count("complete") == 3
    assert len(sleeps) == 2  # jittered backoff between attempts
    assert "fail" not in client.kinds() and "release" not in client.kinds()


async def test_complete_gives_up_after_retries_without_fail(tmp_path: Path) -> None:
    client, sleeps = FakeClient([assignment()]), list[float]()
    client.complete_exc = httpx.ConnectError("x")
    loop = make_loop(client, FakeWorker(), FakeS3(), tmp_path, sleeps, complete_retries=3)
    client.on_empty = loop.stop
    assert await loop.run() == 0
    assert client.kinds().count("complete") == 3
    assert "fail" not in client.kinds()
    assert client.kinds().count("release") == 1  # best-effort hand-back, watchdog is the backstop
    assert len(sleeps) == 2 + 1 + 1  # 2 complete waits, 1 post-failure backoff, 1 idle


async def test_complete_4xx_reports_retryable_failure(tmp_path: Path) -> None:
    client = FakeClient([assignment()])
    client.complete_exc = ApiError(422, "VALIDATION", "bad")
    loop = make_loop(client, FakeWorker(), FakeS3(), tmp_path, [])
    client.on_empty = loop.stop
    await loop.run()
    req = _fail_req(client)
    assert (req.retryable, req.code) == (True, "COMPLETE_REJECTED")
    assert client.kinds().count("complete") == 1


async def test_complete_non_stale_409_is_discarded(tmp_path: Path) -> None:
    client = FakeClient([assignment()])
    client.complete_exc = ApiError(409, "CONFLICT", "x")
    loop = make_loop(client, FakeWorker(), FakeS3(), tmp_path, [])
    client.on_empty = loop.stop
    await loop.run()
    assert "fail" not in client.kinds()
    assert client.kinds().count("complete") == 1


async def test_output_count_mismatch_fails_before_upload(tmp_path: Path) -> None:
    class ShortWorker(FakeWorker):
        def run(self, batch: list[Record]) -> list[Record]:
            return super().run(batch)[:-1]

    client, s3 = FakeClient([assignment()]), FakeS3()
    loop = make_loop(client, ShortWorker(), s3, tmp_path, [])
    client.on_empty = loop.stop
    await loop.run()
    assert _fail_req(client).code == "OUTPUT_MISMATCH"
    assert not s3.puts


@pytest.mark.parametrize(
    ("s3", "code"),
    [
        (FakeS3(get_status=403), "INPUT_UNAVAILABLE"),
        (FakeS3(get_status=500), "INPUT_UNAVAILABLE"),
        (FakeS3(get_status=200), "INPUT_UNAVAILABLE"),  # Range ignored
        (FakeS3(body=INPUT[:-3]), "INPUT_UNAVAILABLE"),  # short read
        (FakeS3(put_status=403), "UPLOAD_FAILED"),  # e.g. expired presigned PUT
    ],
)
async def test_s3_verdicts_fail_retryable_not_release(
    tmp_path: Path, s3: FakeS3, code: str
) -> None:
    client = FakeClient([assignment()])
    loop = make_loop(client, FakeWorker(), s3, tmp_path, [])
    client.on_empty = loop.stop
    await loop.run()
    req = _fail_req(client)  # counts against max_attempts: no fleet-wide livelock
    assert (req.retryable, req.code) == (True, code)
    assert "release" not in client.kinds()
    assert "complete" not in client.kinds()


@pytest.mark.parametrize("phase", ["get", "put"])
@pytest.mark.parametrize("exc", [httpx.ConnectError("x"), httpx.ReadTimeout("x")])
async def test_connectivity_errors_release_not_fail(
    tmp_path: Path, phase: str, exc: Exception
) -> None:
    s3 = FakeS3()

    def handler(req: httpx.Request) -> httpx.Response:
        if (req.method == "GET") == (phase == "get"):
            raise exc
        return s3.handler(req)

    client = FakeClient([assignment()])
    loop = make_loop(client, FakeWorker(), s3, tmp_path, [])
    loop._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client.on_empty = loop.stop
    await loop.run()
    assert "fail" not in client.kinds()
    assert "release" in client.kinds()
    assert "complete" not in client.kinds()


async def test_failures_back_off_and_pause_after_streak(tmp_path: Path) -> None:
    items: list[Assignment | Exception | None] = [assignment() for _ in range(4)]
    client, sleeps = FakeClient(items), list[float]()
    worker = FakeWorker(WorkerCrashed("x"))
    loop = make_loop(
        client,
        worker,
        FakeS3(),
        tmp_path,
        sleeps,
        max_consecutive_failures=3,
        failure_pause_s=123.0,
    )
    client.on_empty = loop.stop
    await loop.run()
    # fail #1, fail #2 -> backoff (<=30); fail #3 -> long pause; fail #4 -> backoff; then idle
    assert len(sleeps) == 5
    assert sleeps[0] <= 10.0 and sleeps[1] <= 20.0
    assert sleeps[2] == 123.0
    assert sleeps[3] <= 10.0


async def test_success_resets_failure_streak(tmp_path: Path) -> None:
    class Flaky(FakeWorker):
        n = 0

        def ensure_model(self, model: ModelEntry, budget: Budget) -> None:
            Flaky.n += 1  # once per assignment
            if Flaky.n in (1, 3):  # fail, succeed, fail: never two failures in a row
                raise WorkerCrashed("x")

    client, sleeps = FakeClient([assignment(), assignment(), assignment()]), list[float]()
    loop = make_loop(client, Flaky(), FakeS3(), tmp_path, sleeps, max_consecutive_failures=2)
    client.on_empty = loop.stop
    await loop.run()
    assert client.kinds().count("fail") == 2 and client.kinds().count("complete") == 1
    assert all(x <= 30.0 for x in sleeps)  # no long pause


async def test_unexpected_error_fails_subtask_and_loop_keeps_going(tmp_path: Path) -> None:
    class Boom(FakeWorker):
        n = 0

        def run(self, batch: list[Record]) -> list[Record]:
            Boom.n += 1
            if Boom.n == 1:
                raise ValueError("secret payload text")
            return super().run(batch)

    client = FakeClient([assignment(), assignment()])
    loop = make_loop(client, Boom(), FakeS3(), tmp_path, [])
    client.on_empty = loop.stop
    assert await loop.run() == 1
    req = _fail_req(client)
    assert (req.retryable, req.code) == (True, "EXECUTOR_ERROR")
    assert "secret" not in req.message
    assert client.kinds().count("complete") == 1


async def test_deeply_nested_json_is_bad_input_not_a_crash(tmp_path: Path) -> None:
    body = b"[" * 200_000 + b"]" * 200_000 + b"\n"
    client, s3 = FakeClient([assignment(1, len(body))]), FakeS3(body)
    loop = make_loop(client, FakeWorker(), s3, tmp_path, [])
    client.on_empty = loop.stop
    await loop.run()
    req = _fail_req(client)
    assert (req.retryable, req.code) == (False, "BAD_INPUT")


async def test_insecure_presigned_url_rejected(tmp_path: Path) -> None:
    client = FakeClient([assignment(url="http://s3.test/in")])
    s3 = FakeS3()
    loop = make_loop(client, FakeWorker(), s3, tmp_path, [])
    client.on_empty = loop.stop
    await loop.run()
    assert _fail_req(client).code == "INSECURE_URL"
    assert not s3.ranges


async def test_loopback_http_allowed_only_when_server_is_loopback(tmp_path: Path) -> None:
    a = assignment(url="http://localhost:9000/in")
    a = a.model_copy(update={"output_url": "http://localhost:9000/out"})
    client, s3 = FakeClient([a]), FakeS3()
    loop = make_loop(client, FakeWorker(), s3, tmp_path, [], allow_http_loopback=True)
    assert await loop.run(max_subtasks=1) == 1
    # non-loopback http stays rejected even then
    bad = assignment(url="http://s3.example.com/in")
    client2 = FakeClient([bad])
    loop2 = make_loop(client2, FakeWorker(), FakeS3(), tmp_path, [], allow_http_loopback=True)
    client2.on_empty = loop2.stop
    await loop2.run()
    assert _fail_req(client2).code == "INSECURE_URL"


async def test_register_retried_on_transport_errors(tmp_path: Path) -> None:
    client, sleeps = FakeClient([]), list[float]()
    client.register_excs = [httpx.ConnectError("x"), ApiError(502, "X", "gw")]
    loop = make_loop(client, FakeWorker(), FakeS3(), tmp_path, sleeps)
    loop._register = object()  # type: ignore[assignment]
    client.on_empty = loop.stop
    await loop.run()
    assert client.kinds().count("register") == 3
    assert len(sleeps) >= 2


async def test_register_4xx_is_fatal(tmp_path: Path) -> None:
    client = FakeClient([])
    client.register_excs = [ApiError(401, "UNAUTHORIZED", "no")]
    loop = make_loop(client, FakeWorker(), FakeS3(), tmp_path, [])
    loop._register = object()  # type: ignore[assignment]
    with pytest.raises(ApiError):
        await loop.run()


def test_cleanup_stale_tmp_only_removes_dead_owner_dirs(tmp_path: Path) -> None:
    import subprocess
    import sys

    dead = subprocess.Popen([sys.executable, "-c", "pass"])
    dead.wait()
    stale = tmp_path / f"minion-exec-{dead.pid}-abc"
    mine = tmp_path / f"minion-exec-{os.getpid()}-abc"
    other = tmp_path / "unrelated"
    legacy = tmp_path / "minion-exec-nopid"
    for d in (stale, mine, other, legacy):
        d.mkdir()
        (d / "f").write_text("x")
    assert cleanup_stale_tmp(tmp_path) == 1
    assert not stale.exists()
    assert mine.exists() and other.exists() and legacy.exists()


async def test_failure_pause_is_interruptible_by_stop(tmp_path: Path) -> None:
    client = FakeClient([assignment()])
    loop = ExecutorLoop(
        client,  # type: ignore[arg-type]
        FakeWorker(WorkerCrashed("x")),
        BUDGET,
        [Engine.MLX],
        http=httpx.AsyncClient(transport=httpx.MockTransport(FakeS3().handler)),
        tmp_root=tmp_path,
        max_consecutive_failures=1,
        failure_pause_s=3600.0,
    )
    task = asyncio.create_task(loop.run())
    for _ in range(500):
        await asyncio.sleep(0.01)
        if "fail" in client.kinds():
            break
    await asyncio.sleep(0.1)  # now inside the hour-long pause
    loop.stop()
    assert await asyncio.wait_for(task, 3) == 0


async def test_register_retry_pause_is_interruptible_by_stop(tmp_path: Path) -> None:
    client = FakeClient([])
    client.register_excs = [httpx.ConnectError("x")] * 50
    loop = ExecutorLoop(
        client,  # type: ignore[arg-type]
        FakeWorker(),
        BUDGET,
        [Engine.MLX],
        http=httpx.AsyncClient(transport=httpx.MockTransport(FakeS3().handler)),
        register=object(),  # type: ignore[arg-type]
        tmp_root=tmp_path,
        backoff=(3600.0, 3600.0),
    )
    task = asyncio.create_task(loop.run())
    for _ in range(500):
        await asyncio.sleep(0.01)
        if "register" in client.kinds():
            break
    await asyncio.sleep(0.1)
    loop.stop()
    assert await asyncio.wait_for(task, 3) == 0
