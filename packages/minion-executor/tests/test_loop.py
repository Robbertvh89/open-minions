"""Control-loop branches with a fake server client, fake S3 (MockTransport) and fake worker."""

import asyncio
import hashlib
import json
import os
import uuid
from datetime import UTC, datetime, timedelta
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
    HeartbeatRequest,
    HeartbeatResponse,
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
            "expires_at": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
        }
    )


def hb_resp(
    cancel: bool = False, url: str = "https://s3.test/out", minutes: int = 0
) -> HeartbeatResponse:
    return HeartbeatResponse(
        cancel=cancel, output_url=url, expires_at=datetime.now(UTC) + timedelta(minutes=minutes)
    )


def gated_sleep(beats: int, delays: list[float]) -> Any:
    """Fake heartbeat sleep: the first `beats` sleeps return quickly, then it blocks forever."""

    async def sleep(d: float) -> None:
        delays.append(d)
        if len(delays) > beats:
            await asyncio.Event().wait()
        await asyncio.sleep(0.01)

    return sleep


class FakeClient:
    def __init__(self, items: list[Assignment | Exception | None]) -> None:
        self.items = items
        self.calls: list[tuple[str, Any]] = []
        self.complete_exc: Exception | None = None
        self.complete_excs: list[Exception] = []
        self.register_excs: list[Exception] = []
        self.on_empty: Any = None
        self.hb_script: list[HeartbeatResponse | Exception] = []

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

    async def heartbeat(self, aid: uuid.UUID, req: HeartbeatRequest) -> HeartbeatResponse:
        self.calls.append(("heartbeat", req))
        if self.hb_script:
            item = self.hb_script.pop(0)
            if isinstance(item, Exception):
                raise item
            return item
        return hb_resp()

    def kinds(self) -> list[str]:
        return [k for k, _ in self.calls]


class FakeWorker:
    def __init__(self, exc: Exception | None = None, block: bool = False) -> None:
        self.exc, self.block = exc, block
        self.batches: list[int] = []
        self.killed = False
        self.interrupted = False
        self.closed = False

    def ensure_model(self, model: ModelEntry, budget: Budget) -> None:
        pass

    def run(self, batch: list[Record]) -> list[Record]:
        if self.block:
            import time

            while not (self.killed or self.interrupted):
                time.sleep(0.01)
            raise WorkerCrashed("killed")
        if self.exc:
            raise self.exc
        self.batches.append(len(batch))
        return [{"id": r["id"], "embedding": [1.0]} for r in batch]

    def interrupt(self) -> None:
        self.interrupted = True

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


async def test_complete_transport_exhaustion_leaves_to_watchdog(tmp_path: Path) -> None:
    client, sleeps = FakeClient([assignment()]), list[float]()
    client.complete_exc = httpx.ConnectError("x")
    loop = make_loop(client, FakeWorker(), FakeS3(), tmp_path, sleeps, complete_retries=3)
    client.on_empty = loop.stop
    assert await loop.run() == 0
    assert client.kinds().count("complete") == 3
    assert "fail" not in client.kinds() and "release" not in client.kinds()
    assert len(sleeps) == 2 + 1 + 1  # 2 complete waits, 1 post-failure backoff, 1 idle


async def test_complete_5xx_exhaustion_fails_retryable(tmp_path: Path) -> None:
    client, sleeps = FakeClient([assignment()]), list[float]()
    client.complete_exc = ApiError(503, "X", "down")
    loop = make_loop(client, FakeWorker(), FakeS3(), tmp_path, sleeps, complete_retries=3)
    client.on_empty = loop.stop
    assert await loop.run() == 0
    assert client.kinds().count("complete") == 3
    assert client.kinds().count("fail") == 1 and "release" not in client.kinds()
    req = next(r for k, r in client.calls if k == "fail")
    assert req.code == "COMPLETE_FAILED" and req.retryable


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


# --- heartbeats (TASK-3.3) ---


async def test_heartbeats_sent_with_progress_and_interval(tmp_path: Path) -> None:
    client, delays = FakeClient([assignment()]), list[float]()
    worker = FakeWorker(block=True)
    loop = make_loop(
        client, worker, FakeS3(), tmp_path, [],
        heartbeat_interval_s=100.0, heartbeat_sleep=gated_sleep(2, delays),
    )  # fmt: skip
    asyncio.get_running_loop().call_later(0.4, loop.stop)
    await loop.run()
    beats = [r for k, r in client.calls if k == "heartbeat"]
    assert len(beats) == 2
    assert all(100.0 <= d <= 110.0 for d in delays[:2])  # jitter is upward only
    assert beats[0].progress.records_total == 3 and beats[0].progress.records_done == 0
    assert beats[0].budget == BUDGET


async def test_heartbeat_reports_progress_and_no_beats_after_subtask(tmp_path: Path) -> None:
    client, delays = FakeClient([assignment()]), list[float]()
    loop = make_loop(
        client, FakeWorker(), FakeS3(), tmp_path, [],
        heartbeat_interval_s=100.0, heartbeat_sleep=gated_sleep(0, delays),
    )  # fmt: skip
    client.on_empty = loop.stop
    assert await loop.run() == 1
    assert "heartbeat" not in client.kinds()
    await asyncio.sleep(0.05)
    assert len(delays) == 1  # the heartbeater was cancelled while sleeping its first interval


async def test_cancel_interrupts_worker_releases_and_continues(tmp_path: Path) -> None:
    first, second = assignment(), assignment()
    client, s3 = FakeClient([first, second]), FakeS3()
    client.hb_script = [hb_resp(cancel=True)]
    worker = FakeWorker(block=True)
    sleeps: list[float] = []
    loop = make_loop(
        client, worker, s3, tmp_path, sleeps,
        heartbeat_interval_s=100.0, heartbeat_sleep=gated_sleep(1, []),
    )  # fmt: skip
    client.on_empty = loop.stop
    # the second assignment must run normally: the worker stops blocking once interrupted
    orig_run = worker.run

    def run(batch: list[Record]) -> list[Record]:
        if worker.interrupted:
            worker.block = False
        return orig_run(batch)

    worker.run = run  # type: ignore[method-assign]
    assert await loop.run() == 1
    assert worker.interrupted and not worker.killed
    assert client.kinds() == ["acquire", "heartbeat", "release", "acquire", "complete", "acquire"]
    assert len(s3.puts) == 1  # only the second subtask uploads
    assert len(sleeps) == 1  # only the final empty-queue sleep: no failure backoff for CANCELLED


async def test_cancel_has_no_failure_backoff_and_no_upload(tmp_path: Path) -> None:
    client, s3, sleeps = FakeClient([assignment()]), FakeS3(), list[float]()
    client.hb_script = [hb_resp(cancel=True)]
    loop = make_loop(
        client, FakeWorker(block=True), s3, tmp_path, sleeps,
        heartbeat_interval_s=100.0, heartbeat_sleep=gated_sleep(1, []),
    )  # fmt: skip
    client.on_empty = loop.stop
    assert await loop.run() == 0
    assert s3.puts == []
    assert client.kinds() == ["acquire", "heartbeat", "release", "acquire"]
    assert len(sleeps) == 1  # only the empty-queue sleep, no failure delay
    assert leftover(tmp_path) == []


async def test_stale_heartbeat_interrupts_and_reports_nothing(tmp_path: Path) -> None:
    client, s3 = FakeClient([assignment()]), FakeS3()
    client.hb_script = [StaleAcquirementError(409, "STALE_ACQUIREMENT", "gone")]
    worker = FakeWorker(block=True)
    loop = make_loop(
        client, worker, s3, tmp_path, [],
        heartbeat_interval_s=100.0, heartbeat_sleep=gated_sleep(1, []),
    )  # fmt: skip
    client.on_empty = loop.stop
    await loop.run()
    assert worker.interrupted
    kinds = client.kinds()
    assert not {"release", "complete", "fail"} & set(kinds)
    assert s3.puts == []


async def test_rate_limited_heartbeat_honours_retry_after(tmp_path: Path) -> None:
    client, delays = FakeClient([assignment()]), list[float]()
    client.hb_script = [ApiError(429, "RATE_LIMITED", "slow", retry_after=42.0)]
    loop = make_loop(
        client, FakeWorker(block=True), FakeS3(), tmp_path, [],
        heartbeat_interval_s=100.0, heartbeat_sleep=gated_sleep(2, delays),
    )  # fmt: skip
    asyncio.get_running_loop().call_later(0.4, loop.stop)
    await loop.run()
    assert delays[1] == 42.0
    assert "release" in client.kinds()  # only because of stop, not the 429
    assert "fail" not in client.kinds()


async def test_transport_error_in_heartbeat_does_not_fail_subtask(tmp_path: Path) -> None:
    client, delays = FakeClient([assignment()]), list[float]()
    client.hb_script = [httpx.ConnectError("x"), ApiError(503, "X", "down")]
    s3 = FakeS3()
    worker = FakeWorker()
    loop = make_loop(
        client, worker, s3, tmp_path, [],
        heartbeat_interval_s=100.0, heartbeat_sleep=gated_sleep(2, delays),
    )  # fmt: skip
    # slow the worker so both beats happen while the subtask is running
    orig = worker.run

    def slow(batch: list[Record]) -> list[Record]:
        import time

        time.sleep(0.2)
        return orig(batch)

    worker.run = slow  # type: ignore[method-assign]
    client.on_empty = loop.stop
    assert await loop.run() == 1
    assert client.kinds().count("heartbeat") == 2
    assert all(d <= 33.0 for d in delays[1:3])  # retry delay capped at 30 s (+jitter)
    assert "fail" not in client.kinds()


async def test_refreshed_output_url_used_for_upload(tmp_path: Path) -> None:
    client, s3 = FakeClient([assignment()]), FakeS3()
    client.hb_script = [hb_resp(url="https://s3.test/out?sig=new", minutes=10)]
    urls: list[str] = []
    orig_handler = s3.handler

    def handler(req: httpx.Request) -> httpx.Response:
        if req.method == "PUT":
            urls.append(str(req.url))
        return orig_handler(req)

    s3.handler = handler  # type: ignore[method-assign]
    worker = FakeWorker()
    orig = worker.run

    def slow(batch: list[Record]) -> list[Record]:
        import time

        time.sleep(0.3)
        return orig(batch)

    worker.run = slow  # type: ignore[method-assign]
    loop = make_loop(
        client, worker, s3, tmp_path, [],
        heartbeat_interval_s=100.0, heartbeat_sleep=gated_sleep(1, []),
    )  # fmt: skip
    client.on_empty = loop.stop
    assert await loop.run() == 1
    assert urls == ["https://s3.test/out?sig=new"]


async def test_stop_during_heartbeat_wait_is_prompt(tmp_path: Path) -> None:
    client = FakeClient([assignment()])
    worker = FakeWorker(block=True)
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], heartbeat_interval_s=100.0)
    asyncio.get_running_loop().call_later(0.2, loop.stop)
    await asyncio.wait_for(loop.run(), timeout=5)
    assert worker.killed
    assert client.kinds().count("release") == 1


def _slow_worker(worker: FakeWorker, secs: float) -> None:
    import time

    orig = worker.run

    def slow(batch: list[Record]) -> list[Record]:
        time.sleep(secs)
        return orig(batch)

    worker.run = slow  # type: ignore[method-assign]


@pytest.mark.parametrize(
    "retry_after,interval,expected", [(9999.0, 100.0, 100.0), (9999.0, 1000.0, 600.0)]
)
async def test_retry_after_is_capped_by_interval_and_max(
    tmp_path: Path, retry_after: float, interval: float, expected: float
) -> None:
    client, delays = FakeClient([assignment()]), list[float]()
    client.hb_script = [ApiError(429, "RATE_LIMITED", "slow", retry_after=retry_after)]
    loop = make_loop(
        client, FakeWorker(block=True), FakeS3(), tmp_path, [],
        heartbeat_interval_s=interval, heartbeat_sleep=gated_sleep(2, delays),
    )  # fmt: skip
    asyncio.get_running_loop().call_later(0.4, loop.stop)
    await loop.run()
    assert delays[1] == expected


async def test_task_finished_409_is_handled_like_stale(tmp_path: Path) -> None:
    client, s3 = FakeClient([assignment()]), FakeS3()
    client.hb_script = [ApiError(409, "TASK_FINISHED", "done")]
    worker = FakeWorker(block=True)
    loop = make_loop(
        client, worker, s3, tmp_path, [],
        heartbeat_interval_s=100.0, heartbeat_sleep=gated_sleep(1, []),
    )  # fmt: skip
    client.on_empty = loop.stop
    await loop.run()
    assert worker.interrupted
    assert client.kinds() == ["acquire", "heartbeat", "acquire"]
    assert s3.puts == []


@pytest.mark.parametrize("sig", ["cancel", "stale"])
async def test_beat_signal_during_inflight_upload(tmp_path: Path, sig: str) -> None:
    s3 = FakeS3()
    started = asyncio.Event()
    orig = s3.handler

    async def handler(req: httpx.Request) -> httpx.Response:
        if req.method == "PUT":
            started.set()
            await asyncio.Event().wait()  # the upload never finishes on its own
        return orig(req)

    s3.handler = handler  # type: ignore[method-assign, assignment]
    client = FakeClient([assignment()])
    client.hb_script = [
        hb_resp(cancel=True)
        if sig == "cancel"
        else StaleAcquirementError(409, "STALE_ACQUIREMENT", "x")
    ]

    async def hb_sleep(_: float) -> None:
        await started.wait()

    worker = FakeWorker()
    loop = make_loop(
        client, worker, s3, tmp_path, [], heartbeat_interval_s=100.0, heartbeat_sleep=hb_sleep
    )
    client.on_empty = loop.stop
    assert await asyncio.wait_for(loop.run(), 10) == 0
    expected = ["acquire", "heartbeat", "acquire"]
    if sig == "cancel":
        expected.insert(2, "release")
    assert client.kinds() == expected
    assert worker.interrupted


async def test_failing_heartbeater_task_is_no_signal_and_work_completes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def boom(self: Any) -> None:
        raise RuntimeError("secret detail")

    monkeypatch.setattr("minion_executor.loop.Heartbeater.run", boom)
    client, s3, worker = FakeClient([assignment()]), FakeS3(), FakeWorker()
    _slow_worker(worker, 0.2)
    loop = make_loop(client, worker, s3, tmp_path, [])
    client.on_empty = loop.stop
    assert await loop.run() == 1
    assert client.kinds() == ["acquire", "complete", "acquire"]


async def test_stop_cancels_work_when_loop_task_is_cancelled(tmp_path: Path) -> None:
    client, worker = FakeClient([assignment()]), FakeWorker(block=True)
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], heartbeat_interval_s=100.0)
    task = asyncio.create_task(loop.run())
    await asyncio.sleep(0.2)
    task.cancel()
    worker.killed = True  # unblock the fake's thread
    with pytest.raises(asyncio.CancelledError):
        await task
    assert [t for t in asyncio.all_tasks() if "_execute" in repr(t)] == []


async def test_upload_refreshes_url_synchronously_when_expiring(tmp_path: Path) -> None:
    a = assignment()
    a.expires_at = datetime.now(UTC) + timedelta(seconds=5)  # inside the 60 s margin
    client, s3 = FakeClient([a]), FakeS3()
    client.hb_script = [hb_resp(url="https://s3.test/out?sig=2", minutes=10)]
    urls: list[str] = []
    orig = s3.handler

    def handler(req: httpx.Request) -> httpx.Response:
        if req.method == "PUT":
            urls.append(str(req.url))
        return orig(req)

    s3.handler = handler  # type: ignore[method-assign]
    loop = make_loop(
        client, FakeWorker(), s3, tmp_path, [],
        heartbeat_interval_s=100.0, heartbeat_sleep=gated_sleep(0, []),
    )  # fmt: skip
    client.on_empty = loop.stop
    assert await loop.run() == 1
    assert client.kinds() == ["acquire", "heartbeat", "complete", "acquire"]
    assert urls == ["https://s3.test/out?sig=2"]


async def test_pre_upload_refresh_cancel_releases_without_upload(tmp_path: Path) -> None:
    a = assignment()
    a.expires_at = datetime.now(UTC)
    client, s3 = FakeClient([a]), FakeS3()
    client.hb_script = [hb_resp(cancel=True)]
    worker = FakeWorker()
    loop = make_loop(
        client, worker, s3, tmp_path, [],
        heartbeat_interval_s=100.0, heartbeat_sleep=gated_sleep(0, []),
    )  # fmt: skip
    client.on_empty = loop.stop
    assert await loop.run() == 0
    assert client.kinds() == ["acquire", "heartbeat", "release", "acquire"]
    assert s3.puts == []
    assert not worker.interrupted  # work had finished: the warm worker is kept


async def test_pre_upload_beat_retries_once_after_429(tmp_path: Path) -> None:
    a = assignment()
    a.expires_at = datetime.now(UTC)
    client, s3 = FakeClient([a]), FakeS3()
    client.hb_script = [ApiError(429, "RATE_LIMITED", "x", retry_after=1.0)]
    delays: list[float] = []

    async def sleep(d: float) -> None:
        delays.append(d)
        if d != 1.0:  # the background beat never fires; only the retry wait returns
            await asyncio.Event().wait()

    loop = make_loop(
        client, FakeWorker(), s3, tmp_path, [],
        heartbeat_interval_s=100.0, heartbeat_sleep=sleep,
    )  # fmt: skip
    client.on_empty = loop.stop
    assert await loop.run() == 1
    assert client.kinds().count("heartbeat") == 2 and 1.0 in delays


async def test_refreshed_url_for_another_object_is_ignored(tmp_path: Path) -> None:
    client, s3 = FakeClient([assignment()]), FakeS3()
    client.hb_script = [hb_resp(url="https://evil.test/out", minutes=10)]
    urls: list[str] = []
    orig = s3.handler

    def handler(req: httpx.Request) -> httpx.Response:
        if req.method == "PUT":
            urls.append(str(req.url))
        return orig(req)

    s3.handler = handler  # type: ignore[method-assign]
    worker = FakeWorker()
    _slow_worker(worker, 0.3)
    loop = make_loop(
        client, worker, s3, tmp_path, [],
        heartbeat_interval_s=100.0, heartbeat_sleep=gated_sleep(1, []),
    )  # fmt: skip
    client.on_empty = loop.stop
    assert await loop.run() == 1
    assert urls == ["https://s3.test/out"]


async def test_cancel_during_output_write_leaves_no_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import time

    def slow_write(path: Path, outputs: list[Record]) -> tuple[int, str]:
        time.sleep(0.5)
        path.write_bytes(b"partial")  # lands after the cancel
        return 7, "d"

    monkeypatch.setattr("minion_executor.loop.io.write_jsonl", slow_write)
    client, s3 = FakeClient([assignment()]), FakeS3()
    client.hb_script = [hb_resp(cancel=True)]
    loop = make_loop(
        client, FakeWorker(), s3, tmp_path, [],
        heartbeat_interval_s=100.0, heartbeat_sleep=gated_sleep(1, []),
    )  # fmt: skip
    seen: list[list[Path]] = []

    async def nap(_: float) -> None:
        await asyncio.sleep(0.3)  # the writer thread has long finished by now
        seen.append(await asyncio.to_thread(lambda: list(tmp_path.glob("*/*"))))
        loop.stop()

    loop._sleep_fn = nap
    await loop.run()
    assert seen == [[]]
    assert "release" in client.kinds() and s3.puts == []
