"""Control-loop branches with a fake server client, fake S3 (MockTransport) and fake worker."""

import asyncio
import hashlib
import json
import os
import threading
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any

import httpx
import pytest
from minion_common.client import ApiError, StaleAcquirementError
from minion_common.enums import CpuPriority, Engine, PausedReason
from minion_common.schemas import (
    AcquireRequest,
    Assignment,
    Budget,
    CompleteRequest,
    FailRequest,
    HeartbeatRequest,
    HeartbeatResponse,
    ModelEntry,
    PresenceRequest,
)
from minion_executor.capacity.governor import Decision, ProfileSource
from minion_executor.capacity.runtime import FixedGovernor
from minion_executor.loop import ExecutorLoop, _release_code, cleanup_stale_tmp
from minion_executor.runners.base import Record, RunnerError
from minion_executor.worker.process import WorkerCrashed

BUDGET = Budget.model_validate(
    {"profile": "balanced", "memory_gb": 8, "gpu_duty_cycle": 0.5, "max_batch_size": 2}
)
MODEL = {
    "model_id": "m",
    "type": "embedding",
    "engine": "mlx",
    "ref": "o/m",
    "revision": "a" * 40,
    "memory_gb": 0.1,
}
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
        self.release_codes: list[str | None] = []
        self.complete_exc: Exception | None = None
        self.complete_excs: list[Exception] = []
        self.register_excs: list[Exception] = []
        self.on_empty: Any = None
        self.hb_script: list[HeartbeatResponse | Exception] = []
        self.acquire_reqs: list[AcquireRequest] = []
        self.presences: list[PresenceRequest] = []
        self.on_presence: Any = None
        self.presence_excs: list[Exception] = []
        self.registers: list[Any] = []

    async def presence(self, req: PresenceRequest) -> None:
        self.calls.append(("presence", req))
        self.presences.append(req)
        if self.on_presence:
            self.on_presence()
        if self.presence_excs:
            raise self.presence_excs.pop(0)

    async def register(self, req: Any) -> None:
        self.calls.append(("register", None))
        self.registers.append(req)
        if self.register_excs:
            raise self.register_excs.pop(0)

    async def acquire(self, req: AcquireRequest) -> Assignment | None:
        self.calls.append(("acquire", None))
        self.acquire_reqs.append(req)
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

    async def release(self, aid: uuid.UUID, req: Any = None) -> None:
        self.calls.append(("release", aid))
        self.release_codes.append(req.code if req is not None else None)

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
        self.params: list[Any] = []
        self.ensured: list[tuple[ModelEntry, Budget]] = []
        self.unloads = 0
        self.controls: list[tuple[float, bool, bool]] = []
        self.started = threading.Event()  # set when run() begins
        self.run_budgets: list[Budget | None] = []
        self.footprints: list[int | None] = []  # scripted samples (last one repeats)
        self.priority: bool | None = None
        self.interrupts = 0
        self.gate: threading.Event | None = None  # run() waits here (a batch in flight)
        self.run_hook: Callable[[int], None] | None = None  # worker thread, at each run() start
        self.footprint_fn: Callable[[], int | None] | None = None
        self.fp_calls = 0  # watcher samples taken (footprint + priority are sampled together)

    def ensure_model(
        self, model: ModelEntry, budget: Budget, model_path: Path | None = None
    ) -> None:
        self.ensured.append((model, budget))

    def unload(self) -> None:
        self.unloads += 1

    def control(self, *, duty: float, paused: bool, trim: bool = False) -> int:
        self.controls.append((duty, paused, trim))
        return len(self.controls)

    def run(
        self,
        batch: list[Record],
        params: Any = None,
        on_progress: Any = None,
        budget: Budget | None = None,
    ) -> list[Record]:
        self.started.set()
        self.run_budgets.append(budget)
        if self.run_hook is not None:
            self.run_hook(len(self.run_budgets))
        if self.gate is not None and not self.gate.wait(10):
            raise WorkerCrashed("gate never opened")
        self.params.append(params)
        if self.block:
            import time

            while not (self.killed or self.interrupted):
                time.sleep(0.01)
            raise WorkerCrashed("killed")
        if self.exc:
            raise self.exc
        self.batches.append(len(batch))
        if on_progress is not None:
            on_progress(len(batch))
        return [{"id": r["id"], "embedding": [1.0]} for r in batch]

    def interrupt(self) -> None:
        self.interrupted = True
        self.interrupts += 1

    def footprint_bytes(self) -> int | None:
        self.fp_calls += 1
        if self.footprint_fn is not None:
            return self.footprint_fn()
        if len(self.footprints) > 1:
            return self.footprints.pop(0)
        return self.footprints[0] if self.footprints else None

    def priority_ok(self) -> bool | None:
        return self.priority

    def gpu_share(self) -> float | None:
        return None

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
        sleep=kw.pop("sleep", rec),
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
    assert (req.retryable, req.code) == (True, "OOM")  # normalized to upper case
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
    assert client.release_codes == ["STOP"]
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


async def test_runner_release_error_releases_without_failing(tmp_path: Path) -> None:
    from minion_executor.runners.base import RunnerError

    class RelWorker(FakeWorker):
        def run(self, batch: list[Record], *_: Any) -> list[Record]:
            raise RunnerError("MODEL_INTEGRITY", "x", release=True)

    client = FakeClient([assignment()])
    loop = make_loop(client, RelWorker(), FakeS3(), tmp_path, [])
    client.on_empty = loop.stop
    await loop.run()
    assert client.kinds().count("release") == 1 and "fail" not in client.kinds()
    assert client.release_codes == ["MODEL_INTEGRITY"]


async def test_runner_release_with_invalid_code_uses_fallback_and_loop_survives(
    tmp_path: Path,
) -> None:
    from minion_executor.runners.base import RunnerError

    class RelWorker(FakeWorker):
        def run(self, batch: list[Record], *_: Any) -> list[Record]:
            raise RunnerError("runner-error!", "x", release=True)

    client = FakeClient([assignment(), assignment()])
    loop = make_loop(client, RelWorker(), FakeS3(), tmp_path, [])
    client.on_empty = loop.stop
    await loop.run()
    assert client.release_codes == ["EXECUTOR_RELEASE", "EXECUTOR_RELEASE"]  # both handled


@pytest.mark.parametrize(
    ("raw", "sent"),
    [("runner_error", "RUNNER_ERROR"), ("oom-killed", "RUNNER_ERROR"), ("", "RUNNER_ERROR")],
)
async def test_runner_fail_code_is_normalized_not_a_validation_error(
    tmp_path: Path, raw: str, sent: str
) -> None:
    from minion_executor.runners.base import RunnerError

    class FailWorker(FakeWorker):
        def run(self, batch: list[Record], *_: Any) -> list[Record]:
            raise RunnerError(raw, "x")

    client = FakeClient([assignment(), assignment()])
    loop = make_loop(client, FailWorker(), FakeS3(), tmp_path, [])
    client.on_empty = loop.stop
    await loop.run()
    assert [c.code for k, c in client.calls if k == "fail"] == [sent, sent]


async def test_output_count_mismatch_fails_before_upload(tmp_path: Path) -> None:
    class ShortWorker(FakeWorker):
        def run(self, batch: list[Record], *_: Any) -> list[Record]:
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

        def ensure_model(
            self, model: ModelEntry, budget: Budget, model_path: Path | None = None
        ) -> None:
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

        def run(self, batch: list[Record], *_: Any) -> list[Record]:
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
    worker, cache = FakeWorker(block=True), _StubCache()
    sleeps: list[float] = []
    loop = make_loop(
        client, worker, s3, tmp_path, sleeps,
        heartbeat_interval_s=100.0, heartbeat_sleep=gated_sleep(1, []), model_cache=cache,
    )  # fmt: skip
    client.on_empty = loop.stop
    # the second assignment must run normally: the worker stops blocking once interrupted
    orig_run = worker.run

    def run(batch: list[Record], *_: Any) -> list[Record]:
        if worker.interrupted:
            worker.block = False
        return orig_run(batch)

    worker.run = run  # type: ignore[method-assign,assignment]
    assert await loop.run() == 1
    assert worker.interrupted and not worker.killed
    assert cache.released >= 1  # the interrupted worker no longer holds the pinned model
    assert client.kinds() == ["acquire", "heartbeat", "release", "acquire", "complete", "acquire"]
    assert client.release_codes == ["CANCELLED"]
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

    def slow(batch: list[Record], *_: Any) -> list[Record]:
        import time

        time.sleep(0.2)
        return orig(batch)

    worker.run = slow  # type: ignore[method-assign,assignment]
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

    def slow(batch: list[Record], *_: Any) -> list[Record]:
        import time

        time.sleep(0.3)
        return orig(batch)

    worker.run = slow  # type: ignore[method-assign,assignment]
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

    def slow(batch: list[Record], *_: Any) -> list[Record]:
        time.sleep(secs)
        return orig(batch)

    worker.run = slow  # type: ignore[method-assign,assignment]


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
    assert client.release_codes == ["CANCELLED"]
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
    assert client.release_codes == ["CANCELLED"]


def _asn(**over: Any) -> Assignment:
    return Assignment.model_validate({**assignment().model_dump(mode="json"), **over})


async def _run_one(
    tmp_path: Path, a: Assignment, worker: FakeWorker, s3: FakeS3 | None = None, **kw: Any
) -> tuple[FakeClient, FakeS3]:
    client, s3 = FakeClient([a]), s3 or FakeS3()
    loop = make_loop(client, worker, s3, tmp_path, [], **kw)
    client.on_empty = loop.stop
    await loop.run()
    return client, s3


async def test_params_are_parsed_and_forwarded_to_worker(tmp_path: Path) -> None:
    worker = FakeWorker()
    await _run_one(tmp_path, _asn(params={"normalize": False}), worker)
    assert worker.params and all(p.normalize is False for p in worker.params)


async def test_bad_params_fail_non_retryable_without_running(tmp_path: Path) -> None:
    worker = FakeWorker()
    client, _ = await _run_one(tmp_path, _asn(params={"trust_remote_code": True}), worker)
    req = next(r for k, r in client.calls if k == "fail")
    assert (req.retryable, req.code) == (False, "BAD_PARAMS")
    assert worker.batches == []


async def test_non_retryable_runner_error_fails_non_retryable(tmp_path: Path) -> None:
    worker = FakeWorker(RunnerError("MODEL_BROKEN", "x", retryable=False))
    client, _ = await _run_one(tmp_path, _asn(), worker)
    req = next(r for k, r in client.calls if k == "fail")
    assert (req.retryable, req.code) == (False, "MODEL_BROKEN")


async def test_outputs_stamped_with_model_and_revision(tmp_path: Path) -> None:
    _, s3 = await _run_one(tmp_path, _asn(), FakeWorker())
    lines = [json.loads(x) for x in s3.puts[0].splitlines()]
    assert len(lines) == 3
    assert all(x["model"] == "m" and x["model_revision"] == "a" * 40 for x in lines)


class TooLongWorker(FakeWorker):
    def run(
        self,
        batch: list[Record],
        params: Any = None,
        on_progress: Any = None,
        budget: Budget | None = None,
    ) -> list[Record]:
        return [
            {"id": r["id"], "error": "PROMPT_TOO_LONG"}
            if r["id"] == "b"
            else {"id": r["id"], "embedding": [1.0]}
            for r in batch
        ]


async def test_prompt_too_long_records_count_and_subtask_completes(tmp_path: Path) -> None:
    client, s3 = await _run_one(tmp_path, _asn(), TooLongWorker())
    assert "fail" not in client.kinds()
    req = next(r for k, r in client.calls if k == "complete")
    assert req.record_count == 3
    errs = [json.loads(x) for x in s3.puts[0].splitlines() if b"PROMPT_TOO_LONG" in x]
    assert [e["id"] for e in errs] == ["b"] and errs[0]["model"] == "m"


class TickingWorker(FakeWorker):
    def __init__(self) -> None:
        super().__init__()
        self.release = threading.Event()
        self.progressed = threading.Event()

    def run(
        self,
        batch: list[Record],
        params: Any = None,
        on_progress: Any = None,
        budget: Budget | None = None,
    ) -> list[Record]:
        on_progress(1)  # one record done mid-batch
        self.progressed.set()
        self.release.wait(10)  # held until a heartbeat has observed it
        return super().run(batch, params, None, budget)


async def test_progress_ticks_feed_heartbeat_progress(tmp_path: Path) -> None:
    worker = TickingWorker()
    client = FakeClient([_asn()])
    s3 = FakeS3()
    inner = gated_sleep(1, [])

    async def hb_sleep(d: float) -> None:
        await asyncio.to_thread(worker.progressed.wait, 10)  # first beat only after progress
        await inner(d)

    loop = make_loop(
        client,
        worker,
        s3,
        tmp_path,
        [],
        heartbeat_interval_s=1.0,
        heartbeat_sleep=hb_sleep,
    )
    client.on_empty = loop.stop
    orig = client.heartbeat

    async def hb(*a: Any, **k: Any) -> Any:
        try:
            return await orig(*a, **k)
        finally:
            worker.release.set()

    client.heartbeat = hb  # type: ignore[method-assign]
    await loop.run()
    beats = [r for k, r in client.calls if k == "heartbeat"]
    assert beats and beats[0].progress.records_done == 1


async def test_budget_duty_cycle_reaches_worker(tmp_path: Path) -> None:
    worker = FakeWorker()
    await _run_one(tmp_path, _asn(), worker)
    assert worker.ensured and worker.ensured[0][1].gpu_duty_cycle == BUDGET.gpu_duty_cycle


async def test_engine_not_advertised_or_too_big_is_always_released(tmp_path: Path) -> None:
    worker = FakeWorker()
    a = _asn(model={**MODEL, "engine": "ollama", "ref": "o/m:t", "revision": "b" * 64})
    client, _ = await _run_one(tmp_path, a, worker)
    assert client.kinds().count("release") == 1 and "fail" not in client.kinds()
    assert worker.ensured == []
    big = _asn(model={**MODEL, "memory_gb": 64})
    client, _ = await _run_one(tmp_path, big, worker)
    assert client.kinds().count("release") == 1 and "fail" not in client.kinds()


async def test_engine_unavailable_runner_error_is_released_not_failed(tmp_path: Path) -> None:
    worker = FakeWorker(RunnerError("ENGINE_UNAVAILABLE", "mlx"))
    client, _ = await _run_one(tmp_path, _asn(), worker)
    assert client.kinds().count("release") == 1 and "fail" not in client.kinds()


async def test_idle_unload_also_while_acquire_keeps_failing(tmp_path: Path) -> None:
    now = [0.0]
    worker = FakeWorker()
    client = FakeClient([httpx.ConnectError("down")] * 4)
    loop = make_loop(
        client, worker, FakeS3(), tmp_path, [], idle_unload_s=60.0, clock=lambda: now[0]
    )
    orig = client.acquire

    async def acquire(req: AcquireRequest) -> Assignment | None:
        now[0] += 40.0
        if len(client.calls) >= 4:
            loop.stop()
        return await orig(req)

    client.acquire = acquire  # type: ignore[method-assign]
    await loop.run()
    assert worker.unloads == 1


async def test_idle_worker_unloaded_once_after_idle_period(tmp_path: Path) -> None:
    now = [0.0]
    worker, client = FakeWorker(), FakeClient([])
    loop = make_loop(
        client, worker, FakeS3(), tmp_path, [], idle_unload_s=60.0, clock=lambda: now[0]
    )
    polls = []

    def tick() -> None:
        polls.append(now[0])
        now[0] += 40.0
        if len(polls) == 5:
            loop.stop()

    client.on_empty = tick
    await loop.run()
    assert worker.unloads == 1  # polls at 0 and 40 do not unload; 80 does; no repeat


async def test_idle_unload_disabled_with_zero(tmp_path: Path) -> None:
    now = [0.0]
    worker, client = FakeWorker(), FakeClient([])
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], idle_unload_s=0, clock=lambda: now[0])

    def tick() -> None:
        now[0] += 1e6
        loop.stop()

    client.on_empty = tick
    await loop.run()
    assert worker.unloads == 0


class _StubCache:
    def __init__(self, exc: Exception | None = None, path: Path | None = None) -> None:
        self.exc, self.path = exc, path
        self.ensures = 0
        self.released = 0
        self.in_use: Path | None = None

    async def ensure(self, model: ModelEntry) -> Path | None:
        self.ensures += 1
        if self.exc:
            raise self.exc
        return self.path

    def release_in_use(self) -> None:
        self.released += 1


async def test_ensure_model_runner_error_unpins_the_cache(tmp_path: Path) -> None:
    class W(FakeWorker):
        def ensure_model(
            self, model: ModelEntry, budget: Budget, model_path: Path | None = None
        ) -> None:
            raise RunnerError("LOAD_FAILED", "x", retryable=False)

    cache = _StubCache(path=tmp_path / "snap")
    client, _ = await _run_one(tmp_path, _asn(), W(), model_cache=cache)
    assert cache.released >= 1
    assert next(r for k, r in client.calls if k == "fail").code == "LOAD_FAILED"


async def test_model_path_from_cache_reaches_worker(tmp_path: Path) -> None:
    seen: list[Path | None] = []

    class W(FakeWorker):
        def ensure_model(
            self, model: ModelEntry, budget: Budget, model_path: Path | None = None
        ) -> None:
            seen.append(model_path)

    cache = _StubCache(path=tmp_path / "snap")
    client, _ = await _run_one(tmp_path, _asn(), W(), model_cache=cache)
    assert seen == [tmp_path / "snap"] and "complete" in client.kinds()


@pytest.mark.parametrize(
    ("code", "retryable", "release"),
    [
        ("MODEL_DOWNLOAD_FAILED", True, True),
        ("MODEL_DOWNLOAD_FAILED", True, False),
        ("MODEL_INTEGRITY", True, False),
        ("MODEL_CACHE_FULL", True, False),
        ("MODEL_UNSUPPORTED", False, False),
    ],
)
async def test_model_fetch_errors_map_to_release_or_fail(
    tmp_path: Path, code: str, retryable: bool, release: bool
) -> None:
    from minion_executor.model_cache import ModelFetchError

    worker = FakeWorker()
    cache = _StubCache(ModelFetchError(code, "x", retryable=retryable, release=release))
    client, _ = await _run_one(tmp_path, _asn(), worker, model_cache=cache)
    assert worker.ensured == []
    if release:
        assert client.kinds().count("release") == 1 and "fail" not in client.kinds()
    else:
        req = next(r for k, r in client.calls if k == "fail")
        assert (req.code, req.retryable) == (code, retryable)


async def test_cache_full_by_loaded_model_unloads_worker_and_retries_once(tmp_path: Path) -> None:
    from minion_executor.model_cache import MODEL_CACHE_FULL, ModelFetchError

    events: list[str] = []

    class W(FakeWorker):
        def unload(self) -> None:
            events.append("unload")
            super().unload()

    class Cache(_StubCache):
        async def ensure(self, model: ModelEntry) -> Path | None:
            events.append("ensure")
            if self.ensures == 0:
                self.ensures += 1
                raise ModelFetchError(MODEL_CACHE_FULL, "x", blocked_by_in_use=True)
            self.ensures += 1
            return self.path

        def release_in_use(self) -> None:
            events.append("release")

    worker = W()
    cache = Cache(path=tmp_path / "snap")
    client, _ = await _run_one(tmp_path, _asn(), worker, model_cache=cache)
    assert events == ["ensure", "unload", "release", "ensure"]
    assert "complete" in client.kinds() and "fail" not in client.kinds()


async def test_cache_full_that_unloading_cannot_fix_fails_without_looping(
    tmp_path: Path,
) -> None:
    from minion_executor.model_cache import MODEL_CACHE_FULL, ModelFetchError

    worker = FakeWorker()
    # too large for the cap regardless: not flagged, so no unload and a single ensure
    cache = _StubCache(ModelFetchError(MODEL_CACHE_FULL, "x"))
    client, _ = await _run_one(tmp_path, _asn(), worker, model_cache=cache)
    assert cache.ensures == 1 and worker.unloads == 0
    assert next(r for k, r in client.calls if k == "fail").code == MODEL_CACHE_FULL

    # flagged on both attempts: retried exactly once, then reported
    worker2 = FakeWorker()
    cache2 = _StubCache(ModelFetchError(MODEL_CACHE_FULL, "x", blocked_by_in_use=True))
    client2, _ = await _run_one(tmp_path, _asn(), worker2, model_cache=cache2)
    assert cache2.ensures == 2 and worker2.unloads == 1
    assert next(r for k, r in client2.calls if k == "fail").code == MODEL_CACHE_FULL


async def test_stop_during_ensure_cancels_it_releases_once_and_reports_stopped(
    tmp_path: Path,
) -> None:
    from minion_executor.loop import _Outcome

    entered, cancelled = asyncio.Event(), asyncio.Event()

    class HangingCache(_StubCache):
        async def ensure(self, model: ModelEntry) -> Path | None:
            self.ensures += 1
            entered.set()
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                cancelled.set()
                raise
            return None

    worker, cache = FakeWorker(), HangingCache()
    client = FakeClient([assignment()])
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], model_cache=cache)
    outcomes: list[Any] = []
    handle = loop._handle

    async def spy(a: Assignment, tmp: Path) -> Any:
        outcomes.append(await handle(a, tmp))
        return outcomes[-1]

    loop._handle = spy  # type: ignore[method-assign]
    task = asyncio.create_task(loop.run())
    await asyncio.wait_for(entered.wait(), 5)
    loop.stop()
    assert await asyncio.wait_for(task, 5) == 0
    assert cancelled.is_set()
    assert outcomes == [_Outcome.STOPPED]
    assert client.kinds().count("release") == 1 and "fail" not in client.kinds()
    assert cache.released == 1  # pin dropped together with the killed worker
    assert worker.ensured == []


async def test_local_oserror_from_ensure_is_released_as_local_io_error(tmp_path: Path) -> None:
    worker = FakeWorker()
    cache = _StubCache(PermissionError("cache dir"))
    client, _ = await _run_one(tmp_path, _asn(), worker, model_cache=cache)
    assert client.kinds().count("release") == 1 and "fail" not in client.kinds()
    assert worker.ensured == []


async def test_idle_unload_releases_the_cache_pin(tmp_path: Path) -> None:
    now = [0.0]
    worker, client = FakeWorker(), FakeClient([])
    cache = _StubCache()
    cache.in_use = tmp_path / "m"
    loop = make_loop(
        client, worker, FakeS3(), tmp_path, [], idle_unload_s=60, clock=lambda: now[0],
        model_cache=cache,
    )  # fmt: skip
    loop._loaded_path = cache.in_use  # a warm model is pinned

    def tick() -> None:
        now[0] += 100
        if worker.unloads:
            loop.stop()

    client.on_empty = tick
    await loop.run()
    assert worker.unloads == 1 and cache.released == 1


async def test_uncached_engine_assignment_drops_the_stale_pin(tmp_path: Path) -> None:
    cache = _StubCache(path=None)  # Ollama / fake: the cache manages nothing
    await _run_one(tmp_path, _asn(), FakeWorker(), model_cache=cache)
    assert cache.released >= 1


async def test_worker_crash_drops_the_cache_pin(tmp_path: Path) -> None:
    cache = _StubCache(path=tmp_path / "snap")
    client, _ = await _run_one(
        tmp_path, _asn(), FakeWorker(exc=WorkerCrashed("boom")), model_cache=cache
    )
    assert cache.released == 1
    assert next(r for k, r in client.calls if k == "fail").code == "WORKER_CRASHED"


async def test_sandbox_failure_is_released_not_burned(tmp_path: Path) -> None:
    from minion_executor.worker.sandbox import SANDBOX_FAILED

    client, _ = await _run_one(
        tmp_path, _asn(), FakeWorker(exc=RunnerError(SANDBOX_FAILED, "x", retryable=True))
    )
    assert client.kinds().count("release") == 1 and "fail" not in client.kinds()


# --- FR-17 graceful stop (TASK-5.2) ---


class SlowWorker(FakeWorker):
    """Finishes a batch after `delay_s`, unless killed/interrupted first."""

    def __init__(self, delay_s: float) -> None:
        super().__init__()
        self.delay_s = delay_s

    def run(
        self,
        batch: list[Record],
        params: Any = None,
        on_progress: Any = None,
        budget: Budget | None = None,
    ) -> list[Record]:
        import time

        end = time.monotonic() + self.delay_s
        while time.monotonic() < end:
            if self.killed or self.interrupted:
                raise WorkerCrashed("killed")
            time.sleep(0.01)
        return super().run(batch, params, on_progress)


async def _until_running(worker: FakeWorker) -> None:
    assert await asyncio.to_thread(worker.started.wait, 5)


async def test_stop_within_grace_completes_inflight_subtask(tmp_path: Path) -> None:
    client, s3, worker = FakeClient([assignment(), assignment()]), FakeS3(), SlowWorker(0.4)
    loop = make_loop(client, worker, s3, tmp_path, [], stop_grace_s=10.0)
    task = asyncio.create_task(loop.run())
    await _until_running(worker)
    loop.stop()
    assert await asyncio.wait_for(task, 5) == 1
    assert client.kinds() == ["acquire", "complete"]  # no second acquire, no release
    assert len(s3.puts) == 1
    assert worker.killed and worker.closed  # freed before complete, not left warm
    assert not leftover(tmp_path)


async def test_stop_past_grace_kills_worker_and_releases_at_deadline(tmp_path: Path) -> None:
    client, s3, worker = FakeClient([assignment()]), FakeS3(), FakeWorker(block=True)
    loop = make_loop(client, worker, s3, tmp_path, [], stop_grace_s=0.5)
    task = asyncio.create_task(loop.run())
    await _until_running(worker)
    t0 = asyncio.get_running_loop().time()
    loop.stop()
    assert await asyncio.wait_for(task, 5) == 0
    elapsed = asyncio.get_running_loop().time() - t0
    assert 0.4 <= elapsed < 3.0  # waited out the grace, then released
    assert client.kinds() == ["acquire", "release"]
    assert worker.killed and s3.puts == []
    assert not leftover(tmp_path)


async def test_second_stop_signal_kills_and_releases_immediately(tmp_path: Path) -> None:
    client, worker = FakeClient([assignment()]), FakeWorker(block=True)
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], stop_grace_s=60.0)
    task = asyncio.create_task(loop.run())
    await _until_running(worker)
    loop.stop()
    assert not task.done()  # still inside the grace period
    loop.stop()
    assert await asyncio.wait_for(task, 5) == 0
    assert client.kinds() == ["acquire", "release"]
    assert worker.killed


async def test_grace_zero_is_immediate(tmp_path: Path) -> None:
    client, worker = FakeClient([assignment()]), FakeWorker(block=True)
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], stop_grace_s=0.0)
    task = asyncio.create_task(loop.run())
    await _until_running(worker)
    loop.stop()
    assert await asyncio.wait_for(task, 5) == 0
    assert client.kinds() == ["acquire", "release"] and worker.killed


async def test_heartbeats_continue_and_cancel_interrupts_during_grace(tmp_path: Path) -> None:
    client, worker = FakeClient([assignment()]), FakeWorker(block=True)
    client.hb_script = [hb_resp(), hb_resp(), hb_resp(cancel=True)]

    async def fast(_: float) -> None:
        await asyncio.sleep(0.15)

    loop = make_loop(
        client, worker, FakeS3(), tmp_path, [],
        stop_grace_s=60.0, heartbeat_interval_s=100.0, heartbeat_sleep=fast,
    )  # fmt: skip
    task = asyncio.create_task(loop.run())
    await _until_running(worker)
    loop.stop()
    assert await asyncio.wait_for(task, 5) == 0  # the cancel ended it long before the grace
    kinds = client.kinds()
    assert kinds.count("heartbeat") == 3  # beats kept flowing after the stop request
    assert kinds[-1] == "release" and worker.interrupted


async def test_stop_during_complete_retry_waits_for_deadline_not_signal(tmp_path: Path) -> None:
    client = FakeClient([assignment()])
    client.complete_excs = [httpx.ConnectError("x"), httpx.ConnectError("x")]
    worker = SlowWorker(0.2)
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], stop_grace_s=30.0, sleep=None,
                     backoff=(0.05, 0.1))  # fmt: skip
    task = asyncio.create_task(loop.run())
    await _until_running(worker)
    loop.stop()  # soft stop: the retry back-off must wait for the deadline, not the signal
    assert await asyncio.wait_for(task, 5) == 1
    assert client.kinds() == ["acquire", "complete", "complete", "complete"]


async def test_acquire_in_flight_when_stop_arrives_is_released_untouched(tmp_path: Path) -> None:
    client, worker, s3 = FakeClient([assignment()]), FakeWorker(), FakeS3()
    loop = make_loop(client, worker, s3, tmp_path, [], stop_grace_s=10.0)
    orig = client.acquire

    async def acquire_then_stop(req: AcquireRequest) -> Assignment | None:
        got = await orig(req)
        loop.stop()  # the stop lands while the acquire is in flight
        return got

    client.acquire = acquire_then_stop  # type: ignore[method-assign]
    assert await asyncio.wait_for(loop.run(), 5) == 0
    assert client.kinds() == ["acquire", "release"]
    assert s3.ranges == [] and s3.puts == []  # no download
    assert worker.ensured == [] and not worker.started.is_set()


async def test_hard_stop_during_complete_backoff_releases_at_grace(tmp_path: Path) -> None:
    client = FakeClient([assignment()])
    client.complete_exc = httpx.ConnectError("down")
    worker = SlowWorker(0.05)
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], stop_grace_s=0.3, sleep=None,
                     backoff=(0.05, 0.1), complete_retries=1000)  # fmt: skip
    task = asyncio.create_task(loop.run())
    await _until_running(worker)
    t0 = asyncio.get_running_loop().time()
    loop.stop()
    assert await asyncio.wait_for(task, 5) == 0
    elapsed = asyncio.get_running_loop().time() - t0
    assert 0.2 <= elapsed < 1.5
    assert client.kinds()[-1] == "release" and "complete" in client.kinds()
    assert client.release_codes == ["STOP"]


class SlowServerClient(FakeClient):
    """complete/release hang (a slow server)."""

    async def complete(self, aid: uuid.UUID, req: CompleteRequest) -> None:
        self.calls.append(("complete", req))
        await asyncio.sleep(60)

    async def release(self, aid: uuid.UUID, req: Any = None) -> None:
        self.calls.append(("release", aid))
        self.release_codes.append(req.code if req is not None else None)
        await asyncio.sleep(60)


async def test_slow_server_after_hard_stop_is_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import minion_executor.loop as lp

    monkeypatch.setattr(lp, "SHUTDOWN_BUDGET_S", 0.3)
    client = SlowServerClient([assignment()])
    worker = SlowWorker(0.05)
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], stop_grace_s=0.3)
    task = asyncio.create_task(loop.run())
    await _until_running(worker)
    t0 = asyncio.get_running_loop().time()
    loop.stop()
    assert await asyncio.wait_for(task, 5) == 0  # never claims success
    elapsed = asyncio.get_running_loop().time() - t0
    assert elapsed < 0.3 + 0.3 + 2 * lp.MIN_CALL_S + 0.7  # grace + shared budget + floors
    assert client.kinds()[-2:] == ["complete", "release"]
    assert worker.killed and worker.closed


async def test_non_finite_grace_is_an_immediate_hard_stop(tmp_path: Path) -> None:
    for grace in (float("nan"), float("inf"), float("-inf")):
        loop = make_loop(FakeClient([]), FakeWorker(), FakeS3(), tmp_path, [], stop_grace_s=grace)
        loop.stop()
        assert loop._hard_stop.is_set()


async def test_work_finishing_during_kill_is_completed_not_released(tmp_path: Path) -> None:
    class KillSlowWorker(SlowWorker):
        def kill(self) -> None:
            import time

            time.sleep(0.4)  # the batch ends while the kill is in progress
            super().kill()

    client, worker = FakeClient([assignment()]), KillSlowWorker(0.3)
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], stop_grace_s=0.1)
    task = asyncio.create_task(loop.run())
    await _until_running(worker)
    loop.stop()
    assert await asyncio.wait_for(task, 5) == 1
    kinds = client.kinds()
    assert kinds[-1] == "complete" and "release" not in kinds


async def test_shutdown_budget_is_shared_across_complete_release_and_close(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import time

    import minion_executor.loop as lp

    monkeypatch.setattr(lp, "SHUTDOWN_BUDGET_S", 1.0)
    monkeypatch.setattr(lp, "MIN_CALL_S", 0.1)

    class SlowCloseWorker(SlowWorker):
        def close(self) -> None:
            time.sleep(2.5)
            super().close()

    client, worker = SlowServerClient([assignment()]), SlowCloseWorker(0.05)
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], stop_grace_s=0.2)
    task = asyncio.create_task(loop.run())
    await _until_running(worker)
    t0 = asyncio.get_running_loop().time()
    loop.stop()
    assert await asyncio.wait_for(task, 10) == 0
    elapsed = asyncio.get_running_loop().time() - t0
    # per-call timeouts would add up to ~0.2 + 3 x 1.0; one shared budget stays near 0.2 + 1.0
    assert elapsed < 0.2 + 1.0 + 4 * 0.1 + 0.6
    assert client.kinds()[-2:] == ["complete", "release"] and worker.killed


async def test_complete_in_flight_at_grace_expiry_gets_budget_after_hard_stop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The shared budget starts at the hard stop, not at the first bounded call during grace."""
    import minion_executor.loop as lp

    monkeypatch.setattr(lp, "SHUTDOWN_BUDGET_S", 1.0)
    monkeypatch.setattr(lp, "MIN_CALL_S", 0.1)

    class SlowCompleteClient(FakeClient):
        async def complete(self, aid: uuid.UUID, req: CompleteRequest) -> None:
            self.calls.append(("complete", req))
            await asyncio.sleep(1.5)  # outlives grace (1.0) but fits in hard stop + budget

    client, worker = SlowCompleteClient([assignment()]), SlowWorker(0.3)
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], stop_grace_s=1.0)
    task = asyncio.create_task(loop.run())
    await _until_running(worker)
    loop.stop()  # work finishes at ~0.3 inside grace; its kill must not start the budget
    assert await asyncio.wait_for(task, 10) == 1
    assert client.kinds()[-1] == "complete" and "release" not in client.kinds()


async def test_acquire_in_flight_at_stop_is_abandoned_within_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import minion_executor.loop as lp

    monkeypatch.setattr(lp, "SHUTDOWN_BUDGET_S", 0.3)
    client, worker = FakeClient([]), FakeWorker()
    entered = asyncio.Event()

    async def hang(req: AcquireRequest) -> Assignment | None:
        client.calls.append(("acquire", None))
        entered.set()
        await asyncio.sleep(60)
        return None

    client.acquire = hang  # type: ignore[method-assign]
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], stop_grace_s=0.0)
    task = asyncio.create_task(loop.run())
    await asyncio.wait_for(entered.wait(), 5)
    t0 = asyncio.get_running_loop().time()
    loop.stop()
    assert await asyncio.wait_for(task, 5) == 0
    assert asyncio.get_running_loop().time() - t0 < 1.5
    assert client.kinds() == ["acquire"] and worker.closed


async def test_register_in_flight_at_stop_is_abandoned_within_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import minion_executor.loop as lp

    monkeypatch.setattr(lp, "SHUTDOWN_BUDGET_S", 0.3)
    client, worker = FakeClient([]), FakeWorker()
    entered = asyncio.Event()

    async def hang(req: Any) -> None:
        client.calls.append(("register", None))
        entered.set()
        await asyncio.sleep(60)

    client.register = hang  # type: ignore[method-assign]
    loop = make_loop(
        client,
        worker,
        FakeS3(),
        tmp_path,
        [],
        stop_grace_s=0.0,
        register=object(),
    )
    task = asyncio.create_task(loop.run())
    await asyncio.wait_for(entered.wait(), 5)
    t0 = asyncio.get_running_loop().time()
    loop.stop()
    assert await asyncio.wait_for(task, 5) == 0
    assert asyncio.get_running_loop().time() - t0 < 1.5
    assert client.kinds() == ["register"] and worker.closed


async def test_second_sigterm_during_blocking_probe_exits_cleanly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import os
    import signal

    import minion_executor.main as m
    from minion_common.enums import CapacityProfile
    from pydantic import SecretStr

    monkeypatch.setattr(m, "restrict_environ", lambda: None)
    started = threading.Event()

    def blocking_probe(requested: list[Engine], sandbox: object = None) -> list[Engine]:
        started.set()
        if not m._probe_cancelled.wait(10):  # only a cancel (second signal) ends it
            raise AssertionError("probe was never cancelled")
        raise WorkerCrashed("probe killed")

    monkeypatch.setattr(m, "probe_available", blocking_probe)
    task = asyncio.create_task(
        m._run("http://localhost:8080", CapacityProfile.LIGHT, [Engine.MLX], SecretStr("t"), BUDGET)
    )
    assert await asyncio.to_thread(started.wait, 5)
    os.kill(os.getpid(), signal.SIGTERM)
    await asyncio.sleep(0.2)
    assert not task.done()  # one signal only asks for a stop; the probe is still running
    os.kill(os.getpid(), signal.SIGTERM)
    await asyncio.wait_for(task, 5)  # returns (exit 0) promptly
    m._probe_cancelled.clear()


# --- governor in the loop (TASK-5.5) ---------------------------------------------------------


def dec(
    budget: Budget = BUDGET,
    reason: PausedReason | None = None,
    **kw: Any,
) -> Decision:
    fields: dict[str, Any] = {
        "profile": budget.profile,
        "budget": budget,
        "paused_reason": reason,
        "release_now": False,
        "pause_expired": False,
        "trim": False,
        "profile_source": ProfileSource.PINNED,
        "candidate": None,
        "candidate_since": None,
    }
    fields.update(kw)
    return Decision(**fields)


async def until(pred: Callable[[], bool], timeout_s: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while not pred():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.01)


async def sampled(worker: FakeWorker, n: int = 3) -> None:
    """Wait until the watcher took `n` more guard samples (it evaluated the latest decision)."""
    start = worker.fp_calls
    await until(lambda: worker.fp_calls >= start + n)


def small(memory_gb: float, **kw: Any) -> Budget:
    return BUDGET.model_copy(update={"memory_gb": memory_gb, **kw})


async def test_paused_never_acquires_and_sends_presence(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    gov.update(dec(reason=PausedReason.THERMAL))
    client = FakeClient([assignment()])
    loop = make_loop(
        client, FakeWorker(), FakeS3(), tmp_path, [], governor=gov, presence_interval_s=0.05
    )

    def after_three() -> None:
        if len(client.presences) == 3:
            loop.stop()

    client.on_presence = after_three
    assert await asyncio.wait_for(loop.run(), 5) == 0
    assert "acquire" not in client.kinds()  # paused: presence only
    assert len(client.presences) == 3  # on entering the pause, then every interval
    assert {p.paused_reason for p in client.presences} == {"thermal"}
    assert client.presences[0].budget == BUDGET


async def test_presence_on_pause_reason_change_not_only_on_interval(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    gov.update(dec(reason=PausedReason.THERMAL))
    client = FakeClient([])
    loop = make_loop(
        client, FakeWorker(), FakeS3(), tmp_path, [], governor=gov, presence_interval_s=100.0
    )

    def on_presence() -> None:
        n = len(client.presences)
        if n == 1:
            gov.update(dec(reason=PausedReason.MEMORY_PRESSURE))
        elif n == 2:
            gov.update(dec(small(4.0, profile="light"), reason=PausedReason.MEMORY_PRESSURE))
        else:
            loop.stop()

    client.on_presence = on_presence
    await asyncio.wait_for(loop.run(), 5)
    assert [p.paused_reason for p in client.presences] == [
        "thermal",
        "memory_pressure",
        "memory_pressure",
    ]
    assert client.presences[2].budget.memory_gb == 4.0  # a profile/budget change is reported too


async def test_resume_acquires_again_with_the_live_budget(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    gov.update(dec(reason=PausedReason.USER_PAUSED))
    client = FakeClient([assignment()])
    loop = make_loop(
        client, FakeWorker(), FakeS3(), tmp_path, [], governor=gov, presence_interval_s=100.0
    )
    held = small(3.0, gpu_duty_cycle=0.25)  # a hold: below the profile's nominal budget

    def on_presence() -> None:
        gov.update(dec(held))

    client.on_presence = on_presence
    assert await asyncio.wait_for(loop.run(max_subtasks=1), 5) == 1
    # the governor's decision.budget, never budget_for(profile) or the constructor budget
    assert client.acquire_reqs[0].budget == held
    assert client.kinds().index("presence") < client.kinds().index("acquire")


async def test_live_budget_reaches_admission_worker_and_heartbeat(tmp_path: Path) -> None:
    held = small(0.5, max_batch_size=1, gpu_duty_cycle=0.25)
    gov = FixedGovernor(held)
    client, worker = FakeClient([assignment()]), FakeWorker()
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], governor=gov, heartbeat_interval_s=1.0)
    assert await loop.run(max_subtasks=1) == 1
    assert worker.ensured[0][1] == held
    assert worker.batches == [1, 1, 1]  # re-sliced with the live max_batch_size
    assert worker.run_budgets == [held, held, held]


async def test_model_over_live_budget_is_released_not_run(tmp_path: Path) -> None:
    gov = FixedGovernor(small(0.05))  # the model needs 0.1
    client = FakeClient([assignment()])
    loop = make_loop(client, FakeWorker(), FakeS3(), tmp_path, [], governor=gov)
    client.on_empty = loop.stop
    await loop.run()
    assert "release" in client.kinds() and "complete" not in client.kinds()


async def test_heartbeat_carries_live_budget_and_paused_reason(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    client, worker = FakeClient([assignment()]), FakeWorker(block=True)
    delays: list[float] = []
    loop = make_loop(
        client,
        worker,
        FakeS3(),
        tmp_path,
        [],
        governor=gov,
        heartbeat_interval_s=1.0,
        heartbeat_sleep=gated_sleep(2, delays),
    )
    changed = small(4.0, profile="light")

    async def flip() -> None:
        await until(lambda: "heartbeat" in client.kinds())
        gov.update(dec(changed, reason=PausedReason.LOW_POWER_MODE))
        await until(lambda: client.kinds().count("heartbeat") >= 2)
        loop.stop()

    task = asyncio.create_task(flip())
    await asyncio.wait_for(loop.run(), 10)
    await task
    beats = [r for k, r in client.calls if k == "heartbeat"]
    assert beats[0].budget == BUDGET and beats[0].paused_reason is None
    assert beats[1].budget == changed and beats[1].paused_reason == "low_power_mode"


async def test_idle_worker_above_new_budget_is_unloaded_at_once(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    client, worker = FakeClient([assignment()]), FakeWorker()
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], governor=gov, idle_unload_s=0)

    def shrink() -> None:
        if worker.unloads == 0 and not hasattr(shrink, "done"):
            shrink.done = True  # type: ignore[attr-defined]
            gov.update(dec(small(0.05)))  # below the loaded model's 0.1
        else:
            loop.stop()

    client.on_empty = shrink
    assert await loop.run() == 1
    assert worker.unloads == 1  # idle_unload_s=0 disables idle unload: the budget did it


async def test_idle_sleep_wakes_on_pause_and_unloads_under_pressure(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    client, worker = FakeClient([assignment()]), FakeWorker()
    loop = make_loop(
        client,
        worker,
        FakeS3(),
        tmp_path,
        [],
        sleep=None,
        backoff=(30.0, 30.0),
        governor=gov,
        idle_unload_s=0,
        presence_interval_s=100.0,
    )

    async def pressure() -> None:
        await until(lambda: "complete" in client.kinds())
        await until(lambda: client.kinds().count("acquire") >= 2)  # now in the 15-30 s idle sleep
        gov.update(dec(reason=PausedReason.MEMORY_PRESSURE, trim=True))

    client.on_presence = loop.stop
    task = asyncio.create_task(pressure())
    t0 = asyncio.get_running_loop().time()
    assert await asyncio.wait_for(loop.run(), 10) == 1
    await task
    assert asyncio.get_running_loop().time() - t0 < 5  # not the full back-off
    assert client.presences[0].paused_reason == "memory_pressure"
    assert worker.unloads == 1  # memory pressure frees a warm worker without a subtask


class FakeCache:
    def __init__(self) -> None:
        self.unpins = 0
        self.in_use: Path | None = None

    async def ensure(self, model: ModelEntry) -> None:
        return None

    def release_in_use(self) -> None:
        self.unpins += 1


async def test_pause_and_duty_reach_the_worker_at_once_and_resume_continues(
    tmp_path: Path,
) -> None:
    gov = FixedGovernor(BUDGET)
    client, worker = FakeClient([assignment()]), FakeWorker()
    worker.gate = threading.Event()
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], governor=gov)
    task = asyncio.create_task(loop.run(max_subtasks=1))
    await until(lambda: worker.started.is_set())
    slow = small(8.0, gpu_duty_cycle=0.2)
    gov.update(dec(slow, reason=PausedReason.THERMAL))
    await until(lambda: (0.2, True, False) in worker.controls)  # pushed, not waiting for a batch
    gov.update(dec(slow, trim=True))  # resumed (hysteresis done), memory-pressure warning trim
    await until(lambda: (0.2, False, True) in worker.controls)
    worker.gate.set()
    assert await asyncio.wait_for(task, 10) == 1  # the same attempt completed
    assert "release" not in client.kinds() and worker.interrupts == 0


@pytest.mark.parametrize(
    ("flags", "label"),
    [
        ({"pause_expired": True}, "PAUSE_RELEASE"),
        ({"release_now": True, "trim": True}, "MEMORY_CRITICAL_RELEASE"),
    ],
)
async def test_pause_expired_or_critical_interrupts_and_releases(
    tmp_path: Path, flags: dict[str, Any], label: str
) -> None:
    gov = FixedGovernor(BUDGET)
    client, worker, sleeps = FakeClient([assignment()]), FakeWorker(block=True), list[float]()
    cache = FakeCache()
    loop = make_loop(
        client,
        worker,
        FakeS3(),
        tmp_path,
        sleeps,
        governor=gov,
        model_cache=cache,
    )
    task = asyncio.create_task(loop.run())
    await until(lambda: worker.started.is_set())
    gov.update(dec(reason=PausedReason.MEMORY_PRESSURE, **flags))
    await until(lambda: "release" in client.kinds())
    loop.stop()
    await asyncio.wait_for(task, 10)
    kinds = client.kinds()
    assert worker.interrupts == 1 and "complete" not in kinds and "fail" not in kinds
    assert client.release_codes == [label]
    assert cache.unpins >= 2  # the killed worker no longer pins the model
    assert sleeps == []  # released, not failed: no failure back-off


async def test_released_by_governor_has_no_failure_backoff(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    client, worker, sleeps = FakeClient([assignment()]), FakeWorker(block=True), list[float]()
    loop = make_loop(client, worker, FakeS3(), tmp_path, sleeps, governor=gov)
    task = asyncio.create_task(loop.run())
    await until(lambda: worker.started.is_set())
    gov.update(dec(reason=PausedReason.THERMAL, pause_expired=True))
    await until(lambda: len(client.presences) >= 1)  # released, then paused: presence, no acquire
    loop.stop()
    await asyncio.wait_for(task, 10)
    assert sleeps == [] and client.kinds().count("acquire") == 1


async def test_downgrade_finishes_the_batch_then_releases_and_unloads(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    client, worker, cache = FakeClient([assignment()]), FakeWorker(), FakeCache()
    worker.gate = threading.Event()
    loop = make_loop(
        client,
        worker,
        FakeS3(),
        tmp_path,
        [],
        governor=gov,
        model_cache=cache,
        guard_interval_s=0.02,
    )
    task = asyncio.create_task(loop.run())
    await until(lambda: worker.started.is_set())
    gov.update(dec(small(0.05)))  # model needs 0.1: the running batch may finish ...
    await sampled(worker)  # the watcher looked at it and let the batch run
    assert worker.interrupts == 0 and "release" not in client.kinds()
    worker.gate.set()
    await until(lambda: "release" in client.kinds())
    loop.stop()
    await asyncio.wait_for(task, 10)
    assert worker.batches == [2]  # ... and no further batch starts
    assert client.release_codes == ["BUDGET_DOWNGRADE"]
    assert worker.unloads >= 1 and worker.interrupts == 0  # graceful unload, not a kill
    assert "complete" not in client.kinds() and "fail" not in client.kinds()
    assert cache.unpins >= 2


async def test_downgrade_overrunning_the_bound_interrupts(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    client, worker = FakeClient([assignment()]), FakeWorker(block=True)
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], governor=gov, downgrade_batch_s=0.1)
    task = asyncio.create_task(loop.run())
    await until(lambda: worker.started.is_set())
    gov.update(dec(small(0.05)))
    await until(lambda: "release" in client.kinds())
    loop.stop()
    await asyncio.wait_for(task, 10)
    assert worker.interrupts == 1 and "fail" not in client.kinds()
    assert client.release_codes == ["BUDGET_DOWNGRADE"]


async def test_budget_drop_during_model_fetch_never_loads(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)

    class SlowCache(FakeCache):
        async def ensure(self, model: ModelEntry) -> None:
            gov.update(dec(small(0.05)))
            return None

    client, worker = FakeClient([assignment()]), FakeWorker()
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], governor=gov, model_cache=SlowCache())
    client.on_empty = loop.stop
    await loop.run()
    assert worker.ensured == [] and "release" in client.kinds()


async def test_memory_guard_fails_retryable_oom_on_two_consecutive_samples(
    tmp_path: Path,
) -> None:
    gov = FixedGovernor(BUDGET)  # 8 GB: the limit is 8.8 GiB
    client, worker = FakeClient([assignment()]), FakeWorker(block=True)
    worker.footprints = [10 * 1024**3]
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], governor=gov, guard_interval_s=0.02)
    client.on_empty = loop.stop
    await asyncio.wait_for(loop.run(), 10)
    req = next(r for k, r in client.calls if k == "fail")
    assert (req.retryable, req.code) == (True, "OOM")
    assert worker.interrupts == 1 and "release" not in client.kinds()


async def test_memory_guard_ignores_a_single_spike(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    client, worker = FakeClient([assignment()]), FakeWorker()
    worker.gate = threading.Event()
    gib = 1024**3
    worker.footprints = [10 * gib, 2 * gib, 10 * gib, 2 * gib, 2 * gib]
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], governor=gov, guard_interval_s=0.02)
    task = asyncio.create_task(loop.run(max_subtasks=1))
    await until(lambda: len(worker.footprints) <= 1)  # all alternating samples were taken
    worker.gate.set()
    assert await asyncio.wait_for(task, 10) == 1
    assert "fail" not in client.kinds()


async def test_footprint_above_the_lowered_budget_is_legit_while_downgrading(
    tmp_path: Path,
) -> None:
    gov = FixedGovernor(BUDGET)
    client, worker = FakeClient([assignment()]), FakeWorker()
    worker.gate = threading.Event()
    worker.footprints = [3 * 1024**3]  # over 1.1 x the lowered 0.05 GB budget, legitimately
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], governor=gov, guard_interval_s=0.02)
    task = asyncio.create_task(loop.run())
    await until(lambda: worker.started.is_set())
    gov.update(dec(small(0.05)))
    await sampled(worker)  # measured against the budget the batch started with, not 0.05
    assert worker.interrupts == 0
    worker.gate.set()
    await until(lambda: "release" in client.kinds())
    loop.stop()
    await asyncio.wait_for(task, 10)
    assert "fail" not in client.kinds()  # released by the downgrade, not failed as OOM


async def test_priority_violation_after_grace_interrupts_and_releases(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    client, worker = FakeClient([assignment()]), FakeWorker(block=True)
    worker.priority = False
    loop = make_loop(
        client, worker, FakeS3(), tmp_path, [], governor=gov, guard_interval_s=0.02,
        priority_grace_s=0.0,
    )  # fmt: skip
    client.on_empty = loop.stop
    await asyncio.wait_for(loop.run(), 10)
    assert worker.interrupts == 1 and "release" in client.kinds()
    assert client.release_codes == ["PRIORITY_VIOLATION"]


async def test_priority_false_inside_the_grace_window_is_ignored(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    client, worker = FakeClient([assignment()]), FakeWorker()
    worker.gate = threading.Event()
    worker.priority = False  # a just-raised priority is applied at the worker's next batch
    loop = make_loop(
        client, worker, FakeS3(), tmp_path, [], governor=gov, guard_interval_s=0.02,
        priority_grace_s=30.0,
    )  # fmt: skip
    task = asyncio.create_task(loop.run(max_subtasks=1))
    await until(lambda: worker.started.is_set())
    await sampled(worker)
    worker.gate.set()
    assert await asyncio.wait_for(task, 10) == 1 and worker.interrupts == 0


async def test_loop_writes_status_on_change_and_at_stop(tmp_path: Path) -> None:
    import json
    import stat

    from minion_executor.capacity.status import STATUS_FILE, StatusFile, read_status

    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    gov = FixedGovernor(BUDGET)
    client, worker = FakeClient([assignment()]), FakeWorker()
    worker.gate = threading.Event()
    loop = make_loop(
        client, worker, FakeS3(), tmp_path, [], governor=gov, status_dir=state_dir,
        status_interval_s=30.0,
    )  # fmt: skip
    task = asyncio.create_task(loop.run(max_subtasks=1))
    await until(lambda: worker.started.is_set())

    def current() -> StatusFile:
        st = read_status(state_dir)
        assert st is not None
        return st

    await until(lambda: (st := read_status(state_dir)) is not None and st.subtask is not None)
    sub = current().subtask
    assert sub is not None and (sub.subtask_id, sub.attempt, sub.records_total) == (0, 1, 3)
    gov.update(dec(BUDGET, reason=PausedReason.THERMAL))  # a decision change: written at once
    await until(lambda: current().paused_reason == "thermal")
    gov.update(dec(BUDGET))
    await until(lambda: current().paused_reason is None)
    raw = (state_dir / STATUS_FILE).read_text()
    assert stat.S_IMODE((state_dir / STATUS_FILE).stat().st_mode) == 0o600
    for forbidden in ("token", "http", "acquirement", "input", "output_url", "text"):
        assert forbidden not in raw.lower(), forbidden
    assert json.loads(raw)["pid"] == os.getpid()
    worker.gate.set()
    assert await asyncio.wait_for(task, 10) == 1
    final = current()
    assert final.stopped and final.subtask is None
    assert (final.today.subtasks, final.today.records) == (1, 3)


# --- fix round: memory guard vs batch budget, watcher after inference, fail closed -------------


class SlowUploadS3(FakeS3):
    """PUT blocks until `release_put` is set; `put_started` marks the upload in flight."""

    def __init__(self) -> None:
        super().__init__()
        self.put_started = asyncio.Event()
        self.release_put = asyncio.Event()

    async def handler(self, req: httpx.Request) -> httpx.Response:  # type: ignore[override]
        if req.method == "GET":
            return super().handler(req)
        self.puts.append(req.read())
        self.put_started.set()
        await self.release_put.wait()
        return httpx.Response(self.put_status)


async def test_budget_drop_that_still_fits_the_model_does_not_trip_the_memory_guard(
    tmp_path: Path,
) -> None:
    gib = 1024**3
    gov = FixedGovernor(BUDGET)  # 8 GB while the first batch runs
    client, worker = FakeClient([assignment()]), FakeWorker()
    worker.gate = threading.Event()
    # the worker only shrinks at the next batch boundary: 5 GiB until the first batch is done
    worker.footprint_fn = lambda: 5 * gib if not worker.batches else gib
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], governor=gov, guard_interval_s=0.02)
    task = asyncio.create_task(loop.run(max_subtasks=1))
    await until(lambda: worker.started.is_set())
    gov.update(dec(small(2.0)))  # full -> light; the 0.1 GB model still fits, 5 GiB > 2.2 GiB
    await sampled(worker, 5)
    assert worker.interrupts == 0
    worker.gate.set()
    assert await asyncio.wait_for(task, 10) == 1
    assert "fail" not in client.kinds() and "release" not in client.kinds()


async def test_memory_guard_stays_on_during_the_downgrade_window(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    client, worker = FakeClient([assignment()]), FakeWorker(block=True)
    worker.footprints = [10 * 1024**3]  # over 1.1 x the batch's 8 GB budget
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], governor=gov, guard_interval_s=0.02)
    task = asyncio.create_task(loop.run())
    await until(lambda: worker.started.is_set())
    gov.update(dec(small(0.05)))  # downgrading: the guard must not switch off
    await until(lambda: "fail" in client.kinds())
    loop.stop()
    await asyncio.wait_for(task, 10)
    req = next(r for k, r in client.calls if k == "fail")
    assert req.code == "OOM" and worker.interrupts == 1


_AFTER_INFERENCE = [
    pytest.param({"pause_expired": True}, id="pause_expired"),
    pytest.param({"release_now": True, "trim": True}, id="memory_critical"),
    pytest.param({}, id="downgrade_timeout"),
]


@pytest.mark.parametrize("flags", _AFTER_INFERENCE)
async def test_verdict_during_upload_still_completes(tmp_path: Path, flags: dict[str, Any]) -> None:
    gov = FixedGovernor(BUDGET)
    client, worker, s3 = FakeClient([assignment()]), FakeWorker(), SlowUploadS3()
    loop = make_loop(
        client, worker, s3, tmp_path, [], governor=gov,
        downgrade_batch_s=0.05, guard_interval_s=0.02,
    )  # fmt: skip
    task = asyncio.create_task(loop.run(max_subtasks=1))
    await asyncio.wait_for(s3.put_started.wait(), 5)  # inference is over, the upload is in flight
    # the shrunken budget also makes the watcher unload the idle worker: proof it saw the decision
    gov.update(dec(small(0.05), reason=PausedReason.MEMORY_PRESSURE, **flags))
    await until(lambda: worker.unloads >= 1)
    assert "release" not in client.kinds() and worker.interrupts == 0
    s3.release_put.set()
    assert await asyncio.wait_for(task, 10) == 1
    assert "complete" in client.kinds()
    assert "release" not in client.kinds() and "fail" not in client.kinds()


class _RacingWorker(FakeWorker):
    """interrupt() opens the gate (the batch finishes while we interrupt) and then waits, on
    events only, until `until_fn` holds."""

    def __init__(self, until_fn: Callable[[], bool], exc: Exception | None = None) -> None:
        super().__init__(exc=exc)
        self.gate = threading.Event()
        self.until_fn = until_fn
        self.proceed = threading.Event()  # never set: a timed wait without sleeping

    def interrupt(self) -> None:
        super().interrupt()
        assert self.gate is not None
        self.gate.set()
        deadline = time.monotonic() + 5
        while not self.until_fn() and time.monotonic() < deadline:
            self.proceed.wait(0.005)


def _track_work_done(loop: ExecutorLoop) -> threading.Event:
    """Set (thread-safe) once the loop's work coroutine has returned or raised."""
    done = threading.Event()
    orig = loop._execute

    async def wrapped(*args: Any, **kw: Any) -> Any:
        try:
            return await orig(*args, **kw)
        finally:
            done.set()

    loop._execute = wrapped  # type: ignore[method-assign]
    return done


async def test_verdict_racing_work_completion_completes(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    client, s3 = FakeClient([assignment()]), FakeS3()
    done: threading.Event | None = None
    worker = _RacingWorker(lambda: done is not None and done.is_set())
    loop = make_loop(client, worker, s3, tmp_path, [], governor=gov)
    done = _track_work_done(loop)  # write, upload and the work task all finish during interrupt
    task = asyncio.create_task(loop.run(max_subtasks=1))
    await until(lambda: worker.started.is_set())
    gov.update(dec(pause_expired=True))
    assert await asyncio.wait_for(task, 10) == 1
    assert "complete" in client.kinds() and "release" not in client.kinds()
    assert len(s3.puts) == 1


async def test_verdict_while_the_upload_is_in_flight_lets_it_complete(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    client, s3 = FakeClient([assignment()]), SlowUploadS3()
    worker = _RacingWorker(lambda: bool(s3.puts))  # inference over, the work not yet done
    loop = make_loop(client, worker, s3, tmp_path, [], governor=gov)
    task = asyncio.create_task(loop.run(max_subtasks=1))
    await until(lambda: worker.started.is_set())
    gov.update(dec(pause_expired=True))
    await asyncio.wait_for(s3.put_started.wait(), 5)
    await until(lambda: worker.interrupts == 1)
    s3.release_put.set()
    assert await asyncio.wait_for(task, 10) == 1
    assert "complete" in client.kinds() and "release" not in client.kinds()


async def test_real_failure_racing_a_verdict_is_reported_with_its_own_code(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    client = FakeClient([assignment()])
    done: threading.Event | None = None
    worker = _RacingWorker(
        lambda: done is not None and done.is_set(),
        exc=RunnerError("MODEL_BROKEN", "x", retryable=False),
    )
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], governor=gov)
    done = _track_work_done(loop)
    task = asyncio.create_task(loop.run(max_subtasks=1))
    await until(lambda: worker.started.is_set())
    gov.update(dec(pause_expired=True))
    await until(lambda: "fail" in client.kinds() or "release" in client.kinds())
    loop.stop()
    await asyncio.wait_for(task, 10)
    req = _fail_req(client)
    assert (req.retryable, req.code) == (False, "MODEL_BROKEN")
    assert "release" not in client.kinds()


async def test_crashed_watcher_before_inference_ends_releases(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    client, worker = FakeClient([assignment()]), FakeWorker(block=True)
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], governor=gov)

    async def boom() -> None:
        raise RuntimeError("watcher bug")

    loop._push_control = boom  # type: ignore[method-assign]
    task = asyncio.create_task(loop.run())
    await until(lambda: "release" in client.kinds())
    loop.stop()
    await asyncio.wait_for(task, 10)
    assert worker.interrupts == 1 and "complete" not in client.kinds()
    assert "fail" not in client.kinds()  # released, not failed
    assert client.release_codes == ["WATCHER_FAILED"]


async def test_crashed_watcher_after_inference_lets_the_upload_complete(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    client, worker, s3 = FakeClient([assignment()]), FakeWorker(), SlowUploadS3()
    loop = make_loop(client, worker, s3, tmp_path, [], governor=gov, guard_interval_s=0.02)
    crashed = asyncio.Event()

    async def boom() -> None:
        crashed.set()
        raise RuntimeError("watcher bug")

    task = asyncio.create_task(loop.run(max_subtasks=1))
    await asyncio.wait_for(s3.put_started.wait(), 5)
    loop._enforce_idle_budget = boom  # type: ignore[method-assign]
    await asyncio.wait_for(crashed.wait(), 5)
    await asyncio.sleep(0)
    s3.release_put.set()
    assert await asyncio.wait_for(task, 10) == 1
    assert "complete" in client.kinds() and "release" not in client.kinds()


class StoppedGovernor(FixedGovernor):
    def __init__(self, budget: Budget, exc: Exception | None) -> None:
        super().__init__(budget)
        self._exc = exc

    async def run(self) -> None:
        if self._exc is not None:
            raise self._exc


@pytest.mark.parametrize("exc", [None, RuntimeError("boom")])
async def test_governor_task_ending_stops_the_executor(
    tmp_path: Path, exc: Exception | None
) -> None:
    from minion_executor.loop import GovernorStoppedError

    client = FakeClient([])
    loop = make_loop(
        client, FakeWorker(), FakeS3(), tmp_path, [], governor=StoppedGovernor(BUDGET, exc)
    )
    with pytest.raises(GovernorStoppedError):
        await asyncio.wait_for(loop.run(), 10)


async def test_register_announces_the_primed_budget(tmp_path: Path) -> None:
    from minion_common.schemas import RegisterRequest

    held = small(3.0, gpu_duty_cycle=0.25, profile="light")
    seed = RegisterRequest(
        hostname="h", chip="c", total_memory_gb=32, os_version="o", engines=[Engine.MLX],
        profile=BUDGET.profile, budget=BUDGET,
    )  # fmt: skip
    client = FakeClient([])
    loop = make_loop(
        client, FakeWorker(), FakeS3(), tmp_path, [], governor=FixedGovernor(held), register=seed
    )
    client.on_empty = loop.stop
    await loop.run()
    assert client.registers[0].budget == held and client.registers[0].profile == held.profile


async def test_idle_sleep_returns_at_once_when_paused_during_acquire(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    client = FakeClient([])
    loop = make_loop(
        client, FakeWorker(), FakeS3(), tmp_path, [], sleep=None, backoff=(30.0, 30.0),
        governor=gov, presence_interval_s=100.0,
    )  # fmt: skip
    client.on_empty = lambda: gov.update(dec(reason=PausedReason.THERMAL))
    client.on_presence = loop.stop
    t0 = asyncio.get_running_loop().time()
    await asyncio.wait_for(loop.run(), 10)
    assert asyncio.get_running_loop().time() - t0 < 5
    assert client.presences[0].paused_reason == "thermal"


# --- priority grace restarts per batch (D8) ----------------------------------------------------


class OffsetClock:
    def __init__(self) -> None:
        self.offset = 0.0

    def __call__(self) -> float:
        return time.monotonic() + self.offset


async def test_priority_grace_is_not_restarted_by_a_decision_change(tmp_path: Path) -> None:
    clock = OffsetClock()
    gov = FixedGovernor(BUDGET)
    client, worker = FakeClient([assignment()]), FakeWorker(block=True)
    worker.priority = False
    loop = make_loop(
        client, worker, FakeS3(), tmp_path, [], governor=gov, guard_interval_s=0.02,
        priority_grace_s=100.0, clock=clock,
    )  # fmt: skip
    client.on_empty = loop.stop
    task = asyncio.create_task(loop.run())
    await until(lambda: worker.started.is_set())
    clock.offset = 60.0  # inside the grace window of the running batch
    gov.update(dec(BUDGET.model_copy(update={"cpu_priority": CpuPriority.NORMAL})))
    await sampled(worker)
    assert worker.interrupts == 0
    clock.offset = 101.0  # past the window opened by the batch start; a change must not extend it
    await asyncio.wait_for(task, 10)
    assert worker.interrupts == 1 and "release" in client.kinds()


async def test_priority_grace_restarts_when_a_batch_starts_with_another_priority(
    tmp_path: Path,
) -> None:
    clock = OffsetClock()
    gov = FixedGovernor(BUDGET)
    client, worker = FakeClient([assignment()]), FakeWorker()
    worker.priority = False
    first, second = threading.Event(), threading.Event()

    def hook(n: int) -> None:
        if n == 1:
            assert first.wait(10)
        else:
            assert second.wait(10)

    worker.run_hook = hook
    loop = make_loop(
        client, worker, FakeS3(), tmp_path, [], governor=gov, guard_interval_s=0.02,
        priority_grace_s=100.0, clock=clock,
    )  # fmt: skip
    task = asyncio.create_task(loop.run(max_subtasks=1))
    await until(lambda: worker.started.is_set())
    clock.offset = 99.0  # batch 1 is nearly out of grace
    await sampled(worker)
    assert worker.interrupts == 0
    gov.update(dec(BUDGET.model_copy(update={"cpu_priority": CpuPriority.NORMAL})))
    first.set()  # batch 2 starts with another priority: a fresh window from here
    await until(lambda: len(worker.run_budgets) == 2)
    clock.offset = 150.0  # past the first window, inside the second
    await sampled(worker)
    assert worker.interrupts == 0
    second.set()
    assert await asyncio.wait_for(task, 10) == 1


# --- control push serialisation, status resilience, rollover ----------------------------------


async def test_concurrent_control_pushes_are_serialised_and_deduplicated(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    worker = FakeWorker()
    inner = worker.control

    def slow(*, duty: float, paused: bool, trim: bool = False) -> int:
        time.sleep(0.1)
        return inner(duty=duty, paused=paused, trim=trim)

    worker.control = slow  # type: ignore[method-assign]
    loop = make_loop(FakeClient([]), worker, FakeS3(), tmp_path, [], governor=gov)
    await asyncio.gather(loop._push_control(), loop._push_control(), loop._push_control())
    assert len(worker.controls) == 1  # the later pushes saw the key already pushed


async def test_status_failures_are_logged_once_and_never_stop_the_loop(tmp_path: Path) -> None:
    from structlog.testing import capture_logs

    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    client = FakeClient([assignment()])
    loop = make_loop(
        client, FakeWorker(), FakeS3(), tmp_path, [], governor=FixedGovernor(BUDGET),
        status_dir=state_dir, status_interval_s=0.02,
    )  # fmt: skip

    def broken(*, stopped: bool = False) -> Any:
        raise RuntimeError("snapshot bug")

    loop._status_snapshot = broken  # type: ignore[method-assign]
    with capture_logs() as logs:
        assert await asyncio.wait_for(loop.run(max_subtasks=1), 10) == 1
    assert [e["event"] for e in logs].count("status_write_failed") == 1


async def test_today_rolls_over_at_local_midnight_on_the_event_loop_snapshot_only_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import datetime as dt

    import minion_executor.loop as lp

    days = [dt.date(2026, 10, 14)]
    monkeypatch.setattr(lp, "_local_today", lambda: days[0])
    loop = make_loop(FakeClient([]), FakeWorker(), FakeS3(), tmp_path, [])
    loop._note_done(5)
    snap = loop._status_snapshot()
    assert (snap.today.date, snap.today.subtasks, snap.today.records) == (days[0], 1, 5)
    days[0] = dt.date(2026, 10, 15)  # midnight passes while idle
    assert loop._status_snapshot().today.date == dt.date(2026, 10, 14)  # a read never rolls
    loop._roll_today()  # what _status_loop does on the event loop before the thread
    snap = loop._status_snapshot()
    assert (snap.today.date, snap.today.subtasks, snap.today.records) == (days[0], 0, 0)


# --- graceful stop during a pause / a downgrade (FR-17) -----------------------------------------


async def test_sigterm_while_paused_mid_subtask_releases_after_the_grace(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    client, worker = FakeClient([assignment()]), FakeWorker(block=True)
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], governor=gov, stop_grace_s=0.2)
    task = asyncio.create_task(loop.run())
    await until(lambda: worker.started.is_set())
    gov.update(dec(reason=PausedReason.THERMAL))  # paused, not yet expired
    await until(lambda: (BUDGET.gpu_duty_cycle, True, False) in worker.controls)
    loop.stop()
    assert await asyncio.wait_for(task, 10) == 0
    assert worker.killed and "release" in client.kinds() and "complete" not in client.kinds()


async def test_sigterm_while_paused_at_a_batch_boundary_releases(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    client, worker = FakeClient([assignment()]), FakeWorker()
    worker.run_hook = lambda n: gov.update(dec(reason=PausedReason.THERMAL)) if n == 1 else None
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], governor=gov, stop_grace_s=0.2)
    task = asyncio.create_task(loop.run())
    await until(lambda: worker.batches == [2])  # batch 1 done, batch 2 waits for the resume
    loop.stop()
    assert await asyncio.wait_for(task, 10) == 0
    assert worker.killed and "release" in client.kinds() and "complete" not in client.kinds()


async def test_sigterm_during_a_downgrade_releases_after_the_grace(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    client, worker = FakeClient([assignment()]), FakeWorker(block=True)
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], governor=gov, stop_grace_s=0.2)
    task = asyncio.create_task(loop.run())
    await until(lambda: worker.started.is_set())
    gov.update(dec(small(0.05)))  # downgrading: the batch is allowed to finish ...
    loop.stop()  # ... but the user stops first: grace, then kill + release
    assert await asyncio.wait_for(task, 10) == 0
    assert worker.killed and "release" in client.kinds() and "complete" not in client.kinds()


async def test_sigterm_during_a_downgrade_lets_a_finishing_batch_release_gracefully(
    tmp_path: Path,
) -> None:
    gov = FixedGovernor(BUDGET)
    client, worker = FakeClient([assignment()]), FakeWorker()
    worker.gate = threading.Event()
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], governor=gov, stop_grace_s=30.0)
    task = asyncio.create_task(loop.run())
    await until(lambda: worker.started.is_set())
    gov.update(dec(small(0.05)))
    loop.stop()
    worker.gate.set()  # the batch ends inside the grace window
    assert await asyncio.wait_for(task, 10) == 0
    assert "release" in client.kinds() and "complete" not in client.kinds()
    assert not worker.killed or worker.closed


async def test_crashed_watcher_backs_off_like_a_local_fault(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    client, worker, sleeps = FakeClient([assignment()]), FakeWorker(block=True), list[float]()
    loop = make_loop(client, worker, FakeS3(), tmp_path, sleeps, governor=gov, backoff=(30.0, 30.0))

    async def boom() -> None:
        raise RuntimeError("watcher bug")

    loop._push_control = boom  # type: ignore[method-assign]
    task = asyncio.create_task(loop.run())
    await until(lambda: "release" in client.kinds())
    await until(lambda: len(sleeps) >= 1)  # FAILED outcome: back-off, though it was released
    loop.stop()
    await asyncio.wait_for(task, 10)
    assert "fail" not in client.kinds()


def other_model() -> Assignment:
    """An assignment for a different model than `assignment()` (a different warm identity)."""
    a = assignment()
    return a.model_copy(update={"model": a.model.model_copy(update={"model_id": "other/model"})})


class _GatedCache(FakeCache):
    def __init__(self) -> None:
        super().__init__()
        self.calls = 0
        self.block_on: int | None = None
        self.release = asyncio.Event()

    async def ensure(self, model: ModelEntry) -> None:
        self.calls += 1
        if self.calls == self.block_on:
            await self.release.wait()
        return None


async def cycles(loop: ExecutorLoop, n: int = 3) -> None:
    """Wait until the watcher ran `n` more guard cycles (it evaluated the latest decision)."""
    real = loop._push_control
    count = 0

    async def counting() -> None:
        nonlocal count
        count += 1
        await real()

    loop._push_control = counting  # type: ignore[method-assign]
    try:
        await until(lambda: count >= n)
    finally:
        loop._push_control = real  # type: ignore[method-assign]


async def test_unloaded_flag_stale_idle_unload_works_after_the_subtask(tmp_path: Path) -> None:
    gib = 1024**3
    gov = FixedGovernor(BUDGET)
    first = assignment()
    first = first.model_copy(update={"model": first.model.model_copy(update={"memory_gb": 8})})
    client, worker = FakeClient([first, other_model()]), FakeWorker()
    worker.footprints = [7 * gib]
    cache = _GatedCache()
    cache.block_on = 2
    loop = make_loop(
        client, worker, FakeS3(), tmp_path, [], governor=gov, model_cache=cache,
        guard_interval_s=0.02,
    )  # fmt: skip
    task = asyncio.create_task(loop.run(max_subtasks=2))
    await until(lambda: cache.calls == 2)
    gov.update(dec(small(4.0)))
    await until(lambda: worker.unloads >= 1)
    cache.release.set()
    assert await asyncio.wait_for(task, 10) == 2
    assert loop._loaded is not None and not loop._unloaded
    before = worker.unloads
    await loop._unload_idle("idle")  # what _maybe_unload does once idle: must not be skipped
    assert worker.unloads == before + 1


async def test_idle_unload_keeps_a_newer_pin(tmp_path: Path) -> None:
    """A watcher unload of a stale warm model must not unpin the subtask's own new pin."""
    client, worker, cache = FakeClient([]), FakeWorker(), FakeCache()
    old, new = tmp_path / "old", tmp_path / "new"
    cache.in_use = old
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], model_cache=cache)
    loop._loaded_path = old  # the warm worker holds the model at `old`

    def unload_and_repin() -> None:
        cache.in_use = new  # the subtask's ensure() pinned its own model during the unload

    worker.unload = unload_and_repin  # type: ignore[method-assign]
    await loop._unload_idle("budget_downgrade")
    assert cache.unpins == 0


async def test_two_step_downgrade_releases_never_oom(tmp_path: Path) -> None:
    gib = 1024**3
    gov = FixedGovernor(BUDGET)  # 8 GB
    a = assignment()
    a = a.model_copy(update={"model": a.model.model_copy(update={"memory_gb": 6})})
    client, worker = FakeClient([a]), FakeWorker(block=True)
    worker.footprints = [int(6.5 * gib)]
    clock = OffsetClock()
    loop = make_loop(
        client, worker, FakeS3(), tmp_path, [], governor=gov,
        guard_interval_s=0.02, downgrade_batch_s=30.0, clock=clock,
    )  # fmt: skip
    client.on_empty = loop.stop
    task = asyncio.create_task(loop.run())
    await until(lambda: worker.started.is_set())
    gov.update(dec(small(7.0)))  # still fits the model: the allowance clock starts
    await sampled(worker, 3)
    clock.offset = 25.0  # the first drop's allowance has almost run out
    gov.update(dec(small(4.0)))  # a real downgrade: the allowance carries on until the release
    await cycles(loop, 3)
    assert worker.interrupts == 0 and "release" not in client.kinds()
    clock.offset = 31.0
    await asyncio.wait_for(task, 10)
    assert "fail" not in client.kinds() and "release" in client.kinds()


async def test_warm_worker_over_a_dropped_budget_is_unloaded_not_counted_as_oom(
    tmp_path: Path,
) -> None:
    gib = 1024**3
    gov = FixedGovernor(BUDGET)  # 8 GB
    first = assignment()
    first = first.model_copy(update={"model": first.model.model_copy(update={"memory_gb": 8})})
    client, worker = FakeClient([first, other_model()]), FakeWorker()
    worker.footprints = [7 * gib]  # the warm 8 GB model: over a 4 GB budget's 4.4 GiB guard
    cache = _GatedCache()
    cache.block_on = 2  # the second subtask is still fetching its (small) model
    loop = make_loop(
        client, worker, FakeS3(), tmp_path, [], governor=gov, model_cache=cache,
        guard_interval_s=0.02,
    )  # fmt: skip
    task = asyncio.create_task(loop.run(max_subtasks=2))
    await until(lambda: cache.calls == 2)
    gov.update(dec(small(4.0)))
    await until(lambda: worker.unloads >= 1)
    await cycles(loop, 5)  # several guard cycles: the old code failed the subtask as OOM
    assert "fail" not in client.kinds() and worker.interrupts == 0
    cache.release.set()
    assert await asyncio.wait_for(task, 10) == 2
    assert "fail" not in client.kinds()
    assert loop._loaded is not None and not loop._unloaded  # the flag follows the new load


async def test_budget_drop_allowance_lasts_only_downgrade_batch_s(tmp_path: Path) -> None:
    gib = 1024**3
    gov = FixedGovernor(BUDGET)  # 8 GB while the batch runs
    client, worker = FakeClient([assignment()]), FakeWorker(block=True)
    worker.footprints = [5 * gib]  # fits 8 GB, over a 4 GB budget's 4.4 GiB guard
    clock = OffsetClock()
    loop = make_loop(
        client, worker, FakeS3(), tmp_path, [], governor=gov,
        guard_interval_s=0.02, downgrade_batch_s=30.0, clock=clock,
    )  # fmt: skip
    client.on_empty = loop.stop
    task = asyncio.create_task(loop.run())
    await until(lambda: worker.started.is_set())
    gov.update(dec(small(4.0)))  # the model (0.1 GB) still fits: no downgrade, the batch goes on
    await sampled(worker, 3)  # samples inside the allowance
    assert worker.interrupts == 0 and "fail" not in client.kinds()  # within the allowance
    clock.offset = 31.0  # the allowance is over
    await asyncio.wait_for(task, 10)
    # after it the owner's policy, not the subtask's fault: released, no attempt burnt
    assert "fail" not in client.kinds() and "release" in client.kinds()


async def test_pause_stops_a_new_model_fetch_from_starting(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)

    class PausingClient(FakeClient):
        async def acquire(self, req: AcquireRequest) -> Assignment | None:
            item = await super().acquire(req)
            if item is not None:
                gov.update(dec(reason=PausedReason.THERMAL))  # paused right after the acquire
            return item

    client, worker, cache = PausingClient([assignment()]), FakeWorker(), _GatedCache()
    loop = make_loop(
        client, worker, FakeS3(), tmp_path, [], governor=gov, model_cache=cache,
        guard_interval_s=0.02,
    )  # fmt: skip
    task = asyncio.create_task(loop.run(max_subtasks=1))
    await until(lambda: "acquire" in client.kinds())
    await cycles(loop, 5)
    assert cache.calls == 0 and not task.done()  # no download started while paused
    gov.update(dec())
    assert await asyncio.wait_for(task, 10) == 1
    assert cache.calls == 1


class _BlockingUnloadWorker(FakeWorker):
    def __init__(self) -> None:
        super().__init__()
        self.events: list[str] = []
        self.unload_gate = threading.Event()
        self.unload_started = threading.Event()

    def unload(self) -> None:
        self.events.append("unload_start")
        self.unload_started.set()
        self.unload_gate.wait(10)
        self.events.append("unload_end")
        super().unload()

    def ensure_model(
        self, model: ModelEntry, budget: Budget, model_path: Path | None = None
    ) -> None:
        self.events.append("ensure")
        super().ensure_model(model, budget, model_path)


async def test_ensure_model_waits_for_an_unload_in_flight(tmp_path: Path) -> None:
    gib = 1024**3
    gov = FixedGovernor(BUDGET)
    first = assignment()
    first = first.model_copy(update={"model": first.model.model_copy(update={"memory_gb": 8})})
    client, worker = FakeClient([first, other_model()]), _BlockingUnloadWorker()
    worker.footprints = [7 * gib]
    cache = _GatedCache()
    cache.block_on = 2
    loop = make_loop(
        client, worker, FakeS3(), tmp_path, [], governor=gov, model_cache=cache,
        guard_interval_s=0.02,
    )  # fmt: skip
    task = asyncio.create_task(loop.run(max_subtasks=2))
    await until(lambda: cache.calls == 2)
    gov.update(dec(small(4.0)))
    await until(worker.unload_started.is_set)  # the watcher's unload is stuck in its thread
    cache.release.set()  # the second subtask now wants ensure_model
    await asyncio.sleep(0.2)
    assert worker.events == ["ensure", "unload_start"]  # first subtask's load only
    worker.unload_gate.set()
    assert await asyncio.wait_for(task, 10) == 2
    assert worker.events == ["ensure", "unload_start", "unload_end", "ensure"]


async def test_idle_unload_unpins_only_the_model_it_unloaded(tmp_path: Path) -> None:
    gov = FixedGovernor(BUDGET)
    client, worker, cache = FakeClient([]), FakeWorker(), FakeCache()
    old, new = tmp_path / "old", tmp_path / "new"
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], governor=gov, model_cache=cache)
    big = assignment().model.model_copy(update={"memory_gb": 8})
    loop._loaded, loop._loaded_path = big, old  # warm: the old model
    cache.in_use = new  # the next subtask already pinned its own
    gov.update(dec(small(4.0)))
    await loop._enforce_idle_budget()
    assert worker.unloads == 1 and cache.unpins == 0 and cache.in_use == new
    loop._loaded, loop._loaded_path = big, new  # unloading the pinned model does unpin it
    await loop._enforce_idle_budget()
    assert worker.unloads == 2 and cache.unpins == 1


async def test_watcher_does_not_busy_spin_while_a_model_is_fetched(tmp_path: Path) -> None:
    client, worker, cache = FakeClient([assignment()]), FakeWorker(), _GatedCache()
    cache.block_on = 1
    loop = make_loop(
        client, worker, FakeS3(), tmp_path, [], governor=FixedGovernor(BUDGET),
        model_cache=cache, guard_interval_s=0.1,
    )  # fmt: skip
    task = asyncio.create_task(loop.run(max_subtasks=1))
    await until(lambda: cache.calls == 1)
    real, count = loop._push_control, 0

    async def counting() -> None:
        nonlocal count
        count += 1
        await real()

    loop._push_control = counting  # type: ignore[method-assign]
    await asyncio.sleep(0.6)  # no batch runs: the old watcher woke every 10 ms (~60 cycles)
    loop._push_control = real  # type: ignore[method-assign]
    assert count <= 10
    cache.release.set()
    assert await asyncio.wait_for(task, 10) == 1


async def test_register_429_backs_off_honouring_retry_after(tmp_path: Path) -> None:
    client, sleeps = FakeClient([]), list[float]()
    client.register_excs = [ApiError(429, "RATE_LIMITED", "slow", retry_after=120.0)]
    loop = make_loop(client, FakeWorker(), FakeS3(), tmp_path, sleeps)
    loop._register = object()  # type: ignore[assignment]
    client.on_empty = loop.stop
    await loop.run()
    assert client.kinds().count("register") == 2
    assert 120.0 <= sleeps[0] <= 144.0  # Retry-After + 0-20 % jitter


async def test_acquire_429_backs_off_and_does_not_escape(tmp_path: Path) -> None:
    client = FakeClient([ApiError(429, "RATE_LIMITED", "slow", retry_after=9999.0)])
    sleeps: list[float] = []
    loop = make_loop(client, FakeWorker(), FakeS3(), tmp_path, sleeps)
    client.on_empty = loop.stop
    await loop.run()
    assert 300.0 <= sleeps[0] <= 360.0  # Retry-After is capped, then jittered


def _grew(items: list[float], seen: int) -> bool:
    return len(items) > seen


class WaitSpyGovernor(FixedGovernor):
    """Records every `wait_change` timeout and signals it: an observable 'the loop is now
    sleeping in a back-off of that length', instead of guessing with asyncio.sleep."""

    def __init__(self, budget: Budget) -> None:
        super().__init__(budget)
        self.timeouts: list[float] = []
        self.waiting = asyncio.Event()

    async def wait_change(self, version: int, timeout_s: float) -> int:
        self.timeouts.append(timeout_s)
        self.waiting.set()
        return await super().wait_change(version, timeout_s)


async def test_governor_downgrade_during_429_backoff_unloads_warm_worker(tmp_path: Path) -> None:
    gib = 1024**3
    gov = WaitSpyGovernor(BUDGET)  # 8 GB
    first = assignment()
    first = first.model_copy(update={"model": first.model.model_copy(update={"memory_gb": 8})})
    client = FakeClient([first, ApiError(429, "RATE_LIMITED", "slow", retry_after=250.0)])
    worker = FakeWorker()
    worker.footprints = [7 * gib]  # warm model: over a 4 GB budget's guard
    loop = make_loop(client, worker, FakeS3(), tmp_path, [], governor=gov, sleep=None)
    task = asyncio.create_task(loop.run())
    await until(lambda: client.kinds().count("acquire") >= 2)
    await until(lambda: any(t >= 200 for t in gov.timeouts))  # inside the 429 back-off
    assert worker.unloads == 0
    gov.update(dec(small(4.0)))
    await until(lambda: worker.unloads >= 1)
    loop.stop()
    await asyncio.wait_for(task, 10)


async def test_429_backoff_cut_short_by_a_pause_is_waited_out_after_resume(
    tmp_path: Path,
) -> None:
    now = [1000.0]
    gov = WaitSpyGovernor(BUDGET)
    client = FakeClient([ApiError(429, "RATE_LIMITED", "slow", retry_after=250.0), assignment()])
    loop = make_loop(
        client, FakeWorker(), FakeS3(), tmp_path, [], governor=gov, sleep=None, clock=lambda: now[0]
    )
    task = asyncio.create_task(loop.run())
    await until(lambda: any(t >= 200 for t in gov.timeouts))  # in the 429 back-off
    gov.update(dec(reason=PausedReason.THERMAL))
    await until(lambda: len(client.presences) >= 1)  # the pause ended the back-off
    now[0] += 100.0  # 100 s of the 250 s passed while paused
    gov.timeouts.clear()
    gov.update(dec())  # resume
    await until(lambda: any(100 <= t <= 210 for t in gov.timeouts))  # 150-200 s still to wait
    assert client.kinds().count("acquire") == 1  # not retried before Retry-After elapsed
    loop.stop()
    await asyncio.wait_for(task, 10)


async def test_presence_429_honours_retry_after_and_keeps_the_pending_state(
    tmp_path: Path,
) -> None:
    now = [1000.0]
    gov = WaitSpyGovernor(BUDGET)
    gov.update(dec(reason=PausedReason.THERMAL))
    client = FakeClient([])
    client.presence_excs = [ApiError(429, "TOO_MANY_REQUESTS", "slow", retry_after=30.0)]
    loop = make_loop(
        client,
        FakeWorker(),
        FakeS3(),
        tmp_path,
        [],
        governor=gov,
        presence_interval_s=100.0,
        clock=lambda: now[0],
    )
    task = asyncio.create_task(loop.run())
    await until(lambda: len(client.presences) == 1)  # rejected
    await until(lambda: gov.waiting.is_set())
    for state in (dec(reason=PausedReason.MEMORY_PRESSURE), dec(small(4.0, profile="light"))):
        seen = len(gov.timeouts)
        now[0] += 5.0
        gov.update(dec(state.budget, reason=PausedReason.MEMORY_PRESSURE))  # changed, limited
        await until(partial(_grew, gov.timeouts, seen))  # the loop looked and waited again
    assert len(client.presences) == 1  # nothing sent inside Retry-After
    now[0] += 25.0  # past the 30 s deadline
    gov.update(dec(reason=PausedReason.THERMAL))
    await until(lambda: len(client.presences) == 2)
    assert client.presences[1].paused_reason == "thermal"
    loop.stop()
    await asyncio.wait_for(task, 10)


def test_release_code_is_upper_cased_before_falling_back() -> None:
    assert _release_code("budget_downgrade") == "BUDGET_DOWNGRADE"
    assert _release_code("STOP") == "STOP"
    assert _release_code("free text!") == "EXECUTOR_RELEASE"


async def test_repeated_429_escalates_to_max_of_backoff_and_retry_after(tmp_path: Path) -> None:
    client = FakeClient([ApiError(429, "RATE_LIMITED", "x", retry_after=3.0)] * 2)
    sleeps: list[float] = []
    loop = make_loop(client, FakeWorker(), FakeS3(), tmp_path, sleeps)
    client.on_empty = loop.stop
    await loop.run()
    assert 3.0 <= sleeps[0] <= 3.6  # first: Retry-After + jitter
    assert sleeps[1] >= 3.0  # repeated: at least Retry-After


def test_rate_limit_delay_first_vs_repeated(tmp_path: Path) -> None:
    loop = make_loop(FakeClient([]), FakeWorker(), FakeS3(), tmp_path, [])
    exc = ApiError(429, "RATE_LIMITED", "x", retry_after=2.0)
    for _ in range(50):
        assert 2.0 <= loop._rate_limit_delay(exc, 0) <= 2.4
        assert loop._rate_limit_delay(exc, 0, repeated=True) >= 2.0
    other = ApiError(503, "X", "x")
    assert loop._rate_limit_delay(other, 0) <= loop._delay(0) * 2
