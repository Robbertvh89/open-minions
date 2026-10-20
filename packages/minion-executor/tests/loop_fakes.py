"""Fakes shared by the executor loop tests."""

import asyncio
import threading
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from email.parser import BytesParser
from email.policy import HTTP
from pathlib import Path
from typing import Any

import httpx
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
    PresenceRequest,
)
from minion_executor.loop import ExecutorLoop
from minion_executor.runners.base import Record
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
            "output_post": POST,
            "expires_at": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
        }
    )


POST = {"url": "https://s3.test/out", "fields": {"key": "k", "policy": "p"}}


def hb_resp(
    cancel: bool = False, policy: str = "p", minutes: int = 0, key: str = "k"
) -> HeartbeatResponse:
    post = {"url": "https://s3.test/out", "fields": {"key": key, "policy": policy}}
    return HeartbeatResponse.model_validate(
        {
            "cancel": cancel,
            "output_post": post,
            "expires_at": (datetime.now(UTC) + timedelta(minutes=minutes)).isoformat(),
        }
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
        # the ack (first beat after an acquire, ADR-0014) is recorded apart so scripted
        # heartbeats and call-order assertions keep their meaning
        self.acks: list[uuid.UUID] = []
        self.ack_script: list[HeartbeatResponse | Exception] = []
        self.on_ack: Any = None
        self._unacked: set[uuid.UUID] = set()

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
        if item is not None:
            self._unacked.add(item.acquirement_id)
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
        if aid in self._unacked:
            self._unacked.discard(aid)
            self.acks.append(aid)
            if self.on_ack:
                self.on_ack()
            if self.ack_script:
                ack = self.ack_script.pop(0)
                if isinstance(ack, Exception):
                    raise ack
                return ack
            return hb_resp()
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
        self.gpu_busy: float | None = None  # GPU busy share of wall time
        self.gpu_t0: float | None = None
        self.gpu_calls = 0
        self.gpu_hang: threading.Event | None = None
        self.fake_pid: int | None = 4242
        self.gpu_run_busy: float | None = None  # GPU busy share inside run() only (gaps idle)
        self._gpu_run_cum = 0.0
        self._gpu_run_t0: float | None = None
        self.tick_at: float | None = None  # latest (fake) worker tick
        self.pause_ack: float | None = None  # when the (fake) worker acked the pause
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
        self._gpu_run_t0 = time.monotonic()
        try:
            return self._run(batch, params, on_progress, budget)
        finally:
            if self.gpu_run_busy is not None and self._gpu_run_t0 is not None:
                self._gpu_run_cum += self.gpu_run_busy * (time.monotonic() - self._gpu_run_t0)
            self._gpu_run_t0 = None

    def _run(
        self,
        batch: list[Record],
        params: Any = None,
        on_progress: Any = None,
        budget: Budget | None = None,
    ) -> list[Record]:
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

    def pause_acked_at(self) -> float | None:
        return self.pause_ack

    def last_tick_at(self) -> float | None:
        return self.tick_at

    @property
    def pid(self) -> int | None:
        return self.fake_pid

    def gpu_clients(self, reader: Callable[[], Any]) -> tuple[int, dict[str, int]] | None:
        """Scripted control-side GPU time: `gpu_busy` x wall time since the first call."""
        self.gpu_calls += 1
        if self.gpu_hang is not None:
            self.gpu_hang.wait(5)  # a stuck IOKit read
        reader()
        if self.gpu_run_busy is not None:
            live = (
                self.gpu_run_busy * (time.monotonic() - self._gpu_run_t0)
                if self._gpu_run_t0 is not None
                else 0.0
            )
            return 4242, {"0x1": int((self._gpu_run_cum + live) * 1e9)}
        if self.gpu_busy is None:
            return None
        now = time.monotonic()
        self.gpu_t0 = self.gpu_t0 if self.gpu_t0 is not None else now
        return 4242, {"0x1": int(self.gpu_busy * (now - self.gpu_t0) * 1e9)}

    def kill(self) -> None:
        self.killed = True

    def close(self) -> None:
        self.closed = True


class FakeS3:
    def __init__(
        self,
        body: bytes = INPUT,
        get_status: int = 206,
        put_status: int = 200,
        put_body: bytes = b"",
    ) -> None:
        self.body = body
        self.get_status, self.put_status, self.put_body = get_status, put_status, put_body
        self.puts: list[bytes] = []  # file parts of the uploaded POST forms
        self.forms: list[dict[str, str]] = []
        self.ranges: list[str] = []

    def handler(self, req: httpx.Request) -> httpx.Response:
        if req.method == "GET":
            self.ranges.append(req.headers["Range"])
            return httpx.Response(self.get_status, content=self.body)
        assert req.method == "POST"
        raw = req.read()
        assert int(req.headers["Content-Length"]) == len(raw)
        msg = BytesParser(policy=HTTP).parsebytes(
            b"Content-Type: " + req.headers["Content-Type"].encode() + b"\r\n\r\n" + raw
        )
        parts = list(msg.iter_parts())
        assert parts[-1].get_param("name", header="content-disposition") == "file"
        self.forms.append(
            {
                str(p.get_param("name", header="content-disposition")): str(p.get_content())
                for p in parts[:-1]
            }
        )
        self.puts.append(bytes(parts[-1].get_payload(decode=True)))
        return httpx.Response(self.put_status, content=self.put_body)


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
