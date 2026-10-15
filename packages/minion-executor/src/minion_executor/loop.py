"""Executor control loop: acquire -> download -> run in worker -> upload -> complete.

Talks to the server only through `ExecutorClient` (invariant 1) and to S3 only through presigned
URLs (invariant 4). Inference happens in the worker subprocess (invariant 7). Logs carry ids only.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import shutil
import tempfile
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, Protocol

import httpx
import structlog
from minion_common.client import ApiError, ExecutorClient, StaleAcquirementError
from minion_common.enums import Engine
from minion_common.schemas import (
    AcquireRequest,
    Assignment,
    Budget,
    CompleteRequest,
    FailRequest,
    ModelEntry,
    RegisterRequest,
)

from minion_executor import io
from minion_executor.runners.base import Record, RunnerError
from minion_executor.worker.process import WorkerCrashed

log = structlog.get_logger("minion_executor.loop")

BAD_INPUT = "BAD_INPUT"


class Worker(Protocol):
    def ensure_model(self, model: ModelEntry, budget: Budget) -> None: ...
    def run(self, batch: list[Record]) -> list[Record]: ...
    def kill(self) -> None: ...
    def close(self) -> None: ...


class _FailureError(Exception):
    def __init__(self, retryable: bool, code: str, message: str) -> None:
        super().__init__(code)
        self.retryable, self.code, self.message = retryable, code, message


class ExecutorLoop:
    def __init__(
        self,
        client: ExecutorClient,
        worker: Worker,
        budget: Budget,
        engines: list[Engine],
        *,
        http: httpx.AsyncClient | None = None,
        register: RegisterRequest | None = None,
        backoff: tuple[float, float] = (10.0, 30.0),
        sleep: Callable[[float], Awaitable[None]] | None = None,
        tmp_root: Path | None = None,
    ) -> None:
        self._client = client
        self._worker = worker
        self._budget = budget
        self._engines = engines
        self._http = http or httpx.AsyncClient(timeout=httpx.Timeout(60.0))
        self._owns_http = http is None
        self._register = register
        self._backoff = backoff
        self._sleep_fn = sleep
        self._tmp_root = tmp_root
        self._stop = asyncio.Event()
        self._idle = 0

    def stop(self) -> None:
        self._stop.set()

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    async def _sleep(self) -> None:
        lo, hi = self._backoff
        delay = min(hi, lo * (2 ** min(self._idle, 16))) * random.uniform(0.5, 1.0)  # noqa: S311
        self._idle += 1
        if self._sleep_fn is not None:
            await self._sleep_fn(delay)
            return
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=delay)

    async def run(self, max_subtasks: int | None = None) -> int:
        """Run until stopped (or `max_subtasks` handled). Returns subtasks completed."""
        done = 0
        tmp = Path(tempfile.mkdtemp(prefix="minion-exec-", dir=self._tmp_root))  # mode 0700
        try:
            if self._register is not None:
                await self._client.register(self._register)
            while not self.stopping and (max_subtasks is None or done < max_subtasks):
                try:
                    assignment = await self._client.acquire(
                        AcquireRequest(engines=self._engines, budget=self._budget)
                    )
                except (httpx.HTTPError, ApiError) as exc:
                    if isinstance(exc, ApiError) and exc.status_code < 500:
                        raise
                    log.warning("acquire_failed", error=type(exc).__name__)
                    await self._sleep()
                    continue
                if assignment is None:
                    await self._sleep()
                    continue
                self._idle = 0
                if await self._handle(assignment, tmp):
                    done += 1
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
            await asyncio.to_thread(self._worker.close)
            if self._owns_http:
                await self._http.aclose()
        return done

    async def _handle(self, a: Assignment, tmp: Path) -> bool:
        bound = log.bind(
            acquirement_id=str(a.acquirement_id),
            team=a.team,
            project=a.project,
            task_name=a.task_name,
            subtask_id=a.subtask_id,
            attempt=a.attempt,
        )
        work = asyncio.create_task(self._execute(a, tmp))
        stopper = asyncio.create_task(self._stop.wait())
        try:
            await asyncio.wait({work, stopper}, return_when=asyncio.FIRST_COMPLETED)
            if not work.done():
                # SIGTERM/stop mid-subtask: reclaim worker memory, hand the subtask back.
                await asyncio.to_thread(self._worker.kill)
                work.cancel()
                await asyncio.gather(work, return_exceptions=True)
                await self._release(a, bound)
                return False
            try:
                result = work.result()
            except _FailureError as f:
                await self._report_failure(a, f, bound)
                return False
            except StaleAcquirementError:
                bound.info("stale_discarded")
                return False
            try:
                await self._client.complete(a.acquirement_id, result)
            except StaleAcquirementError:
                bound.info("stale_discarded")
                return False
            except (httpx.HTTPError, ApiError) as exc:
                bound.warning("complete_failed", error=type(exc).__name__)
                return False
            bound.info("subtask_completed", records=result.record_count)
            return True
        finally:
            stopper.cancel()

    async def _execute(self, a: Assignment, tmp: Path) -> CompleteRequest:
        in_path = tmp / f"{a.acquirement_id}.in.jsonl"
        out_path = tmp / f"{a.acquirement_id}.out.jsonl"
        try:
            try:
                await io.download_range(self._http, a.input_url, a.byte_start, a.byte_end, in_path)
                records = io.parse_jsonl(in_path, a.record_count)
            except io.BadInputError as exc:
                raise _FailureError(False, BAD_INPUT, str(exc)) from None
            except httpx.HTTPError as exc:
                raise _FailureError(True, "INPUT_UNAVAILABLE", type(exc).__name__) from None
            outputs = await self._run_batches(a, records)
            size, digest = io.write_jsonl(out_path, outputs)
            try:
                await io.upload_file(self._http, a.output_url, out_path)
            except httpx.HTTPError as exc:
                raise _FailureError(True, "UPLOAD_FAILED", type(exc).__name__) from None
            return CompleteRequest(record_count=len(outputs), byte_size=size, sha256=digest)
        finally:
            in_path.unlink(missing_ok=True)
            out_path.unlink(missing_ok=True)

    async def _run_batches(self, a: Assignment, records: list[Record]) -> list[Record]:
        step = self._budget.max_batch_size
        outputs: list[Record] = []
        try:
            await asyncio.to_thread(self._worker.ensure_model, a.model, self._budget)
            for i in range(0, len(records), step):
                outputs.extend(await asyncio.to_thread(self._worker.run, records[i : i + step]))
        except WorkerCrashed:
            raise _FailureError(True, "WORKER_CRASHED", "worker process died") from None
        except RunnerError as exc:
            raise _FailureError(True, exc.code[:64] or "RUNNER_ERROR", "runner failed") from None
        return outputs

    async def _report_failure(self, a: Assignment, f: _FailureError, bound: Any) -> None:
        bound.warning("subtask_failed", code=f.code, retryable=f.retryable)
        try:
            await self._client.fail(
                a.acquirement_id,
                FailRequest(retryable=f.retryable, code=f.code, message=f.message[:1000]),
            )
        except StaleAcquirementError:
            bound.info("stale_discarded")
        except (httpx.HTTPError, ApiError) as exc:
            bound.warning("fail_report_failed", error=type(exc).__name__)

    async def _release(self, a: Assignment, bound: Any) -> None:
        try:
            await self._client.release(a.acquirement_id)
            bound.info("subtask_released")
        except (httpx.HTTPError, ApiError) as exc:
            bound.warning("release_failed", error=type(exc).__name__)
