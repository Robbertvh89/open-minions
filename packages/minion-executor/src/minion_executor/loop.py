"""Executor control loop: acquire -> download -> run in worker -> upload -> complete.

Talks to the server only through `ExecutorClient` (invariant 1) and to S3 only through presigned
URLs (invariant 4). Inference happens in the worker subprocess (invariant 7). Logs carry ids only.
"""

from __future__ import annotations

import asyncio
import contextlib
import enum
import os
import random
import re
import shutil
import tempfile
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit

import httpx
import structlog
from minion_common.client import (
    ApiError,
    ExecutorClient,
    StaleAcquirementError,
    require_secure_url,
)
from minion_common.enums import Engine
from minion_common.schemas import (
    AcquireRequest,
    Assignment,
    Budget,
    CompleteRequest,
    FailRequest,
    ModelEntry,
    RegisterRequest,
    TaskParams,
    parse_params,
)
from pydantic import ValidationError

from minion_executor import io
from minion_executor.heartbeat import Heartbeater, HeartbeatSignal
from minion_executor.model_cache import MODEL_CACHE_FULL, ModelCache, ModelFetchError
from minion_executor.runners.base import ENGINE_UNAVAILABLE, Record, RunnerError
from minion_executor.worker.process import WorkerCrashed

log = structlog.get_logger("minion_executor.loop")

BAD_INPUT = "BAD_INPUT"
BAD_PARAMS = "BAD_PARAMS"
MODEL_NOT_ADMISSIBLE = "MODEL_NOT_ADMISSIBLE"
TMP_PREFIX = "minion-exec-"
# Refresh the output URL with one synchronous heartbeat when it expires within this margin.
UPLOAD_URL_MARGIN_S = 60.0
_TMP_RE = re.compile(r"^minion-exec-(\d+)-")
# Connectivity trouble on this Mac: released (no retry budget burned). Everything else from S3
# (HTTP status errors, unexpected bodies) is a retryable `fail` so max_attempts bounds it.
LOCAL_TRANSPORT_ERRORS: tuple[type[Exception], ...] = (
    httpx.ConnectError,
    httpx.TimeoutException,
    httpx.NetworkError,
    httpx.RemoteProtocolError,
    httpx.ProxyError,
)


class _Outcome(enum.Enum):
    DONE = enum.auto()  # completed and acknowledged
    FAILED = enum.auto()  # failed/released/gave up: back off before the next acquire
    DISCARDED = enum.auto()  # stale (409): someone else owns it now, nothing to report
    STOPPED = enum.auto()  # stop requested mid-subtask
    CANCELLED = enum.auto()  # task cancelled via heartbeat: released, no failure backoff


def cleanup_stale_tmp(root: Path | None = None) -> int:
    """Remove `minion-exec-<pid>-*` dirs owned by this user whose pid is no longer alive."""
    base = root or Path(tempfile.gettempdir())
    removed = 0
    try:
        entries = list(base.iterdir())
    except OSError:
        return 0
    for entry in entries:
        m = _TMP_RE.match(entry.name)
        if m is None or entry.is_symlink() or int(m.group(1)) == os.getpid():
            continue
        try:
            if not entry.is_dir() or entry.stat().st_uid != os.getuid():
                continue
            os.kill(int(m.group(1)), 0)
            continue  # owner process is alive: not stale
        except ProcessLookupError:
            pass
        except OSError:  # PermissionError (pid reused by another user) or stat failure
            continue
        shutil.rmtree(entry, ignore_errors=True)
        removed += 1
    return removed


class Worker(Protocol):
    def ensure_model(
        self, model: ModelEntry, budget: Budget, model_path: Path | None = None
    ) -> None: ...
    def run(
        self,
        batch: list[Record],
        params: TaskParams | None = None,
        on_progress: Callable[[int], None] | None = None,
    ) -> list[Record]: ...
    def unload(self) -> None: ...
    def interrupt(self) -> None: ...
    def kill(self) -> None: ...
    def close(self) -> None: ...


class _SignalledError(Exception):
    """A synchronous pre-upload heartbeat returned a stop signal (cancel / stale)."""

    def __init__(self, signal: HeartbeatSignal) -> None:
        super().__init__(signal.name)
        self.signal = signal


class _FailureError(Exception):
    """A subtask failed locally. `release` hands it back without burning the retry budget."""

    def __init__(self, retryable: bool, code: str, message: str, *, release: bool = False) -> None:
        super().__init__(code)
        self.retryable, self.code, self.message, self.release = retryable, code, message, release


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
        max_consecutive_failures: int = 5,
        failure_pause_s: float = 300.0,
        complete_retries: int = 5,
        allow_http_loopback: bool = False,
        heartbeat_interval_s: float = 300.0,
        heartbeat_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        idle_unload_s: float = 600.0,
        clock: Callable[[], float] = time.monotonic,
        model_cache: ModelCache | None = None,
    ) -> None:
        self._cache = model_cache
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
        self._max_fails = max_consecutive_failures
        self._fail_pause_s = failure_pause_s
        self._complete_retries = complete_retries
        self._allow_http = allow_http_loopback
        self._hb_interval = heartbeat_interval_s
        self._hb_sleep = heartbeat_sleep
        self._stop = asyncio.Event()
        self._idle = 0
        self._fails = 0
        self._idle_unload_s = idle_unload_s
        self._clock = clock
        self._last_work = clock()
        self._unloaded = False

    def stop(self) -> None:
        self._stop.set()

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    def _delay(self, n: int) -> float:
        lo, hi = self._backoff
        return float(min(hi, lo * (2 ** min(n, 16))) * random.uniform(0.5, 1.0))  # noqa: S311

    async def _pause(self, delay: float) -> None:
        if self._sleep_fn is not None:
            await self._sleep_fn(delay)
            return
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=delay)

    async def _sleep(self) -> None:
        delay = self._delay(self._idle)
        self._idle += 1
        await self._pause(delay)

    async def _back_off_after_failure(self) -> None:
        """Exponential delay per consecutive failure; a long pause after too many in a row."""
        self._fails += 1
        if self._fails >= self._max_fails:
            log.warning("too_many_consecutive_failures", pause_s=self._fail_pause_s)
            self._fails = 0
            await self._pause(self._fail_pause_s)
        else:
            await self._pause(self._delay(self._fails - 1))

    async def _register_with_retry(self, req: RegisterRequest) -> None:
        attempt = 0
        while not self.stopping:
            try:
                await self._client.register(req)
                return
            except (httpx.HTTPError, ApiError) as exc:
                if isinstance(exc, ApiError) and exc.status_code < 500:
                    raise
                log.warning("register_failed", error=type(exc).__name__)
                await self._pause(self._delay(attempt))
                attempt += 1

    async def run(self, max_subtasks: int | None = None) -> int:
        """Run until stopped (or `max_subtasks` handled). Returns subtasks completed."""
        done = 0
        await asyncio.to_thread(cleanup_stale_tmp, self._tmp_root)
        pid_prefix = f"{TMP_PREFIX}{os.getpid()}-"
        tmp = Path(tempfile.mkdtemp(prefix=pid_prefix, dir=self._tmp_root))  # mode 0700
        try:
            if self._register is not None:
                await self._register_with_retry(self._register)
            while not self.stopping and (max_subtasks is None or done < max_subtasks):
                try:
                    assignment = await self._client.acquire(
                        AcquireRequest(engines=self._engines, budget=self._budget)
                    )
                except (httpx.HTTPError, ApiError, ValueError) as exc:
                    if isinstance(exc, ApiError) and exc.status_code < 500:
                        raise
                    log.warning("acquire_failed", error=type(exc).__name__)
                    await self._maybe_unload()  # a warm model must not outlive a server outage
                    await self._sleep()
                    continue
                if assignment is None:
                    await self._maybe_unload()
                    await self._sleep()
                    continue
                self._idle = 0
                self._unloaded = False
                outcome = await self._handle(assignment, tmp)
                self._last_work = self._clock()
                if outcome is _Outcome.DONE:
                    self._fails = 0
                    done += 1
                elif outcome is _Outcome.FAILED:
                    await self._back_off_after_failure()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
            await asyncio.to_thread(self._worker.close)
            if self._owns_http:
                await self._http.aclose()
        return done

    async def _maybe_unload(self) -> None:
        """Free model memory once idle for `idle_unload_s` (invariant 6); 0 disables."""
        if self._unloaded or self._idle_unload_s <= 0:
            return
        if self._clock() - self._last_work < self._idle_unload_s:
            return
        self._unloaded = True
        try:
            await asyncio.to_thread(self._worker.unload)
            log.info("worker_unloaded_idle")
        except Exception as exc:  # a failed unload must not stop the loop
            log.warning("idle_unload_failed", error=type(exc).__name__)

    def _check_admission(self, a: Assignment) -> None:
        """Release (not fail) a subtask this executor cannot run. The server's RELEASE_LIMIT
        bounds how often one subtask can bounce; the reason is logged here only (release carries
        no reason body)."""
        m = a.model
        if m.engine in self._engines and m.memory_gb <= self._budget.memory_gb:
            return
        raise _FailureError(
            True, MODEL_NOT_ADMISSIBLE, "model not admissible on this executor", release=True
        )

    async def _handle(self, a: Assignment, tmp: Path) -> _Outcome:
        bound = log.bind(
            acquirement_id=str(a.acquirement_id),
            team=a.team,
            project=a.project,
            task_name=a.task_name,
            subtask_id=a.subtask_id,
            attempt=a.attempt,
        )
        hb = Heartbeater(
            self._client,
            a.acquirement_id,
            interval_s=self._hb_interval,
            budget=self._budget,
            total=a.record_count,
            initial_output_url=a.output_url,
            initial_expires_at=a.expires_at,
            sleep=self._hb_sleep,
        )
        work = asyncio.create_task(self._execute(a, tmp, hb))
        stopper = asyncio.create_task(self._stop.wait())
        beat = asyncio.create_task(hb.run())
        try:
            pending: set[asyncio.Future[Any]] = {work, stopper, beat}
            while not work.done():
                await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                if work.done():
                    break
                if stopper.done():
                    # SIGTERM/stop mid-subtask: reclaim worker memory, hand the subtask back.
                    await asyncio.to_thread(self._worker.kill)
                    work.cancel()
                    await asyncio.gather(work, return_exceptions=True)
                    await self._release(a, bound)
                    return _Outcome.STOPPED
                if beat.cancelled() or beat.exception() is not None:
                    # A broken heartbeater is "no signal": the work carries on.
                    exc = None if beat.cancelled() else beat.exception()
                    bound.warning("heartbeat_task_error", error=type(exc).__name__)
                    pending.discard(beat)
                    continue
                return await self._on_signal(a, work, beat.result(), bound)
            try:
                result = work.result()
            except _SignalledError as s:
                return await self._on_signal(a, work, s.signal, bound, interrupt=False)
            except _FailureError as f:
                await self._report_failure(a, f, bound)
                return _Outcome.FAILED
            except StaleAcquirementError:
                bound.info("stale_discarded")
                return _Outcome.DISCARDED
            except Exception as exc:  # poison pill: never let one subtask kill the loop
                bound.warning("unexpected_error", error=type(exc).__name__)
                err = _FailureError(True, "EXECUTOR_ERROR", "unexpected executor error")
                await self._report_failure(a, err, bound)
                return _Outcome.FAILED
            return await self._complete(a, result, bound)
        finally:
            stopper.cancel()
            beat.cancel()  # also during _complete: the lease is kept alive until we return
            if not work.done():
                work.cancel()
            await asyncio.gather(stopper, beat, work, return_exceptions=True)
            # The work task has truly finished (incl. a shielded file write): nothing may linger.
            for p in self._paths(a, tmp):
                p.unlink(missing_ok=True)

    @staticmethod
    def _paths(a: Assignment, tmp: Path) -> tuple[Path, Path]:
        return tmp / f"{a.acquirement_id}.in.jsonl", tmp / f"{a.acquirement_id}.out.jsonl"

    async def _on_signal(
        self,
        a: Assignment,
        work: asyncio.Task[CompleteRequest],
        sig: HeartbeatSignal,
        bound: Any,
        *,
        interrupt: bool = True,
    ) -> _Outcome:
        """Cancel/409 from a heartbeat: stop the work now and free the GPU (invariant 6).

        The worker is interrupted (killed, respawnable) so a cancelled task holds no model memory;
        nothing is uploaded or completed. Cancel is released (server records CANCELLED); a stale
        acquirement reports nothing.
        """
        if interrupt:  # a signal after the work finished leaves the warm worker alone
            await asyncio.to_thread(self._worker.interrupt)
        work.cancel()
        await asyncio.gather(work, return_exceptions=True)
        if sig is HeartbeatSignal.CANCEL:
            await self._release(a, bound)
            return _Outcome.CANCELLED
        bound.info("stale_discarded")
        return _Outcome.DISCARDED

    async def _complete(self, a: Assignment, result: CompleteRequest, bound: Any) -> _Outcome:
        """Report completion, retrying transport errors/5xx (the server treats a same-owner
        repeat as success). Other 4xx are reported as a retryable failure."""
        attempt = 0
        saw_5xx = False
        while True:
            try:
                await self._client.complete(a.acquirement_id, result)
                bound.info("subtask_completed", records=result.record_count)
                return _Outcome.DONE
            except StaleAcquirementError:
                bound.info("stale_discarded")
                return _Outcome.DISCARDED
            except ApiError as exc:
                if exc.status_code == 409:
                    bound.warning("complete_conflict", code=exc.code)
                    return _Outcome.DISCARDED
                if exc.status_code < 500:
                    bound.warning("complete_rejected", status=exc.status_code)
                    err = _FailureError(True, "COMPLETE_REJECTED", f"HTTP {exc.status_code}")
                    await self._report_failure(a, err, bound)
                    return _Outcome.FAILED
                saw_5xx = True
                bound.warning("complete_failed", error=type(exc).__name__, attempt=attempt)
            except httpx.HTTPError as exc:
                bound.warning("complete_failed", error=type(exc).__name__, attempt=attempt)
            attempt += 1
            if self.stopping:
                await self._release(a, bound)
                return _Outcome.STOPPED
            if attempt >= self._complete_retries:
                bound.error("complete_gave_up", server_error=saw_5xx)
                if saw_5xx:  # deterministic server fault: a release would livelock; burn budget
                    err = _FailureError(True, "COMPLETE_FAILED", "complete failed")
                    await self._report_failure(a, err, bound)
                # transport-only: leave it to the watchdog (counts EXPIRED)
                return _Outcome.FAILED
            await self._pause(self._delay(attempt - 1))

    def _check_url(self, url: str) -> None:
        try:
            require_secure_url(url)
            if urlsplit(url).scheme != "https" and not self._allow_http:
                raise ValueError("http not allowed")
        except ValueError:
            raise _FailureError(True, "INSECURE_URL", "presigned URL is not https") from None

    async def _execute(self, a: Assignment, tmp: Path, hb: Heartbeater) -> CompleteRequest:
        in_path, out_path = self._paths(a, tmp)
        write: asyncio.Future[tuple[int, str]] | None = None
        try:
            self._check_admission(a)
            try:
                params = parse_params(a.model.type, a.params)
            except ValidationError:
                raise _FailureError(False, BAD_PARAMS, "invalid task params") from None
            self._check_url(a.input_url)
            self._check_url(a.output_url)
            try:
                await io.download_range(self._http, a.input_url, a.byte_start, a.byte_end, in_path)
                records = await asyncio.to_thread(io.parse_jsonl, in_path, a.record_count)
            except io.BadInputError as exc:
                raise _FailureError(False, BAD_INPUT, str(exc)) from None
            except (io.InputUnavailableError, httpx.HTTPError) as exc:
                msg = type(exc).__name__
                # Only plain connectivity trouble is released; a server verdict (403/404/5xx), a
                # backend ignoring Range or a short body would livelock if released: fail it.
                local = isinstance(exc, LOCAL_TRANSPORT_ERRORS)
                raise _FailureError(True, "INPUT_UNAVAILABLE", msg, release=local) from None
            except OSError as exc:
                msg = type(exc).__name__
                raise _FailureError(True, "LOCAL_IO_ERROR", msg, release=True) from None
            outputs = await self._run_batches(a, records, hb, params)
            if len(outputs) != len(records):
                raise _FailureError(True, "OUTPUT_MISMATCH", "runner returned wrong record count")
            for out in outputs:  # stamp provenance on every line (incl. per-record errors)
                out["model"] = a.model.model_id
                out["model_revision"] = a.model.revision
            try:
                # Shielded: a cancel must not leave the thread writing after cleanup (see finally).
                write = asyncio.ensure_future(asyncio.to_thread(io.write_jsonl, out_path, outputs))
                size, digest = await asyncio.shield(write)
            except OSError as exc:
                msg = type(exc).__name__
                raise _FailureError(True, "LOCAL_IO_ERROR", msg, release=True) from None
            if not hb.url_fresh(UPLOAD_URL_MARGIN_S):  # long run or unknown expiry: refresh now
                signal, _ = await hb.beat(retry_rate_limited=True)
                if signal is not None:
                    raise _SignalledError(signal)
            output_url = hb.output_url  # refreshed by heartbeats; same attempt-scoped key
            self._check_url(output_url)
            try:
                await io.upload_file(self._http, output_url, out_path)
            except (httpx.HTTPError, OSError) as exc:
                msg = type(exc).__name__
                local = isinstance(exc, (*LOCAL_TRANSPORT_ERRORS, OSError))
                raise _FailureError(True, "UPLOAD_FAILED", msg, release=local) from None
            return CompleteRequest(record_count=len(outputs), byte_size=size, sha256=digest)
        finally:
            if write is not None and not write.done():
                await asyncio.wait({write})  # let the writer thread finish before unlinking
            in_path.unlink(missing_ok=True)
            out_path.unlink(missing_ok=True)

    async def _ensure_cached(self, model: ModelEntry) -> Path | None:
        if self._cache is None:
            return None
        try:
            return await self._cache.ensure(model)
        except ModelFetchError as exc:
            if not (exc.code == MODEL_CACHE_FULL and exc.blocked_by_in_use):
                raise
        # The new model fits the cap only if the loaded model's files may go: drop the warm
        # worker (frees its memory too), unprotect that model and retry once.
        log.info("model_cache_unload_for_space")
        await asyncio.to_thread(self._worker.unload)
        self._cache.release_in_use()
        return await self._cache.ensure(model)

    async def _run_batches(
        self, a: Assignment, records: list[Record], hb: Heartbeater, params: TaskParams
    ) -> list[Record]:
        step = self._budget.max_batch_size
        outputs: list[Record] = []
        aloop = asyncio.get_running_loop()

        def progress(base: int) -> Callable[[int], None]:
            def on_progress(done: int) -> None:  # runs in the worker-call thread
                total = min(base + done, len(records))
                with contextlib.suppress(RuntimeError):  # loop already closed
                    aloop.call_soon_threadsafe(_set_done, total)

            return on_progress

        def _set_done(n: int) -> None:
            hb.records_done = max(hb.records_done, n)

        try:
            model_path = await self._ensure_cached(a.model)
            if model_path is None:
                await asyncio.to_thread(self._worker.ensure_model, a.model, self._budget)
            else:
                await asyncio.to_thread(
                    self._worker.ensure_model, a.model, self._budget, model_path
                )
            for i in range(0, len(records), step):
                batch = records[i : i + step]
                outputs.extend(
                    await asyncio.to_thread(self._worker.run, batch, params, progress(i))
                )
                hb.records_done = len(outputs)
        except WorkerCrashed:
            raise _FailureError(True, "WORKER_CRASHED", "worker process died") from None
        except RunnerError as exc:
            code = exc.code[:64] or "RUNNER_ERROR"
            # Engine missing on this Mac is executor-local: release, don't burn an attempt.
            release = exc.code == ENGINE_UNAVAILABLE or (
                isinstance(exc, ModelFetchError) and exc.release
            )
            raise _FailureError(exc.retryable, code, "runner failed", release=release) from None
        return outputs

    async def _report_failure(self, a: Assignment, f: _FailureError, bound: Any) -> None:
        bound.warning("subtask_failed", code=f.code, retryable=f.retryable, released=f.release)
        if f.release:  # executor-local trouble: don't burn the subtask's retry budget
            await self._release(a, bound)
            return
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
