"""Executor control loop: acquire -> download -> run in worker -> upload -> complete.

Talks to the server only through `ExecutorClient` (invariant 1) and to S3 only through presigned
URLs (invariant 4). Inference happens in the worker subprocess (invariant 7). Logs carry ids only.
"""

from __future__ import annotations

import asyncio
import contextlib
import enum
import math
import os
import random
import re
import shutil
import tempfile
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime
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
from minion_common.enums import CpuPriority, Engine
from minion_common.schemas import (
    MAX_OUTPUT_BYTES_DEFAULT,
    AcquireRequest,
    Assignment,
    Budget,
    CompleteRequest,
    FailRequest,
    ModelEntry,
    PresenceRequest,
    RegisterRequest,
    ReleaseRequest,
    TaskParams,
    parse_params,
)
from pydantic import ValidationError

from minion_executor import io
from minion_executor.capacity.governor import Decision
from minion_executor.capacity.profiles import GIB
from minion_executor.capacity.runtime import FixedGovernor, Governor
from minion_executor.capacity.safefile import SafeFileError
from minion_executor.capacity.status import (
    STATUS_INTERVAL_S,
    StatusFile,
    StatusSignals,
    StatusSubtask,
    StatusToday,
    write_status,
)
from minion_executor.heartbeat import Heartbeater, HeartbeatSignal
from minion_executor.model_cache import MODEL_CACHE_FULL, ModelCache, ModelFetchError
from minion_executor.runners.base import ENGINE_UNAVAILABLE, Record, RunnerError
from minion_executor.worker.process import WorkerCrashed
from minion_executor.worker.sandbox import SANDBOX_FAILED

log = structlog.get_logger("minion_executor.loop")

BAD_INPUT = "BAD_INPUT"
BAD_PARAMS = "BAD_PARAMS"
MODEL_NOT_ADMISSIBLE = "MODEL_NOT_ADMISSIBLE"
TMP_PREFIX = "minion-exec-"
# Refresh the output URL with one synchronous heartbeat when it expires within this margin.
UPLOAD_URL_MARGIN_S = 60.0
# One absolute budget for everything after a hard stop (grace over / second signal): the complete
# window, release, fail, worker kill and close all draw from it, so a slow server or wedged worker
# cannot hold the exit past grace + SHUTDOWN_BUDGET_S (+ MIN_CALL_S per call as reaping slack).
SHUTDOWN_BUDGET_S = 10.0
MAX_RETRY_AFTER_S = 300.0
MIN_CALL_S = 0.5
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
    RELEASED = enum.auto()  # governor released it (pause, downgrade, critical): no back-off


class _Verdict(enum.Enum):
    """Why the governor watcher ended the in-flight subtask (values are the log codes)."""

    PAUSE_EXPIRED = "PAUSE_RELEASE"  # paused >= pause_release_after_s: release
    MEMORY_CRITICAL = "MEMORY_CRITICAL_RELEASE"  # memory pressure critical: release at once
    DOWNGRADE_TIMEOUT = "BUDGET_DOWNGRADE"  # batch overran the 60 s bound after a downgrade
    OOM = "OOM"  # worker footprint > 1.1 x budget twice (D7): retryable failure
    PRIORITY = "PRIORITY_VIOLATION"  # worker less throttled than its budget (D8): respawn
    WATCHER_FAILED = "WATCHER_FAILED"  # the watcher itself crashed: fail closed, release


class GovernorStoppedError(RuntimeError):
    """The governor task ended: nothing steers the budget any more, so the executor stops
    (invariant 6) instead of running on a frozen decision."""


class _DowngradeError(Exception):
    """The memory budget fell below the loaded model at a batch boundary (FR-54)."""


MEMORY_GUARD_FACTOR = 1.10  # D7: footprint over budget x 1.10 on two consecutive samples
MEMORY_GUARD_SAMPLES = 2


def _is_shed(exc: Exception) -> bool:
    """429, or a 503 that carries a numeric Retry-After (server overload shedding, TASK-7.4.4):
    both are "come back later", not a fault."""
    return isinstance(exc, ApiError) and (
        exc.status_code == 429 or (exc.status_code == 503 and exc.retry_after is not None)
    )


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


def _same_model(a: ModelEntry, b: ModelEntry) -> bool:
    """Model identity for warmness: the pinned (model_id, revision), not the whole entry."""
    return (a.model_id, a.revision) == (b.model_id, b.revision)


def _local_today() -> date:
    return datetime.now().astimezone().date()


class Worker(Protocol):
    def ensure_model(
        self, model: ModelEntry, budget: Budget, model_path: Path | None = None
    ) -> None: ...
    def run(
        self,
        batch: list[Record],
        params: TaskParams | None = None,
        on_progress: Callable[[int], None] | None = None,
        budget: Budget | None = None,
    ) -> list[Record]: ...
    def control(self, *, duty: float, paused: bool, trim: bool = False) -> int: ...
    def footprint_bytes(self) -> int | None: ...
    def priority_ok(self) -> bool | None: ...
    def gpu_share(self) -> float | None: ...
    def unload(self) -> None: ...
    def interrupt(self) -> None: ...
    def kill(self) -> None: ...
    def close(self) -> None: ...


class _SignalledError(Exception):
    """A synchronous pre-upload heartbeat returned a stop signal (cancel / stale)."""

    def __init__(self, signal: HeartbeatSignal) -> None:
        super().__init__(signal.name)
        self.signal = signal


_CODE_RE = re.compile(r"^[A-Z0-9_]{1,64}$")
RELEASE_FALLBACK = "EXECUTOR_RELEASE"
FAIL_FALLBACK = "RUNNER_ERROR"


def _release_code(code: str) -> str:
    """A release reason the server accepts; anything else (e.g. a runner's free-form code)
    becomes the fixed fallback so a bad code can never break the release."""
    up = code.upper()
    return up if _CODE_RE.fullmatch(up) else RELEASE_FALLBACK


def _fail_code(code: str) -> str:
    """A failure code the server accepts: upper-cased, or the fallback when still invalid."""
    up = code.upper()
    return up if _CODE_RE.fullmatch(up) else FAIL_FALLBACK


class _HardStoppedError(Exception):
    """A server call was abandoned because the hard stop fired and its time ran out."""


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
        complete_shed_budget_s: float = 120.0,
        allow_http_loopback: bool = False,
        heartbeat_interval_s: float = 300.0,
        heartbeat_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        idle_unload_s: float = 600.0,
        clock: Callable[[], float] = time.monotonic,
        model_cache: ModelCache | None = None,
        stop_grace_s: float = 0.0,
        governor: Governor | None = None,
        presence_interval_s: float = 60.0,
        downgrade_batch_s: float = 60.0,
        guard_interval_s: float = 2.0,
        priority_grace_s: float = 60.0,
        status_dir: Path | None = None,
        status_interval_s: float = STATUS_INTERVAL_S,
        acquire_pace_s: float = 0.0,
        first_acquire_jitter_s: float = 0.0,
        max_output_bytes: int = MAX_OUTPUT_BYTES_DEFAULT,
    ) -> None:
        self._max_output_bytes = max_output_bytes
        self._cache = model_cache
        self._acquire_pace_s = acquire_pace_s  # min gap between acquire calls; 0 disables
        self._first_acquire_jitter_s = first_acquire_jitter_s  # ADR-0013 D5; 0 disables
        self._last_acquire: float | None = None
        self._client = client
        self._worker = worker
        # `budget` seeds a FixedGovernor when no governor is given; with one, the governor's live
        # decision is the only source of the budget (never budget_for(profile): a hold lowers it)
        self._gov: Governor = governor if governor is not None else FixedGovernor(budget)
        self._presence_s = presence_interval_s
        self._downgrade_batch_s = downgrade_batch_s  # FR-54: finish the batch, bounded
        self._guard_s = guard_interval_s  # footprint / priority sampling period (D7)
        self._prio_grace_s = priority_grace_s  # a new priority applies at the worker's next batch
        self._status_dir = status_dir  # None: no status.json (tests, bench)
        self._status_s = status_interval_s
        self._status_kick = asyncio.Event()  # set when something status-relevant changed
        self._status_note: str | None = None
        self._started_at = datetime.now(UTC)
        self._current: tuple[Assignment, Heartbeater] | None = None
        self._today: tuple[date, int, int] = (_local_today(), 0, 0)  # date, subtasks, records
        self._ctl_key: tuple[float, bool] | None = None  # last (duty, paused) pushed to the worker
        self._ctl_lock = asyncio.Lock()  # one push at a time (watcher and batch loop both push)
        # per-subtask inference state: the budget the running batch started with (the memory
        # guard measures against it until the next batch boundary), whether inference has ended
        # (then the watcher may no longer end the subtask) and whether it ended successfully
        self._batch_budget: Budget | None = None
        self._infer_done = asyncio.Event()
        self._infer_ok = False
        self._batch_cpu: CpuPriority | None = None  # cpu priority of the previous batch (D8 grace)
        self._grace_until = 0.0
        self._trim_pushed = False
        self._presence_key: tuple[object, object] | None = None
        self._presence_due = 0.0
        self._presence_blocked_until = 0.0  # presence 429: no ping, even a changed one, before this
        # Monotonic (self._clock) deadline of a 429 on acquire: a back-off cut short by a pause
        # must still be waited out after the resume, or the first acquire would 429 again.
        self._rate_limited_until: float | None = None
        self._loaded: ModelEntry | None = None  # model the warm worker holds (None: nothing)
        self._loaded_path: Path | None = None  # cache path pinned for `_loaded` (None: unpinned)
        self._unload_fut: asyncio.Future[None] | None = None  # in-flight watcher/idle unload
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
        # Wall-clock cap on 503+Retry-After (shed) answers to one complete; sheds do not use up
        # `complete_retries`. Heartbeats continue while it retries; past the cap the subtask is
        # released (SERVER_BUSY), not left to the watchdog (TASK-7.4.4).
        self._complete_shed_budget_s = complete_shed_budget_s
        self._allow_http = allow_http_loopback
        self._hb_interval = heartbeat_interval_s
        self._hb_sleep = heartbeat_sleep
        self._stop = asyncio.Event()  # first signal: no new acquires, grace period starts
        self._hard_stop = asyncio.Event()  # grace over / second signal: kill + release now
        self._grace_s = stop_grace_s
        self._grace_timer: asyncio.TimerHandle | None = None
        self._deadline: float | None = None  # monotonic; set once, when the hard stop is set
        self._kill_timed_out = False
        self._idle = 0
        self._fails = 0
        self._idle_unload_s = idle_unload_s
        self._clock = clock
        self._last_work = clock()
        self._unloaded = False
        self._ensure_started = False

    def stop(self) -> None:
        """First call: stop acquiring and give the in-flight subtask `stop_grace_s` to finish
        (heartbeats keep running). A second call, or the grace expiring, hard-stops: the worker is
        killed and the subtask released. Grace 0 hard-stops at once. Call from the event loop."""
        if self._stop.is_set():
            self._set_hard_stop()
            return
        self._stop.set()
        if not math.isfinite(self._grace_s) or self._grace_s <= 0:
            self._set_hard_stop()
            return
        try:
            self._grace_timer = asyncio.get_running_loop().call_later(
                self._grace_s, self._set_hard_stop
            )
        except RuntimeError:  # no running loop: nothing can be in flight
            self._set_hard_stop()

    def _set_hard_stop(self) -> None:
        if self._deadline is None:
            self._deadline = time.monotonic() + SHUTDOWN_BUDGET_S
        self._hard_stop.set()

    def _left(self) -> float:
        """Seconds left of the shared shutdown budget, floored at MIN_CALL_S so a call still gets
        a real attempt. Before the hard stop (a call during grace) each call gets a full budget
        of its own; the shared deadline is only fixed by `_set_hard_stop` or `run`'s cleanup."""
        deadline = self._deadline
        if deadline is None:
            deadline = time.monotonic() + SHUTDOWN_BUDGET_S
        return max(MIN_CALL_S, deadline - time.monotonic())

    def _call_timeout(self) -> float | None:
        """Bound for a server call: the shared budget once stopping, else unbounded."""
        if self._stop.is_set() or self._hard_stop.is_set():
            return self._left()
        return None

    async def _guarded(
        self,
        awaitable: Awaitable[Any],
        trigger: asyncio.Event,
        window: Callable[[], float],
    ) -> Any:
        """Await `awaitable`, but once `trigger` is set give it only `window()` more seconds; then
        cancel it and raise `_HardStoppedError` (the outcome is unknown: the request may have
        reached the server)."""
        call = asyncio.ensure_future(awaitable)
        stopper = asyncio.ensure_future(trigger.wait())
        try:
            await asyncio.wait({call, stopper}, return_when=asyncio.FIRST_COMPLETED)
            if not call.done():
                await asyncio.wait({call}, timeout=window())
                if not call.done():
                    raise _HardStoppedError
            return call.result()
        finally:
            stopper.cancel()
            if not call.done():
                call.cancel()
            await asyncio.gather(call, stopper, return_exceptions=True)

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    def _delay(self, n: int) -> float:
        lo, hi = self._backoff
        return float(min(hi, lo * (2 ** min(n, 16))) * random.uniform(0.5, 1.0))  # noqa: S311

    def _rate_limit_delay(self, exc: Exception, n: int, *, repeated: bool = False) -> float:
        """Backoff for attempt `n`. A first 429 (or a 503 shed, TASK-7.4.4) waits its numeric
        `Retry-After` (cap 300 s) plus 0-20 % jitter; when they repeat (`repeated`) it escalates to
        max(backoff, Retry-After)."""
        if _is_shed(exc) and isinstance(exc, ApiError) and exc.retry_after is not None:
            ra = min(exc.retry_after, MAX_RETRY_AFTER_S)
            if repeated:
                return max(self._delay(n), ra * random.uniform(1.0, 1.2))  # noqa: S311
            return float(ra * random.uniform(1.0, 1.2))  # noqa: S311
        return self._delay(n)

    async def _pause(self, delay: float, *, until: asyncio.Event | None = None) -> None:
        if self._sleep_fn is not None:
            await self._sleep_fn(delay)
            return
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for((until or self._stop).wait(), timeout=delay)

    async def _sleep(self, version: int | None = None, *, delay: float | None = None) -> None:
        """Idle back-off. `version` is the governor version read *before* the acquire call, so a
        change that arrived during it is not missed. `delay` overrides the idle back-off (429)."""
        if delay is None:
            delay = self._delay(self._idle)
            self._idle += 1
        if self._sleep_fn is not None:
            await self._sleep_fn(delay)
            return
        # Idle back-off that still reacts to the governor: a pause ends the sleep (presence goes
        # out at once), any other change gets the idle-worker budget check.
        if self._gov.decision.paused:
            return
        deadline = time.monotonic() + delay
        version = self._gov.version if version is None else version
        while (left := deadline - time.monotonic()) > 0 and not self.stopping:
            new = await self._wait_gov(version, left)
            if new == version:
                break  # timed out
            version = new
            await self._enforce_idle_budget()
            if self._gov.decision.paused:
                break

    async def _wait_gov(self, version: int, timeout_s: float) -> int:
        """Wait for a governor change, the first stop signal or `timeout_s`; returns the version."""
        change = asyncio.ensure_future(self._gov.wait_change(version, timeout_s))
        stopper = asyncio.ensure_future(self._stop.wait())
        try:
            await asyncio.wait({change, stopper}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            change.cancel()
            stopper.cancel()
            await asyncio.gather(change, stopper, return_exceptions=True)
        return self._gov.version

    def _next_wake(self) -> float:
        now = self._clock()
        wake = max(0.05, self._presence_due - now)
        if self._idle_unload_s > 0 and not self._unloaded:
            wake = min(wake, max(0.05, self._idle_unload_s - (now - self._last_work)))
        return wake

    async def _paused_cycle(self) -> None:
        """One round while paused: presence if due, free an idle worker the budget no longer
        covers, then wait for a governor change, the next presence or the idle unload."""
        version = self._gov.version
        await self._maybe_presence(self._gov.decision)
        await self._maybe_unload()
        await self._enforce_idle_budget()
        await self._wait_gov(version, self._next_wake())

    async def _maybe_presence(self, d: Decision) -> None:
        """FR-55: tell the server we are alive but paused: on entering the pause, whenever the
        reason or budget changes, and every `presence_interval_s`."""
        key = (d.paused_reason, d.budget)
        now = self._clock()
        if now < self._presence_blocked_until:
            return
        if key == self._presence_key and now < self._presence_due:
            return
        req = PresenceRequest(
            budget=d.budget, paused_reason=d.paused_reason.value if d.paused_reason else None
        )
        ok = False
        try:
            await self._guarded(self._client.presence(req), self._stop, lambda: SHUTDOWN_BUDGET_S)
            ok = True
        except _HardStoppedError:
            log.warning("presence_abandoned_at_stop")
        except (httpx.HTTPError, ApiError) as exc:
            if isinstance(exc, ApiError) and exc.status_code in (401, 403):
                raise  # the credential is gone: same as a refused acquire
            log.warning("presence_failed", error=type(exc).__name__)
            if isinstance(exc, ApiError) and exc.status_code == 429:
                # keep the key: the (changed) state is sent once the limit allows it
                wait = self._rate_limit_delay(exc, 0) if exc.retry_after is not None else 10.0
                self._presence_blocked_until = now + wait
                self._presence_due = self._presence_blocked_until
                return
        self._presence_key = key
        self._presence_due = now + (self._presence_s if ok else min(self._presence_s, 10.0))

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
        limited_n = 0
        while not self.stopping:
            try:
                await self._guarded(
                    self._client.register(req), self._stop, lambda: SHUTDOWN_BUDGET_S
                )
                return
            except _HardStoppedError:
                # A register that landed is idempotent; the loop below exits on the stop.
                log.warning("register_abandoned_at_stop")
                return
            except (httpx.HTTPError, ApiError) as exc:
                limited = _is_shed(exc)
                if isinstance(exc, ApiError) and exc.status_code < 500 and not limited:
                    raise
                log.warning("register_failed", error=type(exc).__name__)
                if limited:  # 429s are counted separately, like the acquire loop's limited_n
                    limited_n += 1
                await self._pause(
                    self._rate_limit_delay(exc, attempt, repeated=limited and limited_n > 1)
                )
                attempt += 1

    async def run(self, max_subtasks: int | None = None) -> int:
        """Run until stopped (or `max_subtasks` handled). Returns subtasks completed."""
        done = 0
        limited_n = 0
        await asyncio.to_thread(cleanup_stale_tmp, self._tmp_root)
        pid_prefix = f"{TMP_PREFIX}{os.getpid()}-"
        tmp = Path(tempfile.mkdtemp(prefix=pid_prefix, dir=self._tmp_root))  # mode 0700
        gov_task: asyncio.Task[None] | None = None
        status_task: asyncio.Task[None] | None = None
        try:
            await self._gov.prime()  # a real first decision before anything is acquired
            gov_task = asyncio.create_task(self._gov.run())
            if self._status_dir is not None:
                status_task = asyncio.create_task(self._status_loop(self._status_dir))
            if self._register is not None:
                reg = self._register
                if isinstance(reg, RegisterRequest):  # announce the primed budget, not the seed
                    d0 = self._gov.decision
                    reg = reg.model_copy(update={"profile": d0.profile, "budget": d0.budget})
                await self._register_with_retry(reg)
            if self._first_acquire_jitter_s > 0 and not self.stopping:
                # Spread a fleet that restarts together off the queue head (ADR-0013 D5).
                await self._pause(random.uniform(0, self._first_acquire_jitter_s))  # noqa: S311
            while not self.stopping and (max_subtasks is None or done < max_subtasks):
                if gov_task.done():  # nothing steers the budget any more: stop, do not acquire
                    exc = None if gov_task.cancelled() else gov_task.exception()
                    log.error(
                        "governor_task_ended", error=type(exc).__name__ if exc else "returned"
                    )
                    raise GovernorStoppedError("the capacity governor stopped")
                if self._gov.decision.paused:  # FR-53: no acquires while paused (presence only)
                    await self._paused_cycle()
                    continue
                self._presence_key = None
                version = self._gov.version
                if (until := self._rate_limited_until) is not None:
                    if (left := until - self._clock()) > 0:
                        await self._sleep(version, delay=left)  # rest of a 429 back-off
                        if not self._gov.decision.paused:  # a pause cut it short: keep the rest
                            self._rate_limited_until = None
                        continue
                    self._rate_limited_until = None
                if self._acquire_pace_s > 0 and self._last_acquire is not None:
                    # Keep >= pace between acquires (instant subtasks would trip the server's
                    # limit): sleep only the remainder, none after a subtask that took longer.
                    gap = self._last_acquire + self._acquire_pace_s - self._clock()
                    if gap > 0:
                        await self._pause(gap)
                        if self.stopping:
                            break
                version = self._gov.version
                self._last_acquire = self._clock()
                try:
                    assignment = await self._guarded(
                        self._client.acquire(
                            AcquireRequest(engines=self._engines, budget=self._gov.decision.budget)
                        ),
                        self._stop,
                        lambda: SHUTDOWN_BUDGET_S,
                    )
                except _HardStoppedError:
                    # An acquirement that landed server-side is not ours to hold: just exit, the
                    # watchdog reclaims it when its first heartbeat never arrives.
                    log.warning("acquire_abandoned_at_stop")
                    break
                except (httpx.HTTPError, ApiError, ValueError) as exc:
                    limited = _is_shed(exc)
                    if isinstance(exc, ApiError) and exc.status_code < 500 and not limited:
                        raise
                    log.warning("acquire_failed", error=type(exc).__name__)
                    await self._maybe_unload()  # a warm model must not outlive a server outage
                    await self._enforce_idle_budget()
                    if limited:  # 429: back off at least Retry-After, also across a pause
                        limited_n += 1
                        delay = self._rate_limit_delay(exc, limited_n - 1, repeated=limited_n > 1)
                        self._rate_limited_until = self._clock() + delay
                        await self._sleep(version, delay=delay)
                        if not self._gov.decision.paused:  # waited it out (a pause keeps the rest)
                            self._rate_limited_until = None
                    else:
                        await self._sleep(version)
                    continue
                limited_n = 0
                if assignment is None:
                    await self._maybe_unload()
                    await self._enforce_idle_budget()
                    await self._sleep(version)
                    continue
                if self.stopping:  # acquired while a stop arrived: hand it back untouched
                    await self._release(
                        assignment, log.bind(subtask_id=assignment.subtask_id), "STOP"
                    )
                    break
                self._idle = 0
                self._unloaded = False
                outcome = await self._handle(assignment, tmp)
                self._last_work = self._clock()
                if outcome is _Outcome.DONE:
                    self._fails = 0
                    done += 1
                    self._note_done(assignment.record_count)
                elif outcome is _Outcome.FAILED:
                    await self._back_off_after_failure()
        finally:
            for bg in (status_task, gov_task):
                if bg is not None:
                    bg.cancel()
                    await asyncio.gather(bg, return_exceptions=True)
            if self._grace_timer is not None:
                self._grace_timer.cancel()
            shutil.rmtree(tmp, ignore_errors=True)
            if self._deadline is None:  # close and its fallback kill share one budget
                self._deadline = time.monotonic() + SHUTDOWN_BUDGET_S
            # a kill that already timed out means close/kill would wedge as well: skip them
            if not self._kill_timed_out and not await self._bounded_thread(self._worker.close):
                await self._bounded_thread(self._worker.kill, is_kill=True)  # lock-free fallback
            if self._status_dir is not None:  # leave a 'stopped' marker: no stale 'running'
                sdir = self._status_dir
                await self._bounded_thread(lambda: self._write_status(sdir, stopped=True))
            if self._owns_http:
                await self._http.aclose()
        return done

    async def _bounded_thread(self, fn: Callable[[], object], *, is_kill: bool = False) -> bool:
        """Run a blocking worker call off-loop, waiting at most the rest of the shared shutdown
        budget. Returns whether it completed.

        On a timeout the *thread keeps running* (Python cannot cancel it); only the wait is
        abandoned. The exit bound therefore relies on every worker-lock holder aborting once the
        handle is `_closed` (`kill()` sets it), so the leftover thread ends on its own and the
        interpreter's executor shutdown does not wait on it."""
        try:
            await asyncio.wait_for(asyncio.to_thread(fn), self._left())
        except TimeoutError:
            log.warning("worker_call_timed_out", call=getattr(fn, "__name__", "?"))
            if is_kill:
                self._kill_timed_out = True
            return False
        except Exception as exc:  # shutdown must continue
            log.warning("worker_call_failed", error=type(exc).__name__)
            return False
        return True

    async def _maybe_unload(self) -> None:
        """Free model memory once idle for `idle_unload_s` (invariant 6); 0 disables."""
        if self._unloaded or self._idle_unload_s <= 0:
            return
        if self._clock() - self._last_work < self._idle_unload_s:
            return
        await self._unload_idle("idle")

    def _forget_loaded(self) -> None:
        self._loaded = None
        self._loaded_path = None

    async def _await_unload(self) -> None:
        """Wait for an unload still running in its thread (the watcher may have been cancelled
        mid-unload): a model load must never overlap it, or the unload would kill the fresh
        worker."""
        while (fut := self._unload_fut) is not None and not fut.done():
            with contextlib.suppress(Exception):
                await asyncio.shield(fut)

    async def _unload_idle(self, reason: str) -> None:
        self._unloaded = True
        unloaded_path = self._loaded_path  # the model actually unloaded, not whatever is pinned now
        self._forget_loaded()
        await self._await_unload()
        fut = asyncio.ensure_future(asyncio.to_thread(self._worker.unload))
        self._unload_fut = fut
        # the unpin and the log run when the unload finishes, even if the watcher awaiting it
        # was cancelled meanwhile (the shielded unload keeps running)
        fut.add_done_callback(lambda f: self._after_unload(f, unloaded_path, reason))
        # a failed unload must not stop the loop: _after_unload logs it
        with contextlib.suppress(Exception):
            await asyncio.shield(fut)

    def _after_unload(
        self, fut: asyncio.Future[None], unloaded_path: Path | None, reason: str
    ) -> None:
        if fut.cancelled():
            return
        if (exc := fut.exception()) is not None:
            log.warning("idle_unload_failed", error=type(exc).__name__)
            return
        # unpin only that model: a subtask may have pinned its own meanwhile
        if (
            self._cache is not None
            and unloaded_path is not None
            and self._cache.in_use == unloaded_path
        ):
            self._cache.release_in_use()
        log.info("worker_unloaded_idle" if reason == "idle" else "worker_unloaded", reason=reason)

    async def _enforce_idle_budget(self) -> None:
        """FR-54 for a warm worker with no subtask: unload at once when the loaded model no longer
        fits the memory budget, or under memory pressure (invariant 6), without waiting for idle."""
        loaded = self._loaded
        if loaded is None:
            return
        d = self._gov.decision
        if loaded.memory_gb > d.budget.memory_gb:
            await self._unload_idle("budget_downgrade")
        elif d.trim or d.release_now:
            await self._unload_idle("memory_pressure")

    def _check_admission(self, a: Assignment) -> None:
        """Release (not fail) a subtask this executor cannot run. The server's RELEASE_LIMIT
        bounds how often one subtask can bounce; the reason is logged here only (release carries
        no reason body)."""
        m = a.model
        if m.engine in self._engines and m.memory_gb <= self._gov.decision.budget.memory_gb:
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
            budget=None,
            budget_fn=lambda: self._gov.decision.budget,
            pause_fn=self._paused_reason,
            total=a.record_count,
            initial_output_url=a.output_url,
            initial_expires_at=a.expires_at,
            sleep=self._hb_sleep,
        )
        # Ack (ADR-0014): one immediate heartbeat before the worker starts, so the server can tell
        # an owned subtask from a lost acquire response. Failures other than 409 are non-fatal.
        signal, _ = await hb.beat()
        if signal is HeartbeatSignal.STALE:
            bound.info("ack_stale_dropped")
            return _Outcome.DISCARDED
        self._current = (a, hb)
        self._status_kick.set()
        self._batch_budget = None
        self._ensure_started = False
        self._infer_done = asyncio.Event()
        self._infer_ok = False
        work = asyncio.create_task(self._execute(a, tmp, hb))
        stopper = asyncio.create_task(self._hard_stop.wait())
        beat = asyncio.create_task(hb.run())
        watch = asyncio.create_task(self._watch(a))
        try:
            pending: set[asyncio.Future[Any]] = {work, stopper, beat, watch}
            while not work.done():
                await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                if work.done():
                    break
                if stopper.done():
                    # Grace over / second signal: reclaim worker memory, hand the subtask back.
                    await self._bounded_thread(self._worker.kill, is_kill=True)
                    if self._cache is not None:
                        self._cache.release_in_use()
                    finished = work.done()  # raced the kill: its result is still good
                    work.cancel()
                    await asyncio.gather(work, return_exceptions=True)
                    if finished and not work.cancelled() and work.exception() is None:
                        return await self._complete(a, work.result(), bound)
                    await self._release(a, bound, "STOP")
                    return _Outcome.STOPPED
                if watch.done():
                    if watch.cancelled() or watch.exception() is not None:
                        # A crashed watcher no longer steers the budget: fail closed. While
                        # inferring that is a verdict (interrupt + release); once inference has
                        # ended only the upload is left and it simply proceeds.
                        exc = None if watch.cancelled() else watch.exception()
                        bound.error("governor_watch_error", error=type(exc).__name__)
                        verdict = _Verdict.WATCHER_FAILED
                    else:
                        verdict = watch.result()
                    outcome = await self._on_verdict(a, work, verdict, bound)
                    if outcome is not None:
                        return outcome
                    pending.discard(watch)  # moot: inference is over, the result stands
                    continue
                if beat.done():
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
            except _DowngradeError:
                # FR-54: the batch is done; hand the subtask back and free the model memory
                bound.warning("budget_downgrade_release", code="BUDGET_DOWNGRADE")
                self._forget_loaded()
                self._unloaded = True
                await self._bounded_thread(self._worker.unload)  # Ollama keep_alive 0, then kill
                if self._cache is not None:
                    self._cache.release_in_use()
                await self._release(a, bound, "BUDGET_DOWNGRADE")
                return _Outcome.RELEASED
            except Exception as exc:  # poison pill: never let one subtask kill the loop
                bound.warning("unexpected_error", error=type(exc).__name__)
                err = _FailureError(True, "EXECUTOR_ERROR", "unexpected executor error")
                await self._report_failure(a, err, bound)
                return _Outcome.FAILED
            if self.stopping:  # exiting: free GPU memory now, before the (possibly slow) complete
                await self._bounded_thread(self._worker.kill, is_kill=True)
                if self._cache is not None:
                    self._cache.release_in_use()
            return await self._complete(a, result, bound)
        finally:
            stopper.cancel()
            beat.cancel()  # also during _complete: the lease is kept alive until we return
            watch.cancel()  # the governor only steers a running subtask, not its completion
            if not work.done():
                work.cancel()
            await asyncio.gather(stopper, beat, watch, work, return_exceptions=True)
            self._current = None
            self._status_kick.set()
            # The work task has truly finished (incl. a shielded file write): nothing may linger.
            for p in self._paths(a, tmp):
                p.unlink(missing_ok=True)

    async def _push_control(self) -> None:
        """Hand the live duty / pause (and a one-shot trim) to the worker, on change only. The
        worker applies it at its next checkpoint, at once during a pacer sleep (FR-54). Serialised:
        the watcher and the batch loop both push, and the decision is read under the lock so the
        last push is always the newest."""
        async with self._ctl_lock:
            d = self._gov.decision
            key = (d.budget.gpu_duty_cycle, d.paused)
            trim = d.trim and not self._trim_pushed
            self._trim_pushed = d.trim
            if key == self._ctl_key and not trim:
                return
            self._ctl_key = key
            try:
                await asyncio.to_thread(self._worker.control, duty=key[0], paused=key[1], trim=trim)
            except Exception as exc:  # e.g. a malformed value: log, retry on the next change
                self._ctl_key = None
                log.warning("worker_control_failed", error=type(exc).__name__)

    def _sample_worker(self) -> tuple[int | None, bool | None]:
        try:
            return self._worker.footprint_bytes(), self._worker.priority_ok()
        except Exception:  # sampling must never take the watcher down
            return None, None

    async def _watch(self, a: Assignment) -> _Verdict:
        """Beside the work: keep the worker in step with the governor and decide when the
        in-flight subtask must be given up. Returns the verdict; otherwise runs until cancelled.

        - pushes duty / pause / trim to the worker on every change;
        - `release_now` (memory critical) and `pause_expired` (>= 60 s paused) end it at once; both
          are level-triggered, so acting on them is idempotent;
        - FR-54: a memory budget below the model gets `downgrade_batch_s` to finish the batch
          (the batch loop checks at each boundary), then the worker is interrupted;
        - D7: footprint over 1.1 x the memory budget (or, while a batch started under a larger
          budget runs, over that budget) on two consecutive samples;
        - D8: a worker less throttled than its CPU priority, after a grace window that restarts
          whenever a batch starts with a different priority.

        Once inference has ended (`_infer_done`) the watcher never ends the subtask any more: the
        result exists and upload / complete proceed. It then only frees an idle worker the budget
        or the memory pressure no longer allows.
        """
        gov, clock = self._gov, self._clock
        version = gov.version
        over = 0
        downgrade_since: float | None = None
        drop_since: float | None = None  # when the live budget fell below the running batch's
        next_sample = clock() + self._guard_s
        while True:
            if self._infer_done.is_set():
                await self._enforce_idle_budget()
                version = await gov.wait_change(version, self._guard_s)
                continue
            await self._push_control()
            d = gov.decision
            if d.release_now:
                return _Verdict.MEMORY_CRITICAL
            if d.pause_expired:
                return _Verdict.PAUSE_EXPIRED
            if self._batch_budget is None:
                # No batch runs yet (the model is still being fetched or loaded): a warm worker
                # of the previous model is not this subtask's footprint. Never count it toward
                # OOM; just unload it when it no longer fits the budget.
                over = 0
                drop_since = None
                loaded = self._loaded
                if (
                    not self._ensure_started
                    and loaded is not None
                    and not _same_model(loaded, a.model)
                    and loaded.memory_gb > d.budget.memory_gb
                ):
                    await self._unload_idle("budget_downgrade")
            now = clock()
            downgrading = a.model.memory_gb > d.budget.memory_gb
            if downgrading:
                downgrade_since = now if downgrade_since is None else downgrade_since
                if now - downgrade_since >= self._downgrade_batch_s:
                    return _Verdict.DOWNGRADE_TIMEOUT
            else:
                downgrade_since = None
            batch = self._batch_budget
            if batch is not None and d.budget.memory_gb < batch.memory_gb:
                drop_since = now if drop_since is None else drop_since
            else:
                drop_since = None
            sample_due = now >= next_sample
            if sample_due:
                next_sample = now + self._guard_s  # also without a batch: no busy-spin
            if batch is not None and sample_due:
                footprint, prio_ok = await asyncio.to_thread(self._sample_worker)
                # the running batch started under `batch`: a budget drop only takes effect at the
                # next batch boundary (FR-54, ADR-0006 D7), so the allowance (measure against the
                # larger of the two budgets) lasts as long as the drop is within
                # `downgrade_batch_s` of it; the same clock covers a downgrade. Once it expires
                # the subtask is released (DOWNGRADE_TIMEOUT), never failed as OOM.
                ref_gb = d.budget.memory_gb
                if drop_since is not None:
                    ref_gb = max(ref_gb, batch.memory_gb)
                limit = ref_gb * GIB * MEMORY_GUARD_FACTOR
                if footprint is not None and footprint > limit:
                    over += 1
                    if over >= MEMORY_GUARD_SAMPLES:
                        return _Verdict.OOM
                else:
                    over = 0
                    if (
                        footprint is not None
                        and drop_since is not None
                        and now - drop_since >= self._downgrade_batch_s
                        and footprint > d.budget.memory_gb * GIB * MEMORY_GUARD_FACTOR
                    ):
                        # the owner's budget fell and the batch did not finish in time: that is
                        # policy, not the subtask's fault - release, do not burn an attempt
                        return _Verdict.DOWNGRADE_TIMEOUT
                if prio_ok is False and now >= self._grace_until:
                    return _Verdict.PRIORITY
            wake = next_sample - clock()
            for since in (downgrade_since, drop_since):
                if since is not None:  # only a deadline still ahead shortens the wait
                    left = since + self._downgrade_batch_s - clock()
                    if left > 0:
                        wake = min(wake, left)
            version = await gov.wait_change(version, max(0.01, wake))

    async def _on_verdict(
        self, a: Assignment, work: asyncio.Task[CompleteRequest], v: _Verdict, bound: Any
    ) -> _Outcome | None:
        """The governor ended the subtask: interrupt the worker (it is killed, so its memory is
        back at once), drop the work, then release - or, for the memory guard, fail it as a
        retryable OOM. Nothing is uploaded or completed (invariant 5).

        A verdict is moot once inference has produced its result: the subtask then goes on to
        upload and complete (returns None). A verdict that raced the work finishing successfully
        completes it, like the hard-stop path."""
        if self._infer_done.is_set():
            bound.info("governor_verdict_ignored_after_inference", code=v.value)
            return None
        bound.warning("governor_interrupt", code=v.value)
        self._forget_loaded()
        await asyncio.to_thread(self._worker.interrupt)
        if self._cache is not None:
            self._cache.release_in_use()  # the killed worker no longer holds the model
        if self._infer_ok and not work.done():
            return None  # inference finished while we interrupted: the upload carries on
        finished = work.done()
        work.cancel()
        await asyncio.gather(work, return_exceptions=True)
        if finished and not work.cancelled() and work.exception() is None:
            return await self._complete(a, work.result(), bound)
        if finished and not work.cancelled():
            exc = work.exception()
            if isinstance(exc, _FailureError) and exc.code != "WORKER_CRASHED":
                # a real failure that raced the interrupt (not the crash the interrupt causes)
                await self._report_failure(a, exc, bound)
                return _Outcome.FAILED
        if v is _Verdict.OOM:
            err = _FailureError(True, "OOM", "worker memory exceeded the budget")
            await self._report_failure(a, err, bound)
            return _Outcome.FAILED
        await self._release(a, bound, v.value)
        # a priority violation / crashed watcher is a local fault: back off so it cannot churn;
        # the rest is policy
        local = v in (_Verdict.PRIORITY, _Verdict.WATCHER_FAILED)
        return _Outcome.FAILED if local else _Outcome.RELEASED

    def _roll_today(self) -> tuple[date, int, int]:
        """Today's tally, reset at local midnight."""
        today = _local_today()
        if self._today[0] != today:
            self._today = (today, 0, 0)
        return self._today

    def _note_done(self, records: int) -> None:
        day, subtasks, total = self._roll_today()
        self._today = (day, subtasks + 1, total + records)
        self._status_kick.set()

    def _status_snapshot(self, *, stopped: bool = False) -> StatusFile:
        """Ids and numbers only: no payload, URL, token or acquirement id (S-5)."""
        d = self._gov.decision
        sig = self._gov.snapshot
        cur = self._current
        day, subtasks, records = self._today  # read only: rolled on the event loop
        try:
            share = self._worker.gpu_share()
        except Exception:
            share = None
        return StatusFile(
            pid=os.getpid(),
            started_at=self._started_at,
            updated_at=datetime.now(UTC),
            stopped=stopped,
            profile=d.profile,
            profile_source=d.profile_source,
            budget=d.budget,
            held_below_nominal=d.budget != self._gov.nominal,
            call_capped=d.call_capped,
            candidate=d.candidate,
            candidate_since=d.candidate_since,
            paused_reason=d.paused_reason,
            subtask=(
                StatusSubtask(
                    subtask_id=cur[0].subtask_id,
                    attempt=cur[0].attempt,
                    records_done=min(cur[1].records_done, cur[0].record_count),
                    records_total=cur[0].record_count,
                )
                if cur is not None
                else None
            ),
            gpu_share=share,
            signals=(
                StatusSignals(
                    on_ac=sig.on_ac,
                    battery_percent=sig.battery_percent,
                    low_power_mode=sig.low_power_mode,
                    thermal=sig.thermal,
                    memory_pressure=sig.memory_pressure,
                    hid_idle_s=round(sig.hid_idle_s, 1),
                    taken_at=sig.taken_at,
                    call_active=sig.call_active,
                )
                if sig is not None
                else None
            ),
            today=StatusToday(date=day, subtasks=subtasks, records=records),
        )

    def _write_status(self, state_dir: Path, *, stopped: bool = False) -> None:
        """Blocking (fsync): call off-loop. Any failure (including a bug building the snapshot) is
        logged once per distinct message and never propagates: status is best effort."""
        try:
            write_status(state_dir, self._status_snapshot(stopped=stopped))
        except Exception as exc:
            note = str(exc) if isinstance(exc, (SafeFileError, OSError)) else type(exc).__name__
            if note != self._status_note:
                self._status_note = note
                log.warning("status_write_failed", reason=note)
        else:
            self._status_note = None

    async def _status_loop(self, state_dir: Path) -> None:
        """Rewrite status.json on every decision change, subtask change and every interval."""
        version = self._gov.version
        while True:
            self._status_kick.clear()
            self._roll_today()  # on the event loop: the snapshot (a thread) only reads
            try:
                await asyncio.to_thread(self._write_status, state_dir)
            except Exception as exc:  # e.g. the thread pool refused: keep the loop alive
                note = type(exc).__name__
                if note != self._status_note:
                    self._status_note = note
                    log.warning("status_write_failed", reason=note)
            change = asyncio.ensure_future(self._gov.wait_change(version, self._status_s))
            kick = asyncio.ensure_future(self._status_kick.wait())
            try:
                await asyncio.wait({change, kick}, return_when=asyncio.FIRST_COMPLETED)
            finally:
                change.cancel()
                kick.cancel()
                await asyncio.gather(change, kick, return_exceptions=True)
            version = self._gov.version

    def _paused_reason(self) -> str | None:
        reason = self._gov.decision.paused_reason
        return reason.value if reason is not None else None

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
            self._forget_loaded()
            await asyncio.to_thread(self._worker.interrupt)
            if self._cache is not None:
                self._cache.release_in_use()  # the killed worker no longer holds the model
        work.cancel()
        await asyncio.gather(work, return_exceptions=True)
        if sig is HeartbeatSignal.CANCEL:
            await self._release(a, bound, "CANCELLED")
            return _Outcome.CANCELLED
        bound.info("stale_discarded")
        return _Outcome.DISCARDED

    async def _complete(self, a: Assignment, result: CompleteRequest, bound: Any) -> _Outcome:
        """Report completion, retrying transport errors/5xx (the server treats a same-owner
        repeat as success). Other 4xx are reported as a retryable failure."""
        attempt = 0
        saw_5xx = False
        sheds = 0
        shed_since: float | None = None
        while True:
            try:
                await self._call_complete(a, result)
                bound.info("subtask_completed", records=result.record_count)
                return _Outcome.DONE
            except _HardStoppedError:
                # The call may have reached the server: never claim success. If it did, the
                # release below gets a 409 (handled); if not, the subtask requeues at once.
                bound.warning("complete_abandoned_at_hard_stop")
                await self._release(a, bound, "STOP")
                return _Outcome.STOPPED
            except StaleAcquirementError:
                bound.info("stale_discarded")
                return _Outcome.DISCARDED
            except ApiError as exc:
                if exc.status_code == 409:
                    bound.warning("complete_conflict", code=exc.code)
                    return _Outcome.DISCARDED
                if exc.status_code < 500:
                    bound.warning("complete_rejected", status=exc.status_code, code=exc.code)
                    if exc.status_code == 422 and exc.code == "OUTPUT_TOO_LARGE":
                        # deterministic: a re-run produces the same oversize output
                        err = _FailureError(
                            False, "OUTPUT_TOO_LARGE", "output exceeds the size cap"
                        )
                    else:
                        err = _FailureError(True, "COMPLETE_REJECTED", f"HTTP {exc.status_code}")
                    await self._report_failure(a, err, bound)
                    return _Outcome.FAILED
                if _is_shed(exc):
                    # Overload shedding is not a fault: it neither burns `complete_retries` nor
                    # marks a server error; bounded by the hard stop and a wall-clock budget.
                    sheds += 1
                    now = self._clock()
                    shed_since = now if shed_since is None else shed_since
                    bound.warning("complete_shed", sheds=sheds)
                    if self._hard_stop.is_set():
                        await self._release(a, bound, "STOP")
                        return _Outcome.STOPPED
                    if now - shed_since >= self._complete_shed_budget_s:
                        # a shed 503 commits nothing for that request, so release (RELEASED is
                        # not budget counted; a watchdog reclaim would be EXPIRED plus an idle
                        # lease). If an earlier timed-out attempt did commit, the release gets a
                        # 409 and is harmless; if the release fails, the watchdog reclaims it.
                        bound.error("complete_shed_budget_exhausted", sheds=sheds)
                        await self._release(a, bound, "SERVER_BUSY")
                        return _Outcome.RELEASED
                    await self._pause(
                        self._rate_limit_delay(exc, sheds - 1, repeated=sheds > 1),
                        until=self._hard_stop,
                    )
                    continue
                saw_5xx = True
                bound.warning("complete_failed", error=type(exc).__name__, attempt=attempt)
            except httpx.HTTPError as exc:
                bound.warning("complete_failed", error=type(exc).__name__, attempt=attempt)
            attempt += 1
            if self._hard_stop.is_set():
                await self._release(a, bound, "STOP")
                return _Outcome.STOPPED
            if attempt >= self._complete_retries:
                bound.error("complete_gave_up", server_error=saw_5xx)
                if saw_5xx:  # deterministic server fault: a release would livelock; burn budget
                    err = _FailureError(True, "COMPLETE_FAILED", "complete failed")
                    await self._report_failure(a, err, bound)
                # transport-only: leave it to the watchdog (counts EXPIRED)
                return _Outcome.FAILED
            await self._pause(self._delay(attempt - 1), until=self._hard_stop)

    async def _call_complete(self, a: Assignment, result: CompleteRequest) -> None:
        """`client.complete`, but once a hard stop is set (or arrives mid-call) the call gets only
        the rest of the shared shutdown budget; then `_HardStoppedError` (outcome unknown)."""
        await self._guarded(
            self._client.complete(a.acquirement_id, result), self._hard_stop, self._left
        )

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
            cap = min(self._max_output_bytes, a.max_output_bytes or self._max_output_bytes)
            if size > cap:  # never upload what the server would not promote
                raise _FailureError(False, "OUTPUT_TOO_LARGE", "output exceeds the size cap")
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
            try:
                path = await self._cache.ensure(model)
                if path is None:  # Ollama / fake: nothing cached, so no stale pin on an old model
                    self._cache.release_in_use()
                return path
            except ModelFetchError as exc:
                if not (exc.code == MODEL_CACHE_FULL and exc.blocked_by_in_use):
                    raise
            # The new model fits the cap only if the loaded model's files may go: drop the warm
            # worker (frees its memory too), unprotect that model and retry once.
            log.info("model_cache_unload_for_space")
            await asyncio.to_thread(self._worker.unload)
            self._forget_loaded()
            self._cache.release_in_use()
            return await self._cache.ensure(model)
        except OSError as exc:  # cache dir unreadable / unwritable: local, not the subtask's fault
            raise _FailureError(True, "LOCAL_IO_ERROR", type(exc).__name__, release=True) from None

    async def _run_batches(
        self, a: Assignment, records: list[Record], hb: Heartbeater, params: TaskParams
    ) -> list[Record]:
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
            # a pause stops a new download from starting (one in flight runs on; its niceness is
            # fixed at spawn), the watcher bounds the wait
            await self._await_unload()  # a previous subtask's unload (watcher cancelled mid-way)
            await self._wait_unpaused()
            model_path = await self._ensure_cached(a.model)
            await self._wait_unpaused()  # no model load while paused (the watcher bounds this)
            budget = self._gov.decision.budget
            if a.model.memory_gb > budget.memory_gb:
                raise _DowngradeError  # the budget fell while the model was being fetched
            self._ensure_started = True  # the watcher no longer unloads under ensure_model
            await self._await_unload()  # one the watcher started before that must finish first
            self._batch_budget = budget  # the load may respawn the worker: guard and grace follow
            self._grace_until = self._clock() + self._prio_grace_s
            try:
                if model_path is None:
                    await asyncio.to_thread(self._worker.ensure_model, a.model, budget)
                else:
                    await asyncio.to_thread(self._worker.ensure_model, a.model, budget, model_path)
            except RunnerError:
                self._forget_loaded()
                if self._cache is not None:
                    self._cache.release_in_use()  # a failed load holds no model: unpin it
                raise
            self._loaded = a.model
            self._loaded_path = model_path
            self._unloaded = False
            i = 0
            while i < len(records):
                await self._wait_unpaused()
                d = self._gov.decision
                budget = d.budget  # re-read per batch: FR-54 "next batch"
                if a.model.memory_gb > budget.memory_gb:
                    raise _DowngradeError  # finish the batch, then give the subtask back (FR-54)
                await self._push_control()
                batch = records[i : i + budget.max_batch_size]
                self._batch_budget = budget
                if budget.cpu_priority != self._batch_cpu:  # D8: a new priority applies per batch
                    self._batch_cpu = budget.cpu_priority
                    self._grace_until = self._clock() + self._prio_grace_s
                outputs.extend(
                    await asyncio.to_thread(self._worker.run, batch, params, progress(i), budget)
                )
                i += len(batch)
                hb.records_done = len(outputs)
            self._infer_ok = True
        except WorkerCrashed:
            self._forget_loaded()
            if self._cache is not None:
                self._cache.release_in_use()  # the dead worker no longer holds the model
            raise _FailureError(True, "WORKER_CRASHED", "worker process died") from None
        except RunnerError as exc:
            code = exc.code[:64] or "RUNNER_ERROR"
            # Engine missing on this Mac is executor-local: release, don't burn an attempt.
            release = exc.code in (ENGINE_UNAVAILABLE, SANDBOX_FAILED) or exc.release
            raise _FailureError(exc.retryable, code, "runner failed", release=release) from None
        finally:
            self._infer_done.set()  # the watcher may no longer end the subtask (see `_watch`)
        return outputs

    async def _wait_unpaused(self) -> None:
        """Block while the governor is paused. A pause that lasts `pause_release_after_s` makes
        the watcher release the subtask, which cancels this wait."""
        while self._gov.decision.paused:
            await self._gov.wait_change(self._gov.version, 5.0)

    async def _report_failure(self, a: Assignment, f: _FailureError, bound: Any) -> None:
        bound.warning("subtask_failed", code=f.code, retryable=f.retryable, released=f.release)
        if f.release:  # executor-local trouble: don't burn the subtask's retry budget
            await self._release(a, bound, f.code)
            return
        try:
            req = FailRequest(
                retryable=f.retryable, code=_fail_code(f.code), message=f.message[:1000]
            )
            await asyncio.wait_for(self._client.fail(a.acquirement_id, req), self._call_timeout())
        except StaleAcquirementError:
            bound.info("stale_discarded")
        except (httpx.HTTPError, ApiError, TimeoutError, ValidationError) as exc:
            bound.warning("fail_report_failed", error=type(exc).__name__)

    async def _release(self, a: Assignment, bound: Any, code: str) -> None:
        """Hand the subtask back; `code` (stable reason) goes to the attempt history."""
        try:
            await asyncio.wait_for(
                self._client.release(a.acquirement_id, ReleaseRequest(code=_release_code(code))),
                self._call_timeout(),
            )
            bound.info("subtask_released")
        except (httpx.HTTPError, ApiError, TimeoutError, ValidationError) as exc:
            bound.warning("release_failed", error=type(exc).__name__)
