"""Per-subtask heartbeater (ARCH §2.2, FR-14): keeps the lease alive beside the work.

Runs as an asyncio task next to the work coroutine and returns only when the server says to stop
(`cancel=true`) or the acquirement is stale (409). Transport errors, 5xx and 429 are tolerated.
Logs carry ids only, never payloads.
"""

from __future__ import annotations

import asyncio
import enum
import math
import random
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime
from urllib.parse import urlsplit

import httpx
import structlog
from minion_common.client import ApiError, ExecutorClient, StaleAcquirementError
from minion_common.schemas import Budget, HeartbeatRequest, Progress

log = structlog.get_logger("minion_executor.heartbeat")

MIN_INTERVAL_S = 1.0
# Upper bound: the refreshed output URL must outlive the gap between beats, so the interval has to
# stay well inside the presign TTL (ARCH §2.2; default TTL is 24 h, so 1 h leaves a wide margin).
MAX_INTERVAL_S = 3600.0
MAX_ERROR_RETRY_S = 30.0
MAX_RETRY_AFTER_S = 600.0


def check_interval(interval_s: float) -> None:
    """Raise ValueError unless `interval_s` is finite and in [MIN_INTERVAL_S, MAX_INTERVAL_S]."""
    if not math.isfinite(interval_s):
        raise ValueError("heartbeat interval must be a finite number")
    if interval_s < MIN_INTERVAL_S:
        raise ValueError(f"heartbeat interval must be >= {MIN_INTERVAL_S:g}s")
    if interval_s > MAX_INTERVAL_S:
        raise ValueError(f"heartbeat interval must be <= {MAX_INTERVAL_S:g}s")


def _same_object(a: str, b: str) -> bool:
    """Same scheme, host and path (the query - the signature - may differ)."""
    pa, pb = urlsplit(a), urlsplit(b)
    return (pa.scheme, pa.netloc, pa.path) == (pb.scheme, pb.netloc, pb.path)


class HeartbeatSignal(enum.Enum):
    CANCEL = enum.auto()  # the task was cancelled: stop work and release
    STALE = enum.auto()  # 409: someone else owns it now; stop work, report nothing


class Heartbeater:
    def __init__(
        self,
        client: ExecutorClient,
        acquirement_id: uuid.UUID,
        *,
        interval_s: float,
        budget: Budget | None,
        total: int,
        initial_output_url: str,
        initial_expires_at: datetime | None = None,
        pause_fn: Callable[[], str | None] | None = None,
        budget_fn: Callable[[], Budget | None] | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        check_interval(interval_s)
        self._client = client
        self._aid = acquirement_id
        self._interval = interval_s
        self._budget = budget
        self._budget_fn = budget_fn  # live budget (governor); wins over the fixed `budget`
        self._total = total
        self._pause_fn = pause_fn
        self._sleep = sleep
        self._initial_url = initial_output_url
        self.expires_at: datetime | None = initial_expires_at
        self.records_done = 0
        self.output_url = initial_output_url
        self._lock = asyncio.Lock()  # one beat in flight: no self-inflicted 429
        self._rate_limit_wait: float | None = None

    def _jittered(self, base: float) -> float:
        # Upward only: a beat must never come sooner than the interval (server minimum spacing).
        return base * random.uniform(1.0, 1.1)  # noqa: S311

    def _request(self) -> HeartbeatRequest:
        done = min(self.records_done, self._total)
        reason = self._pause_fn() if self._pause_fn is not None else None
        return HeartbeatRequest(
            progress=Progress(records_done=done, records_total=self._total),
            budget=self._budget_fn() if self._budget_fn is not None else self._budget,
            paused_reason=reason,
        )

    def url_fresh(self, margin_s: float) -> bool:
        """True when the current output URL is known to stay valid for at least `margin_s`."""
        exp = self.expires_at
        if exp is None or exp.tzinfo is None:
            return False
        return (exp - datetime.now(exp.tzinfo)).total_seconds() > margin_s

    async def beat(
        self, *, retry_rate_limited: bool = False
    ) -> tuple[HeartbeatSignal | None, float | None]:
        """One heartbeat, serialized with all others. Returns (stop signal or None, delay override
        for the next beat). With `retry_rate_limited`, a 429 waits `retry_after` (capped) and the
        beat is retried once."""
        async with self._lock:
            self._rate_limit_wait = None
            result = await self._beat_once()
            if retry_rate_limited and self._rate_limit_wait is not None:
                await self._sleep(self._rate_limit_wait)
                self._rate_limit_wait = None
                result = await self._beat_once()
            return result

    async def _beat_once(self) -> tuple[HeartbeatSignal | None, float | None]:
        bound = log.bind(acquirement_id=str(self._aid))
        try:
            resp = await self._client.heartbeat(self._aid, self._request())
        except StaleAcquirementError:
            bound.info("heartbeat_stale")
            return HeartbeatSignal.STALE, None
        except ApiError as exc:
            if exc.status_code == 409:  # e.g. TASK_FINISHED: nothing left to do here
                bound.info("heartbeat_conflict", code=exc.code)
                return HeartbeatSignal.STALE, None
            bound.warning("heartbeat_failed", status=exc.status_code)
            if exc.status_code == 429:
                wait = exc.retry_after if exc.retry_after is not None else self._interval
                cap = min(MAX_RETRY_AFTER_S, self._interval)
                wait = min(max(wait, MIN_INTERVAL_S), cap)
                self._rate_limit_wait = wait
                return None, wait
            if exc.status_code >= 500:
                return None, self._jittered(min(MAX_ERROR_RETRY_S, self._interval))
            return None, None
        except httpx.HTTPError as exc:
            bound.warning("heartbeat_failed", error=type(exc).__name__)
            return None, self._jittered(min(MAX_ERROR_RETRY_S, self._interval))
        except Exception as exc:  # never let a bad beat end the heartbeater silently
            bound.warning("heartbeat_error", error=type(exc).__name__)
            return None, None
        if resp.cancel:
            bound.info("heartbeat_cancel")
            return HeartbeatSignal.CANCEL, None
        try:
            newer = self.expires_at is None or resp.expires_at > self.expires_at
            if newer and _same_object(resp.output_url, self._initial_url):
                self.expires_at, self.output_url = resp.expires_at, resp.output_url
            elif newer:  # defence in depth: never redirect the upload to another object
                bound.warning("heartbeat_url_ignored", error="ObjectMismatch")
        except Exception as exc:  # e.g. naive vs aware datetimes: keep the old URL
            bound.warning("heartbeat_url_ignored", error=type(exc).__name__)
        return None, None

    async def run(self) -> HeartbeatSignal:
        """Beat until a stop signal; cancel the task to end it otherwise."""
        delay = self._jittered(self._interval)
        while True:
            await self._sleep(delay)
            signal, override = await self.beat()
            if signal is not None:
                return signal
            delay = override if override is not None else self._jittered(self._interval)
