"""Per-subtask heartbeater (ARCH §2.2, FR-14): keeps the lease alive beside the work.

Runs as an asyncio task next to the work coroutine and returns only when the server says to stop
(`cancel=true`) or the acquirement is stale (409). Transport errors, 5xx and 429 are tolerated.
Logs carry ids only, never payloads.
"""

from __future__ import annotations

import asyncio
import enum
import random
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime

import httpx
import structlog
from minion_common.client import ApiError, ExecutorClient, StaleAcquirementError
from minion_common.schemas import Budget, HeartbeatRequest, Progress

log = structlog.get_logger("minion_executor.heartbeat")

MIN_INTERVAL_S = 1.0
MAX_ERROR_RETRY_S = 30.0
MAX_RETRY_AFTER_S = 600.0


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
        pause_fn: Callable[[], str | None] | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if interval_s < MIN_INTERVAL_S:
            raise ValueError(f"heartbeat interval must be >= {MIN_INTERVAL_S}s")
        self._client = client
        self._aid = acquirement_id
        self._interval = interval_s
        self._budget = budget
        self._total = total
        self._pause_fn = pause_fn
        self._sleep = sleep
        self._expires_at: datetime | None = None
        self.records_done = 0
        self.output_url = initial_output_url

    def _jittered(self, base: float) -> float:
        return base * random.uniform(0.9, 1.1)  # noqa: S311

    def _request(self) -> HeartbeatRequest:
        done = min(self.records_done, self._total)
        reason = self._pause_fn() if self._pause_fn is not None else None
        return HeartbeatRequest(
            progress=Progress(records_done=done, records_total=self._total),
            budget=self._budget,
            paused_reason=reason,
        )

    async def run(self) -> HeartbeatSignal:
        """Beat until a stop signal; cancel the task to end it otherwise."""
        bound = log.bind(acquirement_id=str(self._aid))
        delay = self._jittered(self._interval)
        while True:
            await self._sleep(delay)
            delay = self._jittered(self._interval)
            try:
                resp = await self._client.heartbeat(self._aid, self._request())
            except StaleAcquirementError:
                bound.info("heartbeat_stale")
                return HeartbeatSignal.STALE
            except ApiError as exc:
                if exc.status_code == 409:  # e.g. TASK_FINISHED: nothing left to do here
                    bound.info("heartbeat_conflict", code=exc.code)
                    return HeartbeatSignal.STALE
                bound.warning("heartbeat_failed", status=exc.status_code)
                if exc.status_code == 429:
                    wait = exc.retry_after if exc.retry_after is not None else self._interval
                    delay = min(max(wait, MIN_INTERVAL_S), MAX_RETRY_AFTER_S)
                elif exc.status_code >= 500:
                    delay = self._jittered(min(MAX_ERROR_RETRY_S, self._interval))
                continue
            except httpx.HTTPError as exc:
                bound.warning("heartbeat_failed", error=type(exc).__name__)
                delay = self._jittered(min(MAX_ERROR_RETRY_S, self._interval))
                continue
            except Exception as exc:  # never let a bad beat end the heartbeater silently
                bound.warning("heartbeat_error", error=type(exc).__name__)
                continue
            if resp.cancel:
                bound.info("heartbeat_cancel")
                return HeartbeatSignal.CANCEL
            if self._expires_at is None or resp.expires_at > self._expires_at:
                self._expires_at, self.output_url = resp.expires_at, resp.output_url
