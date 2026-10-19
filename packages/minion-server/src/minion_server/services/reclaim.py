"""Watchdog reclaim (plan D3, ARCHITECTURE §3.4): expire silent acquirements, requeue or fail.

Reclaim never touches `attempt` (the next acquire bumps it) and never talks to S3: the expired
attempt's output stays at its attempt-scoped key, unpromoted. Ownership is only ever decided by
the acquire INSERT; reclaim just deletes the row under a row lock.
"""

import time
import uuid
from collections import Counter
from collections.abc import Sequence
from enum import StrEnum
from typing import Any

import structlog
from minion_common.enums import AttemptOutcome, SubtaskStatus, TaskStatus
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from minion_server.repositories import completion as completion_repo
from minion_server.repositories import reclaim as repo
from minion_server.repositories import tasks as tasks_repo
from minion_server.services.completion import next_status, settle_in_tx

log = structlog.get_logger()


class ReclaimResult(StrEnum):
    EXPIRED_REQUEUED = "EXPIRED_REQUEUED"
    EXPIRED_FAILED = "EXPIRED_FAILED"
    EXPIRED_CANCELLED = "EXPIRED_CANCELLED"
    SKIPPED = "SKIPPED"  # gone, or an owner op / another watchdog holds the lock
    NOT_STALE = "NOT_STALE"  # a heartbeat landed while we waited


_BY_STATUS = {
    SubtaskStatus.PENDING: ReclaimResult.EXPIRED_REQUEUED,
    SubtaskStatus.FAILED: ReclaimResult.EXPIRED_FAILED,
    SubtaskStatus.CANCELLED: ReclaimResult.EXPIRED_CANCELLED,
}

_EXPIRED = frozenset(_BY_STATUS.values())
RECLAIM_CHUNK = 100  # acquirements per transaction: bounds lock time and rollback size


def decide(row: repo.ReclaimRow, *, unacked_free_cap: int) -> repo.Finish | ReclaimResult:
    """Pure reclaim decision for one locked, freshly classified acquirement (FR-31, ADR-0014)."""
    o = row.owned
    cancelled = o.task_status == TaskStatus.CANCELLED
    if row.stale:
        if unacked_free_cap > 0 and row.never_acked:
            if row.unacked_used < unacked_free_cap:
                return repo.Finish(
                    o,
                    AttemptOutcome.EXPIRED,
                    SubtaskStatus.CANCELLED if cancelled else SubtaskStatus.PENDING,
                    attempt_error={
                        "code": completion_repo.NOT_ACKNOWLEDGED,
                        "message": "never acknowledged after claim",
                        "retryable": True,
                    },
                )
            log.info(
                "unacked_cap_reached",
                acquirement_id=str(o.acquirement_id),
                subtask_id=o.subtask_id,
                attempt=o.attempt,
                free_cap=unacked_free_cap,
            )
        code, message = "HEARTBEAT_TIMEOUT", "no heartbeat"
    elif row.over_lease:
        code, message = "MAX_LEASE_EXCEEDED", "max lease exceeded"
    else:
        return ReclaimResult.NOT_STALE
    return repo.Finish(
        o,
        AttemptOutcome.EXPIRED,
        next_status(row.budget_used + 1, o.max_attempts, retryable=True, task_cancelled=cancelled),
        error={"code": code, "message": message, "retryable": True},
    )


async def reclaim_chunk(
    session: AsyncSession,
    ids: Sequence[uuid.UUID],
    *,
    timeout_s: float,
    max_lease_s: float | None = None,
    unacked_free_cap: int = 0,
) -> dict[uuid.UUID, ReclaimResult]:
    """Reclaim a chunk inside the caller's open transaction (caller commits).

    Lock (sorted, SKIP LOCKED) -> classify in a fresh statement -> decide -> write per kind.
    Reclaimed when the heartbeat is older than `timeout_s`, or (if `max_lease_s` is set) when the
    acquirement itself is older than `max_lease_s`: an executor that keeps heartbeating but never
    finishes cannot hold a subtask forever. Both are recorded as EXPIRED and consume budget.
    """
    results = {aid: ReclaimResult.SKIPPED for aid in ids}
    locked = await repo.lock_batch(session, ids)
    if not locked:
        return results
    rows = await repo.classify_locked(session, locked, timeout_s=timeout_s, max_lease_s=max_lease_s)
    finishes: list[repo.Finish] = []
    for row in rows:
        d = decide(row, unacked_free_cap=unacked_free_cap)
        if isinstance(d, ReclaimResult):
            results[row.owned.acquirement_id] = d
        else:
            finishes.append(d)
            results[row.owned.acquirement_id] = _BY_STATUS[d.subtask_status]
    await repo.finish_reclaimed(session, finishes)
    return results


async def reclaim_one(
    session: AsyncSession,
    acquirement_id: uuid.UUID,
    *,
    timeout_s: float,
    max_lease_s: float | None = None,
    unacked_free_cap: int = 0,
) -> ReclaimResult:
    """Reclaim one acquirement inside the caller's open transaction (a 1-id chunk)."""
    res = await reclaim_chunk(
        session,
        [acquirement_id],
        timeout_s=timeout_s,
        max_lease_s=max_lease_s,
        unacked_free_cap=unacked_free_cap,
    )
    return res[acquirement_id]


async def reclaim_stale(
    sm: async_sessionmaker[AsyncSession],
    *,
    timeout_s: float,
    batch_size: int,
    max_lease_s: float | None = None,
    unacked_free_cap: int = 0,
) -> Counter[ReclaimResult]:
    """One transaction per chunk of RECLAIM_CHUNK; a failing chunk is retried row by row."""
    async with sm() as s:
        ids = await repo.stale_ids(s, timeout_s, batch_size)
        if max_lease_s is not None:
            seen = set(ids)
            scan_started = time.monotonic()
            over = await repo.over_lease_ids(s, max_lease_s, batch_size)
            log.debug(
                "over_lease_scan",
                scan_ms=round((time.monotonic() - scan_started) * 1000),
                hits=len(over),
            )
            ids += [i for i in over if i not in seen]
    kw: dict[str, Any] = {
        "timeout_s": timeout_s,
        "max_lease_s": max_lease_s,
        "unacked_free_cap": unacked_free_cap,
    }
    results: Counter[ReclaimResult] = Counter()
    for start in range(0, len(ids), RECLAIM_CHUNK):
        chunk = ids[start : start + RECLAIM_CHUNK]
        try:
            async with sm() as s, s.begin():
                done = await reclaim_chunk(s, chunk, **kw)
        except Exception as exc:  # poisoned chunk: isolate the bad row, keep the rest
            log.error("reclaim_chunk_failed", size=len(chunk), error_type=type(exc).__name__)
            done = {}
            for aid in chunk:
                try:
                    async with sm() as s, s.begin():
                        done[aid] = await reclaim_one(s, aid, **kw)
                except Exception as exc2:  # one bad row must not block the rest of the batch
                    log.error(
                        "reclaim_failed", acquirement_id=str(aid), error_type=type(exc2).__name__
                    )
        for aid, res in done.items():
            results[res] += 1
            if res in _EXPIRED:
                log.info("subtask_reclaimed", acquirement_id=str(aid), result=res.value)
    return results


async def sweep_cancelled(
    sm: async_sessionmaker[AsyncSession],
    *,
    window_s: float = 3600,
    batch: int = tasks_repo.CANCEL_BATCH,
) -> int:
    """PENDING subtasks of recently cancelled tasks -> CANCELLED (plan D4 healing sweep).

    Batched: one short transaction per `batch` rows, until a batch comes back short.
    """
    total = 0
    while True:
        async with sm() as s, s.begin():
            n = await tasks_repo.sweep_cancelled(s, window_s, batch)
        total += n
        if n < batch:
            break
    if total:
        log.info("cancelled_pending_swept", count=total)
    return total


async def settle_stuck(
    sm: async_sessionmaker[AsyncSession], *, batch: int = 100, max_rounds: int = 50
) -> int:
    """Backstop for ADR-0015: finish live tasks whose counters already cover every subtask.

    Covers a settle lost after the owner op committed (crash, DB blip), a reclaim that failed the
    last subtask, and a retried complete. One short transaction per task. Returns the number of
    tasks made terminal.
    Repeats while a batch comes back full, at most `max_rounds` times per call.
    """
    settled = 0
    for _ in range(max_rounds):
        async with sm() as s:
            keys = await completion_repo.settleable_tasks(s, batch)
        failed = 0
        for key in keys:
            try:
                async with sm() as s, s.begin():
                    if await settle_in_tx(s, key):
                        settled += 1
            except Exception as exc:  # one bad task must not block the rest
                failed += 1
                log.error("task_settle_failed", error=type(exc).__name__)
        # a failing task stays settleable and would be re-read forever: stop on any failure
        if len(keys) < batch or failed:
            break
    if settled:
        log.info("tasks_settled", count=settled)
    return settled
