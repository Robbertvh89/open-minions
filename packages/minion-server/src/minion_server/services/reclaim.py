"""Watchdog reclaim (plan D3, ARCHITECTURE §3.4): expire silent acquirements, requeue or fail.

Reclaim never touches `attempt` (the next acquire bumps it) and never talks to S3: the expired
attempt's output stays at its attempt-scoped key, unpromoted. Ownership is only ever decided by
the acquire INSERT; reclaim just deletes the row under a row lock.
"""

import time
import uuid
from collections import Counter
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

import structlog
from minion_common.enums import AttemptOutcome, SubtaskStatus, TaskStatus
from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError
from sqlalchemy.exc import TimeoutError as SATimeoutError
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
QUARANTINE_S = 600.0  # a row that failed even alone is skipped this long
_QUARANTINE_MAX = 1000
_QUARANTINE: dict[uuid.UUID, float] = {}
_quarantine_evicted = 0  # lifetime count of entries evicted because the quarantine was full
RECLAIM_CHUNK = 100  # acquirements per transaction: bounds lock time and rollback size


def decide(row: repo.ReclaimRow, *, unacked_free_cap: int) -> repo.Finish | ReclaimResult:
    """Pure reclaim decision for one locked, freshly classified acquirement (FR-31, ADR-0014)."""
    o = row.owned
    cancelled = o.task_status == TaskStatus.CANCELLED
    cap_reached = False
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
            cap_reached = True
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
        cap_reached=cap_reached,
    )


def reset_quarantine() -> None:
    """Forget every quarantined row (tests)."""
    global _quarantine_evicted
    _QUARANTINE.clear()
    _quarantine_evicted = 0


def _quarantine_add(aid: uuid.UUID, error_type: str) -> bool:
    """Quarantine `aid`; when full, evict the soonest-expiring entry. Returns True if one was
    evicted. Logs ids and exception class names only, never a message or payload."""
    global _quarantine_evicted
    evicted = False
    if aid not in _QUARANTINE and len(_QUARANTINE) >= _QUARANTINE_MAX:
        victim = min(_QUARANTINE, key=_QUARANTINE.__getitem__)
        del _QUARANTINE[victim]
        _quarantine_evicted += 1
        evicted = True
        log.warning(
            "reclaim_quarantine_full", evicted_acquirement_id=str(victim), max=_QUARANTINE_MAX
        )
    _QUARANTINE[aid] = time.monotonic() + QUARANTINE_S  # no 101 tx/cycle
    log.warning(
        "reclaim_quarantined",
        acquirement_id=str(aid),
        until=(datetime.now(UTC) + timedelta(seconds=QUARANTINE_S)).isoformat(),
        error_type=error_type,
    )
    return evicted


# SQLSTATE classes that mean "the database is unwell", not "this row is bad": connection (08),
# transaction rollback incl. deadlock / serialization (40), insufficient resources incl. too many
# connections (53), operator intervention incl. statement timeout / cannot connect now (57), and
# lock_not_available (55P03).
_TRANSIENT_SQLSTATE_PREFIXES = ("08", "40", "53", "57")


def _row_specific(exc: BaseException) -> bool:
    """False for infrastructure errors (connection, pool, timeout, deadlock): a DB blip must not
    quarantine healthy rows."""
    transient = (OperationalError, InterfaceError, SATimeoutError, TimeoutError, OSError)
    if isinstance(exc, transient) or getattr(exc, "connection_invalidated", False):
        return False
    if isinstance(exc, DBAPIError):
        state = getattr(exc.orig, "sqlstate", None) or getattr(exc.orig, "pgcode", None)
        if isinstance(state, str) and (
            state.startswith(_TRANSIENT_SQLSTATE_PREFIXES) or state == "55P03"
        ):
            return False
    return True


async def reclaim_chunk(
    session: AsyncSession,
    ids: Sequence[uuid.UUID],
    *,
    timeout_s: float,
    max_lease_s: float | None = None,
    unacked_free_cap: int = 0,
    cap_hits: list[repo.Finish] | None = None,
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
            if d.cap_reached and cap_hits is not None:
                cap_hits.append(d)
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
    now = time.monotonic()
    lapsed = [k for k, until in _QUARANTINE.items() if until <= now]
    for k in lapsed:
        del _QUARANTINE[k]
    added = evicted = 0
    systemic = False  # a chunk failed with no proof of a bad row: skip row-by-row redo this call
    skip = list(_QUARANTINE)  # excluded in SQL: they must not hold the oldest LIMIT slots
    async with sm() as s:
        ids = await repo.stale_ids(s, timeout_s, batch_size, skip)
        if max_lease_s is not None:
            seen = set(ids)
            scan_started = time.monotonic()
            over = await repo.over_lease_ids(s, max_lease_s, batch_size, skip)
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
        hits: list[repo.Finish] = []
        try:
            async with sm() as s, s.begin():
                done = await reclaim_chunk(s, chunk, cap_hits=hits, **kw)
        except Exception as exc:  # poisoned chunk: isolate the bad row, keep the rest
            log.error("reclaim_chunk_failed", size=len(chunk), error_type=type(exc).__name__)
            done = {}
            hits = []
            failed: list[tuple[uuid.UUID, BaseException]] = []
            proved = False  # a peer row of this chunk committed an expiry, so the fault is a row's
            if len(chunk) > 1 and not systemic:
                for aid in chunk:
                    row_hits: list[repo.Finish] = []
                    try:
                        async with sm() as s, s.begin():
                            one = await reclaim_chunk(s, [aid], cap_hits=row_hits, **kw)
                        done[aid] = one[aid]
                        hits += row_hits
                        proved = proved or one[aid] in _EXPIRED
                    except Exception as exc2:  # one bad row must not block the rest of the batch
                        log.error(
                            "reclaim_failed",
                            acquirement_id=str(aid),
                            error_type=type(exc2).__name__,
                        )
                        failed.append((aid, exc2))
                if failed and not proved:
                    systemic = True
                    log.error(
                        "reclaim_chunk_systemic",
                        size=len(chunk),
                        failed=len(failed),
                        error_type=type(failed[0][1]).__name__,
                    )
                elif proved:
                    for bad, err in failed:
                        if _row_specific(err):
                            added += 1
                            evicted += _quarantine_add(bad, type(err).__name__)
        for f in hits:
            log.info(
                "unacked_cap_reached",
                acquirement_id=str(f.owned.acquirement_id),
                subtask_id=f.owned.subtask_id,
                attempt=f.owned.attempt,
                free_cap=unacked_free_cap,
            )
        for aid, res in done.items():
            results[res] += 1
            if res in _EXPIRED:
                log.info("subtask_reclaimed", acquirement_id=str(aid), result=res.value)
    if added or evicted or lapsed:
        log.info(
            "reclaim_quarantine",
            size=len(_QUARANTINE),
            added=added,
            evicted=evicted,
            expired=len(lapsed),
            evicted_total=_quarantine_evicted,
        )
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
