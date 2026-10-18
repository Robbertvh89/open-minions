"""Acquire service: one-statement claim per round (ADR-0013)."""

import time
import uuid
from datetime import UTC, datetime, timedelta

import structlog
from minion_common.schemas import AcquireRequest, Assignment, ModelEntry
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.auth import ExecutorPrincipal
from minion_server.errors import ExecutorRevokedError, busy_error, capped_error
from minion_server.repositories import acquisition as repo
from minion_server.repositories import executors as executors_repo
from minion_server.settings import ServerSettings
from minion_server.storage import MAX_PRESIGN_TTL_S, S3Storage
from minion_server.uris import parse_s3_uri

MAX_ROUNDS = 10
_log = structlog.get_logger(__name__)


_CAPPED_LOG_EVERY_S = 60.0
_CAPPED_LOG_MAX = 10_000
_capped_logged: dict[uuid.UUID, float] = {}  # executor_id -> monotonic time of the last log


def _log_capped(executor_id: uuid.UUID, open_now: int) -> None:
    """Tell the operator which token sits at the cap (ids and a count only), at most once per
    executor per minute per process; the map is bounded (oldest half dropped when full)."""
    now = time.monotonic()
    last = _capped_logged.get(executor_id)
    if last is not None and now - last < _CAPPED_LOG_EVERY_S:
        return
    if len(_capped_logged) >= _CAPPED_LOG_MAX:
        for k in sorted(_capped_logged, key=_capped_logged.__getitem__)[: _CAPPED_LOG_MAX // 2]:
            del _capped_logged[k]
    _capped_logged[executor_id] = now
    _log.warning("acquire_capped", executor_id=str(executor_id), open_acquirements=open_now)


class _LostRaceError(Exception):
    """Raised inside a transaction to roll it back after losing the race."""


def attempt_key(output_prefix_key: str, subtask_id: int, attempt: int) -> str:
    """`{output_prefix}/_attempts/{subtask_id:06d}/{attempt}.jsonl` (ARCHITECTURE §3.3)."""
    return f"{output_prefix_key.rstrip('/')}/_attempts/{subtask_id:06d}/{attempt}.jsonl"


async def acquire(
    session: AsyncSession,
    principal: ExecutorPrincipal,
    req: AcquireRequest,
    *,
    settings: ServerSettings,
    storage: S3Storage,
    models: dict[str, ModelEntry],
    window: int | None = None,
    deadline: float | None = None,
) -> Assignment | None:
    """Claim one subtask. `deadline` is an absolute `time.monotonic()` value: a round that gets
    its connection after it answers 503 ACQUIRE_DEADLINE before the claim INSERT, so nothing is
    committed for a client that has probably given up (TASK-7.4.4)."""
    window = settings.acquire_window if window is None else window
    if not 1 <= window <= repo.MAX_WINDOW:
        raise ValueError(f"acquire window must be in 1..{repo.MAX_WINDOW}, got {window}")
    async with session.begin():
        live = await executors_repo.touch(
            session,
            principal.executor_id,
            engines=[e.value for e in req.engines],
            profile=req.budget.profile.value,
            budget=req.budget.model_dump(mode="json"),
            token_hash=principal.token_hash,
        )
        if not live:  # revoked / rotated inside the auth-cache TTL: nothing may be claimed
            raise ExecutorRevokedError(principal.token_hash)
        preference = await executors_repo.get_preference(session, principal.executor_id)

    passes = [preference, None] if preference is not None else [None]
    for pref in passes:
        # A round whose pick lost the insert race (n > 0, no row) re-reads a fresh window.
        exhausted = True
        for _ in range(MAX_ROUNDS):
            try:
                async with session.begin():
                    await session.connection()  # the pool wait happens here, not in the claim
                    if deadline is not None and time.monotonic() >= deadline:
                        raise busy_error("ACQUIRE_DEADLINE", "acquire deadline exceeded")
                    if deadline is not None:
                        # the claim may not outlive the deadline (a lock wait on the acquirement
                        # unique index, a slow plan): the statement is cancelled and nothing commits
                        left_ms = int((deadline - time.monotonic()) * 1000) + 100
                        await repo.set_statement_timeout(session, max(100, left_ms))
                    # Per-executor cap (ADR-0019): lock own row, then a separate count statement;
                    # a capped round ends before any candidate read. Admission only: ownership is
                    # still decided by the claim INSERT ... ON CONFLICT DO NOTHING.
                    if not await repo.lock_executor_for_claim(
                        session, principal.executor_id, principal.token_hash
                    ):
                        raise ExecutorRevokedError(principal.token_hash)
                    open_now = await repo.count_open_acquirements(session, principal.executor_id)
                    if open_now >= settings.max_open_acquirements:
                        _log_capped(principal.executor_id, open_now)
                        raise capped_error()
                    result = await repo.claim(
                        session,
                        principal.executor_id,
                        engines=req.engines,
                        model_ids=list(models),
                        memory_gb=req.budget.memory_gb,
                        pool=principal.pool,
                        preference=pref,
                        window=window,
                    )
                    if result.n_candidates > 0 and result.won is None:
                        raise _LostRaceError  # roll back a failed re-check; harmless otherwise
                    if (
                        result.won is not None
                        and deadline is not None
                        and time.monotonic() >= deadline
                    ):
                        # the claim returned too late: roll back so no acquirement is committed
                        raise busy_error("ACQUIRE_DEADLINE", "acquire deadline exceeded")
            except _LostRaceError:
                continue
            except DBAPIError as exc:
                if deadline is not None and repo.is_query_canceled(exc):
                    raise busy_error("ACQUIRE_DEADLINE", "acquire deadline exceeded") from None
                raise
            if result.n_candidates == 0:
                exhausted = False
                break
            if result.candidate is None or result.won is None:  # unreachable: raised above
                continue
            model = models[result.candidate.model_id]  # candidates are filtered to registry keys
            return await _assignment(result.candidate, result.won, model, settings, storage)
        if exhausted:  # every round had work but lost the race: answer "empty", executor re-polls
            _log.warning(
                "acquire_rounds_exhausted",
                rounds=MAX_ROUNDS,
                window=window,
                preferred_pass=pref is not None,
            )
    return None


async def _assignment(
    c: repo.Candidate,
    won: repo.Won,
    model: ModelEntry,
    settings: ServerSettings,
    storage: S3Storage,
) -> Assignment:
    try:
        return await _build_assignment(c, won, model, settings, storage)
    except Exception as exc:  # the claim is already committed: leave a trace of the orphan
        _log.error("assignment_build_failed", error=type(exc).__name__)
        raise


async def _build_assignment(
    c: repo.Candidate,
    won: repo.Won,
    model: ModelEntry,
    settings: ServerSettings,
    storage: S3Storage,
) -> Assignment:
    out = parse_s3_uri(c.output_uri, bucket=settings.s3_bucket, prefixes=("",))
    ttl = min(settings.presign_ttl_s, MAX_PRESIGN_TTL_S)
    input_url = await storage.presign_get(c.input_object_key, ttl, (c.byte_start, c.byte_end))
    akey = attempt_key(out.key, c.subtask_id, won.attempt)
    output_post = await storage.presign_post(akey, settings.max_output_bytes, ttl)
    return Assignment(
        acquirement_id=won.acquirement_id,
        attempt=won.attempt,
        team=c.team,
        project=c.project,
        task_name=c.task_name,
        subtask_id=c.subtask_id,
        type=model.type,
        model=model,
        params=c.params,
        input_url=input_url,
        byte_start=c.byte_start,
        byte_end=c.byte_end,
        record_count=c.record_count,
        output_post=output_post,
        expires_at=datetime.now(UTC) + timedelta(seconds=ttl),
        max_output_bytes=settings.max_output_bytes,
    )
