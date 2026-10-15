"""Complete / fail / release (plan D6). Ownership is checked on every call (invariant 3)."""

import uuid

from minion_common.enums import AttemptOutcome, SubtaskStatus, TaskStatus
from minion_common.schemas import CompleteRequest, FailRequest
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.auth import ExecutorPrincipal
from minion_server.errors import api_error
from minion_server.repositories import completion as repo
from minion_server.services.acquisition import attempt_key
from minion_server.settings import ServerSettings
from minion_server.storage import PreconditionFailedError, S3Storage
from minion_server.uris import parse_s3_uri

STALE_MESSAGE = "acquirement is not current"


def derive_task_status(total: int, succeeded: int, failed: int) -> TaskStatus | None:
    """Terminal task status once every subtask is terminal, else None (task keeps running)."""
    if total <= 0 or succeeded + failed < total:
        return None
    if failed == 0:
        return TaskStatus.SUCCEEDED
    return TaskStatus.PARTIAL if succeeded > 0 else TaskStatus.FAILED


def stale_error() -> Exception:
    # Identical body for "not found" and "not yours" so ids cannot be probed.
    return api_error(409, "STALE_ACQUIREMENT", STALE_MESSAGE)


async def _bump_and_derive(
    session: AsyncSession, o: repo.Owned, *, succeeded: int, failed: int
) -> None:
    counts = await repo.bump_counters(session, o, succeeded=succeeded, failed=failed)
    final = derive_task_status(counts.total, counts.succeeded, counts.failed)
    if final is not None:
        await repo.set_task_terminal(session, o, final)


async def complete(
    session: AsyncSession,
    principal: ExecutorPrincipal,
    acquirement_id: uuid.UUID,
    req: CompleteRequest,
    *,
    settings: ServerSettings,
    storage: S3Storage,
) -> None:
    async with session.begin():
        o = await repo.lock_owned(session, acquirement_id, principal.executor_id)
        if o is None:
            # A retried complete whose first response was lost: already done, same owner.
            if await repo.completed_by(session, acquirement_id) == principal.executor_id:
                return
            raise stale_error()
        if req.record_count != o.record_count:
            raise api_error(422, "RECORD_COUNT_MISMATCH", "record_count differs from the subtask")
        out = parse_s3_uri(o.task_output_uri, bucket=settings.s3_bucket, prefixes=("",))
        src = attempt_key(out.key, o.subtask_id, o.attempt)
        dst = f"{out.key.rstrip('/')}/{o.subtask_id:06d}.jsonl"
        stat = await storage.stat(src)
        if stat is None:
            raise api_error(422, "OUTPUT_MISSING", "attempt output object not found")
        if stat.size != req.byte_size:
            raise api_error(422, "OUTPUT_SIZE_MISMATCH", "attempt output size differs")
        try:
            # If-Match on the HEAD ETag closes the check-then-copy window (TOCTOU).
            await storage.copy(src, dst, if_match=stat.etag)  # idempotent if the commit fails
        except PreconditionFailedError as exc:
            raise api_error(
                422, "OUTPUT_CHANGED", "attempt output changed while being verified"
            ) from exc
        await repo.finish_attempt(
            session,
            o,
            outcome=AttemptOutcome.SUCCEEDED,
            subtask_status=SubtaskStatus.SUCCEEDED,
            output_uri=f"s3://{settings.s3_bucket}/{dst}",
        )
        await _bump_and_derive(session, o, succeeded=1, failed=0)


def next_status(
    used: int, max_attempts: int, *, retryable: bool, task_cancelled: bool
) -> SubtaskStatus:
    """Subtask status after a budget-consuming attempt (`used` includes that attempt)."""
    if task_cancelled:
        return SubtaskStatus.CANCELLED  # never requeue work of a cancelled task
    if not retryable or used >= max_attempts:
        return SubtaskStatus.FAILED
    return SubtaskStatus.PENDING


async def finish_failed(
    session: AsyncSession,
    o: repo.Owned,
    *,
    outcome: AttemptOutcome,
    error: dict[str, object],
    retryable: bool,
) -> SubtaskStatus:
    """Record a budget-consuming attempt (FAILED or EXPIRED); shared by fail, reclaim, release cap.

    Caller holds the acquirement lock. Lock order: acquirement -> heartbeat -> subtask -> task.
    """
    used = await repo.failed_attempt_count(session, o) + 1
    status = next_status(
        used,
        o.max_attempts,
        retryable=retryable,
        task_cancelled=o.task_status == TaskStatus.CANCELLED,
    )
    await repo.finish_attempt(session, o, outcome=outcome, subtask_status=status, error=error)
    if status == SubtaskStatus.FAILED:
        await _bump_and_derive(session, o, succeeded=0, failed=1)
    return status


async def fail(
    session: AsyncSession,
    principal: ExecutorPrincipal,
    acquirement_id: uuid.UUID,
    req: FailRequest,
) -> None:
    async with session.begin():
        o = await repo.lock_owned(session, acquirement_id, principal.executor_id)
        if o is None:
            raise stale_error()
        await finish_failed(
            session,
            o,
            outcome=AttemptOutcome.FAILED,
            error={"code": req.code, "message": req.message, "retryable": req.retryable},
            retryable=req.retryable,
        )


async def release(
    session: AsyncSession,
    principal: ExecutorPrincipal,
    acquirement_id: uuid.UUID,
    *,
    settings: ServerSettings,
) -> None:
    async with session.begin():
        o = await repo.lock_owned(session, acquirement_id, principal.executor_id)
        if o is None:
            raise stale_error()
        if o.task_status == TaskStatus.CANCELLED:
            await repo.finish_attempt(
                session,
                o,
                outcome=AttemptOutcome.CANCELLED,
                subtask_status=SubtaskStatus.CANCELLED,
            )
        elif await repo.released_attempt_count(session, o) >= settings.release_limit:
            # Repeated releases must not livelock a subtask: past the cap they cost budget.
            await finish_failed(
                session,
                o,
                outcome=AttemptOutcome.FAILED,
                error={
                    "code": "RELEASE_LIMIT",
                    "message": "release limit exceeded",
                    "retryable": True,
                },
                retryable=True,
            )
        else:
            await repo.finish_attempt(
                session,
                o,
                outcome=AttemptOutcome.RELEASED,
                subtask_status=SubtaskStatus.PENDING,
            )
