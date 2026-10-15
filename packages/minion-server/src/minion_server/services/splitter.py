"""Asynchronous, resumable task splitter (ARCHITECTURE §3.1).

A SPLITTING task is *claimed* with a lease (`next_split_at`, `split_attempts`) in a short
transaction, then split with no transaction held open. Plan batches are streamed straight into
`insert_subtasks` (own transaction, `ON CONFLICT DO NOTHING`); ids and slices are deterministic
(sorted keys, newline-aligned), so a crashed split is simply re-run and converges on identical
rows. On resume, already-inserted rows are compared with the new plan: a difference means the
input changed underneath us (`INPUT_CHANGED`).

Failure policy per task, so one bad task never blocks the queue:
- deterministic problems (bad URI, empty input, limits, unreadable S3 4xx) -> FAILED at once;
- anything else -> exponential backoff via `next_split_at`; FAILED after `max_attempts`.
Only the error class and task id are logged, never payload contents.
"""

import asyncio
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from itertools import islice

import structlog
from botocore.exceptions import ClientError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from minion_server.db.models import Task
from minion_server.repositories import tasks as tasks_repo
from minion_server.repositories.tasks import NewSubtask
from minion_server.settings import ServerSettings
from minion_server.splitting import LineTooLongError, Slice, slice_jsonl
from minion_server.storage import S3Storage
from minion_server.uris import InvalidUriError, parse_s3_uri

log = structlog.get_logger()

BATCH_SIZE = 1000
EMPTY_INPUT_ERROR = "input prefix contains no records"
DETERMINISTIC_S3_CODES = frozenset(
    {"AccessDenied", "NoSuchKey", "NoSuchBucket", "InvalidObjectState", "403", "404"}
)


@dataclass(frozen=True)
class SplitLimits:
    max_line_bytes: int = 16 << 20
    max_input_bytes: int = 50 << 30
    max_objects: int = 10_000
    max_subtasks: int = 100_000
    max_attempts: int = 5
    backoff_base_s: float = 2.0
    backoff_max_s: float = 300.0
    lease_s: float = 900.0

    @classmethod
    def from_settings(cls, s: ServerSettings) -> "SplitLimits":
        return cls(
            max_line_bytes=s.split_max_line_bytes,
            max_input_bytes=s.split_max_input_bytes,
            max_objects=s.split_max_objects,
            max_subtasks=s.split_max_subtasks,
            max_attempts=s.split_max_attempts,
            backoff_base_s=s.split_backoff_base_s,
            backoff_max_s=s.split_backoff_max_s,
            lease_s=s.split_lease_s,
        )


class SplitFailedError(Exception):
    """Deterministic failure: the task is marked FAILED with this message, no retry."""


def _take(it: Iterator[Slice], n: int) -> list[Slice]:
    return list(islice(it, n))


async def _plan_batches(
    storage: S3Storage, task: Task, limits: SplitLimits, batch_size: int
) -> AsyncIterator[list[NewSubtask]]:
    """Stream the plan in batches; memory is bounded by `batch_size` and the object listing."""
    prefix = parse_s3_uri(task.input_uri, bucket=storage.bucket, prefixes=("",)).key
    listing = await storage.list_keys(prefix, max_keys=limits.max_objects)
    if len(listing) > limits.max_objects:
        raise SplitFailedError(f"input has more than {limits.max_objects} objects")
    if sum(size for _, size in listing) > limits.max_input_bytes:
        raise SplitFailedError(f"input is larger than {limits.max_input_bytes} bytes")
    next_id = 0
    first_record = 0
    batch: list[NewSubtask] = []
    for key, size in listing:  # sorted -> deterministic ids
        if size == 0:
            continue
        slices = slice_jsonl(
            storage.iter_object(key), 0, task.records_per_subtask, limits.max_line_bytes
        )
        try:
            while chunk := await asyncio.to_thread(_take, slices, batch_size):
                for sl in chunk:
                    batch.append(
                        NewSubtask(
                            next_id, key, sl.byte_start, sl.byte_end, first_record, sl.record_count
                        )
                    )
                    next_id += 1
                    first_record += sl.record_count
                    if next_id > limits.max_subtasks:
                        raise SplitFailedError(
                            f"input yields more than {limits.max_subtasks} subtasks"
                        )
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []
        except LineTooLongError as exc:
            raise SplitFailedError(
                f"LINE_TOO_LONG: a line in an input object exceeds {limits.max_line_bytes} bytes"
            ) from exc
        finally:
            slices.close()
    if batch:
        yield batch


async def _insert_batch(
    sessionmaker: async_sessionmaker[AsyncSession], task: Task, batch: list[NewSubtask]
) -> None:
    async with sessionmaker() as s, s.begin():
        have = await tasks_repo.existing_subtasks(s, task, [r.subtask_id for r in batch])
        for r in batch:
            prev = have.get(r.subtask_id)
            if prev is not None and prev != (r.input_object_key, r.byte_start, r.byte_end):
                raise SplitFailedError("INPUT_CHANGED: input differs from a previous split attempt")
        await tasks_repo.insert_subtasks(s, task, batch)


async def _split(
    sessionmaker: async_sessionmaker[AsyncSession],
    storage: S3Storage,
    task: Task,
    limits: SplitLimits,
    batch_size: int,
) -> int:
    total = 0
    async for batch in _plan_batches(storage, task, limits, batch_size):
        await _insert_batch(sessionmaker, task, batch)
        total += len(batch)
    if total == 0:
        raise SplitFailedError(EMPTY_INPUT_ERROR)
    async with sessionmaker() as s, s.begin():
        if await tasks_repo.count_subtasks(s, task) != total:  # leftovers from a longer old plan
            raise SplitFailedError("INPUT_CHANGED: input shrank since a previous split attempt")
    return total


def backoff_s(attempts: int, limits: SplitLimits) -> float:
    return float(min(limits.backoff_max_s, limits.backoff_base_s * 2 ** max(attempts - 1, 0)))


async def split_one(
    sessionmaker: async_sessionmaker[AsyncSession],
    storage: S3Storage,
    limits: SplitLimits | None = None,
    batch_size: int = BATCH_SIZE,
) -> bool:
    """Claim and split one due task. Returns False if nothing was due."""
    limits = limits or SplitLimits()
    async with sessionmaker() as claim, claim.begin():
        task = await tasks_repo.claim_splitting(claim, limits.lease_s)
    if task is None:
        return False
    tid = f"{task.team}/{task.project}/{task.task_name}"
    try:
        total = await _split(sessionmaker, storage, task, limits, batch_size)
    except SplitFailedError as exc:
        await _fail(sessionmaker, task, str(exc))
        log.warning("task_split_failed", task=tid, error_class=type(exc).__name__)
    except InvalidUriError as exc:  # deterministic: retrying would never succeed
        await _fail(sessionmaker, task, "input_uri is not allowed")
        log.warning("task_split_failed", task=tid, error_class=type(exc).__name__)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in DETERMINISTIC_S3_CODES:
            await _fail(sessionmaker, task, f"input is not readable ({code})")
            log.warning("task_split_failed", task=tid, error_class=type(exc).__name__, code=code)
        else:
            await _retry(sessionmaker, task, limits, type(exc).__name__, tid)
    except Exception as exc:
        await _retry(sessionmaker, task, limits, type(exc).__name__, tid)
    else:
        async with sessionmaker() as s, s.begin():
            await tasks_repo.mark_split_done(s, task, total)
        log.info("task_split", task=tid, subtasks=total)
    return True


async def _fail(sessionmaker: async_sessionmaker[AsyncSession], task: Task, error: str) -> None:
    async with sessionmaker() as s, s.begin():
        await tasks_repo.mark_split_failed(s, task, error)


async def _retry(
    sessionmaker: async_sessionmaker[AsyncSession],
    task: Task,
    limits: SplitLimits,
    error_class: str,
    tid: str,
) -> None:
    # `task.split_attempts` already includes this attempt (bumped by the claim).
    if task.split_attempts >= limits.max_attempts:
        await _fail(sessionmaker, task, f"splitting failed after {task.split_attempts} attempts")
        log.error("task_split_gave_up", task=tid, error_class=error_class)
        return
    delay = backoff_s(task.split_attempts, limits)
    async with sessionmaker() as s, s.begin():
        await tasks_repo.schedule_split_retry(s, task, delay)
    log.warning("task_split_retry", task=tid, error_class=error_class, delay_s=delay)


async def run_splitter(
    sessionmaker: async_sessionmaker[AsyncSession],
    storage: S3Storage,
    poll_s: float,
    limits: SplitLimits | None = None,
) -> None:
    """Background loop; cancelled on shutdown. Errors are logged and retried next poll."""
    while True:
        try:
            while await split_one(sessionmaker, storage, limits):
                pass
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error("splitter_error", error_class=type(exc).__name__)
        await asyncio.sleep(poll_s)
