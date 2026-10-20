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
The claim is a lease fenced by `split_attempts`: every write by the holder (lease renewal, done,
failed, retry) matches on it, so a holder that lost its lease becomes a no-op. The lease is renewed
by a background timer every `lease_s / 3`, which also enforces the wall-clock budget, so one long
blocking object read cannot outlive it; the timer trips a stop flag that the read thread checks per
S3 chunk. Each batch insert re-checks the lease in its own transaction (the fence). A shutdown
during a read stops the thread the same way and does not count as a failed attempt.
Only the error class and task id are logged, never payload contents.
"""

import asyncio
import contextlib
import threading
import time
from collections.abc import AsyncGenerator, Generator, Iterator
from contextlib import aclosing
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
from minion_server.storage import PreconditionFailedError, S3Storage
from minion_server.uris import InvalidUriError, parse_s3_uri

log = structlog.get_logger()

BATCH_SIZE = 1000
STOP_WAIT_S = 15.0  # > the S3 read timeout (10 s): how long a shutdown waits for the read thread
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
    max_seconds: float = 3600.0

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
            max_seconds=s.split_max_seconds,
        )


class SplitFailedError(Exception):
    """Deterministic failure: the task is marked FAILED with this message, no retry."""


class LeaseLostError(Exception):
    """Another splitter claimed the task (our lease expired); abandon this split quietly."""


class SplitTimeoutError(Exception):
    """The per-attempt wall-clock budget ran out; handled as a transient failure (backoff)."""


INPUT_CHANGED_DURING_SPLIT = "INPUT_CHANGED: input object changed while it was being split"


class _SplitAbortedError(Exception):
    """Internal: the read thread was asked to stop (shutdown); never escapes `_plan_batches`."""


class _SplitGuard:
    """Stop flag for one split attempt: set on the event loop, checked by the read thread."""

    def __init__(self) -> None:
        self._event = threading.Event()
        self._reason: type[Exception] = _SplitAbortedError

    def trip(self, reason: type[Exception]) -> None:
        if not self._event.is_set():  # the first reason wins (only the event loop trips)
            self._reason = reason
            self._event.set()

    def check(self) -> None:
        if self._event.is_set():
            raise self._reason

    def chunks(self, it: Iterator[bytes]) -> Generator[bytes]:
        """Pass `it` through, but stop at the next chunk once tripped."""
        for chunk in it:
            self.check()
            yield chunk


def _take(it: Iterator[Slice], n: int) -> list[Slice]:
    return list(islice(it, n))


class _ThreadReader:
    """Reads slices in a worker thread, cancellation-safe.

    A cancelled await must not close the generator while the thread is still inside it (that turns
    the `CancelledError` into `ValueError: generator already executing`): stop the thread through
    the guard, wait for it, and only then let the caller close.
    """

    def __init__(
        self, raw: Generator[bytes], guard: _SplitGuard, records_per_subtask: int, max_line: int
    ) -> None:
        self._guard = guard
        self._raw = raw
        self._chunks = guard.chunks(raw)
        self._slices = slice_jsonl(self._chunks, 0, records_per_subtask, max_line)
        self._fut: asyncio.Future[list[Slice]] | None = None

    async def take(self, n: int) -> list[Slice]:
        fut: asyncio.Future[list[Slice]] = asyncio.ensure_future(
            asyncio.to_thread(_take, self._slices, n)
        )
        fut.add_done_callback(lambda f: f.cancelled() or f.exception())  # never "not retrieved"
        self._fut = fut
        try:
            return await asyncio.shield(fut)
        except asyncio.CancelledError:
            self._guard.trip(_SplitAbortedError)  # the thread stops at its next chunk
            await asyncio.wait([fut], timeout=STOP_WAIT_S)
            raise

    def close(self) -> None:
        if self._fut is not None and not self._fut.done():
            return  # the thread is still in the generators: leave them to the GC
        self._slices.close()
        self._chunks.close()
        self._raw.close()


async def _plan_batches(
    storage: S3Storage, task: Task, limits: SplitLimits, batch_size: int, guard: _SplitGuard
) -> AsyncGenerator[list[NewSubtask]]:
    """Stream the plan in batches; memory is bounded by `batch_size` and the object listing."""
    prefix = parse_s3_uri(task.input_uri, bucket=storage.bucket, prefixes=("",)).key
    listing = await storage.list_objects(prefix, max_keys=limits.max_objects)
    if len(listing) > limits.max_objects:
        raise SplitFailedError(f"input has more than {limits.max_objects} objects")
    if sum(size for _, size, _ in listing) > limits.max_input_bytes:
        raise SplitFailedError(f"input is larger than {limits.max_input_bytes} bytes")
    next_id = 0
    first_record = 0
    batch: list[NewSubtask] = []
    for key, size, etag in listing:  # sorted -> deterministic ids
        if size == 0:
            continue
        reader = _ThreadReader(
            storage.iter_object(key, etag=etag, size=size),
            guard,
            task.records_per_subtask,
            limits.max_line_bytes,
        )
        try:
            while chunk := await reader.take(batch_size):
                guard.check()
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
        except PreconditionFailedError as exc:  # replaced/grown after listing
            raise SplitFailedError(INPUT_CHANGED_DURING_SPLIT) from exc
        except LineTooLongError as exc:
            raise SplitFailedError(
                f"LINE_TOO_LONG: a line in an input object exceeds {limits.max_line_bytes} bytes"
            ) from exc
        finally:
            reader.close()
    if batch:
        yield batch


async def _renew_lease(
    sessionmaker: async_sessionmaker[AsyncSession],
    task: Task,
    limits: SplitLimits,
    deadline: float,
    guard: _SplitGuard,
) -> None:
    """Extend the lease every `lease_s / 3` and enforce the wall-clock budget, until tripped.

    Runs on the event loop, so it keeps ticking while a worker thread blocks on an S3 read. Each
    tick is one fenced UPDATE in its own short transaction. A database error is only logged:
    every holder write is fenced, so a lapsed lease is detected at the next write.
    """
    interval = limits.lease_s / 3
    while True:
        await asyncio.sleep(max(0.0, min(interval, deadline - time.monotonic())))
        if time.monotonic() > deadline:
            guard.trip(SplitTimeoutError)
            return
        try:
            async with sessionmaker() as s, s.begin():
                held = await tasks_repo.renew_split_lease(s, task, limits.lease_s)
        except Exception as exc:
            log.warning("task_split_renew_error", task=tid_of(task), error_class=type(exc).__name__)
            continue
        if not held:  # re-claimed by another splitter, or the task was cancelled
            guard.trip(LeaseLostError)
            return


async def _insert_batch(
    sessionmaker: async_sessionmaker[AsyncSession],
    task: Task,
    batch: list[NewSubtask],
    lease_s: float,
) -> None:
    def stored(have: dict[int, tuple[str, int, int]]) -> bool:
        return all(
            have.get(r.subtask_id) == (r.input_object_key, r.byte_start, r.byte_end) for r in batch
        )

    async with sessionmaker() as s, s.begin():
        if not await tasks_repo.renew_split_lease(s, task, lease_s):
            raise LeaseLostError
        have = await tasks_repo.existing_subtasks(s, task, [r.subtask_id for r in batch])
        for r in batch:
            prev = have.get(r.subtask_id)
            if prev is not None and prev != (r.input_object_key, r.byte_start, r.byte_end):
                raise SplitFailedError("INPUT_CHANGED: input differs from a previous split attempt")
        await tasks_repo.insert_subtasks(s, task, batch)
        # Read back what is really stored: a concurrent writer must not leave other rows behind.
        after = await tasks_repo.existing_subtasks(s, task, [r.subtask_id for r in batch])
        if not stored(after):
            raise SplitFailedError("INPUT_CHANGED: stored subtasks differ from the split plan")


async def _split(
    sessionmaker: async_sessionmaker[AsyncSession],
    storage: S3Storage,
    task: Task,
    limits: SplitLimits,
    batch_size: int,
) -> int:
    total = 0
    deadline = time.monotonic() + limits.max_seconds
    guard = _SplitGuard()
    renewer = asyncio.create_task(_renew_lease(sessionmaker, task, limits, deadline, guard))
    try:
        async with aclosing(_plan_batches(storage, task, limits, batch_size, guard)) as batches:
            async for batch in batches:
                guard.check()
                if time.monotonic() > deadline:
                    raise SplitTimeoutError
                await _insert_batch(sessionmaker, task, batch, limits.lease_s)
                total += len(batch)
    finally:
        renewer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await renewer
        current = asyncio.current_task()
        if current is not None and current.cancelling():
            raise asyncio.CancelledError  # our own cancellation is never swallowed
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
        task = await tasks_repo.claim_splitting(claim, limits.lease_s, limits.max_attempts)
    if task is None:
        return False
    tid = f"{task.team}/{task.project}/{task.task_name}"
    try:
        total = await _split(sessionmaker, storage, task, limits, batch_size)
    except LeaseLostError:
        log.warning("task_split_lease_lost", task=tid)
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
            done = await tasks_repo.mark_split_done(s, task, total)
        if done:
            log.info("task_split", task=tid, subtasks=total)
        else:
            log.warning("task_split_lease_lost", task=tid)
    return True


def tid_of(task: Task) -> str:
    return f"{task.team}/{task.project}/{task.task_name}"


async def _fail(sessionmaker: async_sessionmaker[AsyncSession], task: Task, error: str) -> None:
    async with sessionmaker() as s, s.begin():
        if not await tasks_repo.mark_split_failed(s, task, error):
            log.warning("task_split_lease_lost", task=tid_of(task))


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
        if not await tasks_repo.schedule_split_retry(s, task, delay):
            log.warning("task_split_lease_lost", task=tid)
            return
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
