"""Asynchronous, resumable task splitter (ARCHITECTURE §3.1).

One SPLITTING task is claimed with `FOR UPDATE SKIP LOCKED` and the claim transaction is held
for the whole split. Subtasks are inserted in batches in their own transactions with
`ON CONFLICT DO NOTHING`; ids and slices are deterministic (sorted keys, newline-aligned), so a
crashed split is simply re-run and converges on identical rows.
"""

import asyncio

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from minion_server.db.models import Task
from minion_server.repositories import tasks as tasks_repo
from minion_server.repositories.tasks import NewSubtask
from minion_server.splitting import slice_jsonl
from minion_server.storage import S3Storage
from minion_server.uris import InvalidUriError, parse_s3_uri

log = structlog.get_logger()

BATCH_SIZE = 1000
EMPTY_INPUT_ERROR = "input prefix contains no records"


async def _plan(storage: S3Storage, task: Task) -> list[NewSubtask]:
    prefix = parse_s3_uri(task.input_uri, bucket=storage.bucket, prefixes=("",)).key
    plan: list[NewSubtask] = []
    first_record = 0
    for key, size in await storage.list_keys(prefix):  # sorted -> deterministic ids
        if size == 0:
            continue

        def scan(k: str = key) -> list[tuple[int, int, int]]:
            return [
                (s.byte_start, s.byte_end, s.record_count)
                for s in slice_jsonl(storage.iter_object(k), 0, task.records_per_subtask)
            ]

        for start, end, count in await asyncio.to_thread(scan):
            plan.append(NewSubtask(len(plan), key, start, end, first_record, count))
            first_record += count
    return plan


async def split_one(
    sessionmaker: async_sessionmaker[AsyncSession], storage: S3Storage, batch_size: int = BATCH_SIZE
) -> bool:
    """Split one claimed task. Returns False if nothing was waiting."""
    async with sessionmaker() as claim, claim.begin():
        task = await tasks_repo.claim_splitting(claim)
        if task is None:
            return False
        try:
            plan = await _plan(storage, task)
        except InvalidUriError:  # deterministic: retrying would block the queue forever
            await tasks_repo.mark_split_failed(claim, task, "input_uri is not allowed")
            return True
        if not plan:
            await tasks_repo.mark_split_failed(claim, task, EMPTY_INPUT_ERROR)
            return True
        for i in range(0, len(plan), batch_size):
            async with sessionmaker() as batch_session, batch_session.begin():
                await tasks_repo.insert_subtasks(batch_session, task, plan[i : i + batch_size])
        await tasks_repo.mark_split_done(claim, task, len(plan))
        log.info("task_split", team=task.team, project=task.project, subtasks=len(plan))
        return True


async def run_splitter(
    sessionmaker: async_sessionmaker[AsyncSession], storage: S3Storage, poll_s: float
) -> None:
    """Background loop; cancelled on shutdown. Errors are logged and retried next poll."""
    while True:
        try:
            while await split_one(sessionmaker, storage):
                pass
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("splitter_error")
        await asyncio.sleep(poll_s)
