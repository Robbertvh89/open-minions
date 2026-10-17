"""Acquire service: two-pass candidate selection, one transaction per candidate (plan D5)."""

import random
from datetime import UTC, datetime, timedelta

from minion_common.schemas import AcquireRequest, Assignment, ModelEntry
from sqlalchemy.ext.asyncio import AsyncSession

from minion_server.auth import ExecutorPrincipal
from minion_server.repositories import acquisition as repo
from minion_server.repositories import executors as executors_repo
from minion_server.settings import ServerSettings
from minion_server.storage import MAX_PRESIGN_TTL_S, S3Storage
from minion_server.uris import parse_s3_uri

SHUFFLE_HEAD = 4
MAX_ROUNDS = 10


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
    shuffle_head: int = SHUFFLE_HEAD,
    rng: random.Random | None = None,
) -> Assignment | None:
    rng = rng or random.Random()  # noqa: S311 - contention spreading, not security
    async with session.begin():
        await executors_repo.touch(
            session,
            principal.executor_id,
            engines=[e.value for e in req.engines],
            profile=req.budget.profile.value,
            budget=req.budget.model_dump(mode="json"),
        )
        preference = await executors_repo.get_preference(session, principal.executor_id)

    passes = [preference, None] if preference is not None else [None]
    for pref in passes:
        # Losing every candidate means they were all taken meanwhile: re-read the queue.
        for _ in range(MAX_ROUNDS):
            async with session.begin():
                cands = await repo.candidates(
                    session,
                    engines=req.engines,
                    model_ids=list(models),
                    memory_gb=req.budget.memory_gb,
                    pool=principal.pool,
                    preference=pref,
                )
            if not cands:
                break
            head = cands[:shuffle_head]
            rng.shuffle(head)
            for c in head + cands[shuffle_head:]:
                model = models[c.model_id]  # candidates are filtered to registry keys in SQL
                try:
                    async with session.begin():
                        won = await repo.try_acquire(session, c, principal.executor_id)
                        if won is None:
                            raise _LostRaceError
                except _LostRaceError:
                    continue
                return await _assignment(c, won, model, settings, storage)
    return None


async def _assignment(
    c: repo.Candidate,
    won: repo.Won,
    model: ModelEntry,
    settings: ServerSettings,
    storage: S3Storage,
) -> Assignment:
    out = parse_s3_uri(c.output_uri, bucket=settings.s3_bucket, prefixes=("",))
    ttl = min(settings.presign_ttl_s, MAX_PRESIGN_TTL_S)
    input_url = await storage.presign_get(c.input_object_key, ttl, (c.byte_start, c.byte_end))
    output_url = await storage.presign_put(attempt_key(out.key, c.subtask_id, won.attempt), ttl)
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
        output_url=output_url,
        expires_at=datetime.now(UTC) + timedelta(seconds=ttl),
        max_output_bytes=settings.max_output_bytes,
    )
