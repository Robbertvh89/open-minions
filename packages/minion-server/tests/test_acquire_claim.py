"""Single-statement claim (ADR-0013): uniqueness under 200 racers, priority, window=1 order."""

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest
from minion_common.enums import Engine
from minion_common.schemas import ModelEntry
from minion_server.auth import ExecutorPrincipal
from minion_server.db.session import create_engine, create_sessionmaker
from minion_server.repositories import acquisition as repo
from minion_server.services.acquisition import acquire
from minion_server.settings import ServerSettings
from minion_server.storage import S3Storage
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from conftest import _req, _settings, make_executors, seed_task

MODELS = {
    "fake-embed": ModelEntry(
        model_id="fake-embed",
        type="embedding",
        engine=Engine.MLX,
        ref="o/m",
        revision="a" * 40,
        memory_gb=0.1,
    )
}


@pytest.fixture
async def big_engine(fresh_url: str) -> AsyncIterator[AsyncEngine]:
    eng = create_engine(SecretStr(fresh_url), pool_size=50, max_overflow=0)
    yield eng
    await eng.dispose()


@pytest.fixture
async def sm(big_engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return create_sessionmaker(big_engine)


async def _acquire(
    sm: async_sessionmaker[AsyncSession], p: ExecutorPrincipal, cfg: ServerSettings, **kw: Any
) -> Any:
    async with sm() as s:
        return await acquire(
            s, p, _req(), settings=cfg, storage=S3Storage(cfg), models=MODELS, **kw
        )


@pytest.mark.integration
async def test_200_racers_64_pending_unique_owners(
    sm: async_sessionmaker[AsyncSession], fresh_url: str
) -> None:
    cfg = _settings(fresh_url)
    await seed_task(sm, "r64", 64)
    execs = await make_executors(sm, 200)
    results = await asyncio.gather(*(_acquire(sm, p, cfg) for p, _ in execs))
    winners = [r for r in results if r is not None]
    assert len(winners) == 64
    assert len({w.subtask_id for w in winners}) == 64
    assert len({w.acquirement_id for w in winners}) == 64
    assert results.count(None) == 136
    async with sm() as s:
        owners, rows = (
            await s.execute(
                text(
                    "SELECT count(DISTINCT subtask_id), count(*)"
                    " FROM minion_subtask_acquirement WHERE task_name='r64'"
                )
            )
        ).one()
    assert (owners, rows) == (64, 64)


@pytest.mark.integration
async def test_never_lower_priority_while_higher_pending(
    sm: async_sessionmaker[AsyncSession], fresh_url: str
) -> None:
    cfg = _settings(fresh_url)
    await seed_task(sm, "low", 30, priority=0)
    await seed_task(sm, "high", 20, priority=5)
    execs = await make_executors(sm, 20)  # exactly as many racers as high-priority subtasks
    results = await asyncio.gather(*(_acquire(sm, p, cfg) for p, _ in execs))
    winners = [r for r in results if r is not None]
    assert len(winners) >= 10
    # a low win means a high subtask was still PENDING: with 20 racers one would stay unowned
    assert {w.task_name for w in winners} == {"high"}
    assert len({w.subtask_id for w in winners}) == len(winners)


@pytest.mark.integration
async def test_window_1_is_strict_fr22_order(
    sm: async_sessionmaker[AsyncSession], fresh_url: str
) -> None:
    cfg = _settings(fresh_url)
    await seed_task(sm, "o-a", 3)
    await seed_task(sm, "o-b", 3, priority=2)
    await seed_task(sm, "o-c", 3)
    await seed_task(sm, "o-d", 3, priority=2)
    ((p, _),) = await make_executors(sm, 1)
    async with sm() as s, s.begin():
        expected = [
            (c.task_name, c.subtask_id)
            for c in await repo.candidates(
                s,
                engines=[Engine.MLX],
                model_ids=["fake-embed"],
                memory_gb=8,
                pool="default",
                preference=None,
                limit=12,
            )
        ]
    assert len(expected) == 12 and expected[0][0] == "o-b"
    got = []
    for _ in range(12):
        a = await _acquire(sm, p, cfg, window=1)
        got.append((a.task_name, a.subtask_id))
    assert got == expected
