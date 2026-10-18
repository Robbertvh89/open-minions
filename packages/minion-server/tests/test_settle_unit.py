"""Settle failure metric and the settle_stuck round loop (ADR-0015), without a database."""

from typing import Any

import pytest
from minion_server.metrics import Metrics
from minion_server.repositories import completion as completion_repo
from minion_server.repositories.completion import TaskKey
from minion_server.services import completion, reclaim


class _Tx:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *a: object) -> None:
        return None


class _Session:
    def begin(self) -> _Tx:
        return _Tx()

    async def __aenter__(self) -> "_Session":
        return self

    async def __aexit__(self, *a: object) -> None:
        return None


def _sm() -> Any:
    return lambda: _Session()


def _patch(
    monkeypatch: pytest.MonkeyPatch, reads: list[int], *, fail: bool = False, finite: int = -1
) -> None:
    left = [finite]

    async def settleable(session: object, limit: int) -> list[TaskKey]:
        reads.append(limit)
        n = limit if left[0] < 0 else min(limit, left[0])
        if left[0] >= 0:
            left[0] -= n
        return [TaskKey("t", "p", f"n{i}") for i in range(n)]

    async def settle(session: object, key: TaskKey) -> bool:
        if fail:
            raise RuntimeError("x")
        return True

    monkeypatch.setattr(completion_repo, "settleable_tasks", settleable)
    monkeypatch.setattr(reclaim, "settle_in_tx", settle)


async def test_settle_task_failure_counts_without_labels(monkeypatch: pytest.MonkeyPatch) -> None:
    async def boom(*a: object, **k: object) -> bool:
        raise RuntimeError("db down")

    monkeypatch.setattr(completion, "settle_in_tx", boom)
    m = Metrics()
    await completion.settle_task(_Session(), TaskKey("t", "p", "n"), m)  # type: ignore[arg-type]
    body = m.render().decode()
    assert "minion_task_settle_failed_total 1.0" in body
    assert "settle_failed_total{" not in body


async def test_settle_stuck_loops_while_batch_full(monkeypatch: pytest.MonkeyPatch) -> None:
    reads: list[int] = []
    _patch(monkeypatch, reads, finite=5)
    assert await reclaim.settle_stuck(_sm(), batch=2) == 5
    assert len(reads) == 3  # 2, 2, 1 (the short batch ends it)


async def test_settle_stuck_round_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    reads: list[int] = []
    _patch(monkeypatch, reads)  # always a full batch
    assert await reclaim.settle_stuck(_sm(), batch=2, max_rounds=3) == 6
    assert len(reads) == 3


async def test_settle_stuck_stops_when_a_settle_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    reads: list[int] = []
    _patch(monkeypatch, reads, fail=True)
    assert await reclaim.settle_stuck(_sm(), batch=2) == 0
    assert len(reads) == 1
