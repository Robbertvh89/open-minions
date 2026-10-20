"""Unit tests (no DB) for the split lease timer and the cancellation-safe read (TASK-11)."""

import asyncio
import itertools
import threading
import time
from collections.abc import Generator
from types import SimpleNamespace
from typing import Any

import pytest
from minion_server.repositories import tasks as tasks_repo
from minion_server.services import splitter
from minion_server.services.splitter import (
    LeaseLostError,
    SplitLimits,
    SplitTimeoutError,
    _renew_lease,
    _SplitAbortedError,
    _SplitGuard,
    _ThreadReader,
)


class _Ctx:
    async def __aenter__(self) -> "_Ctx":
        return self

    async def __aexit__(self, *a: object) -> None:
        return None

    def begin(self) -> "_Ctx":
        return self


def _sessionmaker() -> Any:
    return lambda: _Ctx()


_TASK: Any = SimpleNamespace(team="t", project="p", task_name="n")


def test_guard_first_reason_wins_and_check_is_noop_until_tripped() -> None:
    g = _SplitGuard()
    g.check()
    g.trip(LeaseLostError)
    g.trip(SplitTimeoutError)
    with pytest.raises(LeaseLostError):
        g.check()


def test_guard_chunks_stop_at_next_chunk_after_trip() -> None:
    g = _SplitGuard()
    it = g.chunks(iter([b"a", b"b", b"c"]))
    assert next(it) == b"a"
    g.trip(SplitTimeoutError)
    with pytest.raises(SplitTimeoutError):
        next(it)


async def test_renewer_ticks_every_third_of_the_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    times: list[float] = []

    async def renew(session: Any, task: Any, lease_s: float) -> bool:
        times.append(time.monotonic())
        return True

    monkeypatch.setattr(tasks_repo, "renew_split_lease", renew)
    g = _SplitGuard()
    t = asyncio.create_task(
        _renew_lease(_sessionmaker(), _TASK, SplitLimits(lease_s=0.3), time.monotonic() + 60, g)
    )
    await asyncio.sleep(0.55)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t
    assert len(times) >= 3
    gaps = [b - a for a, b in itertools.pairwise(times)]
    assert all(0.05 < gap < 0.25 for gap in gaps)
    g.check()  # not tripped


async def test_renewer_trips_lease_lost_when_not_held(monkeypatch: pytest.MonkeyPatch) -> None:
    async def renew(session: Any, task: Any, lease_s: float) -> bool:
        return False

    monkeypatch.setattr(tasks_repo, "renew_split_lease", renew)
    g = _SplitGuard()
    await _renew_lease(_sessionmaker(), _TASK, SplitLimits(lease_s=0.03), time.monotonic() + 60, g)
    with pytest.raises(LeaseLostError):
        g.check()


async def test_renewer_survives_a_database_error(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    async def renew(session: Any, task: Any, lease_s: float) -> bool:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("blip")
        return True

    monkeypatch.setattr(tasks_repo, "renew_split_lease", renew)
    g = _SplitGuard()
    t = asyncio.create_task(
        _renew_lease(_sessionmaker(), _TASK, SplitLimits(lease_s=0.06), time.monotonic() + 60, g)
    )
    await asyncio.sleep(0.2)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t
    assert calls >= 2
    g.check()


async def test_renewer_trips_timeout_at_the_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    async def renew(session: Any, task: Any, lease_s: float) -> bool:
        raise AssertionError("must not renew past the deadline")

    monkeypatch.setattr(tasks_repo, "renew_split_lease", renew)
    g = _SplitGuard()
    await _renew_lease(_sessionmaker(), _TASK, SplitLimits(lease_s=30), time.monotonic() - 1, g)
    with pytest.raises(SplitTimeoutError):
        g.check()


async def test_cancel_during_thread_read_is_clean_and_close_is_safe() -> None:
    started = threading.Event()

    def slow() -> Generator[bytes]:
        while True:
            started.set()
            time.sleep(0.05)
            yield b'{"a":1}\n'

    g = _SplitGuard()
    reader = _ThreadReader(slow(), g, 1, 1 << 20)
    task = asyncio.create_task(reader.take(10_000))
    await asyncio.to_thread(started.wait, 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):  # not ValueError("generator already executing")
        await task
    assert reader._fut is not None and reader._fut.done()
    assert not reader._slices.gi_running  # type: ignore[attr-defined]
    reader.close()  # would raise ValueError if the thread were still inside


async def test_stop_wait_expiry_skips_close(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(splitter, "STOP_WAIT_S", 0.05)
    release = threading.Event()
    started = threading.Event()

    def stuck() -> Generator[bytes]:
        started.set()
        release.wait(5)  # a stalled socket read: no chunk boundary for a while
        yield b'{"a":1}\n'

    reader = _ThreadReader(stuck(), _SplitGuard(), 1, 1 << 20)
    task = asyncio.create_task(reader.take(10))
    await asyncio.to_thread(started.wait, 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    reader.close()  # the thread is still running: must not raise
    release.set()
    assert reader._fut is not None
    await asyncio.wait([reader._fut], timeout=5)
    with pytest.raises(_SplitAbortedError):
        reader._fut.result()
