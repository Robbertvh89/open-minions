"""run_fleet fails fast, and the sims follow the executor's back-off call sites (TASK-7.2 fixes).

A stub HTTP server on loopback answers every request with a fixed status, so no database or
object store is needed.
"""

import asyncio
import itertools
import multiprocessing as mp
import time
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from minion_common.client import ApiError
from minion_scale import sim
from minion_scale.metrics import Recorder
from minion_scale.sim import SimConfig, SimExecutor, run_fleet
from pydantic import SecretStr

from conftest import serve_status

FAST = (0.02, 0.05)


@pytest.fixture
def rejecting() -> Iterator[str]:
    yield from serve_status(401)


@pytest.fixture
def unavailable() -> Iterator[str]:
    yield from serve_status(503)


def _cfg(url: str) -> SimConfig:
    return SimConfig(base_url=url, backoff=FAST, acquire_pace_s=0.0, work_s=0.01)


def _tokens(n: int) -> list[SecretStr]:
    return [SecretStr(f"t{i}") for i in range(n)]


async def _never() -> bool:
    return False


async def test_all_sims_aborted_ends_the_run_instead_of_hanging(rejecting: str) -> None:
    rec, secs, drained = await asyncio.wait_for(
        run_fleet(_cfg(rejecting), _tokens(3), procs=1, until=_never, poll_s=0.05), 20
    )
    assert drained is False and rec.events["sim_aborted"] == 3 and secs < 20


async def test_all_sims_aborted_across_processes(rejecting: str) -> None:
    rec, _secs, drained = await asyncio.wait_for(
        run_fleet(_cfg(rejecting), _tokens(4), procs=2, until=_never, poll_s=0.05), 90
    )
    assert drained is False and rec.events["sim_aborted"] == 4


async def test_insecure_server_url_surfaces_in_process() -> None:
    with pytest.raises(ValueError, match="insecure"):
        await asyncio.wait_for(
            run_fleet(_cfg("http://example.com"), _tokens(2), procs=1, until=_never), 20
        )


async def test_insecure_server_url_is_a_recorded_crash_across_processes() -> None:
    rec, _secs, drained = await asyncio.wait_for(
        run_fleet(_cfg("http://example.com"), _tokens(2), procs=2, until=_never, poll_s=0.05), 90
    )
    assert drained is False
    assert rec.events["proc_crashed"] == 2 and rec.events["proc_crashed_ValueError"] == 2


async def test_a_killed_sim_process_is_detected(unavailable: str) -> None:
    killed = False

    async def until() -> bool:
        nonlocal killed
        children = mp.active_children()
        if children and not killed:
            await asyncio.sleep(1.0)  # let them start talking
            for p in mp.active_children():
                p.kill()
            killed = True
        return False

    rec, secs, drained = await asyncio.wait_for(
        run_fleet(_cfg(unavailable), _tokens(2), procs=2, until=until, poll_s=0.1), 90
    )
    assert killed and drained is False
    assert rec.events["proc_crashed"] == 2 and secs < 90


async def test_max_duration_ends_the_run_undrained(unavailable: str) -> None:
    rec, secs, drained = await run_fleet(
        _cfg(unavailable), _tokens(1), procs=1, until=_never, poll_s=0.05, max_duration_s=0.5
    )
    assert drained is False and 0.4 < secs < 10 and rec.events["sim_aborted"] == 0


# --- call sites of the executor's back-off semantics ------------------------------------------


def _sim_with(client: Any, **cfg: Any) -> SimExecutor:
    conf = SimConfig(base_url="http://127.0.0.1:1", backoff=FAST, acquire_pace_s=0.0, **cfg)
    ex = SimExecutor(0, SecretStr("t"), conf, Recorder(), asyncio.Event(), httpx.AsyncClient())
    ex._client = client
    return ex


async def test_failures_in_a_row_trigger_the_long_pause() -> None:
    ex = _sim_with(None, max_consecutive_failures=3, failure_pause_s=0.01)
    slept: list[float] = []

    async def fake_sleep(s: float) -> None:
        slept.append(s)

    ex._sleep = fake_sleep  # type: ignore[method-assign,assignment]
    for _ in range(4):
        await ex._back_off_after_failure()
    assert slept[2] == 0.01 and ex.rec.events["failure_pause"] == 1
    assert all(s <= FAST[1] for s in (slept[0], slept[1], slept[3]))


async def test_register_429s_escalate_after_the_first(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}

    async def register(_req: Any) -> None:
        calls["n"] += 1
        if calls["n"] <= 3:
            raise ApiError(429, "TOO_MANY_REQUESTS", "slow", retry_after=1.0)

    ex = _sim_with(SimpleNamespace(register=register, aclose=lambda: None))
    seen: list[bool] = []
    real = sim.rate_limit_delay

    def spy(*a: Any, repeated: bool = False, **k: Any) -> float:
        seen.append(repeated)
        return real(*a, repeated=repeated, **k) * 0.001

    monkeypatch.setattr(sim, "rate_limit_delay", spy)
    assert await ex._register() is True
    assert seen == [False, True, True]


async def test_a_value_error_on_acquire_backs_off_instead_of_ending_the_sim() -> None:
    n = {"acquire": 0}

    async def register(_req: Any) -> None:
        return None

    async def acquire(_req: Any) -> None:
        n["acquire"] += 1
        raise ValueError("bad response")

    async def aclose() -> None:
        return None

    ex = _sim_with(SimpleNamespace(register=register, acquire=acquire, aclose=aclose))
    task = asyncio.create_task(ex.run())
    await asyncio.sleep(0.4)
    assert not task.done() and n["acquire"] >= 2
    ex.stop.set()
    await asyncio.wait_for(task, 5)


async def test_a_cancel_from_the_heartbeat_is_released_as_cancelled() -> None:
    released: list[str] = []

    async def heartbeat(_id: Any, _req: Any) -> Any:
        return SimpleNamespace(cancel=True)

    async def release(_id: Any, req: Any) -> None:
        released.append(req.code)

    ex = _sim_with(
        SimpleNamespace(heartbeat=heartbeat, release=release),
        heartbeat_interval_s=0.05,
        work_s=5.0,
    )
    a = SimpleNamespace(acquirement_id="x", record_count=1, output_url="http://127.0.0.1:1/o")
    assert await asyncio.wait_for(ex._handle(a), 5) is sim.Outcome.NEUTRAL  # type: ignore[arg-type]
    assert released == ["CANCELLED"] and ex.rec.events["cancelled"] == 1
    assert ex.rec.events["released"] == 1


async def test_a_stalled_queue_ends_the_run_with_a_stalled_event(unavailable: str) -> None:
    async def progress() -> int:
        return 7  # never changes

    rec, secs, drained = await asyncio.wait_for(
        run_fleet(
            _cfg(unavailable),
            _tokens(1),
            procs=1,
            until=_never,
            poll_s=0.05,
            progress=progress,
            stall_s=0.3,
        ),
        20,
    )
    assert drained is False and rec.events["stalled"] == 1 and secs < 20


async def test_a_moving_queue_is_not_a_stall(unavailable: str) -> None:
    n = {"v": 100}

    async def progress() -> int:
        n["v"] -= 1
        return n["v"]

    _rec, secs, _ = await run_fleet(
        _cfg(unavailable),
        _tokens(1),
        procs=1,
        until=_never,
        poll_s=0.05,
        progress=progress,
        stall_s=0.3,
        max_duration_s=1.0,
    )
    assert secs >= 0.9


def test_join_timeout_follows_the_http_timeout() -> None:
    assert sim.join_timeout_s(SimConfig(base_url="x", timeout_s=30.0)) == 70.0
    assert sim.join_timeout_s(SimConfig(base_url="x", timeout_s=5.0)) == 20.0


def test_a_crashed_sim_process_exits_non_zero_after_reporting() -> None:
    import queue as q

    out: q.Queue[dict[str, Any]] = q.Queue()
    with pytest.raises(SystemExit) as ei:
        sim._proc_main(
            _cfg("http://example.com"), ["t"], 0, SimpleNamespace(is_set=lambda: False), out
        )
    assert ei.value.code == 3
    assert out.get_nowait()["events"]["proc_crashed"] == 1


async def test_only_a_completion_resets_the_failure_streak() -> None:
    async def register(_req: Any) -> None:
        return None

    async def acquire(_req: Any) -> Any:
        return SimpleNamespace(record_count=1)

    async def aclose() -> None:
        return None

    ex = _sim_with(SimpleNamespace(register=register, acquire=acquire, aclose=aclose))
    outcomes = iter([sim.Outcome.FAILED, sim.Outcome.NEUTRAL, sim.Outcome.FAILED, sim.Outcome.DONE])
    seen: list[int] = []

    async def handle(_a: Any) -> sim.Outcome:
        seen.append(ex._fails)  # the streak before this assignment
        o = next(outcomes, None)
        if o is None:
            ex.stop.set()
            return sim.Outcome.NEUTRAL
        return o

    async def no_sleep(_s: float) -> None:
        return None

    ex._handle = handle  # type: ignore[method-assign,assignment]
    ex._sleep = no_sleep  # type: ignore[method-assign,assignment]
    await asyncio.wait_for(ex.run(), 5)
    assert seen == [0, 1, 1, 2, 0]  # NEUTRAL kept the streak, only DONE reset it


async def test_progress_is_polled_every_poll_s_and_stall_is_in_seconds(unavailable: str) -> None:
    stamps: list[float] = []

    async def progress() -> int:
        stamps.append(time.monotonic())
        return 7

    rec, secs, drained = await asyncio.wait_for(
        run_fleet(
            _cfg(unavailable),
            _tokens(1),
            procs=1,
            until=_never,
            poll_s=0.25,
            progress=progress,
            stall_s=0.6,
        ),
        20,
    )
    assert drained is False and rec.events["stalled"] == 1
    gaps = [b - a for a, b in itertools.pairwise(stamps)]
    assert gaps and all(0.2 <= g < 0.6 for g in gaps)
    assert 0.6 <= secs < 1.5  # stall_s seconds, not stall_s rounded up to whole polls
