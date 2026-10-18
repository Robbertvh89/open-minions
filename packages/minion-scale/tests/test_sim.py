"""Simulated executors (TASK-7.2 b): REST-only, executor-equivalent back-off, fault paths."""

import ast
import asyncio
import inspect
import random
import subprocess
import sys
import types
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import minion_scale
import pytest
from minion_common.enums import Engine
from minion_executor import loop as executor_loop
from minion_executor.settings import ExecutorSettings
from minion_scale import sim
from minion_scale.metrics import Recorder
from minion_scale.sim import FaultRates, SimConfig, run_sims
from minion_watchdog.main import run_watchdog
from minion_watchdog.settings import WatchdogSettings
from pydantic import SecretStr
from sqlalchemy import text

from conftest import Live, S3Env, _serve, seed_task

PKG = Path(minion_scale.__file__).parent
FORBIDDEN = ("sqlalchemy", "asyncpg", "psycopg", "minion_server")


# --- invariants 1 and 4 ----------------------------------------------------------------------


@pytest.mark.parametrize("module", ["sim.py", "metrics.py"])
def test_sim_modules_import_no_database_code(module: str) -> None:
    tree = ast.parse((PKG / module).read_text())
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add((node.module or "").split(".")[0])
    assert imported.isdisjoint(FORBIDDEN), imported & set(FORBIDDEN)


def test_importing_sim_loads_no_database_driver() -> None:
    code = (
        "import sys, minion_scale.sim;"
        f"bad = [m for m in {FORBIDDEN!r} if m in sys.modules];"
        "sys.exit(1 if bad else 0)"
    )
    done = subprocess.run([sys.executable, "-I", "-c", code], check=False)  # noqa: S603
    assert done.returncode == 0


# --- back-off equals the executor's ----------------------------------------------------------


def test_constants_match_the_executor() -> None:
    default = inspect.signature(executor_loop.ExecutorLoop.__init__).parameters["backoff"].default
    assert default == sim.EXECUTOR_BACKOFF
    assert sim.MAX_RETRY_AFTER_S == executor_loop.MAX_RETRY_AFTER_S
    assert ExecutorSettings.model_fields["acquire_pace_s"].default == sim.ACQUIRE_PACE_S
    params = inspect.signature(executor_loop.ExecutorLoop.__init__).parameters
    assert params["max_consecutive_failures"].default == sim.MAX_CONSECUTIVE_FAILURES
    assert params["failure_pause_s"].default == sim.FAILURE_PAUSE_S
    assert params["complete_retries"].default == sim.COMPLETE_RETRIES


def _executor_stub(backoff: tuple[float, float]) -> Any:
    stub: Any = types.SimpleNamespace(_backoff=backoff)
    stub._delay = lambda n: executor_loop.ExecutorLoop._delay(stub, n)
    return stub


@pytest.mark.parametrize("n", [0, 1, 2, 3, 5, 40])
def test_idle_delay_equals_executor_delay(n: int) -> None:
    stub = _executor_stub(sim.EXECUTOR_BACKOFF)
    for seed in range(5):
        random.seed(seed)
        want = executor_loop.ExecutorLoop._delay(stub, n)
        assert sim.backoff_delay(n, sim.EXECUTOR_BACKOFF, _rng(seed)) == want


@pytest.mark.parametrize("repeated", [False, True])
@pytest.mark.parametrize("retry_after", [None, 1.0, 7.0, 10_000.0])
@pytest.mark.parametrize("n", [0, 1, 4])
def test_429_delay_equals_executor_delay(n: int, retry_after: float | None, repeated: bool) -> None:
    from minion_common.client import ApiError

    stub = _executor_stub(sim.EXECUTOR_BACKOFF)
    exc = ApiError(429, "TOO_MANY_REQUESTS", "slow down", retry_after=retry_after)
    for seed in range(5):
        random.seed(seed)
        want = executor_loop.ExecutorLoop._rate_limit_delay(stub, exc, n, repeated=repeated)
        got = sim.rate_limit_delay(
            retry_after, n, sim.EXECUTOR_BACKOFF, _rng(seed), repeated=repeated
        )
        assert got == want


def test_first_429_waits_at_least_retry_after_capped() -> None:
    rng = _rng(1)
    assert 7.0 <= sim.rate_limit_delay(7.0, 0, sim.EXECUTOR_BACKOFF, rng) <= 7.0 * 1.2
    huge = sim.rate_limit_delay(10_000.0, 0, sim.EXECUTOR_BACKOFF, rng)
    assert sim.MAX_RETRY_AFTER_S <= huge <= sim.MAX_RETRY_AFTER_S * 1.2


def test_fault_rates_validated_and_picked_by_weight() -> None:
    with pytest.raises(ValueError):
        FaultRates(vanish=0.7, fail=0.4)
    assert FaultRates().pick(_rng(0)) is sim.Fault.NONE
    assert FaultRates(late=1.0).pick(_rng(0)) is sim.Fault.LATE
    rng = _rng(3)
    picks = [FaultRates(vanish=0.5).pick(rng) for _ in range(400)]
    assert 140 < picks.count(sim.Fault.VANISH) < 260


# --- against the real app --------------------------------------------------------------------

FAST = (0.02, 0.05)


def _rng(seed: int) -> random.Random:
    return random.Random(seed)  # noqa: S311 - deterministic jitter in tests


@pytest.fixture
async def lv(fresh_url: str, s3: S3Env, tmp_path: Path) -> AsyncIterator[Live]:
    async with _serve(fresh_url, s3, tmp_path, {}) as live:
        yield live


async def _run(
    lv: Live,
    until: Callable[[Recorder], bool],
    *,
    sims: int = 1,
    faults: FaultRates = FaultRates(),  # noqa: B008 - frozen dataclass
    limit_s: float = 30.0,
    **cfg: Any,
) -> Recorder:
    tokens = [SecretStr(await lv.token(f"sim{i}")) for i in range(sims)]
    conf = SimConfig(
        base_url=lv.url,
        work_s=0.05,
        heartbeat_interval_s=0.2,
        backoff=FAST,
        acquire_pace_s=0.0,
        faults=faults,
        **cfg,
    )
    rec = Recorder()
    stop = asyncio.Event()
    runner = asyncio.create_task(run_sims(conf, tokens, stop, rec=rec))
    deadline = asyncio.get_running_loop().time() + limit_s
    while not until(rec) and asyncio.get_running_loop().time() < deadline:  # noqa: ASYNC110
        await asyncio.sleep(0.05)
    stop.set()
    await runner
    return rec


async def _outcomes(lv: Live) -> dict[str, int]:
    rows = await lv.rows("SELECT outcome, count(*) FROM minion_subtask_attempt GROUP BY 1")
    return {r[0]: r[1] for r in rows}


@pytest.mark.integration
async def test_sims_drain_a_queue_over_rest_only(lv: Live) -> None:
    await seed_task(lv.sm, "happy", 12, engine=Engine.FAKE)
    rec = await _run(lv, lambda r: r.events["completed"] >= 12, sims=3)
    assert rec.events["completed"] == 12
    assert rec.server_errors_5xx == 0 and rec.conflicts_409 == 0
    assert {"acquire_assigned", "acquire_empty", "heartbeat", "complete", "output_put"} >= set(
        rec.latency
    ) - {"register"}
    assert rec.lag.n > 0
    assert set(rec.statuses["acquire_empty"]) == {204}  # an empty acquire is a 204, not a 200
    assert await _outcomes(lv) == {"SUCCEEDED": 12}
    assert (await lv.rows("SELECT status FROM minion_task"))[0][0] == "SUCCEEDED"


@pytest.mark.integration
async def test_release_and_retryable_fail_faults(lv: Live) -> None:
    await seed_task(lv.sm, "rel", 1, engine=Engine.FAKE)
    await _run(lv, lambda r: r.events["released"] >= 1, faults=FaultRates(release=1.0))
    assert (await _outcomes(lv)).get("RELEASED", 0) >= 1
    await lv.rows("SELECT 1")
    async with lv.sm() as s, s.begin():
        await s.execute(text("DELETE FROM minion_subtask_attempt"))
        await s.execute(text("UPDATE minion_subtask SET status='PENDING'"))
        await s.execute(text("DELETE FROM minion_subtask_acquirement"))
    await _run(lv, lambda r: r.events["failed_retryable"] >= 1, faults=FaultRates(fail=1.0))
    assert (await _outcomes(lv)).get("FAILED", 0) >= 1


@pytest.mark.integration
async def test_vanish_is_reclaimed_and_a_late_complete_gets_409(lv: Live) -> None:
    await seed_task(lv.sm, "gone", 1, engine=Engine.FAKE)
    wd = WatchdogSettings(
        database_url=SecretStr("unused"), heartbeat_timeout_s=1.0, watchdog_interval_s=0.3
    )
    stop_wd = asyncio.Event()
    watchdog = asyncio.create_task(run_watchdog(lv.sm, wd, stop_wd))
    try:
        rec = await _run(
            lv,
            lambda r: r.events["late_complete_409"] >= 1,
            faults=FaultRates(late=1.0),
            late_wait_s=3.0,
        )
        assert rec.events["late_complete_409"] >= 1 and rec.conflicts_409 >= 1
        assert (await _outcomes(lv)).get("EXPIRED", 0) >= 1  # reclaimed while silent
        await lv.rows("SELECT 1")
        await _run(lv, lambda r: r.events["fault_vanish"] >= 1, faults=FaultRates(vanish=1.0))
        await _eventually_expired(lv, 2)
    finally:
        stop_wd.set()
        await watchdog


async def _eventually_expired(lv: Live, n: int) -> None:
    for _ in range(100):
        if (await _outcomes(lv)).get("EXPIRED", 0) >= n:
            return
        await asyncio.sleep(0.1)
    raise AssertionError("watchdog did not reclaim the vanished subtask")


@pytest.mark.integration
async def test_two_procs_split_the_sims(lv: Live) -> None:
    await seed_task(lv.sm, "procs", 6, engine=Engine.FAKE)
    tokens = [SecretStr(await lv.token(f"p{i}")) for i in range(2)]
    conf = SimConfig(
        base_url=lv.url, work_s=0.02, heartbeat_interval_s=0.2, backoff=FAST, acquire_pace_s=0.0
    )

    async def drained() -> bool:
        rows = await lv.rows("SELECT count(*) FROM minion_subtask WHERE status='SUCCEEDED'")
        return bool(rows[0][0] >= 6)

    rec, secs, drained_ok = await sim.run_fleet(
        conf, tokens, procs=2, until=drained, poll_s=0.2, max_duration_s=60
    )
    assert rec.events["completed"] == 6 and secs < 60 and drained_ok


# --- complete retry, Retry-After, jitter (TASK-7.4.1) ----------------------------------------


def _complete_sim(
    responses: list[Any], delays: list[float], **cfg: Any
) -> tuple[sim.SimExecutor, list[int]]:
    """A sim whose `complete` plays `responses` (an exception is raised, else returned)."""
    calls: list[int] = []
    reports: list[tuple[str, str, bool | None]] = []

    async def complete(_aid: Any, _req: Any) -> None:
        calls.append(1)
        r = responses.pop(0)
        if isinstance(r, Exception):
            raise r

    ex = sim.SimExecutor(
        0,
        SecretStr("t"),
        SimConfig(base_url="http://127.0.0.1:1", backoff=(1.0, 4.0), **cfg),
        Recorder(),
        asyncio.Event(),
        None,  # type: ignore[arg-type]
    )

    async def fail(_aid: Any, req: Any) -> None:
        reports.append(("fail", req.code, req.retryable))

    async def release(_aid: Any, req: Any) -> None:
        reports.append(("release", req.code, None))

    ex._client = types.SimpleNamespace(complete=complete, fail=fail, release=release)  # type: ignore[assignment]
    ex.reports = reports  # type: ignore[attr-defined]

    async def record(seconds: float) -> None:
        delays.append(seconds)

    ex._sleep = record  # type: ignore[method-assign]
    return ex, calls


def _assignment() -> Any:
    return types.SimpleNamespace(acquirement_id="x", record_count=1)


async def test_two_503s_then_success_is_one_completion_honouring_retry_after() -> None:
    from minion_common.client import ApiError

    err = ApiError(503, "UNAVAILABLE", "busy", retry_after=7.0)
    delays: list[float] = []
    ex, calls = _complete_sim([err, err, None], delays)
    assert await ex._complete(_assignment(), expect_stale=False) is sim.Outcome.DONE
    assert len(calls) == 3 and ex.rec.events["completed"] == 1
    assert ex.rec.events["complete_retried"] == 2 and "complete_failed" not in ex.rec.events
    assert all(7.0 <= d <= 7.0 * 1.2 for d in delays) and len(delays) == 2


async def test_transport_error_retry_that_gets_409_counts_complete_409_once() -> None:
    import httpx
    from minion_common.client import StaleAcquirementError

    delays: list[float] = []
    stale = StaleAcquirementError(409, "STALE", "gone")
    ex, calls = _complete_sim([httpx.ConnectError("x"), stale], delays)
    assert await ex._complete(_assignment(), expect_stale=False) is sim.Outcome.NEUTRAL
    assert len(calls) == 2 and ex.rec.events["complete_409"] == 1
    assert "completed" not in ex.rec.events and len(delays) == 1


async def test_complete_gives_up_on_5xx_with_a_retryable_fail() -> None:
    from minion_common.client import ApiError

    delays: list[float] = []
    ex, calls = _complete_sim([ApiError(500, "E", "x")] * 9, delays)
    assert await ex._complete(_assignment(), expect_stale=False) is sim.Outcome.FAILED
    assert len(calls) == sim.COMPLETE_RETRIES and ex.rec.events["complete_failed"] == 1
    assert ex.reports == [("fail", "COMPLETE_FAILED", True)]  # type: ignore[attr-defined]


async def test_complete_gives_up_on_transport_errors_without_reporting() -> None:
    import httpx

    ex, calls = _complete_sim([httpx.ConnectError("x")] * 9, [])
    assert await ex._complete(_assignment(), expect_stale=False) is sim.Outcome.FAILED
    assert len(calls) == sim.COMPLETE_RETRIES and ex.rec.events["complete_failed"] == 1
    assert ex.reports == []  # type: ignore[attr-defined]  # the watchdog reclaims it


async def test_a_5xx_then_transport_errors_still_counts_as_a_server_fault() -> None:
    import httpx
    from minion_common.client import ApiError

    errs: list[Any] = [ApiError(502, "E", "x"), *[httpx.ConnectError("x")] * 9]
    ex, _ = _complete_sim(errs, [])
    assert await ex._complete(_assignment(), expect_stale=False) is sim.Outcome.FAILED
    assert ex.reports == [("fail", "COMPLETE_FAILED", True)]  # type: ignore[attr-defined]


async def test_a_4xx_complete_is_not_retried_and_is_reported() -> None:
    from minion_common.client import ApiError

    ex, calls = _complete_sim([ApiError(400, "E", "x")], [])
    assert await ex._complete(_assignment(), expect_stale=False) is sim.Outcome.FAILED
    assert len(calls) == 1 and ex.rec.events["complete_failed"] == 1
    assert ex.reports == [("fail", "COMPLETE_REJECTED", True)]  # type: ignore[attr-defined]


async def test_output_too_large_is_a_non_retryable_fail() -> None:
    from minion_common.client import ApiError

    ex, _ = _complete_sim([ApiError(422, "OUTPUT_TOO_LARGE", "x")], [])
    assert await ex._complete(_assignment(), expect_stale=False) is sim.Outcome.FAILED
    assert ex.reports == [("fail", "OUTPUT_TOO_LARGE", False)]  # type: ignore[attr-defined]


async def test_a_non_stale_409_is_discarded_and_counted() -> None:
    from minion_common.client import ApiError

    ex, calls = _complete_sim([ApiError(409, "OTHER", "x")], [])
    assert await ex._complete(_assignment(), expect_stale=False) is sim.Outcome.NEUTRAL
    assert len(calls) == 1 and ex.rec.events["complete_409"] == 1
    assert ex.reports == []  # type: ignore[attr-defined]


async def test_stop_mid_retry_releases_with_stop() -> None:
    import httpx

    ex, calls = _complete_sim([httpx.ConnectError("x")] * 3, [])
    ex.stop.set()
    assert await ex._complete(_assignment(), expect_stale=False) is sim.Outcome.NEUTRAL
    assert len(calls) == 1 and "complete_retried" not in ex.rec.events
    assert ex.reports == [("release", "STOP", None)]  # type: ignore[attr-defined]


async def test_retry_after_is_honoured_only_on_503() -> None:
    from minion_common.client import ApiError

    delays: list[float] = []
    ex, _ = _complete_sim([ApiError(500, "E", "x", retry_after=200.0), None], delays)
    assert await ex._complete(_assignment(), expect_stale=False) is sim.Outcome.DONE
    assert delays[0] <= 4.0


async def test_acquire_503_with_retry_after_waits_it_out() -> None:
    from minion_common.client import ApiError

    delays: list[float] = []
    ex, _ = _complete_sim([], delays)
    exc = ApiError(503, "UNAVAILABLE", "busy", retry_after=9.0)
    assert await ex._after_acquire_error(exc) is True
    assert 9.0 <= delays[0] <= 9.0 * 1.2
    plain = ApiError(503, "UNAVAILABLE", "busy")  # no header: ordinary back-off
    assert await ex._after_acquire_error(plain) is True
    assert delays[1] <= 4.0


async def test_first_acquire_jitter_is_applied_once_before_the_first_acquire() -> None:
    delays: list[float] = []
    ex, _ = _complete_sim([], delays, first_acquire_jitter_s=3.0, acquire_pace_s=0.0)
    order: list[str] = []

    async def register() -> bool:
        order.append("register")
        return True

    async def acquire(_req: Any) -> None:
        order.append("acquire")
        if order.count("acquire") == 2:
            ex.stop.set()

    async def aclose() -> None:
        return None

    ex._register = register  # type: ignore[method-assign]
    ex._client = types.SimpleNamespace(acquire=acquire, aclose=aclose)  # type: ignore[assignment]
    await ex.run()
    assert order == ["register", "acquire", "acquire"]
    # one jitter delay before the first acquire, then one idle back-off per empty acquire
    assert len(delays) == 3 and 0 <= delays[0] <= 3.0 and all(d <= 4.0 for d in delays[1:])
