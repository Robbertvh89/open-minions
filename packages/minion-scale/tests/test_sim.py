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

    rec, secs = await sim.run_fleet(
        conf, tokens, procs=2, until=drained, poll_s=0.2, max_duration_s=60
    )
    assert rec.events["completed"] == 6 and secs < 60
