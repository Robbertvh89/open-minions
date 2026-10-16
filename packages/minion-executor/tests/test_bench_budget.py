"""`bench --budget-check` / `--cpu-check` (TASK-5.6): parsing, verdicts and the fake-engine path.
Nothing here needs a GPU or ioreg: the meter is injected (ioreg absent -> a clear error)."""

from __future__ import annotations

import asyncio
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from minion_common.enums import CapacityProfile, Engine, TaskType
from minion_common.schemas import ModelEntry
from minion_executor import bench_budget as bb
from minion_executor.capacity import gpu_meter
from minion_executor.capacity.gpu_meter import (
    GpuMeterError,
    GpuSample,
    GpuSampler,
    parse_device_utilization,
    read_gpu_utilization,
)
from minion_executor.capacity.profiles import budget_for
from minion_executor.capacity.signals import MemPressure, SignalSnapshot, Thermal
from minion_executor.main import app
from minion_executor.model_cache import ModelCache
from minion_executor.worker.process import WorkerHandle
from typer.testing import CliRunner

RUN = "minion_executor.capacity.gpu_meter.subprocess.run"
IOREG_FIXTURE = """\
+-o AGXAcceleratorG15X  <class AGXAcceleratorG15X, id 0x100000a2c, registered, matched, active>
    {
      "IOClass" = "AGXAcceleratorG15X"
      "gpu-core-count" = 18
      "PerformanceStatistics" = {"Alloc system memory"=2023915520,"Renderer Utilization %"=7,\
"Device Utilization %"=23,"Tiler Utilization %"=5,"In use system memory"=412876800}
    }
"""


def test_parse_device_utilization() -> None:
    assert parse_device_utilization(IOREG_FIXTURE) == 23.0
    assert parse_device_utilization('"Device Utilization %"=0') == 0.0
    assert parse_device_utilization('"Device Utilization %" = 100') == 100.0


def test_parse_takes_the_busiest_accelerator_and_ignores_garbage() -> None:
    text = '"Device Utilization %"=12 ... "Device Utilization %"=64 "Device Utilization %"=900'
    assert parse_device_utilization(text) == 64.0
    assert parse_device_utilization("") is None
    assert parse_device_utilization("Renderer Utilization %=5") is None


def test_ioreg_missing_is_a_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*a: Any, **k: Any) -> Any:
        raise FileNotFoundError

    monkeypatch.setattr(RUN, boom)
    with pytest.raises(GpuMeterError, match="needs macOS"):
        read_gpu_utilization()


def test_ioreg_without_a_gpu_or_failing(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake(rc: int, out: str) -> Any:
        return lambda *a, **k: subprocess.CompletedProcess(a, rc, out, "")

    monkeypatch.setattr(RUN, fake(0, "nothing here"))
    with pytest.raises(GpuMeterError, match="no 'Device Utilization"):
        read_gpu_utilization()
    monkeypatch.setattr(RUN, fake(1, ""))
    with pytest.raises(GpuMeterError, match="exited 1"):
        read_gpu_utilization()
    monkeypatch.setattr(RUN, fake(0, IOREG_FIXTURE))
    assert read_gpu_utilization() == 23.0


def test_sampler_collects_and_surfaces_a_failed_read() -> None:
    s = GpuSampler(read=lambda: 5.0, interval_s=0.01)
    s.start()
    import time

    time.sleep(0.1)
    samples = s.stop()
    assert len(samples) >= 3 and {x.percent for x in samples} == {5.0}

    calls = iter([1.0, 2.0])

    def flaky() -> float:
        try:
            return next(calls)
        except StopIteration:
            raise GpuMeterError("gone") from None

    bad = GpuSampler(read=flaky, interval_s=0.01)
    bad.start()
    time.sleep(0.1)
    with pytest.raises(GpuMeterError, match="gone"):
        bad.stop()  # a broken meter never passes as a quiet GPU


def test_window_and_statistics() -> None:
    samples = [GpuSample(t, p) for t, p in [(0, 10), (1, 20), (2, 90), (3, 30)]]
    assert gpu_meter.window_percent(samples, 1, 2) == [20, 90]
    assert gpu_meter.median_percent([10, 20, 90]) == 20
    assert gpu_meter.mean_percent([10, 20, 30]) == 20
    with pytest.raises(GpuMeterError):
        gpu_meter.mean_percent([])


def test_share_from_percent_subtracts_baseline_and_clamps() -> None:
    assert bb.share_from_percent(45, 5) == pytest.approx(0.40)
    assert bb.share_from_percent(3, 5) == 0.0
    assert bb.share_from_percent(250, 0) == 1.0


def v(**kw: float) -> bb.Nf8Verdict:
    base = {
        "duty": 0.5,
        "measured": 0.4,
        "unpaced": 0.8,
        "footprint_peak_gb": 1.0,
        "memory_gb": 2.0,
    }
    return bb.nf8_verdict(**{**base, **kw})


def test_verdict_pass() -> None:
    r = v()
    assert r.expected_share == pytest.approx(0.4)
    assert r.hard_ok and r.accuracy_ok and r.memory_ok and r.passed


def test_verdict_hard_bound_is_duty_plus_ten_points() -> None:
    assert v(duty=0.3, measured=0.40, unpaced=1.0).hard_ok  # exactly +10 pp
    r = v(duty=0.3, measured=0.45, unpaced=1.0)
    assert not r.hard_ok and not r.passed


def test_verdict_cpu_bound_workload_passes_hard_bound_but_accuracy_uses_unpaced() -> None:
    # partly CPU-bound: only 40 % busy even unpaced. At duty 0.5 the share is ~0.2 (< duty - 10 pp)
    r = v(duty=0.5, measured=0.2, unpaced=0.4)
    assert r.hard_ok and r.accuracy_ok  # literal "+-10 pp of duty" would fail this
    r = v(duty=0.5, measured=0.2, unpaced=0.9)  # the pacer over-throttled a GPU-bound load
    assert r.hard_ok and not r.accuracy_ok


def test_verdict_memory_bound() -> None:
    assert v(footprint_peak_gb=2.2, memory_gb=2.0).memory_ok  # 1.10x
    assert not v(footprint_peak_gb=2.21, memory_gb=2.0).memory_ok


def test_verdict_rejects_nonsense() -> None:
    with pytest.raises(ValueError):
        v(measured=float("nan"))
    with pytest.raises(ValueError):
        v(unpaced=-0.1)


def test_parse_profiles_and_config_bounds() -> None:
    assert bb.parse_profiles("light, full,light") == [CapacityProfile.LIGHT, CapacityProfile.FULL]
    with pytest.raises(ValueError):
        bb.parse_profiles("light,turbo")
    with pytest.raises(ValueError):
        bb.parse_profiles(" , ")
    with pytest.raises(ValueError):
        bb.BudgetCheckConfig(duration_s=0.1)
    with pytest.raises(ValueError):
        bb.BudgetCheckConfig(repeats=0)


# -- fake-engine path --


@pytest.fixture(autouse=True)
def dev(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")


def fake_entry(memory_gb: float = 0.1) -> ModelEntry:
    return ModelEntry(
        model_id="m", type=TaskType.EMBEDDING, engine=Engine.FAKE, ref="fake", memory_gb=memory_gb
    )


def check(tmp_path: Path, entry: ModelEntry, **kw: Any) -> bb.BudgetCheckReport:
    cache = ModelCache(tmp_path / "cache", 10**9, sandbox=False)
    budgets = {
        CapacityProfile.LIGHT: budget_for(CapacityProfile.LIGHT, 32.0),
        CapacityProfile.FULL: budget_for(CapacityProfile.FULL, 32.0),
    }
    return asyncio.run(
        bb.run_budget_check(
            cache,
            lambda engine, b: WorkerHandle([engine]),
            [entry],
            budgets,
            bb.BudgetCheckConfig(duration_s=1.0, baseline_s=1.0),
            environment={"chip": "test"},
            sampler_factory=lambda: GpuSampler(read=lambda: 40.0, interval_s=0.02),
            meter_probe=lambda: 40.0,
            **kw,
        )
    )


def test_budget_check_fake_engine_end_to_end(tmp_path: Path) -> None:
    report = check(tmp_path, fake_entry())
    light, full = report.results
    assert (light.profile, full.profile) == ("light", "full")
    assert light.status == full.status == "ok"
    assert light.duty == 0.3 and full.duty == 1.0
    assert (
        light.baseline_pct == 40.0 and light.measured_share == 0.0
    )  # constant meter: all baseline
    assert light.gpu_samples > 50
    assert light.worker_reported_share is not None and light.worker_reported_share <= 0.4
    assert light.footprint_peak_gb is not None and light.footprint_peak_gb > 0
    assert light.hard_ok and light.memory_ok
    assert full.unpaced_share == full.measured_share  # duty 1.0: no separate unpaced phase
    data = report.to_json()
    assert data["kind"] == "budget-check" and len(data["results"]) == 2
    table = bb.render_budget_table(report)
    assert "| m | light | 1 | ok" in table and "pass" in table


def test_model_that_does_not_fit_is_a_row_not_a_crash(tmp_path: Path) -> None:
    report = check(tmp_path, fake_entry(memory_gb=1000.0))
    assert [r.status for r in report.results] == ["unavailable", "unavailable"]
    assert not report.all_passed


def test_missing_meter_aborts_with_a_clear_error(tmp_path: Path) -> None:
    def no_meter() -> float:
        raise GpuMeterError("ioreg not found: the GPU meter needs macOS")

    cache = ModelCache(tmp_path / "cache", 10**9, sandbox=False)
    with pytest.raises(GpuMeterError, match="needs macOS"):
        asyncio.run(
            bb.run_budget_check(
                cache,
                lambda e, b: WorkerHandle([e]),
                [fake_entry()],
                {CapacityProfile.LIGHT: budget_for(CapacityProfile.LIGHT, 32.0)},
                bb.BudgetCheckConfig(),
                meter_probe=no_meter,
            )
        )


def test_cli_budget_check_without_ioreg_exits_2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(*a: Any, **k: Any) -> Any:
        raise FileNotFoundError

    monkeypatch.setattr(RUN, boom)
    models = tmp_path / "models.yaml"
    models.write_text(
        "models:\n  - {model_id: m, type: embedding, engine: fake, ref: fake, memory_gb: 0.1}\n"
    )
    monkeypatch.setenv("MINION_EXECUTOR_MODEL_CACHE_DIR", str(tmp_path / "cache"))
    r = CliRunner().invoke(
        app, ["bench", "--budget-check", "--models", str(models), "--model", "m"]
    )
    assert r.exit_code == 2
    assert "needs macOS" in r.output


# -- NF-4 --


class QuietSource:
    def read(self) -> SignalSnapshot:
        return SignalSnapshot(
            0.0, True, None, False, Thermal.NOMINAL, MemPressure.NORMAL, datetime.now(UTC)
        )


def test_cpu_percent_and_verdict_logic() -> None:
    assert bb.cpu_percent(0.3, 60.0) == pytest.approx(0.5)
    with pytest.raises(ValueError):
        bb.cpu_percent(1.0, 0.0)

    ticks = iter([10.0, 10.9])  # 0.9 s of CPU in the window
    r = asyncio.run(
        bb.measure_governor_cpu(
            QuietSource(), paused=False, duration_s=1.0, poll_s=0.5, cpu_clock=lambda: next(ticks)
        )
    )
    assert r.cpu_pct == pytest.approx(90.0, rel=0.2) and not r.passed
    assert "FAIL" in bb.render_cpu_table([r])


@pytest.mark.parametrize("paused", [False, True])
def test_governor_loop_cpu_is_small_idle_and_paused(paused: bool) -> None:
    """NF-4 on CI, generously: the real GovernorRuntime polling every 0.5 s (4x the production
    rate) over 3 s stays well under 10 % of one core. The strict < 1 % reading (60 s window,
    production poll) is the Mac measurement in docs/bench/governor-measurements.md."""
    r = asyncio.run(
        bb.measure_governor_cpu(QuietSource(), paused=paused, duration_s=3.0, poll_s=0.5)
    )
    assert r.state == ("paused" if paused else "idle")
    assert r.cpu_pct < 10.0, r


# -- NF-9 Mac helper --


def snap(hid: float, at: datetime, **kw: Any) -> SignalSnapshot:
    return SignalSnapshot(hid, True, None, False, Thermal.NOMINAL, MemPressure.NORMAL, at, **kw)


def test_user_return_is_back_dated_from_the_hid_counter() -> None:
    t = datetime(2026, 10, 17, 12, 0, 0, tzinfo=UTC)
    from datetime import timedelta

    prev = snap(700.0, t)
    now = t + timedelta(seconds=2)
    # input happened 0.5 s before the poll at t+2: taken_at - hid_idle_s = t+1.5; seen at t+2.1
    cur = snap(0.5, t + timedelta(seconds=2))
    r = bb.user_return_reaction_s(prev, cur, now + timedelta(seconds=0.1))
    assert r == pytest.approx(0.6)
    assert bb.user_return_reaction_s(None, cur, now) is None
    assert bb.user_return_reaction_s(snap(3.0, t), snap(1.0, t), now) is None  # not a return


def test_watch_reactions_reports_decisions_and_returns() -> None:
    class Scripted:
        def __init__(self) -> None:
            self.n = 0

        def read(self) -> SignalSnapshot:
            self.n += 1
            now = datetime.now(UTC)
            if self.n < 3:
                return snap(900.0, now)
            return snap(
                0.0,
                now,
            )

    lines: list[str] = []
    asyncio.run(bb.watch_reactions(Scripted(), duration_s=2.0, poll_s=0.2, emit=lines.append))
    assert lines[0].endswith("start poll_s=0.2")
    assert any(" decision paused=-" in line for line in lines)
    assert any("user-return reaction_s=" in line for line in lines)
