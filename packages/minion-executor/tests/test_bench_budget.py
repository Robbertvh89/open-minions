"""`bench --budget-check` / `--cpu-check` (TASK-5.6): parsing, verdicts and the fake-engine path.
Nothing here needs a GPU or ioreg: the meter is injected (ioreg absent -> a clear error)."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from minion_common.enums import CapacityProfile, Engine, TaskType
from minion_common.schemas import ModelEntry
from minion_executor import bench_budget as bb
from minion_executor.capacity import gpu_meter
from minion_executor.capacity.config import ExecutorConfig
from minion_executor.capacity.gpu_meter import (
    GpuMeterError,
    GpuSample,
    GpuSampler,
    parse_device_utilization,
    parse_gpu_clients,
    process_gpu_share,
    read_gpu_clients,
    read_gpu_utilization,
)
from minion_executor.capacity.profiles import budget_for
from minion_executor.capacity.signals import MemPressure, SignalSnapshot, Thermal
from minion_executor.main import app
from minion_executor.model_cache import ModelCache
from minion_executor.worker.process import WorkerHandle
from typer.testing import CliRunner

RUN = "minion_executor.capacity.gpu_meter.subprocess.Popen"
IOREG = "minion_executor.capacity.gpu_meter._ioreg"
IOREG_FIXTURE = """\
+-o AGXAcceleratorG15X  <class AGXAcceleratorG15X, id 0x100000a2c, registered, matched, active>
    {
      "IOClass" = "AGXAcceleratorG15X"
      "gpu-core-count" = 18
      "PerformanceStatistics" = {"Alloc system memory"=2023915520,"Renderer Utilization %"=7,\
"Device Utilization %"=23,"Tiler Utilization %"=5,"In use system memory"=412876800}
    }
"""
CLIENTS_FIXTURE = """\
+-o AGXDeviceUserClient  <class AGXDeviceUserClient, id 0x100000fcb, !registered, active, busy 0>
    {
      "AppUsage" = ()
      "IOUserClientCreator" = "pid 451, WindowServer"
    }

+-o AGXDeviceUserClient  <class AGXDeviceUserClient, id 0x100000fcc, !registered, active, busy 0>
    {
      "AppUsage" = ({"API"="Metal","lastSubmittedTime"=126406542491583,"accumulatedGPUTime"=3000},\
{"API"="Metal","lastSubmittedTime"=85656325280458,"accumulatedGPUTime"=500},\
{"API"="Metal","lastSubmittedTime"=0,"accumulatedGPUTime"=0})
      "IOUserClientCreator" = "pid 451, WindowServer"
    }

+-o AGXDeviceUserClient  <class AGXDeviceUserClient, id 0x100001300, !registered, active, busy 0>
    {
      "AppUsage" = ({"API"="Metal","lastSubmittedTime"=1,"accumulatedGPUTime"=7000000000})
      "IOUserClientCreator" = "pid 4242, python3.12"
    }

+-o AGXDeviceUserClient  <class AGXDeviceUserClient, id 0x100001301, !registered, active, busy 0>
    {
      "AppUsage" = ({"API"="Metal","lastSubmittedTime"=2,"accumulatedGPUTime"=250000000})
      "IOUserClientCreator" = "pid 4242, python3.12"
    }

+-o AGXDeviceUserClient  <class AGXDeviceUserClient, id 0x100001302, !registered, active, busy 0>
    {
      "AppUsage" = ({"accumulatedGPUTime"=99})
      "IOUserClientCreator" = "garbled"
    }
"""


def test_parse_gpu_clients_per_pid_and_per_client() -> None:
    out = parse_gpu_clients(CLIENTS_FIXTURE)
    assert out[451] == {"0x100000fcb": 0, "0x100000fcc": 3500}  # sums the AppUsage entries
    assert out[4242] == {"0x100001300": 7_000_000_000, "0x100001301": 250_000_000}  # 2 clients
    assert set(out) == {451, 4242}  # the unparsable creator is skipped
    assert parse_gpu_clients("") == {} and parse_gpu_clients("no blocks") == {}


def test_parse_gpu_clients_survives_garbage() -> None:
    text = '+-o X\n "IOUserClientCreator" = "pid 99999999999999999999, x"\n\x00\xff junk'
    assert parse_gpu_clients(text) == {}
    big = '+-o AGX id 0xa\n"IOUserClientCreator" = "pid 5, x"\n"accumulatedGPUTime"=' + "9" * 40
    assert parse_gpu_clients(big) == {5: {"0xa": 0}}  # an absurd counter is not a number


def test_read_gpu_clients_needs_agx_clients(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(IOREG, lambda *a, **k: "")
    with pytest.raises(GpuMeterError, match="no AGXDeviceUserClient"):
        read_gpu_clients()
    monkeypatch.setattr(IOREG, lambda *a, **k: CLIENTS_FIXTURE)
    assert read_gpu_clients()[4242]["0x100001300"] == 7_000_000_000


def test_ioreg_output_is_capped_and_time_boxed() -> None:
    with pytest.raises(GpuMeterError, match="too large"):
        gpu_meter._ioreg(("/usr/bin/yes",), timeout_s=5.0, cap=4096)
    with pytest.raises(GpuMeterError, match="timed out"):
        gpu_meter._ioreg(("/bin/sleep", "30"), timeout_s=0.2)
    with pytest.raises(GpuMeterError, match="exited 1"):
        gpu_meter._ioreg(("/usr/bin/false",))
    assert gpu_meter._ioreg(("/bin/echo", "hi")).strip() == "hi"


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
    monkeypatch.setattr(IOREG, lambda *a, **k: "nothing here")
    with pytest.raises(GpuMeterError, match="no 'Device Utilization"):
        read_gpu_utilization()
    monkeypatch.setattr(IOREG, lambda *a, **k: IOREG_FIXTURE)
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
    _wait_dead(bad)
    with pytest.raises(GpuMeterError, match="gone"):
        bad.stop()  # a broken meter never passes as a quiet GPU


def _wait_dead(sampler: GpuSampler) -> None:
    deadline = time.monotonic() + 5
    while sampler._thread.is_alive() and time.monotonic() < deadline:
        time.sleep(0.01)


def test_sampler_turns_any_exception_into_a_meter_error() -> None:
    def boom() -> float:
        raise ValueError("secret payload")

    bad = GpuSampler(read=boom, interval_s=0.01)
    bad.start()
    _wait_dead(bad)
    with pytest.raises(GpuMeterError, match="ValueError") as e:
        bad.stop()
    assert "secret" not in str(e.value)


def test_window_and_statistics() -> None:
    samples = [GpuSample(t, p) for t, p in [(0, 10), (1, 20), (2, 90), (3, 30)]]
    assert gpu_meter.window_percent(samples, 1, 2) == [20, 90]
    assert gpu_meter.median_percent([10, 20, 90]) == 20
    assert gpu_meter.mean_percent([10, 20, 30]) == 20
    with pytest.raises(GpuMeterError):
        gpu_meter.mean_percent([])


def sm(at: float, ns: dict[str, int] | None, pct: float = 40.0) -> GpuSample:
    return GpuSample(at, pct, ns)


def test_process_gpu_share_is_the_delta_over_wall_time() -> None:
    # one client accumulating 0.5 s of GPU time per second
    samples = [sm(t, {"a": 10_000_000_000 + int(t * 0.5e9)}) for t in range(0, 11)]
    assert process_gpu_share(samples, 2, 8) == pytest.approx(0.5)


def test_process_gpu_share_sums_clients_and_handles_one_appearing_mid_window() -> None:
    samples = [
        sm(0, {"a": 1_000_000_000}),
        sm(1, {"a": 1_200_000_000}),
        sm(2, {"a": 1_400_000_000, "b": 600_000_000}),  # b is new: its start value is 0
        sm(3, {"a": 1_600_000_000, "b": 900_000_000}),
        sm(4, {"a": 1_800_000_000, "b": 1_200_000_000}),
    ]
    # a: +0.8 s over 4 s, b: +1.2 s (from 0): 2.0 s / 4 s
    assert process_gpu_share(samples, 0, 4) == pytest.approx(0.5)
    assert process_gpu_share(samples, 1, 4) == pytest.approx((0.6 + 1.2) / 3)


def test_process_gpu_share_a_client_closing_keeps_its_last_value() -> None:
    samples = [
        sm(0, {"a": 0, "b": 0}),
        sm(1, {"a": 500_000_000, "b": 500_000_000}),
        sm(2, {"a": 1_000_000_000}),  # b closed after accumulating 0.5 s
    ]
    assert process_gpu_share(samples, 0, 2) == pytest.approx(0.75)


def test_process_gpu_share_a_client_missing_from_one_sample_continues_from_its_last_value() -> None:
    samples = [
        sm(0, {"a": 0}),
        sm(1, {"a": 500_000_000}),
        sm(2, {}),  # a absent from this reading only
        sm(3, {"a": 1_500_000_000}),
    ]
    assert process_gpu_share(samples, 0, 3) == pytest.approx(0.5)  # not 1.5 s counted from 0


def test_process_gpu_share_needs_readings_and_ignores_counter_resets() -> None:
    with pytest.raises(GpuMeterError):
        process_gpu_share([], 0, 1)
    with pytest.raises(GpuMeterError):  # no reading at or before the window start
        process_gpu_share([sm(5, {"a": 1}), sm(6, {"a": 2})], 1, 6)
    with pytest.raises(GpuMeterError):
        process_gpu_share([sm(0, None), sm(1, None)], 0, 1)  # pid unknown the whole time
    assert process_gpu_share([sm(0, {"a": 900}), sm(1, {"a": 5})], 0, 1) == 0.0
    assert process_gpu_share([sm(0, {}), sm(1, {})], 0, 1) == 0.0  # no client: not on the GPU


def test_process_gpu_share_a_mid_window_drop_loses_one_interval_only() -> None:
    # 0.5 s/s, counter resets to 0 between t=4 and t=5, then keeps going at 0.5 s/s
    vals = [0.0, 0.5, 1.0, 1.5, 2.0, 0.0, 0.5, 1.0, 1.5, 2.0, 2.5]
    samples = [sm(t, {"a": int(v * 1e9)}) for t, v in enumerate(vals)]
    assert process_gpu_share(samples, 0, 10) == pytest.approx(0.45)  # 9 of 10 intervals count


def test_read_gpu_clients_rejects_output_without_creator_and_gpu_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    no_time = '+-o AGXDeviceUserClient id 0x1\n"IOUserClientCreator" = "pid 5, x"\n'
    no_pid = '+-o AGXDeviceUserClient id 0x1\n"accumulatedGPUTime"=5\n"IOUserClientCreator"="x"\n'
    for text in (no_time, no_pid):
        monkeypatch.setattr(IOREG, lambda *a, _t=text, **k: _t)
        with pytest.raises(GpuMeterError, match="creator pid and accumulatedGPUTime"):
            read_gpu_clients()


def test_budget_check_zero_attribution_is_an_error_not_a_pass(tmp_path: Path) -> None:
    report = check(tmp_path, fake_entry(), rate=0.0)
    for row in report.results:
        assert row.status == "error" and "no GPU time attributed" in (row.detail or "")
        assert not row.passed


def test_budget_check_unpaced_share_above_wall_time_is_an_anomaly(tmp_path: Path) -> None:
    report = check(tmp_path, fake_entry(), rate=2.0)
    light = report.results[0]
    assert light.status == "error" and "meter anomaly" in (light.detail or "")


def v(**kw: Any) -> bb.Nf8Verdict:
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


def test_verdict_without_an_unpaced_phase_has_no_accuracy_and_passes() -> None:
    r = v(duty=1.0, measured=0.9, unpaced=None)
    assert r.expected_share is None and r.accuracy_ok is None and r.passed
    assert not v(duty=1.0, measured=1.2, unpaced=None).hard_ok


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


def ramp(rate: float) -> Any:
    """A sampler factory whose worker accumulates `rate` s of GPU time per wall second, from the
    moment it is built (time-scripted); the device meter reads a constant 40 %."""
    t_start = time.monotonic()
    return lambda pid_fn: GpuSampler(
        read=lambda: 40.0,
        interval_s=0.02,
        read_clients=lambda: {"c": int((time.monotonic() - t_start) * rate * 1e9)},
    )


def check(tmp_path: Path, entry: ModelEntry, rate: float = 0.25, **kw: Any) -> bb.BudgetCheckReport:
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
            bb.BudgetCheckConfig(duration_s=2.0, baseline_s=1.0),
            environment={"chip": "test"},
            sampler_factory=ramp(rate),
            meter_probe=lambda: None,
            **kw,
        )
    )


def test_budget_check_fake_engine_end_to_end(tmp_path: Path) -> None:
    report = check(tmp_path, fake_entry(), rate=0.25)
    light, full = report.results
    assert (light.profile, full.profile) == ("light", "full")
    assert light.status == full.status == "ok"
    assert light.duty == 0.3 and full.duty == 1.0
    # the scripted worker burns 0.25 s of GPU per second in every phase
    assert light.measured_share == pytest.approx(0.25, abs=0.03)
    assert light.unpaced_share == pytest.approx(0.25, abs=0.03)
    assert light.expected_share == pytest.approx(0.3 * 0.25, abs=0.01)
    assert light.device_baseline_pct == 40.0 and light.device_paced_pct == 40.0
    assert light.hard_ok  # 0.25 <= 0.3 + 0.1
    assert light.accuracy_ok is False  # |0.25 - 0.075| > 0.1: the scripted meter ignores the pacer
    assert light.gpu_samples > 50
    assert light.worker_reported_share is not None and light.worker_reported_share <= 0.4
    assert light.footprint_peak_gb is not None and light.footprint_peak_gb > 0
    assert light.memory_ok
    # duty 1.0: no unpaced phase, no accuracy verdict, and it does not hold the row back
    assert full.unpaced_share is None and full.expected_share is None
    assert full.accuracy_ok is None and full.passed
    data = report.to_json()
    assert data["kind"] == "budget-check" and len(data["results"]) == 2
    table = bb.render_budget_table(report)
    assert "| m | light | 1 | ok" in table and "40/40/40%" in table


def test_fill_shares_windows_settle_and_device_columns() -> None:
    """Time-scripted meter: idle 0-5 s, unpaced 5-15 s (a 1.5 s burst at rate 1.0 right at the
    start, then 0.8), gap, paced 17-27 s (0 for its first 1.5 s, then 0.3)."""

    def cumulative(t: float) -> float:
        segments = [(5.0, 6.5, 1.0), (6.5, 15.0, 0.8), (18.5, 27.0, 0.3)]
        return sum(max(0.0, min(t, b) - a) * r for a, b, r in segments)

    def device(t: float) -> float:
        return 30.0 if t < 5 else (70.0 if t < 16 else 45.0)

    samples = [sm(t / 2, {"c": int(cumulative(t / 2) * 1e9)}, device(t / 2)) for t in range(0, 57)]
    ph = bb._Phases(
        idle=(0.0, 4.9),
        unpaced=bb._Phase(5.0, 15.0, 0.9, 10),
        paced=bb._Phase(17.0, 27.0, 0.31, 10),
    )
    res = bb.BudgetCheckResult("m", "fake", "embedding", "light", 1, "ok")
    bb._fill_shares(res, samples, ph, bb.BudgetCheckConfig(duration_s=10.0, baseline_s=5.0))
    assert res.unpaced_share == pytest.approx(0.8)  # the first 1.5 s (burst) are dropped
    assert res.measured_share == pytest.approx(0.3)  # and so is the paced warm-in
    assert res.device_baseline_pct == 30.0 and res.device_unpaced_pct == 70.0
    assert res.device_paced_pct == 45.0
    assert (res.unpaced_reported_share, res.worker_reported_share) == (0.9, 0.31)
    assert res.gpu_samples == 57 and res.duration_s == 10.0


def test_all_passed_ignores_unavailable_rows_but_not_errors() -> None:
    def row(status: str, passed: bool | None = None) -> bb.BudgetCheckResult:
        return bb.BudgetCheckResult("m", "fake", "embedding", "light", 1, status, passed=passed)

    rep = bb.BudgetCheckReport({}, {})
    rep.results = [row("ok", True), row("unavailable")]
    assert rep.all_passed
    rep.results = [row("ok", True), row("error")]
    assert not rep.all_passed
    rep.results = [row("unavailable")]
    assert not rep.all_passed  # nothing was judged
    rep.results = [row("ok", False)]
    assert not rep.all_passed


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
            QuietSource(),
            paused=False,
            duration_s=1.0,
            poll_s=0.5,
            config=ExecutorConfig(),
            total_gb=32.0,
            cpu_clock=lambda: next(ticks),
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
        bb.measure_governor_cpu(
            QuietSource(),
            paused=paused,
            duration_s=3.0,
            poll_s=0.5,
            config=ExecutorConfig(),
            total_gb=32.0,
        )
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
    asyncio.run(
        bb.watch_reactions(
            Scripted(),
            duration_s=2.0,
            poll_s=0.2,
            config=ExecutorConfig(),
            total_gb=32.0,
            emit=lines.append,
        )
    )
    assert lines[0].endswith("start poll_s=0.2")
    assert any(" decision paused=-" in line for line in lines)
    assert any("user-return reaction_s=" in line for line in lines)


def test_watch_reactions_rejects_out(tmp_path: Path) -> None:
    r = CliRunner().invoke(
        app, ["bench", "--watch-reactions", "--duration", "5", "--out", str(tmp_path / "x.json")]
    )
    assert r.exit_code == 2 and "--out" in r.output


def _clients_samples(per_s_ns: list[int], ids: list[str] | None = None) -> list[Any]:
    """One sample per second; client c0 accumulates `per_s_ns[i]` ns during second i."""
    out, total = [], 0
    for i in range(len(per_s_ns) + 1):
        out.append(gpu_meter.GpuSample(float(i), 0.0, {"c0": total, **dict.fromkeys(ids or [], 0)}))
        if i < len(per_s_ns):
            total += per_s_ns[i]
    return out


def test_max_window_share_finds_the_burst_window() -> None:
    # 30 s at 20 % busy with a 10 s burst at 60 % starting at t=12
    busy = [200_000_000] * 30
    for i in range(12, 22):
        busy[i] = 600_000_000
    s = _clients_samples(busy)
    assert bb.max_window_share(s, 0.0, 30.0) == pytest.approx(0.6)
    assert bb.max_window_share(s, 0.0, 30.0, window_s=30.0) == pytest.approx(10 / 30)
    assert bb.max_window_share(s, 25.0, 30.0) is None  # shorter than one window
    assert bb.max_window_share([], 0.0, 30.0) is None


def test_distinct_clients_counts_ids_over_the_run() -> None:
    s = _clients_samples([1, 1], ids=["c1", "c2"])
    s.append(gpu_meter.GpuSample(9.0, 0.0, None))
    assert bb.distinct_clients(s) == 3
    assert bb.distinct_clients([]) == 0
