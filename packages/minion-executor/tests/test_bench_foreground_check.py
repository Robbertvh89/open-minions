"""`bench --foreground-check` orchestration (TASK-6.9): fake engine, fake probe, fake launcher,
scripted GPU meter. No GPU, ioreg or osascript needed."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

import pytest
from minion_common.enums import CapacityProfile, Engine, TaskType
from minion_common.schemas import ModelEntry
from minion_executor import bench_foreground as fg
from minion_executor.capacity.gpu_meter import GpuMeterError, GpuSampler
from minion_executor.capacity.profiles import budget_for
from minion_executor.main import app
from minion_executor.model_cache import ModelCache
from minion_executor.runners.fg_probe import ProbeError, ProbeResult
from minion_executor.worker.process import WorkerHandle
from typer.testing import CliRunner


@pytest.fixture(autouse=True)
def dev(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")


class FakeProbe:
    """Scripted per-window latencies: baseline first, then loaded, alternating."""

    def __init__(self, base_ms: float = 4.0, loaded_ms: float = 4.0) -> None:
        self.script = [base_ms, loaded_ms]
        self.calls = 0

    def calibrate(self) -> int:
        return 256

    def run(self, duration_s: float, size: int) -> ProbeResult:
        v = self.script[self.calls % 2]
        self.calls += 1
        time.sleep(duration_s)
        return ProbeResult([v] * 100, 0)


def entry() -> ModelEntry:
    return ModelEntry(
        model_id="m", type=TaskType.EMBEDDING, engine=Engine.FAKE, ref="fake", memory_gb=0.1
    )


def ramp(rate: float) -> Any:
    t_start = time.monotonic()
    return lambda pid_fn: GpuSampler(
        read=lambda: 40.0,
        interval_s=0.02,
        read_clients=lambda: {"c": int((time.monotonic() - t_start) * rate * 1e9)},
    )


def launcher(n: int) -> list[float]:
    return [0.5] * n


def run(
    tmp_path: Path,
    probe: FakeProbe,
    rate: float = 0.25,
    profiles: tuple[CapacityProfile, ...] = (CapacityProfile.LIGHT, CapacityProfile.FULL),
    **kw: Any,
) -> fg.ForegroundReport:
    kw = {"launcher": launcher, "meter_probe": lambda: None} | kw
    cache = ModelCache(tmp_path / "cache", 10**9, sandbox=False)
    return asyncio.run(
        fg.run_foreground_check(
            cache,
            lambda engine, b: WorkerHandle([engine]),
            [entry()],
            {p: budget_for(p, 32.0) for p in profiles},
            fg.ForegroundCheckConfig(duration_s=2.0, launches=3, repeats=1, settle_s=0.1),
            probe=probe,
            environment={"chip": "test"},
            sampler_factory=ramp(rate),
            **kw,
        )
    )


def test_pass_and_json_has_derived_figures_only(tmp_path: Path) -> None:
    rep = run(tmp_path, FakeProbe())
    assert [r.status for r in rep.results] == ["ok", "ok"]
    assert rep.results[0].worker_share == pytest.approx(0.25, abs=0.05)
    assert not rep.failed
    assert rep.probe_size == 256
    data = rep.to_json()
    text = json.dumps(data)
    assert data["kind"] == "foreground-check"
    assert {v["profile"] for v in data["verdicts"]} == {"light", "full"}
    assert all(not v["partial"] for v in data["verdicts"])
    assert str(tmp_path) not in text and "latencies" not in text
    table = fg.render_foreground_table(rep)
    assert "| m | light | yes |" in table and "| m | full | no |" in table


def test_loaded_degradation_fails_at_light(tmp_path: Path) -> None:
    rep = run(tmp_path, FakeProbe(base_ms=4.0, loaded_ms=6.0))
    assert rep.failed
    assert fg.failures(rep.verdicts()) == [("m", CapacityProfile.LIGHT)]


def test_full_is_informational(tmp_path: Path) -> None:
    rep = run(tmp_path, FakeProbe(base_ms=4.0, loaded_ms=6.0), profiles=(CapacityProfile.FULL,))
    assert not rep.verdicts()[("m", CapacityProfile.FULL)].passed
    assert not rep.failed


def test_idle_worker_is_an_error_not_a_pass(tmp_path: Path) -> None:
    rep = run(tmp_path, FakeProbe(), rate=0.0)
    assert all(r.status == "error" and "did not run" in r.detail for r in rep.results)
    assert rep.failed  # a judged row errored
    assert not rep.verdicts()


def test_no_launcher_is_partial(tmp_path: Path) -> None:
    rep = run(tmp_path, FakeProbe(), launcher=None)
    assert all(v.partial for v in rep.verdicts().values())
    assert not rep.failed


def test_loaded_launch_failure_is_error_row(tmp_path: Path) -> None:
    calls = 0

    def flaky(n: int) -> list[float] | None:
        nonlocal calls
        calls += 1
        return [0.5] * n if calls % 2 else None

    rep = run(tmp_path, FakeProbe(), launcher=flaky, profiles=(CapacityProfile.LIGHT,))
    assert rep.results[0].status == "error" and "one window" in rep.results[0].detail
    assert rep.failed


def test_loaded_probe_failure_is_error_row_not_abort(tmp_path: Path) -> None:
    class Dying(FakeProbe):
        def run(self, duration_s: float, size: int) -> ProbeResult:
            if self.calls == 1:
                self.calls += 1
                raise ProbeError("probe timed out")
            return super().run(duration_s, size)

    rep = run(tmp_path, Dying(), profiles=(CapacityProfile.LIGHT,))
    assert rep.results[0].status == "error" and rep.failed


def test_baseline_probe_failure_aborts(tmp_path: Path) -> None:
    class Dead(FakeProbe):
        def run(self, duration_s: float, size: int) -> ProbeResult:
            raise ProbeError("probe timed out")

    with pytest.raises(ProbeError):
        run(tmp_path, Dead(), profiles=(CapacityProfile.LIGHT,))


def test_missing_meter_aborts(tmp_path: Path) -> None:
    def no_meter() -> None:
        raise GpuMeterError("needs macOS")

    with pytest.raises(GpuMeterError):
        run(tmp_path, FakeProbe(), meter_probe=no_meter)


def test_config_bounds() -> None:
    for kw in ({"duration_s": 0.1}, {"launches": -1}, {"repeats": 0}, {"max_tokens": 0}):
        with pytest.raises(ValueError):
            fg.ForegroundCheckConfig(**kw)


def models_file(tmp_path: Path) -> Path:
    models = tmp_path / "models.yaml"
    models.write_text(
        "models:\n  - {model_id: m, type: embedding, engine: fake, ref: fake, memory_gb: 0.1}\n"
    )
    return models


def test_cli_modes_are_exclusive() -> None:
    r = CliRunner().invoke(app, ["bench", "--foreground-check", "--budget-check"])
    assert r.exit_code == 2 and "separate" in r.output


def test_cli_bad_launches_exits_2(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_MODEL_CACHE_DIR", str(tmp_path / "cache"))
    r = CliRunner().invoke(
        app,
        ["bench", "--foreground-check", "--models", str(models_file(tmp_path)), "--launches", "-5"],
    )
    assert r.exit_code == 2 and "launches" in r.output
