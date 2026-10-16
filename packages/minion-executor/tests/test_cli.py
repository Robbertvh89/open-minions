from pathlib import Path

import pytest
from minion_common.enums import CapacityProfile, CpuPriority, Engine
from minion_executor.main import app
from typer.testing import CliRunner

runner = CliRunner()


def test_help_lists_commands() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "run" in result.output
    assert "status" in result.output


def test_run_rejects_unknown_profile() -> None:
    result = runner.invoke(app, ["run", "--profile", "turbo"])
    assert result.exit_code != 0


def test_run_rejects_insecure_server(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    result = runner.invoke(app, ["run", "--server", "http://example.com"])
    assert result.exit_code == 2
    assert "https" in result.output
    assert "tok" not in result.output


def test_run_rejects_undefined_custom_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    result = runner.invoke(app, ["run", "--profile", "custom"])
    assert result.exit_code == 2
    assert "not defined" in result.output


CUSTOM_TOML = """
[profiles.custom]
memory_gb = 6
gpu_duty_cycle = 0.4
max_batch_size = 2
cpu_priority = "low"
"""


def _cfg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, text: str, mode: int = 0o600) -> None:
    f = tmp_path / "executor.toml"
    f.write_text(text)
    f.chmod(mode)
    monkeypatch.setenv("MINION_EXECUTOR_CONFIG", str(f))


def test_run_custom_profile_from_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import minion_executor.main as m

    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    _cfg(tmp_path, monkeypatch, CUSTOM_TOML)
    seen: dict[str, object] = {}

    async def fake_run(*args: object) -> None:
        seen["profile"] = args[1]
        seen["budget"] = args[4]

    monkeypatch.setattr(m, "_run", fake_run)
    result = runner.invoke(app, ["run", "--runner", "fake", "--profile", "custom"])
    assert result.exit_code == 0, result.output
    assert seen["profile"] == "custom"
    budget = seen["budget"]
    assert (budget.memory_gb, budget.cpu_priority) == (6, CpuPriority.LOW)  # type: ignore[attr-defined]


def test_run_without_profile_is_adaptive_and_starts_on_active_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import minion_executor.main as m

    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    _cfg(tmp_path, monkeypatch, 'active_profile = "balanced"\n')
    seen: list[object] = []

    async def fake_run(*args: object) -> None:
        seen.append(args[1])

    monkeypatch.setattr(m, "_run", fake_run)
    assert runner.invoke(app, ["run", "--runner", "fake"]).exit_code == 0
    assert seen == [CapacityProfile.BALANCED]
    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")  # `run` removes it from the environment
    assert runner.invoke(app, ["run", "--runner", "fake", "--profile", "full"]).exit_code == 0
    assert seen[-1] == CapacityProfile.FULL  # explicit --profile pins


@pytest.mark.parametrize(
    "text, mode",
    [
        ("bogus_key = 1\n", 0o600),
        ("idle_after_min = -3\n", 0o600),
        (CUSTOM_TOML, 0o666),
        (CUSTOM_TOML, 0o620),
        ("not toml [", 0o600),
    ],
)
def test_run_bad_config_exits_2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, text: str, mode: int
) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    _cfg(tmp_path, monkeypatch, text, mode)
    result = runner.invoke(app, ["run", "--runner", "fake"])
    assert result.exit_code == 2
    assert "tok" not in result.output


def test_run_missing_explicit_config_exits_2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    monkeypatch.setenv("MINION_EXECUTOR_CONFIG", str(tmp_path / "nope.toml"))
    assert runner.invoke(app, ["run", "--runner", "fake"]).exit_code == 2


def test_run_rejects_unknown_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    result = runner.invoke(app, ["run", "--runner", "nope"])
    assert result.exit_code == 2
    assert "unknown runner" in result.output


def test_fake_runner_refused_without_dev_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    monkeypatch.delenv("MINION_EXECUTOR_DEV", raising=False)
    result = runner.invoke(app, ["run", "--runner", "fake"])
    assert result.exit_code == 2
    assert "MINION_EXECUTOR_DEV" in result.output


def _no_restrict(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Stub restrict_environ (it would strip the test process' env) and record call order."""
    import minion_executor.main as m

    order: list[str] = []
    monkeypatch.setattr(m, "restrict_environ", lambda: order.append("restrict"))
    return order


def test_auto_with_no_engines_exits_2_and_never_fake(monkeypatch: pytest.MonkeyPatch) -> None:
    import minion_executor.main as m

    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")  # dev mode must not make fake implicit
    _no_restrict(monkeypatch)
    asked: list[list[Engine]] = []

    def none_available(requested: list[Engine], sandbox: object = None) -> list[Engine]:
        asked.append(requested)
        return []

    monkeypatch.setattr(m, "probe_available", none_available)
    result = runner.invoke(app, ["run"])
    assert result.exit_code == 2
    assert "no inference engine available" in result.output  # NO_ENGINE_MSG
    assert asked == [[Engine.MLX, Engine.HF]]  # Ollama is opt-in, never probed by auto


def test_probe_runs_after_environ_restricted_with_short_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import minion_executor.main as m

    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    order = _no_restrict(monkeypatch)
    seen: dict[str, object] = {}

    class FakeProbe:
        def __init__(self, engines: list[Engine], **_kw: object) -> None:
            seen["requested"] = list(engines)

        def probe(self, timeout_s: float) -> dict[str, str | None]:
            order.append("probe")
            seen["timeout"] = timeout_s
            return {"mlx": None, "ollama": None, "hf": None}

    monkeypatch.setattr(m, "WorkerHandle", FakeProbe)
    result = runner.invoke(app, ["run"])
    assert result.exit_code == 2, result.output
    assert order == ["restrict", "probe"] and seen["timeout"] == m.PROBE_TIMEOUT_S <= 90


def test_probe_runner_error_exits_2_without_traceback(monkeypatch: pytest.MonkeyPatch) -> None:
    import minion_executor.main as m
    from minion_executor.runners.base import RunnerError

    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    _no_restrict(monkeypatch)

    def boom(requested: list[Engine], sandbox: object = None) -> list[Engine]:
        raise RunnerError("PROBE_FAILED", "x")

    monkeypatch.setattr(m, "probe_available", boom)
    result = runner.invoke(app, ["run"])
    assert result.exit_code == 2 and "PROBE_FAILED" in result.output


def test_auto_advertises_probed_engines(monkeypatch: pytest.MonkeyPatch) -> None:
    import minion_executor.main as m

    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    _no_restrict(monkeypatch)
    seen: dict[str, object] = {}

    class FakeProbe:
        def __init__(self, engines: list[Engine], *a: object, **k: object) -> None:
            seen.setdefault("requested", list(engines))
            seen["last"] = list(engines)

        def probe(self, timeout_s: float) -> dict[str, str | None]:
            return {"mlx": "0.1", "ollama": None, "hf": "4.0"}

        def close(self) -> None:
            pass

    class StopLoop:
        def __init__(self, *a: object, **k: object) -> None:
            seen["engines"] = a[3]

        async def run(self) -> int:
            return 0

        def stop(self) -> None:
            pass

    monkeypatch.setattr(m, "WorkerHandle", FakeProbe)
    monkeypatch.setattr(m, "ExecutorLoop", StopLoop)
    result = runner.invoke(app, ["run"])
    assert result.exit_code == 0, result.output
    assert seen["engines"] == [Engine.MLX, Engine.HF]


def test_keep_alive_is_bounded() -> None:
    from minion_executor.main import keep_alive_s

    assert keep_alive_s(600.0) == 300 and keep_alive_s(90.0) == 90
    assert keep_alive_s(0.0) == 60 and keep_alive_s(0.2) == 1


def test_explicit_engines_are_intersected_with_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    import minion_executor.main as m

    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    _no_restrict(monkeypatch)
    seen: dict[str, object] = {}

    class StopLoop:
        def __init__(self, *a: object, **k: object) -> None:
            seen["engines"] = a[3]

        async def run(self) -> int:
            return 0

        def stop(self) -> None:
            pass

    monkeypatch.setattr(
        m, "probe_available", lambda req, sandbox=None: [e for e in req if e is Engine.OLLAMA]
    )
    monkeypatch.setattr(m, "ExecutorLoop", StopLoop)
    result = runner.invoke(app, ["run", "--engines", "mlx,ollama"])
    assert result.exit_code == 0, result.output
    assert seen["engines"] == [Engine.OLLAMA]


def test_engines_fake_needs_dev_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    monkeypatch.delenv("MINION_EXECUTOR_DEV", raising=False)
    result = runner.invoke(app, ["run", "--engines", "fake"])
    assert result.exit_code == 2
    assert "MINION_EXECUTOR_DEV" in result.output


def test_unknown_engine_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    result = runner.invoke(app, ["run", "--engines", "cuda"])
    assert result.exit_code == 2
    assert "unknown engine" in result.output


def test_token_removed_from_environment_before_start(monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    import minion_executor.main as m

    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    seen: dict[str, object] = {}

    async def fake_run(
        server: str,
        profile: object,
        engines: list[Engine],
        token: object,
        hb: float,
        config: object = None,
    ) -> None:
        seen["env"] = os.environ.get("MINION_EXECUTOR_TOKEN")
        seen["token"] = token.get_secret_value()  # type: ignore[attr-defined]

    monkeypatch.setattr(m, "_run", fake_run)
    result = runner.invoke(app, ["run", "--runner", "fake"])
    assert result.exit_code == 0, result.output
    assert seen == {"env": None, "token": "tok"}
    monkeypatch.undo()


def test_run_rejects_tiny_heartbeat_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    result = runner.invoke(app, ["run", "--runner", "fake", "--heartbeat-interval", "0.5"])
    assert result.exit_code == 2
    assert "heartbeat interval" in result.output


def test_run_rejects_server_timeout_below_twice_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    monkeypatch.setenv("MINION_HEARTBEAT_TIMEOUT_S", "300")
    result = runner.invoke(app, ["run", "--runner", "fake", "--heartbeat-interval", "200"])
    assert result.exit_code == 2
    assert "MINION_HEARTBEAT_TIMEOUT_S" in result.output


@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "3601", "1e9"])
def test_run_rejects_non_finite_or_huge_heartbeat_interval(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    result = runner.invoke(app, ["run", "--runner", "fake", "--heartbeat-interval", value])
    assert result.exit_code == 2
    assert "heartbeat interval" in result.output


def test_runtime_errors_are_not_mapped_to_exit_2(monkeypatch: pytest.MonkeyPatch) -> None:
    import minion_executor.main as m
    from minion_executor.worker.process import WorkerCrashed

    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    _no_restrict(monkeypatch)
    monkeypatch.setattr(m, "probe_available", lambda requested, sandbox=None: list(requested))

    async def boom(self: object) -> None:
        raise WorkerCrashed("died mid-run")

    monkeypatch.setattr("minion_executor.loop.ExecutorLoop.run", boom)
    result = runner.invoke(app, ["run"])
    assert result.exit_code != 2
    assert isinstance(result.exception, WorkerCrashed)


def test_probe_failure_exits_2(monkeypatch: pytest.MonkeyPatch) -> None:
    import minion_executor.main as m
    from minion_executor.worker.process import WorkerCrashed

    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    _no_restrict(monkeypatch)

    def crash(requested: list[Engine], sandbox: object = None) -> list[Engine]:
        raise WorkerCrashed("probe died")

    monkeypatch.setattr(m, "probe_available", crash)
    assert runner.invoke(app, ["run"]).exit_code == 2


def test_sandbox_policy_reaches_probe_and_worker(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import minion_executor.main as m

    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    monkeypatch.setenv("MINION_EXECUTOR_MODEL_CACHE_DIR", str(tmp_path))
    _no_restrict(monkeypatch)
    seen: dict[str, object] = {}

    def probe(requested: list[Engine], sandbox: object = None) -> list[Engine]:
        seen["probe"] = sandbox
        return [Engine.OLLAMA]

    class StopLoop:
        def __init__(self, client: object, worker: object, *a: object, **k: object) -> None:
            seen["worker"] = worker

        async def run(self) -> int:
            return 0

        def stop(self) -> None:
            pass

    class Handle:
        def __init__(self, engines: list[Engine], **kw: object) -> None:
            seen["handle_sandbox"] = kw.get("sandbox")

    monkeypatch.setattr(m, "probe_available", probe)
    monkeypatch.setattr(m, "ExecutorLoop", StopLoop)
    monkeypatch.setattr(m, "WorkerHandle", Handle)
    assert runner.invoke(app, ["run", "--engines", "ollama"]).exit_code == 0
    policy = seen["probe"]
    assert policy is not None and policy.cache_root == tmp_path  # type: ignore[attr-defined]
    assert seen["handle_sandbox"] == policy


def test_sandbox_off_without_dev_exits_2(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    monkeypatch.setenv("MINION_EXECUTOR_SANDBOX", "0")
    monkeypatch.delenv("MINION_EXECUTOR_DEV", raising=False)
    result = runner.invoke(app, ["run"])
    assert result.exit_code == 2 and "SANDBOX" in result.output


def test_sandbox_off_with_dev_passes_no_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    import minion_executor.main as m

    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    monkeypatch.setenv("MINION_EXECUTOR_SANDBOX", "0")
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    _no_restrict(monkeypatch)
    seen: list[object] = []

    def probe(requested: list[Engine], sandbox: object = None) -> list[Engine]:
        seen.append(sandbox)
        return []

    monkeypatch.setattr(m, "probe_available", probe)
    assert runner.invoke(app, ["run"]).exit_code == 2  # no engine, but the probe ran
    assert seen == [None]


def test_sandbox_failure_at_probe_exits_2(monkeypatch: pytest.MonkeyPatch) -> None:
    import minion_executor.main as m
    from minion_executor.runners.base import RunnerError

    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    _no_restrict(monkeypatch)

    class Broken:
        def __init__(self, engines: list[Engine], **_kw: object) -> None:
            pass

        def probe(self, timeout_s: float) -> dict[str, str | None]:
            raise RunnerError("SANDBOX_FAILED", "sandbox_init: boom")

    monkeypatch.setattr(m, "WorkerHandle", Broken)
    result = runner.invoke(app, ["run"])
    assert result.exit_code == 2 and "SANDBOX_FAILED" in result.output
