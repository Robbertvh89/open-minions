import pytest
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


def test_run_rejects_custom_profile_and_help_omits_it(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    result = runner.invoke(app, ["run", "--profile", "custom"])
    assert result.exit_code == 2
    assert "custom" not in runner.invoke(app, ["run", "--help"]).output


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


def test_default_runner_is_real_engine_not_fake(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    monkeypatch.delenv("MINION_EXECUTOR_DEV", raising=False)
    result = runner.invoke(app, ["run"])  # no real engine until M3: refuses, never runs fake
    assert result.exit_code == 2
    assert "fake" not in result.output.replace("unknown runner: 'mlx'", "")


def test_token_removed_from_environment_before_start(monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    import minion_executor.main as m

    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok")
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    seen: dict[str, object] = {}

    async def fake_run(
        server: str, profile: object, runner_name: str, token: object, hb: float
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
