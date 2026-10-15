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
