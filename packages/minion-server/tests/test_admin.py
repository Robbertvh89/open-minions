from pathlib import Path

import pytest
import yaml
from minion_server.admin import app
from minion_server.auth import load_api_keys, lookup_user
from typer.testing import CliRunner


def test_generate_api_key_output_loads_and_authenticates(tmp_path: Path) -> None:
    args = ["generate-api-key", "--name", "alice@x.io", "--teams", "ml,ops"]
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.output
    lines = result.output.splitlines()
    key = lines[0].split(": ", 1)[1]
    snippet = "\n".join(lines[2:]) + "\n"
    assert "keys" in yaml.safe_load(snippet)
    f = tmp_path / "keys.yaml"
    f.write_text(snippet)
    user = lookup_user(load_api_keys(f), key)
    assert user is not None
    assert user.name == "alice@x.io"
    assert user.teams == ("ml", "ops")


@pytest.mark.parametrize("name", ["a\nb", "x: y", "", "a" * 101])
def test_generate_api_key_rejects_bad_name(name: str) -> None:
    result = CliRunner().invoke(app, ["generate-api-key", "--name", name, "--teams", "ml"])
    assert result.exit_code == 2


def test_printable_escapes_control_chars() -> None:
    from minion_server.admin import printable

    out = printable("ev\x1b]0;pwn\x07il\tho\nst")
    assert all(c.isprintable() for c in out)
    assert "\x1b" not in out and "\\x1b" in out and "\\t" in out and "\\n" in out
    assert printable("alice@x.io") == "alice@x.io"


@pytest.mark.parametrize("pool", ["Bad Pool", "a\nb", "", "x" * 65, "-a"])
def test_issue_executor_token_rejects_bad_pool(pool: str) -> None:
    result = CliRunner().invoke(app, ["issue-executor-token", "--owner", "o", "--pool", pool])
    assert result.exit_code == 2
    assert "invalid --pool" in result.output


def test_generate_api_key_caps_teams() -> None:
    teams = ",".join(f"t{i}" for i in range(65))
    result = CliRunner().invoke(app, ["generate-api-key", "--name", "a", "--teams", teams])
    assert result.exit_code == 2
