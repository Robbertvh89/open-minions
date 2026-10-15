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
