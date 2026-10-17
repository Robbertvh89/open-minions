"""Build and run the deploy image read-only (TASK-6.11). Needs the docker CLI."""

import shutil
import subprocess
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(shutil.which("docker") is None, reason="docker CLI not available"),
]


def _docker(*args: str, timeout: int = 900) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        ["docker", *args],  # noqa: S607
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=ROOT,
        check=False,
    )


@pytest.fixture(scope="module")
def image() -> Iterator[str]:
    tag = f"open-minions-app-test:{uuid.uuid4().hex[:8]}"
    built = _docker("build", "-q", "-f", "deploy/Dockerfile", "-t", tag, ".")
    assert built.returncode == 0, built.stderr
    yield tag
    _docker("rmi", "-f", tag)


def _run(image: str, *cmd: str) -> subprocess.CompletedProcess[str]:
    return _docker("run", "--rm", "--read-only", "--tmpfs", "/tmp", image, *cmd)  # noqa: S108


def test_runs_non_root_and_read_only(image: str) -> None:
    out = _run(image, "id", "-u")
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "10001"
    assert _run(image, "sh", "-c", "touch /app/x").returncode != 0


@pytest.mark.parametrize("cmd", ["alembic", "minion-server-admin", "minion-watchdog"])
def test_entrypoints_exist(image: str, cmd: str) -> None:
    out = _run(image, "sh", "-c", f"command -v {cmd}")
    assert out.returncode == 0, out.stderr


def test_imports_and_contents(image: str) -> None:
    code = (
        "import minion_common, minion_server, minion_watchdog; import importlib.util as u;"
        "assert u.find_spec('minion_executor') is None"
    )
    out = _run(image, "python", "-c", code)
    assert out.returncode == 0, out.stderr
    for path in ("/app/.env", "/app/deploy/secrets", "/app/packages"):
        assert _run(image, "test", "!", "-e", path).returncode == 0, path
    assert _run(image, "test", "-f", "/app/alembic.ini").returncode == 0
    assert _run(image, "test", "-f", "/app/deploy/models.yaml").returncode == 0
