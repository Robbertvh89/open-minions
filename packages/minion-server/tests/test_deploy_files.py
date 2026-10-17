"""Static checks of deploy/Dockerfile and .dockerignore (TASK-6.11)."""

import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
DOCKERFILE = ROOT / "deploy" / "Dockerfile"
DOCKERIGNORE = ROOT / ".dockerignore"


def _lines(path: Path) -> list[str]:
    return [ln.strip() for ln in path.read_text().splitlines() if ln.strip()]


def test_every_base_image_is_pinned_by_digest() -> None:
    froms = [ln for ln in _lines(DOCKERFILE) if ln.upper().startswith("FROM ")]
    assert len(froms) >= 2
    for ln in froms:
        assert re.search(r"@sha256:[0-9a-f]{64}", ln), ln


def test_runtime_user_is_numeric_non_root() -> None:
    users = [ln.split(None, 1)[1] for ln in _lines(DOCKERFILE) if ln.upper().startswith("USER ")]
    assert users
    uid = users[-1].split(":")[0]
    assert uid.isdigit()
    assert int(uid) != 0


def test_install_is_locked_and_without_dev_or_executor() -> None:
    text = DOCKERFILE.read_text()
    assert "uv sync --locked --no-dev" in text
    assert "minion-executor/src" not in text
    for copy in re.findall(r"^COPY .*$", text, re.M):
        assert ".env" not in copy
        assert "secrets" not in copy


def test_dockerignore_is_an_allow_list_without_secrets() -> None:
    lines = [ln for ln in _lines(DOCKERIGNORE) if not ln.startswith("#")]
    assert lines[0] == "*"
    for ln in lines:
        if ln.startswith("!"):
            assert ".env" not in ln
            assert "secrets" not in ln


# --- compose services ---------------------------------------------------------------------------

COMPOSE = ROOT / "deploy" / "compose.yml"
APP_SERVICES = ("migrate", "server", "watchdog")


def _services() -> dict[str, Any]:
    return dict(yaml.safe_load(COMPOSE.read_text())["services"])


def test_app_services_are_hardened_and_in_profile() -> None:
    services = _services()
    for name in APP_SERVICES:
        svc = services[name]
        assert svc["profiles"] == ["app"], name
        assert svc["read_only"] is True, name
        assert svc["cap_drop"] == ["ALL"], name
        assert "no-new-privileges:true" in svc["security_opt"], name
        assert svc["user"] == "10001:10001", name
        assert svc["tmpfs"] == ["/tmp:size=64m,mode=1777"], name  # noqa: S108
    assert not {"postgres", "s3", "s3-init", "grafana"} & {
        n for n, s in services.items() if "profiles" in s
    }


def test_app_dependencies() -> None:
    services = _services()
    assert services["migrate"]["depends_on"]["postgres"]["condition"] == "service_healthy"
    assert services["migrate"]["restart"] == "no"
    for name in ("server", "watchdog"):
        dep = services[name]["depends_on"]
        assert dep["migrate"]["condition"] == "service_completed_successfully"
    dep = services["server"]["depends_on"]
    assert dep["s3-init"]["condition"] == "service_completed_successfully"
    assert "healthcheck" in services["server"]
    assert "healthcheck" in services["watchdog"]
    assert "pg_isready -h 127.0.0.1" in services["postgres"]["healthcheck"]["test"][1]


def test_ports_are_loopback_only() -> None:
    for name, svc in _services().items():
        for port in svc.get("ports", []):
            assert str(port).startswith("127.0.0.1:"), (name, port)
    [port] = _services()["server"]["ports"]
    assert "${MINION_SERVER_HOST_PORT:-8080}" in port


def test_secrets_are_interpolated_not_literal() -> None:
    env = _services()["server"]["environment"]
    for key in ("MINION_UI_SESSION_SECRET", "MINION_METRICS_TOKEN"):
        assert str(env[key]).startswith("${"), key
    assert "${MINION_SECRETS_DIR" in _services()["server"]["volumes"][0]


def test_secrets_dir_is_gitignored() -> None:
    assert "deploy/secrets/" in (ROOT / ".gitignore").read_text().splitlines()


def test_restart_policies_and_image_reuse() -> None:
    services = _services()
    assert services["migrate"]["restart"] == "no"
    assert "build" in services["migrate"]
    for name in ("server", "watchdog"):
        assert services[name]["restart"] == "unless-stopped", name
        assert "build" not in services[name], name
        assert services[name]["pull_policy"] == "never", name
        assert services[name]["image"] == services["migrate"]["image"], name


def test_third_party_images_are_pinned_by_digest() -> None:
    for name in ("postgres", "s3", "s3-init", "grafana"):
        assert re.search(r"@sha256:[0-9a-f]{64}$", _services()[name]["image"]), name


def test_watchdog_passes_interval_to_its_healthcheck() -> None:
    env = _services()["watchdog"]["environment"]
    assert env["MINION_WATCHDOG_INTERVAL_S"] == "${MINION_WATCHDOG_INTERVAL_S:-60}"


def _health(path: Path, interval: str | None) -> int:
    test = _services()["watchdog"]["healthcheck"]["test"]
    assert test[:3] == ["CMD", "python", "-c"]
    code = test[3].replace("/tmp/watchdog.health", str(path))  # noqa: S108
    env = {k: v for k, v in os.environ.items() if k != "MINION_WATCHDOG_INTERVAL_S"}
    if interval is not None:
        env["MINION_WATCHDOG_INTERVAL_S"] = interval
    return subprocess.run(  # noqa: S603
        [sys.executable, "-c", code], env=env, capture_output=True, check=False
    ).returncode


@pytest.mark.parametrize("interval", [None, "5"])
def test_watchdog_healthcheck_expression(tmp_path: Path, interval: str | None) -> None:
    f = tmp_path / "h"
    assert _health(f, interval) != 0  # missing
    f.touch()
    assert _health(f, interval) == 0  # fresh
    limit = 3 * float(interval or "60") + 30
    os.utime(f, (time.time() - limit + 20, time.time() - limit + 20))
    assert _health(f, interval) == 0  # just inside the window
    os.utime(f, (time.time() - limit - 20, time.time() - limit - 20))
    assert _health(f, interval) != 0  # stale


def test_every_workspace_member_pyproject_is_copied_before_the_locked_sync() -> None:
    """`uv sync --locked` needs every workspace member's pyproject.toml in the build context."""
    copied = {
        ln.split()[1]
        for ln in _lines(DOCKERFILE)
        if ln.upper().startswith("COPY ") and not ln.split()[1].startswith("--")
    }
    for pyproject in sorted((ROOT / "packages").glob("*/pyproject.toml")):
        rel = pyproject.relative_to(ROOT).as_posix()
        assert rel in copied, f"deploy/Dockerfile has no COPY line for {rel}"
