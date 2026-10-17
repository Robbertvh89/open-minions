"""launchd packaging: plist rendering, install/uninstall/status with a faked launchctl."""

from __future__ import annotations

import os
import plistlib
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from minion_executor import service
from minion_executor.main import app
from minion_executor.settings import ExecutorSettings
from pydantic import SecretStr
from typer.testing import CliRunner

runner = CliRunner()
REPO = Path(__file__).resolve().parents[3]
EXAMPLE = REPO / "deploy" / "launchd" / "com.open-minions.executor.plist"

SECRETS = {
    "MINION_EXECUTOR_TOKEN": "tok-secret",
    "MINION_EXECUTOR_DEV": "1",
    "MINION_EXECUTOR_TEST_HOOKS": "1",
    "MINION_EXECUTOR_SANDBOX": "0",
    "MINION_EXECUTOR_FETCH_SANDBOX": "0",
    "MINION_EXECUTOR_FAKE_SIGNALS": "/x",
    "HF_TOKEN": "hf-secret",
    "HTTPS_PROXY": "http://user:pw@proxy:3128",
    "SSL_CERT_FILE": "/ca.pem",
}


def render(grace: float = 30.0, **kw: Any) -> bytes:
    args: dict[str, Any] = {
        "label": service.DEFAULT_LABEL,
        "executable": "/Users/example/.local/bin/minion-executor",
        "server": "http://localhost:8080",
        "log_path": "/Users/example/Library/Logs/open-minions/executor.log",
        "grace_s": grace,
    }
    args.update(kw)
    return service.render_plist(**args)


@pytest.mark.parametrize(("grace", "timeout"), [(0, 15), (30, 45), (300, 315), (0.5, 16)])
def test_exit_timeout(grace: float, timeout: int) -> None:
    assert service.exit_timeout_for(grace) == timeout
    doc = plistlib.loads(render(grace))
    assert doc["ExitTimeOut"] == timeout
    assert doc["EnvironmentVariables"]["MINION_EXECUTOR_LAUNCHD_EXIT_TIMEOUT_S"] == str(timeout)


def test_render_is_deterministic_and_has_expected_keys() -> None:
    assert render() == render()
    doc = plistlib.loads(render(profile="balanced", engines="mlx"))
    assert doc["ProgramArguments"] == [
        "/Users/example/.local/bin/minion-executor",
        "run",
        "--server",
        "http://localhost:8080",
        "--profile",
        "balanced",
        "--engines",
        "mlx",
    ]
    assert doc["KeepAlive"] == {"SuccessfulExit": False}
    assert doc["ThrottleInterval"] == 60
    assert doc["ProcessType"] == "Standard"
    assert doc["RunAtLoad"] is True
    assert doc["LimitLoadToSessionType"] == "Aqua"
    assert doc["StandardOutPath"] == doc["StandardErrorPath"]
    assert not {"Sockets", "WatchPaths", "StartInterval"} & doc.keys()


def test_no_secret_in_plist_even_when_set() -> None:
    allowed = {"MINION_EXECUTOR_MODEL_CACHE_GB": "20", "MINION_EXECUTOR_IDLE_UNLOAD_S": "60"}
    data = render(env={**SECRETS, **allowed})
    doc = plistlib.loads(data)
    assert doc["EnvironmentVariables"] == {
        **allowed,
        "MINION_EXECUTOR_STOP_GRACE_S": "30",
        "MINION_EXECUTOR_LAUNCHD_EXIT_TIMEOUT_S": "45",
    }
    for secret in ("tok-secret", "hf-secret", "pw@proxy", "FAKE_SIGNALS", "SANDBOX", "HF_TOKEN"):
        assert secret.encode() not in data


def test_example_plist_equals_render() -> None:
    assert EXAMPLE.read_bytes() == render()


@pytest.mark.macos
@pytest.mark.skipif(sys.platform != "darwin", reason="plutil is macOS only")
def test_plutil_lint(tmp_path: Path) -> None:
    p = tmp_path / "x.plist"
    p.write_bytes(render(profile="light", env={"MINION_EXECUTOR_STATE_DIR": str(tmp_path / "s")}))
    out = subprocess.run(  # noqa: S603
        ["/usr/bin/plutil", "-lint", str(p)], capture_output=True, text=True
    )
    assert out.returncode == 0, out.stdout + out.stderr


def test_parse_print() -> None:
    text = "x = {\n\tstate = running\n\tpid = 123\n\tlast exit code = 0\n\texit timeout = 45\n}"
    assert service.parse_print(text) == {
        "state": "running",
        "pid": "123",
        "last_exit": "0",
        "exit_timeout": "45",
    }


def test_run_exits_2_on_inconsistent_launchd_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_STOP_GRACE_S", "30")
    monkeypatch.setenv("MINION_EXECUTOR_LAUNCHD_EXIT_TIMEOUT_S", "44")
    res = runner.invoke(app, ["run"])
    assert res.exit_code == 2
    assert "LAUNCHD_EXIT_TIMEOUT_S" in res.output
    monkeypatch.setenv("MINION_EXECUTOR_LAUNCHD_EXIT_TIMEOUT_S", "45")
    service.check_launchd_timeout(ExecutorSettings())  # exactly grace + 15 is fine


def test_settings_launchd_timeout_empty_is_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_LAUNCHD_EXIT_TIMEOUT_S", "")
    assert ExecutorSettings().launchd_exit_timeout_s is None


class FakeLaunchctl:
    def __init__(self, rc: int = 0, out: str = "") -> None:
        self.calls: list[tuple[list[str], float]] = []
        self.rc, self.out = rc, out

    def __call__(self, args: list[str], timeout: float = 30.0) -> tuple[int, str]:
        self.calls.append((args, timeout))
        return self.rc, self.out


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeLaunchctl:
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("MINION_EXECUTOR_STATE_DIR", str(tmp_path / "state"))
    for k in SECRETS:
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(service, "_executable", lambda: "/opt/bin/minion-executor")
    monkeypatch.setattr("minion_executor.secrets.keychain_token", lambda: SecretStr("kc"))
    fake = FakeLaunchctl()
    monkeypatch.setattr(service, "run_launchctl", fake)
    return fake


def install_args(tmp_path: Path, *extra: str) -> list[str]:
    return [
        "service",
        "install",
        "--label",
        "com.test.x",
        "--agents-dir",
        str(tmp_path / "ag"),
        *extra,
    ]


def test_install_refuses_dev(
    env: FakeLaunchctl, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    res = runner.invoke(app, install_args(tmp_path))
    assert res.exit_code == 2
    assert "MINION_EXECUTOR_DEV" in res.output
    assert env.calls == [] and not (tmp_path / "ag").exists()


def test_install_refuses_env_only_token(
    env: FakeLaunchctl, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("minion_executor.secrets.keychain_token", lambda: None)
    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok-secret")
    res = runner.invoke(app, install_args(tmp_path))
    assert res.exit_code == 2
    assert "Keychain" in res.output
    assert env.calls == [] and not (tmp_path / "ag").exists()


def test_install_refuses_insecure_url(env: FakeLaunchctl, tmp_path: Path) -> None:
    res = runner.invoke(app, install_args(tmp_path, "--server", "http://example.com"))
    assert res.exit_code == 2
    assert env.calls == []


def test_dry_run_prints_and_touches_nothing(env: FakeLaunchctl, tmp_path: Path) -> None:
    res = runner.invoke(app, install_args(tmp_path, "--dry-run"))
    assert res.exit_code == 0, res.output
    assert "<key>ExitTimeOut</key>" in res.output
    assert env.calls == []
    assert not (tmp_path / "ag").exists() and not (tmp_path / "state").exists()
    assert not (tmp_path / "home" / "Library").exists()


def test_install_writes_0644_and_bootstraps(
    env: FakeLaunchctl, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HF_TOKEN", "hf-secret")
    res = runner.invoke(app, install_args(tmp_path, "--profile", "light"))
    assert res.exit_code == 0, res.output
    plist = tmp_path / "ag" / "com.test.x.plist"
    assert plist.stat().st_mode & 0o777 == 0o644
    doc = plistlib.loads(plist.read_bytes())
    assert doc["ProgramArguments"][0] == "/opt/bin/minion-executor"
    assert doc["StandardOutPath"].endswith("Library/Logs/open-minions/com.test.x.log")
    assert b"hf-secret" not in plist.read_bytes()
    assert env.calls == [(["bootstrap", f"gui/{os.getuid()}", str(plist)], 30.0)]


def test_install_no_load_and_existing_refused(env: FakeLaunchctl, tmp_path: Path) -> None:
    assert runner.invoke(app, install_args(tmp_path, "--no-load")).exit_code == 0
    assert env.calls == []
    again = runner.invoke(app, install_args(tmp_path, "--no-load"))
    assert again.exit_code == 2 and "uninstall" in again.output


def test_install_removes_plist_when_bootstrap_fails(env: FakeLaunchctl, tmp_path: Path) -> None:
    env.rc, env.out = 5, "Bootstrap failed"
    res = runner.invoke(app, install_args(tmp_path))
    assert res.exit_code == 2
    assert not (tmp_path / "ag" / "com.test.x.plist").exists()


def test_write_plist_refuses_symlink(tmp_path: Path) -> None:
    target = tmp_path / "t"
    target.write_text("x")
    link = tmp_path / "l.plist"
    link.symlink_to(target)
    with pytest.raises(OSError):
        service.write_plist_safely(link, b"data")
    assert target.read_text() == "x"


def test_uninstall_boots_out_and_removes(env: FakeLaunchctl, tmp_path: Path) -> None:
    runner.invoke(app, install_args(tmp_path, "--no-load"))
    plist = tmp_path / "ag" / "com.test.x.plist"
    res = runner.invoke(
        app, ["service", "uninstall", "--label", "com.test.x", "--agents-dir", str(tmp_path / "ag")]
    )
    assert res.exit_code == 0, res.output
    assert env.calls[0][0] == ["bootout", f"gui/{os.getuid()}/com.test.x"]
    assert env.calls[0][1] > 315  # longer than the largest ExitTimeOut
    assert not plist.exists()


def test_uninstall_keeps_plist_on_bootout_error(env: FakeLaunchctl, tmp_path: Path) -> None:
    runner.invoke(app, install_args(tmp_path, "--no-load"))
    env.rc = 1
    res = runner.invoke(
        app, ["service", "uninstall", "--label", "com.test.x", "--agents-dir", str(tmp_path / "ag")]
    )
    assert res.exit_code == 2
    assert (tmp_path / "ag" / "com.test.x.plist").exists()


def test_status_warns_on_short_exit_timeout(env: FakeLaunchctl) -> None:
    env.out = "state = running\npid = 7\nlast exit code = 0\nexit timeout = 20"
    res = runner.invoke(app, ["service", "status", "--label", "com.test.x"])
    assert "state=running pid=7" in res.output
    assert "warning: exit timeout 20s < grace + 15 = 45s" in res.output


def test_status_not_loaded(env: FakeLaunchctl) -> None:
    env.rc = 113
    res = runner.invoke(app, ["service", "status", "--label", "com.test.x"])
    assert "not loaded" in res.output


def test_bad_label_rejected(env: FakeLaunchctl, tmp_path: Path) -> None:
    res = runner.invoke(app, ["service", "install", "--label", "../evil"])
    assert res.exit_code == 2


def test_launchctl_helper_uses_absolute_path_no_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    def fake(argv: list[str], **kw: Any) -> subprocess.CompletedProcess[str]:
        seen.update(argv=argv, kw=kw)
        return subprocess.CompletedProcess(argv, 0, "o", "")

    monkeypatch.setattr(subprocess, "run", fake)
    assert service.run_launchctl(["print", "x"]) == (0, "o")
    assert seen["argv"] == ["/bin/launchctl", "print", "x"]
    assert seen["kw"]["timeout"] > 0 and not seen["kw"].get("shell")
