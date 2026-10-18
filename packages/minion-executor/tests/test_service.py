"""launchd packaging: plist rendering, install/uninstall/status with a faked launchctl."""

from __future__ import annotations

import os
import plistlib
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from minion_common.client import ApiError
from minion_common.enums import Engine
from minion_executor import launchd, service
from minion_executor.loop import GovernorStoppedError
from minion_executor.main import NO_ENGINE_MSG, StartupError, app
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
        "MINION_EXECUTOR_SERVICE_LABEL": service.DEFAULT_LABEL,
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
    monkeypatch.setenv("MINION_EXECUTOR_SERVICE_LABEL", "com.x")
    monkeypatch.setenv("XPC_SERVICE_NAME", "com.x")
    res = runner.invoke(app, ["run"])
    assert res.exit_code == 0  # service mode: permanent failure, launchd must not relaunch
    assert "LAUNCHD_EXIT_TIMEOUT_S" in res.output and "config_invalid" in res.output
    monkeypatch.setenv("MINION_EXECUTOR_LAUNCHD_EXIT_TIMEOUT_S", "45")
    service.check_launchd_timeout(ExecutorSettings())  # exactly grace + 15 is fine


def test_settings_launchd_timeout_empty_is_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_LAUNCHD_EXIT_TIMEOUT_S", "")
    assert ExecutorSettings().launchd_exit_timeout_s is None


class FakeLaunchctl:
    def __init__(self, rc: int = 0, out: str = "") -> None:
        self.calls: list[tuple[list[str], float]] = []
        self.rc, self.out = rc, out
        self.queue: dict[str, list[tuple[int, str]]] = {}  # per launchctl verb, consumed in order

    def __call__(self, args: list[str], timeout: float = 30.0) -> tuple[int, str]:
        self.calls.append((args, timeout))
        q = self.queue.get(args[0])
        if q:
            return q.pop(0)
        return self.rc, self.out


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeLaunchctl:
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("MINION_EXECUTOR_STATE_DIR", str(tmp_path / "state"))
    for k in SECRETS:
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(launchd, "executable", lambda: "/opt/bin/minion-executor")
    monkeypatch.setattr("minion_executor.secrets.keychain_token", lambda: SecretStr("kc"))
    monkeypatch.setattr("minion_executor.secrets.keychain_present", lambda: True)
    monkeypatch.setattr(launchd, "is_darwin", lambda: True)
    monkeypatch.setattr(service, "probe_engines", lambda requested, settings: [Engine.MLX])
    monkeypatch.setattr(launchd, "_sleep", lambda s: None)
    monkeypatch.setattr(launchd, "check_install_chain", lambda exe: None)
    monkeypatch.setattr("minion_executor.worker.process.restrict_environ", lambda: None)
    fake = FakeLaunchctl()
    monkeypatch.setattr(launchd, "run_launchctl", fake)
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
    monkeypatch.setattr("minion_executor.secrets.keychain_present", lambda: False)
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
        launchd.write_plist_safely(link, b"data")
    assert target.read_text() == "x"


def test_uninstall_boots_out_and_removes(env: FakeLaunchctl, tmp_path: Path) -> None:
    runner.invoke(app, install_args(tmp_path, "--no-load"))
    env.queue["print"] = [(113, "")]
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


FIXTURE = Path(__file__).parent / "fixtures" / "launchctl_print_running.txt"


def test_parse_print_real_sample_top_level_only() -> None:
    info = service.parse_print(FIXTURE.read_text())
    assert info == {
        "state": "running",
        "pid": "2098",
        "path": "/System/Library/LaunchAgents/com.apple.progressd.plist",
        "last_exit": "(never exited)",
        "exit_timeout": "5",
    }


def test_parse_print_ignores_nested_state_and_reads_exit_reason() -> None:
    text = (
        "gui/501/x = {\n"
        "\tendpoints = {\n\t\t\tstate = active\n\t}\n"
        "\tstate = not running\n"
        "\tlast exit code = 9\n"
        "\tlast exit reason = JETSAM_REASON_MEMORY_PERPROCESSLIMIT\n"
        "\tlast terminating signal = Killed: 9\n"
        "}"
    )
    info = service.parse_print(text)
    assert info["state"] == "not running"
    assert info["last_exit_reason"] == "JETSAM_REASON_MEMORY_PERPROCESSLIMIT"
    assert info["last_signal"] == "Killed: 9"


def install_plist(tmp_path: Path, grace: float = 30.0) -> Path:
    ag = tmp_path / "ag"
    ag.mkdir(exist_ok=True)
    plist = ag / "com.test.x.plist"
    plist.write_bytes(render(grace, label="com.test.x", executable="/opt/bin/minion-executor"))
    return plist


def test_status_reports_from_installed_plist_not_shell(
    env: FakeLaunchctl, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plist = install_plist(tmp_path, grace=30.0)  # ExitTimeOut 45 in the plist
    monkeypatch.setenv("MINION_EXECUTOR_STOP_GRACE_S", "300")  # the shell differs: ignored
    text = FIXTURE.read_text().replace(
        "/System/Library/LaunchAgents/com.apple.progressd.plist", str(plist)
    )
    env.out = text + "\n\tlast exit reason = JETSAM_REASON_X\n"
    res = runner.invoke(app, ["service", "status", "--label", "com.test.x"])
    assert "state=running pid=2098 last_exit=(never exited)" in res.output
    assert "last_exit_reason=JETSAM_REASON_X" in res.output
    assert "exit_timeout=45s grace=30s" in res.output
    assert "warning" not in res.output


def test_status_warns_when_installed_exit_timeout_is_short(
    env: FakeLaunchctl, tmp_path: Path
) -> None:
    plist = install_plist(tmp_path, grace=30.0)
    doc = plistlib.loads(plist.read_bytes())
    doc["ExitTimeOut"] = 20  # hand-edited
    plist.write_bytes(plistlib.dumps(doc))
    env.out = f"x = {{\n\tpath = {plist}\n\tstate = running\n}}"
    res = runner.invoke(app, ["service", "status", "--label", "com.test.x"])
    assert "warning: exit timeout 20s < grace + 15 = 45s" in res.output


def test_status_not_loaded_only_for_113(env: FakeLaunchctl) -> None:
    env.rc = 113
    res = runner.invoke(app, ["service", "status", "--label", "com.test.x"])
    assert "not loaded" in res.output
    env.rc, env.out = 1, "boom"
    res = runner.invoke(app, ["service", "status", "--label", "com.test.x"])
    assert "not loaded" not in res.output
    assert "cannot query launchd (1): boom" in res.output


def test_bad_label_rejected(env: FakeLaunchctl, tmp_path: Path) -> None:
    res = runner.invoke(app, ["service", "install", "--label", "../evil"])
    assert res.exit_code == 2


def test_launchctl_helper_uses_absolute_path_no_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    def fake(argv: list[str], **kw: Any) -> subprocess.CompletedProcess[str]:
        seen.update(argv=argv, kw=kw)
        return subprocess.CompletedProcess(argv, 0, "o", "")

    monkeypatch.setattr(subprocess, "run", fake)
    assert launchd.run_launchctl(["print", "x"]) == (0, "o")
    assert seen["argv"] == ["/bin/launchctl", "print", "x"]
    assert seen["kw"]["timeout"] > 0 and not seen["kw"].get("shell")


# --- permanent failures exit 0 under launchd -----------------------------------------------


def _service_run_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, service_mode: bool) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok-secret")
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    # no test may reach the real Keychain (/usr/bin/security)
    monkeypatch.setattr("minion_executor.secrets.keychain_lookup", lambda: (None, False))
    if service_mode:
        monkeypatch.setenv("MINION_EXECUTOR_LAUNCHD_EXIT_TIMEOUT_S", "45")
        monkeypatch.setenv("MINION_EXECUTOR_SERVICE_LABEL", "com.x")
        monkeypatch.setenv("XPC_SERVICE_NAME", "com.x")
    else:
        monkeypatch.delenv("MINION_EXECUTOR_LAUNCHD_EXIT_TIMEOUT_S", raising=False)


def _patch_run(monkeypatch: pytest.MonkeyPatch, exc: BaseException) -> None:
    import minion_executor.main as m

    async def boom(*args: object) -> None:
        raise exc

    monkeypatch.setattr(m, "_run", boom)


@pytest.mark.parametrize("in_service", [True, False])
@pytest.mark.parametrize(
    ("exc", "reason", "plain_code"),
    [
        (ApiError(401, "UNAUTHORIZED", "bad token"), "credential_rejected", 1),
        (ApiError(403, "FORBIDDEN", "no"), "credential_rejected", 1),
        (ApiError(422, "INVALID", "bad"), "request_rejected", 1),
        (StartupError("boom"), "startup_failed", 2),
        (StartupError(NO_ENGINE_MSG), "startup_failed", 2),
    ],
)
def test_permanent_failures(
    exc: Exception,
    reason: str,
    plain_code: int,
    in_service: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _service_run_env(tmp_path, monkeypatch, in_service)
    _patch_run(monkeypatch, exc)
    res = runner.invoke(app, ["run", "--runner", "fake"])
    assert res.exit_code == (0 if in_service else plain_code), res.output
    assert "Traceback" not in res.output and "tok-secret" not in res.output
    assert (f"reason={reason}:" in res.output and "permanent_failure" in res.output) is in_service


def test_config_error_exits_0_in_service_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _service_run_env(tmp_path, monkeypatch, True)
    monkeypatch.setenv("MINION_EXECUTOR_STOP_GRACE_S", "100")  # 45 < 100 + 15
    res = runner.invoke(app, ["run", "--runner", "fake"])
    assert res.exit_code == 0
    assert "reason=config_invalid" in res.output


def test_config_error_exits_2_outside_service(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _service_run_env(tmp_path, monkeypatch, False)
    monkeypatch.setenv("MINION_EXECUTOR_STOP_GRACE_S", "9999")
    assert runner.invoke(app, ["run", "--runner", "fake"]).exit_code == 2


@pytest.mark.parametrize(
    "exc", [ApiError(429, "RATE", "slow"), ApiError(503, "DOWN", "x"), RuntimeError("crash")]
)
def test_transient_and_crashes_stay_nonzero_in_service_mode(
    exc: Exception, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _service_run_env(tmp_path, monkeypatch, True)
    _patch_run(monkeypatch, exc)
    res = runner.invoke(app, ["run", "--runner", "fake"])
    assert res.exit_code != 0


def test_governor_stopped_stays_1_in_service_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _service_run_env(tmp_path, monkeypatch, True)
    _patch_run(monkeypatch, GovernorStoppedError("no governor"))
    assert runner.invoke(app, ["run", "--runner", "fake"]).exit_code == 1


# --- install / uninstall hardening ---------------------------------------------------------


def test_install_fails_without_engine(
    env: FakeLaunchctl, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[list[Engine]] = []

    def prober(requested: list[Engine], settings: ExecutorSettings) -> list[Engine]:
        seen.append(requested)
        return []

    monkeypatch.setattr(service, "probe_engines", prober)
    res = runner.invoke(app, install_args(tmp_path, "--engines", "mlx"))
    assert res.exit_code == 2 and "no inference engine" in res.output
    assert seen == [[Engine.MLX]]
    assert env.calls == [] and not (tmp_path / "ag" / "com.test.x.plist").exists()


def test_install_fails_when_probe_raises(
    env: FakeLaunchctl, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def prober(requested: list[Engine], settings: ExecutorSettings) -> list[Engine]:
        raise RuntimeError("SANDBOX_FAILED")

    monkeypatch.setattr(service, "probe_engines", prober)
    res = runner.invoke(app, install_args(tmp_path))
    assert res.exit_code == 2 and "SANDBOX_FAILED" in res.output
    assert env.calls == []


def test_dry_run_does_not_probe(
    env: FakeLaunchctl, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def prober(requested: list[Engine], settings: ExecutorSettings) -> list[Engine]:
        raise AssertionError("probe must not run on --dry-run")

    monkeypatch.setattr(service, "probe_engines", prober)
    assert runner.invoke(app, install_args(tmp_path, "--dry-run")).exit_code == 0


def test_install_refuses_non_darwin(
    env: FakeLaunchctl, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(launchd, "is_darwin", lambda: False)
    res = runner.invoke(app, install_args(tmp_path))
    assert res.exit_code == 2 and "macOS" in res.output
    assert not (tmp_path / "state").exists()


@pytest.mark.parametrize(
    "url",
    [
        "https://user:pw@example.com",
        "https://user@example.com",
        "https://example.com/?token=x",
        "https://example.com/#frag",
    ],
)
def test_install_and_run_reject_url_extras(
    url: str, env: FakeLaunchctl, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    res = runner.invoke(app, install_args(tmp_path, "--server", url))
    assert res.exit_code == 2 and "pw" not in res.output
    assert env.calls == []
    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "t")
    assert runner.invoke(app, ["run", "--server", url]).exit_code == 2


def test_install_log_dir_and_file_private_and_umask(env: FakeLaunchctl, tmp_path: Path) -> None:
    assert runner.invoke(app, install_args(tmp_path)).exit_code == 0
    log = tmp_path / "home" / "Library" / "Logs" / "open-minions" / "com.test.x.log"
    assert log.stat().st_mode & 0o777 == 0o600
    assert log.parent.stat().st_mode & 0o777 == 0o700
    doc = plistlib.loads((tmp_path / "ag" / "com.test.x.plist").read_bytes())
    assert doc["Umask"] == 63


def test_install_refuses_unsafe_agents_dir(env: FakeLaunchctl, tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    (tmp_path / "ag").symlink_to(real)
    res = runner.invoke(app, install_args(tmp_path))
    assert res.exit_code == 2 and "refusing" in res.output
    assert env.calls == [] and not list(real.iterdir())
    (tmp_path / "ag").unlink()
    (tmp_path / "ag").mkdir()
    (tmp_path / "ag").chmod(0o777)
    res = runner.invoke(app, install_args(tmp_path))
    assert res.exit_code == 2 and "writable" in res.output


def test_install_writes_resolved_paths_and_warns_on_uncarried(
    env: FakeLaunchctl, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_STATE_DIR", "~/st")
    monkeypatch.setenv("MINION_EXECUTOR_MIN_FREE_DISK_GB", "5")
    monkeypatch.setenv("MINION_EXECUTOR_DOWNLOAD_TIMEOUT_S", "100")
    monkeypatch.setenv("MINION_EXECUTOR_PRESENCE_INTERVAL_S", "30")
    res = runner.invoke(app, install_args(tmp_path))
    assert res.exit_code == 0, res.output
    assert "MINION_EXECUTOR_PRESENCE_INTERVAL_S is set but not carried" in res.output
    assert "MIN_FREE_DISK_GB is set but not" not in res.output
    doc = plistlib.loads((tmp_path / "ag" / "com.test.x.plist").read_bytes())
    e = doc["EnvironmentVariables"]
    assert e["MINION_EXECUTOR_STATE_DIR"] == str(tmp_path / "home" / "st")  # ~ expanded
    assert Path(e["MINION_EXECUTOR_MODEL_CACHE_DIR"]).is_absolute()
    assert e["MINION_EXECUTOR_MIN_FREE_DISK_GB"] == "5"
    assert e["MINION_EXECUTOR_DOWNLOAD_TIMEOUT_S"] == "100"
    assert "MINION_EXECUTOR_PRESENCE_INTERVAL_S" not in e


def test_uninstall_refuses_foreign_plist(env: FakeLaunchctl, tmp_path: Path) -> None:
    ag = tmp_path / "ag"
    ag.mkdir()
    plist = ag / "com.test.x.plist"
    for args in (["/bin/sh", "-c", "x"], ["/opt/minion-executor", "bench"]):
        plist.write_bytes(plistlib.dumps({"Label": "com.test.x", "ProgramArguments": args}))
        res = runner.invoke(
            app, ["service", "uninstall", "--label", "com.test.x", "--agents-dir", str(ag)]
        )
        assert res.exit_code == 2 and plist.exists()
        assert env.calls == []


def test_uninstall_waits_for_graceful_stop_on_einprogress(
    env: FakeLaunchctl, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plist = install_plist(tmp_path)
    env.queue["bootout"] = [(36, "Boot-out failed: 36")]
    env.queue["print"] = [(0, "state = running"), (0, "state = running"), (113, "")]
    ticks = {"n": 0}

    def sleep(s: float) -> None:
        ticks["n"] += 1

    monkeypatch.setattr(launchd, "_sleep", sleep)
    res = runner.invoke(
        app, ["service", "uninstall", "--label", "com.test.x", "--agents-dir", str(tmp_path / "ag")]
    )
    assert res.exit_code == 0, res.output
    assert [c[0][0] for c in env.calls] == ["bootout", "print", "print", "print"]
    assert ticks["n"] == 2 and not plist.exists()


def test_uninstall_keeps_plist_if_stop_never_finishes(
    env: FakeLaunchctl, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plist = install_plist(tmp_path)  # ExitTimeOut 45 -> wait 55 s
    env.queue["bootout"] = [(36, "")]
    env.rc, env.out = 0, "state = running"
    clock = {"t": 0.0}
    monkeypatch.setattr(launchd, "_now", lambda: clock["t"])

    def sleep(s: float) -> None:
        clock["t"] += s

    monkeypatch.setattr(launchd, "_sleep", sleep)
    res = runner.invoke(
        app, ["service", "uninstall", "--label", "com.test.x", "--agents-dir", str(tmp_path / "ag")]
    )
    assert res.exit_code == 2 and plist.exists()
    assert 55 <= clock["t"] <= 57


def test_keychain_present_never_asks_for_the_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    from minion_executor import secrets

    seen: list[list[str]] = []

    def fake(argv: list[str], **kw: Any) -> subprocess.CompletedProcess[str]:
        seen.append(argv)
        return subprocess.CompletedProcess(argv, 0, "attrs", "")

    monkeypatch.setattr(subprocess, "run", fake)
    assert secrets.keychain_present() is True
    assert "-w" not in seen[0] and "-g" not in seen[0]


# --- round 2: bootout rc 0 still stopping, missing plist, TCC, failure record ---------------


def _uninstall(tmp_path: Path) -> Any:
    return runner.invoke(
        app, ["service", "uninstall", "--label", "com.test.x", "--agents-dir", str(tmp_path / "ag")]
    )


def test_uninstall_polls_after_bootout_rc0_and_reports_stop_time(
    env: FakeLaunchctl, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plist = install_plist(tmp_path)
    env.queue["bootout"] = [(0, "")]  # launchd answers 0 while the job is still stopping
    env.queue["print"] = [(0, "state = running")] * 5 + [(113, "")]
    clock = {"t": 0.0}
    monkeypatch.setattr(launchd, "_now", lambda: clock["t"])
    monkeypatch.setattr(launchd, "_sleep", lambda s: clock.__setitem__("t", clock["t"] + s))
    res = _uninstall(tmp_path)
    assert res.exit_code == 0, res.output
    assert [c[0][0] for c in env.calls] == ["bootout"] + ["print"] * 6
    assert "stopped com.test.x in 5.0 s" in res.output
    assert not plist.exists()


def test_uninstall_rc0_but_never_unloads_keeps_plist(
    env: FakeLaunchctl, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plist = install_plist(tmp_path)
    env.queue["bootout"] = [(0, "")]
    env.rc, env.out = 0, "state = running"
    clock = {"t": 0.0}
    monkeypatch.setattr(launchd, "_now", lambda: clock["t"])
    monkeypatch.setattr(launchd, "_sleep", lambda s: clock.__setitem__("t", clock["t"] + s))
    res = _uninstall(tmp_path)
    assert res.exit_code == 2 and plist.exists()
    assert "launchctl bootout" in res.output


def test_uninstall_missing_plist_but_loaded_exits_2_with_manual_commands(
    env: FakeLaunchctl, tmp_path: Path
) -> None:
    (tmp_path / "ag").mkdir()
    env.rc = 0  # launchctl print: still loaded
    res = _uninstall(tmp_path)
    assert res.exit_code == 2
    assert "launchctl bootout gui/$(id -u)/com.test.x" in res.output and "rm " in res.output


def test_uninstall_unreadable_plist_exits_2(env: FakeLaunchctl, tmp_path: Path) -> None:
    ag = tmp_path / "ag"
    ag.mkdir()
    (ag / "com.test.x.plist").write_bytes(b"not a plist")
    env.rc = 113
    res = _uninstall(tmp_path)
    assert res.exit_code == 2 and (ag / "com.test.x.plist").exists()
    assert "unreadable or malformed" in res.output and "rm " in res.output


def test_uninstall_nothing_there_is_clean(env: FakeLaunchctl, tmp_path: Path) -> None:
    (tmp_path / "ag").mkdir()
    env.rc = 113
    res = _uninstall(tmp_path)
    assert res.exit_code == 0 and "nothing to remove" in res.output


# TCC -----------------------------------------------------------------------------------------


def _home(tmp_path: Path) -> Path:
    home = tmp_path / "h"
    for rel in service.TCC_PROTECTED:
        (home / rel).mkdir(parents=True)
    return home


@pytest.mark.parametrize("rel", service.TCC_PROTECTED)
def test_tcc_protected_paths_detected(tmp_path: Path, rel: str) -> None:
    home = _home(tmp_path)
    roots = service.tcc_roots(home)
    assert service.is_tcc_protected(home / rel / "proj" / ".venv" / "bin" / "x", roots)
    assert service.is_tcc_protected(home / rel, roots)
    assert not service.is_tcc_protected(home / "Library" / "Logs" / "x", roots)
    assert not service.is_tcc_protected(home / (rel + "2") / "x", roots)  # prefix, not parent


def test_tcc_symlink_into_protected_folder_detected(tmp_path: Path) -> None:
    home = _home(tmp_path)
    (home / "Documents" / "venv").mkdir()
    link = tmp_path / "link"
    link.symlink_to(home / "Documents" / "venv")
    assert service.is_tcc_protected(link / "bin", service.tcc_roots(home))


def test_check_not_protected_lists_offenders(tmp_path: Path) -> None:
    home = _home(tmp_path)
    items: list[tuple[str, Path | str]] = [
        ("executable", home / "Documents" / "r" / ".venv" / "bin" / "minion-executor"),
        ("state dir", home / "Library" / "state"),
        ("model cache dir", home / "Downloads" / "cache"),
    ]
    with pytest.raises(ValueError) as ei:
        service.check_not_protected(home, items)
    msg = str(ei.value)
    assert "executable:" in msg and "model cache dir:" in msg and "state dir:" not in msg
    assert "uv tool install" in msg


def test_pth_target_in_protected_folder_found(tmp_path: Path) -> None:
    home = _home(tmp_path)
    site = tmp_path / "venv" / "site-packages"
    site.mkdir(parents=True)
    (site / "minion_common.pth").write_text(
        f"# editable\nimport os\n{home / 'Documents' / 'r' / 'packages' / 'src'}\n"
    )
    settings = ExecutorSettings(state_dir=tmp_path / "s", model_cache_dir=tmp_path / "c")
    items = service.install_locations("/opt/bin/minion-executor", settings, [str(site)])
    with pytest.raises(ValueError, match=r"\.pth target"):
        service.check_not_protected(home, items)


def test_install_refuses_executable_under_documents(
    env: FakeLaunchctl, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "home"
    exe = home / "Documents" / "repo" / ".venv" / "bin" / "minion-executor"
    monkeypatch.setattr(launchd, "executable", lambda: str(exe))
    for extra in ((), ("--dry-run",)):
        res = runner.invoke(app, install_args(tmp_path, *extra))
        assert res.exit_code == 2 and "macOS-protected folder" in res.output
    assert env.calls == [] and not (tmp_path / "ag" / "com.test.x.plist").exists()


def test_install_refuses_state_dir_under_desktop(
    env: FakeLaunchctl, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_STATE_DIR", str(tmp_path / "home" / "Desktop" / "s"))
    res = runner.invoke(app, install_args(tmp_path))
    assert res.exit_code == 2 and "state dir" in res.output


def test_install_real_locations_pass_with_tmp_home(env: FakeLaunchctl, tmp_path: Path) -> None:
    # sys.path / sys.prefix of this test run are not under the fake HOME
    assert runner.invoke(app, install_args(tmp_path, "--no-load")).exit_code == 0


# call order, lock, log dir ---------------------------------------------------------------------


def test_restrict_environ_runs_before_probe(
    env: FakeLaunchctl, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    order: list[str] = []
    monkeypatch.setattr(
        "minion_executor.worker.process.restrict_environ", lambda: order.append("r")
    )

    def prober(requested: list[Engine], settings: ExecutorSettings) -> list[Engine]:
        order.append("p")
        return [Engine.MLX]

    monkeypatch.setattr(service, "probe_engines", prober)
    assert runner.invoke(app, install_args(tmp_path, "--no-load")).exit_code == 0
    assert order == ["r", "p"]


def test_existing_plist_refused_before_probe(
    env: FakeLaunchctl, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    install_plist(tmp_path)
    probed: list[int] = []

    def prober(requested: list[Engine], settings: ExecutorSettings) -> list[Engine]:
        probed.append(1)
        return [Engine.MLX]

    monkeypatch.setattr(service, "probe_engines", prober)
    res = runner.invoke(app, install_args(tmp_path))
    assert res.exit_code == 2 and "uninstall" in res.output and probed == []


def test_install_warns_when_instance_lock_held(
    env: FakeLaunchctl, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from minion_executor.capacity.instance import acquire_instance_lock
    from minion_executor.capacity.safefile import ensure_private_dir

    state = tmp_path / "state"
    ensure_private_dir(state)
    fd = acquire_instance_lock(state)
    try:
        res = runner.invoke(app, install_args(tmp_path, "--no-load"))
    finally:
        os.close(fd)
    assert res.exit_code == 0 and "already runs" in res.output


def test_prepare_log_file_tightens_existing_dir(tmp_path: Path) -> None:
    d = tmp_path / "logs"
    d.mkdir(mode=0o755)
    d.chmod(0o755)
    service.prepare_log_file(d / "x.log")
    assert d.stat().st_mode & 0o777 == 0o700
    assert (d / "x.log").stat().st_mode & 0o777 == 0o600


# service mode detection ------------------------------------------------------------------------


def test_service_mode_needs_label_match_or_ppid1(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("XPC_SERVICE_NAME", raising=False)
    monkeypatch.delenv("MINION_EXECUTOR_SERVICE_LABEL", raising=False)
    monkeypatch.setenv("MINION_EXECUTOR_LAUNCHD_EXIT_TIMEOUT_S", "45")
    monkeypatch.setattr(os, "getppid", lambda: 4242)
    assert not service.service_mode()  # an exported timeout alone is not enough
    monkeypatch.setenv("MINION_EXECUTOR_SERVICE_LABEL", "com.x")
    monkeypatch.setenv("XPC_SERVICE_NAME", "other")
    assert not service.service_mode()
    monkeypatch.setenv("XPC_SERVICE_NAME", "com.x")
    assert service.service_mode()
    monkeypatch.delenv("XPC_SERVICE_NAME")
    monkeypatch.setattr(os, "getppid", lambda: 1)
    assert service.service_mode()
    monkeypatch.delenv("MINION_EXECUTOR_LAUNCHD_EXIT_TIMEOUT_S")
    assert not service.service_mode()


# transient vs permanent --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc",
    [
        ApiError(403, "HTTP_ERROR", "Forbidden"),  # proxy/WAF: no server error code
        ApiError(404, "NOT_FOUND", "x"),
        ApiError(408, "HTTP_ERROR", "timeout"),
        StartupError("probe crashed", permanent=False),
    ],
)
def test_transient_failures_exit_1_in_service_mode(
    exc: Exception, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _service_run_env(tmp_path, monkeypatch, True)
    _patch_run(monkeypatch, exc)
    res = runner.invoke(app, ["run", "--runner", "fake"])
    assert res.exit_code == 1, res.output
    assert "permanent_failure" not in res.output and "transient_failure" in res.output
    assert not (tmp_path / "state" / service.FAILURE_FILE).exists()


def test_instance_lock_held_is_transient_in_service_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from minion_executor.capacity.instance import acquire_instance_lock
    from minion_executor.capacity.safefile import ensure_private_dir

    _service_run_env(tmp_path, monkeypatch, True)
    ensure_private_dir(tmp_path / "state")
    fd = acquire_instance_lock(tmp_path / "state")
    try:
        res = runner.invoke(app, ["run", "--runner", "fake"])
    finally:
        os.close(fd)
    assert res.exit_code == 1 and "instance_lock_held" in res.output


def test_unsafe_lock_file_stays_permanent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from minion_executor.capacity.safefile import ensure_private_dir

    _service_run_env(tmp_path, monkeypatch, True)
    ensure_private_dir(tmp_path / "state")
    (tmp_path / "state" / "executor.lock").symlink_to(tmp_path / "elsewhere")
    res = runner.invoke(app, ["run", "--runner", "fake"])
    assert (
        res.exit_code == 0 and "permanent_failure" in res.output and "instance_lock" in res.output
    )


def test_proxy_403_is_not_credential_rejected_outside_service(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _service_run_env(tmp_path, monkeypatch, False)
    _patch_run(monkeypatch, ApiError(403, "HTTP_ERROR", "Forbidden"))
    assert runner.invoke(app, ["run", "--runner", "fake"]).exit_code == 1


@pytest.mark.parametrize(("rc", "transient"), [(44, False), (36, True), (51, True), (127, True)])
def test_token_unavailable_transient_vs_missing(
    rc: int, transient: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _service_run_env(tmp_path, monkeypatch, True)
    monkeypatch.delenv("MINION_EXECUTOR_TOKEN")
    monkeypatch.setattr("minion_executor.secrets.keychain_lookup", lambda: (None, transient))
    res = runner.invoke(app, ["run", "--runner", "fake"])
    assert res.exit_code == (1 if transient else 0), res.output


def test_probe_crash_transient_but_sandbox_failed_permanent() -> None:
    from minion_executor.runners.base import RunnerError
    from minion_executor.worker.sandbox import SANDBOX_FAILED

    assert StartupError("x").permanent
    assert StartupError("x", permanent=False).permanent is False
    assert RunnerError(SANDBOX_FAILED, "m").code == SANDBOX_FAILED


# failure record + status -------------------------------------------------------------------------


def test_permanent_failure_recorded_with_utc_timestamp_and_shown_by_status(
    env: FakeLaunchctl, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _service_run_env(tmp_path, monkeypatch, True)
    _patch_run(monkeypatch, ApiError(401, "UNAUTHORIZED", "bad token"))
    res = runner.invoke(app, ["run", "--runner", "fake"])
    assert res.exit_code == 0
    assert re.search(
        r"permanent_failure ts=\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ reason=credential", res.output
    )
    f = tmp_path / "state" / service.FAILURE_FILE
    assert f.stat().st_mode & 0o777 == 0o600
    monkeypatch.setenv("MINION_EXECUTOR_STATE_DIR", str(tmp_path / "state"))
    env.rc, env.out = 113, ""
    st = runner.invoke(app, ["service", "status", "--label", "com.test.x"])
    assert "last permanent failure:" in st.output and "reason=credential_rejected" in st.output


def test_failure_file_cleared_on_next_service_start(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _service_run_env(tmp_path, monkeypatch, True)
    _patch_run(monkeypatch, ApiError(401, "UNAUTHORIZED", "bad token"))
    runner.invoke(app, ["run", "--runner", "fake"])
    f = tmp_path / "state" / service.FAILURE_FILE
    assert f.exists()
    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok-secret")
    _patch_run(monkeypatch, ApiError(503, "DOWN", "x"))
    runner.invoke(app, ["run", "--runner", "fake"])
    assert not f.exists()


def test_sanitize_and_read_failure_untrusted(tmp_path: Path) -> None:
    from minion_executor.capacity.safefile import ensure_private_dir

    assert service.sanitize("a\x1b[31mb\nc" + "x" * 500, 20) == "a?[31mb?cxxxxxxxx..."
    state = tmp_path / "s"
    ensure_private_dir(state)
    assert service.read_failure(state) is None
    (state / service.FAILURE_FILE).write_text("[1]")
    (state / service.FAILURE_FILE).chmod(0o600)
    assert service.read_failure(state) is None
    service.record_failure(state, "r\x1b", "d" * 1000, "2026-01-01T00:00:00Z")
    got = service.read_failure(state)
    assert got is not None and got["reason"] == "r?" and len(got["detail"]) <= 300


@pytest.mark.parametrize(
    "exc",
    [ApiError(401, "UNAUTHORIZED", "bad token"), StartupError(NO_ENGINE_MSG)],
)
def test_permanent_failure_after_restrict_environ_still_exits_0(
    exc: Exception, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import minion_executor.main as m
    from minion_executor.worker.process import restrict_environ

    _service_run_env(tmp_path, monkeypatch, True)
    monkeypatch.setenv("MINION_EXECUTOR_TOKEN", "tok-secret")
    saved = dict(os.environ)

    async def scrub_then_raise(*args: object) -> None:
        restrict_environ()  # the real one: deletes the service env vars
        assert not os.environ.get("MINION_EXECUTOR_LAUNCHD_EXIT_TIMEOUT_S")
        raise exc

    monkeypatch.setattr(m, "_run", scrub_then_raise)
    try:
        res = runner.invoke(app, ["run", "--runner", "fake"])
    finally:
        os.environ.clear()
        os.environ.update(saved)
    assert res.exit_code == 0, res.output
    assert "permanent_failure" in res.output
    assert (tmp_path / "state" / service.FAILURE_FILE).exists()


def test_http_error_400_is_transient_in_service_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _service_run_env(tmp_path, monkeypatch, True)
    _patch_run(monkeypatch, ApiError(400, "HTTP_ERROR", "Bad Request"))
    assert runner.invoke(app, ["run", "--runner", "fake"]).exit_code == 1


def test_tcc_case_insensitive_and_firmlink_paths(tmp_path: Path) -> None:
    home = _home(tmp_path)
    roots = service.tcc_roots(home)
    assert service.is_tcc_protected(str(home).lower() + "/documents/x", roots)
    assert service.is_tcc_protected(home / "DOWNLOADS" / "x" / "y", roots)
    assert not service.is_tcc_protected(home / "Library" / "Logs", roots)
    # a non-existing path under an alias of the same directory (same inode, other spelling)
    alias = tmp_path / "alias"
    alias.symlink_to(home)
    assert service.is_tcc_protected(alias / "Documents" / "missing" / "x", roots)
    data = Path("/System/Volumes/Data" + str(home.resolve()))
    if data.exists():  # real firmlink on macOS
        assert service.is_tcc_protected(data / "Documents" / "x", roots)
