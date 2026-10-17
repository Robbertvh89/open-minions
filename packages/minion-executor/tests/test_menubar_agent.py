"""Menu-bar LaunchAgent: golden plist, idempotent install/uninstall, refusals, isolation."""

from __future__ import annotations

import plistlib
from pathlib import Path

import pytest
from minion_executor import menubar_agent, service
from minion_executor.main import app
from typer.testing import CliRunner

runner = CliRunner()
EXE = "/opt/bin/minion-executor"
LOG = "/Users/example/Library/Logs/open-minions/com.open-minions.menubar.log"


class Fake:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self.loaded = False

    def __call__(self, args: list[str], timeout: float = 30.0) -> tuple[int, str]:
        self.calls.append(args)
        if args[0] == "bootstrap":
            self.loaded = True
            return 0, ""
        if args[0] == "bootout":
            self.loaded = False
            return 0, ""
        if args[0] == "print":
            return (0, "x = {\n\tstate = running\n\tpid = 7\n}") if self.loaded else (113, "")
        return 0, ""


@pytest.fixture
def fake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Fake:
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("MINION_EXECUTOR_STATE_DIR", str(tmp_path / "state"))
    for k in ("MINION_EXECUTOR_DEV", "MINION_EXECUTOR_TOKEN", "MINION_EXECUTOR_CONFIG"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(service, "_executable", lambda: EXE)
    monkeypatch.setattr(service, "_is_darwin", lambda: True)
    monkeypatch.setattr(service, "_sleep", lambda s: None)
    f = Fake()
    monkeypatch.setattr(menubar_agent, "run_launchctl", f)
    monkeypatch.setattr(service, "run_launchctl", f)
    return f


def args(tmp_path: Path, verb: str, *extra: str) -> list[str]:
    return ["menubar", verb, "--agents-dir", str(tmp_path / "ag"), *extra]


def test_golden_plist() -> None:
    data = menubar_agent.render_menubar_plist(
        label=menubar_agent.MENUBAR_LABEL,
        executable=EXE,
        log_path=LOG,
        env={"MINION_EXECUTOR_STATE_DIR": "/s", "MINION_EXECUTOR_TOKEN": "t", "HF_TOKEN": "h"},
    )
    assert plistlib.loads(data) == {
        "Label": "com.open-minions.menubar",
        "ProgramArguments": [EXE, "menubar"],
        "EnvironmentVariables": {"MINION_EXECUTOR_STATE_DIR": "/s"},
        "RunAtLoad": True,
        "KeepAlive": {"SuccessfulExit": False},
        "ThrottleInterval": 30,
        "LimitLoadToSessionType": "Aqua",
        "ProcessType": "Interactive",
        "StandardOutPath": LOG,
        "StandardErrorPath": LOG,
        "Umask": 0o077,
    }
    assert b"tok" not in data and b"hf" not in data.lower().replace(b"hfs", b"")


def test_no_env_block_when_nothing_set() -> None:
    doc = plistlib.loads(
        menubar_agent.render_menubar_plist(label="l", executable=EXE, log_path=LOG)
    )
    assert "EnvironmentVariables" not in doc


def test_dry_run_touches_nothing(fake: Fake, tmp_path: Path) -> None:
    res = runner.invoke(app, args(tmp_path, "install", "--dry-run"))
    assert res.exit_code == 0, res.output
    assert "<string>Aqua</string>" in res.output
    assert fake.calls == [] and not (tmp_path / "ag").exists()


def test_install_idempotent_and_uninstall_idempotent(fake: Fake, tmp_path: Path) -> None:
    plist = tmp_path / "ag" / "com.open-minions.menubar.plist"
    res = runner.invoke(app, args(tmp_path, "install"))
    assert res.exit_code == 0, res.output
    assert plist.stat().st_mode & 0o777 == 0o644
    doc = plistlib.loads(plist.read_bytes())
    assert doc["EnvironmentVariables"] == {"MINION_EXECUTOR_STATE_DIR": str(tmp_path / "state")}
    assert [c[0] for c in fake.calls] == ["bootstrap"]
    again = runner.invoke(app, args(tmp_path, "install"))
    assert again.exit_code == 0 and "up to date" in again.output
    assert [c[0] for c in fake.calls] == ["bootstrap", "print"]  # loaded: no second bootstrap
    assert runner.invoke(app, args(tmp_path, "uninstall")).exit_code == 0
    assert not plist.exists() and not fake.loaded
    again = runner.invoke(app, args(tmp_path, "uninstall"))
    assert again.exit_code == 0 and "nothing to remove" in again.output


def test_install_differing_plist_refused(fake: Fake, tmp_path: Path) -> None:
    ag = tmp_path / "ag"
    ag.mkdir(mode=0o755)
    (ag / "com.open-minions.menubar.plist").write_bytes(plistlib.dumps({"Label": "x"}))
    res = runner.invoke(app, args(tmp_path, "install"))
    assert res.exit_code == 2 and "uninstall" in res.output
    assert fake.calls == []


def test_refuses_dev(fake: Fake, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_DEV", "1")
    res = runner.invoke(app, args(tmp_path, "install"))
    assert res.exit_code == 2 and "MINION_EXECUTOR_DEV" in res.output
    assert fake.calls == [] and not (tmp_path / "ag").exists()


def test_refuses_tcc_path(fake: Fake, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    bad = tmp_path / "home" / "Documents" / "bin" / "minion-executor"
    monkeypatch.setattr(service, "_executable", lambda: str(bad))
    res = runner.invoke(app, args(tmp_path, "install"))
    assert res.exit_code == 2 and "protected folder" in res.output
    assert fake.calls == [] and not (tmp_path / "ag").exists()


def test_label_isolation(fake: Fake, tmp_path: Path) -> None:
    ag = tmp_path / "ag"
    # menubar commands refuse the executor's label
    for verb in ("install", "uninstall", "status"):
        res = runner.invoke(app, ["menubar", verb, "--label", service.DEFAULT_LABEL])
        assert res.exit_code == 2
    # menubar uninstall refuses an executor plist even under another label
    ag.mkdir(mode=0o755)
    (ag / "com.x.plist").write_bytes(
        plistlib.dumps({"Label": "com.x", "ProgramArguments": [EXE, "run"]})
    )
    res = runner.invoke(app, args(tmp_path, "uninstall", "--label", "com.x"))
    assert res.exit_code == 2 and "refusing" in res.output
    # and service uninstall refuses a menubar plist
    mb = ag / "com.open-minions.menubar.plist"
    mb.write_bytes(
        menubar_agent.render_menubar_plist(
            label="com.open-minions.menubar", executable=EXE, log_path=LOG
        )
    )
    res = runner.invoke(
        app,
        ["service", "uninstall", "--label", "com.open-minions.menubar", "--agents-dir", str(ag)],
    )
    assert res.exit_code == 2 and mb.exists()
    assert not any(c[0] == "bootout" for c in fake.calls)


def test_status(fake: Fake) -> None:
    assert "not loaded" in runner.invoke(app, ["menubar", "status"]).output
    fake.loaded = True
    assert "state=running pid=7" in runner.invoke(app, ["menubar", "status"]).output


def test_bare_menubar_still_runs_app(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.platform", "linux")
    res = runner.invoke(app, ["menubar"])
    assert res.exit_code == 2 and "needs macOS" in res.output
