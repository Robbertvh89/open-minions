"""Menu-bar LaunchAgent: golden plist, idempotent install/uninstall, refusals, isolation."""

from __future__ import annotations

import os
import plistlib
import sys
from pathlib import Path

import pytest
from minion_executor import launchd, menubar_agent, service
from minion_executor.main import app
from typer.testing import CliRunner

runner = CliRunner()
EXE = "/opt/bin/minion-executor"
LOG = "/Users/example/Library/Logs/open-minions/com.open-minions.menubar.log"


class Fake:
    """Fake launchctl. `rc` overrides the return code per verb (bootstrap/bootout/print)."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self.loaded = False
        self.rc: dict[str, int] = {}
        self.stuck = False  # bootout answers but the job never unloads
        self.print_seq: list[int] = []  # forced `print` rcs, consumed in order

    def __call__(self, args: list[str], timeout: float = 30.0) -> tuple[int, str]:
        self.calls.append(args)
        verb = args[0]
        if verb in self.rc and not (verb == "print" and self.print_seq):
            return self.rc[verb], "forced"
        if verb == "bootstrap":
            self.loaded = True
        elif verb == "bootout":
            self.loaded = self.stuck
        elif verb == "print":
            if self.print_seq:
                return self.print_seq.pop(0), ""
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
    monkeypatch.setattr(launchd, "executable", lambda: EXE)
    monkeypatch.setattr(launchd, "is_darwin", lambda: True)
    monkeypatch.setattr(launchd, "check_install_chain", lambda exe: None)
    clock = {"t": 0.0}
    monkeypatch.setattr(launchd, "_now", lambda: clock["t"])
    monkeypatch.setattr(launchd, "_sleep", lambda s: clock.__setitem__("t", clock["t"] + s))
    f = Fake()
    monkeypatch.setattr(launchd, "run_launchctl", f)
    return f


def args(tmp_path: Path, verb: str, *extra: str) -> list[str]:
    return ["menubar", verb, "--agents-dir", str(tmp_path / "ag"), *extra]


def test_golden_plist() -> None:
    data = menubar_agent.render_menubar_plist(
        label=launchd.MENUBAR_LABEL,
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
    assert b"TOKEN" not in data and b"/s" in data


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
    monkeypatch.setattr(launchd, "executable", lambda: str(bad))
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


PLIST = "com.open-minions.menubar.plist"


def installed(tmp_path: Path) -> Path:
    assert runner.invoke(app, args(tmp_path, "install", "--no-load")).exit_code == 0
    return tmp_path / "ag" / PLIST


def test_fresh_install_bootstrap_failure_removes_plist(fake: Fake, tmp_path: Path) -> None:
    fake.rc["bootstrap"] = 5
    res = runner.invoke(app, args(tmp_path, "install"))
    assert res.exit_code == 2 and "bootstrap failed (5)" in res.output
    assert not (tmp_path / "ag" / PLIST).exists()


def test_identical_not_loaded_bootstrap_failure_keeps_plist(fake: Fake, tmp_path: Path) -> None:
    plist = installed(tmp_path)
    fake.rc["bootstrap"] = 5
    res = runner.invoke(app, args(tmp_path, "install"))
    assert res.exit_code == 2 and "bootstrap failed" in res.output
    assert plist.exists()
    assert [c[0] for c in fake.calls] == ["print", "bootstrap"]


def test_identical_unqueryable_launchd_exits_2_no_bootstrap(fake: Fake, tmp_path: Path) -> None:
    installed(tmp_path)
    fake.rc["print"] = 5
    res = runner.invoke(app, args(tmp_path, "install"))
    assert res.exit_code == 2 and "cannot query launchd (5)" in res.output
    assert not any(c[0] == "bootstrap" for c in fake.calls)


def test_no_load_writes_only(fake: Fake, tmp_path: Path) -> None:
    plist = installed(tmp_path)
    assert plist.exists() and fake.calls == []
    again = runner.invoke(app, args(tmp_path, "install", "--no-load"))
    assert again.exit_code == 0 and "up to date" in again.output and fake.calls == []


@pytest.mark.parametrize("mutate", ["symlink", "group_writable"])
def test_existing_plist_must_be_a_private_regular_file(
    fake: Fake, tmp_path: Path, mutate: str
) -> None:
    plist = installed(tmp_path)
    if mutate == "symlink":
        real = tmp_path / "real.plist"
        real.write_bytes(plist.read_bytes())
        plist.unlink()
        plist.symlink_to(real)
    else:
        plist.chmod(0o664)
    res = runner.invoke(app, args(tmp_path, "install"))
    assert res.exit_code == 2 and "uninstall" in res.output
    assert fake.calls == []


@pytest.mark.parametrize("rc", [36, 0])
def test_uninstall_waits_when_bootout_in_progress(fake: Fake, tmp_path: Path, rc: int) -> None:
    plist = installed(tmp_path)
    fake.rc["bootout"] = rc
    fake.print_seq = [0, 0, 113]  # still stopping for two polls, then gone
    res = runner.invoke(app, args(tmp_path, "uninstall"))
    assert res.exit_code == 0 and "stopped" in res.output and not plist.exists()
    assert [c[0] for c in fake.calls] == ["bootout", "print", "print", "print"]


def test_uninstall_wait_timeout_keeps_plist(fake: Fake, tmp_path: Path) -> None:
    plist = installed(tmp_path)
    fake.loaded = fake.stuck = True
    res = runner.invoke(app, args(tmp_path, "uninstall"))
    assert res.exit_code == 2 and "still stopping" in res.output and plist.exists()


@pytest.mark.parametrize("rc", [3, 113])
def test_uninstall_bootout_not_loaded_still_removes_plist(
    fake: Fake, tmp_path: Path, rc: int
) -> None:
    plist = installed(tmp_path)
    fake.rc["bootout"] = rc
    res = runner.invoke(app, args(tmp_path, "uninstall"))
    assert res.exit_code == 0 and not plist.exists()


def test_uninstall_bootout_other_error_keeps_plist(fake: Fake, tmp_path: Path) -> None:
    plist = installed(tmp_path)
    fake.rc["bootout"] = 5
    res = runner.invoke(app, args(tmp_path, "uninstall"))
    assert res.exit_code == 2 and plist.exists()


def test_uninstall_malformed_plist_exits_2(fake: Fake, tmp_path: Path) -> None:
    plist = installed(tmp_path)
    plist.write_bytes(b"not a plist")
    res = runner.invoke(app, args(tmp_path, "uninstall"))
    assert res.exit_code == 2 and "malformed" in res.output and plist.exists()
    assert "launchctl bootout" in res.output
    assert not any(c[0] == "bootout" for c in fake.calls)


def test_uninstall_requires_matching_label_and_exact_program_name(
    fake: Fake, tmp_path: Path
) -> None:
    ag = tmp_path / "ag"
    ag.mkdir(mode=0o755)
    other = {"Label": "com.other", "ProgramArguments": [EXE, "menubar"]}
    (ag / PLIST).write_bytes(plistlib.dumps(other))
    res = runner.invoke(app, args(tmp_path, "uninstall"))
    assert res.exit_code == 2 and "refusing" in res.output
    evil = {
        "Label": "com.open-minions.menubar",
        "ProgramArguments": ["/x/evil-minion-executor", "menubar"],
    }
    (ag / PLIST).write_bytes(plistlib.dumps(evil))
    res = runner.invoke(app, args(tmp_path, "uninstall"))
    assert res.exit_code == 2 and "refusing" in res.output
    assert (ag / PLIST).exists() and not any(c[0] == "bootout" for c in fake.calls)


def test_warns_about_uncarried_env(
    fake: Fake, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_MODEL_CACHE_GB", "5")
    res = runner.invoke(app, args(tmp_path, "install", "--dry-run"))
    assert res.exit_code == 0
    assert "MINION_EXECUTOR_MODEL_CACHE_GB is set but not carried" in res.output
    assert "MINION_EXECUTOR_STATE_DIR is set" not in res.output


def test_refuses_root_for_every_launchd_command(
    fake: Fake, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(os, "geteuid", lambda: 0)
    for group in ("menubar", "service"):
        for verb in ("install", "uninstall"):
            res = runner.invoke(app, [group, verb, "--agents-dir", str(tmp_path / "ag")])
            assert res.exit_code == 2 and "root" in res.output, (group, verb)
    assert fake.calls == [] and not (tmp_path / "ag").exists()


def test_service_install_rejects_menubar_label(fake: Fake, tmp_path: Path) -> None:
    res = runner.invoke(app, ["service", "install", "--label", launchd.MENUBAR_LABEL, "--dry-run"])
    assert res.exit_code == 2 and "menubar" in res.output


# --- install chain (Sec4) --------------------------------------------------------------------


def _chain(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "chain"
    (root / "bin").mkdir(parents=True)
    exe = root / "bin" / "minion-executor"
    exe.write_text("x")
    for d in (tmp_path, root, root / "bin"):
        d.chmod(0o755)
    exe.chmod(0o755)
    monkeypatch.setattr(sys, "executable", str(exe))
    monkeypatch.setattr(sys, "prefix", str(root))
    return exe


@pytest.mark.skipif(sys.platform != "darwin", reason="the shared temp dir is sticky elsewhere")
def test_chain_accepts_private_tree(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    exe = _chain(tmp_path, monkeypatch)
    launchd.check_install_chain(str(exe))


@pytest.mark.parametrize("which", ["exe", "bindir", "root"])
@pytest.mark.parametrize("mode", [0o775, 0o757, 0o1777])
def test_chain_refuses_writable_component(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, which: str, mode: int
) -> None:
    exe = _chain(tmp_path, monkeypatch)
    monkeypatch.setattr(launchd, "ADMIN_GID", -1)
    target = {"exe": exe, "bindir": exe.parent, "root": exe.parent.parent}[which]
    target.chmod(mode)
    with pytest.raises(ValueError, match=r"writable|owned"):
        launchd.check_install_chain(str(exe))


@pytest.mark.skipif(sys.platform != "darwin", reason="the shared temp dir is sticky elsewhere")
def test_chain_tolerates_admin_group_write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Stock Homebrew leaves /opt/homebrew/Cellar admin-group writable; admins can sudo anyway."""
    exe = _chain(tmp_path, monkeypatch)
    exe.parent.chmod(0o775)
    monkeypatch.setattr(launchd, "ADMIN_GID", exe.parent.stat().st_gid)
    launchd.check_install_chain(str(exe))
    exe.parent.chmod(0o777)  # others-writable stays refused even for the admin group
    with pytest.raises(ValueError, match="writable by others"):
        launchd.check_install_chain(str(exe))


def test_chain_refuses_foreign_owner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    exe = _chain(tmp_path, monkeypatch)
    monkeypatch.setattr(os, "geteuid", lambda: exe.stat().st_uid + 1000)
    with pytest.raises(ValueError, match="owned by another user"):
        launchd.check_install_chain(str(exe))


def test_chain_vets_resolved_symlink_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    exe = _chain(tmp_path, monkeypatch)
    loose = tmp_path / "loose"
    loose.mkdir()
    loose.chmod(0o777)
    (loose / "real").write_text("x")
    link = exe.parent / "link"
    link.symlink_to(loose / "real")
    with pytest.raises(ValueError, match="writable"):
        launchd.check_install_chain(str(link))


@pytest.mark.skipif(sys.platform != "darwin", reason="the shared temp dir is sticky elsewhere")
def test_chain_vets_intermediate_symlink_hop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    exe = _chain(tmp_path, monkeypatch)
    real = tmp_path / "tight" / "real"
    real.parent.mkdir()
    real.parent.chmod(0o755)
    real.write_text("x")
    loose = tmp_path / "loose"
    loose.mkdir()
    loose.chmod(0o777)
    (loose / "hop").symlink_to(real)  # hop 2 lives in a loose dir, the end is tight
    (exe.parent / "link").symlink_to(loose / "hop")
    with pytest.raises(ValueError, match="writable"):
        launchd.check_install_chain(str(exe.parent / "link"))


def test_chain_symlink_loop_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    exe = _chain(tmp_path, monkeypatch)
    (exe.parent / "a").symlink_to(exe.parent / "b")
    (exe.parent / "b").symlink_to(exe.parent / "a")
    with pytest.raises(ValueError, match="symlink hops"):
        launchd.check_install_chain(str(exe.parent / "a"))


@pytest.mark.parametrize("kind", ["symlink", "group_writable"])
def test_uninstall_refuses_unsafe_plist(fake: Fake, tmp_path: Path, kind: str) -> None:
    plist = installed(tmp_path)
    if kind == "symlink":
        other = tmp_path / "other.plist"
        other.write_bytes(plist.read_bytes())
        plist.unlink()
        plist.symlink_to(other)
    else:
        plist.chmod(0o664)
    res = runner.invoke(app, args(tmp_path, "uninstall"))
    assert res.exit_code == 2 and "launchctl bootout" in res.output
    assert not any(c[0] == "bootout" for c in fake.calls)
    assert plist.is_symlink() or plist.exists()


@pytest.mark.parametrize("kind", ["symlink", "fifo"])
def test_unsafe_menubar_lock_exits_2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    lock = state / "menubar.lock"
    if kind == "symlink":
        lock.symlink_to(tmp_path / "elsewhere")
    else:
        os.mkfifo(lock)
    monkeypatch.setenv("MINION_EXECUTOR_STATE_DIR", str(state))
    monkeypatch.setattr(sys, "platform", "darwin")
    res = runner.invoke(app, ["menubar"])
    assert res.exit_code == 2, res.output


# --- single instance (R6) --------------------------------------------------------------------


def test_second_menubar_instance_exits_0_without_starting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import types

    from minion_executor.menubar.lock import LOCK_FILE, acquire_menubar_lock

    state = tmp_path / "state"
    monkeypatch.setenv("MINION_EXECUTOR_STATE_DIR", str(state))
    monkeypatch.setattr(sys, "platform", "darwin")
    started: list[Path] = []
    fake_app = types.ModuleType("minion_executor.menubar.app")
    fake_app.run = lambda sd, cfg=None: started.append(sd)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "minion_executor.menubar.app", fake_app)
    held = None
    try:
        first = runner.invoke(app, ["menubar"])
        assert first.exit_code == 0 and started == [state]
        held = acquire_menubar_lock(state)  # the first run released its lock on exit
        assert held is not None and (state / LOCK_FILE).stat().st_mode & 0o777 == 0o600
        second = runner.invoke(app, ["menubar"])
        assert second.exit_code == 0 and "already running" in second.output
        assert started == [state]  # the app was not started again
    finally:
        if held is not None:
            os.close(held)
