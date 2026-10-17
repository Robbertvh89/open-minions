"""Owner control file: strict schema, atomic 0600 writes, unsafe files ignored, durations."""

from __future__ import annotations

import json
import os
import stat
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from minion_common.enums import CapacityProfile
from minion_executor.capacity.control import (
    CONTROL_FILE,
    CONTROL_LOCK_FILE,
    ControlUnusable,
    OwnerControl,
    parse_duration,
    read_control,
    update_control,
    write_control,
)
from minion_executor.capacity.safefile import SafeFileError, private_lock

NOW = datetime(2026, 10, 16, 12, 0, tzinfo=UTC)


@pytest.fixture
def sd(tmp_path: Path) -> Path:
    return tmp_path / "state"


@pytest.mark.parametrize(
    ("text", "secs"),
    [("30m", 1800), ("1h", 3600), ("2h30m", 9000), ("90s", 90), ("1d", 86400), (" 45M ", 2700)],
)
def test_parse_duration(text: str, secs: int) -> None:
    assert parse_duration(text) == timedelta(seconds=secs)


@pytest.mark.parametrize("text", ["", "0m", "0h0m", "abc", "10", "-5m", "1.5h", "31d", "h", "1m1h"])
def test_parse_duration_rejects(text: str) -> None:
    with pytest.raises(ValueError):
        parse_duration(text)


def test_write_creates_private_dir_and_file(sd: Path) -> None:
    write_control(sd, OwnerControl(paused=True))
    assert stat.S_IMODE(sd.stat().st_mode) == 0o700
    f = sd / CONTROL_FILE
    assert stat.S_IMODE(f.stat().st_mode) == 0o600
    assert not [p for p in sd.iterdir() if p.name != CONTROL_FILE]  # no tmp left behind
    got = read_control(sd)
    assert isinstance(got, OwnerControl) and got.paused and got.written_at is not None


def test_write_ignores_umask(sd: Path) -> None:
    old = os.umask(0o000)
    try:
        write_control(sd, OwnerControl())
    finally:
        os.umask(old)
    assert stat.S_IMODE((sd / CONTROL_FILE).stat().st_mode) == 0o600


def _nowarn(_msg: str) -> None:
    pass


def test_update_preserves_other_fields(sd: Path) -> None:
    update_control(sd, profile=CapacityProfile.FULL, now=NOW, warn=_nowarn)
    c = update_control(sd, paused=True, pause_for=timedelta(hours=1), now=NOW, warn=_nowarn)
    assert c.profile is CapacityProfile.FULL and c.paused_until == NOW + timedelta(hours=1)
    assert c.is_paused(NOW) and not c.is_paused(NOW + timedelta(hours=1))
    c = update_control(sd, paused=False, warn=_nowarn)
    assert c.profile is CapacityProfile.FULL and not c.paused and c.paused_until is None
    c = update_control(sd, clear_profile=True, warn=_nowarn)
    assert c.profile is None
    c = update_control(sd, paused=True, now=NOW, warn=_nowarn)
    assert c.is_paused(NOW + timedelta(days=999))  # indefinite


def test_atomic_replace_keeps_old_file_when_write_fails(
    sd: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_control(sd, OwnerControl(paused=True))

    def boom(*args: object, **kwargs: object) -> None:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(SafeFileError):
        write_control(sd, OwnerControl(paused=False))
    monkeypatch.undo()
    c = read_control(sd)
    assert isinstance(c, OwnerControl) and c.paused
    assert [p.name for p in sd.iterdir()] == [CONTROL_FILE]


def test_read_missing_is_none(sd: Path) -> None:
    sd.mkdir(mode=0o700)
    assert read_control(sd) is None


@pytest.mark.parametrize(
    "payload",
    [
        "{",
        '{"paused": true, "extra": 1}',
        '{"paused": "yes"}',
        '{"profile": "turbo"}',
        '{"paused_until": "2026-10-16T12:00:00"}',  # naive datetime
        '{"version": "x"}',
        "[]",
    ],
)
def test_invalid_control_is_unusable(sd: Path, payload: str) -> None:
    write_control(sd, OwnerControl())
    f = sd / CONTROL_FILE
    f.write_text(payload)
    f.chmod(0o600)
    assert isinstance(read_control(sd), ControlUnusable)


def test_newer_version_is_unusable(sd: Path) -> None:
    write_control(sd, OwnerControl())
    f = sd / CONTROL_FILE
    f.write_text('{"version": 2, "paused": false}')
    f.chmod(0o600)
    assert isinstance(read_control(sd), ControlUnusable)


def test_missing_state_dir_is_silently_absent(sd: Path) -> None:
    assert read_control(sd) is None  # the dir does not exist yet: no warning, no unusable


def test_unreadable_or_non_regular_is_unusable(sd: Path) -> None:
    write_control(sd, OwnerControl())
    f = sd / CONTROL_FILE
    f.chmod(0o000)
    if os.geteuid() != 0:
        assert isinstance(read_control(sd), ControlUnusable)
    f.unlink()
    f.mkdir()
    assert isinstance(read_control(sd), ControlUnusable)


def test_profile_on_unusable_file_keeps_the_pause(sd: Path) -> None:
    write_control(sd, OwnerControl())
    f = sd / CONTROL_FILE
    f.write_text("{")
    f.chmod(0o600)
    warnings: list[str] = []
    c = update_control(sd, profile=CapacityProfile.FULL, warn=warnings.append)
    assert c.paused and c.profile is CapacityProfile.FULL
    assert len(warnings) == 1 and "pause is kept" in warnings[0]
    got = read_control(sd)
    assert isinstance(got, OwnerControl) and got.paused
    # resume is the explicit way out, and does not warn
    f.write_text("{")
    warnings.clear()
    c = update_control(sd, paused=False, warn=warnings.append)
    assert not c.paused and not warnings


def test_unsafe_files_are_ignored(sd: Path, tmp_path: Path) -> None:
    write_control(sd, OwnerControl(paused=True))
    f = sd / CONTROL_FILE
    f.chmod(0o666)
    assert isinstance(read_control(sd), ControlUnusable)  # world-writable
    f.chmod(0o600)
    assert read_control(sd) is not None
    # symlink at the file: never followed
    real = tmp_path / "real.json"
    real.write_text(json.dumps({"paused": True}))
    real.chmod(0o600)
    f.unlink()
    f.symlink_to(real)
    assert isinstance(read_control(sd), ControlUnusable)
    f.unlink()
    # group/world-writable state dir
    write_control(sd, OwnerControl(paused=True))
    sd.chmod(0o770)
    assert isinstance(read_control(sd), ControlUnusable)
    with pytest.raises(SafeFileError):
        write_control(sd, OwnerControl())


def test_symlinked_state_dir_is_refused(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir(mode=0o700)
    link = tmp_path / "link"
    link.symlink_to(real)
    with pytest.raises(SafeFileError):
        write_control(link, OwnerControl())
    assert isinstance(read_control(link), ControlUnusable)


def test_oversized_control_ignored(sd: Path) -> None:
    write_control(sd, OwnerControl())
    f = sd / CONTROL_FILE
    f.write_text(" " * 5000 + "{}")
    f.chmod(0o600)
    assert isinstance(read_control(sd), ControlUnusable)


def test_symlink_at_target_is_replaced_not_followed(sd: Path, tmp_path: Path) -> None:
    write_control(sd, OwnerControl())
    victim = tmp_path / "victim.txt"
    victim.write_text("keep")
    f = sd / CONTROL_FILE
    f.unlink()
    f.symlink_to(victim)
    write_control(sd, OwnerControl(paused=True))
    assert victim.read_text() == "keep"
    assert not f.is_symlink()


def test_update_control_unsafe_dir_hint_says_fix_permissions(tmp_path: Path) -> None:
    d = tmp_path / "state"
    d.mkdir()
    d.chmod(0o777)
    msgs: list[str] = []
    with pytest.raises(SafeFileError):
        update_control(d, profile=None, warn=msgs.append)
    # read_control reports the unsafe dir; the write then refuses the same dir
    assert msgs and "chmod 700" in msgs[0] and "resume" not in msgs[0]


def test_dir_fsync_failure_after_replace_warns_and_keeps_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import errno

    import minion_executor.capacity.safefile as sf

    real = os.fsync
    dfd_stat = tmp_path.stat()

    def fake(fd: int) -> None:
        st = os.fstat(fd)
        if stat.S_ISDIR(st.st_mode) and st.st_ino == dfd_stat.st_ino:
            raise OSError(errno.EIO, "io")
        real(fd)

    monkeypatch.setattr(os, "fsync", fake)
    assert sf.atomic_write_private(tmp_path / "f", b"x") is False
    assert (tmp_path / "f").read_bytes() == b"x"


def test_update_control_warns_when_not_durably_synced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import minion_executor.capacity.control as ctl_mod

    monkeypatch.setattr(ctl_mod, "atomic_write_private", lambda path, data: False)
    state = tmp_path / "state"
    msgs: list[str] = []
    update_control(state, paused=True, warn=msgs.append)
    assert msgs == [f"written but not durably synced: {state / 'control.json'}"]


def _blocked_update_thread(sd: Path, **kw: object) -> threading.Thread:
    def go() -> None:
        update_control(sd, warn=lambda _m: None, **kw)  # type: ignore[arg-type]

    t = threading.Thread(target=go)
    t.start()
    return t


def test_update_control_waits_for_the_lock_thread(sd: Path) -> None:
    write_control(sd, OwnerControl(profile=CapacityProfile.FULL))
    before = (sd / CONTROL_FILE).read_bytes()
    with private_lock(sd, CONTROL_LOCK_FILE):
        t = _blocked_update_thread(sd, paused=True)
        t.join(0.5)
        assert t.is_alive()  # blocked on the lock
        assert (sd / CONTROL_FILE).read_bytes() == before
    t.join(5)
    assert not t.is_alive()
    ctl = read_control(sd)
    assert isinstance(ctl, OwnerControl) and ctl.paused and ctl.profile is CapacityProfile.FULL


_CHILD = """
import sys
from pathlib import Path
from minion_executor.capacity.control import update_control
print("ready", flush=True)
update_control(Path(sys.argv[1]), paused=True, warn=lambda m: None)
"""


def test_update_control_waits_for_the_lock_process(sd: Path) -> None:
    import subprocess
    import sys

    write_control(sd, OwnerControl(profile=CapacityProfile.FULL))
    before = (sd / CONTROL_FILE).read_bytes()
    with private_lock(sd, CONTROL_LOCK_FILE):
        proc = subprocess.Popen(  # noqa: S603
            [sys.executable, "-c", _CHILD, str(sd)], stdout=subprocess.PIPE, text=True
        )
        try:
            assert proc.stdout is not None and proc.stdout.readline().strip() == "ready"
            time.sleep(0.5)  # the child is now in update_control, waiting for us
            assert proc.poll() is None
            assert (sd / CONTROL_FILE).read_bytes() == before
        except BaseException:
            proc.kill()
            raise
    assert proc.wait(timeout=30) == 0
    ctl = read_control(sd)
    assert isinstance(ctl, OwnerControl) and ctl.paused and ctl.profile is CapacityProfile.FULL


def test_interleaved_read_modify_write_loses_no_field(
    sd: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Writer 1 stalls between its read and its write; writer 2 must not slip in between."""
    import minion_executor.capacity.control as ctl_mod

    write_control(sd, OwnerControl())
    real_read = ctl_mod.read_control
    read_done, go, first = threading.Event(), threading.Event(), threading.Lock()

    def slow_read(state_dir: Path) -> object:
        got = real_read(state_dir)
        if first.acquire(blocking=False):  # only the first reader stalls
            read_done.set()
            assert go.wait(10)
        return got

    monkeypatch.setattr(ctl_mod, "read_control", slow_read)
    t1 = _blocked_update_thread(sd, paused=True)
    assert read_done.wait(10)
    t2 = _blocked_update_thread(sd, profile=CapacityProfile.FULL)
    time.sleep(0.3)  # writer 2 gets as far as it can; with the lock that is the lock wait
    go.set()
    t1.join(10)
    t2.join(10)
    assert not t1.is_alive() and not t2.is_alive()
    monkeypatch.undo()
    ctl = read_control(sd)
    assert isinstance(ctl, OwnerControl)
    assert ctl.paused and ctl.profile is CapacityProfile.FULL


def test_many_threads_lose_no_field(sd: Path) -> None:
    from concurrent.futures import ThreadPoolExecutor

    def pause() -> None:
        update_control(sd, paused=True, warn=lambda _m: None)

    def pin() -> None:
        update_control(sd, profile=CapacityProfile.FULL, warn=lambda _m: None)

    with ThreadPoolExecutor(8) as pool:
        futs = [pool.submit(pause if i % 2 else pin) for i in range(100)]
        for f in futs:
            f.result()
    ctl = read_control(sd)
    assert isinstance(ctl, OwnerControl)
    assert ctl.paused and ctl.profile is CapacityProfile.FULL


def test_private_lock_gives_up_with_a_clear_error(sd: Path) -> None:
    write_control(sd, OwnerControl())
    with private_lock(sd, CONTROL_LOCK_FILE):
        started = time.monotonic()
        with (
            pytest.raises(SafeFileError, match=f"lock {CONTROL_LOCK_FILE} is busy"),
            private_lock(sd, CONTROL_LOCK_FILE, wait_s=0.2),
        ):
            pass
        with (
            pytest.raises(SafeFileError, match="custom busy"),
            private_lock(sd, CONTROL_LOCK_FILE, wait_s=0.1, busy_message="custom busy"),
        ):
            pass
        assert time.monotonic() - started < 2


def test_lock_open_errors_are_safefile_errors(sd: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import errno

    write_control(sd, OwnerControl())

    real = os.fstat

    def boom(fd: int) -> os.stat_result:
        st = real(fd)
        if stat.S_ISREG(st.st_mode):  # the lock file; directory checks are not under test
            raise OSError(errno.EIO, "io")
        return st

    monkeypatch.setattr(os, "fstat", boom)
    with pytest.raises(SafeFileError):
        update_control(sd, paused=True, warn=_nowarn)


def test_control_lock_is_private_and_symlink_refused(sd: Path) -> None:
    update_control(sd, paused=True, warn=lambda _m: None)
    lock = sd / "control.lock"
    assert stat.S_IMODE(lock.stat().st_mode) == 0o600
    lock.unlink()
    (sd / "elsewhere").write_text("")
    lock.symlink_to(sd / "elsewhere")
    with pytest.raises(SafeFileError):
        update_control(sd, paused=False, warn=lambda _m: None)
