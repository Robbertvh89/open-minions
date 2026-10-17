"""One executor per state directory: exclusive flock on executor.lock."""

from __future__ import annotations

import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest
from minion_executor.capacity.instance import (
    LOCK_FILE,
    AlreadyRunningError,
    acquire_instance_lock,
)
from minion_executor.capacity.safefile import SafeFileError


@pytest.fixture
def state_dir(tmp_path: Path) -> Path:
    d = tmp_path / "state"
    d.mkdir(mode=0o700)
    return d


def test_second_holder_is_refused_and_the_lock_is_private(state_dir: Path) -> None:
    fd = acquire_instance_lock(state_dir)
    try:
        assert stat.S_IMODE((state_dir / LOCK_FILE).stat().st_mode) == 0o600
        with pytest.raises(AlreadyRunningError, match="already running"):
            acquire_instance_lock(state_dir)
    finally:
        os.close(fd)
    os.close(acquire_instance_lock(state_dir))  # released with the fd


def test_lock_is_refused_across_processes_and_dies_with_the_holder(state_dir: Path) -> None:
    code = (
        "import sys, time\nfrom pathlib import Path\n"
        "from minion_executor.capacity.instance import acquire_instance_lock\n"
        "fd = acquire_instance_lock(Path(sys.argv[1]))\n"
        "print('locked', flush=True)\ntime.sleep(60)\n"
    )
    proc = subprocess.Popen(  # noqa: S603 - our own interpreter and script
        [sys.executable, "-c", code, str(state_dir)], stdout=subprocess.PIPE, text=True
    )
    try:
        assert proc.stdout is not None and proc.stdout.readline().strip() == "locked"
        with pytest.raises(AlreadyRunningError):
            acquire_instance_lock(state_dir)
    finally:
        proc.kill()
        proc.wait()
    os.close(acquire_instance_lock(state_dir))  # a crashed holder never leaves it stuck


def test_symlinked_lock_file_is_refused(state_dir: Path, tmp_path: Path) -> None:
    target = tmp_path / "elsewhere"
    target.write_text("x")
    (state_dir / LOCK_FILE).symlink_to(target)
    with pytest.raises(SafeFileError):
        acquire_instance_lock(state_dir)


def test_other_flock_errors_are_not_reported_as_already_running(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import errno
    import fcntl

    def boom(fd: int, op: int) -> None:
        raise OSError(errno.ENOLCK, os.strerror(errno.ENOLCK))

    monkeypatch.setattr(fcntl, "flock", boom)
    with pytest.raises(SafeFileError, match="cannot lock") as ei:
        acquire_instance_lock(state_dir)
    assert not isinstance(ei.value, AlreadyRunningError)


def test_concurrent_first_runs_on_a_fresh_dir_get_already_running(tmp_path: Path) -> None:
    import threading
    from concurrent.futures import ThreadPoolExecutor

    for round_ in range(20):
        d = tmp_path / f"fresh{round_}"
        d.mkdir(mode=0o700)
        barrier = threading.Barrier(8)

        def attempt(d: Path = d, barrier: threading.Barrier = barrier) -> int | Exception:
            barrier.wait()
            try:
                return acquire_instance_lock(d)
            except Exception as exc:
                return exc

        with ThreadPoolExecutor(8) as pool:
            results = [f.result() for f in [pool.submit(attempt) for _ in range(8)]]
        fds = [r for r in results if isinstance(r, int)]
        errs = [r for r in results if not isinstance(r, int)]
        for fd in fds:
            os.close(fd)
        assert len(fds) == 1, results
        assert all(type(e) is AlreadyRunningError for e in errs), errs
