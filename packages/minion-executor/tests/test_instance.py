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
