"""One executor per state directory (SPEC FR-56): an exclusive `flock` on `executor.lock`.

Two executors sharing a state dir would fight over `status.json` (one writing `stopped` while the
other runs) and over the owner's `control.json` semantics. The lock is held for the whole process
lifetime on a non-inheritable fd and dies with the process, so a crash never leaves it stuck. Only
the holder runs the loop, hence only the holder ever writes the `stopped` marker.
"""

from __future__ import annotations

import contextlib
import errno
import fcntl
import os
import stat
from pathlib import Path

from minion_executor.capacity.safefile import SafeFileError, open_private_dir

LOCK_FILE = "executor.lock"


class AlreadyRunningError(SafeFileError):
    """Another executor holds the state directory's lock."""


def acquire_instance_lock(state_dir: Path) -> int:
    """Take the exclusive lock and return its fd (keep it open until exit; closing releases it).

    Raises AlreadyRunningError when another process holds it, SafeFileError when the lock file
    cannot be used safely (symlink, not ours, not a regular file).
    """
    dfd = open_private_dir(state_dir)
    try:
        try:
            fd = os.open(
                LOCK_FILE,
                os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK,
                0o600,
                dir_fd=dfd,
            )
        except OSError as exc:
            raise SafeFileError(
                f"cannot open lock file {state_dir / LOCK_FILE}: {exc.strerror}"
            ) from None
    finally:
        os.close(dfd)
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.geteuid():
            raise SafeFileError(f"lock file {state_dir / LOCK_FILE} is not a regular file of ours")
        os.fchmod(fd, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in (errno.EWOULDBLOCK, errno.EAGAIN):
                raise SafeFileError(
                    f"cannot lock {state_dir / LOCK_FILE}: {exc.strerror}"
                ) from None
            raise AlreadyRunningError(
                f"another minion-executor is already running with state directory {state_dir} "
                "(see `minion-executor status`); stop it first"
            ) from None
    except BaseException:
        with contextlib.suppress(OSError):
            os.close(fd)
        raise
    return fd
