"""Single-instance lock of the menu-bar app: `menubar.lock` in the private state dir."""

from __future__ import annotations

import contextlib
import errno
import fcntl
import os
from pathlib import Path

from minion_executor.capacity.safefile import SafeFileError, open_lock_fd, open_private_dir

LOCK_FILE = "menubar.lock"


def acquire_menubar_lock(state_dir: Path) -> int | None:
    """Exclusive non-blocking flock. Returns the fd (keep it open until exit), or None when
    another menu-bar app holds it. SafeFileError when the lock file is unsafe."""
    dfd = open_private_dir(state_dir)
    try:
        fd = open_lock_fd(dfd, LOCK_FILE, state_dir / LOCK_FILE)
    finally:
        os.close(dfd)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        with contextlib.suppress(OSError):
            os.close(fd)
        if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
            return None
        raise SafeFileError(f"cannot lock {state_dir / LOCK_FILE}: {exc.strerror}") from None
    return fd
