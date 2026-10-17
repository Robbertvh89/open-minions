"""Owner-only small files for the capacity code (control.json, dev signal snapshots).

Same trust rule as `executor.toml` (invariant 6): a file that shapes the capacity budget is only
honoured when it belongs to the current user and nobody else can write it. Unlike the TOML config,
symlinks are never followed here (`O_NOFOLLOW`): these files are written by our own CLI, so there is
no dotfile-manager use case that would justify the extra attack surface. `O_NOFOLLOW` covers only
the last path component (symlinks in parents are followed; same-user/admin only, TASK-9).
A failed directory fsync after a successful rename is not an error: `atomic_write_private` returns
False (file in place, durability unconfirmed) and the caller decides how to tell the owner.
"""

from __future__ import annotations

import contextlib
import errno
import fcntl
import os
import secrets
import stat
import time
from collections.abc import Iterator
from pathlib import Path


class SafeFileError(ValueError):
    """Missing, unsafe or oversized file or directory."""


class UnsafeDirError(SafeFileError):
    """The directory exists but cannot be trusted (wrong owner, writable by others, symlink)."""


class MissingDirError(SafeFileError):
    """The directory does not exist (distinct from an unsafe one)."""


def _check_owner_mode(st: os.stat_result, what: str, path: Path) -> None:
    cls = UnsafeDirError if what == "directory" else SafeFileError
    if st.st_uid != os.geteuid():
        raise cls(f"{what} {path} is not owned by the current user")
    if st.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise cls(f"{what} {path} is group- or world-writable (use chmod go-w)")


def open_private_dir(path: Path) -> int:
    """Open `path` as a directory (no symlink at the last component) and vet that very fd.

    Returns an fd for dir-relative operations, so the checked directory is the used directory.
    The caller closes it. Raises MissingDirError when absent, SafeFileError when unsafe.
    """
    try:
        dfd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except FileNotFoundError:
        raise MissingDirError(f"directory {path} does not exist") from None
    except OSError as exc:  # ELOOP / ENOTDIR for a symlink or file, EACCES, ...
        raise UnsafeDirError(
            f"cannot open directory {path}: {exc.strerror}; fix or replace the state directory "
            "(it must be a real directory you own, not group/world-writable: chmod 700)"
        ) from None
    try:
        _check_owner_mode(os.fstat(dfd), "directory", path)
    except BaseException:
        os.close(dfd)
        raise
    return dfd


def check_private_dir(path: Path) -> None:
    """A real directory (not a symlink), owned by us, not writable by group or others."""
    os.close(open_private_dir(path))


def ensure_private_dir(path: Path) -> None:
    """Create `path` with mode 0700 when missing, then check it.

    An existing directory is accepted as is when it is ours and not group/world-writable.
    """
    try:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
    except OSError as exc:
        raise SafeFileError(f"cannot create directory {path}: {exc.strerror}") from None
    check_private_dir(path)


def read_private_file(path: Path, what: str, max_bytes: int) -> bytes | None:
    """Contents of `path`, or None when the file (or its directory) does not exist.

    Raises SafeFileError when unsafe.
    """
    try:
        dfd = open_private_dir(path.parent)
    except MissingDirError:
        return None
    try:
        try:
            fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=dfd)
        except FileNotFoundError:
            return None
        except OSError as exc:  # ELOOP for a symlink, EACCES, ...
            raise SafeFileError(f"cannot read {what} {path}: {exc.strerror}") from None
        try:
            st = os.fstat(fd)  # the opened file, so no check/use race
            if not stat.S_ISREG(st.st_mode):
                raise SafeFileError(f"{what} {path} is not a regular file")
            _check_owner_mode(st, what, path)
            with os.fdopen(fd, "rb") as f:
                fd = -1  # now owned by f
                data = f.read(max_bytes + 1)
        except OSError as exc:
            raise SafeFileError(f"cannot read {what} {path}: {exc.strerror}") from None
        finally:
            if fd >= 0:
                os.close(fd)
    finally:
        os.close(dfd)
    if len(data) > max_bytes:
        raise SafeFileError(f"{what} {path} is larger than {max_bytes} bytes")
    return data


def atomic_write_private(path: Path, data: bytes) -> bool:
    """Write `data` to `path` atomically with mode 0600: tmp file + fsync + rename + dir fsync.

    All steps are relative to one vetted directory fd. Returns False when the file is in place but
    the directory fsync failed (not confirmed durable), True otherwise.
    """
    try:
        dfd = open_private_dir(path.parent)
    except MissingDirError as exc:
        raise SafeFileError(str(exc)) from None
    durable = True
    tmp = f".{path.name}.{secrets.token_hex(8)}.tmp"
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
        try:
            fd = os.open(tmp, flags, 0o600, dir_fd=dfd)
        except OSError as exc:
            raise SafeFileError(f"cannot write {path}: {exc.strerror}") from None
        try:
            try:
                os.fchmod(fd, 0o600)  # not subject to the umask
                with os.fdopen(fd, "wb") as f:
                    fd = -1
                    f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
                # replaces a symlink at `path` itself, never follows it
                os.replace(tmp, path.name, src_dir_fd=dfd, dst_dir_fd=dfd)
            except OSError as exc:
                raise SafeFileError(f"cannot write {path}: {exc.strerror}") from None
            try:
                os.fsync(dfd)
            except OSError:
                durable = False  # the file is in place; only crash durability is unconfirmed
        finally:
            if fd >= 0:
                os.close(fd)
            with contextlib.suppress(OSError):
                os.unlink(tmp, dir_fd=dfd)
    finally:
        os.close(dfd)
    return durable


def open_lock_fd(dfd: int, name: str, path: Path) -> int:
    """Open (creating, 0600) the lock file `name` relative to the vetted directory fd `dfd`.

    Never follows a symlink; the opened file must be a regular file of ours, and its mode is set
    to 0600 on the fd. Returns the fd (the caller closes it). Raises SafeFileError otherwise.
    """
    fd = -1
    for attempt in range(5):
        try:
            fd = os.open(
                name, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=dfd
            )
            break
        except FileNotFoundError:
            # openat(O_CREAT) race on Darwin: concurrent first-time creates can report ENOENT
            # although the file now exists, so retry briefly
            time.sleep(0.01 * (attempt + 1))
        except OSError as exc:
            raise SafeFileError(f"cannot open lock file {path}: {exc.strerror}") from None
    if fd < 0:
        raise SafeFileError(f"cannot open lock file {path}: No such file or directory")
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.geteuid():
            raise SafeFileError(f"lock file {path} is not a regular file of ours")
        os.fchmod(fd, 0o600)
    except OSError as exc:
        os.close(fd)
        raise SafeFileError(f"cannot use lock file {path}: {exc.strerror}") from None
    except BaseException:
        os.close(fd)
        raise
    return fd


LOCK_WAIT_S = 2.0


@contextlib.contextmanager
def private_lock(
    directory: Path, name: str, wait_s: float = LOCK_WAIT_S, busy_message: str | None = None
) -> Iterator[None]:
    """Hold an exclusive `flock` on `directory/name` (0600, O_NOFOLLOW, created when missing).

    Serialises read-modify-write cycles across threads and processes. The directory is vetted
    first and the lock file is opened relative to that fd. Waits at most `wait_s` (non-blocking
    attempts, so a stuck holder cannot hang the caller). Raises SafeFileError when the directory
    or lock file is unsafe, the lock cannot be taken, or the wait runs out. The lock file is never
    removed (that would race). `busy_message` replaces the default "lock {name} is busy".
    """
    dfd = open_private_dir(directory)
    try:
        fd = open_lock_fd(dfd, name, directory / name)
    finally:
        os.close(dfd)
    try:
        deadline = time.monotonic() + wait_s
        delay = 0.005
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError as exc:
                if exc.errno not in (errno.EWOULDBLOCK, errno.EAGAIN):
                    raise SafeFileError(f"cannot lock {directory / name}: {exc.strerror}") from None
            if time.monotonic() >= deadline:
                raise SafeFileError(busy_message or f"lock {name} is busy")
            time.sleep(delay)
            delay = min(delay * 2, 0.05)
        try:
            yield
        finally:
            with contextlib.suppress(OSError):
                fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)
