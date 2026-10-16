"""Owner-only small files for the capacity code (control.json, dev signal snapshots).

Same trust rule as `executor.toml` (invariant 6): a file that shapes the capacity budget is only
honoured when it belongs to the current user and nobody else can write it. Unlike the TOML config,
symlinks are never followed here (`O_NOFOLLOW`): these files are written by our own CLI, so there is
no dotfile-manager use case that would justify the extra attack surface.
"""

from __future__ import annotations

import os
import secrets
import stat
from pathlib import Path


class SafeFileError(ValueError):
    """Missing, unsafe or oversized file or directory."""


def _check_owner_mode(st: os.stat_result, what: str, path: Path) -> None:
    if st.st_uid != os.geteuid():
        raise SafeFileError(f"{what} {path} is not owned by the current user")
    if st.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise SafeFileError(f"{what} {path} is group- or world-writable (use chmod go-w)")


def check_private_dir(path: Path) -> None:
    """A real directory (not a symlink), owned by us, not writable by group or others."""
    try:
        st = path.lstat()
    except OSError as exc:
        raise SafeFileError(f"cannot stat directory {path}: {exc.strerror}") from None
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
        raise SafeFileError(f"{path} is not a plain directory")
    _check_owner_mode(st, "directory", path)


def ensure_private_dir(path: Path) -> None:
    """Create `path` with mode 0700 when missing, then check it."""
    try:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
    except OSError as exc:
        raise SafeFileError(f"cannot create directory {path}: {exc.strerror}") from None
    check_private_dir(path)


def read_private_file(path: Path, what: str, max_bytes: int) -> bytes | None:
    """Contents of `path`, or None when it does not exist. Raises SafeFileError when unsafe."""
    check_private_dir(path.parent)
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
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
    finally:
        if fd >= 0:
            os.close(fd)
    if len(data) > max_bytes:
        raise SafeFileError(f"{what} {path} is larger than {max_bytes} bytes")
    return data


def atomic_write_private(path: Path, data: bytes) -> None:
    """Write `data` to `path` atomically with mode 0600: tmp file + fsync + rename + dir fsync."""
    check_private_dir(path.parent)
    tmp = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
    try:
        fd = os.open(tmp, flags, 0o600)
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
            tmp.replace(path)  # replaces a symlink at `path` itself, never follows it
        except OSError as exc:
            raise SafeFileError(f"cannot write {path}: {exc.strerror}") from None
    finally:
        if fd >= 0:
            os.close(fd)
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass
    dfd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)
