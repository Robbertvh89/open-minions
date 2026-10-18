"""Shared launchd helpers for the `service` and `menubar` LaunchAgents (ADR-0011).

One access path for everything that talks to launchd or vets the agents directory, so tests
fake `run_launchctl` (and the clock) in this module only. `launchctl` is called with an absolute
path, no shell and a timeout.
"""

from __future__ import annotations

import contextlib
import os
import plistlib
import re
import shlex
import stat
import subprocess
import sys
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Annotated

import typer

LAUNCHCTL = "/bin/launchctl"
LAUNCHCTL_TIMEOUT_S = 30.0
BOOTOUT_IN_PROGRESS = 36  # EINPROGRESS: a graceful stop is under way
SERVICE_NOT_FOUND = 113
STOP_POLL_S = 1.0
STOP_WAIT_SLACK_S = 10.0
EXECUTABLE_NAME = "minion-executor"
MENUBAR_LABEL = "com.open-minions.menubar"
_LABEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_now: Callable[[], float] = time.monotonic
_sleep: Callable[[float], None] = time.sleep

LabelOpt = Annotated[str, typer.Option(help="launchd label")]
AgentsDirOpt = Annotated[Path | None, typer.Option(hidden=True)]


def run_launchctl(args: list[str], timeout: float = LAUNCHCTL_TIMEOUT_S) -> tuple[int, str]:
    """(returncode, combined output). OSError/timeout map to (127, message)."""
    try:
        out = subprocess.run(  # noqa: S603 - fixed binary, no shell
            [LAUNCHCTL, *args],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return 127, f"launchctl failed: {exc}"
    return out.returncode, (out.stdout + out.stderr).strip()


def is_darwin() -> bool:
    return sys.platform == "darwin"


def uid() -> int:
    return os.getuid()


def fail(message: str) -> typer.Exit:
    typer.echo(f"error: {message}", err=True)
    return typer.Exit(code=2)


def executable() -> str:
    """Absolute path of the `minion-executor` script in this install's own environment (e.g. the
    `uv tool` venv's bin/), not the `~/.local/bin` shim. It survives `uv tool upgrade`; after an
    uninstall and reinstall of the tool, re-run `service install`."""
    sibling = Path(sys.executable).parent / EXECUTABLE_NAME
    if sibling.is_file():
        return str(sibling.absolute())
    raise ValueError("cannot find the minion-executor script next to the Python interpreter")


def agents_dir(override: Path | None) -> Path:
    return override or Path.home() / "Library" / "LaunchAgents"


def check_label(label: str) -> None:
    if not _LABEL_RE.fullmatch(label):
        raise ValueError(f"invalid label {label!r}")


def refuse_root() -> None:
    """Installing or removing an agent as root would act on root's own launchd domain."""
    if os.geteuid() == 0:
        raise ValueError("refusing to run as root; run as the user who owns the agent")


ADMIN_GID = 80  # macOS "admin" group


def _check_owned_not_writable(path: Path) -> None:
    try:
        st = path.lstat()
    except OSError:
        return  # missing: nothing to vet
    if stat.S_ISLNK(st.st_mode):
        return  # the resolved chain is vetted separately
    if st.st_uid not in (os.geteuid(), 0):
        raise ValueError(f"{path} is owned by another user; refusing")
    if st.st_mode & 0o002:
        raise ValueError(f"{path} is writable by others (or sticky/shared); refusing")
    # Group write is tolerated only for the macOS admin group: its members can sudo, so they
    # gain nothing new. Stock Homebrew leaves /opt/homebrew/Cellar admin-group writable.
    if st.st_mode & 0o020 and st.st_gid != ADMIN_GID:
        raise ValueError(f"{path} is writable by a non-admin group; refusing")


def check_install_chain(exe: str) -> None:
    """The launchd job runs `exe` with `sys.executable` and `sys.prefix` in your name: every
    one of them and every ancestor (as written and symlink-resolved) must be owned by you or root
    and not writable by others or by a group other than admin. Sticky world-writable directories
    such as /tmp are refused."""
    for raw in (exe, sys.executable, sys.prefix):
        candidates = {Path(os.path.abspath(raw))}  # noqa: PTH100 - normalise ".." only
        with contextlib.suppress(OSError, RuntimeError):
            candidates.add(Path(raw).resolve())
        for cand in sorted(candidates):
            for p in (cand, *cand.parents):
                _check_owned_not_writable(p)


def check_agents_dir(path: Path, *, create: bool) -> None:
    """A real directory (not a symlink) we own, not writable by group or others."""
    if create:
        path.mkdir(mode=0o755, parents=True, exist_ok=True)
    try:
        st = path.lstat()
    except FileNotFoundError:
        if create:
            raise
        return
    if not stat.S_ISDIR(st.st_mode):
        raise ValueError(f"{path} is not a real directory (symlink?); refusing")
    if st.st_uid != os.getuid():
        raise ValueError(f"{path} is not owned by you; refusing")
    if st.st_mode & 0o022:
        raise ValueError(f"{path} is writable by group or others; refusing")


def check_plist_file(path: Path) -> None:
    """An existing plist must be a regular, non-symlink file of ours, not group/other writable."""
    st = path.lstat()
    if not stat.S_ISREG(st.st_mode):
        raise ValueError(f"{path} is not a regular file (symlink?); refusing")
    if st.st_uid != os.getuid():
        raise ValueError(f"{path} is not owned by you; refusing")
    if st.st_mode & 0o022:
        raise ValueError(f"{path} is writable by group or others; refusing")


def read_plist(path: Path) -> dict[str, object] | None:
    """The installed plist as a dict, or None when absent or unreadable."""
    try:
        with path.open("rb") as f:
            doc = plistlib.load(f)
    except (OSError, plistlib.InvalidFileException, ValueError):
        return None
    return doc if isinstance(doc, dict) else None


def write_plist_safely(path: Path, data: bytes) -> None:
    """Create `path` 0644, refusing an existing file or symlink (O_EXCL | O_NOFOLLOW)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
    try:
        os.fchmod(fd, 0o644)  # umask-proof
        with os.fdopen(fd, "wb") as f:
            fd = -1
            f.write(data)
    except BaseException:
        if fd >= 0:
            os.close(fd)
        with contextlib.suppress(OSError):
            path.unlink()
        raise


def is_our_program(doc: Mapping[str, object] | None, subcommand: str) -> bool:
    """ProgramArguments[0] is exactly a `minion-executor` and [1] the expected subcommand."""
    args = doc.get("ProgramArguments") if doc else None
    return (
        isinstance(args, list)
        and len(args) >= 2
        and isinstance(args[0], str)
        and Path(args[0]).name == EXECUTABLE_NAME
        and args[1] == subcommand
    )


def wait_unloaded(label: str, timeout_s: float) -> bool:
    """Poll `launchctl print` until the job is gone (rc 113) or `timeout_s` passes."""
    deadline = _now() + timeout_s
    while True:
        rc, _ = run_launchctl(["print", f"gui/{uid()}/{label}"])
        if rc == SERVICE_NOT_FOUND:
            return True
        if _now() >= deadline:
            return False
        _sleep(STOP_POLL_S)


def manual_removal(label: str, plist_path: Path) -> str:
    rm = shlex.quote(str(plist_path))
    return f"remove it by hand: `launchctl bootout gui/$(id -u)/{label}` then `rm {rm}`"


def uninstall_agent(
    *,
    label: str,
    plist_dir: Path,
    is_ours: Callable[[Mapping[str, object] | None], bool],
    wait_s: Callable[[Mapping[str, object]], float],
    kind: str,
    bootout_timeout: float = LAUNCHCTL_TIMEOUT_S,
) -> None:
    """Stop the agent `label` and remove its plist; shared by `service` and `menubar`.

    Idempotent. Refuses a plist that is not ours (`is_ours`) or whose Label differs from `label`.
    The plist is kept whenever the job might still be running. `wait_s(doc)` is how long to wait
    for the job to exit after `bootout` answered.
    """
    plist_path = plist_dir / f"{label}.plist"
    doc = read_plist(plist_path)
    if doc is None:
        # Absent or unreadable. Only "not loaded and no file" is a clean nothing-to-do: a loaded
        # job, a launchd we cannot query, or a file we cannot parse must not be silently left.
        rc, _ = run_launchctl(["print", f"gui/{uid()}/{label}"])
        present = plist_path.is_symlink() or plist_path.exists()
        if rc != SERVICE_NOT_FOUND or present:
            state = (
                "loaded"
                if rc == 0
                else "not loaded"
                if rc == SERVICE_NOT_FOUND
                else "not confirmed unloaded"
            )
            file_state = "unreadable or malformed" if present else "missing"
            raise fail(
                f"{plist_path} is {file_state} and the {kind} is {state}; "
                + manual_removal(label, plist_path)
            )
        typer.echo(f"{plist_path} not present and {label} not loaded; nothing to remove")
        return
    if not is_ours(doc) or doc.get("Label") != label:
        raise fail(f"{plist_path} is not a minion-executor {kind} plist; refusing to touch it")
    started = _now()
    rc, out = run_launchctl(["bootout", f"gui/{uid()}/{label}"], timeout=bootout_timeout)
    # rc 0 and 36 (EINPROGRESS) both mean "stop under way": launchd answers before the job has
    # exited (measured: rc 0 after 0.6 s, process gone ~5 s later). Wait until it is unloaded.
    if rc in (0, BOOTOUT_IN_PROGRESS):
        if not wait_unloaded(label, wait_s(doc)):
            raise fail(
                f"{kind} is still stopping after the exit timeout; plist left in place. "
                + manual_removal(label, plist_path)
            )
        typer.echo(f"stopped {label} in {_now() - started:.1f} s")
    # 3 = not loaded (ESRCH), 113 = could not find service: fine, we still remove the plist
    elif rc not in (3, SERVICE_NOT_FOUND):
        raise fail(f"launchctl bootout failed ({rc}): {out}; plist left in place")
    try:
        plist_path.unlink()
        typer.echo(f"removed {plist_path}")
    except FileNotFoundError:
        typer.echo(f"{plist_path} not present")
    except OSError as exc:
        raise fail(f"cannot remove {plist_path}: {exc}") from None
