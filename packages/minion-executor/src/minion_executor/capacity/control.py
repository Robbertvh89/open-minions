"""Owner control (SPEC FR-56, ADR-0006 D11): `control.json` in the state directory.

`minion-executor pause|resume|profile` write it atomically (0600); the governor polls it with the
signals. A file that is present but not usable (not ours, writable by others, oversized, not
regular, unreadable, bad schema, newer version) is reported as `ControlUnusable`: the governor
fails closed (paused, `control_invalid`) rather than guessing the owner's intent. Nothing here
logs; the reader returns a reason string and the runtime (M4.5) must log it once per change, not on
every poll. A missing file or state directory is simply "absent". No payloads are ever in this file.
"""

from __future__ import annotations

import contextlib
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

from minion_common.enums import CapacityProfile
from pydantic import AwareDatetime, BaseModel, ConfigDict, ValidationError

from minion_executor.capacity.safefile import (
    SafeFileError,
    UnsafeDirError,
    atomic_write_private,
    ensure_private_dir,
    private_lock,
    read_private_file,
)

CONTROL_FILE = "control.json"
CONTROL_LOCK_FILE = "control.lock"
MAX_CONTROL_BYTES = 4096
MAX_PAUSE = timedelta(days=30)
_DURATION = re.compile(r"^(?:(\d{1,3})d)?(?:(\d{1,4})h)?(?:(\d{1,5})m)?(?:(\d{1,7})s)?$")


class OwnerControl(BaseModel):
    """The owner's standing instructions. `profile=None` means automatic (FR-52)."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    version: Literal[1] = 1
    paused: bool = False
    paused_until: AwareDatetime | None = None
    profile: CapacityProfile | None = None
    written_at: AwareDatetime | None = None

    def is_paused(self, now: datetime) -> bool:
        return self.paused and (self.paused_until is None or now < self.paused_until)


def default_state_dir() -> Path:
    return Path.home() / "Library" / "Application Support" / "open-minions"


def parse_duration(text: str) -> timedelta:
    """`30m`, `1h`, `2h30m`, `1d`, `90s`; positive and at most 30 days."""
    m = _DURATION.fullmatch(text.strip().lower())
    if not text.strip() or m is None:
        raise ValueError(f"invalid duration {text!r}; use e.g. 30m, 1h, 2h30m")
    d, h, mi, s = (int(g) if g else 0 for g in m.groups())
    delta = timedelta(days=d, hours=h, minutes=mi, seconds=s)
    if delta <= timedelta(0):
        raise ValueError("duration must be positive")
    if delta > MAX_PAUSE:
        raise ValueError("duration must be at most 30 days")
    return delta


@dataclass(frozen=True)
class ControlUnusable:
    """control.json exists but cannot be trusted. `reason` carries no file contents."""

    reason: str
    dir_unsafe: bool = False  # the state directory itself is the problem, not control.json


def read_control(state_dir: Path) -> OwnerControl | ControlUnusable | None:
    """Tri-state: None (absent), a valid `OwnerControl`, or `ControlUnusable` (fail closed)."""
    path = state_dir / CONTROL_FILE
    try:
        data = read_private_file(path, "control file", MAX_CONTROL_BYTES)
        if data is None:
            return None
        return OwnerControl.model_validate_json(data)
    except SafeFileError as exc:
        return ControlUnusable(str(exc), dir_unsafe=isinstance(exc, UnsafeDirError))
    except ValidationError as exc:
        # loc + message only: pydantic's default text echoes the offending input
        detail = "; ".join(
            f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors()
        )
        return ControlUnusable(f"invalid schema: {detail}")


def write_control(state_dir: Path, ctl: OwnerControl) -> bool:
    """Atomically replace control.json (creates the state dir 0700). Raises SafeFileError.

    Returns False when the file is in place but not confirmed durable (directory fsync failed).
    """
    ensure_private_dir(state_dir)
    stamped = ctl.model_copy(update={"written_at": datetime.now(UTC)})
    return atomic_write_private(state_dir / CONTROL_FILE, stamped.model_dump_json().encode("utf-8"))


def update_control(
    state_dir: Path,
    *,
    paused: bool | None = None,
    pause_for: timedelta | None = None,
    profile: CapacityProfile | None = None,
    clear_profile: bool = False,
    now: datetime | None = None,
    warn: Callable[[str], None],
) -> OwnerControl:
    """Read-modify-write: change only the given fields and keep the rest of the owner's state.

    An unusable existing file never silently drops a pause: unless the call sets `paused`
    explicitly, the result is paused (no end time) and `warn` is told why. `resume` is the
    explicit way out.
    """
    # one writer at a time (CLI and menu bar): an unsafe directory is reported below as before
    try:
        ensure_private_dir(state_dir)
        lock: contextlib.AbstractContextManager[None] = private_lock(state_dir, CONTROL_LOCK_FILE)
    except UnsafeDirError:
        lock = contextlib.nullcontext()
    with lock:
        return _update_locked(
            state_dir,
            paused=paused,
            pause_for=pause_for,
            profile=profile,
            clear_profile=clear_profile,
            now=now,
            warn=warn,
        )


def _update_locked(
    state_dir: Path,
    *,
    paused: bool | None,
    pause_for: timedelta | None,
    profile: CapacityProfile | None,
    clear_profile: bool,
    now: datetime | None,
    warn: Callable[[str], None],
) -> OwnerControl:
    current = read_control(state_dir)
    if isinstance(current, ControlUnusable):
        if paused is None:
            fix = (
                "fix the state directory permissions (chmod 700 / chown), then retry"
                if current.dir_unsafe
                else "the pause is kept, run `minion-executor resume` to lift it"
            )
            warn(f"existing control file was unreadable ({current.reason}); {fix}")
        current = OwnerControl(paused=True)
    fields = (current or OwnerControl()).model_dump()
    if paused is not None:
        now = now or datetime.now(UTC)
        fields["paused"] = paused
        fields["paused_until"] = (now + pause_for) if (paused and pause_for) else None
    if clear_profile:
        fields["profile"] = None
    elif profile is not None:
        fields["profile"] = profile
    new = OwnerControl.model_validate(fields)
    if not write_control(state_dir, new):
        warn(f"written but not durably synced: {state_dir / CONTROL_FILE}")
    return new
