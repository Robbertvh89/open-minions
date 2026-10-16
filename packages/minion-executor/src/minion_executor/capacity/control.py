"""Owner control (SPEC FR-56, ADR-0006 D11): `control.json` in the state directory.

`minion-executor pause|resume|profile` write it atomically (0600); the governor polls it with the
signals. Files that are not ours, are writable by others or fail the strict schema are ignored with
a warning and never crash the loop. No payloads are ever in this file.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

import structlog
from minion_common.enums import CapacityProfile
from pydantic import AwareDatetime, BaseModel, ConfigDict, ValidationError

from minion_executor.capacity.safefile import (
    SafeFileError,
    atomic_write_private,
    ensure_private_dir,
    read_private_file,
)

log = structlog.get_logger()

CONTROL_FILE = "control.json"
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


def read_control(state_dir: Path) -> OwnerControl | None:
    """The current owner control, or None when absent, unsafe or invalid (warning logged)."""
    path = state_dir / CONTROL_FILE
    try:
        data = read_private_file(path, "control file", MAX_CONTROL_BYTES)
        if data is None:
            return None
        return OwnerControl.model_validate_json(data)
    except SafeFileError as exc:
        log.warning("control_file_ignored", reason=str(exc))
    except ValidationError as exc:
        # loc + message only: pydantic's default text echoes the offending input
        detail = "; ".join(
            f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors()
        )
        log.warning("control_file_ignored", reason=f"invalid schema: {detail}")
    return None


def write_control(state_dir: Path, ctl: OwnerControl) -> None:
    """Atomically replace control.json (creates the state dir 0700). Raises SafeFileError."""
    ensure_private_dir(state_dir)
    stamped = ctl.model_copy(update={"written_at": datetime.now(UTC)})
    atomic_write_private(state_dir / CONTROL_FILE, stamped.model_dump_json().encode("utf-8"))


def update_control(
    state_dir: Path,
    *,
    paused: bool | None = None,
    pause_for: timedelta | None = None,
    profile: CapacityProfile | None = None,
    clear_profile: bool = False,
    now: datetime | None = None,
) -> OwnerControl:
    """Read-modify-write: change only the given fields and keep the rest of the owner's state."""
    current = read_control(state_dir) or OwnerControl()
    fields = current.model_dump()
    if paused is not None:
        now = now or datetime.now(UTC)
        fields["paused"] = paused
        fields["paused_until"] = (now + pause_for) if (paused and pause_for) else None
    if clear_profile:
        fields["profile"] = None
    elif profile is not None:
        fields["profile"] = profile
    new = OwnerControl.model_validate(fields)
    write_control(state_dir, new)
    return new
