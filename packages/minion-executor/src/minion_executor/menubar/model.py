"""Pure view model of the menu bar (no AppKit; runs on any OS).

`build_view` turns the status file (or why it is unusable) into the lines and flags the menu
shows; `control_kwargs` turns a menu action into `update_control` keyword arguments. Every string
that came from the file passes through `safe_text`.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import Path

from minion_common.enums import CapacityProfile, PausedReason

from minion_executor.capacity.control import default_state_dir
from minion_executor.capacity.governor import ProfileSource
from minion_executor.capacity.status import StatusFile, render, running, safe_text

POLL_INTERVAL_S = 5.0
TITLE = "Minions"
AUTOMATIC = "Automatic"


def resolve_state_dir(environ: Mapping[str, str] | None = None) -> Path:
    """`MINION_EXECUTOR_STATE_DIR` (absolute, `~` expanded) or the default; same as the CLI."""
    raw = (os.environ if environ is None else environ).get("MINION_EXECUTOR_STATE_DIR", "").strip()
    if not raw:
        return default_state_dir()
    path = Path(raw).expanduser()
    if not path.is_absolute():
        raise ValueError("MINION_EXECUTOR_STATE_DIR must be an absolute path")
    return path


class Kind(StrEnum):
    PAUSE = "pause"
    RESUME = "resume"
    PROFILE = "profile"


@dataclass(frozen=True)
class Action:
    """One owner action. PAUSE with `duration=None` = until resume; PROFILE with None = auto."""

    kind: Kind
    duration: timedelta | None = None
    profile: CapacityProfile | None = None


PAUSE_CHOICES: tuple[tuple[str, Action], ...] = (
    ("30 minutes", Action(Kind.PAUSE, duration=timedelta(minutes=30))),
    ("1 hour", Action(Kind.PAUSE, duration=timedelta(hours=1))),
    ("2 hours", Action(Kind.PAUSE, duration=timedelta(hours=2))),
    ("Until I resume", Action(Kind.PAUSE)),
)
RESUME = Action(Kind.RESUME)
PROFILE_CHOICES: tuple[tuple[str, Action], ...] = (
    (AUTOMATIC, Action(Kind.PROFILE)),
    *((p.value.capitalize(), Action(Kind.PROFILE, profile=p)) for p in CapacityProfile),
)


def control_kwargs(action: Action) -> dict[str, object]:
    """Keyword arguments for `update_control` (besides `warn`)."""
    if action.kind is Kind.PAUSE:
        return {"paused": True, "pause_for": action.duration}
    if action.kind is Kind.RESUME:
        return {"paused": False}
    if action.profile is None:
        return {"clear_profile": True}
    return {"profile": action.profile}


class State(StrEnum):
    NO_STATUS = "no_status"  # nothing written yet
    NOT_RUNNING = "not_running"  # stopped, stale or pid gone
    UNREADABLE = "unreadable"  # unsafe or invalid file
    IDLE = "idle"
    WORKING = "working"
    PAUSED = "paused"


@dataclass(frozen=True)
class View:
    state: State
    title: str
    lines: tuple[str, ...]
    can_pause: bool
    can_resume: bool
    profile_checked: str | None  # a label of PROFILE_CHOICES; None when unknown


def _title(suffix: str = "") -> str:
    return f"{TITLE} ({suffix})" if suffix else TITLE


def _plain(state: State, suffix: str, line: str) -> View:
    # control.json can still be written while nothing runs: the next start honours it
    return View(
        state,
        _title(suffix),
        (safe_text(line)[:300],),
        can_pause=True,
        can_resume=True,
        profile_checked=None,
    )


def build_view(
    st: StatusFile | None, error: str | None = None, now: datetime | None = None
) -> View:
    """View for the current status. `error` = why the status file could not be read."""
    if error is not None:
        return _plain(State.UNREADABLE, "?", f"Status unavailable: {error}")
    if st is None:
        return _plain(State.NO_STATUS, "off", "No executor is running")
    alive, why = running(st, now)
    if not alive:
        return _plain(State.NOT_RUNNING, "off", f"No executor is running: {why}")
    # an unknown enum value (newer executor) is a str subclass without a member; never raise on it
    checked = (
        safe_text(str(st.profile.value)).capitalize()
        if st.profile_source is ProfileSource.OWNER
        else AUTOMATIC
    )
    reason = str(st.paused_reason.value) if st.paused_reason is not None else None
    if reason is not None:
        state, suffix = State.PAUSED, "paused"
    elif st.subtask is not None:
        state, suffix = State.WORKING, "working"
    else:
        state, suffix = State.IDLE, ""
    owner_paused = reason in (PausedReason.USER_PAUSED.value, PausedReason.CONTROL_INVALID.value)
    return View(
        state,
        _title(suffix),
        tuple(render(st, now).split("\n")),
        can_pause=reason != PausedReason.USER_PAUSED.value,
        can_resume=owner_paused,
        profile_checked=checked,
    )
