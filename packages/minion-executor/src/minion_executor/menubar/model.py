"""Pure view model of the menu bar (no AppKit; runs on any OS).

`build_view` turns the status file (or why it is unusable) into the lines and flags the menu
shows; `control_kwargs` turns a menu action into `update_control` keyword arguments. Every string
that came from the file passes through `safe_text`.
"""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path

from minion_common.enums import CapacityProfile, PausedReason

from minion_executor.capacity.config import ConfigError, load_config, resolve_profiles
from minion_executor.capacity.control import ControlUnusable, OwnerControl
from minion_executor.capacity.governor import ProfileSource
from minion_executor.capacity.status import StatusFile, render, running, safe_text

POLL_INTERVAL_S = 5.0
TITLE = "Minions"
AUTOMATIC = "Automatic"
BUILTIN_PROFILES: frozenset[CapacityProfile] = frozenset(
    {CapacityProfile.LIGHT, CapacityProfile.BALANCED, CapacityProfile.FULL}
)


def available_profiles(config_path: Path | None) -> frozenset[CapacityProfile]:
    """Profiles the owner may pin: the built-ins, plus Custom only when executor.toml defines it.

    A config that cannot be loaded offers the built-ins only (the executor will report the
    config error itself).
    """
    try:
        return frozenset(resolve_profiles(load_config(config_path)))
    except (ConfigError, OSError, ValueError):
        return BUILTIN_PROFILES


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


def profile_choices(available: Collection[CapacityProfile]) -> tuple[tuple[str, Action], ...]:
    """PROFILE_CHOICES limited to Automatic and the profiles in `available`."""
    return tuple(
        (label, a) for label, a in PROFILE_CHOICES if a.profile is None or a.profile in available
    )


def action_error(action: Action, available: Collection[CapacityProfile]) -> str | None:
    """Why `action` must not be performed, or None."""
    profile = action.profile if action.kind is Kind.PROFILE else None
    if profile is not None and profile not in available:
        return f"profile {profile.value!r} is not configured (define it in executor.toml first)"
    return None


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
    # The "updated: 3s ago" line changes on every write; it must not force a menu rebuild
    updated: str | None = field(default=None, compare=False)


def _title(suffix: str = "") -> str:
    return f"{TITLE} ({suffix})" if suffix else TITLE


def _until(ctl: OwnerControl, now: datetime) -> str:
    if ctl.paused_until is None:
        return "Paused by you until you resume"
    return f"Paused by you until {ctl.paused_until.astimezone().strftime('%H:%M')}"


def _owner_paused(ctl: OwnerControl | ControlUnusable | None, now: datetime) -> bool | None:
    """True/False for a readable control file, None when absent or unusable."""
    return ctl.is_paused(now) if isinstance(ctl, OwnerControl) else None


def _plain(
    state: State, suffix: str, line: str, ctl: OwnerControl | ControlUnusable | None, now: datetime
) -> View:
    # control.json can still be written while nothing runs: the next start honours it
    lines = [safe_text(line)[:300]]
    paused = _owner_paused(ctl, now)
    can_pause, can_resume = True, True
    checked: str | None = None
    if isinstance(ctl, OwnerControl):
        if paused:
            lines.append(_until(ctl, now))
        can_pause, can_resume = not paused, bool(paused)
        checked = ctl.profile.value.capitalize() if ctl.profile is not None else AUTOMATIC
    elif ctl is None:
        checked = AUTOMATIC
    return View(state, _title(suffix), tuple(lines), can_pause, can_resume, profile_checked=checked)


def build_view(
    st: StatusFile | None,
    error: str | None = None,
    now: datetime | None = None,
    control: OwnerControl | ControlUnusable | None = None,
) -> View:
    """View for the current status. `error` = why the status file could not be read.

    `control` (the owner's control.json) is shown when nothing runs: a pause and a pinned
    profile are honoured by the next start.
    """
    now = now or datetime.now(UTC)
    if error is not None:
        return _plain(State.UNREADABLE, "?", f"Status unavailable: {error}", control, now)
    if st is None:
        return _plain(State.NO_STATUS, "off", "No executor is running", control, now)
    alive, why = running(st, now)
    if not alive:
        return _plain(State.NOT_RUNNING, "off", f"No executor is running: {why}", control, now)
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
    lines = render(st, now).split("\n")
    updated = lines.pop() if lines and lines[-1].startswith("updated:") else None
    if (
        reason == PausedReason.USER_PAUSED.value
        and isinstance(control, OwnerControl)
        and control.is_paused(now)
    ):
        lines.append(_until(control, now))
    return View(
        state,
        _title(suffix),
        tuple(lines),
        can_pause=reason != PausedReason.USER_PAUSED.value,
        can_resume=owner_paused,
        profile_checked=checked,
        updated=updated,
    )
