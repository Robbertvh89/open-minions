"""Menu-bar view model: pure, runs on any OS."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from minion_common.enums import CapacityProfile as P
from minion_common.enums import PausedReason
from minion_common.schemas import Budget
from minion_executor.capacity.control import ControlUnusable, OwnerControl
from minion_executor.capacity.safefile import SafeFileError, ensure_private_dir
from minion_executor.capacity.status import (
    STATUS_FILE,
    StatusFile,
    StatusSubtask,
    StatusToday,
    read_status,
)
from minion_executor.menubar.model import (
    AUTOMATIC,
    PAUSE_CHOICES,
    PROFILE_CHOICES,
    RESUME,
    Action,
    Kind,
    State,
    action_error,
    available_profiles,
    build_view,
    control_kwargs,
    profile_choices,
)

NOW = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)
BUDGET = Budget.model_validate(
    {
        "profile": "light",
        "memory_gb": 8,
        "gpu_duty_cycle": 0.3,
        "max_batch_size": 4,
        "cpu_priority": "background",
    }
)


def status(**kw: object) -> StatusFile:
    base: dict[str, object] = {
        "pid": os.getpid(),
        "started_at": NOW - timedelta(hours=1),
        "updated_at": NOW,
        "profile": P.LIGHT,
        "profile_source": "active",
        "budget": BUDGET,
        "held_below_nominal": False,
        "today": StatusToday(date="2026-10-14", subtasks=3, records=120),
    }
    base.update(kw)
    return StatusFile(**base)


def test_no_status_and_unreadable() -> None:
    v = build_view(None, now=NOW)
    assert v.state is State.NO_STATUS
    assert v.can_pause and v.can_resume
    bad = build_view(None, error="status file is group-writable\x1b[31m", now=NOW)
    assert bad.state is State.UNREADABLE
    assert "\x1b" not in bad.lines[0]


def test_idle_working_paused() -> None:
    idle = build_view(status(), now=NOW)
    assert idle.state is State.IDLE and idle.title == "Minions"
    assert idle.profile_checked == AUTOMATIC
    assert idle.can_pause and not idle.can_resume
    st = StatusSubtask(subtask_id=7, attempt=1, records_done=1, records_total=4)
    working = build_view(status(subtask=st), now=NOW)
    assert working.state is State.WORKING
    assert any("subtask 7" in line for line in working.lines)


def test_paused_reasons() -> None:
    user = build_view(status(paused_reason=PausedReason.USER_PAUSED), now=NOW)
    assert user.state is State.PAUSED and "paused" in user.title
    assert not user.can_pause and user.can_resume
    thermal = build_view(status(paused_reason=PausedReason.THERMAL), now=NOW)
    assert thermal.can_pause and not thermal.can_resume
    invalid = build_view(status(paused_reason=PausedReason.CONTROL_INVALID), now=NOW)
    assert invalid.state is State.PAUSED and invalid.can_resume  # resume rewrites control.json


def test_owner_pinned_profile_is_checked() -> None:
    v = build_view(status(profile=P.FULL, profile_source="owner"), now=NOW)
    assert v.profile_checked == "Full"
    assert v.profile_checked in {label for label, _ in PROFILE_CHOICES}


def test_stale_stopped_and_dead_pid() -> None:
    stale = build_view(status(updated_at=NOW - timedelta(minutes=5)), now=NOW)
    assert stale.state is State.NOT_RUNNING and "stale" in stale.lines[0]
    stopped = build_view(status(stopped=True), now=NOW)
    assert stopped.state is State.NOT_RUNNING and "stopped" in stopped.lines[0]
    gone = build_view(status(pid=2**22 + 12345), now=NOW)
    assert gone.state is State.NOT_RUNNING


def test_unknown_enums_from_a_newer_executor(tmp_path: Path) -> None:
    sd = tmp_path / "s"
    ensure_private_dir(sd)
    raw = json.loads(status(paused_reason=PausedReason.THERMAL).model_dump_json())
    raw["paused_reason"] = "solar_flare\x1b[2J"
    raw["profile"] = "turbo"
    raw["profile_source"] = "owner"
    raw["future_key"] = 1
    path = sd / STATUS_FILE
    path.write_text(json.dumps(raw))
    path.chmod(0o600)
    v = build_view(read_status(sd), now=NOW)
    assert v.state is State.PAUSED
    assert all("\x1b" not in line for line in v.lines)
    assert v.profile_checked is not None and "\x1b" not in v.profile_checked
    assert not v.can_resume  # an unknown reason is not an owner pause


def test_unsafe_status_file_becomes_error_view(tmp_path: Path) -> None:
    sd = tmp_path / "s"
    ensure_private_dir(sd)
    (sd / STATUS_FILE).write_text("{}")
    (sd / STATUS_FILE).chmod(0o666)
    with pytest.raises(SafeFileError) as exc:
        read_status(sd)
    assert build_view(None, error=str(exc.value)).state is State.UNREADABLE


def test_actions_map_to_update_control_kwargs() -> None:
    kw = [control_kwargs(a) for _, a in PAUSE_CHOICES]
    assert kw[0] == {"paused": True, "pause_for": timedelta(minutes=30)}
    assert kw[1]["pause_for"] == timedelta(hours=1)
    assert kw[2]["pause_for"] == timedelta(hours=2)
    assert kw[3] == {"paused": True, "pause_for": None}
    assert control_kwargs(RESUME) == {"paused": False}
    by_label = {label: control_kwargs(a) for label, a in PROFILE_CHOICES}
    assert by_label[AUTOMATIC] == {"clear_profile": True}
    assert by_label["Light"] == {"profile": P.LIGHT}
    assert set(by_label) == {AUTOMATIC, "Light", "Balanced", "Full", "Custom"}


@pytest.mark.parametrize("raw", ["", "   ", "/var/x", "~/x", "rel/dir"])
@pytest.mark.parametrize("name", ["MINION_EXECUTOR_STATE_DIR", "minion_executor_state_dir"])
def test_state_dir_is_the_same_for_settings_and_menubar(
    raw: str, name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from minion_executor.settings import ExecutorSettings, state_dir_from_env

    monkeypatch.delenv("MINION_EXECUTOR_STATE_DIR", raising=False)
    monkeypatch.setenv(name, raw)
    try:
        expected: Path | str = ExecutorSettings().state_dir
    except ValueError:
        expected = "error"
    try:
        got: Path | str = state_dir_from_env()
    except ValueError:
        got = "error"
    assert got == expected
    if raw == "rel/dir":
        assert got == "error"
    if raw.strip() == "":
        assert isinstance(got, Path) and got.name == "open-minions"


def test_only_configured_profiles_are_offered(tmp_path: Path) -> None:
    cfg = tmp_path / "executor.toml"
    cfg.write_text("")
    cfg.chmod(0o600)
    avail = available_profiles(cfg)
    assert P.CUSTOM not in avail and {P.LIGHT, P.BALANCED, P.FULL} <= avail
    labels = [label for label, _ in profile_choices(avail)]
    assert labels == [AUTOMATIC, "Light", "Balanced", "Full"]
    cfg.write_text(
        "[profiles.custom]\ngpu_duty_cycle = 0.5\nmax_batch_size = 4\n"
        'cpu_priority = "background"\nmemory_gb = 4\n'
    )
    avail = available_profiles(cfg)
    assert P.CUSTOM in avail
    assert "Custom" in [label for label, _ in profile_choices(avail)]
    # an unloadable config offers the built-ins only
    cfg.write_text("not toml [")
    assert P.CUSTOM not in available_profiles(cfg)


def test_action_error_rejects_unconfigured_profile() -> None:
    custom = Action(Kind.PROFILE, profile=P.CUSTOM)
    assert action_error(custom, {P.LIGHT}) is not None
    assert action_error(custom, {P.CUSTOM}) is None
    assert action_error(Action(Kind.PROFILE), set()) is None
    assert action_error(RESUME, set()) is None


def test_age_line_is_not_part_of_view_equality() -> None:
    a = build_view(status(), now=NOW)
    b = build_view(status(), now=NOW + timedelta(seconds=30))
    assert a.updated is not None and b.updated is not None and a.updated != b.updated
    assert a == b
    assert all(not line.startswith("updated:") for line in a.lines)


def test_owner_pause_and_pin_shown_when_no_executor_runs() -> None:
    until = NOW + timedelta(hours=1)
    ctl = OwnerControl(paused=True, paused_until=until, profile=P.FULL)
    v = build_view(None, now=NOW, control=ctl)
    assert v.state is State.NO_STATUS
    assert f"Paused by you until {until.astimezone().strftime('%H:%M')}" in v.lines
    assert v.profile_checked == "Full" and not v.can_pause and v.can_resume
    forever = build_view(None, now=NOW, control=OwnerControl(paused=True))
    assert "Paused by you until you resume" in forever.lines
    expired = build_view(None, now=until + timedelta(minutes=1), control=ctl)
    assert len(expired.lines) == 1 and expired.can_pause and not expired.can_resume
    auto = build_view(None, now=NOW, control=OwnerControl())
    assert auto.profile_checked == AUTOMATIC
    running_paused = build_view(
        status(paused_reason=PausedReason.USER_PAUSED), now=NOW, control=ctl
    )
    assert any(line.startswith("Paused by you until") for line in running_paused.lines)


def test_unusable_control_file_is_shown_when_nothing_runs() -> None:
    view = build_view(None, control=ControlUnusable("not valid JSON"))
    assert "Control file unusable: not valid JSON" in view.lines
    assert view.state.value != "working"
