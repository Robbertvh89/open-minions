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
    State,
    build_view,
    control_kwargs,
    resolve_state_dir,
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


def test_resolve_state_dir() -> None:
    assert resolve_state_dir({"MINION_EXECUTOR_STATE_DIR": "/var/x"}) == Path("/var/x")
    assert resolve_state_dir({}).name == "open-minions"
    with pytest.raises(ValueError, match="absolute"):
        resolve_state_dir({"MINION_EXECUTOR_STATE_DIR": "rel/dir"})
