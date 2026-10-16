"""status.json: schema, safe file handling, staleness, rendering, the CLI and the loop's writer."""

from __future__ import annotations

import os
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from minion_common.enums import CapacityProfile as P
from minion_common.enums import PausedReason
from minion_common.schemas import Budget
from minion_executor.capacity.safefile import SafeFileError, ensure_private_dir
from minion_executor.capacity.status import (
    STALE_AFTER_S,
    STATUS_FILE,
    StatusFile,
    StatusSignals,
    StatusSubtask,
    StatusToday,
    read_status,
    render,
    running,
    write_status,
)
from minion_executor.main import app
from typer.testing import CliRunner

runner = CliRunner()
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


@pytest.fixture
def state_dir(tmp_path: Path) -> Path:
    d = tmp_path / "state"
    ensure_private_dir(d)
    return d


def test_roundtrip_is_atomic_and_private(state_dir: Path) -> None:
    st = status(
        subtask=StatusSubtask(subtask_id=7, attempt=2, records_done=5, records_total=9),
        signals=StatusSignals(
            on_ac=True,
            battery_percent=88,
            low_power_mode=False,
            thermal="nominal",
            memory_pressure="normal",
            hid_idle_s=12.0,
            taken_at=NOW,
        ),
    )
    write_status(state_dir, st)
    path = state_dir / STATUS_FILE
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert read_status(state_dir) == st
    assert [p.name for p in state_dir.iterdir()] == [STATUS_FILE]  # no temp file left behind


def test_absent_is_none_and_garbage_or_unsafe_is_an_error(state_dir: Path) -> None:
    assert read_status(state_dir) is None
    path = state_dir / STATUS_FILE
    path.write_text('{"version": 1, "pid": "VALUE-LEAK"}')
    path.chmod(0o600)
    with pytest.raises(SafeFileError) as exc:
        read_status(state_dir)
    assert "VALUE-LEAK" not in str(exc.value)  # the offending value is not echoed
    path.chmod(0o666)
    with pytest.raises(SafeFileError):
        read_status(state_dir)


def test_running_detection() -> None:
    st = status()
    assert running(st, NOW + timedelta(seconds=5)) == (True, "")
    ok, why = running(st, NOW + timedelta(seconds=STALE_AFTER_S + 5))
    assert not ok and "stale" in why
    ok, why = running(status(stopped=True), NOW)
    assert not ok and "stopped" in why
    ok, why = running(status(pid=2**22 + 12345), NOW)  # no such process
    assert not ok and "gone" in why


def test_render_shows_the_fields_and_no_payload() -> None:
    st = status(
        paused_reason=PausedReason.THERMAL.value,
        held_below_nominal=True,
        candidate=P.FULL,
        candidate_since=NOW - timedelta(seconds=20),
        gpu_share=0.31,
        subtask=StatusSubtask(subtask_id=7, attempt=1, records_done=2, records_total=9),
    )
    out = render(st, NOW)
    for needle in (
        "light",
        "held below nominal",
        "full",
        "paused (thermal)",
        "8 GB",
        "30%",
        "31%",
        "3 subtasks, 120 records",
    ):
        assert needle in out, needle
    running_out = render(status(subtask=st.subtask), NOW)
    assert "running subtask 7 attempt 1 (2/9 records)" in running_out
    assert "idle" in render(status(), NOW)


def test_cli_status_exit_codes(state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_EXECUTOR_STATE_DIR", str(state_dir))
    r = runner.invoke(app, ["status"])
    assert r.exit_code == 1 and "no executor is running" in r.output

    write_status(state_dir, status(updated_at=datetime.now(UTC)))
    r = runner.invoke(app, ["status"])
    assert r.exit_code == 0 and "profile:   light" in r.output

    write_status(state_dir, status(updated_at=datetime.now(UTC) - timedelta(minutes=5)))
    r = runner.invoke(app, ["status"])
    assert r.exit_code == 1 and "stale" in r.output and "profile:" in r.output  # last known info

    write_status(state_dir, status(updated_at=datetime.now(UTC), stopped=True))
    assert runner.invoke(app, ["status"]).exit_code == 1


def test_cli_status_unsafe_state_dir_exits_2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    d = tmp_path / "open"
    d.mkdir()
    d.chmod(0o777)
    (d / STATUS_FILE).write_text(status().model_dump_json())
    monkeypatch.setenv("MINION_EXECUTOR_STATE_DIR", str(d))
    r = runner.invoke(app, ["status"])
    assert r.exit_code == 2 and "writable" in r.output
