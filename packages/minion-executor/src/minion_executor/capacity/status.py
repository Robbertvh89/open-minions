"""Executor status (SPEC FR-56, ADR-0006 D11): `status.json` in the state directory.

The running executor rewrites it atomically (0600) on every governor decision change, whenever the
current subtask changes and every `STATUS_INTERVAL_S`; `minion-executor status` reads and prints it.
The file holds ids and numbers only: never a payload, a prompt, a URL or a token (S-5). A file
nobody has refreshed for `STALE_AFTER_S` (or whose pid is gone, or that says `stopped`) means "no
executor is running".
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from minion_common.enums import CapacityProfile
from minion_common.schemas import Budget
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError

from minion_executor.capacity.safefile import (
    SafeFileError,
    atomic_write_private,
    read_private_file,
)

STATUS_FILE = "status.json"
MAX_STATUS_BYTES = 16384
STATUS_INTERVAL_S = 10.0
STALE_AFTER_S = 3 * STATUS_INTERVAL_S


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class StatusSubtask(_Model):
    subtask_id: int = Field(ge=0)
    attempt: int = Field(ge=1)
    records_done: int = Field(ge=0)
    records_total: int = Field(ge=0)


class StatusSignals(_Model):
    """Coarse host state. No identifiers (the IOKit battery serial never reaches this far)."""

    on_ac: bool
    battery_percent: int | None
    low_power_mode: bool
    thermal: str
    memory_pressure: str
    hid_idle_s: float
    taken_at: AwareDatetime


class StatusToday(_Model):
    """Contribution of this executor process on `date` (local); resets at local midnight."""

    date: str
    subtasks: int = Field(ge=0)
    records: int = Field(ge=0)


class StatusFile(_Model):
    version: Literal[1] = 1
    pid: int
    started_at: AwareDatetime
    updated_at: AwareDatetime
    stopped: bool = False
    profile: CapacityProfile
    profile_source: str
    budget: Budget
    held_below_nominal: bool
    candidate: CapacityProfile | None = None
    candidate_since: AwareDatetime | None = None
    paused_reason: str | None = None
    subtask: StatusSubtask | None = None
    gpu_share: float | None = None
    signals: StatusSignals | None = None
    today: StatusToday


def write_status(state_dir: Path, status: StatusFile) -> bool:
    """Atomically replace status.json (0600). Raises SafeFileError; False = not durably synced."""
    return atomic_write_private(state_dir / STATUS_FILE, status.model_dump_json().encode("utf-8"))


def read_status(state_dir: Path) -> StatusFile | None:
    """The status file, or None when absent. Raises SafeFileError when unsafe or invalid."""
    data = read_private_file(state_dir / STATUS_FILE, "status file", MAX_STATUS_BYTES)
    if data is None:
        return None
    try:
        return StatusFile.model_validate_json(data)
    except ValidationError as exc:
        # loc + message only: pydantic's default text echoes the offending input
        detail = "; ".join(
            f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors()
        )
        raise SafeFileError(f"invalid status file ({detail})") from None


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:  # PermissionError: it exists (as another user's process)
        return True
    return True


def running(st: StatusFile, now: datetime | None = None) -> tuple[bool, str]:
    """(is the executor running, why not). Stale = not refreshed for `STALE_AFTER_S`."""
    now = now or datetime.now(UTC)
    age = (now - st.updated_at).total_seconds()
    if st.stopped:
        return False, f"executor stopped {_ago(age)}"
    if age > STALE_AFTER_S:
        return False, f"status is stale (last update {_ago(age)}): the executor is not running"
    if not _pid_alive(st.pid):
        return False, f"process {st.pid} is gone (last update {_ago(age)})"
    return True, ""


def _dur(seconds: float) -> str:
    s = max(0, round(seconds))
    if s < 90:
        return f"{s}s"
    if s < 90 * 60:
        return f"{s // 60}m"
    return f"{s // 3600}h"


def _ago(seconds: float) -> str:
    return f"{_dur(seconds)} ago"


def render(st: StatusFile, now: datetime | None = None) -> str:
    """Human-readable status (no payload by construction)."""
    now = now or datetime.now(UTC)
    b = st.budget
    lines = [f"profile:   {st.profile.value} (source: {st.profile_source})"]
    if st.held_below_nominal:
        lines[0] += "  [held below nominal]"
    if st.candidate is not None and st.candidate != st.profile:
        since = st.candidate_since
        wait = f" for {_dur((now - since).total_seconds())}" if since is not None else ""
        lines.append(f"candidate: {st.candidate.value} (waiting out the hold{wait})")
    prio = b.cpu_priority.value if b.cpu_priority is not None else "unknown"
    lines.append(
        f"budget:    {b.memory_gb:g} GB, GPU duty {b.gpu_duty_cycle:.0%}, "
        f"batch {b.max_batch_size}, cpu {prio}"
    )
    if st.paused_reason is not None:
        lines.append(f"state:     paused ({st.paused_reason})")
    elif st.subtask is not None:
        t = st.subtask
        lines.append(
            f"state:     running subtask {t.subtask_id} attempt {t.attempt} "
            f"({t.records_done}/{t.records_total} records)"
        )
    else:
        lines.append("state:     idle")
    if st.gpu_share is not None:
        lines.append(f"gpu share: {st.gpu_share:.0%} of wall time (current run)")
    if st.signals is not None:
        g = st.signals
        power = "AC" if g.on_ac else "battery"
        if g.battery_percent is not None:
            power += f" {g.battery_percent}%"
        extra = ", low power mode" if g.low_power_mode else ""
        lines.append(
            f"signals:   {power}, thermal {g.thermal}, memory {g.memory_pressure}, "
            f"user idle {g.hid_idle_s:.0f}s{extra}"
        )
    lines.append(
        f"today:     {st.today.subtasks} subtasks, {st.today.records} records ({st.today.date}, "
        "since this executor started)"
    )
    lines.append(f"updated:   {_ago((now - st.updated_at).total_seconds())} (pid {st.pid})")
    return "\n".join(lines)
