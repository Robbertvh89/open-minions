"""API schemas shared by server and executor (SPEC §6)."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from minion_common.enums import CapacityProfile, Engine, TaskStatus, TaskType

SLUG_PATTERN = r"^[a-z0-9]([a-z0-9_-]{0,62}[a-z0-9])?$"


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())


class Budget(_Model):
    """Capacity budget the Mac's owner granted (SPEC §4.6)."""

    profile: CapacityProfile
    memory_gb: float = Field(gt=0, le=1024, allow_inf_nan=False)
    gpu_duty_cycle: float = Field(gt=0, le=1, allow_inf_nan=False)
    max_batch_size: int = Field(ge=1, le=4096)


class ModelEntry(_Model):
    model_id: str = Field(min_length=1, max_length=200)
    type: TaskType
    engine: Engine
    ref: str = Field(min_length=1, max_length=500)
    memory_gb: float = Field(gt=0)


class TaskCreate(_Model):
    team: str = Field(pattern=SLUG_PATTERN)
    project: str = Field(pattern=SLUG_PATTERN)
    task_name: str = Field(pattern=SLUG_PATTERN)
    type: TaskType
    model: str = Field(min_length=1, max_length=200)
    input_uri: str = Field(min_length=1, max_length=2048)
    output_uri: str = Field(min_length=1, max_length=2048)
    params: dict[str, Any] = Field(default_factory=dict)
    records_per_subtask: int = Field(default=500, ge=1, le=100_000)
    priority: int = Field(default=0, ge=-1000, le=1000)
    allowed_pool: str | None = Field(default=None, min_length=1, max_length=64)


class TaskCounts(_Model):
    total: int = 0
    pending: int = 0
    running: int = 0
    succeeded: int = 0
    failed: int = 0
    cancelled: int = 0


class TaskOut(_Model):
    team: str
    project: str
    task_name: str
    type: TaskType
    model: str
    input_uri: str
    output_uri: str
    params: dict[str, Any] = Field(default_factory=dict)
    records_per_subtask: int
    priority: int
    allowed_pool: str | None = None
    status: TaskStatus
    created_by: str
    created_at: datetime
    finished_at: datetime | None = None
    error: str | None = None
    counts: TaskCounts = Field(default_factory=TaskCounts)


class RegisterRequest(_Model):
    hostname: str = Field(min_length=1, max_length=255)
    chip: str = Field(min_length=1, max_length=100)
    total_memory_gb: float = Field(gt=0)
    os_version: str = Field(min_length=1, max_length=100)
    engines: list[Engine] = Field(min_length=1, max_length=16)
    profile: CapacityProfile
    budget: Budget


class RegisterResponse(_Model):
    executor_id: uuid.UUID


class PreferenceRequest(_Model):
    """All-None clears the preference; None fields are wildcards."""

    team: str | None = Field(default=None, pattern=SLUG_PATTERN)
    project: str | None = Field(default=None, pattern=SLUG_PATTERN)
    task_name: str | None = Field(default=None, pattern=SLUG_PATTERN)


class AcquireRequest(_Model):
    engines: list[Engine] = Field(min_length=1, max_length=16)
    budget: Budget


class Assignment(_Model):
    acquirement_id: uuid.UUID
    attempt: int = Field(ge=1)
    team: str
    project: str
    task_name: str
    subtask_id: int = Field(ge=0)
    type: TaskType
    model: ModelEntry
    params: dict[str, Any] = Field(default_factory=dict)
    input_url: str
    byte_start: int = Field(ge=0)
    byte_end: int = Field(gt=0, description="Exclusive end offset")
    record_count: int = Field(ge=1)
    output_url: str
    expires_at: datetime

    @model_validator(mode="after")
    def _check_range(self) -> Self:
        if self.byte_end <= self.byte_start:
            raise ValueError("byte_end must be greater than byte_start")
        return self


class CompleteRequest(_Model):
    record_count: int = Field(ge=0)
    byte_size: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class FailRequest(_Model):
    retryable: bool
    code: str = Field(min_length=1, max_length=64)
    message: str = Field(max_length=1000, description="Diagnostic only; never payload content")


class Progress(_Model):
    records_done: int = Field(ge=0, le=2**31 - 1)
    records_total: int = Field(ge=0, le=2**31 - 1)

    @model_validator(mode="after")
    def _done_le_total(self) -> Self:
        if self.records_done > self.records_total:
            raise ValueError("records_done must not exceed records_total")
        return self


class HeartbeatRequest(_Model):
    """Liveness beat (SPEC §6.2). Bounded fields only: no free-form payload."""

    progress: Progress | None = None
    budget: Budget | None = None
    paused_reason: str | None = Field(default=None, max_length=64, pattern=r"^[a-z_]+$")


class HeartbeatResponse(_Model):
    cancel: bool
    output_url: str = Field(description="Refreshed presigned PUT URL for the same attempt key")
    expires_at: AwareDatetime


class ErrorBody(_Model):
    code: str
    message: str


__all__ = [
    "SLUG_PATTERN",
    "AcquireRequest",
    "Assignment",
    "Budget",
    "CompleteRequest",
    "ErrorBody",
    "FailRequest",
    "HeartbeatRequest",
    "HeartbeatResponse",
    "ModelEntry",
    "PreferenceRequest",
    "Progress",
    "RegisterRequest",
    "RegisterResponse",
    "TaskCounts",
    "TaskCreate",
    "TaskOut",
]
