"""API schemas shared by server and executor (SPEC §6)."""

from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any, Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from minion_common.enums import (
    CapacityProfile,
    CpuPriority,
    Engine,
    ExecutorStatus,
    SubtaskStatus,
    TaskStatus,
    TaskType,
)

SLUG_PATTERN = r"^[a-z0-9]([a-z0-9_-]{0,62}[a-z0-9])?$"

_HF_REPO_RE = re.compile(r"[A-Za-z0-9][\w.-]*/[\w.-]+", re.ASCII)
_OLLAMA_TAG_RE = re.compile(r"[a-z0-9][a-z0-9._/-]*(:[\w.-]+)?", re.ASCII)
_SHA1_RE = re.compile(r"[0-9a-f]{40}", re.ASCII)
_SHA256_RE = re.compile(r"[0-9a-f]{64}", re.ASCII)


def _bad_segments(ref: str) -> bool:
    """True for '.', '..' or empty path segments (and a bare '..' anywhere)."""
    name, _, tag = ref.partition(":")
    return ".." in ref or tag == "." or any(seg in ("", ".") for seg in name.split("/"))


def _ollama_ref_problem(ref: str) -> str | None:
    """Why `ref` is not a plain 'name:tag' / 'namespace/name:tag' library ref, else None.

    A tag is required (no implicit ':latest' to drift). A first path segment with '.' or ':' (or
    'localhost') is a registry host, which would make the daemon pull from an arbitrary server.
    """
    if not _OLLAMA_TAG_RE.fullmatch(ref) or _bad_segments(ref):
        return "ref must be an Ollama tag like 'name:tag'"
    last = ref.rsplit("/", 1)[-1]
    if ":" not in last:
        return "ref must carry an explicit tag like 'name:tag'"
    if ref.count("/") > 1:
        return "ref must be 'name:tag' or 'namespace/name:tag'"
    if "/" in ref:
        first = ref.split("/", 1)[0]
        if "." in first or ":" in first or first == "localhost":
            return "ref must not name a registry host"
    return None


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())


class _ResponseModel(_Model):
    """Parsed by the executor from server responses: unknown fields are ignored so a newer
    server can add fields without breaking older executors (requests stay strict)."""

    model_config = ConfigDict(extra="ignore", protected_namespaces=())


class Budget(_Model):
    """Capacity budget the Mac's owner granted (SPEC §4.6)."""

    profile: CapacityProfile
    memory_gb: float = Field(gt=0, le=1024, allow_inf_nan=False)
    gpu_duty_cycle: float = Field(gt=0, le=1, allow_inf_nan=False)
    max_batch_size: int = Field(ge=1, le=4096)
    # Optional so requests from older clients (and the server's view of them) stay valid.
    cpu_priority: CpuPriority | None = None


class ModelEntry(_Model):
    model_id: str = Field(min_length=1, max_length=200)
    type: TaskType
    engine: Engine
    ref: str = Field(min_length=1, max_length=500)
    memory_gb: float = Field(gt=0, le=1024, allow_inf_nan=False)
    revision: str | None = Field(
        default=None,
        max_length=71,
        description=(
            "Pinned model revision: 40-hex commit SHA (mlx/hf) or 64-hex manifest digest, "
            "optional 'sha256:' prefix (ollama). Optional only for the fake engine."
        ),
    )

    @model_validator(mode="after")
    def _check_ref_and_revision(self) -> Self:
        ref, rev = self.ref, self.revision
        if self.engine in (Engine.MLX, Engine.HF):
            if not _HF_REPO_RE.fullmatch(ref) or _bad_segments(ref):
                raise ValueError("ref must be a Hugging Face repo id 'owner/name'")
            if rev is None or not _SHA1_RE.fullmatch(rev):
                raise ValueError("revision must be a 40-hex commit SHA for mlx/hf models")
        elif self.engine == Engine.OLLAMA:
            if (problem := _ollama_ref_problem(ref)) is not None:
                raise ValueError(problem)
            digest = rev.removeprefix("sha256:") if rev is not None else None
            if digest is None or not _SHA256_RE.fullmatch(digest):
                raise ValueError("revision must be a 64-hex manifest digest for ollama models")
            self.revision = digest
        return self


class CompletionParams(_Model):
    """Allow-listed completion parameters (SPEC FR-4)."""

    temperature: float = Field(default=0.0, ge=0, le=2, allow_inf_nan=False)
    top_p: float = Field(default=1.0, gt=0, le=1, allow_inf_nan=False)
    max_tokens: int = Field(default=256, ge=1, le=4096, strict=True)
    seed: int | None = Field(default=None, ge=0, le=2**63 - 1, strict=True)
    system: str | None = Field(default=None, max_length=8000)


class EmbeddingParams(_Model):
    """Allow-listed embedding parameters (SPEC FR-4)."""

    normalize: bool = Field(default=True, strict=True)


TaskParams = CompletionParams | EmbeddingParams


def parse_params(task_type: TaskType, raw: dict[str, Any]) -> TaskParams:
    """Validate raw task params against the allow-list for `task_type`.

    Raises `pydantic.ValidationError` on unknown keys or out-of-range values.
    """
    if task_type == TaskType.COMPLETION:
        return CompletionParams.model_validate(raw)
    return EmbeddingParams.model_validate(raw)


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


class TaskPage(_Model):
    """One page of `GET /tasks`; `next_cursor` is opaque (None = last page)."""

    items: list[TaskOut]
    next_cursor: str | None = None


class SubtaskOut(_Model):
    """Subtask progress row. No input/output keys, URLs or executor identity (SPEC S-11)."""

    subtask_id: int = Field(ge=0)
    status: SubtaskStatus
    attempt: int = Field(ge=0)
    max_attempts: int = Field(ge=1)
    record_count: int = Field(ge=1)
    error_code: str | None = Field(default=None, pattern=r"^[A-Z0-9_]{1,64}$")
    updated_at: AwareDatetime


class SubtaskPage(_Model):
    items: list[SubtaskOut]
    next_cursor: str | None = None


class ExecutorOut(_Model):
    """Fleet row for operators (SPEC §6.1). Never carries the token hash."""

    executor_id: uuid.UUID
    owner: str
    pool: str
    hostname: str
    chip: str | None = None
    total_memory_gb: float | None = None
    os_version: str | None = None
    engines: list[str] = Field(default_factory=list)
    status: ExecutorStatus
    profile: CapacityProfile | None = None
    budget: Budget | None = None
    paused_reason: str | None = None
    registered_at: AwareDatetime
    last_seen_at: AwareDatetime | None = None
    last_heartbeat_at: AwareDatetime | None = None
    online: bool
    busy: bool


class ExecutorPage(_Model):
    """One page of `GET /executors`; `next_cursor` is opaque (None = last page)."""

    items: list[ExecutorOut]
    next_cursor: str | None = None


class RegisterRequest(_Model):
    hostname: str = Field(min_length=1, max_length=255)
    chip: str = Field(min_length=1, max_length=100)
    total_memory_gb: float = Field(gt=0)
    os_version: str = Field(min_length=1, max_length=100)
    engines: list[Engine] = Field(min_length=1, max_length=16)
    profile: CapacityProfile
    budget: Budget


class RegisterResponse(_ResponseModel):
    executor_id: uuid.UUID


class PreferenceRequest(_Model):
    """All-None clears the preference; None fields are wildcards."""

    team: str | None = Field(default=None, pattern=SLUG_PATTERN)
    project: str | None = Field(default=None, pattern=SLUG_PATTERN)
    task_name: str | None = Field(default=None, pattern=SLUG_PATTERN)


_POST_FIELD_NAME_RE = re.compile(r"[A-Za-z0-9_-]{1,64}", re.ASCII)
_POST_MAX_FIELDS = 16
_POST_MAX_VALUE_BYTES = 8 << 10


class PresignedPost(_ResponseModel):
    """S3 presigned POST: form `url` plus the signed `fields` (policy incl. size range).

    Both are credentials: never log them.
    """

    url: str = Field(min_length=1, max_length=4096)
    fields: dict[str, str]

    @model_validator(mode="after")
    def _check_fields(self) -> Self:
        if not 1 <= len(self.fields) <= _POST_MAX_FIELDS:
            raise ValueError(f"fields must have 1..{_POST_MAX_FIELDS} entries")
        for name, value in self.fields.items():
            if not _POST_FIELD_NAME_RE.fullmatch(name):
                raise ValueError("invalid form field name")
            if len(value.encode()) > _POST_MAX_VALUE_BYTES or any(c in value for c in '\r\n"'):
                raise ValueError("invalid form field value")
        if "key" not in self.fields or "policy" not in self.fields:
            raise ValueError("fields must contain 'key' and 'policy'")
        return self


class AcquireRequest(_Model):
    engines: list[Engine] = Field(min_length=1, max_length=16)
    budget: Budget
    # Capability marker: executors that upload outputs with a presigned POST send "post".
    output_upload: Literal["post"] | None = None


class Assignment(_ResponseModel):
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
    output_url: str | None = None  # legacy PUT URL; removed in the cutover
    output_post: PresignedPost | None = None
    expires_at: AwareDatetime
    # The server's output size cap; the executor enforces min(local, this). Absent on old servers.
    max_output_bytes: int | None = Field(default=None, gt=0)

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
    # Lenient on the wire (old executors); the server normalizes it before storing.
    code: str = Field(min_length=1, max_length=64)
    message: str = Field(max_length=1000, description="Diagnostic only; never payload content")


class ReleaseRequest(_Model):
    """Optional body of a voluntary release: why the executor handed the subtask back.

    A stable upper-case code (e.g. `BUDGET_DOWNGRADE`), stored in the attempt history for
    dashboards. Never free text or payload content. Old executors send no body at all.
    """

    code: str = Field(pattern=r"^[A-Z0-9_]{1,64}$")


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


class PresenceRequest(_Model):
    """Low-frequency ping while paused/idle (SPEC FR-55). Bounded fields only (S-5)."""

    budget: Budget
    paused_reason: str | None = Field(default=None, max_length=64, pattern=r"^[a-z_]+$")


class HeartbeatResponse(_ResponseModel):
    cancel: bool
    output_url: str | None = None  # legacy PUT URL; removed in the cutover
    output_post: PresignedPost | None = None
    expires_at: AwareDatetime


# Cap on one subtask output object (server promotion check and executor pre-upload check).
MAX_OUTPUT_BYTES_DEFAULT = 4 << 30
MAX_OUTPUT_BYTES_LIMIT = 5 << 30  # S3 single-PUT / CopyObject ceiling (5 GiB)


def range_header(start: int, end_exclusive: int) -> str:
    """HTTP `Range` value for bytes `[start, end_exclusive)`; shared by signer and client."""
    if start < 0 or end_exclusive <= start:
        raise ValueError("range must satisfy 0 <= start < end")
    return f"bytes={start}-{end_exclusive - 1}"


ERROR_CODE_PATTERN = r"^[A-Z0-9_]{1,64}$"


class ErrorBody(_Model):
    model_config = ConfigDict(extra="ignore", protected_namespaces=())

    code: str = Field(pattern=ERROR_CODE_PATTERN)
    message: str


__all__ = [
    "ERROR_CODE_PATTERN",
    "SLUG_PATTERN",
    "AcquireRequest",
    "Assignment",
    "Budget",
    "CompleteRequest",
    "CompletionParams",
    "EmbeddingParams",
    "ErrorBody",
    "ExecutorOut",
    "ExecutorPage",
    "FailRequest",
    "HeartbeatRequest",
    "HeartbeatResponse",
    "ModelEntry",
    "PreferenceRequest",
    "PresenceRequest",
    "Progress",
    "RegisterRequest",
    "RegisterResponse",
    "SubtaskOut",
    "SubtaskPage",
    "TaskCounts",
    "TaskCreate",
    "TaskOut",
    "TaskPage",
    "TaskParams",
    "parse_params",
]
