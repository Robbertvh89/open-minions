"""Enumerations shared by server, executor and watchdog (SPEC §5, §4.6, §7)."""

from enum import StrEnum


class TaskType(StrEnum):
    EMBEDDING = "embedding"
    COMPLETION = "completion"


class Engine(StrEnum):
    """Inference engine (ADR-0005). `mlx` is the default."""

    MLX = "mlx"
    OLLAMA = "ollama"
    HF = "hf"
    FAKE = "fake"  # test/dev only: deterministic fake runner, never serves real work


class TaskStatus(StrEnum):
    SPLITTING = "SPLITTING"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"
    CANCELLED = "CANCELLED"


class SubtaskStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class AttemptOutcome(StrEnum):
    """Terminal outcome of one subtask attempt, recorded in `minion_subtask_attempt`."""

    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"
    RELEASED = "RELEASED"
    CANCELLED = "CANCELLED"


class ExecutorStatus(StrEnum):
    ACTIVE = "ACTIVE"
    REVOKED = "REVOKED"


class CapacityProfile(StrEnum):
    """Capacity profiles for partial allocation (SPEC §4.6, FR-51)."""

    LIGHT = "light"
    BALANCED = "balanced"
    FULL = "full"
    CUSTOM = "custom"
