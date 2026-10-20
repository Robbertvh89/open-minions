"""ORM models for the 8 PostgreSQL tables (SPEC §7, ARCHITECTURE §5).

Subtasks are identified by the natural key `(team, project, task_name, subtask_id)`, as in the
paper. The UNIQUE constraint on `minion_subtask_acquirement` is the only mutual-exclusion mechanism
for subtask ownership (CLAUDE.md invariant 2) — do not weaken it.
"""

import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from minion_common.enums import (
    AttemptOutcome,
    CapacityProfile,
    Engine,
    ExecutorStatus,
    SubtaskStatus,
    TaskStatus,
    TaskType,
)
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Identity,
    Index,
    Integer,
    MetaData,
    Numeric,
    PrimaryKeyConstraint,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

SUBTASK_KEY = ("team", "project", "task_name", "subtask_id")


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


def _enum(enum_cls: type[StrEnum], name: str) -> SAEnum:
    """Store enums as VARCHAR + CHECK (easier to migrate than native PG enums)."""
    return SAEnum(
        enum_cls,
        name=name,
        native_enum=False,
        create_constraint=True,
        length=32,
        values_callable=lambda e: [m.value for m in e],
    )


def _now() -> Mapped[datetime]:
    return mapped_column(DateTime(timezone=True), server_default=func.now())


def _updated_at() -> Mapped[datetime]:
    return mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class Task(Base):
    __tablename__ = "minion_task"
    __table_args__ = (
        PrimaryKeyConstraint("team", "project", "task_name"),
        Index("ix_minion_task_queue", "status", text("priority DESC"), "created_at"),
        # Serves the acquire candidate query: live tasks in FR-22 order (ARCHITECTURE §3.2).
        Index(
            "ix_minion_task_live_queue",
            text("priority DESC"),
            "created_at",
            "team",
            "project",
            "task_name",
            postgresql_where=text("status IN ('PENDING', 'RUNNING')"),
        ),
        # Bounded per-team live count for the create cap (FR-9, ADR-0020). Deliberately not
        # partial: a partial index on the live statuses is usable by the acquire candidate query
        # and displaced its ordered walk (~120x slower at 10k same-priority live tasks).
        Index("ix_minion_task_team_status", "team", "status"),
        # Serves `GET /tasks` keyset pages (single-team listing, newest first).
        Index(
            "ix_minion_task_team_created",
            "team",
            text("created_at DESC"),
            text("project DESC"),
            text("task_name DESC"),
        ),
        CheckConstraint("records_per_subtask > 0", name="records_per_subtask_positive"),
        CheckConstraint("max_attempts > 0", name="max_attempts_positive"),
        CheckConstraint("total_subtasks >= 0", name="total_subtasks_nonneg"),
        # Two tasks sharing an output prefix would share attempt and promoted keys.
        UniqueConstraint("output_uri"),
        CheckConstraint("split_attempts >= 0", name="split_attempts_nonneg"),
    )

    team: Mapped[str] = mapped_column(String(64))
    project: Mapped[str] = mapped_column(String(64))
    task_name: Mapped[str] = mapped_column(String(128))
    type: Mapped[TaskType] = mapped_column(_enum(TaskType, "task_type"))
    model_id: Mapped[str] = mapped_column(String(256))
    # Snapshotted from models.yaml at create time (SPEC FR-4a).
    engine: Mapped[Engine] = mapped_column(_enum(Engine, "engine"))
    model_memory_gb: Mapped[Decimal] = mapped_column(Numeric(6, 2))
    input_uri: Mapped[str] = mapped_column(Text)
    output_uri: Mapped[str] = mapped_column(Text)
    params: Mapped[dict[str, Any]] = mapped_column(JSONB, server_default=text("'{}'::jsonb"))
    records_per_subtask: Mapped[int] = mapped_column(Integer, server_default=text("500"))
    priority: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    max_attempts: Mapped[int] = mapped_column(Integer, server_default=text("3"))
    allowed_pool: Mapped[str | None] = mapped_column(String(64))
    status: Mapped[TaskStatus] = mapped_column(
        _enum(TaskStatus, "task_status"), server_default=TaskStatus.SPLITTING.value
    )
    total_subtasks: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    created_by: Mapped[str] = mapped_column(String(128))
    error: Mapped[str | None] = mapped_column(Text)  # task-level failure (e.g. empty input)
    # Splitter lease/backoff: a SPLITTING task is only claimable once next_split_at has passed.
    split_attempts: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    next_split_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = _now()
    updated_at: Mapped[datetime] = _updated_at()
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class TaskCounter(Base):
    """Succeeded/failed/cancelled subtask counters, sharded by `subtask_id % 16` (ADR-0015).

    Summed on read; sharding spreads completions of one task over several rows instead of one
    hot `minion_task` row.
    """

    __tablename__ = "minion_task_counter"
    __table_args__ = (
        PrimaryKeyConstraint("team", "project", "task_name", "shard"),
        ForeignKeyConstraint(
            ["team", "project", "task_name"],
            ["minion_task.team", "minion_task.project", "minion_task.task_name"],
            ondelete="CASCADE",
        ),
        CheckConstraint("succeeded >= 0", name="succeeded_nonneg"),
        CheckConstraint("failed >= 0", name="failed_nonneg"),
        CheckConstraint("cancelled >= 0", name="cancelled_nonneg"),
    )

    team: Mapped[str] = mapped_column(String(64))
    project: Mapped[str] = mapped_column(String(64))
    task_name: Mapped[str] = mapped_column(String(128))
    shard: Mapped[int] = mapped_column(SmallInteger)
    succeeded: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    failed: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    cancelled: Mapped[int] = mapped_column(Integer, server_default=text("0"))


class Subtask(Base):
    __tablename__ = "minion_subtask"
    __table_args__ = (
        PrimaryKeyConstraint(*SUBTASK_KEY),
        ForeignKeyConstraint(
            ["team", "project", "task_name"],
            ["minion_task.team", "minion_task.project", "minion_task.task_name"],
            ondelete="CASCADE",
        ),
        # Serves the acquire candidate query (ARCHITECTURE §3.2).
        Index(
            "ix_minion_subtask_pending",
            *SUBTASK_KEY,
            postgresql_where=text("status = 'PENDING'"),
        ),
        Index("ix_minion_subtask_progress", "team", "project", "task_name", "status"),
        CheckConstraint("byte_end > byte_start", name="byte_range"),
        CheckConstraint("record_count > 0", name="record_count_positive"),
        CheckConstraint("attempt >= 0", name="attempt_nonneg"),
        CheckConstraint("first_record >= 0", name="first_record_nonneg"),
        CheckConstraint("max_attempts > 0", name="max_attempts_positive"),
    )

    team: Mapped[str] = mapped_column(String(64))
    project: Mapped[str] = mapped_column(String(64))
    task_name: Mapped[str] = mapped_column(String(128))
    subtask_id: Mapped[int] = mapped_column(Integer)
    status: Mapped[SubtaskStatus] = mapped_column(
        _enum(SubtaskStatus, "subtask_status"), server_default=SubtaskStatus.PENDING.value
    )
    attempt: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    # Snapshotted from the task by the splitter.
    max_attempts: Mapped[int] = mapped_column(Integer, server_default=text("3"))
    # Input slice (ARCHITECTURE §5.2).
    input_object_key: Mapped[str] = mapped_column(Text)
    byte_start: Mapped[int] = mapped_column(BigInteger)
    byte_end: Mapped[int] = mapped_column(BigInteger)
    first_record: Mapped[int] = mapped_column(BigInteger)
    record_count: Mapped[int] = mapped_column(Integer)
    output_uri: Mapped[str | None] = mapped_column(Text)
    error: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = _now()
    updated_at: Mapped[datetime] = _updated_at()


class Executor(Base):
    __tablename__ = "minion_executor"
    __table_args__ = (
        CheckConstraint(
            "engines <@ ARRAY['mlx','ollama','hf','fake']::varchar[]", name="engines_known"
        ),
    )

    executor_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    token_hash: Mapped[str] = mapped_column(String(64), unique=True)
    hostname: Mapped[str] = mapped_column(String(255))
    owner: Mapped[str] = mapped_column(String(128))
    pool: Mapped[str] = mapped_column(String(64), server_default="default")
    chip: Mapped[str | None] = mapped_column(String(64))
    total_memory_gb: Mapped[Decimal | None] = mapped_column(Numeric(6, 2))
    os_version: Mapped[str | None] = mapped_column(String(64))
    engines: Mapped[list[str]] = mapped_column(
        ARRAY(String(16)), server_default=text("'{}'::varchar[]")
    )
    status: Mapped[ExecutorStatus] = mapped_column(
        _enum(ExecutorStatus, "executor_status"), server_default=ExecutorStatus.ACTIVE.value
    )
    # Capacity reporting (SPEC §4.6, FR-55).
    profile: Mapped[CapacityProfile | None] = mapped_column(
        _enum(CapacityProfile, "capacity_profile")
    )
    budget: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    paused_reason: Mapped[str | None] = mapped_column(String(64))
    registered_at: Mapped[datetime] = _now()
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # NULL = token never expires; auth rejects tokens past this instant (security carry-forward 2).
    token_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class SubtaskAcquirement(Base):
    """Ownership record. Exactly one row may exist per subtask (paper §acquisition)."""

    __tablename__ = "minion_subtask_acquirement"
    __table_args__ = (
        UniqueConstraint(*SUBTASK_KEY, name="uq_minion_subtask_acquirement_subtask"),
        ForeignKeyConstraint(
            list(SUBTASK_KEY),
            [f"minion_subtask.{c}" for c in SUBTASK_KEY],
            ondelete="CASCADE",
        ),
        Index("ix_minion_subtask_acquirement_acquired_at", "acquired_at"),
        Index("ix_minion_subtask_acquirement_executor_id", "executor_id"),
    )

    acquirement_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    team: Mapped[str] = mapped_column(String(64))
    project: Mapped[str] = mapped_column(String(64))
    task_name: Mapped[str] = mapped_column(String(128))
    subtask_id: Mapped[int] = mapped_column(Integer)
    executor_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("minion_executor.executor_id", ondelete="RESTRICT")
    )
    attempt: Mapped[int] = mapped_column(Integer)
    acquired_at: Mapped[datetime] = _now()


class ExecutorHeartbeat(Base):
    """One row per active acquirement; the watchdog only scans this table (paper).

    The executor is reached by joining `minion_subtask_acquirement`.
    """

    __tablename__ = "minion_executor_heartbeat"

    acquirement_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("minion_subtask_acquirement.acquirement_id", ondelete="CASCADE"),
        primary_key=True,
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    progress: Mapped[dict[str, Any] | None] = mapped_column(JSONB)


class ExecutorPreference(Base):
    """Dynamic task binding (paper). NULL columns are wildcards."""

    __tablename__ = "minion_executor_preference"

    executor_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("minion_executor.executor_id", ondelete="CASCADE"), primary_key=True
    )
    team: Mapped[str | None] = mapped_column(String(64))
    project: Mapped[str | None] = mapped_column(String(64))
    task_name: Mapped[str | None] = mapped_column(String(128))
    updated_at: Mapped[datetime] = _updated_at()


class SubtaskAttempt(Base):
    """History of finished attempts; feeds Grafana."""

    __tablename__ = "minion_subtask_attempt"
    __table_args__ = (
        Index("ix_minion_subtask_attempt_subtask", *SUBTASK_KEY),
        Index("ix_minion_subtask_attempt_finished_at", "finished_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    team: Mapped[str] = mapped_column(String(64))
    project: Mapped[str] = mapped_column(String(64))
    task_name: Mapped[str] = mapped_column(String(128))
    subtask_id: Mapped[int] = mapped_column(Integer)
    attempt: Mapped[int] = mapped_column(Integer)
    acquirement_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), unique=True)
    executor_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("minion_executor.executor_id", ondelete="SET NULL")
    )
    outcome: Mapped[AttemptOutcome] = mapped_column(_enum(AttemptOutcome, "attempt_outcome"))
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime] = _now()
    error: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
