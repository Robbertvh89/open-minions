"""baseline schema: the 7 core tables (SPEC §7)

Enum columns are VARCHAR(32) with one named CHECK constraint each.

Revision ID: 0001
Revises:
Create Date: 2026-10-15 16:08:20.884972
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "minion_executor",
        sa.Column("executor_id", sa.UUID(), nullable=False),
        sa.Column("token_hash", sa.String(length=64), nullable=False),
        sa.Column("hostname", sa.String(length=255), nullable=False),
        sa.Column("owner", sa.String(length=128), nullable=False),
        sa.Column("pool", sa.String(length=64), server_default="default", nullable=False),
        sa.Column("chip", sa.String(length=64), nullable=True),
        sa.Column("total_memory_gb", sa.Numeric(precision=6, scale=2), nullable=True),
        sa.Column("os_version", sa.String(length=64), nullable=True),
        sa.Column(
            "engines",
            postgresql.ARRAY(sa.String(length=16)),
            server_default=sa.text("'{}'::varchar[]"),
            nullable=False,
        ),
        sa.Column("status", sa.String(length=32), server_default="ACTIVE", nullable=False),
        sa.Column("profile", sa.String(length=32), nullable=True),
        sa.Column("budget", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("paused_reason", sa.String(length=64), nullable=True),
        sa.Column(
            "registered_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "profile IN ('light', 'balanced', 'full', 'custom')",
            name=op.f("ck_minion_executor_capacity_profile"),
        ),
        sa.CheckConstraint(
            "status IN ('ACTIVE', 'REVOKED')", name=op.f("ck_minion_executor_executor_status")
        ),
        sa.PrimaryKeyConstraint("executor_id", name=op.f("pk_minion_executor")),
        sa.UniqueConstraint("token_hash", name=op.f("uq_minion_executor_token_hash")),
    )
    op.create_table(
        "minion_task",
        sa.Column("team", sa.String(length=64), nullable=False),
        sa.Column("project", sa.String(length=64), nullable=False),
        sa.Column("task_name", sa.String(length=128), nullable=False),
        sa.Column("type", sa.String(length=32), nullable=False),
        sa.Column("model_id", sa.String(length=256), nullable=False),
        sa.Column("engine", sa.String(length=32), nullable=False),
        sa.Column("model_memory_gb", sa.Numeric(precision=6, scale=2), nullable=False),
        sa.Column("input_uri", sa.Text(), nullable=False),
        sa.Column("output_uri", sa.Text(), nullable=False),
        sa.Column(
            "params",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "records_per_subtask", sa.Integer(), server_default=sa.text("500"), nullable=False
        ),
        sa.Column("priority", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("max_attempts", sa.Integer(), server_default=sa.text("3"), nullable=False),
        sa.Column("allowed_pool", sa.String(length=64), nullable=True),
        sa.Column("status", sa.String(length=32), server_default="SPLITTING", nullable=False),
        sa.Column("total_subtasks", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("succeeded_subtasks", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("failed_subtasks", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("created_by", sa.String(length=128), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("engine IN ('mlx', 'ollama', 'hf')", name=op.f("ck_minion_task_engine")),
        sa.CheckConstraint(
            "status IN ('SPLITTING', 'PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'PARTIAL', 'CANCELLED')",
            name=op.f("ck_minion_task_task_status"),
        ),
        sa.CheckConstraint(
            "type IN ('embedding', 'completion')", name=op.f("ck_minion_task_task_type")
        ),
        sa.PrimaryKeyConstraint("team", "project", "task_name", name=op.f("pk_minion_task")),
    )
    op.create_index(
        "ix_minion_task_queue",
        "minion_task",
        ["status", sa.literal_column("priority DESC"), "created_at"],
        unique=False,
    )
    op.create_table(
        "minion_executor_preference",
        sa.Column("executor_id", sa.UUID(), nullable=False),
        sa.Column("team", sa.String(length=64), nullable=True),
        sa.Column("project", sa.String(length=64), nullable=True),
        sa.Column("task_name", sa.String(length=128), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["executor_id"],
            ["minion_executor.executor_id"],
            name=op.f("fk_minion_executor_preference_executor_id_minion_executor"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("executor_id", name=op.f("pk_minion_executor_preference")),
    )
    op.create_table(
        "minion_subtask",
        sa.Column("team", sa.String(length=64), nullable=False),
        sa.Column("project", sa.String(length=64), nullable=False),
        sa.Column("task_name", sa.String(length=128), nullable=False),
        sa.Column("subtask_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), server_default="PENDING", nullable=False),
        sa.Column("attempt", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("input_object_key", sa.Text(), nullable=False),
        sa.Column("byte_start", sa.BigInteger(), nullable=False),
        sa.Column("byte_end", sa.BigInteger(), nullable=False),
        sa.Column("first_record", sa.BigInteger(), nullable=False),
        sa.Column("record_count", sa.Integer(), nullable=False),
        sa.Column("output_uri", sa.Text(), nullable=True),
        sa.Column("error", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')",
            name=op.f("ck_minion_subtask_subtask_status"),
        ),
        sa.ForeignKeyConstraint(
            ["team", "project", "task_name"],
            ["minion_task.team", "minion_task.project", "minion_task.task_name"],
            name=op.f("fk_minion_subtask_team_minion_task"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "team", "project", "task_name", "subtask_id", name=op.f("pk_minion_subtask")
        ),
    )
    op.create_index(
        "ix_minion_subtask_pending",
        "minion_subtask",
        ["team", "project", "task_name", "subtask_id"],
        unique=False,
        postgresql_where=sa.text("status = 'PENDING'"),
    )
    op.create_index(
        "ix_minion_subtask_progress",
        "minion_subtask",
        ["team", "project", "task_name", "status"],
        unique=False,
    )
    op.create_table(
        "minion_subtask_attempt",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("team", sa.String(length=64), nullable=False),
        sa.Column("project", sa.String(length=64), nullable=False),
        sa.Column("task_name", sa.String(length=128), nullable=False),
        sa.Column("subtask_id", sa.Integer(), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("acquirement_id", sa.UUID(), nullable=False),
        sa.Column("executor_id", sa.UUID(), nullable=True),
        sa.Column("outcome", sa.String(length=32), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "finished_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("error", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.CheckConstraint(
            "outcome IN ('SUCCEEDED', 'FAILED', 'EXPIRED', 'RELEASED', 'CANCELLED')",
            name=op.f("ck_minion_subtask_attempt_attempt_outcome"),
        ),
        sa.ForeignKeyConstraint(
            ["executor_id"],
            ["minion_executor.executor_id"],
            name=op.f("fk_minion_subtask_attempt_executor_id_minion_executor"),
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_minion_subtask_attempt")),
    )
    op.create_index(
        "ix_minion_subtask_attempt_finished_at",
        "minion_subtask_attempt",
        ["finished_at"],
        unique=False,
    )
    op.create_index(
        "ix_minion_subtask_attempt_subtask",
        "minion_subtask_attempt",
        ["team", "project", "task_name", "subtask_id"],
        unique=False,
    )
    op.create_table(
        "minion_subtask_acquirement",
        sa.Column("acquirement_id", sa.UUID(), nullable=False),
        sa.Column("team", sa.String(length=64), nullable=False),
        sa.Column("project", sa.String(length=64), nullable=False),
        sa.Column("task_name", sa.String(length=128), nullable=False),
        sa.Column("subtask_id", sa.Integer(), nullable=False),
        sa.Column("executor_id", sa.UUID(), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column(
            "acquired_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["executor_id"],
            ["minion_executor.executor_id"],
            name=op.f("fk_minion_subtask_acquirement_executor_id_minion_executor"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["team", "project", "task_name", "subtask_id"],
            [
                "minion_subtask.team",
                "minion_subtask.project",
                "minion_subtask.task_name",
                "minion_subtask.subtask_id",
            ],
            name=op.f("fk_minion_subtask_acquirement_team_minion_subtask"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("acquirement_id", name=op.f("pk_minion_subtask_acquirement")),
        sa.UniqueConstraint(
            "team",
            "project",
            "task_name",
            "subtask_id",
            name="uq_minion_subtask_acquirement_subtask",
        ),
    )
    op.create_table(
        "minion_executor_heartbeat",
        sa.Column("acquirement_id", sa.UUID(), nullable=False),
        sa.Column("executor_id", sa.UUID(), nullable=False),
        sa.Column(
            "last_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("progress", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.ForeignKeyConstraint(
            ["acquirement_id"],
            ["minion_subtask_acquirement.acquirement_id"],
            name=op.f("fk_minion_executor_heartbeat_acquirement_id_minion_subtask_acquirement"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["executor_id"],
            ["minion_executor.executor_id"],
            name=op.f("fk_minion_executor_heartbeat_executor_id_minion_executor"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("acquirement_id", name=op.f("pk_minion_executor_heartbeat")),
    )
    op.create_index(
        op.f("ix_minion_executor_heartbeat_last_seen_at"),
        "minion_executor_heartbeat",
        ["last_seen_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_minion_executor_heartbeat_last_seen_at"), table_name="minion_executor_heartbeat"
    )
    op.drop_table("minion_executor_heartbeat")
    op.drop_table("minion_subtask_acquirement")
    op.drop_index("ix_minion_subtask_attempt_subtask", table_name="minion_subtask_attempt")
    op.drop_index("ix_minion_subtask_attempt_finished_at", table_name="minion_subtask_attempt")
    op.drop_table("minion_subtask_attempt")
    op.drop_index("ix_minion_subtask_progress", table_name="minion_subtask")
    op.drop_index(
        "ix_minion_subtask_pending",
        table_name="minion_subtask",
        postgresql_where=sa.text("status = 'PENDING'"),
    )
    op.drop_table("minion_subtask")
    op.drop_table("minion_executor_preference")
    op.drop_index("ix_minion_task_queue", table_name="minion_task")
    op.drop_table("minion_task")
    op.drop_table("minion_executor")
