"""splitter retry/backoff columns and unique task output_uri

Revision ID: 0004
Revises: 0003
Create Date: 2026-10-15 20:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "minion_task",
        sa.Column("split_attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
    )
    op.add_column(
        "minion_task", sa.Column("next_split_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.create_check_constraint(
        op.f("ck_minion_task_split_attempts_nonneg"), "minion_task", "split_attempts >= 0"
    )
    # Plain (blocking) unique index build: minion_task is small (one row per task).
    op.create_unique_constraint(op.f("uq_minion_task_output_uri"), "minion_task", ["output_uri"])


def downgrade() -> None:
    op.drop_constraint(op.f("uq_minion_task_output_uri"), "minion_task", type_="unique")
    op.drop_constraint(op.f("ck_minion_task_split_attempts_nonneg"), "minion_task", type_="check")
    op.drop_column("minion_task", "next_split_at")
    op.drop_column("minion_task", "split_attempts")
