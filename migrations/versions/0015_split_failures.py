"""minion_task.split_failures: real split failures, separate from claims (TASK-56)

Revision ID: 0015
Revises: 0014
Create Date: 2026-10-20 13:00:00

`split_attempts` counts claims and is the lease fencing token; it must never be decremented.
Exhaustion (SPLIT_ATTEMPTS_EXHAUSTED) now counts `split_failures`.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0015"
down_revision: str | None = "0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    # Constant default: metadata-only on PG 11+, no table rewrite.
    op.add_column(
        "minion_task",
        sa.Column("split_failures", sa.Integer(), server_default=sa.text("0"), nullable=False),
    )
    op.create_check_constraint(
        op.f("ck_minion_task_split_failures_nonneg"), "minion_task", "split_failures >= 0"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.drop_constraint(op.f("ck_minion_task_split_failures_nonneg"), "minion_task", type_="check")
    op.drop_column("minion_task", "split_failures")
