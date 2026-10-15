"""task-level error message: minion_task.error

Revision ID: 0003
Revises: 0002
Create Date: 2026-10-15 18:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("minion_task", sa.Column("error", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("minion_task", "error")
