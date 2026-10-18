"""autovacuum reloptions on minion_subtask (hot-update churn at scale)

Revision ID: 0010
Revises: 0009
Create Date: 2026-10-18 12:00:00
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # ALTER TABLE ... SET (storage parameter) takes SHARE UPDATE EXCLUSIVE: no rewrite, does not
    # block reads or writes. Fail fast rather than queue behind a long vacuum.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE minion_subtask SET ("
        "autovacuum_vacuum_scale_factor = 0.02, "
        "autovacuum_vacuum_threshold = 1000, "
        "autovacuum_analyze_scale_factor = 0.02)"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE minion_subtask RESET ("
        "autovacuum_vacuum_scale_factor, "
        "autovacuum_vacuum_threshold, "
        "autovacuum_analyze_scale_factor)"
    )
