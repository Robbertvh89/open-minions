"""allow engine 'fake' (test/dev-only fake runner) in task and executor engine checks

Revision ID: 0005
Revises: 0004
Create Date: 2026-10-15 22:00:00
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_NEW_TASK = "engine IN ('mlx', 'ollama', 'hf', 'fake')"
_OLD_TASK = "engine IN ('mlx', 'ollama', 'hf')"
_NEW_EXEC = "engines <@ ARRAY['mlx','ollama','hf','fake']::varchar[]"
_OLD_EXEC = "engines <@ ARRAY['mlx','ollama','hf']::varchar[]"


def _swap(task_expr: str, exec_expr: str) -> None:
    # Both tables are small (one row per task / executor); a brief ACCESS EXCLUSIVE lock is fine.
    op.drop_constraint(op.f("ck_minion_task_engine"), "minion_task", type_="check")
    op.create_check_constraint(op.f("ck_minion_task_engine"), "minion_task", task_expr)
    op.drop_constraint(op.f("ck_minion_executor_engines_known"), "minion_executor", type_="check")
    op.create_check_constraint(
        op.f("ck_minion_executor_engines_known"), "minion_executor", exec_expr
    )


def upgrade() -> None:
    _swap(_NEW_TASK, _NEW_EXEC)


def downgrade() -> None:
    # Fails (by design) if any fake-engine rows still exist: remove them first.
    _swap(_OLD_TASK, _OLD_EXEC)
