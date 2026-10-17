---
name: db-migration
description: How to change the open-minions PostgreSQL schema safely — edit ORM models, autogenerate and review an Alembic migration, test upgrade/downgrade, and protect the acquisition constraint. Use whenever a change touches minion_server/db/models.py, adds a table/column/index/constraint, or the user mentions "migration", "schema" or "alembic".
---

# DB migration

1. Edit `packages/minion-server/src/minion_server/db/models.py`. Keep the naming convention; enums
   as VARCHAR + CHECK via `_enum(...)`.
2. Ensure the dev DB is at head:
   ```bash
   docker compose -f deploy/compose.yml up -d --wait postgres
   uv run alembic upgrade head
   ```
3. Generate with the next sequential id:
   ```bash
   uv run alembic revision --autogenerate --rev-id 000N -m "<what changed>"
   ```
4. **Review the generated file by hand.** Autogenerate misses/mangles: partial indexes
   (`postgresql_where`), CHECK constraints on enums, server defaults, renames (shows as drop+add —
   rewrite as `op.alter_column`/`op.rename_table`), data migrations.
5. Run `uv run ruff format migrations && uv run mypy migrations`.
6. Round-trip test:
   ```bash
   uv run alembic upgrade head && uv run alembic downgrade -1 && uv run alembic upgrade head
   ```
7. Integration tests (`uv run pytest -m integration`) must pass — they migrate a fresh container.

## Never
- Edit a migration that is already on `main`.
- Drop or weaken `uq_minion_subtask_acquirement_subtask` (CLAUDE.md invariant 2).
- Add a migration that locks `minion_subtask` for long on large tables without noting it in the PR
  (use `CREATE INDEX CONCURRENTLY` via `op.create_index(..., postgresql_concurrently=True)` inside
  `with op.get_context().autocommit_block():`).
- Exceed 7 core tables without updating SPEC §7 and an ADR.
- Alter or drop a column used by a `minion_ro` view (migration 0007) without dropping and recreating
  the view in the same migration: views block `ALTER COLUMN TYPE` and `DROP COLUMN` on the columns
  they use. Keep the views free of secrets (token hash, params, object keys) when you add columns.
