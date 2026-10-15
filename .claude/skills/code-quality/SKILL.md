---
name: code-quality
description: Code-quality gate for open-minions — lint, format, strict typing, layering rules, conventions from CLAUDE.md, test quality and complexity. Use when reviewing a branch, before a PR, or when asked about "code quality", "clean up", "lint", or "conventions".
---

# Code quality

## 1. Automated gates (must be clean)
```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy packages migrations
uv run pytest -q
```

## 2. Layering rules (grep-checkable)
```bash
# SQL only in repositories/ and migrations/
grep -rnE "text\(|select\(|insert\(|update\(|delete\(" packages/minion-server/src --include=*.py | grep -v "/repositories/" | grep -v "/db/models.py"
# engine libs only in executor runners/
grep -rnE "^(import|from) (mlx|mlx_lm|mlx_embeddings|transformers|sentence_transformers|ollama)" packages | grep -v "minion_executor/runners/"
# executors never touch the DB
grep -rnE "sqlalchemy|asyncpg|psycopg" packages/minion-executor/src
# no duplicated API models: request/response schemas live in minion-common
grep -rn "class .*(BaseModel)" packages/minion-server/src packages/minion-executor/src
```
Any hit is a finding unless clearly justified. The first grep also matches `dict.update(` and
similar; check each hit before reporting it.

## 3. Conventions (CLAUDE.md)
- Type hints everywhere; no `Any` leaking through public interfaces without reason.
- UTC only: `datetime.now(UTC)`, `timestamptz`. Flag naive datetimes.
- Config via `MINION_*` env vars with pydantic-settings; no hard-coded hosts/secrets.
- Structured logging (`structlog`), never log payload contents.
- Every schema change has an Alembic migration; applied migrations are never edited.

## 4. Test quality
- Each acceptance criterion maps to at least one test.
- Concurrency paths have concurrency tests (not just sequential happy paths).
- No `sleep()` for synchronization; no test depends on execution order or on a dev DB.
- Tests assert behaviour, not implementation details.

## 5. Design smells
- Functions > ~50 lines or > 3 nesting levels; modules mixing layers.
- Speculative abstraction (interfaces with one implementation and no planned second).
- Swallowed exceptions; broad `except Exception` without re-raise or explicit handling.
- Duplicate logic that should live in `minion-common`.

Report findings as `path:line: <severity>: <problem>. <fix>.`
