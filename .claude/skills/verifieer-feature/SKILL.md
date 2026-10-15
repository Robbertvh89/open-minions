---
name: verifieer-feature
description: Verify (verifiëren) that an open-minions feature works and meets its backlog acceptance criteria — static checks, full test suite incl. integration, and a live run against the compose stack. Use before review and before opening a PR, or when asked to "verify", "verifieer", "check it works", or "prove the AC".
---

# Verifieer feature

Goal: hard evidence, per acceptance criterion, that the feature works. Never claim PASS without
command output to back it.

## 1. Context
```bash
git branch --show-current
git diff main...HEAD --stat
backlog task <id> --plain        # acceptance criteria + plan
```

## 2. Static checks (all must pass)
```bash
uv sync --locked
uv run ruff check .
uv run ruff format --check .
uv run mypy packages migrations
```

## 3. Tests
```bash
uv run pytest -q                       # unit + integration (Docker needed)
```
Record pass/fail/skip counts. Any skip of an integration test = NOT VERIFIED for the ACs it covers.

## 4. Live system check (when the change affects runtime behaviour)
```bash
docker compose -f deploy/compose.yml up -d --wait postgres s3 && docker compose -f deploy/compose.yml up s3-init
uv run alembic upgrade head
uv run alembic downgrade -1 && uv run alembic upgrade head      # if a migration was added
uv run minion-server &  SERVER_PID=$!                            # then curl the affected endpoints
```
Exercise the feature the way a user/executor would (curl the API, run `minion-executor` against the
server, inspect S3 with the aws-cli container or boto3, query Postgres with
`docker compose -f deploy/compose.yml exec postgres psql -U minion -c "..."`).
Always stop what you started (`kill $SERVER_PID`).

macOS/Metal-only features (MLX runners, capacity governor signals): run them natively on this Mac;
if hardware/permissions prevent it, mark NOT VERIFIABLE HERE with the reason.

## 5. Report
```
AC1 <text>: PASS — <command> → <key output line>
AC2 <text>: FAIL — <repro in ≤3 lines>
Static: ruff ✓ format ✓ mypy ✓   Tests: 42 passed, 0 failed, 0 skipped
Verdict: PASS | FAIL
```
On PASS tick each verified criterion: `backlog task edit <id> --check-ac <n>`.
