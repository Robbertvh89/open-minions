---
id: TASK-1
title: 'M0: Project scaffolding'
status: To Do
assignee: []
created_date: '2026-10-15 14:02'
labels:
  - milestone
  - M0
dependencies: []
documentation:
  - docs/ARCHITECTURE.md
priority: high
ordinal: 1000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
uv workspace with minion-common/server/executor/watchdog packages, docker compose (postgres, minio, grafana), Alembic baseline with the 7 tables from SPEC §7, CI (ruff, mypy, pytest).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 uv sync installs all 4 packages
- [ ] #2 docker compose up starts postgres, minio, grafana
- [ ] #3 alembic upgrade head creates 7 tables incl. UNIQUE constraint on minion_subtask_acquirement
- [ ] #4 ruff, mypy --strict and pytest pass locally and in GitHub Actions
<!-- AC:END -->
