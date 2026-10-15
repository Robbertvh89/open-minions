---
id: TASK-1
title: 'M0: Project scaffolding'
status: Done
assignee: []
created_date: '2026-10-15 14:02'
updated_date: '2026-10-15 14:22'
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
- [x] #1 uv sync installs all 4 packages
- [x] #2 docker compose up starts postgres, s3 (SeaweedFS), grafana
- [x] #3 alembic upgrade head creates 7 tables incl. UNIQUE constraint on minion_subtask_acquirement
- [x] #4 ruff, mypy --strict and pytest pass locally and in GitHub Actions
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
uv workspace (4 packages), compose stack (Postgres, SeaweedFS S3, Grafana; localhost-bound), baseline migration with 7 tables + CHECK guards + acquisition UNIQUE, drift/round-trip/constraint tests (23), CI with pinned actions and read-only token.
<!-- SECTION:FINAL_SUMMARY:END -->
