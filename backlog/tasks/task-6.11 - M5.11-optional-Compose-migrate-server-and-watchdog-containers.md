---
id: TASK-6.11
title: 'M5.11 (optional) Compose: migrate, server and watchdog containers'
status: In Progress
assignee: []
created_date: '2026-10-17 03:33'
updated_date: '2026-10-17 17:49'
labels:
  - M5
dependencies:
  - TASK-6.5
parent_task_id: TASK-6
ordinal: 42000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
deploy/Dockerfile (one image, non-root, read-only rootfs); compose services migrate (alembic upgrade head), server, watchdog with healthchecks and depends_on, per ARCHITECTURE 6. Can slip to M6.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 docker compose up -d then an executor run on the host takes a task to SUCCEEDED and the Grafana dashboards show it
<!-- AC:END -->
