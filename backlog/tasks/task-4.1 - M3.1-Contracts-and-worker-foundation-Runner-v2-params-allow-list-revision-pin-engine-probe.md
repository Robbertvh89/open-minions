---
id: TASK-4.1
title: >-
  M3.1: Contracts and worker foundation (Runner v2, params allow-list, revision
  pin, engine probe)
status: In Progress
assignee: []
created_date: '2026-10-15 20:42'
updated_date: '2026-10-15 20:42'
labels:
  - M3
dependencies: []
parent_task_id: TASK-4
priority: high
ordinal: 18000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Plan steps S1-S3 of TASK-4 (D1-D4, D9, D10, D14 idle unload). Delivers TASK-9 #1 (schema) and #3.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 ModelEntry requires revision (40-hex mlx/hf, 64-hex digest ollama); unknown task params rejected with 422 INVALID_PARAMS
- [ ] #2 Runner v2: StepControl ticks keep long runs alive past step timeout; no ticks -> WorkerCrashed; RunnerError.retryable round-trips
- [ ] #3 run --engines auto probes engines in a worker subprocess; none available -> exit 2; fake never implicit
- [ ] #4 Output lines carry model and model_revision; idle worker unloaded after MINION_EXECUTOR_IDLE_UNLOAD_S
<!-- AC:END -->
