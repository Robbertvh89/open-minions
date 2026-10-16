---
id: TASK-14
title: Fix pre-existing HF gpu test failures (OOM vs 2 GB test budget)
status: In Progress
assignee: []
created_date: '2026-10-16 19:01'
updated_date: '2026-10-16 19:01'
labels:
  - executor
  - test
dependencies: []
ordinal: 30000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
test_hf_runners_gpu.py::test_unload_frees_model_memory and ::test_e2e_completion_through_sandboxed_worker fail with OOM on main (M3 Pro, 36 GB) since the HF Qwen2.5-0.5B memory_gb was raised to 2.5 (TASK-4.4 bench). The tests use a 2 GB budget. gpu-marked, so CI skips them.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Both tests pass under uv run pytest -m 'gpu or model' on Apple Silicon
- [ ] #2 Root cause stated (test budget vs model memory_gb vs memory guard); no production limit loosened without reason
<!-- AC:END -->
