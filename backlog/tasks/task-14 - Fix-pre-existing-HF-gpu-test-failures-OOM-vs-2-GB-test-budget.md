---
id: TASK-14
title: Fix pre-existing HF gpu test failures (OOM vs 2 GB test budget)
status: Done
assignee: []
created_date: '2026-10-16 19:01'
updated_date: '2026-10-16 19:17'
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
- [x] #1 Both tests pass under uv run pytest -m 'gpu or model' on Apple Silicon
- [x] #2 Root cause stated (test budget vs model memory_gb vs memory guard); no production limit loosened without reason
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Root cause: test budget vs model memory_gb. Shared BUDGET is 2 GB; HF Qwen catalogue memory_gb is 2.5. (1) e2e: ExecutorLoop only takes models with memory_gb <= budget.memory_gb (loop.py:414), so the subtask was never acquired (correct production behaviour). (2) unload test: with the MPS fraction capped at 2 GB, loading Qwen OOMs when earlier tests leave allocations behind (passes alone). Fix: tests size the budget to the model (_budget_for). No production limit changed.

Repro evidence for the unload OOM: on main the full 'gpu or model' run failed it (TASK-5.3 implementer and verifier, clean main worktree: 2 failed / 12 passed in the HF file) while it passes alone, consistent with leftover allocations under the 2 GB MPS cap. BGE loads keep the shared BUDGET (memory_gb < 2).
<!-- SECTION:NOTES:END -->
