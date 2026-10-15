---
id: TASK-4.1
title: >-
  M3.1: Contracts and worker foundation (Runner v2, params allow-list, revision
  pin, engine probe)
status: In Progress
assignee: []
created_date: '2026-10-15 20:42'
updated_date: '2026-10-15 22:49'
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
- [x] #1 ModelEntry requires revision (40-hex mlx/hf, 64-hex digest ollama); unknown task params rejected with 422 INVALID_PARAMS
- [x] #2 Runner v2: StepControl ticks keep long runs alive past step timeout; no ticks -> WorkerCrashed; RunnerError.retryable round-trips
- [x] #3 run --engines auto probes engines in a worker subprocess; none available -> exit 2; fake never implicit
- [x] #4 Output lines carry model and model_revision; idle worker unloaded after MINION_EXECUTOR_IDLE_UNLOAD_S
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
S1 (b18ce94): ModelEntry.revision required (40-hex mlx/hf, 64-hex ollama digest; fullmatch, ASCII, dot segments rejected); CompletionParams/EmbeddingParams allow-list -> 422 INVALID_PARAMS. Files: packages/minion-common/src/minion_common/schemas.py, deploy/models.yaml, server api/tasks.py. S2 (aa5b690): Runner v2 (runners/base.py: EngineConfig, RunnerError.retryable, StepControl, probe/stats/unload), worker step ticks keep runs alive (worker/process.py), static GPU duty-cycle pacer (worker/throttle.py, no 60 s cap). S3 (5482c07, 43ceb7b, 712ea3a): run --engines auto probes in worker subprocess after restrict_environ() (90 s timeout), SUPPORTED={FAKE} until TASK-4.3, no engine -> StartupError exit 2; output lines stamped model/model_revision, per-record PROMPT_TOO_LONG counts as records; idle unload MINION_EXECUTOR_IDLE_UNLOAD_S (600, 0 disables), also on acquire-error path (loop.py); MODEL_NOT_ADMISSIBLE and ENGINE_UNAVAILABLE always released, reasons logged executor-side only (server RELEASE_LIMIT bounds releases); Keychain subprocess gets scrubbed env (secrets.py). Docs: SPEC FR-4a/FR-6/M3 update, ARCH §2.2 executor block, CLAUDE.md run line, .env.example MINION_EXECUTOR_IDLE_UNLOAD_S. No new ADR: decisions follow SPEC/TASK-9.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
M3.1 done: revision pin, params allow-list (422 INVALID_PARAMS), Runner v2 with step ticks and retryable errors, engine probe (--engines auto, exit 2 when none), output stamping, idle unload, admission/release semantics. TASK-9 items #1 and #3 delivered. Open: SUPPORTED={FAKE} until TASK-4.3; ARCH §2 names throttle.pace() but code seam is StepControl.checkpoint() (naming only).
<!-- SECTION:FINAL_SUMMARY:END -->
