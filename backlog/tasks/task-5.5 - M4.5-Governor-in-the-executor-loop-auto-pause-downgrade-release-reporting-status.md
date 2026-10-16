---
id: TASK-5.5
title: >-
  M4.5 Governor in the executor loop: auto-pause, downgrade-release, reporting,
  status
status: To Do
assignee: []
created_date: '2026-10-16 13:32'
updated_date: '2026-10-16 20:20'
labels:
  - M4
dependencies: []
parent_task_id: TASK-5
ordinal: 28000
---

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 acquire/heartbeat/presence carry live budget+paused_reason; paused -> no acquire, presence on enter + every 60 s
- [ ] #2 mid-subtask duty/pause pushed at once; pause>=60 s or memory critical -> interrupt+release; resume continues attempt
- [ ] #3 FR-54 downgrade: finish batch (<=60 s), release, unload (Ollama keep_alive 0), unpin cache; idle worker above budget unloaded
- [ ] #4 memory guard: footprint > 1.1x budget twice -> fail retryable OOM
- [ ] #5 status.json + minion-executor status
- [ ] #6 integration e2e with FileSignals for pause/resume/release/downgrade; macos no-PM-assertion test
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
From TASK-5.3 fix round: wire WorkerHandle.priority_ok() into the governor poll (False = worker less throttled than its budget: interrupt/respawn), with a grace window of about one batch after a budget change since the worker applies a new priority only when its next run starts. Also poll WorkerHandle.footprint_bytes() (None = no live worker) for the D7 memory guard.

From TASK-5.4 fix round: (1) at startup, if the state dir fails ownership/mode checks (open_private_dir raises SafeFileError other than MissingDirError) exit 2. (2) read_control returns None | OwnerControl | ControlUnusable and never logs; log the unusable reason ONCE PER CHANGE (not every poll). (3) pass a UTC now to decide(); seed with initial_state(cfg, pinned); Decision.release_now/pause_expired are level-triggered, act idempotently; Decision.profile_source/candidate/candidate_since are available for status. (4) read_with_timeout raises SignalError('previous read still hung') while a read is wedged: counts as a failure.

From TASK-5.4 final review: (a) the loop must pass decision.budget (not budget_for(decision.profile)) to worker/runner/presence; add a test. (b) Reuse one signal source object for the whole run: the one-read-in-flight bound is per source. (c) Presence reports the current profile name with the held per-dimension min budget during a hold.

From TASK-5.4 polish: (1) runtime must always start from initial_state(cfg, pinned, specs=..., total_gb=...) (assert/test it); GovState() cold start has no applied budget and skips the hold. (2) Decision.candidate may equal Decision.profile while a hold keeps the budget below nominal; status must show 'held below nominal', not 'custom -> custom'. (3) The escalation timer keeps running while paused (ADR-0006 D5 allows it). Decision for D5: no extra ramp after resume; the applied budget stays the held minimum through a pause and the normal 60 s timer is the only ramp, so a pause longer than 60 s resumes straight at the target budget (no work ran in between, and every de-escalation already applied at once).
<!-- SECTION:NOTES:END -->
