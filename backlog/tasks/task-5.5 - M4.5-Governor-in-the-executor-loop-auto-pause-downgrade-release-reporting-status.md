---
id: TASK-5.5
title: >-
  M4.5 Governor in the executor loop: auto-pause, downgrade-release, reporting,
  status
status: To Do
assignee: []
created_date: '2026-10-16 13:32'
updated_date: '2026-10-16 17:50'
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
<!-- SECTION:NOTES:END -->
