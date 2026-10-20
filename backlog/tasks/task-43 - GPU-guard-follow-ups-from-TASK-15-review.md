---
id: TASK-43
title: GPU guard follow-ups from TASK-15 review
status: In Progress
assignee: []
created_date: '2026-10-19 14:36'
updated_date: '2026-10-20 00:04'
labels:
  - security
dependencies: []
ordinal: 83000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
From TASK-15 final review/security: (1) a GPU verdict already taken by the watcher is dropped when work finished in the same wake-up or _infer_done was set before _on_verdict (loop.py ~880, ~1240, ~1327): put it back into _gpu_pending or run _gpu_kill_idle after _complete. (2) Meter kills never count toward quarantine: a worker that churns IOKit clients so every registry walk fails gets back-off only (~3 % avg GPU, ~7 s unmetered bursts); count meter kills at a higher threshold (e.g. 6/h/engine) or when read failures coincide with the worker's client set changing. (3) Optionally persist quarantine expiry in the state dir across executor restarts.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Taken GPU verdicts are never lost (tests for both paths)
- [ ] #2 Meter-failure churn is bounded by quarantine (test + sim)
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Round-4 review follow-ups: (a) idle->busy edge not split: at duty 1.0 a rogue burning only in idle gaps < ~1 s is never charged (stays within 100 % budget) - record the busy edge in set_idle(False,t) and charge margin for [prev_t, edge). (b) _gpu_killed_pid never cleared: key on spawn generation or clear on spawn (pid reuse). (c) after a quiet-mode kill the gap drains at the last busy limit, not the margin - tighten wording or drain at margin. (d) test_honest_paced_worker_with_short_subtasks_is_never_killed passes on pre-fix code too (coverage only).
<!-- SECTION:NOTES:END -->
