---
id: TASK-43
title: GPU guard follow-ups from TASK-15 review
status: To Do
assignee: []
created_date: '2026-10-19 14:36'
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
