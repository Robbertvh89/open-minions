---
id: TASK-5.3
title: >-
  M4.3 Live worker control: governor-driven pacer, pause, per-batch budget, CPU
  priority, footprint
status: In Progress
assignee: []
created_date: '2026-10-16 13:32'
updated_date: '2026-10-16 15:11'
labels:
  - M4
dependencies: []
parent_task_id: TASK-5
ordinal: 26000
---

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 WorkerHandle.control changes duty/pause at next checkpoint incl. mid pacer-sleep; ack per generation; no timeout while paused
- [ ] #2 run carries Budget; Runner.set_budget/trim in all runners; gpu tests show mlx limits updated
- [ ] #3 cpu_priority applied (PRIO_DARWIN_BG/nice) incl. inside sandbox
- [ ] #4 footprint sampler darwin/linux
- [ ] #5 pacer busy/wall within +-3 pp of target in CI
<!-- AC:END -->
