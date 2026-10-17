---
id: TASK-5.6
title: 'M4.6 Measure NF-8 and NF-9, ADR-0006, docs'
status: In Progress
assignee: []
created_date: '2026-10-16 13:32'
updated_date: '2026-10-17 02:59'
labels:
  - M4
dependencies: []
parent_task_id: TASK-5
ordinal: 29000
---

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 bench --budget-check: GPU share (ioreg, baseline-subtracted) and worker peak footprint vs budget; M3 Pro results committed
- [x] #2 NF-9: CI 20-trial reaction test max < 5 s; Mac opt-in measurements (memory_pressure, user return, FileSignals thermal/power) documented
- [x] #3 NF-4 idle/paused CPU < 1 % recorded
- [x] #4 ADR-0006 final; SPEC/ARCH/CLAUDE.md/.env.example synced
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Measured on M3 Pro (docs/bench/m4-governor-apple-m3-pro.md): NF-8 all 18 rows pass after quantum pacing (the first per-process run exposed an mlx-lm async_eval pacing bug: qwen balanced 74–76 % at duty 50 %); NF-9 CI max 0.433 s over 20 trials; NF-4 0.134 % idle / 0.162 % paused. NF-9 real-signal Mac measurements not run here (need a person at the keyboard and induced swap): opt-in procedure documented. Controller-side duty enforcement split out as TASK-15.
<!-- SECTION:NOTES:END -->
