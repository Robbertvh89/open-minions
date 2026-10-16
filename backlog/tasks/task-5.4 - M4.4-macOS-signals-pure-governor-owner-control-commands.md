---
id: TASK-5.4
title: 'M4.4 macOS signals, pure governor, owner control commands'
status: In Progress
assignee: []
created_date: '2026-10-16 13:32'
updated_date: '2026-10-16 19:17'
labels:
  - M4
dependencies: []
parent_task_id: TASK-5
ordinal: 27000
---

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 MacSignals reads HID idle, power/battery, Low Power Mode, thermal, memory pressure; NullSignals off macOS; FileSignals dev-only
- [ ] #2 governor.decide pure: FR-52, FR-53, hysteresis, pause-release timer, signals_unavailable; table tests
- [ ] #3 pause [--for], resume, profile <name|auto> write control.json atomically 0600
- [ ] #4 deps locked; Linux CI green
<!-- AC:END -->
