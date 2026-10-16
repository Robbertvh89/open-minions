---
id: TASK-5.1
title: 'M4.1 Contracts, presence endpoint, executor.toml and custom profile'
status: Done
assignee: []
created_date: '2026-10-16 13:32'
updated_date: '2026-10-16 14:14'
labels:
  - M4
dependencies: []
parent_task_id: TASK-5
ordinal: 24000
---

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Budget.cpu_priority sent on register/acquire/heartbeat; old clients accepted
- [x] #2 POST /executor/presence 204 updates profile/budget/paused_reason/last_seen_at; 401 revoked; 422 bad reason; acquire clears paused_reason
- [x] #3 executor.toml validated (overrides, custom, active/idle, battery, schedules); unknown keys/perms/bad values exit 2; 75% cap
- [x] #4 run --profile custom works; nice derived from cpu_priority
- [x] #5 ADR-0006 draft and SPEC FR-51/52 default wording proposed
<!-- AC:END -->
