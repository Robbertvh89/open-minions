---
id: TASK-7.4.7
title: M6.4g Re-run M6.3 scenarios after fixes; update report and SPEC NF-1..NF-3
status: Done
assignee: []
created_date: '2026-10-18 02:21'
updated_date: '2026-10-18 18:18'
labels:
  - scale
dependencies: []
parent_task_id: TASK-7.4
ordinal: 66000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A (stress, realistic with jitter, knee 1/10/50/500), B at 100k, C, D on the same Mac, production-like heartbeat ratio noted; new report + JSON; re-check bump_counters.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 NF-1 met, or the gap documented in an ADR (SKIP LOCKED only via ADR)
- [x] #2 B at 100k: verify PASS
- [x] #3 SPEC NF rows and TASK-7.4 ACs updated
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Re-run on main + 1cda843 (4 workers, 500 sims, 1M pending). Report docs/bench/m6.4-scale-apple-m3-pro.md/.json, ADR-0016, SPEC NF-1..NF-3 updated. NF-1 met for realistic mix (server p99 <= 50 ms), not for 2 s stress mix (ADR-0016). B at 100k verify PASS. Follow-ups: TASK-35 (watchdog tick 9 s), TASK-36 (server CPU).
<!-- SECTION:NOTES:END -->
