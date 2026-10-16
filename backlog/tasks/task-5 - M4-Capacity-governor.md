---
id: TASK-5
title: 'M4: Capacity governor'
status: In Progress
assignee: []
created_date: '2026-10-15 14:02'
updated_date: '2026-10-16 13:19'
labels:
  - milestone
  - M4
dependencies: []
ordinal: 5000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Profiles, macOS signals, duty-cycle throttle, auto-pause, budget reporting, downgrade-release. SPEC §4.6, ARCHITECTURE §3.6.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 NF-8 budget adherence measured
- [ ] #2 NF-9 reaction <5s measured
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Carry-over from M1 (TASK-2.3): (1) FR-17 grace period — SIGTERM currently kills worker and releases immediately; spec wants finish-or-release within 30 s grace; (2) FR-51 executor.toml overrides + custom profile not built (parse_profile rejects custom); (3) CPU priority only via nice — add taskpolicy / Budget field.
<!-- SECTION:NOTES:END -->
