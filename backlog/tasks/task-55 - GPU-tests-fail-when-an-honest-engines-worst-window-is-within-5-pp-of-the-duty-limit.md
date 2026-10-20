---
id: TASK-55
title: >-
  GPU tests: fail when an honest engine's worst window is within 5 pp of the
  duty limit
status: To Do
assignee: []
created_date: '2026-10-20 04:26'
labels:
  - executor
  - gpu
  - test
dependencies: []
ordinal: 97000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Cheap early-warning guard from the TASK-15.1 deferral: honest gpu-marked tests/bench windows should fail (or warn loudly) when the worst 10 s window is within 5 percentage points of duty+10pp, so a regression toward the kill threshold is caught before field kills.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Honest gpu tests assert worst window <= limit - 5 pp
<!-- AC:END -->
