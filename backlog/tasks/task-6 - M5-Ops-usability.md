---
id: TASK-6
title: 'M5: Ops & usability'
status: In Progress
assignee: []
created_date: '2026-10-15 14:02'
updated_date: '2026-10-17 03:20'
labels:
  - milestone
  - M5
dependencies: []
ordinal: 6000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Grafana dashboards, web UI, launchd packaging, auth hardening, usability test during video calls (NF-7).
<!-- SECTION:DESCRIPTION:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
BLOCKING (from TASK-5.2): the executor launchd plist must set ExitTimeOut >= MINION_EXECUTOR_STOP_GRACE_S + 15 s (default grace 30 -> >= 45 s). launchd's default ExitTimeOut is 20 s; a SIGKILL before the bounded shutdown finishes skips release and leaves payload temp files until the next start (cleanup_stale_tmp).
<!-- SECTION:NOTES:END -->
