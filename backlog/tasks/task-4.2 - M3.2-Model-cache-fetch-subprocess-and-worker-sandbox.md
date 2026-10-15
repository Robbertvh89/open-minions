---
id: TASK-4.2
title: 'M3.2: Model cache, fetch subprocess and worker sandbox'
status: In Progress
assignee: []
created_date: '2026-10-15 20:42'
updated_date: '2026-10-15 22:54'
labels:
  - M3
  - security
dependencies:
  - TASK-4.1
parent_task_id: TASK-4
priority: high
ordinal: 19000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Plan steps S4-S5 of TASK-4 (D5-D8). ADR-0009.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Cache: pinned revision, safetensors-only allow-patterns, sha256 verified, LRU eviction under MINION_EXECUTOR_MODEL_CACHE_GB, cancel/SIGTERM kills fetch
- [ ] #2 Worker is offline and sandboxed: cannot read HOME secrets, open non-loopback sockets or reach securityd; fail-closed if sandbox_init fails
- [ ] #3 HF_TOKEN/proxy/CA reach only the fetch subprocess, never the worker or logs
- [ ] #4 ADR-0009 written
<!-- AC:END -->
