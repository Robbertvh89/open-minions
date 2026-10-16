---
id: TASK-4.2
title: 'M3.2: Model cache, fetch subprocess and worker sandbox'
status: In Progress
assignee: []
created_date: '2026-10-15 20:42'
updated_date: '2026-10-16 00:52'
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
- [x] #1 Cache: pinned revision, safetensors-only allow-patterns, sha256 verified, LRU eviction under MINION_EXECUTOR_MODEL_CACHE_GB, cancel/SIGTERM kills fetch
- [x] #2 Worker is offline and sandboxed: cannot read HOME secrets, open non-loopback sockets or reach securityd; fail-closed if sandbox_init fails
- [x] #3 HF_TOKEN/proxy/CA reach only the fetch subprocess, never the worker or logs
- [x] #4 ADR-0009 written
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Files: packages/minion-executor/src/minion_executor/model_cache.py (pinned revision, allow-list, sha256/blob-sha1 verify, LRU under MODEL_CACHE_GB, free-disk reserve, flock pinning, staging poll, local-failure release + TTL memory, fetch subprocess with network/unix-socket/fork/bind sandbox, HF_TOKEN/proxy/CA only to fetch child); worker/sandbox.py (Seatbelt profile: deny-default writes, mach allow-list, no exec/fork/network except Ollama port, procargs/process-info denied, read-deny HOME/Users/Volumes/tmp/Library/Homebrew data, self-test exit 78 -> SANDBOX_FAILED); worker/process.py (offline env, sandboxed spawn); settings.py (MODEL_CACHE_*, MIN_FREE_DISK_GB, DOWNLOAD_TIMEOUT_S, SANDBOX/FETCH_SANDBOX dev-only); loop.py (release/fail mapping); pyproject.toml (optional extra mlx); conftest.py + pyproject.toml (macos/model markers). ADR-0009 Accepted. SPEC S-10 and FR-12 table, ARCHITECTURE 2.2/9, .env.example, CLAUDE.md updated.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Model cache and fetch subprocess (pinned, verified, LRU, disk-reserved) and the offline Seatbelt-sandboxed worker are implemented; fetch and worker sandboxes fail closed (SANDBOX_FAILED -> release). ADR-0009 accepted. Known doc/code gap: SPEC §4.2 says a too-large model is remembered 1 h, code uses 10 min for all MODEL_CACHE_FULL.
<!-- SECTION:FINAL_SUMMARY:END -->
