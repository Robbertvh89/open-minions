---
id: TASK-2.3
title: 'M1.3: Executor loop, worker subprocess, end-to-end test'
status: In Progress
assignee: []
created_date: '2026-10-15 14:26'
updated_date: '2026-10-15 17:30'
labels:
  - M1
dependencies:
  - TASK-2.2
parent_task_id: TASK-2
priority: high
ordinal: 11000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Plan steps C1, C2, E1 of TASK-2: Runner protocol + fake runner, spawned worker subprocess, static profile budgets; control loop (backoff, 409 discard, BAD_INPUT, worker crash -> retryable fail, SIGTERM release), CLI run wiring; E2E test with 2 executors; ADR-0004 and ADR-0007; SPEC/ARCH/CLAUDE.md updates.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 E2E: task submitted -> split -> 2 executors with fake runner -> all outputs promoted in S3, no duplicates
- [x] #2 Worker crash reported as retryable fail; SIGTERM releases current subtask
- [x] #3 ADR-0004 and ADR-0007 written; docs match code
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Docs sync (TASK-2.3 docs step): ADR-0004 written (byte-range slicing, was ARCH §5.2 / SPEC Q1); ADR-0007 set to Accepted after verifying auth.lookup_user full scan and startup load in app.py. ARCH §2.2 gained an 'Executor implementation (M1)' block (control loop, error→fail mapping, stop/release, backoff, worker IPC, profiles, token/CLI); ARCH §9 index updated. SPEC §4.2 and §4.6 gained implementation-status notes (FR-17 no grace period, FR-51 static only, toml and custom not built). CLAUDE.md run/bench lines corrected. Key files: loop.py, main.py, worker/process.py, capacity/profiles.py, secrets.py, runners/fake.py, minion_server/splitting.py, minion_server/auth.py. Deviations: structlog dep; Keychain via /usr/bin/security; hidden --runner (default mlx, exits 2 until M3; `fake` needs MINION_EXECUTOR_DEV=1; Engine.FAKE added via migration 0005); backoff exponent cap 2^16 and 5–30 s jittered delay; FakeRunner oom admission and sleep_s (<=60 s); WorkerHandle 600 s call timeout; cpu nice only, not in Budget schema.

Fix round: release only for connect/timeout/network/remote-protocol errors and local OSError; S3 HTTP status errors, non-206 and short bodies are retryable fail (release would livelock, it burns no retry budget); complete retries exhausted -> best-effort release. Worker env is an allowlist (scrub_env) and main calls restrict_environ() after httpx clients are built so spawn children inherit no secrets. WorkerHandle.kill/spawn race closed by re-checking _closed after assignment.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Executor M1 loop, worker subprocess, static profiles and e2e test are in place; docs synced. ADR-0004 (byte-range slicing) written and ADR-0007 marked Accepted after code check. Open doc/code gaps are recorded in SPEC §4.2/§4.6 and ARCH §2.2: FR-17 has no 30 s grace, backoff is 5–30 s not 10–30 s, FR-51 toml and custom profiles are not built, and only the fake runner exists (MLX is M3).
<!-- SECTION:FINAL_SUMMARY:END -->
