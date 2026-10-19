---
id: TASK-40
title: Fix minion-scale test_sims_drain_a_queue_over_rest_only conflicts_409 flake
status: In Progress
assignee: []
created_date: '2026-10-18 21:55'
updated_date: '2026-10-19 12:35'
labels:
  - test
  - scale
dependencies: []
ordinal: 80000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Reviewer of TASK-33 saw packages/minion-scale/tests/test_sim.py::test_sims_drain_a_queue_over_rest_only fail with conflicts_409 == 1..2 (3/3 in their runs, also on main), while full local suites passed. Likely late/stop-time release 409 (cf. M6.4 B-100k 12 release 409s) counted by an assertion that expects 0. Reproduce, root-cause, fix assertion or sim.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Root cause stated
- [ ] #2 Passes 20/20
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## Verdict: the test's assertion is too strict. The 409 is a real, harmless protocol race that the production executor has too. No server or sim bug.

### Root cause (AC #1), reproduced
Reproduced on the first run in isolation (`conflicts_409 == 1`). A scratch pytest plugin that traced `Recorder.record` showed only `op=heartbeat` 409s, timestamped within about 1 ms before the matching `completed` events. No release, complete, fail or acquire call got a 409.

Mechanism: the sim's heartbeater (packages/minion-scale/src/minion_scale/sim.py:390-407) keeps beating through upload and complete and is only cancelled in `_handle`'s `finally` (sim.py:376-379). The real executor does the same on purpose ("also during _complete: the lease is kept alive until we return", packages/minion-executor/src/minion_executor/loop.py:951), so this is executor parity and must stay. When upload + complete take longer than `heartbeat_interval_s - work_s` (0.2 - 0.05 = 0.15 s), a beat is sent while the complete holds the acquirement row lock. The beat's `lock_owned` (packages/minion-server/src/minion_server/services/heartbeat.py:34-36) waits, finds the row deleted once the complete commits, and answers 409 STALE. `_call` records it under `heartbeat` (sim.py:212-219), and `conflicts_409` sums 409s over every op (packages/minion-scale/src/minion_scale/metrics.py:119-121).

Why it fails in isolation but not in full suites: the `s3` fixture is session-scoped (conftest.py:187). On a fresh SeaweedFS the first presigned POSTs are slow (the log shows "No writable volumes" while volumes grow), so the first wave of uploads goes past 0.15 s. In a full suite earlier tests have already warmed the volumes. The reviewer ran test_sim.py alone, which explains 3/3.

This is not the stop-time release 409 that the task description suspected. The traced runs had no release calls at all.

### Fix (single step, test-only, about 15 lines; file: packages/minion-scale/tests/test_sim.py:176-187)
Replace `assert rec.server_errors_5xx == 0 and rec.conflicts_409 == 0` with a justified, still-strict check:
```python
assert rec.server_errors_5xx == 0
# The heartbeater keeps beating through upload + complete (executor parity, loop.py
# "the lease is kept alive until we return"): a beat queued behind its own complete's row
# lock gets 409 once the complete commits. That is the only 409 a fault-free drain may see.
hb_409 = rec.statuses.get("heartbeat", Counter())[409]
assert rec.conflicts_409 == hb_409, rec.statuses  # none on acquire/complete/release/fail
assert hb_409 <= rec.events["completed"]  # at most one racing beat per completion
assert "complete_409" not in rec.events and "discarded_stale" not in rec.events
```
(import `Counter` from `collections`). Together with the existing `_outcomes == {"SUCCEEDED": 12}` and `completed == 12`, this proves that no ownership was lost mid-work. A 409 during `_wait` would show up as `discarded_stale`. A 409 before the complete would show up as a `complete_409` and fewer SUCCEEDED rows.

Not chosen (and why):
- Stopping the sim's heartbeater before complete would break executor parity, and the TASK-7.4 shed retries need the lease kept alive.
- Re-labelling these 409s inside the sim/Recorder would change the meaning of the scale report and hide real server answers.
- Pre-warming S3 or raising `heartbeat_interval_s` only hides the race.

### Optional docs (docs-keeper)
docs/bench/scale-report-schema.md:67: one clause noting that `conflicts_409` includes a heartbeat that raced its own successful complete (harmless, expected at fleet scale), so read `calls.heartbeat.statuses.409` separately from `complete_409` / `late_complete_409`.

### Invariants
Test-only. Invariant 3 (stale writes get 409) is exactly what the race shows. Invariants 1/4 are untouched (the sim stays REST plus presigned only).

### Verification (AC #2)
- 20 separate runs, each with a fresh session (fresh containers, which is the failing condition): `for i in $(seq 20); do uv run pytest -m integration "packages/minion-scale/tests/test_sim.py::test_sims_drain_a_queue_over_rest_only" -q || break; done` -> 20/20 (about 35 s each).
- Whole `packages/minion-scale/tests` green; ruff/mypy clean.

### Risks
None for production. A future real conflict on complete/release/fail/acquire still fails the test, now with `rec.statuses` in the message.
<!-- SECTION:PLAN:END -->
