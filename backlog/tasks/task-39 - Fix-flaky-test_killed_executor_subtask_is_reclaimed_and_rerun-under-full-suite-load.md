---
id: TASK-39
title: >-
  Fix flaky test_killed_executor_subtask_is_reclaimed_and_rerun under full-suite
  load
status: In Progress
assignee: []
created_date: '2026-10-18 20:49'
updated_date: '2026-10-19 12:35'
labels:
  - test
dependencies: []
ordinal: 78000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Failed twice in full local runs (TASK-19, TASK-32): once unspecified, once reclaim reason NOT_ACKNOWLEDGED instead of HEARTBEAT_TIMEOUT (ack heartbeat raced the kill, ADR-0014). Passes in isolation. Make the test wait for the ack before killing, or accept either reason with a justified assertion.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Root cause stated
- [ ] #2 Test passes 20/20 under parallel load
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## Verdict: test-timing problem, no production bug.

### Root cause (AC #1)
The test kills executor A as soon as the `minion_subtask_acquirement` row shows up (packages/minion-executor/tests/test_e2e.py:338-339). The executor sends its ADR-0014 ack heartbeat only after the acquire response comes back (packages/minion-executor/src/minion_executor/loop.py:852-854). The uvicorn server runs in the test's own event loop, so under full-suite load the 50 ms poll in `eventually` often sees the row before the ack commits. SIGKILL then leaves `last_seen_at == acquired_at`. The watchdog subprocess runs with the default `MINION_RELEASE_LIMIT=10` (packages/minion-watchdog/src/minion_watchdog/settings.py:21), so `unacked_free_cap > 0`, and `never_acknowledged` (packages/minion-server/src/minion_server/repositories/reclaim.py:107-125) sends the reclaim to `finish_unacknowledged` (packages/minion-server/src/minion_server/services/completion.py:170-207). The result is EXPIRED / NOT_ACKNOWLEDGED. The outcome sequence still matches (`(1, EXPIRED), (2, SUCCEEDED)`), but `att[0][2] == "HEARTBEAT_TIMEOUT"` (line 369) fails. The server and watchdog are doing what ADR-0014 says. The test is the problem.

Second, smaller race (a plausible source of the earlier "unspecified" failure): the watchdog keeps running while in-process executor B re-runs the subtask (HB 1 s, timeout 4 s, lines 256-257). B shares the event loop with the server and the test, so a stall of 4 s or more could expire attempt 2 and give 3 attempts.

### Fix (single step, test-only, about 20 lines; file: packages/minion-executor/tests/test_e2e.py)
1. Add a helper next to `_inflight` (line 265):
   ```python
   async def _acked(lv: Live) -> bool:
       """ADR-0014 ack landed: a beat moved last_seen_at off the claim's acquired_at."""
       r = await lv.rows(
           "SELECT count(*) FROM minion_executor_heartbeat h"
           " JOIN minion_subtask_acquirement a USING (acquirement_id)"
           " WHERE h.last_seen_at <> a.acquired_at"
       )
       return bool(r[0][0] == 1)
   ```
   (`<>` mirrors `never_acknowledged` exactly: the claim writes both columns with the same `now()`.)
2. In `test_killed_executor_subtask_is_reclaimed_and_rerun`, after `await eventually(lambda: _inflight(live))` (line 338), add `await eventually(lambda: _acked(live), 30)` before `a.send_signal(SIGKILL)`. Update the comment: kill only after the ack, so the reclaim is a HEARTBEAT_TIMEOUT (the NOT_ACKNOWLEDGED path is covered in packages/minion-watchdog/tests/test_watchdog.py and packages/minion-server/tests/test_reclaim_races.py).
3. After `await eventually(expired, 30)` (line 346), call `await _reap(wd)`: the watchdog's job in this test is done, and stopping it means it can no longer expire B's attempt under load. `_reap` is idempotent (checks `returncode`), so the `finally` reap stays. Task settlement on B's complete happens in the complete transaction (ADR-0015, `settle_in_tx`), not in the watchdog.
4. Give the final assertions a diagnostic message, e.g. `assert [...] == [(1, "EXPIRED"), (2, "SUCCEEDED")], att` and `assert att[0][2] == "HEARTBEAT_TIMEOUT", att`, so any future flake reports itself.

Keep the strict `HEARTBEAT_TIMEOUT` assertion. Do not relax it to "either reason": once the ack has landed, NOT_ACKNOWLEDGED would be a real bug.

### Invariants
Test-only. Invariants 2/3 (the acquire INSERT decides ownership; stale writes get 409) are untouched. The test still proves watchdog reclaim plus exactly-once re-run (invariant 5).

### Verification (AC #2)
- `uv run pytest -m integration packages/minion-executor/tests/test_e2e.py::test_killed_executor_subtask_is_reclaimed_and_rerun` 20 times in a loop while CPU load runs in parallel (e.g. 10x `yes > /dev/null &`, or run the loop next to a full `uv run pytest -m "not integration"`). Expect 20/20.
- One full `uv run pytest` run green; `ruff check`, `ruff format --check`, `mypy` clean.

### Risks
None for production. If the ack never lands (executor crash before the ack), the new wait times out after 30 s with a clear `TimeoutError` instead of a wrong-reason assertion.
<!-- SECTION:PLAN:END -->
