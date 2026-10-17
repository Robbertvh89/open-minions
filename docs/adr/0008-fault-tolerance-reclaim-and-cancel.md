# ADR-0008: Fault-tolerance deviations: SKIP LOCKED reclaim, lazy cancel, max lease, executor interrupt

- Status: **Accepted** (implemented in TASK-3.1, TASK-3.2, TASK-3.3)
- Date: 2026-10-15
- Deciders: project owner
- Deviates from paper: **yes**, in the reclaim mechanism, the retry-budget accounting and the cancel
  path. The paper's watchdog deletes the acquirement with `DELETE ... RETURNING` and is silent on
  caps, cancellation and rate limits.

## Context

The paper's fault tolerance is a watchdog that scans heartbeat rows and deletes stale
acquirements. That is enough for silent executors, but the M2 review found five gaps:

1. Two watchdog replicas, a slow owner `complete`, and a reclaim can all touch one acquirement.
2. A reclaimed attempt must not reset the retry count, or a crash loop never reaches `FAILED`.
3. An executor that keeps heartbeating but never finishes can hold a subtask forever.
4. Cancelling a task that has 10^5 pending subtasks cannot be one transaction.
5. A cancelled task's running subtask has no way to learn about the cancel except through its owner.

## Decision

1. **Reclaim uses `SELECT ... FOR UPDATE SKIP LOCKED` plus a re-check, not `DELETE ... RETURNING`.**
   The watchdog scans unlocked (`ExecutorHeartbeat.last_seen_at` index, and an acquirement scan for
   the max lease). Each candidate is then locked with `SKIP LOCKED` in its own short transaction.
   If the lock is busy (an owner op or another replica holds it), the row is skipped. Under the lock
   the staleness is re-checked with a fresh statement, so a heartbeat committed in the meantime wins
   (`NOT_STALE`). Reclaim then records an `EXPIRED` attempt and drops the acquirement through the same
   `finish_failed` path that owner ops use, so every owner op and reclaim shares one lock order.
   Lock order: acquirement → heartbeat → subtask → task; the executor row is written after the
   acquirement lock (heartbeat `report_state`).
2. **Reclaim never increments `attempt`.** `attempt` keys the output object and keeps counting
   acquisitions; the next acquire bumps it, so attempt-scoped keys stay unique.
3. **Retry budget = `FAILED` + `EXPIRED` attempts.** A reclaim consumes budget. A voluntary
   `release` does not, up to `MINION_RELEASE_LIMIT` (default 10) per subtask; past the cap a release
   is recorded as a retryable `FAILED` (`RELEASE_LIMIT`), so a misclassified error cannot livelock.
4. **Hard lease.** `MINION_MAX_LEASE_S` (default 6 h) reclaims an acquirement as `EXPIRED`
   (`MAX_LEASE_EXCEEDED`) even while it heartbeats. Must exceed `MINION_HEARTBEAT_TIMEOUT_S`
   (validated at watchdog startup).
5. **Lazy two-phase cancel.** `POST .../cancel` runs txn 1 (task → `CANCELLED`) alone, then txn 2 in
   batches of `CANCEL_BATCH` (10 000) rows: `PENDING` → `CANCELLED`, leaving `RUNNING` rows to their
   owner. Owners learn through `heartbeat.cancel=true`, release, and the subtask becomes `CANCELLED`.
   A watchdog sweep (`MINION_CANCEL_SWEEP_WINDOW_S`, default 1 h) heals stragglers. Acquire
   re-checks task status under its lock and rolls back on `CANCELLED`.
6. **Heartbeat rate limit.** Beats closer than `MINION_HEARTBEAT_MIN_INTERVAL_S` (default 1 s) get
   `429 TOO_MANY_HEARTBEATS` with `Retry-After`, and are not recorded. Executors treat 429 as
   non-fatal.
7. **Executor interrupt.** On `cancel=true` the executor interrupts the worker (`WorkerHandle.interrupt`,
   hard kill, respawnable) and releases. On `409` (stale acquirement or `TASK_FINISHED`) it
   interrupts and reports nothing. The control loop keeps polling in both cases (invariant 7).

## Consequences

- Reclaim and owner ops are race-safe by row lock and re-check, not by a single `DELETE`. Two
  watchdog replicas are safe; a skipped row is picked up on the next tick.
- Reclaim consumes budget, so a subtask that keeps dying ends `FAILED` after `max_attempts`.
- `attempt` can exceed the budget count; the budget is always computed from `minion_subtask_attempt`.
- Known window: an acquire that committed just before a cancel can leave a `RUNNING` subtask under a
  `CANCELLED` task. The owner stops on its next beat; the sweep heals the rest.
- The max-lease scan is served by `ix_minion_subtask_acquirement_acquired_at` (migration 0008), so it
  stays an index range scan at M6 scale. Each tick ends with one `watchdog_tick` event
  (`duration_ms`, `rounds`) at debug level, or info when the tick takes over 1000 ms; the scan itself
  logs `over_lease_scan` (`scan_ms`, `hits`) at debug. `watchdog_reclaim` carries only counters.
- Write cost of that index: one extra btree insert per acquire and one delete per release. Rows are
  short-lived, so vacuum and bottom-up index deletion keep it small; `acquired_at` is roughly
  monotonic, so inserts hit the rightmost leaf page and stay cheap.
- Operators must keep `MINION_HEARTBEAT_TIMEOUT_S` at least 2x (recommended 3x) the executor
  heartbeat interval, or live executors are reclaimed between beats.
