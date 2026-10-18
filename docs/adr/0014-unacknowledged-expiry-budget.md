# ADR-0014: Unacknowledged EXPIRED attempts do not consume the retry budget (capped)

- Status: **Accepted** (TASK-7.4.5, M6.4e)
- Deviates from paper: yes (FR-31/FR-32 budget accounting)

## Context

The paper counts every `EXPIRED` attempt against `max_attempts` (SPEC FR-31, FR-32). Since
TASK-7.4.4 the server no longer commits an acquire after its deadline, but one case remains: the
acquire commits and the response is lost (network drop, client timeout). The executor never learns
it owns the subtask, never heartbeats, and the watchdog expires it after `heartbeat_timeout`. Under
overload this repeats, and a `max_attempts = 1..3` subtask runs out of budget without any executor
ever having started it.

## Decision

- The claim transaction inserts the heartbeat row with `last_seen_at = now()` and the acquirement
  with `acquired_at = now()` in the same transaction, so both are equal until the first beat. `beat`
  sets `last_seen_at = now()` in a later transaction, so the two differ after any heartbeat.
- Reclaim under the acquirement lock: a stale heartbeat whose `last_seen_at = acquired_at` was
  **never acknowledged**. It is archived as `EXPIRED` with error code `NOT_ACKNOWLEDGED`, subtask
  back to `PENDING`, and it does **not** count against `max_attempts`, as long as the subtask has
  fewer than `MINION_RELEASE_LIMIT` (default 10) earlier `NOT_ACKNOWLEDGED` attempts. Past that cap
  the expiry is recorded as an ordinary `EXPIRED` / `HEARTBEAT_TIMEOUT` attempt and counts, so a
  subtask that is never acknowledged is bounded (no livelock). The free count is its own counter
  (not shared with `RELEASED`).
- Budget accounting (`failed_attempt_count`) counts `FAILED` + `EXPIRED` except `EXPIRED` attempts
  with code `NOT_ACKNOWLEDGED`. The free count is `count(*) FROM minion_subtask_attempt WHERE
  subtask key AND outcome = 'EXPIRED' AND error->>'code' = 'NOT_ACKNOWLEDGED'`. No schema change.
  A cancelled task still ends the subtask `CANCELLED`.
- Max-lease expiries (`MAX_LEASE_EXCEEDED`) are unaffected: an acquirement that passes the lease
  check on a never-beaten row is still the unacknowledged case only when the heartbeat is stale;
  the lease path keeps counting.
- Executor ack: right after a successful acquire and before the worker starts, the executor sends
  an immediate heartbeat. 429 / 5xx / transport errors on the ack are non-fatal (regular heartbeats
  follow); `409` means the assignment is stale and it is dropped without running. The server exempts
  a never-beaten acquirement from the heartbeat minimum interval, so the ack is not rate limited.
- The watchdog runs the same `reclaim_one` service, so it inherits this behaviour.
- Dashboards read `error_code` from the attempt history; `NOT_ACKNOWLEDGED` shows up there with no
  view change.

## Consequences

- Old executors (without the ack) heartbeat only after the interval. If they die before the first
  beat they get free retries up to the cap, then count: a bounded loss of strictness, not a
  correctness issue. An old executor that dies after its first beat counts as before.
- A real executor that crashes within the first interval after claiming (before the ack) is also
  treated as unacknowledged; with the ack it is a few milliseconds of exposure.
- Worst case per subtask: `MINION_RELEASE_LIMIT` extra attempts from ghost acquires.
