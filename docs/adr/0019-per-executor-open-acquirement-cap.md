# ADR-0019: Per-executor open-acquirement cap under an executor-row lock

- Status: **Accepted** (TASK-33)
- Deviates from paper: no (an admission quota; ownership is unchanged)

## Context

A valid but rogue executor token could acquire subtasks and never acknowledge or finish them,
holding each for up to the heartbeat timeout times the free-expiry cap (about 2.5 h at defaults)
before budget is used, or up to `MINION_MAX_LEASE_S` once acknowledged. Nothing bounded how many
acquirements one token held. Token revocation stays the primary response (SPEC S-3).

## Decision

1. Each claim round, in its transaction and after `SET LOCAL statement_timeout`:
   `SELECT status FROM minion_executor WHERE executor_id = :e AND token_hash = :h FOR NO KEY
   UPDATE`; a missing, REVOKED or rotated-away row answers `401` (same path as ADR-0018). The
   touch at the start of acquire already refuses these (TASK-32); the lock re-checks them in every
   claim round, so a token revoked or rotated between rounds cannot claim.
2. Then a **separate** `SELECT count(*)` of the executor's acquirements (new index
   `ix_minion_subtask_acquirement_executor_id`, migration 0012). The count runs after the
   lock was granted, so in READ COMMITTED it takes a fresh snapshot and sees a claim that
   committed while we waited. (Only a count in the same statement as the lock would use the
   earlier snapshot; the claim CTE is a separate, later statement.)
3. `count >= MINION_MAX_OPEN_ACQUIREMENTS` (default 4, 1..64, no off switch) answers
   `429 TOO_MANY_ACQUIREMENTS` with `Retry-After: 30` before any candidate read; the round commits
   nothing. Otherwise the ADR-0013 claim statement runs unchanged.
4. Metric `minion_acquire_total{outcome="capped"}` (also timed in `minion_acquire_seconds`).

**429, not 409.** The executor raises on any 4xx other than 429 at acquire, so a 409 would kill a
legitimate executor that has a ghost acquirement. A 429 with Retry-After is already a back-off
(cap 300 s, escalation); the executor needs no change.

**Default 4.** A normal executor holds 1 (sequential loop); the rest is room for ghosts that
coexist within one heartbeat timeout: a lost acquire response (ADR-0014), a crash that orphans the
running subtask, a shed release that falls back to the watchdog.

**Hoarding bound.** At most N subtasks per token at any moment; unacknowledged ones for at most
the heartbeat timeout each, acknowledged ones for at most the max lease.

## Invariant 2 (ownership)

Unchanged. The cap can only stop an INSERT from happening; it never lets a second INSERT win. Who
owns a subtask is still decided solely by `INSERT ... ON CONFLICT DO NOTHING` on
`uq_minion_subtask_acquirement_subtask`, and no lock is taken on a subtask or acquirement before
it. The executor-row lock is a row lock on the caller's own row: it serialises only that
executor's claims, and unlike the ADR-0012 per-process limits it holds across workers and replicas.

## Lock order

Claim: executor row -> new acquirement -> subtask -> heartbeat -> task. Owner ops: acquirement ->
heartbeat -> subtask -> counter shard -> (heartbeat / presence only) executor row. No cycle: the
claim never waits on a lock an owner op holds (it inserts a new acquirement row, and ON CONFLICT
DO NOTHING waits only on an in-progress insert/delete of the same key, made by another claim
(different executor row) or by complete/release/reclaim). Those three **never write
`minion_executor`**; this is documented at `lock_executor_for_claim` and must stay true.
`FOR NO KEY UPDATE` does not conflict with the FK `KEY SHARE` locks of attempt/acquirement
inserts, so the watchdog is never blocked. Heartbeats and `touch` of the same executor wait for
its claim (milliseconds), and the wait is bounded by the acquire deadline (57014 -> 503
`ACQUIRE_DEADLINE`).

## Alternatives rejected

- **Count inside the claim statement, no lock.** Also fine for invariant 2, but not strict:
  overshoot is bounded by the executor's concurrent in-flight acquires (2 per worker times
  `MINION_SERVER_WORKERS`), so a hoarder could exceed N.
- **A per-slot unique constraint** (`executor_id, slot`): strict without a lock, but needs a
  migration with backfill and slot recycling for a LOW finding.
- **Per-process counters** (ADR-0012 style): not shared across workers or replicas.
- **Per-executor accounting of free NOT_ACKNOWLEDGED expiries:** already capped per subtask
  (ADR-0014); not added.

## Measurement (NF-1)

Knee scenario of the M6.4 run (`docs/bench/m6.4-scale-apple-m3-pro.md`): `open-minions-scale`
overlay, 4 workers, 1,000,000 PENDING seeded (100 x 10,000), 50 sims, work 2 s, heartbeat 5 s,
120 s, same Mac with the load generator; cap 64 (the statements run either way). 2,867
completions (23.8/s), 0 5xx, 0 429, 22 shed 503 (M6.4: 17). Server `minion_acquire_seconds`
(assigned, n=2,910): mean 18.7 ms, p50 <= 25 ms, p99 <= 100 ms (99.8 % <= 100 ms), the same
buckets as M6.4 (p50 <= 25 ms, p99 <= 100 ms); client p99 <= 250 ms, as M6.4. In
`pg_stat_statements`: the executor-row lock 0.030 ms and the count 0.016 ms per call (3,019
calls each), against 0.58 ms for the claim statement, so about 0.05 ms of statement time per
round (+8 % of the claim) plus two local round trips. The regression is far below the 20 %
threshold, so the count stays a separate statement (folding it into the claim CTE was not needed).
One run per variant on a shared Mac, no same-day baseline run: read it as "no visible change".

## Consequences

- Two extra indexed statements per claim round (well under 1 ms); the full-fleet acquire path
  serialises per executor only.
- An executor with ghost acquirements can see 429 for up to one heartbeat timeout after a crash
  loop; this is the intended bound.
- `minion-scale` `vanish` faults would reach the cap; `deploy/compose.scale.yml` raises it to 64 for
  M6.4 parity (set `MINION_MAX_OPEN_ACQUIREMENTS=4` to measure the cap under faults).
- Code that makes complete/release/reclaim write `minion_executor` must revisit the lock order.
