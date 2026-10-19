---
id: TASK-33
title: Per-executor limit on open acquirements
status: Done
assignee: []
created_date: '2026-10-18 08:03'
updated_date: '2026-10-19 06:58'
labels:
  - security
dependencies: []
ordinal: 72000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Security LOW from TASK-7.4.5: a rogue executor with a valid token can acquire subtasks and never ack, holding each up to cap x heartbeat_timeout (~2.5 h at defaults) before budget is used; acked executors can already hold until max_lease_s. Add a per-executor cap on concurrently open acquirements (server-side, checked in the claim path) and/or count free unacked expiries per executor; token revocation stays the primary response.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Executor with N open acquirements gets 429/409 on further acquires (configurable N, default sized for normal executors)
- [ ] #2 Test: hoarding executor cannot hold more than N subtasks
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## Goal (PR 2 of 2, after TASK-32)
A valid-but-rogue executor token must not hoard subtasks. The server caps the acquirements one executor holds open at once, checked in the claim transaction. Default N = 4. Above it, acquire answers `429 TOO_MANY_ACQUIREMENTS` + Retry-After. SPEC: FR-20/21 (ownership unchanged), FR-31/31a/ADR-0014 (hold durations), S-3 (revocation stays primary), NF-1 (no measurable acquire cost).
AC1 -> setting `MINION_MAX_OPEN_ACQUIREMENTS` + 429. AC2 -> concurrency test: one executor firing concurrent acquires never holds more than N.

## Touchpoints
- services/acquisition.py:63-98, the round txn: connection -> deadline check -> `set_statement_timeout` -> `repo.claim`.
- repositories/acquisition.py:290-407 `claim` (ADR-0013 statement; **left unchanged**).
- api/executor.py:154-204 acquire handler (in-flight cap, outcome labels).
- db/models.py:251-258 acquirement indexes (none on executor_id; FK at :266).
- metrics.py:112-118 outcome help. settings.py. executor loop.py:128 `_is_shed`, :609-624 (429 = back-off; any other 4xx raises).
- minion-scale sim.py `vanish` fault (keeps acquiring while abandoned acquirements wait for the watchdog).

## Design
In each claim round's transaction, after `set_statement_timeout` (so lock waits are bounded by the acquire deadline -> 57014 -> 503 ACQUIRE_DEADLINE) and before the claim statement:
1. `SELECT status FROM minion_executor WHERE executor_id = :e FOR NO KEY UPDATE`. A missing or REVOKED row -> 401, which also makes revocation immediate for acquire, on top of TASK-32's TTL.
2. `SELECT count(*) FROM minion_subtask_acquirement WHERE executor_id = :e`. It has to be a *separate statement*: in READ COMMITTED a count inside the locking statement (or inside the claim CTE without the lock) uses the snapshot taken before the lock wait and would miss a concurrent claim that just committed.
3. count >= N -> 429 before any candidate read. Otherwise the existing claim statement runs, unchanged.

Why this does not break invariant 2 (please confirm in review / ADR-0018): the cap is an admission quota per executor, not an ownership decision. It can only stop an INSERT from happening. It never lets a second INSERT win. Who owns a subtask is still decided only by `INSERT ... ON CONFLICT DO NOTHING` on `uq_minion_subtask_acquirement_subtask`, and no lock is taken on any subtask or acquirement before it. A count check in the same statement *without* a lock would also be acceptable for invariant 2, but it is not strict: overshoot <= concurrent in-flight acquires of that executor (2 per worker x MINION_SERVER_WORKERS), which fails AC2. The executor-row lock is a plain DB row lock on the caller's own row, it serialises only that executor's claims, and unlike the ADR-0012 per-process limits it holds across workers and replicas.
Lock order (deadlock argument, add to ARCHITECTURE §4): the claim takes executor row -> new acquirement -> subtask -> heartbeat (new row) -> task. Owner ops take acquirement -> heartbeat -> subtask -> counter -> executor row (heartbeat `report_state`, heartbeat.py:55). No cycle, for two reasons. First, the claim never waits on a lock an owner op holds: it inserts a new acquirement row, and ON CONFLICT DO NOTHING waits only for an in-progress insert/delete of the same key, done by another claim (which holds a different executor row) or by complete/release/reclaim (which never write minion_executor). Second, FOR NO KEY UPDATE does not conflict with the FK KEY SHARE locks taken by attempt/acquirement inserts that reference the executor, so the watchdog is never blocked. The heartbeats and the `touch` of the same executor wait for its claim txn (milliseconds). Only a client running concurrent acquires sees this.

429 vs 409: **429**. On acquire, the executor raises (and exits) on any 4xx other than 429 (loop.py:609-612), so a 409 would kill a legit executor that has a ghost acquirement. A 429 with Retry-After is already handled (back-off, cap 300 s, escalation; same in the sim), so the executor needs no change. Code `TOO_MANY_ACQUIREMENTS`, Retry-After fixed at 30 s: a slot frees on complete or when the watchdog expires a ghost after heartbeat_timeout. Metric: `minion_acquire_total{outcome="capped"}`, also timed in `minion_acquire_seconds`.
Default N = 4: a normal executor holds 1 (sequential loop), plus room for up to 3 ghosts that coexist within one heartbeat_timeout (900 s). Ghosts come from a lost acquire response (ADR-0014, NOT_ACKNOWLEDGED), a crash/restart that orphans the running subtask, or a shed release falling back to the watchdog. Range 1..64, no off switch. Hoarding bound: <= N subtasks per token at any moment, unacked ones <= heartbeat_timeout each, acked ones <= max_lease (6 h). Free NOT_ACKNOWLEDGED expiries are already capped per subtask (ADR-0014); per-executor expiry accounting is not added.

## Steps
### Step 1: migration + repo + service + handler (implementer, db-migration skill, about 200 lines)
- NEW `migrations/versions/0012_acquirement_executor_index.py`: `ix_minion_subtask_acquirement_executor_id` on `(executor_id)`, same pattern as 0008 (`SET LOCAL lock_timeout '5s'`, plain CREATE INDEX on a small table) + downgrade; `db/models.py` `__table_args__` Index. The count must not seq-scan at 10k running subtasks; the fleet join on executor_id benefits too.
- repositories/acquisition.py: `async def lock_executor_for_claim(session, executor_id) -> bool` (False = missing or REVOKED; uses `_lit(...)`-style literal or a bind, `.with_for_update(key_share=False, read=False, no_key=True)` -> FOR NO KEY UPDATE); `async def count_open_acquirements(session, executor_id) -> int`.
- errors.py: `capped_error() -> HTTPException` (429, `TOO_MANY_ACQUIREMENTS`, `Retry-After: 30`).
- services/acquisition.py round body: after `set_statement_timeout`: lock (False -> `api_error(401, "UNAUTHORIZED", ...)`), count (>= `settings.max_open_acquirements` -> raise `capped_error()`), then the claim. Every round does this, since the lock is transaction-scoped.
- api/executor.py: in `except HTTPException`, status 429 -> outcome `"capped"` (counted + timed). Leave the per-process in-flight cap (:154) as is; it protects the slots and is orthogonal.
- settings.py: `max_open_acquirements: int = Field(default=4, ge=1, le=64)` (env `MINION_MAX_OPEN_ACQUIREMENTS`, comment: per executor, DB-enforced across workers).
- metrics.py help text: add `capped`.
- Unit: `test_settings.py` range; `test_metrics.py` label; executor `tests/test_loop*`: a 429 `TOO_MANY_ACQUIREMENTS` + Retry-After 30 backs off >= 30 s and does not raise (check whether an existing generic 429 test already covers it).

### Step 2: integration + concurrency tests (test-engineer), `tests/test_acquire_cap.py` (marked integration)
- **Concurrency (AC2), service level:** max_open_acquirements=3, seed 20 subtasks, one principal, `asyncio.gather` of 20 `acquisition.acquire(...)` calls on 20 separate sessions (no HTTP, so the per-process in-flight cap does not hide the race). Assert exactly 3 assignments, 17 raise 429 TOO_MANY_ACQUIREMENTS, `count(*) WHERE executor_id` = 3. Loop 10 iterations (clean up between them). Add a check that fails without the lock: monkeypatch `lock_executor_for_claim` to a no-op and assert overshoot can happen; optional/xfail if flaky. It documents why the lock exists.
- **HTTP multi-worker:** 3 apps via `make()` on the same DB (simulating MINION_SERVER_WORKERS=3), `_slow_preference` (as in test_overload.py:274) so the acquires overlap, 2 concurrent acquires per app with the same token, N=2: total 200s == 2, the rest 429 (TOO_MANY_ACQUIREMENTS or the in-flight 429), DB count == 2.
- A capped acquire does no candidate read (statement listener as in test_overload.py:209: no `minion_subtask` SELECT after the count) and commits nothing.
- The slot frees: complete or release one -> the next acquire is 200. A watchdog `reclaim_one` of an unacked one -> 200.
- Another executor is unaffected by the first one's cap.
- A revoked executor (TASK-32 cache warm, TTL 60) gets 401 on acquire immediately (lock query); heartbeat on its existing acquirement stays within the TASK-32 TTL bound.
- Acquire concurrency race tests (test_protocol_races, >= 50 executors) stay green. Deadline: a lock wait past the deadline -> 503 ACQUIRE_DEADLINE (hold the executor row FOR UPDATE in a side connection).
- `test_migrations.py` upgrade/downgrade 0012; an index-use test like test_acquired_at_index.py.

### Step 3: docs (docs-keeper)
- SPEC §4.3: note FR-23 (new) "per-executor open-acquirement cap" (or under §6 overload paragraph); §6 error codes (`TOO_MANY_ACQUIREMENTS` 429); S-3 (acquire stops immediately on revoke).
- ARCHITECTURE §3.2 diagram (lock + count before the claim statement), §4 lock order paragraph + hazard row "rogue executor hoards subtasks", §6 note (DB-enforced, holds across workers, unlike ADR-0012).
- NEW ADR-0018 "Per-executor open-acquirement cap under an executor-row lock" (invariant-2 argument, the same-statement alternative and its overshoot bound, slot-unique-constraint alternative rejected: migration + backfill for a LOW finding). ADR-0013: one-line cross-reference. CLAUDE.md minion-server line: `MINION_MAX_OPEN_ACQUIREMENTS (4, 1..64)`. docs/bench/scale-report-schema.md if the sim counts the new 429 separately.

## Invariants
- 2: kept, as argued above. The reviewer must confirm this explicitly; the ADR records it. 3, 5: untouched. 1, 4: n/a. 6/7: the executor needs no change; the back-off honours Retry-After.

## Risks
- Two extra round trips per claim round (lock + indexed count, well under 1 ms) against NF-1. The verifier should run a short `minion-scale run` (50 sims) and compare acquire p99 with M6.4, attaching the numbers to the PR.
- minion-scale `vanish` faults: a sim keeps acquiring while its abandoned acquirements wait for the watchdog, so with N = 4 sims can hit the cap (realistic, but the report changes). Either count `acquire_429_capped` in the sim or set `MINION_MAX_OPEN_ACQUIREMENTS` higher in compose.scale.yml for M6.4 parity. Decide in the PR.
- Deadlock argument rests on "complete/release/reclaim never write minion_executor" (true today: only heartbeat/presence/register/touch do). Add a comment at `lock_executor_for_claim` so future owner-op changes respect it.
- ADR numbering: 0018 assumes TASK-32 takes 0017 (TASK-19 is also in flight).
<!-- SECTION:PLAN:END -->
