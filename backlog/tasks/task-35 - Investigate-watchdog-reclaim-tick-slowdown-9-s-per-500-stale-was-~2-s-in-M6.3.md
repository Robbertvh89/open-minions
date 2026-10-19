---
id: TASK-35
title: >-
  Investigate watchdog reclaim tick slowdown (9 s per 500 stale, was ~2 s in
  M6.3)
status: In Progress
assignee: []
created_date: '2026-10-18 11:55'
updated_date: '2026-10-19 18:35'
labels:
  - perf
  - scale
dependencies: []
ordinal: 74000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
TASK-7.4.7 scenario C: tick 8.8-9.3 s for 500-705 stale rows; DB execution ~0.4 s, rest host round trips. Probably per-row statements (reclaim_stale loop, settle_stuck). Profile and batch.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Root cause identified with numbers
- [ ] #2 Tick for 500 stale back to <= M6.3 (2.4 s) or ADR
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## Goal (restated)
Find out why one watchdog tick that reclaims 500 stale acquirements went from 1.7-2.4 s (M6.3, docs/bench/m6-scale-apple-m3-pro.md:173) to 8.8-9.3 s (M6.4, docs/bench/m6.4-scale-apple-m3-pro.md:139), back that up with numbers (AC#1), and bring the tick back to <= 2.4 s for 500 stale rows on the same stack, or write an ADR explaining why not (AC#2). Requirements involved: FR-30/31/31a/32 (reclaim semantics must not change), NF-2/NF-3 (no double completion, no lost subtasks: verify again after the fix), ADR-0008 (SKIP LOCKED + re-check), ADR-0014 (free NOT_ACKNOWLEDGED expiries), ADR-0015 (settle_stuck backstop).

## What the code and history show (suspected cause)
Path in scenario C: sims with `--vanish 1.0` never send a heartbeat (packages/minion-scale/src/minion_scale/sim.py:349-350), so every row goes through the ADR-0014 unacked requeue path.
- Per row, reclaim_stale opens its own session and transaction (packages/minion-server/src/minion_server/services/reclaim.py:100-109). Client round trips per row today: pool pre-ping on checkout (db/session.py:27, pre_ping=True), BEGIN, lock_for_reclaim (repositories/reclaim.py:75-86), owned() join (repositories/completion.py:77-95), heartbeat_stale (reclaim.py:89-104), never_acknowledged (reclaim.py:107-125), unacknowledged_attempt_count (completion.py:164-180), then finish_attempt's INSERT/DELETE/UPDATE (completion.py:197-232), COMMIT. That is about 11 round trips and 1 commit per row.
- M6.3 (f654e0a) to M6.4 (1cda843), the reclaim path changed (git diff f654e0a 1cda843): +never_acknowledged (185ac01, ADR-0014), and unacknowledged_attempt_count took the place of failed_attempt_count. That is about 10 to 11 round trips per row (+10%). settle_stuck (0a373db, services/reclaim.py:141-169) adds 1 query per tick, plus 1 transaction per settleable task (none in scenario C). The counter upsert only runs for FAILED, which does not happen in scenario C. So the M6.4 report's explanation ("more statements per reclaim now: settle, counter select", m6.4 report:142-143) does **not** hold for this path. Migrations 0011/0012 do not touch the reclaim statements: 0012 is an index on acquirement.executor_id, and it came after the M6.4 run anyway (b168367).
- Per-row wall time went from 3.9-4.7 ms to 12.4-18.6 ms, which is about 0.4 ms vs about 1.1-1.7 ms per round trip. The ~10% extra statements cannot explain that. The remaining ~3-4x has to come from the cost of each round trip or each commit. Hypotheses to separate by measurement:
  - H1: Docker Desktop port-forward latency or host CPU contention. The M6.4 stack runs 4 server workers (compose.scale.yml MINION_SERVER_WORKERS=4) instead of 1. 500 sims ran right before; were any still running?
  - H2: COMMIT/WAL fsync cost. 500 commits per tick. The 0.4 s "DB execution" in the report lists only INSERT/UPDATE and probably leaves out COMMIT. Migration 0010 makes autovacuum on the 1M-row minion_subtask much more aggressive, and checkpoints after A stress compete for IO.
  - H3: client-side SQLAlchemy overhead per statement while the host is busy.
- Structural root cause in any case: reclaim cost is O(rows x ~11 round trips + rows x 1 commit). The per-row transaction design (ADR-0008) scales with round-trip latency, which the DB cannot hide. The fix is to batch round trips and commits. This helps under H1, H2 and H3 alike.

## Step 1: Measure (AC#1). No product code changes. Scratch script only, not committed.
Stack: scale project only. `export MINION_PG_PORT=55432 MINION_S3_PORT=59000 MINION_SERVER_HOST_PORT=58080 MINION_GRAFANA_PORT=53000`; `docker compose -p open-minions-scale -f deploy/compose.yml -f deploy/compose.scale.yml --profile app up -d --build --wait postgres s3 s3-init migrate server watchdog`. Never use port 8080. Never touch the open-minions_* dev volumes. Writes need the loopback guard (minion-scale cli.py:129).
1. Seed 1M PENDING like M6.3/M6.4 (`minion-scale seed ... --tokens-file <0600>`). Stop the container watchdog (`docker compose -p open-minions-scale ... stop watchdog`).
2. Make the stale rows: `minion-scale run --sims 500 --vanish 1.0 --acquire-pace-s 100000 --max-duration-s 150`, plus a top-up to exactly 500. Wait > 20 s. Before every tick, check that no sim is still running and the server is idle (pg_stat_activity shows only our session; /metrics request counters flat). This rules out H1-by-load.
3. Instrumented tick (scratch script in the session scratchpad, run with `uv run python -I`). It calls minion_watchdog.main.tick with WatchdogSettings(heartbeat_timeout_s=20, watchdog_batch_size=500). SQLAlchemy events `before/after_cursor_execute`, pool `checkout`, and `after_begin`/commit timing give: number of statements per category, client ms per statement, ms per commit, and tick wall time.
4. Server-side: `pg_stat_statements_reset()` before each tick. Afterwards read **all** entries, including BEGIN/COMMIT (track=all, track_utility on). On the scale DB only: `ALTER SYSTEM SET track_wal_io_timing = on; SELECT pg_reload_conf()`. Take pg_stat_wal (wal_sync, wal_sync_time) and pg_stat_checkpointer/bgwriter before and after. Sample pg_stat_progress_vacuum / autovacuum workers during the tick.
5. Round-trip baseline: 1000 x `SELECT 1` from the host via 55432, and the same from inside the compose network (`docker compose -p open-minions-scale ... run --rm --no-deps watchdog python -c ...`).
6. Cases, n=3 each, report the median and range: (a) current code from the host; (b) MINION_RELEASE_LIMIT=0, i.e. the M6.3-equivalent statement path (no never_acknowledged); (c) current code inside the network (watchdog container image); (d) diagnostic only: watchdog session with `server_settings={"synchronous_commit": "off"}` to isolate fsync; (e) optional: MINION_SERVER_WORKERS=1.
7. EXPLAIN (ANALYZE, BUFFERS) on the per-row statements: lock, owned join, heartbeat_stale, never_acknowledged, unacknowledged/failed_attempt_count, the finish_attempt writes (in BEGIN ... ROLLBACK), settleable_tasks (completion.py:314-334) and sweep_cancelled (tasks.py:341-385). `minion-scale explain` only covers the stale/over_lease scans (explain.py:59-64). Run the rest from the scratch script so minion-scale stays unchanged.
8. Cleanup: `minion-scale cleanup` (revokes the scale tokens), `ALTER SYSTEM RESET track_wal_io_timing`, then `docker compose -p open-minions-scale -f deploy/compose.yml -f deploy/compose.scale.yml down -v` (scale volumes only).
Deliverable: a table (round trips per row, client ms per round trip, DB exec ms, commit ms, wal_sync_time, tick wall) per case, and a root-cause sentence in the task notes.
Decision gate: if one statement dominates (e.g. a bad plan in settleable_tasks or the attempt counts), fix that instead of or before Step 2-3. Steps 2-3 assume round trips/commits dominate (expected from the numbers above).

## Step 2: Batched reclaim repository (~150 lines). File: packages/minion-server/src/minion_server/repositories/reclaim.py
- `async def lock_batch(session, ids: Sequence[uuid.UUID]) -> list[uuid.UUID]`:
  `SELECT acquirement_id FROM minion_subtask_acquirement WHERE acquirement_id = ANY(:ids) ORDER BY acquirement_id FOR UPDATE SKIP LOCKED`.
- `@dataclass(frozen=True) class ReclaimRow: owned: Owned; stale: bool; never_acked: bool; over_lease: bool; budget_used: int; unacked_used: int`
- `async def classify_locked(session, ids, *, timeout_s: float, max_lease_s: float | None) -> list[ReclaimRow]`: **a separate statement after the lock**, so under READ COMMITTED it takes a new snapshot and sees any beat committed before the lock (the same argument as heartbeat_stale today; a beat after the lock is impossible because beat holds the acquirement lock, heartbeats.py:17). acquirement JOIN subtask JOIN task, LEFT JOIN heartbeat. Select-list: the Owned fields; `coalesce(h.last_seen_at < cutoff, false)` (a missing heartbeat counts as not stale, like heartbeat_stale today); `coalesce(h.last_seen_at = a.acquired_at, false)`; `a.acquired_at < lease_cutoff` (false if max_lease_s is None); two correlated counts on ix_minion_subtask_attempt_subtask that reuse BUDGET_OUTCOMES and _unacked_expiry() from repositories/completion.py (import, do not modify).
- `async def finish_reclaimed(session, items: Sequence[Finish]) -> None`. Finish = (Owned, outcome, subtask_status, error, attempt_error), with the same meaning as finish_attempt's parameters. One round trip per write kind per chunk: INSERT attempts (multi-values), `DELETE ... WHERE acquirement_id = ANY(:ids)` (cascades the heartbeat), UPDATE subtask (executemany or UPDATE ... FROM unnest). Counter shards for FAILED rows: aggregate per (team, project, task_name, shard), **sort**, and call completion_repo.bump_counters(o, succeeded=0, failed=n) per shard. That path is rare.
- Lock order stays acquirement -> heartbeat -> subtask -> counter shard (ARCHITECTURE §4). Acquirements are taken in sorted order with SKIP LOCKED, so the lock never waits. Subtasks are only reachable through acquirements we hold. Shards are taken in sorted order, and owner ops lock 1 shard last, so there is no cycle with owner ops or another replica. The watchdog writes no minion_executor (ADR-0019 rule intact).
- Tests (integration, packages/minion-server/tests/test_reclaim_batch.py): classify returns correct flags/counts for fresh, stale, never-acked, over-lease, and budget-used rows; lock_batch skips a row locked by another transaction (held FOR UPDATE in a second connection).

## Step 3: Service and chunking (~150 lines + tests). File: packages/minion-server/src/minion_server/services/reclaim.py
- Pure `decide(row: ReclaimRow, *, unacked_free_cap: int) -> Finish | ReclaimResult`. Same rules as today, in this order: not stale and not over lease -> NOT_STALE. Stale, never acked, cap > 0 and unacked_used < cap -> EXPIRED/NOT_ACKNOWLEDGED, PENDING or CANCELLED, no budget. At the cap, log `unacked_cap_reached` and fall through to HEARTBEAT_TIMEOUT. Stale -> HEARTBEAT_TIMEOUT. Over lease -> MAX_LEASE_EXCEEDED. Budget statuses go through `next_status(budget_used + 1, ...)` from services/completion.py:132. One source of truth: reuse next_status; do not copy it.
- `async def reclaim_chunk(session, ids, *, timeout_s, max_lease_s=None, unacked_free_cap=0) -> dict[uuid.UUID, ReclaimResult]`: lock_batch -> classify_locked -> decide -> finish_reclaimed, inside the caller's transaction. Ids not locked are SKIPPED.
- `reclaim_one(...)` keeps its exact signature (used by test_protocol_races.py:432, test_acquire_cap.py:247, test_cancel.py:460, test_reclaim_races.py:123/316/585). It becomes `reclaim_chunk(session, [id])[id]`, so the existing race tests run the new code.
- `reclaim_stale`: same scan (services/reclaim.py:87-98). Then one transaction per chunk of `RECLAIM_CHUNK = 100` ids (a constant, not a new env var). If a chunk raises, roll back and retry that chunk row by row with reclaim_one, each in its own transaction, logging `reclaim_failed` per bad id. That keeps the "one bad row does not block the batch" property. Same Counter result and `subtask_reclaimed` logs.
- `finish_unacknowledged` (services/completion.py:170-207) becomes unused. Delete it in this step, or leave it for a follow-up to stay out of completion.py (see parallel note). Owner ops (complete/fail/release, finish_failed, finish_attempt) stay **unchanged**.
- Tests:
  - unit, test_reclaim_decide_unit.py: a table for decide() that matches next_status and the ADR-0014 cap boundary.
  - integration:
    - two concurrent reclaim_stale calls (2 replicas) over 200 stale rows while heartbeats and completes run on some of them: every acquirement ends in exactly 1 attempt row, beaten rows are NOT_STALE, no 40P01, counters match subtask statuses;
    - a beat committed after the scan but before the lock gives NOT_STALE in the chunk path;
    - a poisoned chunk: update test_reclaim_races.py:539-570 to monkeypatch `reclaim_chunk` so it raises when the poisoned id is in `ids`; the other 3 are still reclaimed;
    - a round-trip guard: count statements via a SQLAlchemy `before_cursor_execute` listener for reclaim_stale over 250 stale rows and assert <= ~12 per chunk plus the scan (bounded, not timed: no flaky perf test);
    - the existing test_watchdog.py suite (tick, round cap, release_limit, SIGTERM) passes unchanged.
- Invariants: 2 (ownership is still only the acquire INSERT; reclaim only deletes under a row lock), 3 (stale owners get 409 because the acquirement row is gone), 5 (attempt not touched, outputs stay attempt-keyed). Invariant 1 does not apply (server-side process).

## Step 4: Re-measure, verify, docs
- Rerun the Step 1 matrix with the fix (n=3; cases a and c). Target: tick <= 2.4 s for 500 stale (median and max). Also run a short B chaos drain (20k-100k, vanish/release/fail/late) plus `minion-scale verify` to show NF-2/NF-3 still PASS with chunked reclaim. Then `minion-scale cleanup` and `down -v` on open-minions-scale.
- Docs:
  - new docs/bench/task35-watchdog-tick-apple-m3-pro.md/.json (before/after, decomposition);
  - a correction note in docs/bench/m6.4-scale-apple-m3-pro.md:139-145 (the misattributed cause);
  - ARCHITECTURE §3.4 pseudo-code (docs/ARCHITECTURE.md:464-480: per-chunk transaction, lock batch then fresh classify) and the §4 hazard row "Two watchdog replicas" (:572);
  - SPEC FR-31 implementation status line (docs/SPEC.md:236) if it mentions per-row;
  - an **ADR-0008 addendum** (reclaim moves from one transaction per acquirement to one per chunk of 100, still with SKIP LOCKED and a fresh re-check under the lock). This is not a deviation from the paper, so an addendum is enough instead of a new ADR. No new env var, so the CLAUDE.md commands section is unchanged.
- If the target is missed because of round-trip latency that cannot be reduced (e.g. Docker port forward on the host), write an ADR with the measured floor, as AC#2 allows.

## Risks / open questions
- A chunk transaction holds up to 100 acquirement locks for a few ms. A late heartbeat or complete of such an owner waits that long and then gets 409. That is the same outcome as today.
- `now()` in a chunk is the transaction start, so the cutoff is a few ms earlier than per row. This is conservative: it never reclaims a fresh row.
- Multi-row INSERT ... ON CONFLICT is not used for counters (to avoid "cannot affect row a second time"). They are pre-aggregated and sorted instead.
- The M6.3/M6.4 numbers are n=1. Step 1 uses n=3 so the regression itself is confirmed before it is "fixed".
- Should RECLAIM_CHUNK be a setting? No for now (no speculative knobs). 100 bounds the lock time and the size of a rollback.

## Parallel-work footprint
Changes: packages/minion-server/src/minion_server/repositories/reclaim.py, services/reclaim.py, optionally services/completion.py (only to delete finish_unacknowledged; can be deferred), tests test_reclaim_races.py (1 test) plus new test files, docs (ARCHITECTURE §3.4/§4, ADR-0008 addendum, bench reports). Probably no source change in packages/minion-watchdog (tick() calls reclaim_stale unchanged). No migrations, no minion-common/executor/scale code, no owner-op SQL (repositories/completion.py untouched).
<!-- SECTION:PLAN:END -->
