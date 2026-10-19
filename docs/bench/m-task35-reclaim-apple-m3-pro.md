# TASK-35 watchdog reclaim tick, before and after chunked reclaim: Apple M3 Pro

TASK-35 steps 1 and 4. Raw data: [`m-task35-reclaim-apple-m3-pro.json`](m-task35-reclaim-apple-m3-pro.json)
(every tick run with its statement counts, commit/WAL numbers and, for two runs, the full
`pg_stat_statements` list). Harness: `packages/minion-scale` plus a scratch instrumented tick (not
committed: it wraps `minion_watchdog.main.tick` with SQLAlchemy cursor events, a timer around the
asyncpg commit and `pg_stat_statements` / `pg_stat_wal` deltas). Run on 2026-10-19, Mac otherwise idle.

BEFORE = `origin/main` (3295544), AFTER = branch `perf/task-35-chunked-reclaim` (06fc70b, one transaction
per chunk of 100 acquirements, about 5 statements per chunk).

## Setup

- Apple M3 Pro, 12 cores; Docker Desktop 27.4.1, VM 12 CPUs. Compose project `open-minions-scale`
  (`deploy/compose.scale.yml`), PostgreSQL 16 with `shared_buffers` 1 GB, `pg_stat_statements.track=all`,
  `track_io_timing` on; `track_wal_io_timing` was switched on with `ALTER SYSTEM` for the tick runs and
  reset afterwards (the volumes were dropped anyway).
- 1,000,000 PENDING subtasks (100 tasks x 10,000). Per run: 500 sims with `--vanish 1.0 --acquire-pace-s 100000`
  each acquire once and walk away (exactly 500 rows in `minion_subtask_acquirement`), wait 25 s (timeout 20 s),
  container watchdog stopped, no other active session (`pg_stat_activity` checked: 0 in every run), then ONE
  `minion_watchdog.main.tick` (batch 500, `heartbeat_timeout_s` 20, `release_limit` 10 unless stated; 500 rows reclaimed in every run).
- "host": tick from the Mac through the 55432 port forward (as in M6.3/M6.4). "net": the same script in a
  container on the compose network (image built from the same checkout).
- Logging was set to WARNING in the instrumented tick (the M6.4 tick logged 500 `subtask_reclaimed` lines at
  DEBUG/INFO). A 20 Hz `pg_stat_activity` sampler (autovacuum check) ran in the same event loop; two extra
  BEFORE runs without it show it makes no difference (`before-nosample`).
- n = 3 per case (the 3 AFTER host runs: the first had debug logging on and no commit timing; it is
  included, its tick was 196 ms).

## Result (AC#2): tick for 500 stale rows

| Case | BEFORE ticks (ms) | median | AFTER ticks (ms) | median | Target <= 2,400 ms |
|---|---|---|---|---|---|
| host, current code (case a) | 8,158 / 9,230 / 2,574 | **8,158** (max 9,230) | 196 / 187 / 189 | **189** (max 196) | AFTER passes, BEFORE misses |
| in network (case c) | 1,100 / 1,307 / 1,267 | 1,267 (max 1,307) | 116 / 121 / 118 | 118 (max 121) | both pass |
| host, `MINION_RELEASE_LIMIT=0` (M6.3 statement path, case b) | 9,235 / 7,116 / 9,218 | 9,218 | 325 / 205 / 204 | 205 (max 325) | |
| host, `synchronous_commit=off` (case d, diagnostic) | 8,239 / 7,046 / 8,165 | 8,165 | 193 / 212 / 198 | 198 | |
| host, BEFORE without the sampler (n=2) | 9,095 / 3,013 | | | | |

AFTER meets the target with a wide margin: median and max for the host case are 189 and 196 ms
(12x under 2.4 s), 0.38 ms per row. The slowest AFTER run of all 12 was 325 ms. AFTER is insensitive to
the host speed state described below: all 12 AFTER ticks are within 116 to 325 ms.

## Root cause (AC#1)

Per 500 stale rows (median of the runs; the JSON has all of them):

| | BEFORE host | BEFORE net | AFTER host | AFTER net |
|---|---|---|---|---|
| Client statements | 4,006 (8 per row) | 4,006 to 4,487 | 31 | 31 |
| Server calls incl. BEGIN/COMMIT/ROLLBACK (excl. FK trigger internals) | 6,106 | 6,026 to 6,516 | 71 to 91 | 71 |
| Commits | 501 | 501 | 6 | 6 |
| Server execution total (`pg_stat_statements`) | 323 / 374 / 93 ms | 62 to 74 ms | 36 to 39 ms | 22 ms |
| Commit time (client) | 600 / 680 / 196 ms (1.2 / 1.4 / 0.4 ms each) | 73 to 77 ms (0.15 ms each) | 4 to 5 ms | 2 ms |
| `wal_sync` count / `wal_sync_time` | 501 / 193 to 221 ms | 500 / 45 to 49 ms | 6 / 2 to 3 ms | 6 / 2 ms |
| Tick | 8,158 / 9,230 / 2,574 ms | 1.1 to 1.3 s | 187 to 196 ms | 116 to 121 ms |

The 8 statements per row are: lock+owned join, heartbeat_stale, never_acknowledged, unacknowledged
count, the attempt INSERT, the acquirement DELETE and the subtask UPDATE, plus the budget SELECT. The plan was
right to suspect round trips: BEFORE does about 12 server round trips and 1 commit per row.

What it is not:

- **Not the ADR-0014 never_acknowledged statement and not extra statements.** `MINION_RELEASE_LIMIT=0`
  removes 500 statements (3,506 instead of 4,006) and the tick is still 7.1 to 9.2 s. The M6.4 explanation
  ("more statements per reclaim now: settle, counter select") does not hold: settle_stuck is 1 query per
  tick, and the counter upsert only runs for FAILED rows, none here.
- **Not fsync.** `synchronous_commit=off` cuts `wal_sync` from 501 to 34 to 39 and the tick stays at
  7.0 to 8.2 s. `wal_sync_time` is at most 0.22 s of an 8 to 9 s tick.
- **Not a bad plan or database work.** Server execution is 0.09 to 0.4 s in total for 500 rows (0.03 to 0.15 ms
  per statement; no statement dominates). An autovacuum worker was sampled in 1 of 26 tick runs (a 9.2 s one, so
  it does not explain the others) and no checkpoint started during any tick (`checkpoints_timed`/`_req` delta 0).
- **Not contention.** 0 other active sessions, container watchdog stopped, sims stopped.

What it is: the per-row design costs rows x about 12 sequential round trips, and each round trip goes through
the host to Docker VM path. Round-trip baselines (1,000 x `SELECT 1`, asyncpg): host 0.23 to 0.27 ms mean
(p99 0.45 to 0.87), inside the compose network 0.05 to 0.06 ms. Through SQLAlchemy (300 transactions of
pre-ping, BEGIN, N x `select 1`, COMMIT): host 1.6 ms for 1 statement and 3.45 ms for 8 (0.27 ms per extra statement),
network 0.46 and 1.12 ms (0.09 ms per extra statement). That predicts about 1.8 s for 500 rows from the host
(the M6.3 figure, 1.7 to 2.4 s, and the fast BEFORE runs: 2.6 s and 3.0 s) and about 0.6 s in the network
(measured 1.1 to 1.3 s).

**Open: a slow host mode.** With identical code and an idle machine, 9 of 11 BEFORE host runs took 7.0 to
9.2 s and 2 took 2.6 to 3.0 s. In the slow runs everything on the same statements is 3x to 4x slower, including
the server's own execution time (323 to 399 ms against 93 to 114 ms) and the commit time, so the whole
host plus Docker VM path is in a slower state; it is not a property of one statement. This is the M6.4
9 s: the M6.3 1.7 to 2.4 s is the fast state. I could not tell from these measurements what switches the state
(Docker Desktop VM scheduling or host power state are candidates; not investigated). The in-network runs
never showed it (3 of 3 fast). What matters for the fix: BEFORE's cost is proportional to (rows x round trips x
per-trip latency) and so swings by 3.5x with the state; AFTER's 31 statements and 6 commits do not.

Fix effect: client statements 4,006 to 31 (129x fewer), commits 501 to 6, server calls about 6,100 to about 70
to 90; tick 8.2 s to 0.19 s median on the host (43x), 1.27 s to 0.12 s in the network (11x).
Hence no ADR is needed for AC#2: the target is met.

Note: 3 of the 6 BEFORE net/rl0 runs show about 470 more client statements than the others (4,476 and
4,487 instead of 4,006; 3,970 instead of 3,506). Which rows take the extra per-row query was not
investigated; it does not change the conclusion.

## Step 4: chaos drain with chunked reclaim (NF-2, NF-3)

Fresh stack (`down -v`, up), watchdog container running the branch image (timeout 20 s, interval 5 s),
20 tasks x 5,000 subtasks, 500 sims, 8 procs, work 1 s, hb 5 s, vanish 0.01 / release 0.02 / fail 0.02 /
late 0.005, late-wait 40 s. Same parameters as the M6.4 B run.

| Metric | Value |
|---|---|
| Duration / throughput | 656 s / 152.4/s (100,000 completed), drained (M6.4: 661 s / 151.2/s) |
| Faults injected | vanish 1,050; late 544; release 2,071; fail 2,103 |
| 5xx / 503 / 409 / transport | 0 / 26,473 / 544 (the late completes, all rejected) / 0 |
| Attempts by outcome | 100,000 SUCCEEDED, 2,103 FAILED, 2,071 RELEASED, 1,594 EXPIRED (all `NOT_ACKNOWLEDGED`; = 1,050 vanish + 544 late) |
| Watchdog | 126 reclaim ticks, 1,594 x `EXPIRED_REQUEUED` (= EXPIRED attempts), 0 error or warning lines |
| Left-over acquirements | 0 |

`minion-scale verify` (exit 0, **PASS**): `nf2_one_succeeded_attempt_per_succeeded_subtask` 0 violations;
`nf2_final_objects_match_succeeded` 100,000 / 100,000, 0 missing, 0 unexpected; `nf3_all_subtasks_terminal`
0 non-terminal; `nf3_no_orphan_acquirements` 0; `task_counters_match_subtasks` 0 mismatches;
`tasks_terminal` 0. Compare M6.4: 1,595 EXPIRED, same checks, also PASS.

## Caveats

- Single machine, load generator and DB on the same Mac. n = 3 per case; the BEFORE host case is bimodal,
  so its median (8.2 s) hides a 2.6 s run; the full list is in the table.
- The tick was measured with 1M PENDING; the reclaim path only goes through primary keys and does not depend
  on the queue length. The chaos drain used 100k, as M6.4 B.
- The instrumented tick adds cursor-event bookkeeping to both BEFORE and AFTER (a few microseconds per statement).
- A concurrent two-replica run was not part of this measurement (covered by the integration tests).
