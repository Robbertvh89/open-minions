# M6 scale test (NF-1, NF-2, NF-3): Apple M3 Pro (36 GB, macOS 27.0.1)

TASK-7.3. Derived data: [`m6-scale-apple-m3-pro.json`](m6-scale-apple-m3-pro.json). Harness:
`packages/minion-scale` ([report schema](scale-report-schema.md)). Run on 2026-10-18 at commit
`3d03959`, on the branch `docs/task-7.3-scale-report`. Total wall time was about 80 minutes. The
JSON keeps derived figures only: no tokens, no URLs, no object content.

## Verdicts

| Requirement | Target | Result | Verdict |
|---|---|---|---|
| NF-1 | acquire p99 < 200 ms, 500 executors, 1M pending | 500 sims, stress mix: server p50/p95/p99 all **> 10 s**, client acquires time out at 30 s, the pool is exhausted (QueuePool timeouts, 500s). Realistic mix: server p50 ≤ 250 ms, p95/p99 **> 10 s**. Even **one** sim pays about 200 ms (candidate query alone: 187 ms mean) | **FAIL** |
| NF-2 | zero double completion | Scenario B (20k subtasks, 500 sims, faults vanish/release/fail/late): `verify` found 0 violations and 20,000 final objects for 20,000 SUCCEEDED subtasks | **PASS** (at 20k, not 100k) |
| NF-3 | no lost subtasks | Scenario B: 0 non-terminal subtasks, 0 orphan acquirements, task counters match, all 20 tasks terminal | **PASS** (at 20k, not 100k) |

The NF-1 failure is structural: the candidate query sorts every PENDING row on each acquire (see
[EXPLAIN](#explain-plans)), so the cost grows with queue length, not with fleet size. Two more
bottlenecks show up once that query is cheap: head-of-queue contention (about 19 acquirement
`INSERT` attempts per win) and the single server process. Both are listed under
[M6.4 items](#m64-tasks-74-items-from-this-run).

## Setup

| Item | Value |
|---|---|
| Host | Apple M3 Pro, 12 cores, 36 GB, macOS 27.0.1, on AC |
| Docker | Docker Desktop, engine 29.5.2 (client 27.4.1); VM with 12 CPUs and 7.75 GiB (8,321,429,504 bytes) |
| Compose | `deploy/compose.yml` + `deploy/compose.scale.yml`, project `open-minions-scale`, loopback ports 55432 / 59000 / 58080 |
| PostgreSQL | 16.15 (postgres:16-alpine, aarch64). `shared_buffers=1GB`, `effective_cache_size=3GB`, `max_connections=300`, `work_mem=4MB`, `random_page_cost=4`, `synchronous_commit=on`, `max_wal_size=1GB`, `pg_stat_statements` preloaded (`track=all`), `track_io_timing=on`, `shm_size=1gb`. Alembic head 0008 |
| Server | 1 container, 1 uvicorn process. Python 3.12.15, FastAPI 0.143.2, SQLAlchemy 2.1.4, asyncpg 0.32.0, uvicorn 0.54.0. DB pool 20 + overflow 20. `acquire_min_interval_s=0.2`, `heartbeat_min_interval_s=1.0`, `CANDIDATE_LIMIT=16`, `/metrics` on |
| Watchdog | `MINION_HEARTBEAT_TIMEOUT_S=20`, interval 5 s, batch 500 (scale overlay) |
| Load generator | `minion-scale run` on the same Mac (Python 3.12.7, httpx 0.28.1), `--procs 8` for 500 sims, `ulimit -n 65536`. Client timeout 30 s. Event-loop lag stayed at p99 ≤ 2.5 ms (max 100 ms) in every run, so the client was not the bottleneck |
| Seeding | `minion-scale seed` bulk-inserts PENDING rows (no splitter), then `VACUUM ANALYZE`. 1M rows: 22 s, database 351 MB (`minion_subtask` 337 MB) |

Environment for every command:

```bash
export MINION_PG_PORT=55432 MINION_S3_PORT=59000 MINION_SERVER_HOST_PORT=58080 MINION_GRAFANA_PORT=53000
export MINION_DATABASE_URL=postgresql+asyncpg://minion:minion@localhost:55432/minion
export MINION_S3_ENDPOINT_URL=http://localhost:59000 MINION_S3_ACCESS_KEY=minion MINION_S3_SECRET_KEY=minion-secret
docker compose -p open-minions-scale -f deploy/compose.yml -f deploy/compose.scale.yml --profile app \
  up -d --build --wait postgres s3 s3-init migrate server watchdog
```

The tokens file lived in a 0600 scratch file and was revoked and deleted with `minion-scale cleanup`
after each phase. The stack was taken down with `down -v` on project `open-minions-scale` only.

## Scenario A: NF-1 at 1M pending

```bash
uv run minion-scale seed --tasks 100 --subtasks 10000 --executors 500 --tokens-file $T
uv run minion-scale explain --heartbeat-timeout-s 20 --out explain.json
# stress: work 2 s (the middle of 1-3 s; --work-s is a fixed value, the harness has no range)
uv run minion-scale run --server http://localhost:58080 --tokens-file $T --sims 500 --procs 8 \
  --work-s 2 --heartbeat-interval-s 5 --max-duration-s 600 --docker-project open-minions-scale --out a1.json
# realistic: work 75 s (the middle of 30-120 s)
uv run minion-scale run ... --sims 500 --procs 8 --work-s 75 --heartbeat-interval-s 5 --max-duration-s 600 --stall-s 0 --out a2.json
# knee: 1, 10, 50 sims, stress mix, 120 s each (1 sim with --procs 1, the others with --procs 4)
uv run minion-scale run ... --sims N --work-s 2 --heartbeat-interval-s 5 --max-duration-s 120 --stall-s 0 --out kN.json
```

`/metrics` (`minion_acquire_seconds`) was scraped before and after each run. Server quantiles are
bucket upper bounds; the largest finite bucket is 10 s. Client quantiles use the harness buckets
(…, 0.25, 0.5, 1, 2.5, 5, 10, 30 s). A run that hits `--max-duration-s` exits 1 ("not drained"),
which is expected for Scenario A.

The queue stayed at ≥ 993k PENDING in every run (the most completed in one run was 2,937).

| Run | Sims | Duration | Completed (per s) | Client acquire (assigned) mean / p50 / p95 / p99 | Server acquire (assigned) mean / p50 / p95 / p99 | Candidate query (pg_stat_statements) calls × mean | Acquirement INSERT attempts per win | 409 / 429 / 5xx / transport |
|---|---|---|---|---|---|---|---|---|
| stress | 500 | 629 s | 1,279 (2.0/s) | 18.9 s / ≤30 s / ≤30 s / ≤30 s; plus 3,487 acquires that failed (3,483 timeouts at 30 s, 4 × 500) | 15.7 s / >10 s / >10 s / >10 s (n=5,086; 0 % ≤ 250 ms) | 9,916 × **2,436 ms** | 24.1 (122,701 / 5,086) | 775 / 0 / 6 / 4,857 |
| realistic | 500 | 602 s | 2,937 (4.9/s) | 4.4 s / ≤250 ms / ≤30 s / ≤30 s; plus 320 timeouts | 3.95 s / ≤250 ms / >10 s / >10 s (n=4,163; 63 % ≤ 250 ms) | 5,500 × 1,243 ms | 8.9 | 525 / 0 / 0 / 545 |
| knee | 1 | 120 s | 53 (0.44/s) | 206 ms / ≤250 / ≤250 / ≤250 (max 216 ms) | 198 ms / ≤250 / ≤250 / ≤250 | 54 × **187 ms** | 1.0 | 0 / 0 / 0 / 0 |
| knee | 10 | 121 s | 520 (4.3/s) | 244 ms / ≤250 / ≤500 / ≤1000 (max 772 ms) | 237 ms / ≤250 / ≤500 / ≤1000 (86 % ≤ 250 ms) | 530 × 223 ms | 1.7 | 0 / 0 / 0 / 0 |
| knee | 50 | 122 s | 877 (7.2/s) | 4.5 s / ≤5 s / ≤30 s / ≤30 s | 4.4 s / ≤5 s / >10 s / >10 s | 1,937 × 2,063 ms | 26.2 | 0 / 0 / 0 / 0 |

Empty acquires (204) were rare (stress: 8 server-side, realistic: 3), and all were slow (> 10 s),
because with 1M pending almost every acquire finds work.

**System behavior under the stress mix (500 sims):**

- **PostgreSQL** was CPU-bound: median **1,175 %** (12 vCPUs, max 1,194 %), 709 MiB max. The candidate
  query used 24,154 s of execution time in 629 s, which is 38 cores' worth of demand on 12 cores. A
  sample of `pg_stat_activity` showed 49 active backends (the server pool of 40 plus parallel
  workers) with no lock waits.
- **Server** CPU was at median 44 % (max 64 %), so it was waiting on the DB. The log shows 3,181 ×
  `QueuePool limit of size 20 overflow 20 reached ... timeout 30.00`, each answered with 500. Most
  clients had already given up at 30 s (4,857 transport errors), so they saw only 6 × 5xx.
- **Knock-on effects:** heartbeats and completes queue for the same pool (heartbeat p50 ≤ 30 s, 322
  heartbeats got 409 after reclaim). 446 completes got 409, and 1,285 completes failed client-side.
  By the end, **269 subtasks were FAILED** although no fault was injected: overload burned attempts
  (timeouts, reclaims, retryable fails) until `max_attempts=5` ran out. The exact path was not
  traced further.
- The harness polls the remaining count once per second (`SELECT count(*) ... status IN (...)`).
  That cost 748 × 274 ms, about 0.3 core: a few percent of the DB CPU, but not zero.

**Realistic mix (500 sims, 75 s work):** the demand is only about 6.7 acquires/s, but all 500 sims
acquire at the start. That burst collapses in the same way (PG at 1,183 %, 320 acquire timeouts).
Heartbeats queued behind the burst for more than 20 s, so the watchdog reclaimed 468 assignments
from live sims (`discarded_stale` 468, heartbeat 409 × 492). After the burst, acquires settle at
about 250 ms (PG median 394 %).

**Load knee:** one executor already costs about 200 ms per acquire, nearly all of it the
candidate query (187 ms). At 10 sims the p99 is about 1 s. At 50 sims the DB is saturated (2 s per
candidate query, 5 s per acquire) and throughput levels off at about 7 acquires/s. That is below the naive 12 cores ÷ (0.2 s × ~3 processes per parallel query) ≈ 20/s,
because each query slows down further under contention. With a 1M queue, this setup cannot serve 500
executors at any work mix.

## Scenario B: drain with chaos (NF-2, NF-3)

```bash
uv run minion-scale seed --tasks 20 --subtasks 1000 --executors 500 --tokens-file $T   # fresh stack (down -v; up)
uv run minion-scale run --server http://localhost:58080 --tokens-file $T --sims 500 --procs 8 --work-s 1 \
  --heartbeat-interval-s 5 --vanish 0.01 --release 0.02 --fail 0.02 --late 0.005 --late-wait-s 40 \
  --stall-s 120 --max-duration-s 2400 --docker-project open-minions-scale --out b.json
sleep 30; uv run minion-scale verify --out verify.json
```

**Scaled down from 100k to 20k.** A first attempt with 100k subtasks and 500 sims drained at
about 22 subtasks/s: 2,672 done in about 150 s, server container at about 98 % CPU, PG at
350–430 %, and the candidate query 32 ms by EXPLAIN. That projected to about 75 minutes, so I
stopped it (sims released cleanly, 0 RUNNING left), reset the stack, and re-ran B on 20k.

| Metric | Value |
|---|---|
| Drained | yes, in **671 s**; 19,895 completes accepted, **29.7 subtasks/s** |
| Faults injected | vanish 203, release 418, fail 427, late 101 |
| Protocol answers | late complete → 409: 98 (the reclaim had already happened); complete 409 total 178; heartbeat 409: 47; 0 × 5xx; 535 client timeouts |
| Attempts / final state | 21,518 attempts for 20,000 subtasks; 20,000 SUCCEEDED, 0 FAILED |
| Acquire latency | client p50 ≤ 5 s, p99 ≤ 30 s. Server (assigned) mean 4.4 s, p50 ≤ 5 s, p95/p99 > 10 s, 17 % ≤ 250 ms |
| Candidate query | 47,098 calls × **6.1 ms** (cheap at ≤ 20k pending) |
| Acquirement INSERT attempts | **418,083** for 21,518 wins: **19.4 per win**, 0.7 ms each |
| `verify` | **PASS**: `nf2_one_succeeded_attempt_per_succeeded_subtask` 0 violations; `nf2_final_objects_match_succeeded` 20,000 / 20,000, 0 missing, 0 unexpected; `nf3_all_subtasks_terminal` 0; `nf3_no_orphan_acquirements` 0; `task_counters_match_subtasks` 0 violations; `tasks_terminal` 0 |

In this run the candidate query is cheap, yet the acquire is still seconds long. The server
process runs at about 98 % of one core: it handles about 19 `INSERT ... ON CONFLICT DO NOTHING` round
trips per acquire, because all 500 sims get the same 16 head candidates and fight over them. Every
other endpoint queues behind that same event loop (complete p50 ≤ 5 s).

## Scenario C: watchdog with 500 stale acquirements on the 1M table

Procedure, on the 1M database after Scenario A:

1. Stop the container watchdog.
2. Start 500 sims that each acquire once and vanish: `run --sims 500 --vanish 1.0 --acquire-pace-s 100000 --max-duration-s 150`. Top up with a second small run to exactly 500.
3. Wait more than 20 s.
4. Run `minion-scale explain --heartbeat-timeout-s 20`. Run it again with `--max-lease-s 30` so the over-lease scan has 500 hits.
5. Run one tick of `minion_watchdog.main.tick` from the host with DEBUG logging. This is the same code path as the container; only the log level differs, because the container logs at INFO and prints `watchdog_tick` only when a tick takes > 1 s, and never prints `over_lease_scan`.
6. Repeat with `ix_minion_subtask_acquirement_acquired_at` (0008) dropped in the scale DB, then recreate it with the same name and definition.

| Case | Stale reclaimed | Tick `duration_ms` | Rounds | `over_lease_scan` `scan_ms` (hits) |
|---|---|---|---|---|
| with 0008 | 500 | 2,361 | 2 (batch 500 was full) | 2, 0 (0) |
| with 0008, repeat | 436 | 1,687 | 1 | 2 (0) |
| without 0008 | 500 | 6,971 | 2 | 3, 2 (0) |

The tick time is dominated by the per-row reclaim: one short transaction per acquirement, about
3.9–4.7 ms per row with the index. The without-index tick (14 ms/row) is slower, but its scan took
the same 2–3 ms, so the gap comes from run-to-run variance in the per-row transactions
(autovacuum on `minion_subtask` was due: 24k dead tuples). It does not come from the over-lease
scan. **0008 makes no measurable difference here.** `minion_subtask_acquirement` only holds
running work (≤ 500 rows), whatever the size of `minion_subtask`. The planner uses the index only
when it is selective (no hits: Index Scan, 2 buffers). With 500 hits or no index it picks a Seq
Scan of 42–63 buffers in < 0.2 ms. 0008 still bounds the scan if the acquirement table ever grows
(mass outage, many replicas). One tick that reclaims 500 rows in 1.7–7 s
stays well below the 20 s heartbeat timeout.

## Scenario D: rate limits

```bash
# 1) one executor, 50 back-to-back acquires (scratch script using minion_common.ExecutorClient; token never printed),
#    with pg_stat_statements + pg_stat_user_tables snapshots before/after (12 s wait for the stats flush)
# 2) harness, 10k PENDING (seed --tasks 10 --subtasks 1000 --prefix scaled), 20 sims, work 0, 60 s each:
uv run minion-scale run ... --sims 20 --procs 2 --work-s 0 --acquire-pace-s 0.05 --max-duration-s 60 --stall-s 0 --out d-throttled.json
uv run minion-scale run ... --sims 20 --procs 2 --work-s 0 --acquire-pace-s 0.4  --max-duration-s 60 --stall-s 0 --out d-control.json
```

| Check | Result |
|---|---|
| Single-executor burst | 50 calls in 0.26 s: **2 × 200, 48 × 429**. Every 429 carried **`Retry-After: 1`** (`ceil(0.2)`). 429 latency p50 3.6 ms |
| DB work for the burst | pg_stat_statements: candidate SELECT **+2**, acquirement INSERT +2, executor UPDATE on acquire +2, attempt INSERT +2 (the 2 releases), register UPDATE +1. Tuples ins/upd/del +6/+8/+4. **All of it belongs to the 2 admitted acquires and their releases. The 48 throttled calls ran no statement and wrote nothing.** |
| Harness throttled (pace 0.05 s < 0.2 s) | 1,034 × 429 on acquire, 1,049 admitted. Candidate SELECT 1,053 calls (= admitted + 4 conflict rounds); executor UPDATE 1,049 (= admitted). Writes per completion: ins 2.96 / upd 3.95 / del 1.98 |
| Harness control (pace 0.4 s) | 0 × 429, 2,980 admitted. Candidate SELECT 3,421; executor UPDATE 2,980. Writes per completion: ins 3.25 / upd 3.96 / del 2.27 |

The limiter rejects before any DB work, as NF-5 / ADR-0012 require. The sims honor `Retry-After`
(1–1.2 s), which cuts their own throughput (17.3/s throttled vs 49.1/s control). That is the
intended back-pressure. The control run at only 20 sims and 10k pending already shows 8.3 INSERT
attempts per win (24,605 / 2,980), the same head contention as in Scenario B.

## EXPLAIN plans

All plans are `EXPLAIN (ANALYZE, BUFFERS)` with literal values, after `ANALYZE`. The full plan
text is in the JSON.

| Query | Queue | Plan | Exec ms | Buffers |
|---|---|---|---|---|
| candidate (FR-22 order, LIMIT 16) | 1M PENDING | Gather Merge → top-N heapsort over a **Parallel Seq Scan of `minion_subtask`** (333k rows per worker × 3) hash-joined to `minion_task`; the partial index `ix_minion_subtask_pending` is not used | **214** (199–202 on repeats) | shared hit 20.6k |
| candidate_preference (team filter) | 1M | same shape | 195 | 20.6k |
| candidate | 100k | same shape | 32 | 2.1k |
| stale_heartbeat | 500 rows in `minion_executor_heartbeat` | Seq Scan + sort (the `last_seen_at` index exists; a seq scan is cheaper at 500 rows) | 0.15–0.18 | 38–47 |
| over_lease (6 h, 0 hits) | 500 acquirements, with 0008 | **Index Scan** `ix_minion_subtask_acquirement_acquired_at` | 0.009 | 2 |
| over_lease (30 s, 500 hits) | with 0008 | Seq Scan (not selective) | 0.16 | 63 |
| over_lease | without 0008 | Seq Scan | 0.06–0.17 | 42 |

The candidate query has no index path: its `ORDER BY` mixes task columns (`priority DESC,
created_at`) with subtask columns, so PostgreSQL must join every PENDING row before it can take
the top 16. The cost grows linearly with the pending count: about 0.6 µs per row per process, with 333k rows per process.

## M6.4 (TASK-7.4) items from this run

1. **Candidate query: index-driven two-step pick (highest impact, needed for NF-1).** Evidence: 187 ms
   with one executor at 1M pending, 2.4 s mean under load. 38 cores of demand in the stress run.
   EXPLAIN shows a parallel seq scan plus a top-N sort of all 1M PENDING rows. Fix as in the TASK-7.4
   description: walk `minion_task` in queue order (`ix_minion_task_queue` or an index on `(priority
   DESC, created_at)` for live tasks), then take the per-task top-K from `ix_minion_subtask_pending`
   (an index range scan, no full sort). Expected cost: O(K + number of tasks), independent of queue
   length. Target: ≤ 5 ms at 1M.
2. **Head contention / conflict rounds.** Evidence: 19.4 acquirement INSERT attempts per win
   (Scenario B), 24–26 per win under load at 1M, 8.3 per win at only 20 sims. 2–3 candidate queries per
   acquire. The server sat at about 98 % CPU on these round trips while the DB was not saturated.
   Options that keep invariant 2: randomize or offset the start within the top-K per executor (e.g.
   hash of executor_id), or a larger K with a random pick. `SKIP LOCKED` deviates from the paper and
   needs an ADR first.
3. **Server throughput: one uvicorn process.** Evidence: 98 % of one core in Scenario B, and every
   endpoint (complete, heartbeat) queues behind acquire. Run several workers or replicas (NF-5 allows
   it; the rate-limiter caveat is in ADR-0012) and re-measure.
4. **Overload must not turn into permanent failures or false reclaims.** Evidence: 269 subtasks FAILED
   with no injected fault (stress), and 468 live assignments reclaimed because heartbeats waited in the
   pool for > 20 s (realistic burst). Ideas: answer acquire with 503 / 429 + Retry-After when the DB
   pool is saturated (load shedding) instead of letting heartbeats and completes time out; give
   heartbeats and completes priority over acquires (separate pool or semaphore); look at whether
   EXPIRED attempts caused by server overload should count against `max_attempts`.
5. **DB pool sizing and timeout.** Evidence: QueuePool 20 + 20 exhausted with a 30 s wait, 3,181 × 500.
   A bigger pool would not help while the candidate query saturates PG. Re-tune after item 1:
   pool ≈ cores × 2–4, and a short pool timeout (≤ 2–5 s) so a request fails fast instead of holding a
   client for 30 s.
6. **Startup burst.** 500 executors acquiring at the same moment collapsed the realistic mix even at
   6.7 acquires/s average. Jitter the executor's first acquire after register (the real executor
   matters here, not only the sims).
7. Smaller items: autovacuum on `minion_subtask` (24k dead tuples by Scenario C, no autovacuum of it
   during the runs; scale factor 0.2 means 200k dead rows before the first autovacuum at 1M; tune via
   migration as TASK-7.4 (3) suggests). `bump_counters` (`UPDATE minion_task SET succeeded_subtasks`)
   cost 0.98 ms mean in B and is not a hot spot yet. Re-check it after items 1–3.

## Limits and extrapolation

- **Co-location.** The load generator (8 processes), the server, PostgreSQL, SeaweedFS and the
  watchdog all share one Mac. PG was given all 12 VM CPUs and competed with the sims and the server
  for them. A dedicated DB host would raise the knee somewhat, but the 187 ms single-executor cost does
  not depend on load. NF-1 fails on query shape, not on co-location.
- **Loopback, no TLS.** The client talks to the server over the Docker Desktop port forward
  (`127.0.0.1`), so there is no network RTT and no TLS handshake. Real executors on a LAN or VPN add
  RTT and TLS cost to every call.
- **Single server replica, single process.** See item 3. Results scale differently with N replicas
  (DB-bound vs CPU-bound).
- **Docker Desktop VM.** PG ran in a Linux VM (7.75 GiB, 12 vCPUs) on virtualized storage, with
  `synchronous_commit=on`. The 1M table (337 MB) fit in `shared_buffers` (all buffer hits), so
  I/O was not tested. Bare-metal or managed PG numbers will differ.
- **Short durations.** A: 10 min per mix, knee 2 min per point. B: 11 min. D: 1 min per run. There was
  no soak test, so long-run effects (autovacuum, bloat, WAL, connection churn) were not observed.
- **Full 1M drain not run.** At the measured ≤ 7 acquires/s at 1M, a full drain would take days.
  Scenario B was also cut to 20k (from 100k) for time. NF-2/NF-3 are shown at 20k under chaos, plus
  the earlier CI and integration runs. They are correctness properties of the protocol, and nothing
  here suggests they depend on scale, but 100k and 1M drains are unverified.
- **Simulated work.** The work is a fixed `--work-s` (2 s / 75 s / 1 s), not a 1–3 s or 30–120 s
  distribution. Sims skip input reads and inference, and outputs are a few bytes. The harness's own
  1 Hz `count(*)` poll costs about 0.3 core at 1M.
- **Quantile resolution.** Server histogram buckets top out at 10 s and client buckets are coarse
  (0.25 / 0.5 / 1 / 2.5 / 5 / 10 / 30 s), so quantiles are bucket upper bounds. The 1-sim figure is
  "≤ 250 ms bucket, mean 198–206 ms, max 216 ms", which puts the p99 at or above the 200 ms target.
- **CI-runner variance (TASK-28).** GitHub runners measured acquire p50 about 500 ms vs about 100 ms
  locally on the 2k-subtask smoke, and about 21 subtasks/s: about 5× slower. Absolute numbers from
  this Mac do not carry over to other hardware. The ratios (cost ∝ pending rows, attempts per win)
  do.
- **Watchdog tick** was timed with a host-side run of the same `tick()` (DEBUG logging), because the
  container watchdog logs `watchdog_tick` only above 1 s and never logs `over_lease_scan` at INFO.
