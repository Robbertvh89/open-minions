# TASK-36: server CPU per acquire and worker scaling (Apple M3 Pro)

Question: is the stress-mix ceiling of the [M6.4 run](m6.4-scale-apple-m3-pro.md#bottleneck-server-cpu)
(about 169 acquires/s, four server workers at 388 % CPU, PostgreSQL at about 94 %) caused by Python
interpreter work, or is it a sizing question? This decides the ADR-0001 revisit trigger that
[ADR-0016](../adr/0016-nf1-workload-scope.md) left open.

Derived data: [`m-task36-server-cpu-apple-m3-pro.json`](m-task36-server-cpu-apple-m3-pro.json) (every run,
the curve, every profile, the decision numbers; no tokens, URLs or payloads). Raw py-spy stacks and
the scripts that produced the tables are not committed. Measured on 2026-10-19/20 at `main` commit
`fe631c9` (includes the TASK-35 chunked reclaim). No source was changed for this task.

## Verdict

**ADR-0001 stands. The server-rewrite trigger is not met.** The rule in the task (all of A, B, C) fails
on C: the throughput shortfall with more workers coincides with the PostgreSQL claim query mean rising
5x to 15x (W=4 to 8) and claim calls per win rising 1.6x (1.06 to 1.73), so the rule attributes it to
PostgreSQL claim contention. This is a rule outcome, not proof that the Python cost is fine: server CPU
per subtask grows 2.3x by W=8 and most of it is SQLAlchemy statement building (see below).

| | Condition | Measured | Result |
|---|---|---|---|
| A | P_py >= 0.50 in P1 and P2, and C_srv >= 2 x C_pg | P_py 0.67 (P1) and 0.80 (P2); C_srv / C_pg = 4.4 (W=4), 4.1 (W=8) | met |
| B | some W <= W_hr with E(W) < 0.70, or C_srv(W) > 1.3 x C_srv(1) | W_hr = 8; E(4) = 0.57, E(8) = 0.32; C_srv(4) = 23.4 ms = 1.62 x 14.4 ms | met |
| C | not explained otherwise: PostgreSQL < 3 cores, claim mean ms <= 2 x W=1, generator lag p99 <= 20 ms, shed > 0 in S-sat | PostgreSQL 1.44 cores max for W <= 8 (ok); **claim mean 3.6 ms (W=4), 10.7 ms (W=8) vs 0.73 ms at W=1, limit 1.46 ms (fails)**; lag p99 <= 5 ms at W <= 8 (ok); shed 13 to 28 % of requests (24 to 44 % of acquire attempts) (ok) | **not met** |

What this means in practice:

- One uvicorn worker serves about 70 acquire+complete cycles/s on one core (14.4 ms of server CPU
  per subtask, S-sat mix). That cost is Python: 67 to 80 % of the non-idle worker samples are in
  Python-level categories, and SQLAlchemy statement construction and execution alone is about 40 to
  48 % of self time (see profiles).
- Adding workers does not scale linearly: 70, 103, 160, 176, 178, 164 completes/s at W = 1, 2, 4, 6, 8,
  12. The ceiling is (server cores used) / C_srv. C_srv grows with W (14.4 to 33.4 ms at W=8, 2.3x;
  42.5 ms at W=12), and the cores used grow less than W (3.75 at W=4, 6.01 at W=8 = 0.94 and 0.75 per
  worker). Going from W=4 to W=8 the shortfall from a doubling (1.12x instead of 2x) is a 1.43x higher
  C_srv and a 1.25x lower core count than 8 workers at the W=4 rate: roughly 60 % cost growth and 40 %
  fewer cores per worker.
- Where C_srv goes: mostly Python, SQLAlchemy statement building and execution (40 to 48 % self, `sql/`
  alone 21 to 26 %). Claim contention is a measured part of the growth, not the explanation: the claim
  query mean rises 0.73 to 10.7 ms and claim calls per win 1.06 to 1.73, but the claim wait is async and
  uses no server CPU. With `MINION_ACQUIRE_WINDOW=64` at W=8 calls per win fall to 1.14 (the W=2 level),
  the claim mean to 4.0 ms and goodput rises 11 % (178 to 198/s), yet C_srv only falls 33.4 to 30.0 ms
  (W=2: 17.2). So collisions account for about 10 % of C_srv at W=8 (about 18 % of its rise since W=1);
  the rest is unattributed (efficiency-core spill, host and Docker VM contention, the W=1 no-multiprocess
  metrics mode). The profile does not separate these.
- The curve stops at about 178/s (W = 6 to 8) with the load generator, PostgreSQL, S3 and the Docker VM
  on the same 6 performance + 6 efficiency cores; H = 0.83 at W=8 and 0.93 at W=12, so the Mac itself is
  close to full. Higher W is not judged (W=12 is informational, H >= 0.85).
- Python work per request is real and reducible (cheap wins below); the verdict is the pre-registered
  rule's, and a rerun after TASK-46/47 (statements built once, fewer claim collisions) re-judges it.
- NF-5 (stateless server, add hosts): **not measured** (one Mac). C_srv is not flat across W, so a
  constant cost per subtask cannot be assumed; any claim-contention part is against a single PostgreSQL,
  which more hosts would share too.

## Configuration

| Item | Value |
|---|---|
| Host | Apple M3 Pro, 12 cores (6 performance + 6 efficiency), macOS 27.0.1, on AC; Docker VM 12 CPUs, 7.75 GiB |
| Stack | project `open-minions-scale` (`deploy/compose.yml` + `deploy/compose.scale.yml`), PostgreSQL 16 (`shared_buffers` 1 GB, `max_connections` 300, `pg_stat_statements`), SeaweedFS 4.05, Alembic head as at `fe631c9` |
| Server | `MINION_SERVER_WORKERS` = W, pool 8 + overflow 4 and `MINION_ACQUIRE_CONCURRENCY` 8 per worker, `MINION_ACQUIRE_WINDOW` 16 (64 in the window runs), `MINION_MAX_OPEN_ACQUIREMENTS` 64, `/metrics` on. Connections 14 W + 2 (W=12: 170 of 300); no `db_connection_budget_exceeded` in any matrix server log. W=1 has no Prometheus multiprocess mode |
| Fixture | per W a fresh stack: `seed --tasks 100 --subtasks 10000 --executors 500` (1M PENDING). PENDING at the end of a W block was 957,631 (W=1), 962,648 (W=2), 782,064 (W=4), 936,304 (W=6), 801,694 (W=8), 940,168 (W=12); a block's curve runs and profile runs shared one seed |
| Generator | `minion-scale run --sims 500 --procs 8` on the same Mac; event-loop lag p99 <= 10 ms in every run (<= 5 ms for W <= 8), so `--procs 12` was not needed |
| Profiler | py-spy 0.4.2 in a sidecar container (`python:3.12.15-slim-bookworm`, `--pid=container:<server>`, `SYS_PTRACE` on the sidecar only; the server kept `cap_drop ALL`, read-only root, `no-new-privileges`), `--subprocesses`, never `--locals`. Host py-spy was rejected (needs sudo, wrong CPython) |
| Image note | `--build` retags `open-minions-app:dev`, which the dev stack shares |

Exact commands (environment as in [M6.3](m6-scale-apple-m3-pro.md), tokens file 0600 outside the repo,
never printed):

```bash
MINION_SERVER_WORKERS=$W docker compose -p open-minions-scale -f deploy/compose.yml -f deploy/compose.scale.yml \
  --profile app up -d --build --wait postgres s3 s3-init migrate server watchdog
uv run minion-scale seed --tasks 100 --subtasks 10000 --executors 500 --tokens-file $T
# S-sat (capacity, ~1000/s offered), 120 s, n = 3, 30 s between runs
uv run minion-scale run --server http://localhost:58080 --tokens-file $T --sims 500 --procs 8 \
  --work-s 0.25 --heartbeat-interval-s 5 --max-duration-s 120 --stall-s 0 --docker-project open-minions-scale --out r.json
# S-stress (M6.4 parity, demand-bound ~200/s), 240 s, n = 2: same with --work-s 2 --max-duration-s 240
# profile (py-spy sidecar), started 30 s into a 240 s run:
docker run --rm --network none --pid=container:$SRV --cap-add SYS_PTRACE -v $PROF:/out open-minions-scale-pyspy:0.4.2 \
  py-spy record --pid 1 --subprocesses --nonblocking --rate 100 --duration 120 --format raw -o /out/P.txt
# native split: one uvicorn worker pid, blocking mode (--native cannot be combined with --nonblocking,
# and --subprocesses --native produced no output)
py-spy record --pid $WORKER --native --rate 50 --duration 60 --format raw -o /out/P4.txt
uv run minion-scale cleanup --tokens-file $T   # before
docker compose -p open-minions-scale ... down -v
```

Per run, the CPU seconds of the server, postgres, s3 and watchdog containers are the difference of
`usage_usec` in `/sys/fs/cgroup/cpu.stat` before and after; the generator CPU is `/usr/bin/time -l`
user+sys. C_srv and C_pg are CPU milliseconds per completed subtask; H = (all containers + generator
CPU) / (run seconds x 12).

## Throughput versus workers

Goodput X is completed subtasks per second over the whole run (it includes the 500-sim ramp-up).
E(W) = X(W) / (W x X(1)); E2(W) = 2 X(W) / (W x X(2)) (W=1 has a different metrics mode). Latencies
are harness (client) bucket upper bounds.

| Mix | W | n | X min / **med** / max (completes/s) | E | E2 | Server cores | X per server core | C_srv ms/subtask | C_pg ms/subtask | Postgres cores | H | client acquire / complete p99 | shed 503 % (of acquire + complete requests) | claim calls per win | claim mean ms |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| S-sat | 1 | 3 | 68.6 / **69.6** / 70.4 | 1.00 | 1.35 | 1.00 | 69.3 | 14.4 | 3.2 | 0.22 | 0.17 | 500 ms / 500 ms | 28 | 1.06 | 0.7 |
| S-sat | 2 | 3 | 102.9 / **103.3** / 103.5 | 0.74 | 1.00 | 1.78 | 58.0 | 17.2 | 3.7 | 0.38 | 0.26 | 500 ms / 500 ms | 22 | 1.14 | 1.7 |
| S-sat | 4 | 3 | 158.0 / **159.7** / 160.9 | 0.57 | 0.77 | 3.75 | 42.6 | 23.4 | 5.4 | 0.85 | 0.52 | 500 ms / 500 ms | 16 | 1.30 | 3.6 |
| S-sat | 6 | 3 | 175.4 / **175.5** / 176.2 | 0.42 | 0.57 | 5.17 | 33.9 | 29.5 | 6.9 | 1.21 | 0.72 | 1000 ms / 500 ms | 15 | 1.50 | 6.7 |
| S-sat | 8 | 3 | 178.0 / **178.3** / 181.7 | 0.32 | 0.43 | 6.01 | 29.6 | 33.4 | 8.1 | 1.44 | 0.83 | 1000 ms / 1000 ms | 13 | 1.73 | 10.7 |
| S-sat | 12 | 3 | 163.6 / **164.3** / 165.6 | 0.20 | 0.27 | 6.98 | 23.5 | 42.5 | 10.3 | 1.69 | 0.93 | 2500 ms / 1000 ms | 13 | 2.19 | 20.6 |
| S-stress | 4 | 2 | 168.8 / **169.8** / 170.7 | - | - | 3.76 | 45.2 | 22.1 | 5.7 | 0.97 | 0.53 | 500 ms / 500 ms | 8 | 1.20 | 2.4 |
| S-stress | 8 | 2 | 193.7 / **194.2** / 194.7 | - | - | 5.60 | 34.7 | 28.9 | 7.5 | 1.46 | 0.78 | 1000 ms / 1000 ms | 3 | 1.35 | 5.1 |

- S-stress at W=4 reproduces the M6.4 ceiling (170/s, server about 3.8 cores, PostgreSQL about 1 core).
  S-stress is demand-bound (about 200/s), so W=8 (194/s) is near its offered load. 0 5xx and 0 409/429
  in every run; shed 503s are the intended fail-fast path (ADR-0016). NF-1 (acquire p99 < 200 ms) is
  not met for either mix at any W (client buckets 0.5 s or more).
- W_hr (largest W with H < 0.85) = 8. W=12 is shown for completeness, not judged.
- Capacity per server core falls from 69 (W=1) to 30 (W=8) completes/s: sizing rule of thumb for this
  workload on this Mac, about 40 to 70/s per server core at W <= 4 with PostgreSQL on the same machine.
- Window experiment (config only, W=8, S-sat, n=2): `MINION_ACQUIRE_WINDOW=64` gives X = 197.3 and
  199.5 (median 198.4, +11 % over 178.3), claim calls per win 1.15 and 1.14, claim mean 4.0 and 3.8 ms,
  C_srv 30.0 ms (was 33.4), H 0.83. Throughput is still far from linear in W.
- Profiler overhead: profile runs last 241 s (py-spy attached for 120 s) and are compared with the
  unprofiled curve runs. Only W=4 stress has a like-for-like baseline (241 s): 156.4 (100 Hz) and 157.3
  (50 Hz) vs 169.8, i.e. -7.9 % and -7.4 % over the whole run. The S-sat baselines are 121 s runs, where
  the ramp-up weighs more, so the profiled 241 s runs are favoured: W=8 sat 163.4 (100 Hz) and 167.4
  (50 Hz) vs 178.3 (-6 to -8 %, understated) and W=1 sat 71.4 vs 69.6 ("none" may be a ramp-up artefact).
  Halving the rate did not help, and category shares at 100 and 50 Hz agree within 2 points (below). The shape is taken
  under about 15 % lower throughput during the sampling window.

## Profiles (AC#1)

py-spy `--nonblocking` (a sample of a busy thread may be dropped: 18 to 31 % of the ticks were
"errors", so counts are partial; the dropped ticks are assumed unbiased). Samples are worker processes only (the supervisor
is idle). py-spy marks threads blocked in a syscall as idle only in part, so samples whose leaf is a
known wait are removed here: the thread-pool queue wait (`concurrent/futures/thread.py _worker`) and
the S3 socket read/send inside botocore, 16 to 22 % of the non-idle samples. Self category: from the
leaf upwards the first frame in a known library wins; generic stdlib frames (typing, enum, json, re,
functools, contextlib, copy, datetime, abc, http, socket, ssl, threading, concurrent) count for the
library above. Category rules: `pydantic*` → pydantic; `fastapi` → fastapi; `starlette` → starlette;
`uvicorn`, `httptools`, `h11` → uvicorn; `structlog`, `logging` → structlog; `prometheus_client` and
`minion_server/metrics.py` → metrics; `botocore`, `boto3`, `s3transfer`, `minion_server/storage.py` →
presign/boto; `sqlalchemy`, `greenlet` → sqlalchemy; `asyncpg` → asyncpg; `minion_server/auth*`,
`tokencache`, `hashlib`, `hmac` → auth; other `minion_server` / `minion_common` → app logic;
`asyncio`, `uvloop`, `selectors`, `anyio` → event loop / native I/O; else other.

Profiles: P1 = W=4 S-stress, P2 = W=8 S-sat, P3 = W=1 S-sat (each 100 Hz, 120 s); P1b and P2b repeat
P1/P2 at 50 Hz. P_py = non-idle worker samples whose self category is not "event loop / native I/O",
divided by all non-idle worker samples. Samples after the wait filter: P1 28,612, P1b 14,577, P2 65,752,
P2b 33,020, P3 6,885.

Self % of non-idle worker samples (inclusive % in the JSON):

| Category | P3 (W=1, sat) | P1 (W=4, stress) | P1b (50 Hz) | P2 (W=8, sat) | P2b (50 Hz) |
|---|---|---|---|---|---|
| sqlalchemy | 41.8 | 40.3 | 40.0 | 47.6 | 46.6 |
| event loop / native I/O | 28.3 | 32.7 | 33.7 | 20.4 | 21.7 |
| asyncpg | 8.7 | 9.3 | 8.7 | 12.2 | 10.2 |
| fastapi | 5.9 | 4.3 | 3.8 | 3.5 | 3.4 |
| app logic | 4.7 | 4.7 | 5.0 | 4.2 | 4.4 |
| presign / boto | 2.8 | 2.6 | 2.7 | 4.9 | 6.5 |
| starlette | 2.7 | 2.0 | 2.1 | 2.0 | 1.8 |
| structlog | 1.6 | 1.5 | 1.3 | 2.4 | 2.5 |
| other | 1.3 | 1.3 | 1.4 | 1.4 | 1.7 |
| pydantic | 0.7 | 0.4 | 0.4 | 0.4 | 0.4 |
| metrics | 0.6 | 0.4 | 0.4 | 0.4 | 0.3 |
| auth | 0.5 | 0.4 | 0.4 | 0.3 | 0.3 |
| uvicorn | 0.5 | 0.3 | 0.2 | 0.3 | 0.3 |
| **P_py** | **0.717** | **0.673** | 0.664 | **0.796** | 0.783 |

`metrics`, `starlette`, `fastapi` and `uvicorn` are 28 to 58 % inclusive only because the ASGI
middleware chain wraps every request; their self cost is below 6 %.

SQLAlchemy self split by sub-package (% of all non-idle samples): `sql/` (statement construction,
coercion, cache-key generation, compilation) 23.0 (P3), 21.2 (P1), 25.8 (P2); `util/` (greenlet
bridge) 5.5, 5.6, 6.3; `orm/` 5.9, 5.6, 6.1; `engine/` 2.6, 3.2, 3.8; `dialects/` 2.1, 1.9, 2.3.

Top self frames (P1, W=4 stress, % of non-idle samples; the full top 25 for every profile is in the JSON):

| Frame | Self % |
|---|---|
| `run` (asyncio/runners.py): uvloop native code (HTTP parsing, socket I/O, asyncpg protocol) | 28.6 |
| `_generate_cache_key` (sqlalchemy/sql/cache_key.py) | 3.8 |
| `__do_execute` (asyncpg/prepared_stmt.py) | 3.7 |
| `execute` (asyncpg/connection.py) | 2.8 |
| `expect` (sqlalchemy/sql/coercions.py) | 2.4 |
| `greenlet_spawn` (sqlalchemy/util/concurrency.py) | 2.4 |
| `__init__` (sqlalchemy/sql/schema.py) | 1.3 |
| `emit` (logging/__init__.py) | 1.1 |
| `_prepare_and_execute` (sqlalchemy/dialects/postgresql/asyncpg.py) | 1.1 |

P2 (W=8 sat) is similar: uvloop `run` 16.7, `__do_execute` 5.8, `execute` 3.7, `_generate_cache_key` 3.5,
`expect` 3.0, `greenlet_spawn` 2.4, `emit` 2.1, `__init__` (schema.py) 1.5.

Routes (inclusive % of non-idle samples; a route is known only when the `minion_server/api/executor.py`
handler frame is on the stack). SQLAlchemy runs the compile/execute work in a greenlet whose stack
does not link back to the awaiting handler, so about 28 % of the samples (all SQL work) cannot be
assigned to a route; that bucket is shown separately:

| Bucket | P3 (W=1) | P1 (W=4) | P2 (W=8) |
|---|---|---|---|
| event loop thread, no handler frame (uvloop, middleware, response) | 43.2 | 43.9 | 31.4 |
| SQL in greenlet (route not recoverable) | 30.1 | 28.1 | 27.9 |
| `acquire` (:138) | 17.8 | 18.2 | 26.5 |
| `complete` (:239) | 6.0 | 6.9 | 8.7 |
| thread pool (S3 head/copy and other sync work) | 3.0 | 2.9 | 5.5 |
| `register`, `presence`, `heartbeat` | no samples | no samples | no samples |

Inside the handler frames the self cost is SQLAlchemy 75 % / app logic 14 % / asyncpg 8 % for `acquire`
(P1) and SQLAlchemy 58 % / app logic 20 % / asyncpg 15 % for `complete`. `claim` and `candidates_stmt`
in `repositories/acquisition.py` are on 10.6 % and 2.1 % (P1) of all samples as inclusive callers.

Request rate at W=4 S-sat: acquire (assigned) 161/s, acquire shed 62/s, complete 161/s, output PUT
161/s, register 4.8/s. At W=8: 179/s, 56/s, 178/s, 178/s. Heartbeats do not occur (0.25 s work,
5 s interval).

### Native split (P4: W=4 S-stress, one worker, blocking `--native`, 50 Hz, 60 s)

Event-loop thread only (1,122 samples; 1,934 samples from the other threads, 86 % parked in a futex,
were dropped). py-spy folds the interpreter's own native frames into the Python frame, so a Python
leaf means bytecode execution plus libpython internals.

| Leaf class | % |
|---|---|
| Python frame (bytecode execution, libpython internals folded in) | 41.6 |
| unresolved native in stripped extension `.so` (uvloop, asyncpg, greenlet, ...) | 25.3 |
| libc I/O syscalls (socket write/read, epoll) | 12.5 |
| libc other / unresolved address | 12.2 |
| libc memory (mmap/munmap/malloc) | 3.7 |
| libc thread sync | 2.8 |
| named extensions (`_asyncio`, libcrypto, `_hashlib`) | 1.8 |

Both P_py figures are reported: 0.67 to 0.80 by Python-level category (the plan's definition, condition
A) and 0.42 as the bytecode-leaf share of event-loop-thread samples (P4). The second figure is below
0.50, but it does not change the verdict, which turns on condition C. Reading: about 40 % of worker time
is Python bytecode (mostly SQLAlchemy statement handling), 25 % native extension code, 30 % kernel and
libc (mostly socket writes).

## Decision (AC#3)

Rule as written in the task: rewrite only if all of (A), (B), (C) hold. The numbers are in the Verdict
table. (A) and (B) hold; (C) fails because the claim mean at W = 2 to 8 is 2.3 to 14.6 times the W=1 value
(limit 2x), with PostgreSQL below 3 cores, generator lag within limit and sheds present. Therefore
ADR-0001 stands and no ADR-0020 is opened. This is the outcome of the rule fixed in the TASK-36 plan
before the runs (it applies ADR-0001's "Python CPU time, not Postgres" wording as: claim mean > 2x the
W=1 value means Postgres-side contention explains the shortfall); it is not proof that the Python
cost is fine. A rerun after TASK-46/47 re-judges it. SPEC §9 NF-1 text is unchanged (the verdict does not change it).

The curve is judged for W <= W_hr = 8 (H < 0.85). H was 0.52 at W=4 and 0.83 at W=8, so the
conclusion for W <= 4 is not confounded by host saturation; W=6 to 8 partly is.

## Cheap wins (self >= 5 %)

Each is a follow-up task, not done here.

1. **SQLAlchemy statement construction and cache-key generation** (TASK-46; `sqlalchemy/sql/*`: 21 to 26 % of
   non-idle samples, `_generate_cache_key` alone 3.5 to 4.6 %, `coercions.expect`, `schema.__init__`
   and `elements.__init__` next): the hot acquire/complete statements (`WITH cand`, claim INSERT,
   counters) are rebuilt on every call (`candidates_stmt`). Build them once (module-level statements
   with bind parameters, or Core/`text` for the claim path).
2. **Claim collisions at W > 1** (TASK-47; 1.73 claim calls per win at W=8, each a full round trip plus
   statement work): the window experiment above gave +11 % goodput and 10 % less server CPU per
   subtask. Tune or make the window adapt to the worker count.
3. **asyncpg round trips** (TASK-48; 9 to 12 % self; `complete` is 15 to 30 % asyncpg): fewer statements per
   acquire/complete (merge the per-complete SELECT/UPDATE/INSERT sequence).
4. **S3 promote at complete** (TASK-49; presign/boto 2.6 % at W=4, 4.9 to 6.5 % at W=8, in the thread pool):
   one HEAD plus one copy per complete; a shared client or fewer calls. Mind invariant 5 (idempotent
   outputs).

Not worth chasing: pydantic (0.4 to 0.7 %), metrics (0.3 to 0.6 %), auth (0.3 to 0.5 %), structlog
(1.3 to 2.5 %), response-model work (fastapi 3.4 to 5.9 %, under the 5 % line at W >= 4).

## Limits

- One Mac; the generator, Docker VM, PostgreSQL, S3 and the server share 12 cores (6 performance, 6
  efficiency). Efficiency E(W) mixes software effects with core heterogeneity and host contention;
  H is the guard (W <= 8). Multi-host (NF-5) was not measured.
- S-sat is a closed loop of 500 sims with fail-fast shedding (13 to 28 % of acquire + complete requests, 24 to 44 % of acquire attempts, shed and
  the sims back off); X is goodput under shedding, not a pure capacity at 0 % shed.
- py-spy overhead of about 15 % while sampling; nonblocking mode drops samples; the native split is
  one worker for 60 s at 50 Hz and has unresolved extension frames; route attribution misses the SQL
  done in a greenlet. Category shares at two sample rates agree within 2 points.
- P4 first ran with `--subprocesses --native` (no output) and `--nonblocking --native` (rejected by
  py-spy); the reported P4 is a later run of the same config on a fresh W=4 stack.
- Per-W stacks were rebuilt and reseeded; PENDING never fell below 782k in the matrix, so the claim query stayed on
  the 1M-row plan shape. Single-run variance between repeats was under 3 % at every W.
- The claim-contention reading is supported by the call-per-win and window=64 data, not isolated by a
  Postgres-side trace.
