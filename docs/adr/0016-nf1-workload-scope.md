# ADR-0016: NF-1 is defined against the realistic workload

- Status: **Accepted** (TASK-7.4.7, M6.4g)
- Deviates from paper: no (the paper gives no acquire latency target; this scopes our own NF-1)

## Context

NF-1 reads "acquire p99 < 200 ms with 500 executors and 1M pending subtasks". The M6.3 run failed it
at every intensity. After TASK-7.4.2 to 7.4.8 the [M6.4 re-run](../bench/m6.4-scale-apple-m3-pro.md)
(4 server workers, 500 sims, 1M pending, same 12-core Mac as the load generator) shows two regimes:

| Mix | Acquire demand | Server p99 (bucket) | Errors |
|---|---|---|---|
| Realistic: 894,924 PENDING, 75 s subtasks, heartbeat 300 s : timeout 900 s, 15 s first-acquire jitter | 4,000 acquires in 602 s = about 6.6/s | <= 50 ms (client max 124 ms) | 0 5xx/503, 1 transport (retried), 0 FAILED |
| Knee (about 898k PENDING), 2 s subtasks, 1 / 10 / 50 sims | 0.5 / 4.7 / 23.5 per s | <= 100 ms | 0 5xx; 17 sheds at 50 sims |
| Stress: 500 sims, 1,000,000 PENDING, 2 s subtasks, no jitter | about 169/s (about 25x realistic) | <= 500 ms (interpolated ~435 ms), p50 <= 250 ms | 0 5xx, 0 FAILED, 11 % of acquire calls shed with 503 + Retry-After |

The stress mix is a 500-executor fleet finishing a subtask every 2 s. A real subtask is a batch of
embeddings or completions that takes minutes. The stress run is capped by server CPU, not PostgreSQL:
the four server workers ran at a median 388 % (max 409 %) while PostgreSQL ran at a median 94 % (about
one core of twelve), with the load generator on the same cores.

## Decision

- NF-1 is defined against a **tested numeric scope**: 500 executors, 0.9 to 1M PENDING subtasks
  (894,924 in the realistic run), production heartbeat 300 s with watchdog timeout 900 s, first-acquire
  jitter 15 s, 75 s subtasks, and an aggregate acquire rate up to the highest measured passing rate:
  about 24 acquires/s (23.5 completes/s; 50 sims, server p99 <= 100 ms) and about 6.6/s (the realistic run, server p99 <=
  50 ms). Within that scope it is **met**. Above that rate the contract is fail-fast shedding, not a
  latency promise.
- It is **not met** for the 2 s-work stress mix (gap about 2x over 200 ms). That is a capacity limit,
  not a correctness one, and it is documented here and in SPEC §9.
- Overload is handled by **fail-fast shedding**, not by latency: bounded pool wait, acquire concurrency
  cap and acquire deadline answer `503 SERVER_BUSY` / `ACQUIRE_DEADLINE` with `Retry-After`, which the
  executor honours like a 429 (TASK-7.4.4, ARCHITECTURE §3.2 overload paragraph; ADR-0014 covers only the ack heartbeat and ghost acquirements). Under the stress mix this gave 0 5xx,
  0 transport errors, 0 FAILED subtasks and 0 ghost acquirements.
- **Measured (TASK-36,** [report](../bench/m-task36-server-cpu-apple-m3-pro.md)**):** capacity does not
  grow linearly with `MINION_SERVER_WORKERS` on one Mac. S-sat goodput (500 sims, 0.25 s work, 1M PENDING,
  median of 3) is 69.6, 103.3, 159.7, 175.5, 178.3 and 164.3 completes/s for W = 1, 2, 4, 6, 8, 12
  (efficiency 1.00, 0.74, 0.57, 0.42, 0.32, 0.20; the host is at H = 0.83 at W=8 and 0.93 at W=12). The
  stress mix reaches 170/s at W=4 and 194/s at W=8 (near its ~200/s demand). Server CPU per subtask rises
  from 14.4 to 33.4 ms (W=1 to 8) together with the claim query mean (0.73 to 10.7 ms) and claim calls per
  win (1.06 to 1.73); `MINION_ACQUIRE_WINDOW=64` at W=8 gave +11 % goodput. Server and generator on
  separate hosts (NF-5) remain not measured.
- **No `FOR UPDATE SKIP LOCKED`.** Ownership stays decided solely by the unique-constraint
  `INSERT ... ON CONFLICT DO NOTHING` (invariant 2). The claim now costs 1.26 calls per win and
  2.97 ms mean under stress, so the contention is reduced from 19.4 INSERT attempts per win (M6.3) to 1.26.

## Consequences

- SPEC §9 NF-1 states the workload mix and links the report and this ADR.
- A fleet that is both large and finishes subtasks in seconds needs more workers or hosts; an
  operator sees it as 503 shedding (`minion_acquire_total{outcome=shed}`), not as slow acquires.
- Results are single runs on one Mac that also hosted the load generator; absolute numbers depend on
  that. NF-2 and NF-3 were verified at a 100k chaos drain (PASS); a 1M drain was not run.
- ADR-0001 revisit trigger ("M6 scale test misses NF-1 and profiling shows Python CPU time, not
  Postgres, as the bottleneck"): the stress pattern matches it (server containers at 388 % of 4 cores,
  PostgreSQL about 94 %), but container CPU from `docker stats` is not profiling, so the trigger is
  **not yet decided**. TASK-36 decides. A server rewrite (and an ADR-0001 revisit) is warranted if the
  profile shows Python interpreter work dominating the acquire/complete path and adding workers or
  hosts does not scale throughput roughly linearly; if throughput scales with workers, the Python
  server stays and the gap is a sizing question.
  **TASK-36 decided: the trigger is not met and ADR-0001 stands.** Python work is 67 to 80 % of
  non-idle worker samples and scaling is sub-linear, but the loss coincides with claim-query contention
  (5x to 15x slower claims at W=4 to 8 with PostgreSQL at 1.4 cores), so the rule's "not explained
  otherwise" condition fails. Follow-ups: cheaper SQLAlchemy statement handling, fewer claim collisions at
  W > 1.
- Revisit if the observed production acquire rate approaches the stress rate, or if TASK-36 finds
  server CPU per acquire that can be cut cheaply.
