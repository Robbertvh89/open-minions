# ADR-0016: NF-1 is defined against the realistic workload

- Status: **Accepted** (TASK-7.4.7, M6.4g)
- Deviates from paper: no (the paper gives no acquire latency target; this scopes our own NF-1)

## Context

NF-1 reads "acquire p99 < 200 ms with 500 executors and 1M pending subtasks". The M6.3 run failed it
at every intensity. After TASK-7.4.2 to 7.4.8 the [M6.4 re-run](../bench/m6.4-scale-apple-m3-pro.md)
(4 server workers, 500 sims, 1M pending, same 12-core Mac as the load generator) shows two regimes:

| Mix | Acquire demand | Server p99 (bucket) | Errors |
|---|---|---|---|
| Realistic: 75 s subtasks, heartbeat 300 s : timeout 900 s, 15 s first-acquire jitter | about 7/s | <= 50 ms (client max 124 ms) | 0 5xx, 0 sheds, 0 FAILED |
| Knee, 2 s subtasks, 1 / 10 / 50 sims | 0.5 / 4.7 / 23.5 per s | <= 100 ms | 0 |
| Stress: 500 sims, 2 s subtasks, no jitter | about 169/s (about 25x realistic) | <= 500 ms (interpolated ~435 ms), p50 <= 250 ms | 0 5xx, 0 FAILED, 11 % of acquire calls shed with 503 + Retry-After |

The stress mix is a 500-executor fleet finishing a subtask every 2 s. A real subtask is a batch of
embeddings or completions that takes minutes. The stress run is capped by server CPU, not PostgreSQL:
the four server workers ran at a median 388 % (max 409 %) while PostgreSQL ran at a median 94 % (about
one core of twelve), with the load generator on the same cores.

## Decision

- NF-1 is defined against the **realistic workload**: production heartbeat ratio, jittered first
  acquire, minutes-long subtasks. It is **met** for that mix (server p99 <= 50 ms at 500 executors and
  1M pending) and at the knee up to about 50 simulated executors.
- It is **not met** for the 2 s-work stress mix (gap about 2x over 200 ms). That is a capacity limit,
  not a correctness one, and it is documented here and in SPEC §9.
- Overload is handled by **fail-fast shedding**, not by latency: bounded pool wait, acquire concurrency
  cap and acquire deadline answer `503 SERVER_BUSY` / `ACQUIRE_DEADLINE` with `Retry-After`, which the
  executor honours like a 429 (ADR-0013, ADR-0014, TASK-7.4.4). Under the stress mix this gave 0 5xx,
  0 transport errors, 0 FAILED subtasks and 0 ghost acquirements.
- Stress capacity scales with `MINION_SERVER_WORKERS` and with server hosts (NF-5: the server is
  stateless), because server CPU is the limit, not Postgres. Not measured: more than 4 workers, or
  server and generator on separate hosts. Profiling of server CPU per acquire is TASK-36.
- **No `FOR UPDATE SKIP LOCKED`.** Ownership stays decided solely by the unique-constraint
  `INSERT ... ON CONFLICT DO NOTHING` (invariant 2). The claim now costs 1.26 calls per win and
  2.97 ms mean under stress, so the contention that motivated the question is gone.

## Consequences

- SPEC §9 NF-1 states the workload mix and links the report and this ADR.
- A fleet that is both large and finishes subtasks in seconds needs more workers or hosts; an
  operator sees it as 503 shedding (`minion_acquire_total{outcome=shed}`), not as slow acquires.
- Results are single runs on one Mac that also hosted the load generator; absolute numbers depend on
  that. NF-2 and NF-3 were verified at a 100k chaos drain (PASS); a 1M drain was not run.
- Revisit if the observed production acquire rate approaches the stress rate, or if TASK-36 finds
  server CPU per acquire that can be cut cheaply.
