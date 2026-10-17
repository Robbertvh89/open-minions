# ADR-0012: In-memory per-replica rate limits on executor endpoints

- Status: **Accepted** (TASK-6.2, plan decision D4)
- Deviates from paper: no (the paper does not cover abuse protection)

## Context

A buggy or hostile executor can hammer `register`, `acquire` and `presence`, each a DB write or a
candidate scan. Heartbeats already have a DB-backed minimum interval. NF-5 wants a stateless server.

## Decision

- `minion_server/ratelimit.py`: a minimum interval per `(executor_id, op)`, monotonic clock, bounded
  LRU (40 000 keys by default), kept in process memory. Intervals: `MINION_REGISTER_MIN_INTERVAL_S` 5,
  `MINION_ACQUIRE_MIN_INTERVAL_S` 0.2, `MINION_PRESENCE_MIN_INTERVAL_S` 5,
  `MINION_PREFERENCE_MIN_INTERVAL_S` 1; `0` disables. LRU bound `MINION_RATE_LIMIT_MAX_ENTRIES`
  (default 40 000). `check` is guarded by a lock and the dependency is `async` (runs on the event
  loop, not the threadpool), so admission is atomic.
- Executor-side pacing: the loop keeps `MINION_EXECUTOR_ACQUIRE_PACE_S` (0.4, 0 or >= 0.2; margin over the 0.2 s server interval absorbs network jitter) between
  acquire calls, sleeping only the remainder, so back-to-back instant subtasks never earn a 429.
- Applied as a route dependency after authentication and before any DB work: `429
  TOO_MANY_REQUESTS` + `Retry-After`. Only admitted calls are recorded.
- The counters are soft state, not coordination state: N replicas admit up to N calls per interval,
  and a restart forgets them. Nothing depends on them for correctness; ownership is still decided
  only by the `INSERT ... ON CONFLICT DO NOTHING` (invariant 2). Per-IP limits stay at the proxy.
- Executors treat 429 as a back-off of at least `Retry-After` (cap 300 s), also across a pause.

## Alternatives

A DB or Redis counter: shared and exact, but adds a write on the hot path or a new dependency to
guard against a problem that approximate limits solve.
