# ADR-0018: Executor token cache with a bounded lane for unknown tokens

- Status: **Accepted** (TASK-32)
- Deviates from paper: no (authentication is ours, SPEC S-3)

## Context

Every executor request looked up its token on a pooled connection before any limiter. A flood of
requests with missing, garbage or revoked bearer tokens therefore took the connections that owner
operations (heartbeat, complete, fail, release) need, and acquire slot shedding did not help
because it runs after authentication.

## Decision

1. **Positive cache** per server process (LRU, `MINION_EXECUTOR_AUTH_CACHE_MAX_ENTRIES`, 50 000),
   keyed by the SHA-256 digest of the token (never the raw token), holding the principal and
   `token_expires_at`. A hit younger than `MINION_EXECUTOR_AUTH_CACHE_TTL_S` (10 s, 0..60) skips
   the DB. `token_expires_at` is checked on every hit, so expiry is immediate.
2. A digest with a stale positive entry (a known executor) revalidates on the main pool.
3. A digest never seen goes to a **separate engine**: `MINION_DB_AUTH_POOL_SIZE` (2) connections,
   no overflow, `MINION_AUTH_LOOKUP_TIMEOUT_S` (1.0) wait, then `503 SERVER_BUSY` + `Retry-After`
   1..3. This is counted in `minion_executor_auth_total{outcome=busy}` and not logged per request.
   A flood can hold at most that many connections per worker and never one from the main pool.
4. **Negative cache** (LRU, 10 000, same TTL) answers 401 for digests found invalid with no DB call.
   It only helps against a retrying revoked executor; the flood bound does not rely on it.
5. Metric `minion_executor_auth_total{outcome=cache_hit|negative_hit|db_valid|db_invalid|busy}`.
6. `connection_budget` = workers x (pool + overflow + auth pool) + watchdog.

**Revocation bound.** A revoked or rotated-away token keeps authenticating for at most the TTL
after the DB change, per worker (each worker's cache expires on its own). `0` restores immediate
revocation. Acquire, register, presence and heartbeat are immediate: their UPDATE matches the live
row (not revoked) and the token hash the request authenticated with, and a miss answers 401 and
drops the digest from the cache. Only complete/fail/release (ownership-checked by acquirement id)
rely on the TTL. `admin revoke/rotate` prints the configured bound.

**Warm start.** At startup each worker preloads live executors as stale entries (one query), so a
known executor revalidates on the main pool and never competes with a flood for the auth lane.
A revalidation records the time before its DB read, so a concurrent 401 is not overwritten.
Concurrent stale lookups of one digest are not coalesced (not worth the cancellation risk).

## Alternatives rejected

- Cache alone: random tokens always miss, so each flood request still took a main-pool connection.
- Dedicated pool alone: legitimate owner ops authenticate too and would queue behind the flood.
- Per-IP or global token bucket in the app: a global bucket also throttles legitimate executors;
  per-IP needs trusted `X-Forwarded-For`, and laptops behind a corporate NAT share one egress IP, so
  a per-IP bucket would throttle the whole fleet. The server keeps ignoring `X-Forwarded-For` and
  has no new trust setting; per-IP limits belong at the TLS reverse proxy (ADR-0012), sized for NAT.

## Consequences

- Revocation is no longer instant for complete/fail/release (bounded by the TTL).
- `PUT /executors/me/preference` also does not re-check the token, so a revoked token can change its own
  work preference for up to the TTL; nothing leaks, since acquire is refused immediately.
- Executors registered after a worker started are unknown to it and authenticate through the small
  pool once (about 1-2 ms per lookup); a first-contact herd may see 503 and retries. Executor register honours 503 +
  Retry-After; release is best effort and falls back to the watchdog.
- Auth pool connections count against the Postgres budget (workers x 2).
