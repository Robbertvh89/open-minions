---
id: TASK-32
title: Pre-auth rate limiting or dedicated auth pool for executor endpoints
status: Done
assignee: []
created_date: '2026-10-18 06:57'
updated_date: '2026-10-18 20:49'
labels:
  - security
dependencies: []
ordinal: 71000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Security LOW from TASK-7.4.4: every executor request (incl. unauthenticated bearer floods and acquires that get shed) does the token lookup on a pooled connection before any limiter, so the connections kept back from acquires are not reserved for owner ops. Options: per-IP/global token bucket before current_executor; small dedicated auth pool; short-TTL token-hash lookup cache (with revocation latency bound).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Unauthenticated flood cannot make owner ops (heartbeat/complete) wait on the pool (test)
- [x] #2 Revocation still takes effect within a documented bound
<!-- AC:END -->



## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## Goal (PR 1 of 2; TASK-33 follows on top)
Floods of executor requests with missing/garbage/unknown bearer tokens must not take the DB connections that owner ops (heartbeat/complete/fail/release) need. Revoking a token must still take effect within a documented bound. SPEC: S-3, S-8 (rotate), §6 overload shedding, NF-1 (owner ops stay fast under load), NF-5 (soft per-replica state).
AC1 -> integration flood test. AC2 -> `MINION_EXECUTOR_AUTH_CACHE_TTL_S` bound + test with a fake clock.

## Touchpoints
- auth.py:429-446 `current_executor`: one main-pool session per request (`app.state.sessionmaker`) before any limiter.
- repositories/executors.py:31 `get_by_token_hash` (unique index lookup; unchanged).
- app.py:44-49 engine; app.py:112-123 PoolTimeoutError -> 503 handler (logs a warning on every request).
- api/executor.py:59-78 `_limited` (runs after auth), :151-170 acquire slots.
- settings.py:26-37 pool/overload knobs, :213-216 `connection_budget`.
- metrics.py:112 counters. conftest.py:249 `_settings`.

## Decision: known-token cache + tiny dedicated pool for unknown tokens (no in-app per-IP limiting)
None of the three options works alone:
- Cache alone: random tokens always miss (negative caching is useless against them), so every flood request still takes a main-pool connection.
- Dedicated auth pool alone: legit owner ops also have to authenticate, so they queue on the same saturated auth pool.
- Per-IP/global bucket in the app: a global bucket also throttles legit executors. Per-IP needs X-Forwarded-For trust behind the pilot's TLS proxy, and the fleet is employee laptops that often share one corporate NAT egress IP, so a per-IP bucket would throttle the whole fleet. Rejected in-app. Per-IP limits stay at the proxy (ADR-0012, S-12). The server keeps ignoring X-Forwarded-For (it never needs the client IP), so there is no new trust setting.

Combination chosen:
1. Positive cache (per process, LRU): sha256 digest -> principal + token_expires_at + checked_at. A fresh hit (age < TTL) does not touch the DB. Expiry is checked on every hit against the cached `token_expires_at`, so expiry takes effect immediately.
2. A *known* digest (a positive entry, even a stale one) revalidates on the main pool, as today. Legit traffic only.
3. An *unknown* digest (no positive entry) is looked up on a separate engine, `app.state.auth_sessionmaker`: pool_size = `MINION_DB_AUTH_POOL_SIZE` (2), max_overflow 0, pool_timeout = `MINION_AUTH_LOOKUP_TIMEOUT_S` (1.0). If the wait times out: `503 SERVER_BUSY` + Retry-After 1-3 (busy_error), caught in `current_executor` so the app-wide handler does not log once per flood request. A metric is counted instead. A flood can therefore hold at most N_auth connections per worker, and never one from the main pool.
4. Negative cache (LRU, fixed 10 000 entries, same TTL): not-found/revoked/expired digests answer 401 with no DB call. This only helps against a revoked or misconfigured executor that keeps retrying. AC1 does not rely on it.
Revocation bound: a revoked or rotated-away token keeps authenticating for at most `MINION_EXECUTOR_AUTH_CACHE_TTL_S` (default 10 s, 0..60; 0 = revalidate on every request) after the DB change, per worker. Workers do not add to it because each worker's cache expires on its own. Some paths are already immediate through DB filters: register (executors.py:94) and presence (report_state status filter). After TASK-33, acquire is immediate too.

## Steps
### Step 1: token cache + auth pool + settings (implementer, about 250 lines)
- NEW `minion_server/tokencache.py` (pure, no DB): `ExecutorTokenCache(ttl_s, max_entries, *, negative_max=10_000, clock=time.monotonic)`; `lookup(digest) -> Lookup` where `Lookup.state` is one of `fresh|stale|negative|unknown` (with `principal`, `token_expires_at` when positive); `put_valid(digest, principal, token_expires_at)`; `put_invalid(digest)` (drops the positive entry, adds a negative one). OrderedDict LRU, no await between read and write (event loop only, like ratelimit.py). With ttl 0 positive entries are always stale and negative entries never fresh.
- auth.py `current_executor`: the flow above. Lookups stay `get_by_token_hash` + `hmac.compare_digest`. Valid -> `put_valid`. Invalid (missing, REVOKED, expired) -> `put_invalid` + 401. A known-stale lookup that fails with a pool timeout keeps the existing 503 behaviour. Never log tokens or digests.
- app.py lifespan: second `create_engine(..., pool_size=cfg.db_auth_pool_size, max_overflow=0, pool_timeout=cfg.auth_lookup_timeout_s)`; `app.state.auth_engine`, `app.state.auth_sessionmaker`, `app.state.executor_tokens = ExecutorTokenCache(...)`; dispose the auth engine in `finally`.
- settings.py: `executor_auth_cache_ttl_s: float = Field(10.0, ge=0, le=60)`, `executor_auth_cache_max_entries: int = Field(50_000, gt=0)`, `db_auth_pool_size: int = Field(2, ge=1, le=8)`, `auth_lookup_timeout_s: float = Field(1.0, ge=0.1, le=5.0)`. `connection_budget` = workers x (pool + overflow + db_auth_pool_size) + watchdog. The startup warning in app.py:136 picks this up automatically.
- metrics.py: `minion_executor_auth_total{outcome}` with outcome in `cache_hit|negative_hit|db_valid|db_invalid|busy` (fixed values, no ids).
- conftest.py `_settings`: `executor_auth_cache_ttl_s=0`, like the other limits disabled there, so the existing revoke/expire/rotate tests (test_auth.py:120-134, 280-340, test_presence.py:106, test_completion.py:364, test_read_api.py:283/427) keep their immediate-401 semantics. New tests opt in to a TTL.
- Unit tests `tests/test_tokencache.py`: fresh/stale/negative/unknown with a fake clock; ttl 0; LRU bounds on both maps (a random-digest flood never evicts positives); put_invalid removes the positive entry. `test_settings.py`: ranges and the connection_budget formula. `test_metrics.py`: new outcomes.

### Step 2: integration tests (test-engineer), `tests/test_auth_flood.py` (marked integration)
- AC1 flood: make(db_pool_size=2, db_max_overflow=0, db_pool_timeout_s=0.5, acquire_concurrency=1, db_auth_pool_size=1, auth_lookup_timeout_s=0.2), parametrized over executor_auth_cache_ttl_s in {0, 60}. One executor acquires (warms the cache, gets an acquirement). Monkeypatch `executors_repo.get_by_token_hash` with a wrapper that sleeps 1 s for unknown digests and records `session.bind`. Fire 50 concurrent requests with random bearers at heartbeat/acquire, and during the flood send the legit heartbeat and release. Assert: legit 200/204 with latency < 0.5 s; flood responses are only 401 or 503 SERVER_BUSY with numeric Retry-After, never 500; no flood lookup ran on the main engine; peak concurrent unknown lookups <= db_auth_pool_size.
- AC2 revocation: TTL 5 with a fake clock swapped into `app.state.executor_tokens`. Request ok -> revoke in DB -> (documents the bound) -> advance the clock past TTL -> 401 -> a further request is 401 with no DB call (negative hit, spy). Same for rotate: the old token is 401 after TTL and the new token works at once (unknown lane). A token whose `token_expires_at` passes while cached -> 401 on the next hit with no clock advance.
- Regression: `test_overload.py::test_full_acquire_slots_shed_before_any_candidate_work` still passes (the comment at :219 about the auth lookup may need a tweak); `test_protocol_races` 50-way acquire with fresh executors stays green with the auth pool of 2 (lookups take ms, each waits < 1 s). If it is flaky, raise `db_auth_pool_size` in that test, not the default.

### Step 3: docs (docs-keeper)
- SPEC S-3 ("401 on its next request" -> within TTL; expiry immediate), S-8 rotate (old token 401 within TTL), §6 overload paragraph: new item (d) on unknown-token lookups via the auth pool -> 503 SERVER_BUSY; NF-5: the cache is soft per-replica state.
- ARCHITECTURE §3.2: replace the "Residual: the connection used by the auth lookup..." sentence. §6: connection budget formula + reverse-proxy note (per-IP limits at the proxy sized for NAT; server ignores XFF).
- NEW ADR-0017 "Executor token cache with a bounded lane for unknown tokens" (alternatives and NAT/XFF argument as above); ARCHITECTURE §9 index. CLAUDE.md minion-server command line: the 4 new env vars. admin.py revoke/rotate output: add "(takes effect within MINION_EXECUTOR_AUTH_CACHE_TTL_S, default 10 s)" plus the test_auth CLI assertion.

## Invariants
- 1, 2, 4, 5: untouched. 3: ownership checks (acquirement_id + executor_id under the row lock) are unchanged; the cache only affects *who* is authenticated, and a stale identity cannot pass another executor's acquirement check. Security: cache keys are SHA-256 digests (never raw tokens), process memory only, never logged. The dict lookup timing on a digest is no worse than today's unique-index lookup.

## Risks / open questions
- Behaviour change: revocation is no longer instant for complete/fail/release/heartbeat (<= 10 s). It is documented, and TTL 0 restores the old behaviour. Flag to the security-auditor.
- After a server restart every executor is unknown and authenticates through the 2-connection pool. Lookups take about 1-2 ms, so about 1000/s per worker is fine for the pilot. If a flood coincides with a restart, legit first contacts may see 503 and retry (all executor paths honour 503 + Retry-After; implementer: confirm register/release do too, loop.py:128 `_is_shed`).
- The extra connections count against the PG budget (workers x 2).
- ADR number collision with the in-progress TASK-19: settle the number at merge time.
<!-- SECTION:PLAN:END -->
