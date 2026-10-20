---
id: TASK-30
title: Guard against many unrunnable live tasks slowing acquire
status: In Progress
assignee: []
created_date: '2026-10-18 04:08'
updated_date: '2026-10-20 03:07'
labels:
  - security
dependencies: []
ordinal: 68000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Security LOW from TASK-7.4.2: the live-task walk filters engine/model/memory/pool as Filter, not Index Cond, so a user can create many high-priority tasks no executor can run (e.g. model_memory_gb above any fleet budget) and every acquire walks past them (~1 us each). Options: per-team cap on live tasks; feasibility check at submission (model in allow-list with memory_gb <= max profile memory); index on (priority, created_at) including model_id.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A per-team live-task cap or submit-time feasibility check exists with a clear error code
- [ ] #2 Test: 5k unrunnable tasks don't push acquire beyond a stated bound
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
# TASK-30 plan: per-team live-task cap (guard against unrunnable live tasks slowing acquire)

Worktree: /Users/robbertvanhintum/Documents/claude_code/open-minions-task30 (fix/task-30-unrunnable-tasks)

## 1. Goal and acceptance criteria

Goal: one user should not be able to make every acquire slow by flooding the live queue with
high-priority tasks that no executor will take. The acquire candidate walk (`candidates_stmt`)
goes through live tasks in FR-22 order on `ix_minion_task_live_queue`. It checks
model/engine/memory/pool as a heap **Filter**, not an Index Cond, so each such task costs about
1 us per acquire.

- AC#1: a per-team live-task cap with a clear error code exists.
  SPEC: new FR-9 (§4.1), FR-1/FR-2 (create path, REST + UI the same), NF-5 (enforced in the DB,
  holds across workers and replicas), §6 error body list, S-1 (keys-file `teams` policy).
- AC#2: a test with 5,000 unrunnable live tasks shows acquire stays within a stated, deterministic
  bound. SPEC: NF-1 (acquire p99 < 200 ms), FR-16/FR-22 (the filters and the order are unchanged).

## 2. Recommendation (one approach): per-team live-task cap, enforced in the DB

`MINION_MAX_LIVE_TASKS_PER_TEAM` (default 500, 1..100000). A team can override it in the keys file
(`teams.<slug>.max_live_tasks`, live-reloaded like the pool policy). Live = `SPLITTING|PENDING|RUNNING`.
Over the cap, create answers **`429 TOO_MANY_LIVE_TASKS`** (no Retry-After). How it is enforced:
inside the create transaction, take `pg_advisory_xact_lock` keyed by team, then do a bounded count
on a new partial index, then the INSERT.

Why this approach and not the other two:
- **Submit-time feasibility check: rejected as the guard.**
  (a) The user does not choose `memory_gb`. It is snapshotted from the operator's `models.yaml`
  (`services/tasks.py:132`, `repositories/tasks.py:70`), and model-in-allow-list is already
  enforced (`422 UNKNOWN_MODEL`, `services/tasks.py:133-134`).
  (b) The largest budget is not a constant. Profiles are RAM fractions: light 0.25, balanced 0.40,
  full 0.70, plus a custom `memory_gb` (`minion_executor/capacity/profiles.py:48-50`). The server
  only sees registered `total_memory_gb`, which is fleet state. A fleet-based check would reject
  tasks submitted before the fleet is online. A `MINION_MAX_MODEL_MEMORY_GB` constant would only
  lint `models.yaml`.
  (c) It misses the vectors users actually control. `allowed_pool` is free text for teams without a
  pool policy (`auth.py:418-430`), priority goes up to 1000 (`schemas.py:150`), and opt-in engines
  (ollama) nobody runs. The cap covers every vector, including a model removed from `models.yaml`
  later.
- **Index change: rejected.**
  (a) The order needs `(priority DESC, created_at, team, project, task_name)` as the leading keys.
  `model_id IN (...)`, `engine IN`, `memory <=` and `pool IS NULL OR =` are not prefix equalities,
  so they can only ever be Filters.
  (b) A leading `model_id` loses FR-22 order. PG16 does not MergeAppend an IN-list, so it would sort
  all matching live tasks.
  (c) `INCLUDE (model_id, engine, model_memory_gb, allowed_pool)` only helps an Index Only Scan.
  The `live` subquery also selects `output_uri` and `params` (JSONB) (`acquisition.py:100-110`), so
  it would need a keys-only rewrite plus a join-back. That saves heap fetches, a constant factor,
  not an asymptotic one, and every status update would maintain a wider index.
- **Runnable task spam is already cheap.** The walk stops once `window` candidates are found.
  Only tasks that sort ahead and yield nothing cost anything:
  (i) tasks this executor filters out: the cap bounds them;
  (ii) eligible tasks with no PENDING subtask: these already have a bound and a test
  (`test_candidate_index_path.py:296-302`, ~3 buffers each, and they are bounded by
  executors x MINION_MAX_OPEN_ACQUIREMENTS).

The bound is stated as: extra walk cost per acquire <= sum over teams of `cap`, times the measured
per-filtered-task cost. One key (up to 64 teams, `auth.py:83`) can add at most 64 x cap tasks.
Default 500 means 5,000 unrunnable tasks need 10 teams at the cap. That is the AC#2 scenario.

### Decisions on the open points
- **Error code:** `429 TOO_MANY_LIVE_TASKS`, the same family as FR-23 `429 TOO_MANY_ACQUIREMENTS`
  (a quota that frees up over time). No `Retry-After`: there is no meaningful time.
  - CLI: `create_task` is not retried (`minion_cli/flows.py:188-191`), and `api_failure` maps it
    to exit 2 with `server rejected the request: TOO_MANY_LIVE_TASKS ...` (`flows.py:67-75`).
  - The UI renders `CODE: message` with the form refilled (`ui/routes.py:464-475`).
  - Message: `team has N live tasks (limit N); wait for some to finish or cancel some`. It must
    not echo any payload.
- **Admin override:** per team in the keys file: `max_live_tasks: int | None` (ge=1, le=100000).
  It is live-reloaded. The global default comes from the env.
  - No disable switch (0) and no operator-key bypass. Operator keys stay about read scope (YAGNI).
- **Exactness and the race:** a count followed by an INSERT under READ COMMITTED is racy.
  Overshoot would be bounded only by concurrent in-flight creates.
  - Use `SELECT pg_advisory_xact_lock(hashtextextended('minion_task_live:' || :team, 0))` as the
    first statement of the existing create transaction (`services/tasks.py:152`).
  - Precedent: ADR-0019 locks the executor row for its cap. There is no team row, hence the
    advisory lock. Hash collisions between teams only serialise their creates.
  - The lock is held for count + INSERT only (ms). The S3 `any_key` call stays before the
    transaction (`services/tasks.py:149`).
  - No deadlock: nothing else takes this lock, and the create transaction takes no other locks
    before it.
  - It is **not** an ownership mechanism, so invariant 2 is untouched. Say so in the ADR.
- **Count path:** new partial index `ix_minion_task_team_live ON minion_task (team) WHERE status
  IN ('SPLITTING','PENDING','RUNNING')`, in **new migration 0014**. Count as
  `SELECT count(*) FROM (SELECT 1 FROM minion_task WHERE team=:t AND status IN (<literals>) LIMIT :cap) s`.
  That is O(cap) even if the planner ignores the index. Literal statuses (the `_lit` pattern,
  `acquisition.py:67-73`) keep generic plans matching the partial index. Without the index, the
  count would scan the team's full history via `ix_minion_task_team_created` (`db/models.py:97-103`).
- **Order of checks:** the cap check runs after the cheap 403/422 validations and `EMPTY_INPUT`,
  inside the transaction before the INSERT. So a team at the cap gets 429 even for a duplicate
  name. Documented, and acceptable.
- **SPLITTING counts:** a SPLITTING task becomes live without passing the check again.
  CANCELLED/SUCCEEDED/FAILED/PARTIAL free their slot. Cancel is immediate (`repositories/tasks.py`
  `cancel_task`).

## 3. Touchpoints (current code)
- `packages/minion-server/src/minion_server/services/tasks.py:118-167`: `create_task`, the
  transaction at :152-166 (insert + TASK_EXISTS/OUTPUT_URI_IN_USE).
- `packages/minion-server/src/minion_server/repositories/tasks.py:47-87`: `insert_task`.
- `packages/minion-server/src/minion_server/auth.py:71-77` (`TeamPolicy`), `:96-109`
  (`_TeamEntry`, extra=forbid), `:127-129` (`_FIELD_NAMES`, used for error-path redaction at
  :286), `:259-264` (policy build), `:418-430` (`resolve_pool`, unchanged semantics).
- `packages/minion-server/src/minion_server/settings.py:102-108`: next to `acquire_window` and
  `max_open_acquirements`.
- `packages/minion-server/src/minion_server/db/models.py:83-110`: `Task.__table_args__` (add the index).
- `migrations/versions/0013_task_counter_cancelled.py`: head. New file `0014_task_team_live_index.py`
  in the style of 0009/0012 (`SET LOCAL lock_timeout='5s'`, plain CREATE INDEX: minion_task is small).
- `packages/minion-server/src/minion_server/repositories/acquisition.py:76-178`: `candidates_stmt`.
  **Not changed**; AC#2 measures it.
- `packages/minion-server/tests/test_candidate_index_path.py:208-302`: EXPLAIN helpers (`_json_plan`,
  `_nodes`, `_buffers`, `assert_index_paths`, `_plans`) and the precedent bound test.
- `packages/minion-server/src/minion_server/ui/routes.py:436-475`: UI create. No code change (same service).
- Docs: `docs/SPEC.md:53-81` (§4.1), `:363-372` (error codes), `:445-449` (S-1); `docs/ARCHITECTURE.md:384-394`
  (residual cost note); `docs/adr/0020-per-team-live-task-cap.md` (new); `CLAUDE.md` server command line;
  `.env.example:42` neighbourhood.

## 4. Steps

### Step 1: config and team policy (no DB) [implementer, ~120 lines]
- `settings.py`: `max_live_tasks_per_team: int = Field(default=500, ge=1, le=100_000)`, env
  `MINION_MAX_LIVE_TASKS_PER_TEAM`, with a comment pointing at FR-9 / ADR-0020.
- `auth.py`:
  - `TeamPolicy` gains `max_live_tasks: int | None = None`.
  - `_TeamEntry` gains `max_live_tasks: int | None = Field(default=None, ge=1, le=100_000)`
    (StrictInt-like: reject bool/float the way the other fields do).
  - Add `"max_live_tasks"` to `_FIELD_NAMES`, and pass it through at :259-264.
- New pure helper in `services/tasks.py`:
  `def live_task_cap(policy: TeamPolicy | None, settings: ServerSettings) -> int`. It returns the
  team value if set, else the global default.
- Unit tests (`packages/minion-server/tests/test_live_task_cap_config.py`):
  - settings bounds (0, 100001 rejected);
  - keys file with `max_live_tasks` parses;
  - bool/0/negative/str rejected (the file fails closed like other invalid files);
  - a pool-only entry is unchanged;
  - an entry with only `max_live_tasks` leaves `resolve_pool` returning the requested pool
    (S-1 semantics unchanged);
  - `live_task_cap` precedence.
- Invariants: none touched.

### Step 2: migration, repo, service enforcement, error [implementer + `db-migration` skill, ~250 lines]
- `db/models.py`: add
  `Index("ix_minion_task_team_live", "team", postgresql_where=text("status IN ('SPLITTING', 'PENDING', 'RUNNING')"))`.
- `migrations/versions/0014_task_team_live_index.py` (revises 0013): create/drop with
  `SET LOCAL lock_timeout = '5s'` in both directions. Do not edit any applied migration.
- `repositories/tasks.py`:
  - `LIVE_TASK_LOCK_PREFIX = "minion_task_live:"`
  - `async def lock_team_for_create(session: AsyncSession, team: str) -> None`:
    `SELECT pg_advisory_xact_lock(hashtextextended(:k, 0))`.
  - `async def count_live_tasks(session: AsyncSession, team: str, *, limit: int) -> int`:
    LIMIT-bounded count with literal statuses (reuse or promote `_lit` from
    `repositories/acquisition.py`, e.g. move it to `repositories/_sql.py`; keep both import sites working).
- `services/tasks.py` `create_task`, inside `async with session.begin():` before `insert_task`:
  ```python
  cap = live_task_cap(keys_config.teams.get(body.team), settings)
  await tasks_repo.lock_team_for_create(session, body.team)
  if await tasks_repo.count_live_tasks(session, body.team, limit=cap) >= cap:
      raise api_error(429, "TOO_MANY_LIVE_TASKS", f"team has {cap} live tasks (limit {cap}); wait for tasks to finish or cancel some")
  ```
  Log `task_create_capped` (team, cap; no task name or payload) at info level. No new metric (keep simple).
- Integration tests (`packages/minion-server/tests/test_live_task_cap.py`, `integration`; use the
  `live`/`_serve(..., extra={"max_live_tasks_per_team": 2})` fixtures in `conftest.py:671-708`):
  1. Two creates succeed, the third gets `429`, body `{"detail":{"code":"TOO_MANY_LIVE_TASKS",...}}`,
     no row inserted. Another team is unaffected.
  2. Cancel one, then create succeeds. A task set to SUCCEEDED/FAILED/PARTIAL via SQL frees a slot.
     A SPLITTING task counts.
  3. A keys-file override `max_live_tasks: 3` beats the global 2, and is picked up after live reload.
  4. **Concurrency:** cap 5, fire 20 concurrent `POST /tasks` (distinct names and output URIs, shared
     input prefix). Exactly 5 get 202, 15 get 429, and DB live count = 5. Repeat 3x.
  5. UI `POST /ui/tasks` at the cap renders the 429 page containing `TOO_MANY_LIVE_TASKS`
     (if the UI fixture is cheap; otherwise a unit test of the shared service is enough).
  6. Plan: with 20k finished tasks of the team plus 400 live, `EXPLAIN` of the count (literal and
     generic, `plan_cache_mode=force_generic_plan`, as `_plans` does) uses `ix_minion_task_team_live`
     and has no Seq Scan on minion_task.
  7. Migration round-trip: upgrade head, downgrade -1, upgrade head (existing migration test
     pattern; the index exists after the upgrade and is gone after the downgrade).
- Invariants:
  - #1: executors are untouched.
  - #2: the advisory lock guards an admission quota on task creation, not subtask ownership. The
    acquirement INSERT stays the only arbiter.
  - #4: no payload goes through the server.
  - NF-5: DB-enforced, so it holds across workers and replicas.

### Step 3: AC#2 bound test, 5,000 unrunnable live tasks [test-engineer, ~150 lines]
Extend `packages/minion-server/tests/test_candidate_index_path.py` (reuse its helpers in-file):
`test_candidate_bounded_with_5k_unrunnable_live_tasks`, parametrized over `kind in {analyze, generic}`.
- Seed (SQL, as `_seed_big`): the existing `big` 12 runnable tasks (priority 0..2, 100 pending each)
  and 20k finished history. Add **5,000 live (RUNNING) tasks at priority 1000, created earlier**
  (they sort first), each with 3 PENDING subtasks, cycling 4 unrunnable reasons for the
  probe `engines=[FAKE], model_ids=["m1"], memory_gb=8.0, pool="default"`:
  - `model_memory_gb=512`
  - `engine='ollama'`
  - `allowed_pool='nowhere'`
  - `model_id='m-removed'`
  
  Then run `ANALYZE`.
- Hard (deterministic) assertions:
  1. The returned rows are all `team='big'` and equal the oracle (`old_candidates_stmt`) output:
     correctness, FR-22 order unchanged.
  2. `assert_index_paths(plan, live_index=False)`: no Seq Scan on minion_subtask, no Sort over subtask.
  3. **No subtask probe for an unrunnable task:** the sum of `Actual Loops` over nodes scanning
     `minion_subtask` / `ix_minion_subtask_pending` is <= the number of eligible tasks (<= 12).
  4. **Buffers:** `_buffers(plan) <= UNRUNNABLE_BUFFER_BOUND`. Set it to <= 4 per unrunnable task,
     i.e. 20,000. Measure first: if the measured value is far below that, tighten it to about
     1.5x measured, rounded up.
- Loose wall-clock ceiling (catches pathologies, not a perf claim): median `Execution Time` of 5
  `EXPLAIN ANALYZE` runs < 50 ms (NF-1 budget is 200 ms p99). Put the measured median in the PR
  and in the ARCHITECTURE note.
- Functional: one `repositories.acquisition.claim(...)` round on that seed (window 16, inside a
  transaction that is rolled back) wins a `big` subtask.
- Cap interplay (unit-level, no 5k API calls): with cap 500 per team, 5,000 unrunnable tasks need
  >= 10 teams at the cap. This is stated in the ADR, not tested.
- Invariants: read-only on the claim path. Invariant 2 is untouched (claim runs unchanged).

### Step 4: docs [docs-keeper, after reviewer]
- SPEC §4.1 new **FR-9** [ours]: per-team live-task cap, default 500 (1..100000), keys-file
  override, `429 TOO_MANY_LIVE_TASKS`, SPLITTING|PENDING|RUNNING count, DB-enforced. Add the code to
  the §6 error list (:363-372). In S-1, `teams: {slug: {default_pool, allowed_pools, max_live_tasks}}`.
- ARCHITECTURE §3.2 residual-cost note (:384-394): measured cost per unrunnable (filtered) live task
  (buffers, us, no subtask probe) and the bound `sum(team caps) x cost`. List the new index in §5.
- New `docs/adr/0020-per-team-live-task-cap.md`:
  - context: TASK-7.4.2 security LOW;
  - decision: cap, advisory xact lock (why not a row lock, why not invariant 2);
  - alternatives rejected: feasibility check and index change, with the reasons above;
  - consequences: CLI uploads before create (re-run reuses identical parts), duplicate name at the
    cap gives 429.
  - Deviates from paper: adds a quota the paper doesn't have.
- `CLAUDE.md` server command line: mention `MINION_MAX_LIVE_TASKS_PER_TEAM`. `.env.example`: one
  line. ADR-0007: keys-file field list gains `max_live_tasks`.

## 5. Risks and open questions
- **Default 500 could reject legitimate sweeps.** Mitigations: the per-team override, and a clear
  message. Orchestrator: confirm 500, or pick 1000 (doubles the worst case).
- **Migration 0014 / ADR-0020 number collisions** with parallel worktrees (TASK-53 may add an
  index; TASK-12/20/43.1 may add ADRs). Re-check `migrations/versions` and `docs/adr` at rebase.
  Renumber before merge, never after.
- **Advisory locks are new to the codebase.** The reviewer must confirm the lock is
  transaction-scoped (`_xact_`) so it is never leaked by the pool, and that no other path takes it.
- **The cap is per team, not per key.** A key with 64 teams gets 64 x cap. That is accepted: team
  membership is operator-granted. Document it in the ADR.
- **Not covered:** a priority cap per team. An operator wanting stricter isolation can lower
  `max_live_tasks` per team.
- **The planner may switch to a bitmap scan + sort of the live tasks** at 5k (already noted in
  `test_candidate_index_path.py:298`). It is still O(live). The test allows `live_index=False`.

## 6. Optional follow-ups (do not run; orchestrator decides)
```
backlog task create "Reject allowed_pool with no issued executor token (UNKNOWN_POOL)" -d "Follow-up to TASK-30: 422 when no non-revoked minion_executor row has the pool; false rejection before tokens are issued" -l security
backlog task create "Lint models.yaml memory_gb against MINION_MAX_MODEL_MEMORY_GB at startup" -d "Follow-up to TASK-30: operator footgun, not a user guard" -l server
```
<!-- SECTION:PLAN:END -->
