# ADR-0020: Per-team live-task cap under a per-team advisory lock

- Status: **Accepted** (TASK-30)
- Deviates from paper: yes (adds an admission quota the paper does not have; ownership is unchanged)

## Context

The acquire candidate walk (`candidates_stmt`, ARCHITECTURE section 3.2) visits live tasks in FR-22
order on `ix_minion_task_live_queue`. Model, engine, memory and pool are checked as a heap Filter,
not an Index Cond, so every live task an executor cannot run costs about 1 us per acquire before the
walk reaches a runnable one. A user could flood the queue with high-priority tasks nobody can run
(`allowed_pool` is free text for teams without a pool policy, priority goes up to 1000, a model can
leave `models.yaml` later) and slow every acquire (TASK-7.4.2 security finding, LOW).

## Decision

1. At most `MINION_MAX_LIVE_TASKS_PER_TEAM` (500, 1..10000) tasks in `SPLITTING`, `PENDING` or
   `RUNNING` per team. Create above it answers `429 TOO_MANY_LIVE_TASKS`, no `Retry-After`. The keys
   file can override it per team (`teams.<slug>.max_live_tasks`, live-reloaded). There is no 0 switch
   and no operator bypass.
2. Enforcement, inside the existing create transaction, before the INSERT:
   `pg_advisory_xact_lock(hashtextextended('minion_task_live:' || team, 0))`, then a count bounded by
   `LIMIT cap` over the new index `ix_minion_task_team_status (team, status)` (migration 0014; status
   values are SQL literals). The index is deliberately **not partial** and has mcv extended statistics
   `st_minion_task_team_status`, so the planner knows a team with a long finished history has few live
   tasks and does not choose a Seq Scan (checked for literal and generic plans). The lock is transaction-scoped (never leaked by the pool) and held for
   the count and INSERT only; the S3 existence check stays before the transaction. The lock wait is
   capped by `SET LOCAL lock_timeout = '2s'`; on timeout create answers `503 SERVER_BUSY` +
   `Retry-After`. Hash collisions between teams only serialise their creates. The cap is exact under
   READ COMMITTED because the count runs after the lock and takes a fresh statement snapshot, so it
   sees every create committed before; without the lock a count-then-insert would overshoot by the
   number of concurrent creates.
3. **Admission only.** The advisory lock guards a quota on task creation. It never decides who owns a
   subtask attempt: the `INSERT ... ON CONFLICT DO NOTHING` into `minion_subtask_acquirement` is still
   the only arbiter (invariant 2 is unchanged), and nothing on the acquire path takes this lock.
4. The cap check runs after the cheap validations, so a team at its cap gets 429 even for a duplicate
   task name. A `SPLITTING` task that becomes live does not pass the check again. Terminal states and
   cancel free the slot at once.

Bound: the extra walk cost per acquire is at most the sum over teams of `cap` times the cost per
filtered task. A key may belong to up to 64 teams, so 64 x cap tasks at most; with the default, 5,000
unrunnable tasks need 10 teams at their cap. Measured with 5,000 such tasks
(`test_candidate_bounded_with_5k_unrunnable_live_tasks`): 110 buffers, 2 subtask probes, about 1 ms.

## Alternatives rejected

- **Submit-time feasibility check.** `memory_gb` is snapshotted from the operator's `models.yaml`, and
  the model allow-list is already enforced. The executor budget is a fraction of RAM per profile
  (plus a custom value), and the server only knows the fleet that is registered now, so a fleet check
  would reject tasks submitted before executors come online. It also misses the vectors users control
  (`allowed_pool`, priority, engines nobody runs, a model removed later).
- **A partial index on the live statuses** (first implementation). It has the same predicate as
  `ix_minion_task_live_queue` plus SPLITTING, so the acquire candidate query could use it and the
  planner replaced the ordered live-queue walk with a bitmap scan plus sort: about 120x slower at
  10,000 same-priority live tasks (0.1 -> 13.5 ms, reviewer measurement). The shipped index is
  `(team, status)`. It cannot serve the general candidate walk (no `team` equality, wrong order), so
  the ordered live-queue walk is unchanged. With a team preference (`prefer --team`) the generic plan
  may bitmap-scan that team's live tasks on it: O(preferred team's live tasks) <= cap, measured
  0.10 / 0.15 / 0.30 / 0.58 ms at 50 / 100 / 250 / 500 live tasks. A guard test
  (`test_cap_index_does_not_displace_live_queue_walk`) pins the walk with the cap index present.
- **Index change on the candidate query.** The order needs `(priority DESC, created_at, team, project, task_name)` leading;
  `model_id IN`, `engine IN`, `memory <=` and `pool IS NULL OR =` are not prefix equalities, so they
  stay Filters. A leading `model_id` loses FR-22 order. `INCLUDE` columns help only an Index Only Scan,
  and the walk selects `output_uri` and `params`; the gain is a constant factor.

## Consequences

- A legitimate sweep above the cap needs a keys-file override (a clear 429 message says so).
- `minion submit` uploads inputs before creating the task, so a rejected create leaves uploaded parts;
  a re-run reuses identical parts. It exits 2 with the code in the message.
- The cap is per team, not per key.
- Write cost: one extra index on `minion_task` (status changes touch it; `(team, status)` is small).

## Addendum (TASK-54)

The candidate walk and its index changed: migration 0016 recreates `ix_minion_task_live_queue` on
`((-priority), created_at, team, project, task_name)` and `candidates_stmt` is a recursive chunked walk
(ADR-0021). The filters stay heap Filters. The bound numbers above (`0.10 ... 0.58 ms`, ~1 us per filtered
task) were measured on the old shape; current numbers (5k unrunnable: ~161 buffers, 3 probes, <1 ms; 10k
live: 96-127 buffers) are in ADR-0021 and ARCHITECTURE section 3.2.
