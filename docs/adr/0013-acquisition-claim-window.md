# ADR-0013: Single-statement acquisition claim over a random window

- Status: **Accepted** (TASK-7.4.3)
- Deviates from paper: **partly** (the paper's candidate order is strict FIFO; see Decision 2).
  Mutual exclusion is unchanged.

## Context

The M6.3 scale run (`docs/bench/m6-scale-apple-m3-pro.md`) showed head contention: every idle
executor read the same 16 head candidates, shuffled only the first 4 (`SHUFFLE_HEAD`) and tried them
one by one. That cost about 19 acquirement INSERT attempts per win (24-26 under load at 1M pending),
about 2 candidate reads per acquire, and each attempt was its own transaction (BEGIN / INSERT /
ROLLBACK plus a pool checkout). The server saturated on these round trips while the DB was idle.

## Decision

1. **One statement per round.** `repositories.acquisition.claim` runs a single SQL statement:
   a CTE with the candidate query (TASK-7.4.2's `candidates_stmt`, `LIMIT window`), a CTE that picks
   one candidate at random, and a data-modifying CTE `INSERT INTO minion_subtask_acquirement ...
   SELECT ... WHERE subtask.status = 'PENDING' ON CONFLICT ON CONSTRAINT
   uq_minion_subtask_acquirement_subtask DO NOTHING RETURNING ...`. The statement returns the
   number of candidates read and, if the insert won, the acquirement and the candidate's data.
   - 0 candidates: nothing to run, `204`.
   - candidates but no row: another executor won the pick; the service starts another round
     (bounded by `MAX_ROUNDS = 10`), which re-reads a fresh window.
   - a row: the existing re-check `UPDATE` (PENDING -> RUNNING, attempt bump), heartbeat insert and
     task `PENDING -> RUNNING` follow in the same transaction, exactly as before.
2. **FR-22 relaxation: random pick within a window.** The pick is uniform among the top
   `MINION_ACQUIRE_WINDOW` candidates (default 16, 1..256) *of the highest priority present in that
   window*. There is never a priority inversion: a lower-priority subtask is never claimed while a
   higher-priority eligible one is PENDING and visible in the window. Within a priority the order is
   approximately FIFO (the window is the first N in `created_at, team, project, task_name,
   subtask_id` order). With `MINION_ACQUIRE_WINDOW=1` the claim is strictly FR-22 order (head
   contention returns, by design). Preference (pass 1) and global fallback (pass 2) are unchanged.
3. **Invariant 2 is unchanged.** Ownership is decided only by the `INSERT ... ON CONFLICT DO NOTHING`
   on the acquirement unique constraint. The `status = 'PENDING'` predicate in the insert's SELECT is
   an optimisation against a stale read, not the arbiter; the post-win re-check `UPDATE` remains.
   No application-level lock, no check-then-insert.
4. **Not using `FOR UPDATE SKIP LOCKED`.** It would also remove the head contention, but it moves
   the arbitration from the explicit ownership record to row locks held across the candidate read,
   deviates further from the paper, and its guarantees would still need the unique constraint for
   reclaimed attempts. The window pick gets the contention down without it. Revisit only if
   measurements at the target scale still show contention. (ARCHITECTURE §3.2 used to cite a
   never-written "ADR-0002" for this; this ADR is that record.)
5. **Executor first-acquire jitter.** After `register`, an executor waits `uniform(0,
   MINION_EXECUTOR_FIRST_ACQUIRE_JITTER_S)` (default 15, 0..300; 0 disables; a stop interrupts) before
   its first acquire, so a fleet that restarts together does not stampede the head. Later acquires
   are already spread by the working time.

## Consequences

- Acquire is one candidate read and (normally) one claim statement per acquire instead of ~2 reads
  and ~19 transactions. `SHUFFLE_HEAD` and the service `rng` parameter are removed.
- Under light load executors may take a subtask other than the strict head; ordering across tasks
  of equal priority is only approximate FIFO. Operators who need strict order set the window to 1.
- Data-modifying CTE: the INSERT sees the statement snapshot like the candidate read. A candidate
  taken by a concurrent winner is skipped via the unique constraint; one whose winner completed or
  was reclaimed after the snapshot (acquirement deleted, subtask no longer PENDING at the old
  attempt) can win the INSERT, but is caught by the re-check `UPDATE` (status PENDING and
  `attempt = won attempt - 1`), which rolls the round back.
- A round now makes one attempt (the old loop tried several candidates per transaction). Under heavy
  contention a round can lose, so an acquire may exhaust `MAX_ROUNDS` while work exists and answer
  `204`; the executor re-polls. This trade-off is accepted (each round is one cheap statement) and is
  visible as the `acquire_rounds_exhausted` log event (rounds, window, pass; no identities). Exhausted acquires are logged (`acquire_rounds_exhausted`) and counted as
  `minion_acquire_total{outcome="exhausted"}` (TASK-31), separate from `empty`.
- The claim returns the inserted key next to the picked key and refuses to serve a mismatch.

See also ADR-0019: each claim round now first locks the executor row and checks the per-executor open-acquirement cap; the claim statement itself is unchanged.
