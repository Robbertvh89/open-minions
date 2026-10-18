# Scale harness report schema (`minion-scale-report/1`)

`minion-scale` (dev-only workspace package `packages/minion-scale`, TASK-7.2) writes JSON reports
with `--out`. Every command (`run`, `explain`, `verify`) writes a *partial* document with the same
layout; `minion-scale report a.json b.json ... --out report.json` merges them (a later file wins
for a section it contains) and prints a summary. Nothing in a report is secret: no executor token,
no API key, no presigned URL, no object or record content. The server is reduced to `host:port`.

## Workflow

```bash
export MINION_PG_PORT=55432 MINION_S3_PORT=59000 MINION_SERVER_HOST_PORT=58080 MINION_GRAFANA_PORT=53000   # never 8080
docker compose -p open-minions-scale -f deploy/compose.yml -f deploy/compose.scale.yml \
  --profile app up -d --build --wait postgres s3 s3-init migrate server watchdog
export MINION_DATABASE_URL=postgresql+asyncpg://minion:minion@localhost:55432/minion
export MINION_S3_ENDPOINT_URL=http://localhost:59000 MINION_S3_ACCESS_KEY=minion MINION_S3_SECRET_KEY=minion-secret
uv run minion-scale seed --tasks 100 --subtasks 1000 --executors 200 --tokens-file tokens.json
uv run minion-scale explain --heartbeat-timeout-s 20 --out explain.json
uv run minion-scale run --server http://localhost:58080 --tokens-file tokens.json --procs 4 \
  --work-s 1 --heartbeat-interval-s 5 --vanish 0.01 --release 0.02 --fail 0.02 --late 0.005 \
  --late-wait-s 40 --docker-project open-minions-scale --out run.json
uv run minion-scale verify --out verify.json          # exit 1 on an NF-2 / NF-3 violation
uv run minion-scale report run.json explain.json verify.json --out report.json
docker compose -p open-minions-scale -f deploy/compose.yml -f deploy/compose.scale.yml down -v
```

`seed` writes the tokens file with mode 0600 and refuses to overwrite it; `run` refuses a file
that is group/world readable. `seed` issues the tokens into the executor pool `scale` and creates the
tasks with `allowed_pool = 'scale'`; `minion-scale cleanup --tokens-file tokens.json` revokes them and
deletes the file.

Settings are read from the `MINION_*` environment only (no `.env` file); `MINION_S3_ENDPOINT_URL`
has no default. `seed`, `run`, `explain` and `cleanup` refuse a database host that is not loopback,
or a database holding tasks not created by `minion-scale`, unless `--yes-this-is-a-scale-db`.

Exit codes: `run` exits 1 when `run.drained` is false (the time limit hit, every sim ended or
aborted, or a sim process died) or any 5xx was seen, and 2 on bad input (insecure server URL,
unreadable tokens file); `verify` exits 1 on a violation.

## Top level

| key | type | present in |
|---|---|---|
| `schema` | string, always `"minion-scale-report/1"` | all |
| `generated_at` | ISO-8601 UTC timestamp | all |
| `config` | object | `run` |
| `run` | object | `run` |
| `db` | object | `run` |
| `docker` | list or absent | `run` with `--docker-project` and a running compose project |
| `explain` | list | `explain` |
| `verify` | object | `verify` |

## `config`

`server` (`host:port`), `sims`, `procs`, `work_s`, `heartbeat_interval_s`, `backoff_s` (`[lo, hi]`,
default `[10, 30]`, the executor's), `acquire_pace_s` (default 0.4), `complete_retries` (default 5, 1..20), `fault_rates`
(`vanish`, `release`, `fail`, `late`: probability per assignment), `late_wait_s`, `poll_s`, `first_acquire_jitter_s`, `seed`, `team`.

## `run`

| key | meaning |
|---|---|
| `duration_s`, `sims`, `procs` | wall clock until drained (or `--max-duration-s`) |
| `drained` | bool: the queue emptied. False when `--max-duration-s` hit, the remaining count did not change for `--stall-s` seconds (default 60, 0 disables; polled every `--poll-s`, default 5, from the sum of the seeded tasks' counters; event `stalled`; keep it above `--work-s` and `--late-wait-s`), every sim ended (`sim_aborted`), or a sim process crashed (`proc_crashed`); `run` then exits 1 |
| `completed`, `throughput_per_s` | subtasks the sims completed (HTTP 200) and per second |
| `assigned` | acquires that returned work |
| `conflicts_409`, `rate_limited_429`, `server_errors_5xx`, `transport_errors` | status totals over all API calls (`transport_errors`: connect/timeout, no response) |
| `faults` | injected faults by kind: `vanish`, `release`, `fail`, `late` |
| `events` | named counters: `assigned`, `completed`, `released`, `failed_retryable`, `late_complete_409` (the 409 the server must answer), `late_complete_accepted` (reclaim had not happened yet), `complete_409`, `discarded_stale`, `complete_failed` (gave up: 4xx, or `--complete-retries` transport/5xx attempts; the sim then reports a fail like the executor: 4xx and 5xx give-ups, not a transport-only give-up, which is left to the watchdog), `complete_retried` (one count per retry of a complete after a transport error or 5xx), `cancelled` (the heartbeat said cancel; released as `CANCELLED`), `failure_pause` (long pause after the consecutive-failure limit), `sim_aborted`, `stalled`, `proc_crashed` (+ `proc_crashed_<ExceptionType>`: type only), `fault_*` |
| `calls` | per API operation, see below |
| `event_loop_lag` | histogram of how late a 50 ms sleep woke up, summed over the sim processes (client-side saturation) |

`calls` keys: `register`, `acquire_assigned`, `acquire_empty`, `acquire_error` (failed acquires),
`heartbeat`, `complete`, `fail`, `release`, `output_put` (the presigned S3 PUT). Each value:

```json
{"count": 0, "mean_s": 0.0, "p50_s": 0.0, "p90_s": 0.0, "p99_s": 0.0, "max_s": 0.0, "total_s": 0.0,
 "bucket_le_s": [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, null],
 "bucket_counts": [0],
 "statuses": {"200": 0, "409": 0}}
```

Quantiles are the upper bound of the bucket holding them (the observed max in the overflow
bucket). `bucket_counts` has one more entry than finite bounds (`null` = overflow). `statuses`
maps the HTTP status to a count, `"0"` meaning a transport error.

The server side has a matching `minion_acquire_seconds{outcome=assigned|empty|error}` Prometheus
histogram (`/metrics`, `MINION_METRICS_ENABLED=true`; outcome is the only label).

## `db`

* `pg_stat_statements`: `null` when the extension is not preloaded, else
  `{"top_by_total_time": [...]}`, the statements that ran during the run (delta of two snapshots),
  each `{query, calls, total_exec_ms, mean_exec_ms, rows, shared_blks_hit, shared_blks_read}`.
  `query` is the normalized statement (parameters are `$n`), truncated to 160 characters.
  Only the connecting role's SELECT/INSERT/UPDATE/DELETE/WITH statements are read (not other roles'
  or utility statements).
  `deploy/compose.scale.yml` preloads it; `run` issues `CREATE EXTENSION IF NOT EXISTS` itself.
* `tables_before`, `tables_after`: per `minion_*` table from `pg_stat_user_tables`:
  `table`, `live_tuples`, `dead_tuples`, `seq_scans`, `idx_scans`, `inserts`, `updates`,
  `deletes`, `hot_updates`, `autovacuum_count`, `autoanalyze_count`, `last_autovacuum`
  (cumulative counters: subtract the two for the run).
* `connections_before`, `connections_after`: `{by_state: {active: n, idle: n, ...}, total, max}`
  from `pg_stat_activity` for this database and `max_connections`.

## `docker`

One entry per container of the compose project given with `--docker-project`
(`docker stats --no-stream`): `Name`, `CPUPerc`, `MemUsage`, `MemPerc`, `NetIO`, `BlockIO`,
`PIDs` (strings as Docker prints them). Containers of other projects are never read.

## `explain`

One entry per hot query: `candidate`, `candidate_preference`, `stale_heartbeat`, `over_lease`,
built from the repository's own statements and run as `EXPLAIN (ANALYZE, BUFFERS)` in a read-only
transaction after `ANALYZE`:

`name`, `plan_kind` (always `custom`: literals are inlined so the plan is for those values; the server's asyncpg prepared statements may switch to a generic plan after five executions), `sql` (inline literals), `planning_ms`, `execution_ms`, `top_buffers` (first
`Buffers: shared ...` line), `seq_scan` (any `Seq Scan` node), `plan` (the plan text lines).

## `verify`

`{"ok": bool, "counts": {...}, "checks": [{"name", "ok", "detail"}]}`. `counts`: `tasks`,
`subtasks`, `succeeded`, `failed`, `cancelled`, `attempts`, `acquirements`. Checks (details carry
counts and up to five `team/project/task#subtask_id` samples only):

| check | invariant |
|---|---|
| `nf2_one_succeeded_attempt_per_succeeded_subtask` | every SUCCEEDED subtask has exactly one SUCCEEDED attempt; no other subtask has one |
| `nf2_final_objects_match_succeeded` | promoted output objects (outside `_attempts/`) equal the SUCCEEDED subtasks one to one (`final_objects`, `succeeded`, `missing`, `unexpected`); `{"skipped": true}` without S3 settings |
| `nf3_all_subtasks_terminal` | no PENDING/RUNNING subtask after the drain |
| `nf3_no_orphan_acquirements` | no acquirement whose subtask is not RUNNING on that attempt, and none without a heartbeat row |
| `task_counters_match_subtasks` | `total_subtasks`, `succeeded_subtasks`, `failed_subtasks` equal the actual counts |
| `tasks_terminal` | every task is SUCCEEDED, FAILED, PARTIAL or CANCELLED |

## Notes

* Seeding bypasses the splitter: all subtasks of all tasks slice one tiny input object, so the
  harness measures the acquire / heartbeat / complete / reclaim path, not splitting or S3 reads.
* Sims never GET the input slice and never run inference; outputs are a few bytes per record.
* "Conflict rounds" inside one acquire (candidates lost to another executor) are not visible to
  clients; use `minion_acquire_seconds` and the `pg_stat_statements` call ratio of the candidate
  and acquirement `INSERT` statements.
