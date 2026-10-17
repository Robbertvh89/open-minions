# CLAUDE.md

Guidance for Claude Code when working in this repository.

## Project

**open-minions** is an open-source implementation of *Minions: A Dynamic and Distributed Virtual GPU
Cloud Built on Idle Apple Silicon Devices* (Li et al., arXiv:2610.12970, Oct 2026).

Idle Apple Silicon Macs run an **executor** that *pulls* batch inference subtasks (embeddings, LLM
completions) from a central **server**, runs them on the local GPU — within a **capacity budget** the
Mac's owner chooses, so they can keep working — and writes results to S3. There
is no central scheduler: coordination happens through PostgreSQL (behind a REST API) and a
unique-constraint-based acquisition protocol. A **watchdog** reclaims subtasks from executors that
stop sending heartbeats.

- Spec: [`docs/SPEC.md`](docs/SPEC.md) — what the system must do (requirements, API, data model).
- Architecture: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — how it is built (components, flows, decisions).
- Read both before making non-trivial changes. If code and docs disagree, flag it; do not silently
  pick one. Update the docs in the same PR when behavior changes.

## Tech stack

| Area | Choice |
|---|---|
| Language | Python 3.12+ (all components) — [ADR-0001](docs/adr/0001-python-for-all-components.md) |
| Packaging | `uv` workspace, one package per component under `packages/` |
| Server | FastAPI + Pydantic v2, SQLAlchemy 2.x (async) + asyncpg, Alembic migrations |
| Database | PostgreSQL 16 |
| Object storage | S3 API (SeaweedFS for local dev) via `boto3`; executors use presigned URLs only |
| Executor inference | **MLX by default**: `mlx-lm` (batched generation) for completions, a small BERT encoder on `mlx` for embeddings (not the heavy `mlx-embeddings` package). Fallback engines: Ollama HTTP API, HF `transformers` on `mps` — [ADR-0005](docs/adr/0005-mlx-default-inference-engine.md) |
| Executor host integration | `pyobjc` (IOKit / Foundation) for user-idle time, power source, thermal state, memory pressure |
| Observability | Grafana reading PostgreSQL; structured JSON logs (`structlog`) |
| Tests | `pytest`, `pytest-asyncio`, `testcontainers` (Postgres, S3) |
| Lint/format/types | `ruff check`, `ruff format`, `mypy --strict` |

## Repository layout (target)

```
packages/
  minion-common/     # shared Pydantic schemas, enums, API client, config
  minion-server/     # FastAPI app: task API, executor API, splitter, web UI
  minion-executor/   # macOS agent: poll → acquire → run → upload → report
  minion-watchdog/   # heartbeat monitor; requeues orphaned subtasks
migrations/          # Alembic
deploy/              # docker-compose (postgres, s3, grafana, server, watchdog), launchd plist
docs/                # SPEC.md, ARCHITECTURE.md, ADRs in docs/adr/
```

## Commands

```bash
uv sync                                   # install workspace
uv sync --package minion-executor --extra mlx --extra hf  # optional engines: mlx, hf (Apple Silicon; extras are per package)
cp .env.example .env                      # dev settings (database_url is required)
docker compose -f deploy/compose.yml up -d  # postgres + s3 (seaweedfs) + grafana
uv run alembic upgrade head               # migrate
uv run minion-server                      # API on :8080; web UI at /ui when MINION_UI_SESSION_SECRET (>= 32 bytes) is set, else /ui is 404; login with a user API key, session cookie MINION_UI_SESSION_TTL_S (8 h), ADR-0010
uv run minion-watchdog                    # reclaim + cancel sweep; DB only, no S3
uv run minion-server-admin --help          # operator CLI (typer); subcommands below
uv run minion-server-admin generate-api-key --name <who> --teams a,b [--operator] [--expires-days N]  # prints key once + keys-file entry (file is live-reloaded, MINION_API_KEYS_RELOAD_S)
uv run minion-server-admin issue-executor-token --owner <name> [--pool default] [--ttl-days 1..365]  # prints token once
uv run minion-server-admin generate-ui-secret   # prints MINION_UI_SESSION_SECRET=... for the web UI
uv run minion-server-admin revoke-executor-token --executor-id <uuid>
uv run minion-server-admin list-executors   # never prints token hashes
uv run minion-server-admin rotate-executor-token --executor-id <uuid> [--ttl-days N]  # same id, new token printed once
uv run minion-executor run --server http://localhost:8080 [--profile light|balanced|full|custom] [--engines auto|mlx,ollama,hf] [--heartbeat-interval SECONDS]  # heartbeat default 300 s (server timeout >= 2-3x); token: $MINION_EXECUTOR_TOKEN, else Keychain item open-minions-executor; exit 2 if missing or insecure URL or another executor holds the state dir (`executor.lock` flock), exit 1 if the capacity governor task dies (`--engines auto` (default) probes mlx and hf in a worker subprocess and advertises what works, exit 2 if none; Ollama is opt-in (`--engines ollama`) because its daemon runs outside the sandbox and `/api/pull` writes to `~/.ollama` outside the cache cap (ADR-0009); the `fake` engine is never implicit: `MINION_EXECUTOR_DEV=1 uv run minion-executor run --engines fake ...`; SIGTERM/SIGINT: finish in-flight subtask within `MINION_EXECUTOR_STOP_GRACE_S` (30, 0..300) else kill + release, second signal stops at once; idle worker unloaded after `MINION_EXECUTOR_IDLE_UNLOAD_S` (600, 0 disables); worker runs in a Seatbelt sandbox, `MINION_EXECUTOR_SANDBOX=0` only with `MINION_EXECUTOR_DEV=1`, else exit 2; models fetched by a pinned, sha256-verified fetch subprocess into `MINION_EXECUTOR_MODEL_CACHE_DIR` (LRU cap `MINION_EXECUTOR_MODEL_CACHE_GB`, 50; `HF_TOKEN`/proxy/CA go only to that subprocess; worker runs offline); `--profile` omitted = adaptive: starts on `executor.toml` `active_profile` (default light); `executor.toml` at `~/.config/minion/executor.toml` or `$MINION_EXECUTOR_CONFIG`, owner-only perms, unknown keys exit 2)
uv run minion-executor pause [--for 30m|1h|2h30m]   # owner pause: writes control.json in $MINION_EXECUTOR_STATE_DIR (default ~/Library/Application Support/open-minions, 0700; files 0600); wired into the run loop in TASK-5.5
uv run minion-executor resume              # lift the owner pause
uv run minion-executor profile <light|balanced|full|custom|auto>   # pin the running executor's profile, or auto = back to the run's `--profile` pin, or adaptive if none (FR-52); a bad control.json pauses the executor (`control_invalid`)
uv run minion-executor pause [--for 1h] | resume | profile <light|balanced|full|custom|auto>  # owner control of the running executor (control.json in $MINION_EXECUTOR_STATE_DIR, default ~/Library/Application Support/open-minions)
uv run minion-executor status            # profile, budget, paused_reason, current subtask, signals from status.json; exit 1 if no executor is running (missing/stopped/stale), 2 on an unsafe state dir; governor knobs: MINION_EXECUTOR_SIGNAL_POLL_S (2), MINION_EXECUTOR_PRESENCE_INTERVAL_S (60); dev-only timers (need MINION_EXECUTOR_DEV=1): MINION_EXECUTOR_PAUSE_RELEASE_AFTER_S / RESUME_AFTER_S / ESCALATE_AFTER_S / DOWNGRADE_BATCH_S; signals from a JSON file with MINION_EXECUTOR_FAKE_SIGNALS (dev only)
uv run minion-executor service install|uninstall|status [--label com.open-minions.executor] [--server URL] [--profile P] [--engines E] [--dry-run] [--no-load]  # per-user LaunchAgent (ADR-0011): token must be in the Keychain, refuses MINION_EXECUTOR_DEV=1; ExitTimeOut = ceil(STOP_GRACE_S)+15; `run` exits 2 if MINION_EXECUTOR_LAUNCHD_EXIT_TIMEOUT_S < grace+15
uv run minion-executor bench --models deploy/bench-models.yaml --model <id> [--model <id>...] [--records 256] [--batch-sizes 1,8,32] [--max-tokens 128] [--profile full] [--out FILE.json]  # engine throughput/memory on this Mac (real worker path, no server/token); results in docs/bench/
uv run minion-executor bench --budget-check --models deploy/bench-models.yaml --model <id> [--profiles light,balanced,full] [--duration 60] [--baseline-s 20] [--repeats 1] [--out FILE.json]  # NF-8 on this Mac: worker GPU busy share (per-process `accumulatedGPUTime` from `ioreg -c AGXDeviceUserClient`, ~2 Hz; device utilization informational) + worker footprint vs budget per profile, pass/fail per ADR-0006 Gap 1; exit 1 on a fail, 2 without ioreg. Also `bench --cpu-check [--duration 60]` (NF-4: governor CPU % idle/paused, < 1 %) and `bench --watch-reactions --duration 120` (NF-9 timestamps on real signals); procedure in docs/bench/governor-measurements.md
uv run pytest                             # all tests
uv run pytest -m "not integration"        # fast unit tests only (the `macos` sandbox tests run on darwin, skip elsewhere)
uv run pytest -m "gpu or model"            # real MLX models on this Mac (mlx extra + network once); auto-skipped elsewhere
uv run pytest -m model                    # real tiny hub download (network; skipped by default)
uv run ruff check . && uv run ruff format --check . && uv run mypy packages migrations conftest.py
```

(Commands become valid as the scaffolding lands; keep this section accurate.)

## Invariants — never break these

1. **Executors never talk to PostgreSQL.** All executor state changes go through the server REST API.
2. **At most one owner per subtask attempt.** Ownership is decided solely by the
   `INSERT ... ON CONFLICT DO NOTHING` into `minion_subtask_acquirement`. No application-level locks,
   no "check then insert".
3. **Only the current owner may mutate a subtask.** Every executor write (state, heartbeat, result)
   carries `acquirement_id`; the server rejects stale ones with `409`.
4. **Large payloads never pass through the server.** Inputs/outputs move via S3 presigned URLs.
5. **Subtask execution is idempotent.** Outputs are keyed by `subtask_id` + `attempt`; a reclaimed and
   re-run subtask must not corrupt or duplicate final results.
6. **The executor must yield to the user.** It never exceeds the owner's capacity budget (memory cap,
   GPU duty cycle, CPU priority), pauses under memory pressure / thermal stress / low battery, stops
   cleanly on SIGTERM / user stop, and never blocks the Mac from sleeping. See SPEC §4.6.
7. **Inference runs in a worker subprocess**, never in the executor's control process, so a Metal/OOM
   crash or a budget downgrade can kill and reclaim model memory without losing the control loop.

## Conventions

- Type hints everywhere; `mypy --strict` clean. Pydantic models for every API boundary.
- API schemas live in `minion-common` and are shared by server and executor — no duplicated models.
- DB access only in `minion-server/repositories/`; route handlers stay thin.
- Every schema change ships with an Alembic migration (never edit an applied migration).
- Time: always UTC, `timestamptz` in DB, `datetime` with tzinfo in Python.
- Config via environment variables (`MINION_*`), parsed with `pydantic-settings`.
- Tests: unit tests for logic; integration tests (marked `integration`) for anything touching SQL
  concurrency — the acquisition race **must** have a test that fires concurrent acquires.
- Write a short ADR in `docs/adr/NNNN-title.md` for any decision that deviates from the paper.
- Engine-specific code stays behind the `Runner` protocol (`minion_executor/runners/base.py`); nothing
  outside `runners/` imports `mlx`, `transformers` or the Ollama client.
- Performance claims about engines need a `minion-executor bench` result attached to the PR.

## Workflow

Follow the **`feature-workflow`** skill for every feature or fix:

1. Every feature or bugfix starts with a backlog item: `backlog task create "<title>"`.
2. One branch per feature/fix off `main`, one PR per branch. Conventional Commits. Squash-merge after
   CI is green and review/security verdicts pass.
3. The main session orchestrates; work is delegated to project subagents (`.claude/agents/`):

| Agent | Model | Role | Skill |
|---|---|---|---|
| `planner` | Opus | Backlog task → file-level plan | — |
| `implementer` | Sonnet | Code + unit tests per plan step; applies fixes | `db-migration` when schema changes |
| `test-engineer` | Sonnet | Integration, concurrency, fault-injection tests | — |
| `verifier` | Sonnet | Evidence per acceptance criterion, live run | `verifieer-feature` |
| `reviewer` | Opus | Correctness, invariants, spec conformance | `code-quality` |
| `security-auditor` | Opus | Threat-model audit | `security-audit` |
| `docs-keeper` | Haiku | Sync docs, ADR stubs, backlog notes | — |

Pipeline: plan → implement → test → verify → (review ∥ security) → fix loop → docs → PR → CI → merge.

Project skills (`.claude/skills/`): `feature-workflow`, `verifieer-feature`, `code-quality`,
`security-audit`, `db-migration`.

## Security notes

- Executors run on employee laptops: treat inputs as potentially sensitive. Never log payload contents.
- Executor tokens are per-executor, revocable, stored hashed server-side.
- Presigned URLs are short-lived and scoped to a single object.
- See SPEC §8 for the full threat model.
