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
| Object storage | S3 API (MinIO for local dev) via `boto3`; executors use presigned URLs only |
| Executor inference | **MLX by default**: `mlx-lm` (batched generation) for completions, `mlx-embeddings` for embeddings. Fallback engines: Ollama HTTP API, HF `transformers` on `mps` — [ADR-0005](docs/adr/0005-mlx-default-inference-engine.md) |
| Executor host integration | `pyobjc` (IOKit / Foundation) for user-idle time, power source, thermal state, memory pressure |
| Observability | Grafana reading PostgreSQL; structured JSON logs (`structlog`) |
| Tests | `pytest`, `pytest-asyncio`, `testcontainers` (Postgres, MinIO) |
| Lint/format/types | `ruff check`, `ruff format`, `mypy --strict` |

## Repository layout (target)

```
packages/
  minion-common/     # shared Pydantic schemas, enums, API client, config
  minion-server/     # FastAPI app: task API, executor API, splitter, web UI
  minion-executor/   # macOS agent: poll → acquire → run → upload → report
  minion-watchdog/   # heartbeat monitor; requeues orphaned subtasks
migrations/          # Alembic
deploy/              # docker-compose (postgres, minio, grafana, server, watchdog), launchd plist
docs/                # SPEC.md, ARCHITECTURE.md, ADRs in docs/adr/
```

## Commands

```bash
uv sync                                   # install workspace
docker compose -f deploy/compose.yml up -d  # postgres + minio + grafana
uv run alembic upgrade head               # migrate
uv run minion-server                      # API on :8080
uv run minion-watchdog
uv run minion-executor run --server http://localhost:8080 --profile balanced   # token from Keychain / $MINION_EXECUTOR_TOKEN
uv run minion-executor bench --model <id>  # engine/throughput benchmark on this Mac
uv run pytest                             # all tests
uv run pytest -m "not integration"        # fast unit tests only
uv run ruff check . && uv run ruff format --check . && uv run mypy packages
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
