# open-minions

Open-source implementation of *Minions: A Dynamic and Distributed Virtual GPU Cloud Built on Idle
Apple Silicon Devices* ([arXiv:2610.12970](https://arxiv.org/abs/2610.12970)).

Idle Apple Silicon Macs run an executor that pulls batch inference subtasks (embeddings, LLM
completions) from a central server, runs them on the local GPU with MLX within a capacity budget the
owner chooses, and writes results to S3.

- [Specification](docs/SPEC.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Decision records](docs/adr/)

## Quick start: submit a task

```bash
uv sync
export MINION_API_KEY=...                       # or Keychain item open-minions-api-key
export MINION_CLI_SERVER=https://minion.example.com MINION_CLI_TEAM=a
uv run minion submit data.jsonl --type embedding --model M -o out/
```

`minion submit` uploads the JSONL to S3, submits the task, waits and downloads the results to `out/`.
Exit code 0 on success, 1 if the task FAILED or was CANCELLED, 3 if PARTIAL. Ctrl-C does not cancel the
task; `minion status NAME`, `minion fetch NAME` and `minion cancel NAME` follow it up.

## Quick start (development)

```bash
uv sync
cp .env.example .env
docker compose -f deploy/compose.yml up -d
uv run alembic upgrade head
uv run minion-server
uv run pytest
```
