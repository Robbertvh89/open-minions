# open-minions

Open-source implementation of *Minions: A Dynamic and Distributed Virtual GPU Cloud Built on Idle
Apple Silicon Devices* ([arXiv:2610.12970](https://arxiv.org/abs/2610.12970)).

Idle Apple Silicon Macs run an executor that pulls batch inference subtasks (embeddings, LLM
completions) from a central server, runs them on the local GPU with MLX within a capacity budget the
owner chooses, and writes results to S3.

- [Specification](docs/SPEC.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Decision records](docs/adr/)

## Quick start (development)

```bash
uv sync
cp .env.example .env
docker compose -f deploy/compose.yml up -d
uv run alembic upgrade head
uv run minion-server
uv run pytest
```
