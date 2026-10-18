# ADR-0001: Python for all components

- Status: **Accepted**
- Date: 2026-10-15
- Deciders: project owner

## Context

open-minions has four components: server (REST API + splitter), executor (macOS agent), watchdog, and a
shared library. The paper does not prescribe an implementation language. We considered whether a
compiled language (Go, Rust, Swift) would make the system meaningfully faster.

Where time is spent:

| Component | Dominant cost | Language impact |
|---|---|---|
| Executor | GPU kernels (Metal via MLX / PyTorch / Ollama). Python orchestration is milliseconds per subtask vs. minutes of inference. | Negligible on throughput. |
| Server | Postgres round-trips and network I/O. | Small: Go/Rust give lower memory and tighter p99 under heavy load; Postgres saturates first. |
| Watchdog | One query per minute. | None. |

Ecosystem: the inference stacks we depend on are Python-first — `mlx-lm`, `mlx-embeddings`,
HuggingFace `transformers`/`sentence-transformers`. Swift (`mlx-swift`) is the only native alternative
and has a smaller model/tooling ecosystem. A non-Python executor would still have to embed or shell out
to Python for inference.

## Decision

Use **Python 3.12+** for all components, managed as a `uv` workspace:
- Server: FastAPI + Pydantic v2 + SQLAlchemy 2 async/asyncpg.
- Executor: async control process + inference worker subprocess.
- Shared Pydantic schemas in `minion-common` act as the API contract.

## Consequences

**Positive**
- One language, one toolchain, shared models between server and executor — no schema drift.
- Direct access to MLX and HF ecosystems; fastest path to working inference.
- Low contributor barrier for ML-oriented teams.

**Negative / risks**
- Executor distribution needs a Python runtime on volunteer Macs. Mitigation: `uv tool install`
  (self-contained venv, managed interpreter); later a signed `.pkg` if needed.
- Server memory footprint and p99 latency are worse than Go/Rust. Mitigation: async I/O, connection
  pooling, horizontal replicas; NF-1 is validated in the M6 scale test.

**Revisit when**
- The M6 scale test misses NF-1 (acquire p99 < 200 ms @ 500 executors) *and* profiling shows Python
  CPU time, not Postgres, as the bottleneck → rewrite the server only (the REST contract is stable).
  *Status (M6.4):* NF-1 is met for the tested realistic scope and missed under the 2 s-work stress mix,
  where server CPU is near-saturated; whether Python is the cause is decided by the profile in TASK-36
  ([ADR-0016](0016-nf1-workload-scope.md)).
- Executor installation friction becomes the main adoption blocker → consider a Swift menu-bar shell
  around the Python worker, or a full `mlx-swift` executor.
