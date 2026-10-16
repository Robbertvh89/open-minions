"""MLX runners against real tiny models on the Apple GPU (TASK-4.3 S6).

Opt in: `uv run pytest -m "gpu or model"` (needs the `mlx` extra on an Apple Silicon Mac and the
network for the first download; models are cached in `$MINION_TEST_MODEL_CACHE`, default
`~/.cache/open-minions-test-models`). Auto-skipped everywhere else (root conftest).
"""

from __future__ import annotations

import asyncio
import importlib
import json
import math
import os
import threading
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from minion_common.enums import CapacityProfile
from minion_common.schemas import (
    Assignment,
    Budget,
    CompletionParams,
    EmbeddingParams,
    HeartbeatResponse,
    ModelEntry,
)
from minion_executor.loop import ExecutorLoop
from minion_executor.model_cache import FetchEnv, ModelCache
from minion_executor.runners.base import EngineConfig, Record
from minion_executor.runners.mlx_common import current_wired_limit
from minion_executor.runners.mlx_completion import PROMPT_TOO_LONG, MlxCompletionRunner
from minion_executor.runners.mlx_embedding import MlxEmbeddingRunner
from minion_executor.worker.process import WorkerCrashed, WorkerHandle
from minion_executor.worker.sandbox import SandboxPolicy

pytestmark = [pytest.mark.gpu, pytest.mark.model]

GIB = 1024**3
REPO_ROOT = Path(__file__).resolve().parents[3]
CACHE_DIR = Path(
    os.environ.get("MINION_TEST_MODEL_CACHE", Path.home() / ".cache" / "open-minions-test-models")
)
BUDGET = Budget(profile=CapacityProfile.BALANCED, memory_gb=2, gpu_duty_cycle=1.0, max_batch_size=4)
PROMPTS = ["Say hi.", "What is 2+2?", "Name a color.", "Reply with the word yes."]


def _models() -> dict[str, ModelEntry]:
    raw = yaml.safe_load((REPO_ROOT / "deploy" / "models.yaml").read_text())["models"]
    return {m["model_id"]: ModelEntry(**m) for m in raw}


QWEN = _models()["qwen2.5-0.5b-instruct"]
BGE = _models()["bge-small-en"]


class Ctl:
    """Counts `checkpoint` calls and optionally runs a probe at each one."""

    def __init__(self, probe: Any = None) -> None:
        self.calls = 0
        self.last_done: int | None = None
        self.probe = probe

    def checkpoint(self, records_done: int | None = None) -> None:
        self.calls += 1
        self.last_done = records_done
        if self.probe is not None:
            self.probe()


@pytest.fixture(scope="session")
def cache() -> ModelCache:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return ModelCache(CACHE_DIR, 5 * GIB, fetch_env=FetchEnv.capture())


@pytest.fixture(scope="session")
async def qwen_path(cache: ModelCache) -> Path:
    path = await cache.ensure(QWEN)
    assert path is not None
    return path


@pytest.fixture(scope="session")
async def bge_path(cache: ModelCache) -> Path:
    path = await cache.ensure(BGE)
    assert path is not None
    return path


@pytest.fixture
def completion(qwen_path: Path) -> Iterator[MlxCompletionRunner]:
    r = MlxCompletionRunner()
    r.load(QWEN, qwen_path, BUDGET, EngineConfig(max_prompt_tokens=512))
    yield r
    r.unload()


@pytest.fixture
def embedding(bge_path: Path) -> Iterator[MlxEmbeddingRunner]:
    r = MlxEmbeddingRunner()
    r.load(BGE, bge_path, BUDGET, EngineConfig())
    yield r
    r.unload()


def _recs(prompts: list[str]) -> list[Record]:
    return [{"id": f"r{i}", "prompt": p} for i, p in enumerate(prompts)]


# --- completion -------------------------------------------------------------------------------


def test_completion_n_records_in_n_lines_out(completion: MlxCompletionRunner) -> None:
    ctl = Ctl()
    out = completion.run(_recs(PROMPTS), CompletionParams(max_tokens=24), ctl)
    assert [o["id"] for o in out] == [f"r{i}" for i in range(len(PROMPTS))]
    for o in out:
        assert isinstance(o["completion"], str) and o["completion"]
        assert o["finish_reason"] in ("stop", "length")
        assert o["prompt_tokens"] > 0 and 0 < o["completion_tokens"] <= 24
    assert "4" in out[1]["completion"]


def test_completion_more_records_than_batch_slots(completion: MlxCompletionRunner) -> None:
    recs = _recs(PROMPTS * 3)  # 12 records through 4 slots: continuous batching refills them
    out = completion.run(recs, CompletionParams(max_tokens=8), Ctl())
    assert [o["id"] for o in out] == [r["id"] for r in recs]
    assert all("completion" in o for o in out)


def test_completion_greedy_is_deterministic(completion: MlxCompletionRunner) -> None:
    p = CompletionParams(max_tokens=32)
    first = completion.run(_recs(PROMPTS), p, Ctl())
    second = completion.run(_recs(PROMPTS), p, Ctl())
    assert [o["completion"] for o in first] == [o["completion"] for o in second]


def test_completion_same_seed_same_sampled_output(completion: MlxCompletionRunner) -> None:
    p = CompletionParams(max_tokens=32, temperature=0.9, top_p=0.9, seed=7)
    a = completion.run(_recs(PROMPTS[:2]), p, Ctl())
    b = completion.run(_recs(PROMPTS[:2]), p, Ctl())
    assert [o["completion"] for o in a] == [o["completion"] for o in b]


def test_completion_checkpoints_every_step(completion: MlxCompletionRunner) -> None:
    ctl = Ctl()
    out = completion.run(_recs(PROMPTS), CompletionParams(max_tokens=16), ctl)
    steps = max(o["completion_tokens"] for o in out)
    assert ctl.calls >= steps >= 1
    assert ctl.last_done == len(PROMPTS)


def test_completion_prompt_too_long_is_a_per_record_line(completion: MlxCompletionRunner) -> None:
    recs = [{"id": "ok", "prompt": "Say hi."}, {"id": "long", "prompt": "word " * 2000}]
    out = completion.run(recs, CompletionParams(max_tokens=8), Ctl())
    assert out[0]["id"] == "ok" and "completion" in out[0]
    assert out[1]["id"] == "long" and out[1]["error"] == PROMPT_TOO_LONG
    assert out[1]["prompt_tokens"] > 512 - 8


def test_completion_all_too_long_still_checkpoints(completion: MlxCompletionRunner) -> None:
    ctl = Ctl()
    out = completion.run([{"id": 1, "prompt": "word " * 2000}], CompletionParams(), ctl)
    assert out[0]["error"] == PROMPT_TOO_LONG and ctl.calls == 1


def test_completion_system_param_changes_output(completion: MlxCompletionRunner) -> None:
    rec = [{"id": 1, "prompt": "What is your name?"}]
    plain = completion.run(rec, CompletionParams(max_tokens=24), Ctl())
    sys_ = completion.run(rec, CompletionParams(max_tokens=24, system="You are Zork."), Ctl())
    assert plain[0]["completion"] != sys_[0]["completion"]


def test_completion_peak_memory_within_model_estimate(completion: MlxCompletionRunner) -> None:
    completion.run(_recs(PROMPTS), CompletionParams(max_tokens=32), Ctl())
    mem = completion.memory_in_use()
    assert 0 < mem["peak_bytes"] < QWEN.memory_gb * GIB
    assert mem["active_bytes"] <= mem["peak_bytes"]


def test_wired_limit_is_within_budget_during_and_after_run(
    completion: MlxCompletionRunner,
) -> None:
    budget_bytes = int(BUDGET.memory_gb * GIB)
    seen: list[int] = []
    ctl = Ctl(probe=lambda: seen.append(current_wired_limit()))
    completion.run(_recs(PROMPTS[:2]), CompletionParams(max_tokens=8), ctl)
    assert seen and max(seen) <= budget_bytes  # BatchGenerator alone would wire far more
    assert 0 < (completion.wired_limit_bytes or 0) <= budget_bytes
    assert current_wired_limit() <= budget_bytes  # restored to the previous (unwired) value


def test_unload_frees_model_memory(qwen_path: Path) -> None:
    import mlx.core as mx

    r = MlxCompletionRunner()
    r.load(QWEN, qwen_path, BUDGET, EngineConfig())
    r.run(_recs(PROMPTS[:1]), CompletionParams(max_tokens=4), Ctl())
    loaded = r.memory_in_use()["active_bytes"]
    r.unload()
    assert loaded > 100 * 1024**2
    assert mx.get_active_memory() < loaded // 4


def test_completion_metal_oom_maps_to_runner_error(
    completion: MlxCompletionRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    from minion_executor.runners.base import OOM, RunnerError

    def boom(*a: Any, **k: Any) -> None:
        raise RuntimeError("[METAL] Command buffer execution failed: Insufficient Memory")

    gen_mod = importlib.import_module("mlx_lm.generate")  # the package attr is a function
    monkeypatch.setattr(gen_mod.BatchGenerator, "next", boom)
    with pytest.raises(RunnerError) as ei:
        completion.run(_recs(PROMPTS[:1]), CompletionParams(max_tokens=4), Ctl())
    assert ei.value.code == OOM and ei.value.retryable


# --- embeddings -------------------------------------------------------------------------------


def _vecs(out: list[Record]) -> list[list[float]]:
    return [o["embedding"] for o in out]


def _cos(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b, strict=True))
    return dot / (math.hypot(*a) * math.hypot(*b))


TEXTS = ["a cat sat on the mat", "a kitten sits on a rug", "stock markets fell sharply today"]


def test_embedding_n_records_in_n_lines_out(embedding: MlxEmbeddingRunner) -> None:
    recs: list[Record] = [{"id": f"e{i}", "text": t} for i, t in enumerate(TEXTS * 7)]  # 21 > chunk
    ctl = Ctl()
    out = embedding.run(recs, EmbeddingParams(), ctl)
    assert [o["id"] for o in out] == [r["id"] for r in recs]
    assert all(len(o["embedding"]) == 384 and o["tokens"] >= 3 for o in out)
    assert ctl.calls >= 2  # one checkpoint per forward-pass chunk
    assert ctl.last_done == len(recs)


def test_embedding_normalization_and_semantics(embedding: MlxEmbeddingRunner) -> None:
    recs: list[Record] = [{"id": i, "text": t} for i, t in enumerate(TEXTS)]
    norm = _vecs(embedding.run(recs, EmbeddingParams(normalize=True), Ctl()))
    raw = _vecs(embedding.run(recs, EmbeddingParams(normalize=False), Ctl()))
    assert all(abs(math.hypot(*v) - 1.0) < 1e-3 for v in norm)
    assert any(abs(math.hypot(*v) - 1.0) > 1e-2 for v in raw)
    assert _cos(raw[0], raw[1]) == pytest.approx(_cos(norm[0], norm[1]), abs=1e-3)
    assert _cos(norm[0], norm[1]) > _cos(norm[0], norm[2]) + 0.2  # cat~kitten, not markets


def test_embedding_is_independent_of_batch_composition(embedding: MlxEmbeddingRunner) -> None:
    """Padding must not leak into the result: a text alone equals the same text padded."""
    alone = embedding.run([{"id": 0, "text": TEXTS[0]}], EmbeddingParams(), Ctl())
    mixed = embedding.run(
        [{"id": 0, "text": TEXTS[0]}, {"id": 1, "text": "long " * 100}], EmbeddingParams(), Ctl()
    )
    assert _cos(alone[0]["embedding"], mixed[0]["embedding"]) > 0.999
    assert mixed[1]["tokens"] > alone[0]["tokens"]


def test_embedding_truncates_long_text_instead_of_failing(embedding: MlxEmbeddingRunner) -> None:
    out = embedding.run([{"id": 0, "text": "word " * 5000}], EmbeddingParams(), Ctl())
    assert out[0]["tokens"] == 512


def test_embedding_deterministic_and_memory_bounded(embedding: MlxEmbeddingRunner) -> None:
    recs: list[Record] = [{"id": i, "text": t} for i, t in enumerate(TEXTS)]
    a = _vecs(embedding.run(recs, EmbeddingParams(), Ctl()))
    b = _vecs(embedding.run(recs, EmbeddingParams(), Ctl()))
    assert a == b
    assert 0 < embedding.memory_in_use()["peak_bytes"] < BGE.memory_gb * GIB


# --- worker: interrupt, sandboxed e2e ---------------------------------------------------------


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def test_interrupt_mid_generation_kills_worker_and_frees_memory(qwen_path: Path) -> None:
    h = WorkerHandle(
        [QWEN.engine],
        sandbox=SandboxPolicy(cache_root=CACHE_DIR),
        tick_interval_s=0.05,
        step_timeout_s=120,
    )
    try:
        h.ensure_model(QWEN, BUDGET, qwen_path)
        pid = h.pid
        assert pid is not None and _alive(pid)
        progress: list[int] = []
        err: list[BaseException] = []

        def go() -> None:
            try:
                h.run(
                    [{"id": 1, "prompt": "Write a very long story about a dragon."}],
                    CompletionParams(max_tokens=4096),
                    progress.append,
                )
            except BaseException as e:
                err.append(e)

        t = threading.Thread(target=go)
        t.start()
        deadline = time.monotonic() + 60
        while not progress and time.monotonic() < deadline and t.is_alive():
            time.sleep(0.05)
        time.sleep(0.3)  # in flight, mid-generation
        h.interrupt()
        t.join(30)
        assert not t.is_alive()
        assert err and isinstance(err[0], WorkerCrashed)
        deadline = time.monotonic() + 10
        while _alive(pid) and time.monotonic() < deadline:
            time.sleep(0.1)
        assert not _alive(pid)  # the process (and with it all Metal memory) is gone
    finally:
        h.close()


class _Client:
    def __init__(self, items: list[Assignment]) -> None:
        self.items = items
        self.completed: list[Any] = []
        self.failed: list[Any] = []
        self.on_empty: Any = None

    async def register(self, req: Any) -> None: ...

    async def acquire(self, req: Any) -> Assignment | None:
        return self.items.pop(0) if self.items else None

    async def complete(self, aid: uuid.UUID, req: Any) -> None:
        self.completed.append(req)

    async def fail(self, aid: uuid.UUID, req: Any) -> None:
        self.failed.append(req)

    async def release(self, aid: uuid.UUID) -> None: ...

    async def heartbeat(self, aid: uuid.UUID, req: Any) -> HeartbeatResponse:
        return HeartbeatResponse(
            cancel=False,
            output_url="https://s3.test/out",
            expires_at=datetime.now(UTC) + timedelta(minutes=5),
        )


def _assignment(model: ModelEntry, body: bytes, count: int) -> Assignment:
    return Assignment.model_validate(
        {
            "acquirement_id": str(uuid.uuid4()),
            "attempt": 1,
            "team": "t",
            "project": "p",
            "task_name": "n",
            "subtask_id": 0,
            "type": model.type.value,
            "model": model.model_dump(mode="json"),
            "params": {"max_tokens": 16} if model.type.value == "completion" else {},
            "input_url": "https://s3.test/in",
            "byte_start": 0,
            "byte_end": len(body),
            "record_count": count,
            "output_url": "https://s3.test/out",
            "expires_at": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
        }
    )


async def _executor_e2e(
    model: ModelEntry, cache: ModelCache, lines: list[dict[str, Any]], tmp: Path
) -> list[dict[str, Any]]:
    """Full executor path with a real model: stub server + fake S3 (MockTransport) around the real
    ModelCache, the Seatbelt-sandboxed worker subprocess and the MLX runner."""
    body = b"".join(json.dumps(r).encode() + b"\n" for r in lines)
    puts: list[bytes] = []

    def s3(req: httpx.Request) -> httpx.Response:
        if req.method == "GET":
            return httpx.Response(206, content=body)
        puts.append(req.read())
        return httpx.Response(200)

    client = _Client([_assignment(model, body, len(lines))])
    worker = WorkerHandle(
        [model.engine],
        sandbox=SandboxPolicy(cache_root=CACHE_DIR),
        tick_interval_s=0.2,
        step_timeout_s=120,
    )
    loop = ExecutorLoop(
        client,  # type: ignore[arg-type]
        worker,
        BUDGET,
        [model.engine],
        http=httpx.AsyncClient(transport=httpx.MockTransport(s3)),
        tmp_root=tmp,
        model_cache=cache,
    )
    done = await asyncio.wait_for(loop.run(max_subtasks=1), 300)
    assert client.failed == [], client.failed
    assert done == 1 and len(client.completed) == 1
    assert client.completed[0].record_count == len(lines)
    assert len(puts) == 1
    return [json.loads(line) for line in puts[0].splitlines()]


async def test_e2e_completion_through_sandboxed_worker(cache: ModelCache, tmp_path: Path) -> None:
    lines = [{"id": f"c{i}", "prompt": p} for i, p in enumerate(PROMPTS[:3])]
    out = await _executor_e2e(QWEN, cache, lines, tmp_path)
    assert [o["id"] for o in out] == ["c0", "c1", "c2"]
    for o in out:
        assert o["completion"] and o["model"] == QWEN.model_id
        assert o["model_revision"] == QWEN.revision
        assert o["finish_reason"] in ("stop", "length") and o["completion_tokens"] > 0


async def test_e2e_embedding_through_sandboxed_worker(cache: ModelCache, tmp_path: Path) -> None:
    lines = [{"id": f"e{i}", "text": t} for i, t in enumerate(TEXTS)]
    out = await _executor_e2e(BGE, cache, lines, tmp_path)
    assert [o["id"] for o in out] == ["e0", "e1", "e2"]
    for o in out:
        assert len(o["embedding"]) == 384 and o["tokens"] > 0
        assert o["model"] == BGE.model_id and o["model_revision"] == BGE.revision
    assert _cos(out[0]["embedding"], out[1]["embedding"]) > _cos(
        out[0]["embedding"], out[2]["embedding"]
    )
