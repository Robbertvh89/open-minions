"""HF transformers runners on the Apple GPU (`mps`) against real tiny models (TASK-4.3 S8).

Opt in: `uv run pytest -m "gpu or model"` (needs the `mlx` and `hf` extras on an Apple Silicon Mac
and the network for the first download). Skipped when `torch` is not installed. Reuses the helpers
of the MLX GPU tests (stub server + fake S3 around the real worker).
"""

from __future__ import annotations

import importlib.util
import math
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType

import pytest

torch = pytest.importorskip("torch")
if not torch.backends.mps.is_available():
    pytest.skip("torch has no MPS backend", allow_module_level=True)

from minion_common.enums import CapacityProfile  # noqa: E402
from minion_common.schemas import Budget, CompletionParams, EmbeddingParams, ModelEntry  # noqa: E402
from minion_executor.model_cache import ModelCache  # noqa: E402
from minion_executor.runners import probe_engines  # noqa: E402
from minion_executor.runners.base import OOM, EngineConfig, Record, RunnerError  # noqa: E402
from minion_executor.runners.hf import PROMPT_TOO_LONG, HfCompletionRunner, HfEmbeddingRunner  # noqa: E402
from minion_executor.worker.process import WorkerCrashed, WorkerHandle  # noqa: E402
from minion_executor.worker.sandbox import SandboxPolicy  # noqa: E402

pytestmark = [pytest.mark.gpu, pytest.mark.model]


def _load_helpers() -> ModuleType:
    path = Path(__file__).with_name("test_mlx_runners_gpu.py")
    spec = importlib.util.spec_from_file_location("_mlx_gpu_helpers", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


H = _load_helpers()
Ctl, BUDGET, CACHE_DIR, PROMPTS, TEXTS = H.Ctl, H.BUDGET, H.CACHE_DIR, H.PROMPTS, H.TEXTS
GIB = 1024**3
_ALL = H._models()
QWEN_HF: ModelEntry = _ALL["qwen2.5-0.5b-instruct-hf"]
BGE_HF: ModelEntry = _ALL["bge-small-en-hf"]
BGE_MLX: ModelEntry = _ALL["bge-small-en"]


@pytest.fixture(scope="session")
def cache() -> ModelCache:
    from minion_executor.model_cache import FetchEnv

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return ModelCache(CACHE_DIR, 5 * GIB, fetch_env=FetchEnv.capture())


@pytest.fixture(scope="session")
async def qwen_path(cache: ModelCache) -> Path:
    path = await cache.ensure(QWEN_HF)
    assert path is not None
    return path


@pytest.fixture(scope="session")
async def bge_path(cache: ModelCache) -> Path:
    path = await cache.ensure(BGE_HF)
    assert path is not None
    return path


@pytest.fixture(scope="session")
async def bge_mlx_path(cache: ModelCache) -> Path:
    path = await cache.ensure(BGE_MLX)
    assert path is not None
    return path


@pytest.fixture
def completion(qwen_path: Path) -> Iterator[HfCompletionRunner]:
    r = HfCompletionRunner()
    r.load(QWEN_HF, qwen_path, BUDGET, EngineConfig(max_prompt_tokens=512))
    yield r
    r.unload()


@pytest.fixture
def embedding(bge_path: Path) -> Iterator[HfEmbeddingRunner]:
    r = HfEmbeddingRunner()
    r.load(BGE_HF, bge_path, BUDGET, EngineConfig())
    yield r
    r.unload()


def _recs(prompts: list[str]) -> list[Record]:
    return [{"id": f"r{i}", "prompt": p} for i, p in enumerate(prompts)]


def _cos(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b, strict=True)) / (math.hypot(*a) * math.hypot(*b))


def test_probe_reports_torch_version() -> None:
    from minion_common.enums import Engine

    assert probe_engines([Engine.HF], EngineConfig())["hf"] is not None


# --- completion -------------------------------------------------------------------------------


def test_completion_smoke(completion: HfCompletionRunner) -> None:
    out = completion.run(_recs(PROMPTS), CompletionParams(max_tokens=24), Ctl())
    assert [o["id"] for o in out] == [f"r{i}" for i in range(len(PROMPTS))]
    for o in out:
        assert isinstance(o["completion"], str) and o["completion"]
        assert o["finish_reason"] in ("stop", "length")
        assert o["prompt_tokens"] > 0 and 0 < o["completion_tokens"] <= 24
    assert "4" in out[1]["completion"]


def test_completion_more_records_than_batch_slots_and_deterministic(
    completion: HfCompletionRunner,
) -> None:
    recs = _recs(PROMPTS * 3)  # 12 records through batches of 4
    p = CompletionParams(max_tokens=8)
    a = completion.run(recs, p, Ctl())
    assert [o["id"] for o in a] == [r["id"] for r in recs]
    b = completion.run(recs, p, Ctl())
    assert [o["completion"] for o in a] == [o["completion"] for o in b]


def test_completion_same_seed_same_sampled_output(completion: HfCompletionRunner) -> None:
    p = CompletionParams(max_tokens=24, temperature=0.9, top_p=0.9, seed=7)
    a = completion.run(_recs(PROMPTS[:2]), p, Ctl())
    b = completion.run(_recs(PROMPTS[:2]), p, Ctl())
    assert [o["completion"] for o in a] == [o["completion"] for o in b]


def test_completion_checkpoints_every_step(completion: HfCompletionRunner) -> None:
    ctl = Ctl()
    out = completion.run(_recs(PROMPTS), CompletionParams(max_tokens=16), ctl)
    steps = max(o["completion_tokens"] for o in out)
    assert ctl.calls >= steps >= 1
    assert ctl.last_done == len(PROMPTS)


def test_completion_prompt_too_long_is_a_per_record_line(completion: HfCompletionRunner) -> None:
    recs: list[Record] = [
        {"id": "ok", "prompt": "Say hi."},
        {"id": "long", "prompt": "word " * 2000},
    ]
    out = completion.run(recs, CompletionParams(max_tokens=8), Ctl())
    assert out[0]["id"] == "ok" and "completion" in out[0]
    assert out[1]["id"] == "long" and out[1]["error"] == PROMPT_TOO_LONG


def test_completion_oom_maps_to_runner_error(
    completion: HfCompletionRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(*a: object, **k: object) -> None:
        raise RuntimeError("MPS backend out of memory (MPS allocated: 1.00 GB)")

    monkeypatch.setattr(completion._model, "generate", boom)
    with pytest.raises(RunnerError) as ei:
        completion.run(_recs(PROMPTS[:1]), CompletionParams(max_tokens=4), Ctl())
    assert ei.value.code == OOM and ei.value.retryable


def test_unload_frees_model_memory(qwen_path: Path) -> None:
    r = HfCompletionRunner()
    r.load(QWEN_HF, qwen_path, BUDGET, EngineConfig())
    r.run(_recs(PROMPTS[:1]), CompletionParams(max_tokens=4), Ctl())
    loaded = r.memory_in_use()["active_bytes"]
    r.unload()
    assert loaded > 400 * 1024**2
    assert int(torch.mps.current_allocated_memory()) < loaded // 4


# --- embeddings -------------------------------------------------------------------------------


def test_embedding_shape_normalisation_and_semantics(embedding: HfEmbeddingRunner) -> None:
    recs: list[Record] = [{"id": f"e{i}", "text": t} for i, t in enumerate(TEXTS * 7)]  # > chunk
    ctl = Ctl()
    out = embedding.run(recs, EmbeddingParams(), ctl)
    assert [o["id"] for o in out] == [r["id"] for r in recs]
    assert all(len(o["embedding"]) == 384 and o["tokens"] >= 3 for o in out)
    assert all(abs(math.hypot(*o["embedding"]) - 1.0) < 1e-3 for o in out)
    assert ctl.calls >= 2 and ctl.last_done == len(recs)
    v = [o["embedding"] for o in out[:3]]
    assert _cos(v[0], v[1]) > _cos(v[0], v[2]) + 0.2
    raw = embedding.run(recs[:1], EmbeddingParams(normalize=False), Ctl())
    assert abs(math.hypot(*raw[0]["embedding"]) - 1.0) > 1e-2


def test_embedding_independent_of_batch_composition(embedding: HfEmbeddingRunner) -> None:
    alone = embedding.run([{"id": 0, "text": TEXTS[0]}], EmbeddingParams(), Ctl())
    mixed = embedding.run(
        [{"id": 0, "text": TEXTS[0]}, {"id": 1, "text": "long " * 100}], EmbeddingParams(), Ctl()
    )
    assert _cos(alone[0]["embedding"], mixed[0]["embedding"]) > 0.999


def test_embedding_hf_matches_mlx_cosine(embedding: HfEmbeddingRunner, bge_mlx_path: Path) -> None:
    """AC #3: the same model through two engines agrees (cosine >= 0.99 on 32 texts)."""
    from minion_executor.runners.mlx_embedding import MlxEmbeddingRunner

    topics = ["cats", "markets", "rain", "compilers", "football", "soup", "trains", "poetry"]
    texts = [f"A short note number {i} about {topics[i % 8]} and life." for i in range(29)]
    texts += ["x", "word " * 700, "Tokyo is the capital of Japan."]  # shortest, truncated, plain
    recs: list[Record] = [{"id": i, "text": t} for i, t in enumerate(texts)]
    assert len(recs) == 32
    hf = embedding.run(recs, EmbeddingParams(), Ctl())
    mlx = MlxEmbeddingRunner()
    mlx.load(BGE_MLX, bge_mlx_path, BUDGET, EngineConfig())
    try:
        ref = mlx.run(recs, EmbeddingParams(), Ctl())
    finally:
        mlx.unload()
    cos = [_cos(a["embedding"], b["embedding"]) for a, b in zip(hf, ref, strict=True)]
    print(f"hf vs mlx cosine: min={min(cos):.5f} mean={sum(cos) / len(cos):.5f}")
    assert min(cos) >= 0.99


# --- worker: interrupt, sandboxed e2e ---------------------------------------------------------


def test_interrupt_mid_generation_kills_worker_and_frees_memory(qwen_path: Path) -> None:
    h = WorkerHandle(
        [QWEN_HF.engine],
        sandbox=SandboxPolicy(cache_root=CACHE_DIR),
        tick_interval_s=0.05,
        step_timeout_s=120,
    )
    try:
        h.ensure_model(QWEN_HF, BUDGET, qwen_path)
        pid = h.pid
        assert pid is not None and H._alive(pid)
        ticks: list[int] = []
        err: list[BaseException] = []

        def on_progress(done: int) -> None:
            # First checkpoint tick = a decode step is running: interrupt from inside it, so the
            # outcome never depends on how fast the model happens to finish.
            ticks.append(done)
            if len(ticks) == 1:
                h.interrupt()

        def go() -> None:
            try:
                h.run(
                    [
                        {
                            "id": 1,
                            "prompt": "Count from 1 to 5000, one number per line, nothing else.",
                        }
                    ],
                    CompletionParams(max_tokens=4096),
                    on_progress,
                )
            except BaseException as e:
                err.append(e)

        t = threading.Thread(target=go)
        t.start()
        t.join(60)
        assert not t.is_alive()
        assert ticks, "no checkpoint tick arrived"
        assert err and isinstance(err[0], WorkerCrashed)
        deadline = time.monotonic() + 10
        while H._alive(pid) and time.monotonic() < deadline:
            time.sleep(0.1)
        assert not H._alive(pid)  # the process, and with it all MPS memory, is gone
    finally:
        h.close()


async def test_e2e_completion_through_sandboxed_worker(cache: ModelCache, tmp_path: Path) -> None:
    lines = [{"id": f"c{i}", "prompt": p} for i, p in enumerate(PROMPTS[:3])]
    out = await H._executor_e2e(QWEN_HF, cache, lines, tmp_path)
    assert [o["id"] for o in out] == ["c0", "c1", "c2"]
    for o in out:
        assert o["completion"] and o["model"] == QWEN_HF.model_id
        assert o["model_revision"] == QWEN_HF.revision
        assert o["finish_reason"] in ("stop", "length") and o["completion_tokens"] > 0


async def test_e2e_embedding_through_sandboxed_worker(cache: ModelCache, tmp_path: Path) -> None:
    lines = [{"id": f"e{i}", "text": t} for i, t in enumerate(TEXTS)]
    out = await H._executor_e2e(BGE_HF, cache, lines, tmp_path)
    assert [o["id"] for o in out] == ["e0", "e1", "e2"]
    for o in out:
        assert len(o["embedding"]) == 384 and o["tokens"] > 0
        assert o["model"] == BGE_HF.model_id and o["model_revision"] == BGE_HF.revision
    assert _cos(out[0]["embedding"], out[1]["embedding"]) > _cos(
        out[0]["embedding"], out[2]["embedding"]
    )


# --- live budget changes (TASK-5.3) -------------------------------------------------------------


def test_set_budget_reapplies_the_mps_memory_fraction(
    completion: HfCompletionRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[float] = []
    real = torch.mps.set_per_process_memory_fraction

    def spy(fraction: float) -> None:
        seen.append(fraction)
        real(fraction)

    monkeypatch.setattr(torch.mps, "set_per_process_memory_fraction", spy)
    smaller = Budget(
        profile=CapacityProfile.BALANCED, memory_gb=1, gpu_duty_cycle=1.0, max_batch_size=2
    )
    bigger = smaller.model_copy(update={"memory_gb": 3})
    completion.set_budget(smaller)
    completion.set_budget(bigger)
    assert len(seen) == 2 and 0 < seen[0] < seen[1] <= 1.0
    out = completion.run(_recs(PROMPTS[:2]), CompletionParams(max_tokens=8), Ctl())
    assert all(isinstance(o["completion"], str) for o in out)
    completion.trim()  # frees the MPS cache; the model keeps working
    assert completion.run(_recs(PROMPTS[:1]), CompletionParams(max_tokens=8), Ctl())[0][
        "completion"
    ]


def test_embedding_set_budget_and_trim(embedding: HfEmbeddingRunner) -> None:
    embedding.set_budget(
        Budget(profile=CapacityProfile.BALANCED, memory_gb=1, gpu_duty_cycle=1.0, max_batch_size=2)
    )
    embedding.trim()
    out = embedding.run(
        [{"id": f"e{i}", "text": t} for i, t in enumerate(TEXTS[:2])], EmbeddingParams(), Ctl()
    )
    assert len(out) == 2
