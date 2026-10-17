"""MLX runner pieces that need no GPU or `mlx` import (run in CI)."""

from __future__ import annotations

from pathlib import Path

import pytest
from minion_common.enums import Engine, TaskType
from minion_common.schemas import CompletionParams, EmbeddingParams
from minion_executor.runners import SUPPORTED, create_runner
from minion_executor.runners.base import OOM, RunnerError
from minion_executor.runners.mlx_common import MODEL_UNSUPPORTED, is_oom, oom_error, pooling_mode
from minion_executor.runners.mlx_completion import MlxCompletionRunner
from minion_executor.runners.mlx_embedding import MlxEmbeddingRunner


class _Ctl:
    def checkpoint(self, records_done: int | None = None) -> None:
        pass


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (RuntimeError("[METAL] Command buffer execution failed: Insufficient Memory"), True),
        (RuntimeError("kIOGPUCommandBufferCallbackErrorOutOfMemory"), True),
        (RuntimeError("[metal::malloc] Resource limit (499000) exceeded."), True),
        (RuntimeError("Out of memory"), True),
        (MemoryError(), True),
        (RuntimeError("shape mismatch"), False),
        (ValueError("out of memory"), False),  # only RuntimeError carries Metal failures
    ],
)
def test_is_oom(exc: BaseException, expected: bool) -> None:
    assert is_oom(exc) is expected


def test_oom_error_is_retryable_and_payload_free() -> None:
    e = oom_error()
    assert e.code == OOM and e.retryable


def test_pooling_mode() -> None:
    assert pooling_mode(None) == "cls"
    assert pooling_mode({}) == "cls"
    assert pooling_mode({"pooling_mode_cls_token": True}) == "cls"
    assert (
        pooling_mode({"pooling_mode_cls_token": False, "pooling_mode_mean_tokens": True}) == "mean"
    )
    assert pooling_mode({"pooling_mode_lasttoken": True}) == "lasttoken"
    assert pooling_mode({"word_embedding_dimension": 384}) == "mean"
    with pytest.raises(RunnerError) as ei:
        pooling_mode({"pooling_mode_max_tokens": True})
    assert ei.value.code == MODEL_UNSUPPORTED and not ei.value.retryable


def test_mlx_is_supported_and_factory_picks_runner_by_type() -> None:
    assert Engine.MLX in SUPPORTED
    assert isinstance(create_runner(Engine.MLX, TaskType.COMPLETION), MlxCompletionRunner)
    assert isinstance(create_runner(Engine.MLX, TaskType.EMBEDDING), MlxEmbeddingRunner)


def test_run_before_load_is_not_loaded() -> None:
    for r, p in (
        (MlxCompletionRunner(), CompletionParams()),
        (MlxEmbeddingRunner(), EmbeddingParams()),
    ):
        with pytest.raises(RunnerError) as ei:
            r.run([{"id": 1}], p, _Ctl())
        assert ei.value.code == "not_loaded"


def test_load_without_cached_model_is_non_retryable() -> None:
    from minion_common.schemas import Budget, ModelEntry
    from minion_executor.runners.base import EngineConfig

    model = ModelEntry(
        model_id="m", type=TaskType.COMPLETION, engine=Engine.MLX, ref="o/m",
        revision="a" * 40, memory_gb=1,
    )  # fmt: skip
    budget = Budget.model_validate(
        {"profile": "balanced", "memory_gb": 2, "gpu_duty_cycle": 1.0, "max_batch_size": 2}
    )
    with pytest.raises(RunnerError) as ei:
        MlxCompletionRunner().load(model, None, budget, EngineConfig())
    assert ei.value.code == "MODEL_NOT_FOUND" and not ei.value.retryable


@pytest.mark.parametrize(
    "conf",
    [
        {"model_type": "xlm-roberta", "architectures": ["XLMRobertaModel"]},
        {"model_type": "roberta", "architectures": ["RobertaModel"]},
        {"model_type": "llama"},
    ],
)
def test_embedding_load_rejects_non_bert_before_any_mlx_import(
    conf: dict[str, object], tmp_path: Path
) -> None:
    import json

    from minion_common.schemas import Budget, ModelEntry
    from minion_executor.runners.base import EngineConfig

    (tmp_path / "config.json").write_text(json.dumps(conf))
    model = ModelEntry(
        model_id="m", type=TaskType.EMBEDDING, engine=Engine.MLX, ref="o/m",
        revision="a" * 40, memory_gb=1,
    )  # fmt: skip
    budget = Budget.model_validate(
        {"profile": "balanced", "memory_gb": 2, "gpu_duty_cycle": 1.0, "max_batch_size": 2}
    )
    with pytest.raises(RunnerError) as ei:
        MlxEmbeddingRunner().load(model, tmp_path, budget, EngineConfig())
    assert ei.value.code == MODEL_UNSUPPORTED and not ei.value.retryable


def test_context_window_from_config() -> None:
    from minion_executor.runners.mlx_completion import _context_window

    assert _context_window({"max_position_embeddings": 32768}) == 32768
    assert _context_window({"text_config": {"max_position_embeddings": 4096}}) == 4096
    assert _context_window({"max_position_embeddings": "x"}) == 0
    assert _context_window({"max_position_embeddings": True}) == 0
    assert _context_window({}) == 0


def test_is_paced_follows_the_control_and_defaults_to_the_safe_side() -> None:
    from minion_executor.runners.base import is_paced

    class Paced(_Ctl):
        paced = False

    assert is_paced(_Ctl())  # unknown control: drain GPU work
    assert not is_paced(Paced())


def test_sync_gpu_synchronizes_default_and_given_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    import sys
    import types

    from minion_executor.runners.mlx_common import sync_gpu

    calls: list[object] = []
    core = types.ModuleType("mlx.core")
    core.synchronize = lambda s=None: calls.append(s)  # type: ignore[attr-defined]
    pkg = types.ModuleType("mlx")
    pkg.core = core  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "mlx", pkg)
    monkeypatch.setitem(sys.modules, "mlx.core", core)
    sync_gpu("gen-stream")
    assert calls == [None, "gen-stream"]
