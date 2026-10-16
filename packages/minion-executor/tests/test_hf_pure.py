"""HF runner pieces that need neither torch nor a GPU (run in CI)."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from minion_common.enums import Engine, TaskType
from minion_executor.runners import SUPPORTED, create_runner
from minion_executor.runners.base import RunnerError
from minion_executor.runners.hf import HfCompletionRunner, HfEmbeddingRunner, confine_caches
from minion_executor.runners.pooling import MODEL_UNSUPPORTED, pooling_mode, read_json


def test_hf_is_supported_and_builds_per_task_type() -> None:
    assert Engine.HF in SUPPORTED
    assert isinstance(create_runner(Engine.HF, TaskType.EMBEDDING), HfEmbeddingRunner)
    assert isinstance(create_runner(Engine.HF, TaskType.COMPLETION), HfCompletionRunner)


@pytest.mark.parametrize(
    ("cfg", "mode"),
    [
        (None, "cls"),
        ({}, "cls"),
        ({"pooling_mode_cls_token": True, "pooling_mode_mean_tokens": False}, "cls"),
        ({"pooling_mode_cls_token": False, "pooling_mode_mean_tokens": True}, "mean"),
        ({"pooling_mode_lasttoken": True}, "lasttoken"),
        ({"word_embedding_dimension": 384}, "mean"),
    ],
)
def test_pooling_mode_from_sentence_transformers_config(
    cfg: dict[str, object] | None, mode: str
) -> None:
    assert pooling_mode(cfg) == mode


def test_max_pooling_is_unsupported() -> None:
    with pytest.raises(RunnerError) as ei:
        pooling_mode({"pooling_mode_max_tokens": True})
    assert ei.value.code == MODEL_UNSUPPORTED and not ei.value.retryable


def test_read_json_tolerates_missing_bad_and_non_object(tmp_path: Path) -> None:
    good = tmp_path / "g.json"
    good.write_text(json.dumps({"a": 1}))
    bad = tmp_path / "b.json"
    bad.write_text("{nope")
    arr = tmp_path / "a.json"
    arr.write_text("[1]")
    assert read_json(good) == {"a": 1}
    assert read_json(bad) is None and read_json(arr) is None and read_json(tmp_path / "x") is None


def test_confine_caches_points_into_tmpdir_and_keeps_existing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    env = {k: v for k, v in os.environ.items() if not k.startswith(("TORCH", "XDG_", "TRITON"))}
    monkeypatch.setattr(os, "environ", env)  # confine_caches() writes here; nothing leaks out
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    monkeypatch.setenv("XDG_CACHE_HOME", "/keep")
    confine_caches()
    assert os.environ["TORCH_HOME"].startswith(str(tmp_path))
    assert os.environ["TORCHINDUCTOR_CACHE_DIR"].startswith(str(tmp_path))
    assert os.environ["XDG_CACHE_HOME"] == "/keep"
    assert os.environ["TORCH_COMPILE_DISABLE"] == "1"


def test_run_before_load_is_not_loaded() -> None:
    from minion_common.schemas import EmbeddingParams

    class Ctl:
        def checkpoint(self, records_done: int | None = None) -> None: ...

    with pytest.raises(RunnerError) as ei:
        HfEmbeddingRunner().run([], EmbeddingParams(), Ctl())
    assert ei.value.code == "not_loaded"


def test_mean_sqrt_len_pooling_is_unsupported() -> None:
    with pytest.raises(RunnerError) as ei:
        pooling_mode({"pooling_mode_mean_sqrt_len_tokens": True})
    assert ei.value.code == MODEL_UNSUPPORTED and not ei.value.retryable
