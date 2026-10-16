"""Real tiny download (network). Opt in: `uv run pytest -m model`."""

from __future__ import annotations

from pathlib import Path

import pytest
from minion_common.enums import Engine, TaskType
from minion_common.schemas import ModelEntry
from minion_executor.model_cache import FetchEnv, ModelCache

pytestmark = pytest.mark.model

REPO = "mlx-community/bge-small-en-v1.5-bf16"


async def test_real_download_verifies_against_hub_metadata(tmp_path: Path) -> None:
    from huggingface_hub import HfApi

    rev = HfApi().model_info(REPO).sha
    assert rev is not None
    model = ModelEntry(
        model_id="bge-small",
        type=TaskType.EMBEDDING,
        engine=Engine.MLX,
        ref=REPO,
        revision=rev,
        memory_gb=1,
    )
    cache = ModelCache(tmp_path, 5 * 1024**3, fetch_env=FetchEnv.capture())
    path = await cache.ensure(model)
    assert path is not None and cache.verify(path)
    assert any(p.suffix == ".safetensors" for p in path.iterdir())
    assert not list(path.rglob("*.py"))
