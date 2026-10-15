from pathlib import Path

import pytest
from minion_server.models_registry import load_models

REPO = Path(__file__).resolve().parents[3]


def test_default_file_loads() -> None:
    reg = load_models(REPO / "deploy" / "models.yaml")
    assert "fake-embed" in reg
    assert reg["fake-embed"].type == "embedding"


def test_duplicate_rejected(tmp_path: Path) -> None:
    f = tmp_path / "m.yaml"
    entry = "- {model_id: a, type: embedding, engine: mlx, ref: r, memory_gb: 1}\n"
    f.write_text("models:\n" + entry * 2)
    with pytest.raises(ValueError, match="duplicate"):
        load_models(f)
