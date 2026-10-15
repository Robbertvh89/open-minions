from pathlib import Path

import pytest
from minion_server.models_registry import load_models

REPO = Path(__file__).resolve().parents[3]


def test_default_file_loads() -> None:
    reg = load_models(REPO / "deploy" / "models.yaml")
    assert "bge-small-en" in reg
    assert "fake-embed" not in reg  # test-only entry lives in tests/models.test.yaml


def test_test_file_loads() -> None:
    assert load_models(Path(__file__).with_name("models.test.yaml"))["fake-embed"].memory_gb == 0.1


def test_duplicate_rejected(tmp_path: Path) -> None:
    f = tmp_path / "m.yaml"
    entry = (
        "- {model_id: a, type: embedding, engine: mlx, ref: o/m, revision: "
        + "a" * 40
        + ", memory_gb: 1}\n"
    )
    f.write_text("models:\n" + entry * 2)
    with pytest.raises(ValueError, match="duplicate"):
        load_models(f)


def test_default_file_pins_revisions() -> None:
    reg = load_models(REPO / "deploy" / "models.yaml")
    assert all(e.revision and len(e.revision) == 40 for e in reg.values())


@pytest.mark.parametrize("rev", ["", "main", "abc", "A" * 40, "a" * 39])
def test_bad_or_missing_revision_rejected(tmp_path: Path, rev: str) -> None:
    f = tmp_path / "m.yaml"
    extra = f", revision: '{rev}'" if rev else ""
    f.write_text(
        f"models:\n- {{model_id: a, type: embedding, engine: mlx, ref: o/m{extra}, memory_gb: 1}}\n"
    )
    with pytest.raises(ValueError, match="revision"):
        load_models(f)
