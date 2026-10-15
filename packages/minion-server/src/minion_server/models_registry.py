"""Model allow-list loaded from `models.yaml` (SPEC §5)."""

from pathlib import Path

import yaml
from minion_common.schemas import ModelEntry


def load_models(path: str | Path) -> dict[str, ModelEntry]:
    """Parse a YAML file of `models: [ {model_id, type, engine, ref, memory_gb}, ... ]`."""
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    entries = [ModelEntry.model_validate(m) for m in raw.get("models", [])]
    registry: dict[str, ModelEntry] = {}
    for e in entries:
        if e.model_id in registry:
            raise ValueError(f"duplicate model_id in models file: {e.model_id}")
        registry[e.model_id] = e
    return registry
