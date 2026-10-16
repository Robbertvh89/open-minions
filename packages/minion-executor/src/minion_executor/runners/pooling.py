"""Pure helpers shared by the MLX and HF embedding runners (no engine imports)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from minion_executor.runners.base import RunnerError

MODEL_UNSUPPORTED = "MODEL_UNSUPPORTED"


def read_json(path: Path) -> dict[str, Any] | None:
    """A JSON object from `path`, or `None` if missing/unreadable/not an object."""
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def pooling_mode(cfg: dict[str, Any] | None) -> str:
    """`cls` | `mean` | `lasttoken` from a sentence-transformers `1_Pooling/config.json`.

    No config means CLS pooling (BERT's pooled-output convention, what bge models use).
    """
    if not cfg:
        return "cls"
    if cfg.get("pooling_mode_mean_sqrt_len_tokens"):  # sum / sqrt(n): not plain mean pooling
        raise RunnerError(
            MODEL_UNSUPPORTED, "mean_sqrt_len pooling is not supported", retryable=False
        )
    if cfg.get("pooling_mode_cls_token"):
        return "cls"
    if cfg.get("pooling_mode_mean_tokens"):
        return "mean"
    if cfg.get("pooling_mode_lasttoken"):
        return "lasttoken"
    if cfg.get("pooling_mode_max_tokens"):
        raise RunnerError(MODEL_UNSUPPORTED, "max pooling is not supported", retryable=False)
    return "mean"
