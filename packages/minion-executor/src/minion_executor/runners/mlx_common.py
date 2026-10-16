"""MLX helpers shared by the completion and embedding runners. `mlx` is imported lazily inside
functions, so the pure helpers (`is_oom`, `pooling_mode`) stay importable and unit-testable on
machines without the extra."""

from __future__ import annotations

import gc
from pathlib import Path

from minion_common.schemas import Budget

from minion_executor.runners.base import OOM, MemoryStats, RunnerError
from minion_executor.runners.pooling import MODEL_UNSUPPORTED as MODEL_UNSUPPORTED
from minion_executor.runners.pooling import pooling_mode as pooling_mode

_GIB = 1024**3
_CACHE_LIMIT_BYTES = 256 * 1024**2  # freed Metal buffers kept for reuse; small on purpose
_OOM_MARKERS = (
    "out of memory",
    "outofmemory",
    "insufficient memory",
    "resource limit",
    "[metal::malloc]",
)


def is_oom(exc: BaseException) -> bool:
    """True if `exc` is a Metal/MLX allocation failure (they surface as plain `RuntimeError`)."""
    if isinstance(exc, MemoryError):
        return True
    text = str(exc).lower()
    return isinstance(exc, RuntimeError) and any(m in text for m in _OOM_MARKERS)


def oom_error() -> RunnerError:
    return RunnerError(OOM, "accelerator out of memory", retryable=True)


def budget_bytes(budget: Budget) -> int:
    return int(budget.memory_gb * _GIB)


def apply_memory_limits(budget: Budget) -> None:
    """Invariant 6: MLX allocations stay within the owner's memory cap."""
    import mlx.core as mx

    mx.set_memory_limit(budget_bytes(budget))
    mx.set_cache_limit(min(_CACHE_LIMIT_BYTES, budget_bytes(budget) // 8))
    mx.reset_peak_memory()


def trim_cache() -> None:
    """Free MLX's cached (unreferenced) Metal buffers; the model stays loaded."""
    import mlx.core as mx

    gc.collect()
    mx.clear_cache()


def clamp_wired_limit(budget: Budget) -> int:
    """Set the wired limit to `min(budget, recommended working set)`; returns the new value.

    `mlx_lm.BatchGenerator` wires up to the recommended maximum on construction, which would pin
    far more than the budget, so callers reset it right after creating one (D14).
    """
    import mlx.core as mx

    recommended = int(mx.device_info().get("max_recommended_working_set_size", 0))
    limit = budget_bytes(budget)
    if recommended:
        limit = min(limit, recommended)
    mx.set_wired_limit(limit)
    return limit


def current_wired_limit() -> int:
    """Read the live wired limit (MLX has no getter: set returns the previous value)."""
    import mlx.core as mx

    old = int(mx.set_wired_limit(0))
    mx.set_wired_limit(old)
    return old


def memory_stats() -> MemoryStats:
    import mlx.core as mx

    return {"active_bytes": int(mx.get_active_memory()), "peak_bytes": int(mx.get_peak_memory())}


def release_memory() -> None:
    """Drop Python references first (caller), then free MLX's buffer cache."""
    import mlx.core as mx

    gc.collect()
    mx.clear_cache()
    mx.reset_peak_memory()


def require_model_path(model_path: Path | None) -> Path:
    if model_path is None or not model_path.is_dir():
        raise RunnerError("MODEL_NOT_FOUND", "model is not in the local cache", retryable=False)
    return model_path
