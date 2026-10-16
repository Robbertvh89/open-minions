"""Runner implementations. Only this package may import inference engines."""

from __future__ import annotations

import importlib
from collections.abc import Collection

from minion_common.enums import Engine, TaskType

from minion_executor.runners.base import ENGINE_UNAVAILABLE, EngineConfig, Runner, RunnerError

# Engines `create_runner` can build. Ollama/HF land with their runners (TASK-4.3 S7/S8); until
# then they probe as unavailable so they are never advertised.
SUPPORTED: frozenset[Engine] = frozenset({Engine.FAKE, Engine.MLX})


def create_runner(engine: Engine, task_type: TaskType, *, test_hooks: bool = False) -> Runner:
    """Instantiate the runner for `engine`/`task_type`. Engine imports stay lazy and run only in
    the worker process, so the control process stays light (invariant 7)."""
    if engine is Engine.FAKE:
        from minion_executor.runners.fake import FakeRunner

        return FakeRunner(test_hooks=test_hooks)
    if engine is Engine.MLX:
        if task_type is TaskType.EMBEDDING:
            from minion_executor.runners.mlx_embedding import MlxEmbeddingRunner

            return MlxEmbeddingRunner()
        from minion_executor.runners.mlx_completion import MlxCompletionRunner

        return MlxCompletionRunner()
    raise RunnerError(ENGINE_UNAVAILABLE, engine.value)  # ollama/hf land in TASK-4.3 S7/S8


_PROBE_MODULES: dict[Engine, tuple[str, ...]] = {
    Engine.MLX: ("mlx.core", "transformers", "mlx_lm"),
    Engine.HF: ("torch", "transformers"),
}


def probe_engines(engines: Collection[Engine], cfg: EngineConfig) -> dict[str, str | None]:
    """Report `{engine: version | None}`; runs in the worker so the control process never
    imports engines. `None` means unavailable."""
    out: dict[str, str | None] = {}
    for engine in engines:
        out[engine.value] = _probe(engine, cfg)
    return out


def _probe(engine: Engine, cfg: EngineConfig) -> str | None:
    if engine not in SUPPORTED:
        return None
    if engine is Engine.FAKE:
        return "fake"
    if engine is Engine.OLLAMA:
        import httpx

        try:
            r = httpx.get(f"{cfg.ollama_url}/api/version", timeout=2.0)
            version = r.json().get("version")
            return str(version) if r.status_code == 200 and version else None
        except (httpx.HTTPError, ValueError, AttributeError):
            return None
    try:
        mods = [importlib.import_module(m) for m in _PROBE_MODULES[engine]]
    except Exception:  # missing extra, wrong platform, broken install
        return None
    return str(getattr(mods[-1], "__version__", "unknown"))


__all__ = [
    "SUPPORTED",
    "EngineConfig",
    "Runner",
    "RunnerError",
    "create_runner",
    "probe_engines",
]
