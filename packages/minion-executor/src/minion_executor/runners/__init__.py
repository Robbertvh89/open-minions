"""Runner implementations. Only this package may import inference engines."""

from __future__ import annotations

import importlib
from collections.abc import Collection

from minion_common.enums import Engine, TaskType

from minion_executor.runners.base import ENGINE_UNAVAILABLE, EngineConfig, Runner, RunnerError

# Engines `create_runner` can build (a probe still decides which are advertised).
SUPPORTED: frozenset[Engine] = frozenset({Engine.FAKE, Engine.MLX, Engine.OLLAMA, Engine.HF})


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
    if engine is Engine.OLLAMA:
        from minion_executor.runners.ollama import OllamaRunner

        return OllamaRunner()
    if engine is Engine.HF:
        if task_type is TaskType.EMBEDDING:
            from minion_executor.runners.hf import HfEmbeddingRunner

            return HfEmbeddingRunner()
        from minion_executor.runners.hf import HfCompletionRunner

        return HfCompletionRunner()
    raise RunnerError(ENGINE_UNAVAILABLE, engine.value)


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

        from minion_executor.runners.ollama import check_loopback

        try:
            url = check_loopback(cfg.ollama_url)
            r = httpx.get(f"{url}/api/version", timeout=2.0, trust_env=False)
            version = r.json().get("version")
            return str(version) if r.status_code == 200 and version else None
        except (httpx.HTTPError, ValueError, AttributeError, RunnerError):
            return None
    if engine is Engine.HF:
        from minion_executor.runners.hf import confine_caches

        confine_caches()
    try:
        mods = [importlib.import_module(m) for m in _PROBE_MODULES[engine]]
    except Exception:  # missing extra, wrong platform, broken install
        return None
    if engine is Engine.HF and not mods[0].backends.mps.is_available():
        return None  # torch without the Apple GPU cannot run the hf runners
    return str(getattr(mods[-1], "__version__", "unknown"))


__all__ = [
    "SUPPORTED",
    "EngineConfig",
    "Runner",
    "RunnerError",
    "create_runner",
    "probe_engines",
]
