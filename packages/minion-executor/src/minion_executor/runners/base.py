"""Engine-agnostic Runner protocol v2 (ADR-0005). Engines live only under `runners/`."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, TypedDict

from minion_common.schemas import Budget, ModelEntry, TaskParams

Record = dict[str, Any]


# Engine cannot run here (extra missing, no runner implemented yet). Executor-local: the loop
# releases the subtask instead of burning an attempt.
ENGINE_UNAVAILABLE = "ENGINE_UNAVAILABLE"
# The accelerator ran out of memory (retryable: another Mac, or a smaller batch, may fit).
OOM = "OOM"
# A model on this Mac does not match its pinned digest (executor-local: release, don't burn).
MODEL_INTEGRITY = "MODEL_INTEGRITY"


class RunnerError(Exception):
    """A runner failed on a batch (e.g. OOM). `code` is short and never contains payload.

    `retryable` tells the loop whether another attempt (here or elsewhere) could succeed.
    `release` marks executor-local trouble: the loop hands the subtask back without burning an
    attempt.
    """

    def __init__(
        self, code: str, message: str = "", *, retryable: bool = True, release: bool = False
    ) -> None:
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.release = release


@dataclass(frozen=True)
class EngineConfig:
    """Engine settings that come from executor settings via the `load` message (never env)."""

    ollama_url: str = "http://127.0.0.1:11434"
    max_prompt_tokens: int = 8192
    # Ollama only: how long the daemon keeps the model after a request (bounded, invariant 6).
    keep_alive_s: int = 60
    # The worker's load timeout: a model pull must finish well inside it (set by the handle).
    load_timeout_s: float = 1800.0


class MemoryStats(TypedDict):
    active_bytes: int
    peak_bytes: int


class StepControl(Protocol):
    def checkpoint(self, records_done: int | None = None) -> None:
        """Call between generation steps / embedding batches.

        Applies GPU duty-cycle pacing, emits a rate-limited progress/liveness tick to the control
        process, and is where live pause / duty-cycle changes take effect (it may block while the
        executor is paused).
        """
        ...


class Runner(Protocol):
    def load(
        self, model: ModelEntry, model_path: Path | None, budget: Budget, cfg: EngineConfig
    ) -> None: ...

    def run(self, batch: list[Record], params: TaskParams, ctl: StepControl) -> list[Record]: ...

    def set_budget(self, budget: Budget) -> None:
        """Re-apply `budget` (memory / wired limits, batch cap) before the next `run`. Called
        between batches, never during one; a lower memory limit takes effect for new allocations."""
        ...

    def trim(self) -> None:
        """Free cached memory that is not needed to keep the model loaded. Safe at a checkpoint."""
        ...

    def memory_in_use(self) -> MemoryStats: ...

    def unload(self) -> None: ...
