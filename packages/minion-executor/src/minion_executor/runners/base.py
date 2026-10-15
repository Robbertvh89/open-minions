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


class RunnerError(Exception):
    """A runner failed on a batch (e.g. OOM). `code` is short and never contains payload.

    `retryable` tells the loop whether another attempt (here or elsewhere) could succeed.
    """

    def __init__(self, code: str, message: str = "", *, retryable: bool = True) -> None:
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code
        self.message = message
        self.retryable = retryable


@dataclass(frozen=True)
class EngineConfig:
    """Engine settings that come from executor settings via the `load` message (never env)."""

    ollama_url: str = "http://127.0.0.1:11434"
    max_prompt_tokens: int = 8192


class MemoryStats(TypedDict):
    active_bytes: int
    peak_bytes: int


class StepControl(Protocol):
    def checkpoint(self, records_done: int | None = None) -> None:
        """Call between generation steps / embedding batches.

        Applies static GPU duty-cycle pacing, emits a rate-limited progress/liveness tick to the
        control process, and is the seam where later milestones poll pause/budget messages.
        """
        ...


class Runner(Protocol):
    def load(
        self, model: ModelEntry, model_path: Path | None, budget: Budget, cfg: EngineConfig
    ) -> None: ...

    def run(self, batch: list[Record], params: TaskParams, ctl: StepControl) -> list[Record]: ...

    def memory_in_use(self) -> MemoryStats: ...

    def unload(self) -> None: ...
