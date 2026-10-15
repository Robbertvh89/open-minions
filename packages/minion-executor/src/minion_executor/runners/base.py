"""Engine-agnostic Runner protocol (ADR-0005). Engines live only under `runners/`."""

from __future__ import annotations

from typing import Any, Protocol

from minion_common.schemas import Budget, ModelEntry

Record = dict[str, Any]


class RunnerError(Exception):
    """A runner failed on a batch (e.g. OOM). `code` is short and never contains payload."""

    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code
        self.message = message


class Runner(Protocol):
    def load(self, model: ModelEntry, budget: Budget) -> None: ...

    def run(self, batch: list[Record]) -> list[Record]: ...

    def unload(self) -> None: ...
