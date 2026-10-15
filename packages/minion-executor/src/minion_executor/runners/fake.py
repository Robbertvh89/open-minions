"""Deterministic fake runner for tests and the e2e path (no GPU, no engine imports)."""

from __future__ import annotations

import hashlib
import time

from minion_common.enums import TaskType
from minion_common.schemas import Budget, ModelEntry

from minion_executor.runners.base import Record, RunnerError

EMBED_DIM = 8


def fake_vector(text: str) -> list[float]:
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return [round(b / 255.0, 6) for b in digest[:EMBED_DIM]]


class FakeRunner:
    def __init__(self) -> None:
        self._model: ModelEntry | None = None

    def load(self, model: ModelEntry, budget: Budget) -> None:
        if model.memory_gb > budget.memory_gb:
            raise RunnerError("oom", "model exceeds budget")
        self._model = model

    def run(self, batch: list[Record]) -> list[Record]:
        if self._model is None:
            raise RunnerError("not_loaded")
        out: list[Record] = []
        for rec in batch:
            if rec.get("fail"):
                raise RunnerError(str(rec["fail"]), "injected failure")
            if rec.get("sleep_s"):  # test hook: simulate a slow subtask (capped)
                time.sleep(min(float(rec["sleep_s"]), 60.0))
            text = str(rec.get("text", ""))
            if self._model.type is TaskType.EMBEDDING:
                out.append({"id": rec.get("id"), "embedding": fake_vector(text)})
            else:
                out.append({"id": rec.get("id"), "completion": text})
        return out

    def unload(self) -> None:
        self._model = None
