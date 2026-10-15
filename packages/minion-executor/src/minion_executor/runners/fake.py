"""Deterministic fake runner for tests and the e2e path (no GPU, no engine imports)."""

from __future__ import annotations

import hashlib
import os
import time

from minion_common.enums import TaskType
from minion_common.schemas import Budget, ModelEntry

from minion_executor.runners.base import Record, RunnerError

EMBED_DIM = 8


def fake_vector(text: str) -> list[float]:
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return [round(b / 255.0, 6) for b in digest[:EMBED_DIM]]


INJECTED_FAILURE = "INJECTED_FAILURE"


class FakeRunner:
    """Deterministic runner. Per-record `fail` / `sleep_s` / `echo_env` hooks are honoured only
    when constructed with `test_hooks=True` (tests); input data can never trigger them otherwise."""

    def __init__(self, *, test_hooks: bool = False) -> None:
        self._model: ModelEntry | None = None
        self._hooks = test_hooks

    def load(self, model: ModelEntry, budget: Budget) -> None:
        if model.memory_gb > budget.memory_gb:
            raise RunnerError("oom", "model exceeds budget")
        self._model = model

    def run(self, batch: list[Record]) -> list[Record]:
        if self._model is None:
            raise RunnerError("not_loaded")
        out: list[Record] = []
        for rec in batch:
            if self._hooks:
                if rec.get("fail"):
                    raise RunnerError(INJECTED_FAILURE, "injected failure")  # fixed code
                if rec.get("sleep_s"):  # simulate a slow subtask (capped)
                    time.sleep(min(float(rec["sleep_s"]), 60.0))
                if rec.get("echo_env"):  # report whether an env var is visible to the worker
                    out.append({"id": rec.get("id"), "present": str(rec["echo_env"]) in os.environ})
                    continue
            text = str(rec.get("text", ""))
            if self._model.type is TaskType.EMBEDDING:
                out.append({"id": rec.get("id"), "embedding": fake_vector(text)})
            else:
                out.append({"id": rec.get("id"), "completion": text})
        return out

    def unload(self) -> None:
        self._model = None
