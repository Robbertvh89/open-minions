"""MLX completion runner: `mlx-lm` continuous batching (`BatchGenerator`)."""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Any

from minion_common.schemas import Budget, CompletionParams, ModelEntry, TaskParams

from minion_executor.runners import mlx_common as common
from minion_executor.runners.base import (
    EngineConfig,
    MemoryStats,
    Record,
    RunnerError,
    StepControl,
)
from minion_executor.runners.prompts import build_messages

PROMPT_TOO_LONG = "PROMPT_TOO_LONG"
_STALL_STEPS = 1000  # consecutive steps without any response before giving up


class MlxCompletionRunner:
    def __init__(self) -> None:
        self._model: Any = None
        self._tok: Any = None
        self._budget: Budget | None = None
        self._cfg = EngineConfig()
        self.wired_limit_bytes: int | None = None  # last value set by `run` (tests, diagnostics)

    def load(
        self, model: ModelEntry, model_path: Path | None, budget: Budget, cfg: EngineConfig
    ) -> None:
        path = common.require_model_path(model_path)
        import mlx_lm

        common.apply_memory_limits(budget)
        try:
            self._model, self._tok = mlx_lm.load(str(path))
        except RuntimeError as exc:
            self.unload()
            if common.is_oom(exc):
                raise common.oom_error() from None
            raise
        if not getattr(self._tok, "has_chat_template", False):
            self.unload()
            raise RunnerError(
                common.MODEL_UNSUPPORTED, "model has no chat template", retryable=False
            )
        self._budget, self._cfg = budget, cfg

    def _encode(self, rec: Record, params: TaskParams) -> list[int]:
        messages = build_messages(rec, params)
        text = self._tok.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
        # The template already holds the special tokens as text: do not add BOS/EOS again.
        return list(self._tok.encode(text, add_special_tokens=False))

    def run(self, batch: list[Record], params: TaskParams, ctl: StepControl) -> list[Record]:
        if self._model is None or self._budget is None:
            raise RunnerError("not_loaded")
        if not isinstance(params, CompletionParams):
            raise RunnerError("BAD_PARAMS", "completion params expected", retryable=False)
        try:
            return self._run(batch, params, ctl)
        except RuntimeError as exc:
            if common.is_oom(exc):
                common.release_memory()
                raise common.oom_error() from None
            raise

    def _run(self, batch: list[Record], params: CompletionParams, ctl: StepControl) -> list[Record]:
        import mlx.core as mx
        from mlx_lm.generate import BatchGenerator
        from mlx_lm.sample_utils import make_sampler

        assert self._budget is not None  # noqa: S101 - narrowed by run()
        out: list[Record | None] = [None] * len(batch)
        prompts: list[list[int]] = []
        slots: list[int] = []  # batch index per inserted prompt
        limit = self._cfg.max_prompt_tokens - params.max_tokens
        for i, rec in enumerate(batch):
            ids = self._encode(rec, params)  # BAD_INPUT fails the whole batch (non-retryable)
            if len(ids) > limit or not ids:
                out[i] = {"id": rec.get("id"), "error": PROMPT_TOO_LONG, "prompt_tokens": len(ids)}
            else:
                prompts.append(ids)
                slots.append(i)
        done = len(batch) - len(prompts)
        if not prompts:
            ctl.checkpoint(done)
            return [o for o in out if o is not None]

        if params.seed is not None:
            mx.random.seed(params.seed)
        sampler = make_sampler(
            temp=params.temperature, top_p=params.top_p if params.top_p < 1.0 else 0.0
        )
        gen = BatchGenerator(
            self._model,
            max_tokens=params.max_tokens,
            stop_tokens=[[t] for t in self._tok.eos_token_ids],
            sampler=sampler,
            completion_batch_size=self._budget.max_batch_size,
            prefill_batch_size=min(8, len(prompts)),
        )
        try:
            # Construction wired the Metal working set up to the recommended maximum (D14).
            self.wired_limit_bytes = common.clamp_wired_limit(self._budget)
            uids = gen.insert(prompts)
            tokens: dict[int, list[int]] = {u: [] for u in uids}
            reasons: dict[int, str] = {}
            idle = 0
            while len(reasons) < len(uids):
                prompt_resps, gen_resps = gen.next()
                idle = 0 if (prompt_resps or gen_resps) else idle + 1
                if idle > _STALL_STEPS:
                    raise RunnerError("ENGINE_STALLED", "no progress", retryable=True)
                for r in gen_resps:
                    if r.finish_reason != "stop":  # the stop token is not part of the text
                        tokens[r.uid].append(int(r.token))
                    if r.finish_reason is not None:
                        reasons[r.uid] = str(r.finish_reason)
                ctl.checkpoint(done + len(reasons))
        finally:
            gen.close()
        for uid, slot, ids in zip(uids, slots, prompts, strict=True):
            out[slot] = {
                "id": batch[slot].get("id"),
                "completion": self._tok.decode(tokens[uid]),
                "finish_reason": reasons[uid],
                "prompt_tokens": len(ids),
                "completion_tokens": len(tokens[uid]),
            }
        return [o for o in out if o is not None]

    def memory_in_use(self) -> MemoryStats:
        try:
            return common.memory_stats()
        except ImportError:
            return {"active_bytes": 0, "peak_bytes": 0}

    def unload(self) -> None:
        self._model = None
        self._tok = None
        self._budget = None
        with contextlib.suppress(ImportError):
            common.release_memory()
