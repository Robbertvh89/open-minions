"""Hugging Face `transformers` runners on the Apple GPU (`mps`), the fallback engine (ADR-0005).

One class per task type, both loading safetensors only, from the local cache only, with remote
code off (`local_files_only`, `trust_remote_code=False`, `use_safetensors`). `torch` and
`transformers` are imported lazily inside functions so this module stays importable (and the
pure helpers testable) on machines without the `hf` extra. Embedding pooling follows the
sentence-transformers `1_Pooling/config.json`, shared with the MLX runner (`pooling.py`).

Capacity (invariant 6): `torch.mps.set_per_process_memory_fraction` caps the allocator at the
budget; `ctl.checkpoint` runs on every decode step (a `LogitsProcessor`) and every embedding chunk.
"""

from __future__ import annotations

import contextlib
import gc
import os
from pathlib import Path
from typing import Any

from minion_common.schemas import Budget, CompletionParams, EmbeddingParams, ModelEntry, TaskParams

from minion_executor.runners.base import (
    ENGINE_UNAVAILABLE,
    EngineConfig,
    MemoryStats,
    Record,
    RunnerError,
    StepControl,
    sync_due,
)
from minion_executor.runners.mlx_common import (
    MODEL_UNSUPPORTED,
    budget_bytes,
    is_oom,
    oom_error,
    require_model_path,
)
from minion_executor.runners.pooling import pooling_mode, read_json
from minion_executor.runners.prompts import build_messages, embedding_text

PROMPT_TOO_LONG = "PROMPT_TOO_LONG"
MAX_LENGTH = 512
CHUNK = 16  # texts per forward pass; bounds activation memory and sets the checkpoint cadence


def confine_caches() -> None:
    """Point every cache torch/transformers might write at the worker's tmp dir (the only
    writable place in the sandbox) instead of widening the sandbox. Must run before `import
    torch`. No-op outside a worker tmp dir (dev, tests): existing values are kept."""
    tmp = os.environ.get("TMPDIR")
    if not tmp:
        return
    for name, sub in (
        ("TORCH_HOME", "torch"),
        ("TORCHINDUCTOR_CACHE_DIR", "inductor"),
        ("XDG_CACHE_HOME", "xdg"),
        ("TRITON_CACHE_DIR", "triton"),
        ("HF_MODULES_CACHE", "hf-modules"),
    ):
        os.environ.setdefault(name, str(Path(tmp) / sub))
    os.environ.setdefault("TORCH_COMPILE_DISABLE", "1")
    os.environ.setdefault("TORCHDYNAMO_DISABLE", "1")


def _import_torch() -> Any:
    confine_caches()
    import torch

    if not torch.backends.mps.is_available():
        raise RunnerError(ENGINE_UNAVAILABLE, "hf: mps is not available")
    return torch


def _apply_memory_limit(torch: Any, budget: Budget) -> None:
    """Invariant 6: cap the MPS allocator at the owner's memory budget."""
    recommended = int(torch.mps.recommended_max_memory())
    fraction = budget_bytes(budget) / recommended if recommended else 1.0
    torch.mps.set_per_process_memory_fraction(max(0.05, min(fraction, 1.0)))


class _MemoryTracker:
    """`torch.mps` has no peak counter: sample the driver footprint (what the OS charges us)."""

    def __init__(self) -> None:
        self.peak = 0

    def sample(self) -> MemoryStats:
        try:
            import torch
        except ImportError:
            return {"active_bytes": 0, "peak_bytes": 0}
        active = int(torch.mps.current_allocated_memory())
        self.peak = max(self.peak, int(torch.mps.driver_allocated_memory()), active)
        return {"active_bytes": active, "peak_bytes": self.peak}

    def reset(self) -> None:
        self.peak = 0


def _release(torch: Any) -> None:
    gc.collect()
    torch.mps.empty_cache()


def _load_error(exc: BaseException) -> RunnerError:
    if isinstance(exc, RuntimeError) and is_oom(exc):
        return oom_error()
    return RunnerError(MODEL_UNSUPPORTED, type(exc).__name__, retryable=False)


class HfEmbeddingRunner:
    def __init__(self) -> None:
        self._model: Any = None
        self._tok: Any = None
        self._pooling = "cls"
        self._max_len = MAX_LENGTH
        self._mem = _MemoryTracker()

    def load(
        self, model: ModelEntry, model_path: Path | None, budget: Budget, cfg: EngineConfig
    ) -> None:
        path = require_model_path(model_path)
        torch = _import_torch()
        from transformers import AutoModel, AutoTokenizer

        conf = read_json(path / "config.json")
        if conf is None:
            raise RunnerError(MODEL_UNSUPPORTED, "missing config.json", retryable=False)
        pooling = pooling_mode(read_json(path / "1_Pooling" / "config.json"))
        _apply_memory_limit(torch, budget)
        try:
            tok = AutoTokenizer.from_pretrained(
                str(path), local_files_only=True, trust_remote_code=False
            )
            if pooling == "lasttoken":
                tok.padding_side = "right"  # the last real token sits at mask.sum() - 1
            net = AutoModel.from_pretrained(
                str(path),
                local_files_only=True,
                trust_remote_code=False,
                use_safetensors=True,
                dtype=torch.float16,
            )
            net = net.to("mps").eval()
        except (RuntimeError, ValueError, KeyError, OSError) as exc:
            self.unload()
            raise _load_error(exc) from None
        self._model, self._tok, self._pooling = net, tok, pooling
        self._max_len = min(MAX_LENGTH, int(conf.get("max_position_embeddings", MAX_LENGTH)))
        self._mem.reset()
        self._mem.sample()

    def run(self, batch: list[Record], params: TaskParams, ctl: StepControl) -> list[Record]:
        if self._model is None:
            raise RunnerError("not_loaded")
        if not isinstance(params, EmbeddingParams):
            raise RunnerError("BAD_PARAMS", "embedding params expected", retryable=False)
        texts = [embedding_text(rec) for rec in batch]  # BAD_INPUT before any GPU work
        import torch

        out: list[Record] = []
        try:
            with torch.inference_mode():
                for start in range(0, len(texts), CHUNK):
                    vecs, counts = self._embed(texts[start : start + CHUNK], params.normalize)
                    for rec, vec, n in zip(batch[start : start + CHUNK], vecs, counts, strict=True):
                        out.append({"id": rec.get("id"), "embedding": vec, "tokens": n})
                    self._mem.sample()
                    if sync_due(ctl):
                        torch.mps.synchronize()
                    ctl.checkpoint(len(out))
        except RuntimeError as exc:
            if is_oom(exc):
                _release(torch)
                raise oom_error() from None
            raise
        return out

    def _embed(self, texts: list[str], normalize: bool) -> tuple[list[list[float]], list[int]]:
        import torch

        enc = self._tok(
            texts, padding=True, truncation=True, max_length=self._max_len, return_tensors="pt"
        ).to("mps")
        mask = enc["attention_mask"]
        hidden = self._model(
            input_ids=enc["input_ids"], attention_mask=mask
        ).last_hidden_state.float()
        if self._pooling == "cls":
            pooled = hidden[:, 0]
        elif self._pooling == "lasttoken":
            last = (mask.sum(dim=1) - 1).view(-1, 1, 1).expand(-1, 1, hidden.shape[-1])
            pooled = hidden.gather(1, last)[:, 0]
        else:
            m = mask.unsqueeze(-1).float()
            pooled = (hidden * m).sum(dim=1) / m.sum(dim=1).clamp(min=1.0)
        if normalize:
            pooled = torch.nn.functional.normalize(pooled, p=2.0, dim=1, eps=1e-12)
        counts = [int(n) for n in mask.sum(dim=1).tolist()]
        vecs: list[list[float]] = pooled.cpu().tolist()
        return vecs, counts

    def set_budget(self, budget: Budget) -> None:
        torch = _import_torch()
        _apply_memory_limit(torch, budget)

    def trim(self) -> None:
        with contextlib.suppress(ImportError):
            import torch

            if torch.backends.mps.is_available():
                _release(torch)

    def memory_in_use(self) -> MemoryStats:
        return self._mem.sample()

    def unload(self) -> None:
        self._model = None
        self._tok = None
        with contextlib.suppress(ImportError):
            import torch

            if torch.backends.mps.is_available():
                _release(torch)
        self._mem.reset()


class HfCompletionRunner:
    def __init__(self) -> None:
        self._model: Any = None
        self._tok: Any = None
        self._budget: Budget | None = None
        self._cfg = EngineConfig()
        self._context = 0
        self._eos: list[int] = []
        self._mem = _MemoryTracker()

    def load(
        self, model: ModelEntry, model_path: Path | None, budget: Budget, cfg: EngineConfig
    ) -> None:
        path = require_model_path(model_path)
        torch = _import_torch()
        from transformers import AutoModelForCausalLM, AutoTokenizer

        _apply_memory_limit(torch, budget)
        try:
            tok = AutoTokenizer.from_pretrained(
                str(path), local_files_only=True, trust_remote_code=False
            )
            tok.padding_side = "left"  # generate() appends on the right
            if tok.pad_token_id is None:
                tok.pad_token = tok.eos_token
            net = AutoModelForCausalLM.from_pretrained(
                str(path),
                local_files_only=True,
                trust_remote_code=False,
                use_safetensors=True,
                dtype=torch.float16,
            )
            net = net.to("mps").eval()
        except (RuntimeError, ValueError, KeyError, OSError) as exc:
            self.unload()
            raise _load_error(exc) from None
        if not getattr(tok, "chat_template", None):
            del net
            self.unload()
            raise RunnerError(MODEL_UNSUPPORTED, "model has no chat template", retryable=False)
        eos: list[int] = []
        gen_eos = getattr(net.generation_config, "eos_token_id", None)
        for t in (*(gen_eos if isinstance(gen_eos, list) else [gen_eos]), tok.eos_token_id):
            if isinstance(t, int) and t not in eos:
                eos.append(t)
        self._model, self._tok, self._budget, self._cfg, self._eos = net, tok, budget, cfg, eos
        self._context = int(getattr(net.config, "max_position_embeddings", 0) or 0)
        self._mem.reset()
        self._mem.sample()

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
        import torch

        try:
            with torch.inference_mode():
                return self._run(batch, params, ctl, torch)
        except RuntimeError as exc:
            if is_oom(exc):
                _release(torch)
                raise oom_error() from None
            raise

    def _run(
        self, batch: list[Record], params: CompletionParams, ctl: StepControl, torch: Any
    ) -> list[Record]:
        assert self._budget is not None  # noqa: S101 - narrowed by run()
        window = self._cfg.max_prompt_tokens
        if self._context:
            window = min(window, self._context)
        limit = window - params.max_tokens
        out: list[Record | None] = [None] * len(batch)
        todo: list[tuple[int, list[int]]] = []
        for i, rec in enumerate(batch):
            ids = self._encode(rec, params)  # BAD_INPUT fails the whole batch (non-retryable)
            if len(ids) > limit or not ids:
                out[i] = {"id": rec.get("id"), "error": PROMPT_TOO_LONG, "prompt_tokens": len(ids)}
            else:
                todo.append((i, ids))
        done = len(batch) - len(todo)
        if not todo:
            ctl.checkpoint(done)
            return [o for o in out if o is not None]

        if params.seed is not None:
            torch.manual_seed(params.seed)
        gen_cfg = self._generation_config(params)
        size = max(1, self._budget.max_batch_size)
        for start in range(0, len(todo), size):
            chunk = todo[start : start + size]
            for (slot, ids), text, n_tok in self._generate(chunk, gen_cfg, done, ctl, torch):
                out[slot] = {
                    "id": batch[slot].get("id"),
                    "completion": text[0],
                    "finish_reason": text[1],
                    "prompt_tokens": len(ids),
                    "completion_tokens": n_tok,
                }
            done += len(chunk)
            ctl.checkpoint(done)
        return [o for o in out if o is not None]

    def _generation_config(self, params: CompletionParams) -> Any:
        from transformers import GenerationConfig

        sample = params.temperature > 0
        kw: dict[str, Any] = {
            "temperature": params.temperature,
            "top_p": params.top_p,
        }
        # Explicit values override whatever the model's generation_config.json prescribes
        # (e.g. Qwen's repetition_penalty/top_k): the same params give the same sampler everywhere.
        return GenerationConfig(
            max_new_tokens=params.max_tokens,
            do_sample=sample,
            # Every sampler knob is explicit, so the model's own generation_config.json (Qwen
            # ships top_k=20, repetition_penalty, ...) can never change what the params mean.
            top_k=0,
            min_p=0.0,
            typical_p=1.0,
            repetition_penalty=1.0,
            no_repeat_ngram_size=0,
            encoder_no_repeat_ngram_size=0,
            length_penalty=1.0,
            num_beams=1,
            num_return_sequences=1,
            epsilon_cutoff=0.0,
            eta_cutoff=0.0,
            diversity_penalty=0.0,
            eos_token_id=self._eos,
            pad_token_id=self._tok.pad_token_id,
            **(kw if sample else {}),
        )

    def _generate(
        self,
        chunk: list[tuple[int, list[int]]],
        gen_cfg: Any,
        done: int,
        ctl: StepControl,
        torch: Any,
    ) -> list[tuple[tuple[int, list[int]], tuple[str, str], int]]:
        from transformers import LogitsProcessor, LogitsProcessorList

        mem = self._mem

        class Checkpoint(LogitsProcessor):  # type: ignore[misc]
            def __call__(self, input_ids: Any, scores: Any) -> Any:
                mem.sample()
                if sync_due(ctl):  # MPS is async: drain it so the pacer sees the GPU time
                    torch.mps.synchronize()
                ctl.checkpoint(done)
                return scores

        width = max(len(ids) for _, ids in chunk)
        pad = int(self._tok.pad_token_id)
        input_ids = torch.tensor([[pad] * (width - len(ids)) + ids for _, ids in chunk])
        mask = torch.tensor([[0] * (width - len(ids)) + [1] * len(ids) for _, ids in chunk])
        gen = self._model.generate(
            input_ids=input_ids.to("mps"),
            attention_mask=mask.to("mps"),
            generation_config=gen_cfg,
            logits_processor=LogitsProcessorList([Checkpoint()]),
        )
        new = gen[:, width:].cpu().tolist()
        res: list[tuple[tuple[int, list[int]], tuple[str, str], int]] = []
        for item, toks in zip(chunk, new, strict=True):
            reason = "length"
            for k, t in enumerate(toks):
                if t in self._eos:  # the stop token is not part of the text
                    toks, reason = toks[:k], "stop"
                    break
            res.append((item, (self._tok.decode(toks), reason), len(toks)))
        return res

    def set_budget(self, budget: Budget) -> None:
        torch = _import_torch()
        _apply_memory_limit(torch, budget)
        self._budget = budget

    def trim(self) -> None:
        with contextlib.suppress(ImportError):
            import torch

            if torch.backends.mps.is_available():
                _release(torch)

    def memory_in_use(self) -> MemoryStats:
        return self._mem.sample()

    def unload(self) -> None:
        self._model = None
        self._tok = None
        self._budget = None
        with contextlib.suppress(ImportError):
            import torch

            if torch.backends.mps.is_available():
                _release(torch)
        self._mem.reset()
