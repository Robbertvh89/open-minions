"""MLX embedding runner: a small BERT-family encoder written directly on `mlx`.

Why not `mlx-embeddings`: it depends on `mlx-vlm` and `mlx-audio` (opencv, scipy, pandas,
datasets, sounddevice, ...), a heavy tree to ship to every Mac and a bigger attack surface for
the sandboxed worker. A BERT encoder is ~60 lines on `mlx.nn`; tokenisation reuses the
`transformers` tokenizer that `mlx-lm` already requires (local files only, no remote code).
Pooling follows the sentence-transformers `1_Pooling/config.json` (CLS for bge models).
"""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Any, cast

from minion_common.schemas import Budget, EmbeddingParams, ModelEntry, TaskParams

from minion_executor.runners import mlx_common as common
from minion_executor.runners.base import (
    EngineConfig,
    MemoryStats,
    Record,
    RunnerError,
    StepControl,
)
from minion_executor.runners.pooling import read_json
from minion_executor.runners.prompts import embedding_text

MAX_LENGTH = 512
CHUNK = 16  # texts per forward pass; bounds activation memory and sets the checkpoint cadence
# XLM-R is out: its position ids start at padding_idx + 1 and skip pads, which this encoder
# (positions 0..n-1) does not model, so it would silently produce wrong vectors.
_SUPPORTED_ARCH = ("BertModel", "BertForMaskedLM")


def _build_encoder(cfg: dict[str, Any]) -> Any:
    """BERT encoder as an `mlx.nn.Module` whose parameter names match HF `BertModel`."""
    import mlx.core as mx
    import mlx.nn as nn

    hidden = int(cfg["hidden_size"])
    heads = int(cfg["num_attention_heads"])
    eps = float(cfg.get("layer_norm_eps", 1e-12))

    class Embeddings(nn.Module):  # type: ignore[misc]
        def __init__(self) -> None:
            super().__init__()
            self.word_embeddings = nn.Embedding(int(cfg["vocab_size"]), hidden)
            self.position_embeddings = nn.Embedding(int(cfg["max_position_embeddings"]), hidden)
            self.token_type_embeddings = nn.Embedding(int(cfg.get("type_vocab_size", 2)), hidden)
            self.LayerNorm = nn.LayerNorm(hidden, eps=eps)

        def __call__(self, ids: Any) -> Any:
            pos = mx.arange(ids.shape[1])[None, :]
            x = self.word_embeddings(ids) + self.position_embeddings(pos)
            x = x + self.token_type_embeddings(mx.zeros_like(ids))
            return self.LayerNorm(x)

    class Attention(nn.Module):  # type: ignore[misc]
        def __init__(self) -> None:
            super().__init__()
            self.query = nn.Linear(hidden, hidden)
            self.key = nn.Linear(hidden, hidden)
            self.value = nn.Linear(hidden, hidden)

        def __call__(self, x: Any, mask: Any) -> Any:
            b, n, _ = x.shape

            def split(t: Any) -> Any:
                return t.reshape(b, n, heads, hidden // heads).transpose(0, 2, 1, 3)

            o = mx.fast.scaled_dot_product_attention(
                split(self.query(x)),
                split(self.key(x)),
                split(self.value(x)),
                scale=(hidden // heads) ** -0.5,
                mask=mask,
            )
            return o.transpose(0, 2, 1, 3).reshape(b, n, hidden)

    class SelfOutput(nn.Module):  # type: ignore[misc]
        def __init__(self) -> None:
            super().__init__()
            self.dense = nn.Linear(hidden, hidden)
            self.LayerNorm = nn.LayerNorm(hidden, eps=eps)

        def __call__(self, x: Any, residual: Any) -> Any:
            return self.LayerNorm(self.dense(x) + residual)

    class AttentionBlock(nn.Module):  # type: ignore[misc]
        def __init__(self) -> None:
            super().__init__()
            self.self = Attention()
            self.output = SelfOutput()

        def __call__(self, x: Any, mask: Any) -> Any:
            return self.output(self.self(x, mask), x)

    class Intermediate(nn.Module):  # type: ignore[misc]
        def __init__(self) -> None:
            super().__init__()
            self.dense = nn.Linear(hidden, int(cfg["intermediate_size"]))

        def __call__(self, x: Any) -> Any:
            return nn.gelu(self.dense(x))

    class Output(nn.Module):  # type: ignore[misc]
        def __init__(self) -> None:
            super().__init__()
            self.dense = nn.Linear(int(cfg["intermediate_size"]), hidden)
            self.LayerNorm = nn.LayerNorm(hidden, eps=eps)

        def __call__(self, x: Any, residual: Any) -> Any:
            return self.LayerNorm(self.dense(x) + residual)

    class Layer(nn.Module):  # type: ignore[misc]
        def __init__(self) -> None:
            super().__init__()
            self.attention = AttentionBlock()
            self.intermediate = Intermediate()
            self.output = Output()

        def __call__(self, x: Any, mask: Any) -> Any:
            x = self.attention(x, mask)
            return self.output(self.intermediate(x), x)

    class Encoder(nn.Module):  # type: ignore[misc]
        def __init__(self) -> None:
            super().__init__()
            self.layer = [Layer() for _ in range(int(cfg["num_hidden_layers"]))]

    class Bert(nn.Module):  # type: ignore[misc]
        def __init__(self) -> None:
            super().__init__()
            self.embeddings = Embeddings()
            self.encoder = Encoder()

        def __call__(self, ids: Any, attention_mask: Any) -> Any:
            """Last hidden state `(batch, seq, hidden)`; padding keys are masked out."""
            x = self.embeddings(ids)
            mask = mx.where(attention_mask[:, None, None, :] > 0, 0.0, -1e9).astype(x.dtype)
            for layer in self.encoder.layer:
                x = layer(x, mask)
            return x

    return Bert()


def _load_weights(path: Path) -> dict[str, Any]:
    import mlx.core as mx

    weights: dict[str, Any] = {}
    for f in sorted(path.glob("*.safetensors")):
        weights.update(cast(dict[str, Any], mx.load(str(f))))
    # Hub checkpoints name things `bert.*` / `model.*` or nothing, and carry pooler/head weights.
    out: dict[str, Any] = {}
    for k, v in weights.items():
        for prefix in ("bert.", "model."):
            if k.startswith(prefix):
                k = k[len(prefix) :]
        if k.startswith(("embeddings.", "encoder.")) and "position_ids" not in k:
            out[k] = v
    return out


class MlxEmbeddingRunner:
    def __init__(self) -> None:
        self._net: Any = None
        self._tok: Any = None
        self._pooling = "cls"
        self._max_len = MAX_LENGTH

    def load(
        self, model: ModelEntry, model_path: Path | None, budget: Budget, cfg: EngineConfig
    ) -> None:
        path = common.require_model_path(model_path)
        conf = read_json(path / "config.json")
        if conf is None:
            raise RunnerError(common.MODEL_UNSUPPORTED, "missing config.json", retryable=False)
        archs = conf.get("architectures") or []
        if conf.get("model_type") != "bert" and not any(a in _SUPPORTED_ARCH for a in archs):
            raise RunnerError(common.MODEL_UNSUPPORTED, "not a BERT-family model", retryable=False)
        import mlx.core as mx
        from transformers import AutoTokenizer

        common.apply_memory_limits(budget)
        try:
            net = _build_encoder(conf)
            net.load_weights(list(_load_weights(path).items()), strict=True)
            mx.eval(net.parameters())
            self._tok = AutoTokenizer.from_pretrained(
                str(path), local_files_only=True, trust_remote_code=False
            )
        except RuntimeError as exc:
            self.unload()
            if common.is_oom(exc):
                raise common.oom_error() from None
            raise
        except (ValueError, KeyError, OSError) as exc:
            self.unload()
            raise RunnerError(
                common.MODEL_UNSUPPORTED, type(exc).__name__, retryable=False
            ) from None
        self._net = net
        self._pooling = common.pooling_mode(read_json(path / "1_Pooling" / "config.json"))
        self._max_len = min(MAX_LENGTH, int(conf.get("max_position_embeddings", MAX_LENGTH)))

    def run(self, batch: list[Record], params: TaskParams, ctl: StepControl) -> list[Record]:
        if self._net is None:
            raise RunnerError("not_loaded")
        if not isinstance(params, EmbeddingParams):
            raise RunnerError("BAD_PARAMS", "embedding params expected", retryable=False)
        texts = [embedding_text(rec) for rec in batch]  # BAD_INPUT before any GPU work
        out: list[Record] = []
        try:
            for start in range(0, len(texts), CHUNK):
                chunk = texts[start : start + CHUNK]
                vecs, counts = self._embed(chunk, params.normalize)
                for rec, vec, n in zip(batch[start : start + CHUNK], vecs, counts, strict=True):
                    out.append({"id": rec.get("id"), "embedding": vec, "tokens": n})
                ctl.checkpoint(len(out))
        except RuntimeError as exc:
            if common.is_oom(exc):
                common.release_memory()
                raise common.oom_error() from None
            raise
        return out

    def _embed(self, texts: list[str], normalize: bool) -> tuple[list[list[float]], list[int]]:
        import mlx.core as mx

        enc = self._tok(
            texts,
            padding=True,
            truncation=True,
            max_length=self._max_len,
            return_tensors="np",
        )
        ids = mx.array(enc["input_ids"])
        mask = mx.array(enc["attention_mask"])
        hidden = self._net(ids, mask).astype(mx.float32)
        if self._pooling == "cls":
            pooled = hidden[:, 0]
        elif self._pooling == "lasttoken":
            last = mask.sum(axis=1) - 1
            pooled = mx.take_along_axis(hidden, last[:, None, None], axis=1)[:, 0]
        else:
            m = mask[:, :, None].astype(mx.float32)
            pooled = (hidden * m).sum(axis=1) / mx.maximum(m.sum(axis=1), 1.0)
        if normalize:
            pooled = pooled / mx.maximum(mx.linalg.norm(pooled, axis=1, keepdims=True), 1e-12)
        counts = [int(n) for n in cast(list[int], mask.sum(axis=1).tolist())]
        return cast(list[list[float]], pooled.tolist()), counts

    def memory_in_use(self) -> MemoryStats:
        try:
            return common.memory_stats()
        except ImportError:
            return {"active_bytes": 0, "peak_bytes": 0}

    def unload(self) -> None:
        self._net = None
        self._tok = None
        with contextlib.suppress(ImportError):
            common.release_memory()
