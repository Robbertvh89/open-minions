"""Ollama runner: local Ollama HTTP API over loopback, `httpx` only (ADR-0005, ADR-0009).

The Ollama daemon is a separate, unsandboxed process: `/api/pull` makes *it* download weights from
its own registry (confused-deputy residual risk, ADR-0009). The pin is the manifest digest, checked
against `/api/tags` after every load. Prompts and responses are never logged or put in messages.
"""

from __future__ import annotations

import contextlib
import ipaddress
import math
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx
from minion_common.schemas import (
    Budget,
    CompletionParams,
    EmbeddingParams,
    ModelEntry,
    TaskParams,
)

from minion_executor.runners.base import (
    ENGINE_UNAVAILABLE,
    EngineConfig,
    MemoryStats,
    Record,
    RunnerError,
    StepControl,
)
from minion_executor.runners.prompts import build_messages, embedding_text

MODEL_INTEGRITY = "MODEL_INTEGRITY"
EMBED_CHUNK = 16  # texts per /api/embed call; sets the checkpoint cadence
_REQUEST_TIMEOUT = httpx.Timeout(600.0, connect=5.0)
_PULL_TIMEOUT = httpx.Timeout(3600.0, connect=5.0)


def check_loopback(url: str) -> str:
    """Return `url` without a trailing slash, or raise if it is not an http loopback URL."""
    parts = urlsplit(url)
    host = parts.hostname or ""
    ok = parts.scheme == "http" and not parts.username and not parts.path.strip("/")
    if ok and host != "localhost":
        try:
            ok = ipaddress.ip_address(host).is_loopback
        except ValueError:
            ok = False
    if not ok:
        raise RunnerError(ENGINE_UNAVAILABLE, "ollama url must be http loopback", retryable=False)
    return url.rstrip("/")


def _unavailable(detail: str) -> RunnerError:
    return RunnerError(ENGINE_UNAVAILABLE, detail, retryable=True)


class OllamaRunner:
    def __init__(self, *, transport: httpx.BaseTransport | None = None) -> None:
        self._transport = transport
        self._client: httpx.Client | None = None
        self._ref = ""
        self._budget: Budget | None = None

    # -- HTTP -------------------------------------------------------------------------------
    def _request(
        self, method: str, path: str, body: dict[str, Any] | None = None, *, pull: bool = False
    ) -> httpx.Response:
        assert self._client is not None  # noqa: S101 - callers check loaded state
        try:
            return self._client.request(
                method, path, json=body, timeout=_PULL_TIMEOUT if pull else _REQUEST_TIMEOUT
            )
        except httpx.HTTPError as exc:  # transport, timeout, protocol: never echo the message
            raise _unavailable(type(exc).__name__) from None

    def _json(self, resp: httpx.Response) -> dict[str, Any]:
        if resp.status_code >= 400:  # body may echo the prompt: status only
            raise _unavailable(f"ollama http {resp.status_code}")
        try:
            data = resp.json()
        except ValueError:
            raise _unavailable("ollama returned invalid json") from None
        if not isinstance(data, dict):
            raise _unavailable("ollama returned unexpected json")
        return data

    # -- Runner protocol --------------------------------------------------------------------
    def load(
        self, model: ModelEntry, model_path: Path | None, budget: Budget, cfg: EngineConfig
    ) -> None:
        base = check_loopback(cfg.ollama_url)
        self.unload()
        self._client = httpx.Client(
            base_url=base,
            transport=self._transport,
            trust_env=False,
            follow_redirects=False,
        )
        self._ref, self._budget = model.ref, budget
        try:
            show = self._request("POST", "/api/show", {"model": model.ref})
            if show.status_code == 404:
                pulled = self._request(
                    "POST", "/api/pull", {"model": model.ref, "stream": False}, pull=True
                )
                self._json(pulled)
            elif show.status_code >= 400:
                raise _unavailable(f"ollama http {show.status_code}")
            self._check_digest(model)
        except BaseException:
            self.unload()
            raise

    def _check_digest(self, model: ModelEntry) -> None:
        tags = self._json(self._request("GET", "/api/tags"))
        want = (model.revision or "").removeprefix("sha256:").lower()
        for m in tags.get("models") or []:
            if isinstance(m, dict) and model.ref in (m.get("name"), m.get("model")):
                have = str(m.get("digest", "")).removeprefix("sha256:").lower()
                if want and have == want:
                    return
                break
        raise RunnerError(MODEL_INTEGRITY, "ollama model digest mismatch", retryable=False)

    def run(self, batch: list[Record], params: TaskParams, ctl: StepControl) -> list[Record]:
        if self._client is None or self._budget is None:
            raise RunnerError("not_loaded")
        if isinstance(params, CompletionParams):
            return self._complete(batch, params, ctl)
        if isinstance(params, EmbeddingParams):
            return self._embed(batch, params, ctl)
        raise RunnerError("BAD_PARAMS", "unknown params", retryable=False)

    def _chat(self, rec: Record, messages: list[Any], params: CompletionParams) -> Record:
        options: dict[str, Any] = {
            "temperature": params.temperature,
            "top_p": params.top_p,
            "num_predict": params.max_tokens,
        }
        if params.seed is not None:
            options["seed"] = params.seed
        data = self._json(
            self._request(
                "POST",
                "/api/chat",
                {"model": self._ref, "messages": messages, "stream": False, "options": options},
            )
        )
        msg = data.get("message")
        content = msg.get("content") if isinstance(msg, dict) else None
        if not isinstance(content, str):
            raise _unavailable("ollama returned no message")
        reason = "length" if data.get("done_reason") == "length" else "stop"
        return {
            "id": rec.get("id"),
            "completion": content,
            "finish_reason": reason,
            "prompt_tokens": int(data.get("prompt_eval_count") or 0),
            "completion_tokens": int(data.get("eval_count") or 0),
        }

    def _complete(
        self, batch: list[Record], params: CompletionParams, ctl: StepControl
    ) -> list[Record]:
        assert self._budget is not None  # noqa: S101
        all_messages = [build_messages(rec, params) for rec in batch]  # BAD_INPUT before any I/O
        if not batch:
            return []
        out: list[Record | None] = [None] * len(batch)
        workers = max(1, min(len(batch), self._budget.max_batch_size))
        pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="ollama")
        futures: dict[Future[Record], int] = {}
        try:
            for i, (rec, msgs) in enumerate(zip(batch, all_messages, strict=True)):
                futures[pool.submit(self._chat, rec, list(msgs), params)] = i
            for done, fut in enumerate(as_completed(futures), 1):
                out[futures[fut]] = fut.result()  # first failure fails the batch
                ctl.checkpoint(done)  # control-side calls stay on this thread
        except BaseException:
            for f in futures:
                f.cancel()
            raise
        finally:
            pool.shutdown(wait=True, cancel_futures=True)
        return [o for o in out if o is not None]

    def _embed(
        self, batch: list[Record], params: EmbeddingParams, ctl: StepControl
    ) -> list[Record]:
        if not params.normalize:
            # Ollama only returns L2-normalised vectors: let another executor/engine take it.
            raise _unavailable("ollama cannot return unnormalised embeddings")
        texts = [embedding_text(rec) for rec in batch]
        out: list[Record] = []
        for start in range(0, len(texts), EMBED_CHUNK):
            chunk = texts[start : start + EMBED_CHUNK]
            data = self._json(
                self._request("POST", "/api/embed", {"model": self._ref, "input": chunk})
            )
            vecs = data.get("embeddings")
            if not isinstance(vecs, list) or len(vecs) != len(chunk):
                raise _unavailable("ollama returned wrong embedding count")
            for rec, vec in zip(batch[start : start + EMBED_CHUNK], vecs, strict=True):
                out.append({"id": rec.get("id"), "embedding": _l2(vec)})
            ctl.checkpoint(len(out))
        return out

    def memory_in_use(self) -> MemoryStats:
        """Bytes Ollama holds for our model (`/api/ps`); the daemon's memory is not ours."""
        if self._client is None:
            return {"active_bytes": 0, "peak_bytes": 0}
        try:
            data = self._json(self._request("GET", "/api/ps"))
        except RunnerError:
            return {"active_bytes": 0, "peak_bytes": 0}
        size = 0
        for m in data.get("models") or []:
            if isinstance(m, dict) and self._ref in (m.get("name"), m.get("model")):
                size = int(m.get("size") or 0)
        return {"active_bytes": size, "peak_bytes": size}

    def unload(self) -> None:
        client, ref = self._client, self._ref
        if client is not None and ref:
            # best effort: ask the daemon to drop the weights now
            with contextlib.suppress(httpx.HTTPError):
                client.post("/api/generate", json={"model": ref, "keep_alive": 0}, timeout=10.0)
        if client is not None:
            client.close()
        self._client, self._ref, self._budget = None, "", None


def _l2(vec: Any) -> list[float]:
    if not isinstance(vec, list) or not vec:
        raise _unavailable("ollama returned a bad embedding")
    floats = [float(x) for x in vec]
    norm = math.sqrt(sum(x * x for x in floats))
    return [x / norm for x in floats] if norm > 1e-12 else floats
