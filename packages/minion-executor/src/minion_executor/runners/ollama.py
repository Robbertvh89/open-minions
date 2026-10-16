"""Ollama runner: local Ollama HTTP API over loopback, `httpx` only (ADR-0005, ADR-0009).

The Ollama daemon is a separate, unsandboxed process: `/api/pull` makes *it* download weights from
its own registry (confused-deputy residual risk, ADR-0009). The pin is the manifest digest, checked
against `/api/tags` after every load and again after every `run` (TOCTOU: a swapped model must
not get its outputs stamped with our pin). Ollama is opt-in (`--engines ollama`). Prompts and
responses are never logged or put in messages.

Failure mapping. Load phase (`show`, `pull`, `tags`): transport error and 5xx -> release
(`ENGINE_UNAVAILABLE`, this Mac's daemon is unwell); 4xx -> non-retryable fail. Run phase:
transport error and 404 (model removed locally) -> release; other 4xx -> non-retryable fail; 5xx ->
retryable fail (`OOM` when the status body says the model needs more memory; the body is matched,
never copied). Digest mismatch -> release (`MODEL_INTEGRITY`, executor-local).
"""

from __future__ import annotations

import contextlib
import ipaddress
import math
from collections.abc import Callable, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from functools import partial
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
    MODEL_INTEGRITY,
    OOM,
    EngineConfig,
    MemoryStats,
    Record,
    RunnerError,
    StepControl,
)
from minion_executor.runners.prompts import build_messages, embedding_text

PARAMS_UNSUPPORTED = "PARAMS_UNSUPPORTED"
ENGINE_REJECTED = "ENGINE_REJECTED"  # 4xx: the daemon refused this request; retrying cannot help
ENGINE_ERROR = "ENGINE_ERROR"  # 5xx: the daemon failed; another attempt may succeed
_TICK_S = 1.0  # checkpoint cadence while requests are in flight (duty cycle, pause, step timeout)
_REQUEST_TIMEOUT = httpx.Timeout(600.0, connect=5.0)
_PULL_MARGIN_S = 60.0  # a pull must end this long before the worker's load timeout
_UNLOAD_TIMEOUT = httpx.Timeout(3.0, connect=1.0)  # < worker UNLOAD_TIMEOUT_S
_NUM_CTX_HEADROOM = 4096  # constant context headroom (output tokens) on top of the prompt cap
_OOM_MARKERS = ("more system memory", "out of memory", "insufficient memory")


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


def _release(detail: str) -> RunnerError:
    return RunnerError(ENGINE_UNAVAILABLE, detail, retryable=True, release=True)


def release_model(url: str, ref: str) -> None:
    """Best-effort `keep_alive: 0` for `ref` on the loopback daemon (used by the control side
    after a hard kill of the worker). Short timeout, no env proxies, never raises."""
    with (
        contextlib.suppress(Exception),
        httpx.Client(
            base_url=check_loopback(url), trust_env=False, follow_redirects=False
        ) as client,
    ):
        client.post("/api/generate", json={"model": ref, "keep_alive": 0}, timeout=_UNLOAD_TIMEOUT)


def _status_error(resp: httpx.Response) -> RunnerError:
    """Map an HTTP error status to a RunnerError. The body may echo the prompt: it is only
    pattern-matched for out-of-memory, never copied into the message."""
    status = resp.status_code
    if status < 500:
        return RunnerError(ENGINE_REJECTED, f"ollama http {status}", retryable=False)
    try:
        text = resp.text.lower()
    except (httpx.HTTPError, ValueError):
        text = ""
    if any(m in text for m in _OOM_MARKERS):
        return RunnerError(OOM, "ollama out of memory", retryable=True)
    return RunnerError(ENGINE_ERROR, f"ollama http {status}", retryable=True)


class OllamaRunner:
    def __init__(self, *, transport: httpx.BaseTransport | None = None) -> None:
        self._transport = transport
        self._client: httpx.Client | None = None
        self._base = ""
        self._model: ModelEntry | None = None
        self._ref = ""
        self._budget: Budget | None = None
        self._cfg = EngineConfig()

    # -- HTTP -------------------------------------------------------------------------------
    def _new_client(self) -> httpx.Client:
        return httpx.Client(
            base_url=self._base,
            transport=self._transport,
            trust_env=False,
            follow_redirects=False,
        )

    def _request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        timeout: httpx.Timeout = _REQUEST_TIMEOUT,
    ) -> httpx.Response:
        client = self._client
        if client is None:  # unloaded while a request was queued
            raise _unavailable("ollama client closed")
        try:
            return client.request(method, path, json=body, timeout=timeout)
        except httpx.HTTPError as exc:  # transport, timeout, protocol: never echo the message
            raise _unavailable(type(exc).__name__) from None

    def _json(self, resp: httpx.Response, *, run_phase: bool = False) -> dict[str, Any]:
        if run_phase and resp.status_code == 404:  # model removed from this Mac's store
            raise _release("ollama model not found")
        if resp.status_code >= 400:
            raise _status_error(resp)
        try:
            data = resp.json()
        except ValueError:
            raise _unavailable("ollama returned invalid json") from None
        if not isinstance(data, dict):
            raise _unavailable("ollama returned unexpected json")
        return data

    def _keep_alive(self) -> str:
        return f"{max(1, self._cfg.keep_alive_s)}s"

    # -- Runner protocol --------------------------------------------------------------------
    def load(
        self, model: ModelEntry, model_path: Path | None, budget: Budget, cfg: EngineConfig
    ) -> None:
        base = check_loopback(cfg.ollama_url)
        self.unload()
        self._base = base
        self._client = self._new_client()
        self._model, self._ref, self._budget, self._cfg = model, model.ref, budget, cfg
        try:
            show = self._request("POST", "/api/show", {"model": model.ref})
            if show.status_code == 404:
                # Bounded below the worker's load timeout: a slow pull is a release, never a
                # killed worker. (A pull that did not finish resumes on the next attempt.)
                limit = max(10.0, cfg.load_timeout_s - _PULL_MARGIN_S)
                pulled = self._request(
                    "POST",
                    "/api/pull",
                    {"model": model.ref, "stream": False},
                    timeout=httpx.Timeout(limit, connect=5.0),
                )
                self._json(pulled)
            elif show.status_code >= 400:
                raise _status_error(show)
            self._check_digest()
        except RunnerError as exc:
            self.unload()
            if exc.code in (ENGINE_ERROR, OOM, ENGINE_UNAVAILABLE) and not exc.release:
                # load phase: a sick or unreachable daemon is this Mac's problem, not the subtask's
                raise _release(exc.message) from None
            raise
        except BaseException:
            self.unload()
            raise

    def _check_digest(self) -> None:
        """The loaded tag must still resolve to the pinned manifest digest. A mismatch is
        executor-local (this Mac's Ollama store differs): release, outputs are never stamped."""
        assert self._model is not None  # noqa: S101 - callers check loaded state
        tags = self._json(self._request("GET", "/api/tags"))
        want = (self._model.revision or "").removeprefix("sha256:").lower()
        for m in tags.get("models") or []:
            if isinstance(m, dict) and self._ref in (m.get("name"), m.get("model")):
                have = str(m.get("digest", "")).removeprefix("sha256:").lower()
                if want and have == want:
                    return
                break
        raise RunnerError(
            MODEL_INTEGRITY, "ollama model digest mismatch", retryable=True, release=True
        )

    def run(self, batch: list[Record], params: TaskParams, ctl: StepControl) -> list[Record]:
        if self._budget is None:
            raise RunnerError("not_loaded")
        if self._client is None:  # closed by a failed run's abort: reconnect for this one
            self._client = self._new_client()
        if isinstance(params, CompletionParams):
            out = self._complete(batch, params, ctl)
        elif isinstance(params, EmbeddingParams):
            out = self._embed(batch, params, ctl)
        else:
            raise RunnerError("BAD_PARAMS", "unknown params", retryable=False)
        if batch:
            self._check_digest()  # the model may have been replaced while we ran (TOCTOU)
        return out

    def _chat(self, rec: Record, messages: list[Any], params: CompletionParams) -> Record:
        options: dict[str, Any] = {
            "temperature": params.temperature,
            "top_p": params.top_p,
            "num_predict": params.max_tokens,
            # constant per executor so the daemon never reloads the model for a changed context
            "num_ctx": self._cfg.max_prompt_tokens + _NUM_CTX_HEADROOM,
        }
        if params.seed is not None:
            options["seed"] = params.seed
        data = self._json(
            self._request(
                "POST",
                "/api/chat",
                {
                    "model": self._ref,
                    "messages": messages,
                    "stream": False,
                    "keep_alive": self._keep_alive(),
                    "options": options,
                },
            ),
            run_phase=True,
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

    def _embed_one(self, rec: Record, text: str) -> Record:
        data = self._json(
            self._request(
                "POST",
                "/api/embed",
                {"model": self._ref, "input": text, "keep_alive": self._keep_alive()},
            ),
            run_phase=True,
        )
        vecs = data.get("embeddings")
        if not isinstance(vecs, list) or len(vecs) != 1:
            raise _unavailable("ollama returned wrong embedding count")
        # `prompt_eval_count` can under-report when Ollama's prompt cache holds a shared prefix.
        return {
            "id": rec.get("id"),
            "embedding": _l2(vecs[0]),
            "tokens": int(data.get("prompt_eval_count") or 0),
        }

    def _fan_out(self, jobs: Sequence[Callable[[], Record]], ctl: StepControl) -> list[Record]:
        """Run `jobs` on a pool bounded by the budget's batch size. The main thread wakes at
        least every `_TICK_S` to checkpoint (duty cycle and pause stay live during long requests).
        Those ticks also keep the control side's inactivity timeout from firing, so the real bound
        on a hung daemon is `_REQUEST_TIMEOUT`, not the step timeout. Control-side calls never
        leave this thread. The first failure aborts (and closes the client under in-flight jobs)."""
        assert self._budget is not None  # noqa: S101
        if not jobs:
            return []
        self._check_digest()  # a swapped tag must not even start (inputs are validated already)
        pool = ThreadPoolExecutor(
            max_workers=max(1, min(len(jobs), self._budget.max_batch_size)),
            thread_name_prefix="ollama",
        )
        futures: list[Future[Record]] = [pool.submit(job) for job in jobs]
        pending = set(futures)
        try:
            while pending:
                done, pending = wait(pending, timeout=_TICK_S, return_when=FIRST_COMPLETED)
                for fut in done:
                    fut.result()  # re-raise the first failure
                ctl.checkpoint(len(futures) - len(pending))
        except BaseException:
            # fail in-flight requests fast: close the client under them (reopened by the next run)
            client, self._client = self._client, None
            if client is not None:
                with contextlib.suppress(Exception):
                    client.close()
            pool.shutdown(wait=False, cancel_futures=True)
            raise
        pool.shutdown(wait=True)
        return [f.result() for f in futures]

    def _complete(
        self, batch: list[Record], params: CompletionParams, ctl: StepControl
    ) -> list[Record]:
        all_messages = [build_messages(rec, params) for rec in batch]  # BAD_INPUT before any I/O
        jobs = [
            partial(self._chat, rec, list(msgs), params)
            for rec, msgs in zip(batch, all_messages, strict=True)
        ]
        return self._fan_out(jobs, ctl)

    def _embed(
        self, batch: list[Record], params: EmbeddingParams, ctl: StepControl
    ) -> list[Record]:
        if not params.normalize:
            # Ollama only returns L2-normalised vectors. The server rejects such tasks for
            # ollama models (422 INVALID_PARAMS); this is the backstop, and it is final.
            raise RunnerError(
                PARAMS_UNSUPPORTED, "ollama cannot return unnormalised embeddings", retryable=False
            )
        texts = [embedding_text(rec) for rec in batch]  # BAD_INPUT before any I/O
        jobs = [partial(self._embed_one, rec, text) for rec, text in zip(batch, texts, strict=True)]
        return self._fan_out(jobs, ctl)

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
                client.post(
                    "/api/generate", json={"model": ref, "keep_alive": 0}, timeout=_UNLOAD_TIMEOUT
                )
        if client is not None:
            client.close()
        self._client, self._model, self._ref, self._budget = None, None, "", None


def _l2(vec: Any) -> list[float]:
    if not isinstance(vec, list) or not vec:
        raise _unavailable("ollama returned a bad embedding")
    floats = [float(x) for x in vec]
    norm = math.sqrt(sum(x * x for x in floats))
    return [x / norm for x in floats] if norm > 1e-12 else floats
