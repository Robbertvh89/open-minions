"""Ollama runner against `httpx.MockTransport` (no daemon needed)."""

from __future__ import annotations

import json
import math
import threading
import time
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from minion_common.enums import Engine, TaskType
from minion_common.schemas import Budget, CompletionParams, EmbeddingParams, ModelEntry
from minion_executor.runners import SUPPORTED, create_runner, probe_engines
from minion_executor.runners.base import EngineConfig, RunnerError
from minion_executor.runners.ollama import OllamaRunner

DIGEST = "ab" * 32
REF = "qwen2.5:0.5b"
MODEL = ModelEntry(
    model_id="m", type=TaskType.COMPLETION, engine=Engine.OLLAMA, ref=REF,
    revision=DIGEST, memory_gb=1,
)  # fmt: skip


def _budget(n: int = 2) -> Budget:
    return Budget.model_validate(
        {"profile": "balanced", "memory_gb": 2, "gpu_duty_cycle": 1.0, "max_batch_size": n}
    )


class Daemon:
    """Fake Ollama: records requests, answers by path."""

    def __init__(self, *, digest: str = DIGEST, present: bool = True) -> None:
        self.digest, self.present = digest, present
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.chat_hook: Callable[[], None] | None = None
        self.embed_hook: Callable[[], None] | None = None
        self.lock = threading.Lock()

    def __call__(self, req: httpx.Request) -> httpx.Response:
        body = json.loads(req.content) if req.content else {}
        with self.lock:
            self.calls.append((req.url.path, body))
        p = req.url.path
        if p == "/api/show":
            return httpx.Response(200 if self.present else 404, json={})
        if p == "/api/pull":
            self.present = True
            return httpx.Response(200, json={"status": "success"})
        if p == "/api/tags":
            return httpx.Response(200, json={"models": [{"name": REF, "digest": self.digest}]})
        if p == "/api/chat":
            if self.chat_hook:
                self.chat_hook()
            return httpx.Response(
                200,
                json={
                    "message": {"role": "assistant", "content": "hi"},
                    "done_reason": "stop",
                    "prompt_eval_count": 7,
                    "eval_count": 3,
                },
            )
        if p == "/api/embed":
            if self.embed_hook:
                self.embed_hook()
            inputs = body["input"] if isinstance(body["input"], list) else [body["input"]]
            return httpx.Response(
                200,
                json={"embeddings": [[3.0, 4.0] for _ in inputs], "prompt_eval_count": 5},
            )
        if p == "/api/ps":
            return httpx.Response(200, json={"models": [{"name": REF, "size": 1234}]})
        return httpx.Response(200, json={})

    def paths(self) -> list[str]:
        return [p for p, _ in self.calls]


class Ctl:
    def __init__(self) -> None:
        self.ticks: list[int | None] = []

    def checkpoint(self, records_done: int | None = None) -> None:
        self.ticks.append(records_done)


def _loaded(d: Daemon, n: int = 2) -> OllamaRunner:
    r = OllamaRunner(transport=httpx.MockTransport(d))
    r.load(MODEL, None, _budget(n), EngineConfig())
    return r


def test_supported_and_factory() -> None:
    assert Engine.OLLAMA in SUPPORTED
    assert isinstance(create_runner(Engine.OLLAMA, TaskType.COMPLETION), OllamaRunner)


def test_load_present_skips_pull() -> None:
    d = Daemon()
    _loaded(d)
    assert d.paths() == ["/api/show", "/api/tags"]


def test_load_404_pulls_without_streaming() -> None:
    d = Daemon(present=False)
    _loaded(d)
    assert d.paths() == ["/api/show", "/api/pull", "/api/tags"]
    assert d.calls[1][1] == {"model": REF, "stream": False}


def test_digest_mismatch_is_released_not_failed() -> None:
    d = Daemon(digest="cd" * 32)
    r = OllamaRunner(transport=httpx.MockTransport(d))
    with pytest.raises(RunnerError) as ei:
        r.load(MODEL, None, _budget(), EngineConfig())
    assert ei.value.code == "MODEL_INTEGRITY" and ei.value.release  # executor-local


def test_unknown_model_in_tags_is_integrity_error() -> None:
    d = Daemon()
    d.__dict__["digest"] = DIGEST
    other = ModelEntry(
        model_id="m", type=TaskType.COMPLETION, engine=Engine.OLLAMA, ref="other:1b",
        revision=DIGEST, memory_gb=1,
    )  # fmt: skip
    r = OllamaRunner(transport=httpx.MockTransport(d))
    with pytest.raises(RunnerError) as ei:
        r.load(other, None, _budget(), EngineConfig())
    assert ei.value.code == "MODEL_INTEGRITY"


@pytest.mark.parametrize(
    "url",
    ["http://example.com:11434", "http://10.0.0.5:11434", "https://127.0.0.1:11434",
     "http://127.0.0.1.evil.com:11434", "http://user@127.0.0.1:11434", "http://127.0.0.1/x"],
)  # fmt: skip
def test_non_loopback_url_refused(url: str) -> None:
    d = Daemon()
    r = OllamaRunner(transport=httpx.MockTransport(d))
    with pytest.raises(RunnerError) as ei:
        r.load(MODEL, None, _budget(), EngineConfig(ollama_url=url))
    assert ei.value.code == "ENGINE_UNAVAILABLE"
    assert d.calls == []


@pytest.mark.parametrize("url", ["http://127.0.0.1:1", "http://localhost:2", "http://[::1]:3"])
def test_loopback_urls_accepted(url: str) -> None:
    r = OllamaRunner(transport=httpx.MockTransport(Daemon()))
    r.load(MODEL, None, _budget(), EngineConfig(ollama_url=url))


def test_transport_error_is_retryable_engine_unavailable() -> None:
    def boom(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("secret prompt text")

    r = OllamaRunner(transport=httpx.MockTransport(boom))
    with pytest.raises(RunnerError) as ei:
        r.load(MODEL, None, _budget(), EngineConfig())
    assert ei.value.code == "ENGINE_UNAVAILABLE" and ei.value.retryable
    assert "secret" not in str(ei.value)


def test_chat_request_body_and_output() -> None:
    d = Daemon()
    r = _loaded(d)
    params = CompletionParams(temperature=0.5, top_p=0.9, max_tokens=16, seed=3, system="be nice")
    out = r.run([{"id": "a", "prompt": "hello"}], params, Ctl())
    body = next(b for p, b in d.calls if p == "/api/chat")
    assert body == {
        "model": REF,
        "stream": False,
        "messages": [
            {"role": "system", "content": "be nice"},
            {"role": "user", "content": "hello"},
        ],
        "keep_alive": "60s",
        "options": {
            "temperature": 0.5,
            "top_p": 0.9,
            "num_predict": 16,
            "seed": 3,
            "num_ctx": 8192 + 16,
        },
    }
    assert out == [
        {"id": "a", "completion": "hi", "finish_reason": "stop",
         "prompt_tokens": 7, "completion_tokens": 3}
    ]  # fmt: skip


def test_chat_omits_seed_when_unset() -> None:
    d = Daemon()
    _loaded(d).run([{"id": 1, "prompt": "x"}], CompletionParams(), Ctl())
    body = next(b for p, b in d.calls if p == "/api/chat")
    assert set(body["options"]) == {"temperature", "top_p", "num_predict", "num_ctx"}


def test_bad_input_fails_before_any_request() -> None:
    d = Daemon()
    r = _loaded(d)
    n = len(d.calls)
    with pytest.raises(RunnerError) as ei:
        r.run([{"id": 1, "prompt": "ok"}, {"id": 2}], CompletionParams(), Ctl())
    assert ei.value.code == "BAD_INPUT" and len(d.calls) == n


def test_concurrency_bound_checkpoint_and_order() -> None:
    d = Daemon()
    live = peak = 0
    lock = threading.Lock()

    def hook() -> None:
        nonlocal live, peak
        with lock:
            live += 1
            peak = max(peak, live)
        time.sleep(0.05)
        with lock:
            live -= 1

    d.chat_hook = hook
    r = _loaded(d, n=3)
    ctl = Ctl()
    batch = [{"id": i, "prompt": f"p{i}"} for i in range(8)]
    out = r.run(batch, CompletionParams(), ctl)
    assert [o["id"] for o in out] == list(range(8))
    assert 1 < peak <= 3
    assert ctl.ticks[-1] == 8 and ctl.ticks == sorted(t or 0 for t in ctl.ticks)


def _failing(status: int, text: str = "echo of the prompt") -> OllamaRunner:
    def handler(req: httpx.Request) -> httpx.Response:
        if req.url.path == "/api/chat":
            return httpx.Response(status, text=text)
        return Daemon()(req)

    r = OllamaRunner(transport=httpx.MockTransport(handler))
    r.load(MODEL, None, _budget(), EngineConfig())
    return r


@pytest.mark.parametrize("status", [400, 404, 413])
def test_4xx_is_a_non_retryable_fail(status: int) -> None:
    with pytest.raises(RunnerError) as ei:
        _failing(status).run([{"id": 1, "prompt": "x"}], CompletionParams(), Ctl())
    assert ei.value.code == "ENGINE_REJECTED" and not ei.value.retryable
    assert not ei.value.release and "echo" not in str(ei.value)


@pytest.mark.parametrize("status", [500, 502, 503])
def test_5xx_is_a_retryable_fail_not_a_release(status: int) -> None:
    with pytest.raises(RunnerError) as ei:
        _failing(status).run([{"id": 1, "prompt": "x"}], CompletionParams(), Ctl())
    assert ei.value.code == "ENGINE_ERROR" and ei.value.retryable and not ei.value.release
    assert "echo" not in str(ei.value)


def test_oom_looking_500_maps_to_oom_without_echoing_the_body() -> None:
    body = "model requires more system memory (9.0 GiB) than is available: SECRET PROMPT"
    with pytest.raises(RunnerError) as ei:
        _failing(500, body).run([{"id": 1, "prompt": "x"}], CompletionParams(), Ctl())
    assert ei.value.code == "OOM" and ei.value.retryable
    assert "SECRET" not in str(ei.value) and "9.0" not in str(ei.value)


def test_mid_run_transport_error_is_a_release() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        if req.url.path == "/api/chat":
            raise httpx.ReadTimeout("secret prompt text")
        return Daemon()(req)

    r = OllamaRunner(transport=httpx.MockTransport(handler))
    r.load(MODEL, None, _budget(), EngineConfig())
    with pytest.raises(RunnerError) as ei:
        r.run([{"id": 1, "prompt": "x"}], CompletionParams(), Ctl())
    assert ei.value.code == "ENGINE_UNAVAILABLE" and ei.value.retryable
    assert "secret" not in str(ei.value)


def test_show_5xx_at_load_is_a_fail_not_a_release() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    r = OllamaRunner(transport=httpx.MockTransport(handler))
    with pytest.raises(RunnerError) as ei:
        r.load(MODEL, None, _budget(), EngineConfig())
    assert ei.value.code == "ENGINE_ERROR" and not ei.value.release


def test_first_failure_aborts_without_waiting_for_the_rest() -> None:
    calls = 0
    lock = threading.Lock()
    gate = threading.Event()

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal calls
        if req.url.path == "/api/chat":
            with lock:
                calls += 1
                first = calls == 1
            if first:
                return httpx.Response(400, text="no")
            gate.wait(5)  # the rest are slow: the batch must not wait for them
            return Daemon()(req)
        return Daemon()(req)

    r = OllamaRunner(transport=httpx.MockTransport(handler))
    r.load(MODEL, None, _budget(2), EngineConfig())
    t0 = time.monotonic()
    try:
        with pytest.raises(RunnerError):
            r.run([{"id": i, "prompt": "x"} for i in range(6)], CompletionParams(), Ctl())
        assert time.monotonic() - t0 < 2.0
    finally:
        gate.set()


def test_checkpoints_keep_firing_during_one_long_request(monkeypatch: pytest.MonkeyPatch) -> None:
    import minion_executor.runners.ollama as mod

    monkeypatch.setattr(mod, "_TICK_S", 0.05)
    d = Daemon()
    d.chat_hook = lambda: time.sleep(0.5)
    ctl = Ctl()
    _loaded(d).run([{"id": 1, "prompt": "x"}], CompletionParams(), ctl)
    assert len(ctl.ticks) >= 5 and ctl.ticks[:-1] == [0] * (len(ctl.ticks) - 1)
    assert ctl.ticks[-1] == 1


def test_checkpoint_raising_aborts_the_run(monkeypatch: pytest.MonkeyPatch) -> None:
    import minion_executor.runners.ollama as mod

    monkeypatch.setattr(mod, "_TICK_S", 0.05)

    class StopError(Exception):
        pass

    class StopCtl:
        def checkpoint(self, records_done: int | None = None) -> None:
            raise StopError

    d = Daemon()
    d.chat_hook = lambda: time.sleep(0.3)
    with pytest.raises(StopError):
        _loaded(d).run([{"id": 1, "prompt": "x"}], CompletionParams(), StopCtl())


def test_digest_is_rechecked_after_run_and_mismatch_releases() -> None:
    d = Daemon()
    r = _loaded(d)
    d.chat_hook = lambda: setattr(d, "digest", "cd" * 32)  # swapped while we ran
    with pytest.raises(RunnerError) as ei:
        r.run([{"id": 1, "prompt": "x"}], CompletionParams(), Ctl())
    assert ei.value.code == "MODEL_INTEGRITY" and ei.value.release and ei.value.retryable
    assert d.paths()[-1] == "/api/tags"


def test_embed_one_text_per_call_with_tokens_and_keep_alive() -> None:
    d = Daemon()
    r = _loaded(d, n=3)
    ctl = Ctl()
    batch = [{"id": i, "text": f"t{i}"} for i in range(5)]
    out = r.run(batch, EmbeddingParams(normalize=True), ctl)
    bodies = [b for p, b in d.calls if p == "/api/embed"]
    assert sorted(b["input"] for b in bodies) == [f"t{i}" for i in range(5)]
    assert all(
        set(b) == {"model", "input", "keep_alive"} and b["keep_alive"] == "60s" for b in bodies
    )
    assert [o["id"] for o in out] == list(range(5))
    assert all(set(o) == {"id", "embedding", "tokens"} and o["tokens"] == 5 for o in out)
    assert math.isclose(out[0]["embedding"][0], 0.6) and math.isclose(out[0]["embedding"][1], 0.8)
    assert ctl.ticks[-1] == 5


def test_embed_unnormalized_is_a_final_params_error() -> None:
    d = Daemon()
    r = _loaded(d)
    n = len(d.calls)
    with pytest.raises(RunnerError) as ei:
        r.run([{"id": 1, "text": "x"}], EmbeddingParams(normalize=False), Ctl())
    assert ei.value.code == "PARAMS_UNSUPPORTED" and not ei.value.retryable
    assert not ei.value.release and len(d.calls) == n


def test_keep_alive_follows_config() -> None:
    d = Daemon()
    r = OllamaRunner(transport=httpx.MockTransport(d))
    r.load(MODEL, None, _budget(), EngineConfig(keep_alive_s=120))
    r.run([{"id": 1, "prompt": "x"}], CompletionParams(), Ctl())
    assert next(b for p, b in d.calls if p == "/api/chat")["keep_alive"] == "120s"


def test_num_ctx_covers_prompt_window_plus_output() -> None:
    d = Daemon()
    r = OllamaRunner(transport=httpx.MockTransport(d))
    r.load(MODEL, None, _budget(), EngineConfig(max_prompt_tokens=1000))
    r.run([{"id": 1, "prompt": "x"}], CompletionParams(max_tokens=24), Ctl())
    assert next(b for p, b in d.calls if p == "/api/chat")["options"]["num_ctx"] == 1024


def test_pull_timeout_is_below_the_worker_load_timeout() -> None:
    seen: list[float | None] = []

    def handler(req: httpx.Request) -> httpx.Response:
        if req.url.path == "/api/pull":
            seen.append(req.extensions["timeout"]["read"])
        return Daemon(present=False)(req)

    r = OllamaRunner(transport=httpx.MockTransport(handler))
    r.load(MODEL, None, _budget(), EngineConfig(load_timeout_s=600.0))
    assert seen == [540.0]


def test_pull_timeout_is_a_release_not_a_crash() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        if req.url.path == "/api/pull":
            raise httpx.ReadTimeout("slow")
        return Daemon(present=False)(req)

    r = OllamaRunner(transport=httpx.MockTransport(handler))
    with pytest.raises(RunnerError) as ei:
        r.load(MODEL, None, _budget(), EngineConfig())
    assert ei.value.code == "ENGINE_UNAVAILABLE" and ei.value.retryable


def test_memory_in_use_from_ps() -> None:
    assert _loaded(Daemon()).memory_in_use() == {"active_bytes": 1234, "peak_bytes": 1234}
    assert OllamaRunner().memory_in_use() == {"active_bytes": 0, "peak_bytes": 0}


def test_unload_sends_keep_alive_zero() -> None:
    d = Daemon()
    r = _loaded(d)
    r.unload()
    assert d.calls[-1] == ("/api/generate", {"model": REF, "keep_alive": 0})
    with pytest.raises(RunnerError):
        r.run([{"id": 1, "prompt": "x"}], CompletionParams(), Ctl())
    r.unload()  # idempotent


def test_probe_uses_api_version(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []

    def fake_get(url: str, **kw: Any) -> httpx.Response:
        seen.append(url)
        return httpx.Response(200, json={"version": "0.12.3"})

    monkeypatch.setattr(httpx, "get", fake_get)
    assert probe_engines([Engine.OLLAMA], EngineConfig()) == {"ollama": "0.12.3"}
    assert seen == ["http://127.0.0.1:11434/api/version"]
    bad = EngineConfig(ollama_url="http://example.com:11434")
    assert probe_engines([Engine.OLLAMA], bad) == {"ollama": None}


def test_probe_daemon_down(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(url: str, **kw: Any) -> httpx.Response:
        raise httpx.ConnectError("down")

    monkeypatch.setattr(httpx, "get", boom)
    assert probe_engines([Engine.OLLAMA], EngineConfig()) == {"ollama": None}


@pytest.mark.ollama
def test_live_daemon_tiny_model() -> None:
    """Needs `ollama serve` with a pulled tiny model. Set MINION_TEST_OLLAMA_REF/_DIGEST."""
    import os

    ref, digest = (
        os.environ.get("MINION_TEST_OLLAMA_REF"),
        os.environ.get("MINION_TEST_OLLAMA_DIGEST"),
    )
    if not ref or not digest:
        pytest.skip("set MINION_TEST_OLLAMA_REF and MINION_TEST_OLLAMA_DIGEST")
    model = ModelEntry(
        model_id="live", type=TaskType.COMPLETION, engine=Engine.OLLAMA, ref=ref,
        revision=digest, memory_gb=1,
    )  # fmt: skip
    r = OllamaRunner()
    r.load(model, None, _budget(), EngineConfig())
    try:
        out = r.run([{"id": 1, "prompt": "Say hi."}], CompletionParams(max_tokens=8, seed=1), Ctl())
        assert out[0]["id"] == 1 and out[0]["completion_tokens"] > 0
    finally:
        r.unload()
