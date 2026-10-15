"""Model cache: pinned/verified fetch, LRU, cancel. Fake fetch only; CI stays offline."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from multiprocessing.connection import Connection
from pathlib import Path

import pytest
from minion_common.enums import Engine, TaskType
from minion_common.schemas import ModelEntry
from minion_executor.model_cache import (
    MANIFEST_NAME,
    MODEL_CACHE_FULL,
    MODEL_DOWNLOAD_FAILED,
    MODEL_INTEGRITY,
    MODEL_UNSUPPORTED,
    SAFE_PATTERNS,
    FetchEnv,
    FetchFn,
    FetchSpec,
    ModelCache,
    ModelFetchError,
    allowed_file,
)
from minion_executor.worker.process import FORCED_WORKER_ENV, scrub_env

REV = "a" * 40

pytestmark = pytest.mark.filterwarnings("ignore:.*fork.*:DeprecationWarning")


def _model(name: str = "m", engine: Engine = Engine.MLX, rev: str = REV) -> ModelEntry:
    return ModelEntry(
        model_id=name,
        type=TaskType.EMBEDDING,
        engine=engine,
        ref=f"org/{name}" if engine != Engine.OLLAMA else f"{name}:latest",
        revision=rev if engine != Engine.OLLAMA else "b" * 64,
        memory_gb=1,
    )


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def fake_fetch(
    files: dict[str, bytes] | None = None,
    *,
    bad_sha: bool = False,
    extra: dict[str, bytes] | None = None,
    counter: Path | None = None,
    delay: float = 0.0,
) -> FetchFn:
    payload = files if files is not None else {"w.safetensors": b"x" * 100, "config.json": b"{}"}

    def fetch(spec: FetchSpec, conn: Connection) -> None:
        plan = {k: [len(v), "0" * 64 if bad_sha else _sha(v)] for k, v in payload.items()}
        conn.send(("plan", sum(len(v) for v in payload.values()), plan))
        assert conn.recv() == "go"
        if counter is not None:
            with counter.open("a") as f:
                f.write(f"{spec.repo}\n")
        time.sleep(delay)
        for rel, data in {**payload, **(extra or {})}.items():
            p = Path(spec.staging) / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(data)
        conn.send(("done",))

    return fetch


def _cache(
    tmp_path: Path, fetch: FetchFn, cap: int = 10_000, download_timeout_s: float = 3600.0
) -> ModelCache:
    # fork: the fake fetch is a closure (not picklable); production uses spawn.
    return ModelCache(
        tmp_path / "c", cap, fetch=fetch, mp_context="fork", download_timeout_s=download_timeout_s
    )


async def test_ensure_fetches_verifies_and_writes_manifest(tmp_path: Path) -> None:
    cache = _cache(tmp_path, fake_fetch())
    path = await cache.ensure(_model())
    assert path == tmp_path / "c" / "hf" / "org--m" / REV
    manifest = json.loads((path / MANIFEST_NAME).read_text())
    assert manifest["revision"] == REV
    assert manifest["files"]["w.safetensors"]["sha256"] == _sha(b"x" * 100)
    assert cache.verify(path)
    assert not (tmp_path / "c" / ".staging").exists()


async def test_second_ensure_is_a_hit_and_touches(tmp_path: Path) -> None:
    counter = tmp_path / "n"
    cache = _cache(tmp_path, fake_fetch(counter=counter))
    path = await cache.ensure(_model())
    assert path is not None
    old = time.time() - 1000
    os.utime(path / MANIFEST_NAME, (old, old))
    await cache.ensure(_model())
    assert counter.read_text().count("\n") == 1
    assert (path / MANIFEST_NAME).stat().st_mtime > old + 500


async def test_ollama_and_fake_return_none(tmp_path: Path) -> None:
    cache = _cache(tmp_path, fake_fetch(counter=tmp_path / "n"))
    assert await cache.ensure(_model("o", Engine.OLLAMA)) is None
    fake = ModelEntry(
        model_id="f", type=TaskType.EMBEDDING, engine=Engine.FAKE, ref="f", memory_gb=1
    )
    assert await cache.ensure(fake) is None
    assert not (tmp_path / "n").exists()


async def test_corrupt_or_truncated_entry_is_refetched(tmp_path: Path) -> None:
    counter = tmp_path / "n"
    cache = _cache(tmp_path, fake_fetch(counter=counter))
    path = await cache.ensure(_model())
    assert path is not None
    (path / "w.safetensors").write_bytes(b"short")
    assert not cache.verify(path)
    await cache.ensure(_model())
    assert counter.read_text().count("\n") == 2
    (path / MANIFEST_NAME).unlink()
    await cache.ensure(_model())
    assert counter.read_text().count("\n") == 3
    assert cache.verify(path)


async def test_sha_mismatch_is_integrity_error_and_leaves_nothing(tmp_path: Path) -> None:
    cache = _cache(tmp_path, fake_fetch(bad_sha=True))
    with pytest.raises(ModelFetchError) as ei:
        await cache.ensure(_model())
    assert ei.value.code == MODEL_INTEGRITY and ei.value.retryable and not ei.value.release
    assert not (tmp_path / "c" / "hf" / "org--m" / REV).exists()


async def test_extra_or_disallowed_files_rejected(tmp_path: Path) -> None:
    with pytest.raises(ModelFetchError) as ei:
        await _cache(tmp_path, fake_fetch(extra={"evil.py": b"x"})).ensure(_model())
    assert ei.value.code == MODEL_UNSUPPORTED and not ei.value.retryable
    with pytest.raises(ModelFetchError) as ei2:
        await _cache(tmp_path, fake_fetch(extra={"other.safetensors": b"x"})).ensure(_model())
    assert ei2.value.code == MODEL_INTEGRITY


async def test_auto_map_is_unsupported_non_retryable(tmp_path: Path) -> None:
    files = {"w.safetensors": b"x", "config.json": b'{"auto_map": {"AutoModel": "m.M"}}'}
    with pytest.raises(ModelFetchError) as ei:
        await _cache(tmp_path, fake_fetch(files)).ensure(_model())
    assert ei.value.code == MODEL_UNSUPPORTED and not ei.value.retryable
    assert not (tmp_path / "c" / "hf" / "org--m" / REV).exists()


async def test_model_larger_than_cap_is_cache_full(tmp_path: Path) -> None:
    cache = _cache(tmp_path, fake_fetch(), cap=50)
    with pytest.raises(ModelFetchError) as ei:
        await cache.ensure(_model())
    assert ei.value.code == MODEL_CACHE_FULL and ei.value.retryable and not ei.value.release


async def test_lru_eviction_under_cap_keeps_in_use_model(tmp_path: Path) -> None:
    fetch = fake_fetch({"w.safetensors": b"x" * 100})
    cache = _cache(tmp_path, fetch, cap=250)
    pa = await cache.ensure(_model("a"))
    pb = await cache.ensure(_model("b"))
    assert pa is not None and pb is not None
    # b is older than a by mtime, but the in-use model (last ensured = b) is protected.
    os.utime(pb / MANIFEST_NAME, (1, 1))
    os.utime(pa / MANIFEST_NAME, (2, 2))
    pc = await cache.ensure(_model("c"))
    assert pc is not None and pc.exists()
    assert pb.exists()  # in use: kept although oldest
    assert not pa.exists()  # LRU among the rest
    assert cache.used_bytes() <= 250


async def test_cache_full_flags_in_use_block_and_release_unblocks(tmp_path: Path) -> None:
    cache = _cache(tmp_path, fake_fetch({"w.safetensors": b"x" * 100}), cap=150)
    pa = await cache.ensure(_model("a"))
    with pytest.raises(ModelFetchError) as ei:
        await cache.ensure(_model("b"))
    assert ei.value.code == MODEL_CACHE_FULL and ei.value.blocked_by_in_use
    assert pa is not None and pa.exists()  # nothing deleted by the failed attempt
    cache.release_in_use()
    pb = await cache.ensure(_model("b"))
    assert pb is not None and pb.exists() and not pa.exists()


async def test_model_larger_than_cap_is_not_flagged_in_use_blocked(tmp_path: Path) -> None:
    cache = _cache(tmp_path, fake_fetch({"w.safetensors": b"x" * 100}), cap=80)
    with pytest.raises(ModelFetchError) as ei:
        await cache.ensure(_model("a"))
    assert not ei.value.blocked_by_in_use


async def test_lru_order_oldest_first(tmp_path: Path) -> None:
    fetch = fake_fetch({"w.safetensors": b"x" * 100})
    cache = _cache(tmp_path, fetch, cap=300)
    paths = [await cache.ensure(_model(n)) for n in "abc"]
    for i, p in enumerate([paths[2], paths[0], paths[1]]):  # c oldest, then a, then b
        assert p is not None
        os.utime(p / MANIFEST_NAME, (10 + i, 10 + i))
    evicted = cache.evict_for(150, keep=paths[1])
    assert evicted == [paths[2], paths[0]]


def test_evict_for_impossible_deletes_nothing(tmp_path: Path) -> None:
    cache = _cache(tmp_path, fake_fetch(), cap=150)
    entry = cache.root / "hf" / "org--x" / REV
    entry.mkdir(parents=True)
    (entry / "w.safetensors").write_bytes(b"x" * 100)
    (entry / MANIFEST_NAME).write_text(json.dumps({"files": {"w.safetensors": {"size": 100}}}))
    with pytest.raises(ModelFetchError) as ei:
        cache.evict_for(100, keep=entry)
    assert ei.value.code == MODEL_CACHE_FULL
    assert entry.exists()


async def test_concurrent_ensure_is_serialised(tmp_path: Path) -> None:
    counter = tmp_path / "n"
    cache = _cache(tmp_path, fake_fetch(counter=counter, delay=0.3))
    other = _cache(tmp_path, fake_fetch(counter=counter, delay=0.3))  # second "process", same root
    results = await asyncio.gather(
        cache.ensure(_model()), cache.ensure(_model()), other.ensure(_model())
    )
    assert len(set(results)) == 1
    assert counter.read_text().count("\n") == 1


async def test_cancel_kills_the_fetch_process(tmp_path: Path) -> None:
    pidfile = tmp_path / "pid"

    def hang(spec: FetchSpec, conn: Connection) -> None:
        conn.send(("plan", 1, {"w.safetensors": [1, None]}))
        conn.recv()
        pidfile.write_text(str(os.getpid()))
        time.sleep(60)

    cache = _cache(tmp_path, hang)
    task = asyncio.create_task(cache.ensure(_model()))
    for _ in range(100):
        if pidfile.exists():
            break
        await asyncio.sleep(0.05)
    pid = int(pidfile.read_text())
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0.2)
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
    assert not (tmp_path / "c" / ".staging").exists()
    # the lock is free again
    assert await _cache(tmp_path, fake_fetch()).ensure(_model()) is not None


async def test_download_timeout_releases(tmp_path: Path) -> None:
    cache = _cache(tmp_path, fake_fetch(delay=30), download_timeout_s=0.5)
    with pytest.raises(ModelFetchError) as ei:
        await cache.ensure(_model())
    assert ei.value.code == MODEL_DOWNLOAD_FAILED and ei.value.release


class _Resp:
    status_code = 503


class _HubError(Exception):
    response = _Resp()


async def test_error_mapping_network_releases_hub_error_fails(tmp_path: Path) -> None:
    def net(spec: FetchSpec, conn: Connection) -> None:
        raise ConnectionError("http://user:pw@proxy:3128 unreachable")

    def hub(spec: FetchSpec, conn: Connection) -> None:
        raise _HubError("secret detail")

    with pytest.raises(ModelFetchError) as e1:
        await _cache(tmp_path, net).ensure(_model())
    assert e1.value.code == MODEL_DOWNLOAD_FAILED and e1.value.release
    assert "pw" not in str(e1.value)  # only the exception type crosses over
    with pytest.raises(ModelFetchError) as e2:
        await _cache(tmp_path, hub).ensure(_model())
    assert e2.value.code == MODEL_DOWNLOAD_FAILED and e2.value.retryable and not e2.value.release
    assert "secret detail" not in str(e2.value)


async def test_child_dying_without_message_releases(tmp_path: Path) -> None:
    def die(spec: FetchSpec, conn: Connection) -> None:
        os._exit(1)

    with pytest.raises(ModelFetchError) as ei:
        await _cache(tmp_path, die).ensure(_model())
    assert ei.value.code == MODEL_DOWNLOAD_FAILED and ei.value.release


async def test_child_env_has_fetch_env_and_parent_does_not(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out = tmp_path / "env.json"

    def dump(spec: FetchSpec, conn: Connection) -> None:
        out.write_text(json.dumps(dict(os.environ)))
        raise ConnectionError

    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "leak")
    fe = FetchEnv.capture({"HF_TOKEN": "hf_s3cret", "HTTPS_PROXY": "http://u:p@px:1"})
    cache = ModelCache(tmp_path / "c", 10**6, fetch_env=fe, fetch=dump, mp_context="fork")
    with pytest.raises(ModelFetchError):
        await cache.ensure(_model())
    env = json.loads(out.read_text())
    assert env["HF_TOKEN"] == "hf_s3cret" and env["HTTPS_PROXY"] == "http://u:p@px:1"
    assert env["HF_HUB_DISABLE_TELEMETRY"] == "1"
    assert "AWS_SECRET_ACCESS_KEY" not in env


def test_fetch_env_capture_and_redaction() -> None:
    fe = FetchEnv.capture(
        {
            "HF_TOKEN": "hf_s3cret",
            "HTTPS_PROXY": "http://user:pw@proxy:3128",
            "SSL_CERT_FILE": "/ca.pem",
            "HF_HUB_ETAG_TIMEOUT": "30",
            "HF_HUB_OFFLINE": "1",
            "AWS_SECRET_ACCESS_KEY": "nope",
            "PATH": "/bin",
        }
    )
    assert fe.names == ["HF_HUB_ETAG_TIMEOUT", "HF_TOKEN", "HTTPS_PROXY", "SSL_CERT_FILE"]
    for text in (repr(fe), str(fe), f"{fe!r}", repr(fe.values)):
        assert "hf_s3cret" not in text and "pw" not in text
    spec = FetchSpec("o/m", REV, "/s", fe.to_env())
    assert "hf_s3cret" not in repr(spec) + str(spec)
    assert fe.to_env()["HF_TOKEN"] == "hf_s3cret"


def test_fetch_env_rejects_non_https_endpoint() -> None:
    with pytest.raises(ValueError, match="https"):
        FetchEnv.capture({"HF_ENDPOINT": "http://mirror.example"})
    assert FetchEnv.capture({"HF_ENDPOINT": "https://mirror.example"}).names == ["HF_ENDPOINT"]


def test_worker_env_is_offline_and_secret_free() -> None:
    env = scrub_env({"HF_TOKEN": "t", "HTTPS_PROXY": "p", "SSL_CERT_FILE": "c", "PATH": "/bin"})
    assert env == {"PATH": "/bin"}
    assert FORCED_WORKER_ENV["HF_HUB_OFFLINE"] == "1"
    assert FORCED_WORKER_ENV["TRANSFORMERS_OFFLINE"] == "1"
    assert FORCED_WORKER_ENV["HF_DATASETS_OFFLINE"] == "1"


def test_safe_patterns_exclude_code_and_pickles() -> None:
    for bad in ("m.py", "pytorch_model.bin", "a.pt", "a.pkl", "a.gguf", "sub/x.py", "../a.json"):
        assert not allowed_file(bad)
    for good in ("model.safetensors", "config.json", "tokenizer.model", "1_Pooling/config.json"):
        assert allowed_file(good)
    assert not any(p.endswith((".py", ".bin", ".pt", ".pkl", ".gguf")) for p in SAFE_PATTERNS)
