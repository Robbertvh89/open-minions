"""Model cache: pinned/verified fetch, LRU, cancel. Fake fetch only; CI stays offline."""

from __future__ import annotations

import asyncio
import errno
import hashlib
import json
import os
import shutil
import threading
import time
from collections import namedtuple
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

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
    _hash_file,
    _HashCancelledError,
    allowed_file,
)
from minion_executor.worker.process import FORCED_WORKER_ENV, scrub_env

REV = "a" * 40

pytestmark = pytest.mark.filterwarnings("ignore:.*fork.*:DeprecationWarning")


_Usage = namedtuple("_Usage", "total used free")
TB = 1024**4


@pytest.fixture(autouse=True)
def roomy_disk(monkeypatch: pytest.MonkeyPatch) -> None:
    """The dev machine's real free space must not decide unit tests."""
    monkeypatch.setattr(shutil, "disk_usage", lambda _p: _Usage(2 * TB, 0, 2 * TB))


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
    tmp_path: Path,
    fetch: FetchFn,
    cap: int = 10_000,
    download_timeout_s: float = 3600.0,
    **kw: Any,
) -> ModelCache:
    # fork: the fake fetch is a closure (not picklable); production uses spawn.
    return ModelCache(
        tmp_path / "c",
        cap,
        fetch=fetch,
        mp_context="fork",
        download_timeout_s=download_timeout_s,
        **{"min_free_bytes": 0, "sandbox": False, **kw},
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
    # nothing to evict on this Mac can ever make it fit: hand the subtask back, remember it
    assert ei.value.code == MODEL_CACHE_FULL and ei.value.release and ei.value.local


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
    cache.release_in_use()  # c was pinned as the last ensured model
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
        await task  # _run_child's finally already killed and joined the child
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
    cache = ModelCache(
        tmp_path / "c", 10**6, fetch_env=fe, fetch=dump, mp_context="fork", sandbox=False
    )
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


# --- TASK-4.2 review fixes ---


def _spawn_counter(tmp_path: Path) -> tuple[Path, FetchFn]:
    """A fetch that records every spawn, then runs the normal fake."""
    spawned = tmp_path / "spawned"
    inner = fake_fetch()

    def fetch(spec: FetchSpec, conn: Connection) -> None:
        with spawned.open("a") as f:
            f.write("x\n")
        inner(spec, conn)

    return spawned, fetch


async def test_free_disk_reserve_releases_without_downloading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spawned, fetch = _spawn_counter(tmp_path)
    gib = 1024**3
    # 100 GiB disk, 20 GiB free: reserve = max(10 GiB, 10% = 10 GiB); model of 102 bytes fits
    # unless the configured floor is raised above what is left.
    monkeypatch.setattr(shutil, "disk_usage", lambda _p: _Usage(100 * gib, 80 * gib, 20 * gib))
    ok = _cache(tmp_path, fetch, min_free_bytes=10 * gib)
    assert await ok.ensure(_model("fits")) is not None
    assert not (tmp_path / "c" / ".staging").exists()

    tight = _cache(tmp_path, fetch, min_free_bytes=20 * gib)
    with pytest.raises(ModelFetchError) as ei:
        await tight.ensure(_model("big"))
    e = ei.value
    assert e.code == MODEL_CACHE_FULL and e.release and e.local and not e.blocked_by_in_use
    assert spawned.read_text().count("x") == 2  # ok + the failed attempt (spawned, no "go")
    assert not (tmp_path / "c" / ".staging").exists()
    assert not (tmp_path / "c" / "hf" / "org--big").exists()
    # remembered: the next assignment is released without even spawning a fetch child
    with pytest.raises(ModelFetchError) as ei2:
        await tight.ensure(_model("big"))
    assert ei2.value.release and ei2.value.code == MODEL_CACHE_FULL
    assert spawned.read_text().count("x") == 2


async def test_free_disk_reserve_is_ten_percent_of_disk_when_larger(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # 10% of 1 TB = ~100 GB beats the 1-byte floor: 50 GB free is not enough
    monkeypatch.setattr(shutil, "disk_usage", lambda _p: _Usage(10**12, 95 * 10**10, 5 * 10**10))
    with pytest.raises(ModelFetchError) as ei:
        await _cache(tmp_path, fake_fetch(), min_free_bytes=1).ensure(_model())
    assert ei.value.code == MODEL_CACHE_FULL and ei.value.release


async def test_disk_full_memory_expires(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = [1000.0]
    spawned, fetch = _spawn_counter(tmp_path)
    cache = _cache(tmp_path, fetch, clock=lambda: now[0])
    monkeypatch.setattr(shutil, "disk_usage", lambda _p: _Usage(10**12, 0, 0))
    with pytest.raises(ModelFetchError):
        await cache.ensure(_model())
    monkeypatch.setattr(shutil, "disk_usage", lambda _p: _Usage(2 * TB, 0, 2 * TB))
    with pytest.raises(ModelFetchError):  # still remembered
        await cache.ensure(_model())
    assert spawned.read_text().count("x") == 1
    now[0] += 3601
    assert await cache.ensure(_model()) is not None  # space freed, memory expired


async def test_enospc_in_child_cleans_staging_and_releases(tmp_path: Path) -> None:
    spawned = tmp_path / "spawned"

    def full(spec: FetchSpec, conn: Connection) -> None:
        with spawned.open("a") as f:
            f.write("x\n")
        conn.send(("plan", 1, {"w.safetensors": [1, "0" * 64, None]}))
        conn.recv()
        (Path(spec.staging) / "partial").write_bytes(b"x")
        raise OSError(errno.ENOSPC, "No space left on device")

    cache = _cache(tmp_path, full)
    with pytest.raises(ModelFetchError) as ei:
        await cache.ensure(_model())
    e = ei.value
    assert e.code == MODEL_CACHE_FULL and e.release and e.local and e.retryable is not False
    assert not (tmp_path / "c" / ".staging").exists()
    with pytest.raises(ModelFetchError):  # fast release from memory, no second download
        await cache.ensure(_model())
    assert spawned.read_text().count("x") == 1


async def test_enospc_while_sealing_in_parent_is_mapped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(*_a: Any, **_k: Any) -> Any:
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr("minion_executor.model_cache._hash_file", boom)
    with pytest.raises(ModelFetchError) as ei:
        await _cache(tmp_path, fake_fetch()).ensure(_model())
    assert ei.value.code == MODEL_CACHE_FULL and ei.value.release
    assert not (tmp_path / "c" / ".staging").exists()


@pytest.mark.parametrize(("status", "release"), [(401, True), (403, True), (500, False)])
async def test_hub_auth_errors_are_local_and_remembered(
    tmp_path: Path, status: int, release: bool
) -> None:
    spawned = tmp_path / "spawned"

    class Resp:
        status_code = status

    class HubError(Exception):
        response = Resp()

    def hub(spec: FetchSpec, conn: Connection) -> None:
        with spawned.open("a") as f:
            f.write("x\n")
        raise HubError("detail")

    cache = _cache(tmp_path, hub)
    with pytest.raises(ModelFetchError) as ei:
        await cache.ensure(_model())
    assert ei.value.code == MODEL_DOWNLOAD_FAILED and ei.value.release is release
    with pytest.raises(ModelFetchError):
        await cache.ensure(_model())
    assert spawned.read_text().count("x") == (1 if release else 2)


async def test_local_memory_is_bounded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shutil, "disk_usage", lambda _p: _Usage(10**12, 0, 0))
    cache = _cache(tmp_path, fake_fetch())
    for i in range(80):
        with pytest.raises(ModelFetchError):
            await cache.ensure(_model(f"m{i}"))
    assert len(cache._local) <= 64


async def test_pinned_model_survives_eviction_by_another_process(tmp_path: Path) -> None:
    fetch = fake_fetch({"w.safetensors": b"x" * 100})
    proc_a = _cache(tmp_path, fetch, cap=150)
    proc_b = _cache(tmp_path, fetch, cap=150)  # same root, "another executor process"
    pa = await proc_a.ensure(_model("a"))
    assert pa is not None
    with pytest.raises(ModelFetchError) as ei:  # a is loaded in A: B may not evict it
        await proc_b.ensure(_model("b"))
    assert ei.value.code == MODEL_CACHE_FULL and ei.value.release
    assert not ei.value.blocked_by_in_use and not ei.value.local  # unloading B's own would not help
    assert pa.exists()
    proc_a.release_in_use()  # A unloaded
    pb = await proc_b.ensure(_model("b"))
    assert pb is not None and pb.exists() and not pa.exists()


async def test_switching_pin_unpins_previous_entry(tmp_path: Path) -> None:
    fetch = fake_fetch({"w.safetensors": b"x" * 100})
    a = _cache(tmp_path, fetch, cap=250)
    other = _cache(tmp_path, fetch, cap=250)
    pa = await a.ensure(_model("a"))
    pb = await a.ensure(_model("b"))
    assert pa is not None and pb is not None
    os.utime(pa / MANIFEST_NAME, (1, 1))
    pc = await other.ensure(_model("c"))  # a is no longer pinned and oldest; b is pinned
    assert pc is not None and not pa.exists() and pb.exists()


def test_hash_file_cancels_between_chunks(tmp_path: Path) -> None:
    f = tmp_path / "f"
    f.write_bytes(b"x" * 10)
    ev = threading.Event()
    assert _hash_file(f, ev, git_blob=False)[0] == _sha(b"x" * 10)
    ev.set()
    with pytest.raises(_HashCancelledError):
        _hash_file(f, ev, git_blob=False)


async def test_cancel_during_hashing_sets_event_and_stops_thread(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    started, stopped = threading.Event(), threading.Event()

    def slow_hash(path: Path, cancel: threading.Event, *, git_blob: bool) -> tuple[str, None]:
        started.set()
        cancel.wait(10)
        stopped.set()
        raise _HashCancelledError

    monkeypatch.setattr("minion_executor.model_cache._hash_file", slow_hash)
    task = asyncio.create_task(_cache(tmp_path, fake_fetch()).ensure(_model()))
    for _ in range(200):
        if started.is_set():
            break
        await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await asyncio.to_thread(stopped.wait, 5)


async def test_child_gets_nice_parent_watch_and_staging_scoped_hub_dirs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import minion_executor.worker.process as proc

    watched = tmp_path / "watched"
    monkeypatch.setattr(
        proc, "_watch_parent", lambda pid, interval: watched.write_text(f"{pid} {interval}")
    )
    out = tmp_path / "env.json"
    inner = fake_fetch()

    def dump(spec: FetchSpec, conn: Connection) -> None:
        out.write_text(json.dumps({"nice": os.nice(0), **dict(os.environ), "s": spec.staging}))
        inner(spec, conn)

    path = await _cache(tmp_path, dump, nice=7).ensure(_model())
    assert path is not None
    d = json.loads(out.read_text())
    assert d["nice"] >= 7
    assert d["HF_HOME"].startswith(d["s"]) and d["HF_XET_CACHE"].startswith(d["s"])
    assert d["TMPDIR"].startswith(d["s"])
    assert not (path / ".hf-home").exists() and not (path / ".tmp").exists()
    for _ in range(100):
        if watched.exists():
            break
        await asyncio.sleep(0.05)
    assert watched.read_text().split()[0] == str(os.getpid())


async def test_sweep_removes_stale_staging_and_trash_and_entries_skip_dotnames(
    tmp_path: Path,
) -> None:
    cache = _cache(tmp_path, fake_fetch())
    root = tmp_path / "c"
    stale_stage = root / ".staging" / "dead"
    trash = root / "hf" / "org--x" / ".trash-abc"
    for d in (stale_stage, trash):
        d.mkdir(parents=True)
        (d / "w.safetensors").write_bytes(b"x" * 5)
    (trash / MANIFEST_NAME).write_text(json.dumps({"files": {"w.safetensors": {"size": 5}}}))
    assert cache.used_bytes() == 0  # dot-named dirs are not entries
    assert await cache.ensure(_model()) is not None
    assert not stale_stage.exists() and not trash.exists()


async def test_leftover_trash_is_removed_after_eviction(tmp_path: Path) -> None:
    cache = _cache(tmp_path, fake_fetch({"w.safetensors": b"x" * 100}), cap=150)
    await cache.ensure(_model("a"))
    cache.release_in_use()
    await cache.ensure(_model("b"))
    assert not list((tmp_path / "c" / "hf").glob("*/.trash-*"))


def _git_blob(data: bytes) -> str:
    return hashlib.sha1(b"blob %d\0" % len(data) + data, usedforsecurity=False).hexdigest()


def _blob_fetch(files: dict[str, bytes], blob_ids: dict[str, str | None]) -> FetchFn:
    def fetch(spec: FetchSpec, conn: Connection) -> None:
        plan = {
            k: [len(v), _sha(v) if k.endswith(".safetensors") else None, blob_ids.get(k)]
            for k, v in files.items()
        }
        conn.send(("plan", sum(len(v) for v in files.values()), plan))
        conn.recv()
        for k, v in files.items():
            (Path(spec.staging) / k).write_bytes(v)
        conn.send(("done",))

    return fetch


async def test_non_lfs_files_are_verified_against_git_blob_sha1(tmp_path: Path) -> None:
    cfg = b'{"a": 1}'
    files = {"w.safetensors": b"x" * 10, "config.json": cfg}
    good = _blob_fetch(files, {"config.json": _git_blob(cfg)})
    assert await _cache(tmp_path, good).ensure(_model("g")) is not None
    bad = _blob_fetch(files, {"config.json": _git_blob(b"other")})
    with pytest.raises(ModelFetchError) as ei:
        await _cache(tmp_path, bad).ensure(_model("b"))
    assert ei.value.code == MODEL_INTEGRITY
    none = _blob_fetch(files, {})  # non-LFS file with no digest from the hub: fail closed
    with pytest.raises(ModelFetchError) as ei2:
        await _cache(tmp_path, none).ensure(_model("n"))
    assert ei2.value.code == MODEL_INTEGRITY


async def test_symlinked_directory_is_rejected(tmp_path: Path) -> None:
    inner = fake_fetch()

    def fetch(spec: FetchSpec, conn: Connection) -> None:
        inner(spec, conn)

    def with_link(spec: FetchSpec, conn: Connection) -> None:
        # hook into the "done" step: add a directory symlink after the files are written
        class Wrap:
            def send(self, m: Any) -> None:
                if m[0] == "done":
                    (Path(spec.staging) / "linkdir").symlink_to("/")
                conn.send(m)

            def recv(self) -> Any:
                return conn.recv()

        inner(spec, Wrap())  # type: ignore[arg-type]

    with pytest.raises(ModelFetchError) as ei:
        await _cache(tmp_path, with_link).ensure(_model())
    assert ei.value.code == MODEL_INTEGRITY
    assert not (tmp_path / "c" / "hf" / "org--m" / REV).exists()


@pytest.mark.parametrize(
    "name", ["tokenizer_config.json", "preprocessor_config.json", "sub/processor.json"]
)
async def test_auto_map_in_any_json_is_unsupported(tmp_path: Path, name: str) -> None:
    files = {
        "w.safetensors": b"x",
        "config.json": b"{}",
        name: b'{"x": {"auto_map": {"AutoTokenizer": ["m.T", null]}}}',
    }
    fetch = fake_fetch(files)
    # sub/ needs a directory in staging: fake_fetch creates parents
    with pytest.raises(ModelFetchError) as ei:
        await _cache(tmp_path, fetch).ensure(_model())
    assert ei.value.code == MODEL_UNSUPPORTED and not ei.value.retryable


async def test_spawn_context_pickles_spec_and_replaces_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Real `spawn` (production path): FetchSpec and the fetch function must pickle, the
    child imports the module-level fake, and its env is exactly the explicit one."""
    monkeypatch.syspath_prepend(str(Path(__file__).parent))
    import exec_fetch_helpers as h

    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "leak")
    fe = FetchEnv.capture({"HF_TOKEN": "hf_s3cret"})
    cache = ModelCache(
        tmp_path / "c",
        10**6,
        fetch_env=fe,
        fetch=h.env_probe_fetch,
        mp_context="spawn",
        min_free_bytes=0,
        sandbox=False,
        nice=3,
    )
    path = await cache.ensure(_model())
    assert path is not None
    seen = json.loads((path / "config.json").read_text())
    assert seen["has_token"] is True and seen["leak"] is False
    assert seen["repo"] == "org/m" and seen["nice"] >= 3
    assert seen["hf_home"].startswith(seen["staging"]) and seen["xet"].startswith(seen["staging"])


async def test_sandbox_failure_fails_closed_and_releases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from minion_executor.runners.base import RunnerError
    from minion_executor.worker import sandbox as seatbelt

    ran = tmp_path / "ran"

    def deny(_spec: dict[str, Any]) -> None:
        raise RunnerError("SANDBOX_FAILED", "nope")

    monkeypatch.setattr(seatbelt, "apply_from_spec", deny)

    def fetch(spec: FetchSpec, conn: Connection) -> None:
        ran.write_text("x")

    cache = _cache(tmp_path, fetch, sandbox=True)
    with pytest.raises(ModelFetchError) as ei:
        await cache.ensure(_model())
    assert ei.value.code == "SANDBOX_FAILED" and ei.value.release and ei.value.local
    assert not ran.exists()  # the fetch never ran unconfined


async def test_hub_401_with_a_token_is_released_but_not_remembered(tmp_path: Path) -> None:
    spawned = tmp_path / "spawned"

    class Resp:
        status_code = 401

    class HubError(Exception):
        response = Resp()

    def hub(spec: FetchSpec, conn: Connection) -> None:
        with spawned.open("a") as f:
            f.write("x\n")
        raise HubError("detail")

    cache = _cache(tmp_path, hub, fetch_env=FetchEnv.capture({"HF_TOKEN": "hf_x"}))
    for _ in range(2):  # not remembered: a rotated token should work next time
        with pytest.raises(ModelFetchError) as ei:
            await cache.ensure(_model())
        assert ei.value.release is True and ei.value.local is False
    assert spawned.read_text().count("x") == 2


def _entry(cache: ModelCache, name: str, size: int, mtime: float) -> Path:
    d = cache.root / "hf" / f"org--{name}" / REV
    d.mkdir(parents=True)
    (d / "w.safetensors").write_bytes(b"x" * size)
    (d / MANIFEST_NAME).write_text(json.dumps({"files": {"w.safetensors": {"size": size}}}))
    os.utime(d / MANIFEST_NAME, (mtime, mtime))
    return d


def test_evict_for_checks_disk_with_planned_evictions_before_deleting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = _cache(tmp_path, fake_fetch(), cap=250, min_free_bytes=100)
    old = _entry(cache, "old", 100, 1)
    new = _entry(cache, "new", 100, 2)
    # free 150, need 100: 150 - 100 = 50 < reserve 100, but evicting `old` frees 100 more
    monkeypatch.setattr(shutil, "disk_usage", lambda _p: _Usage(1000, 0, 150))
    assert cache.evict_for(100) == [old]
    assert not old.exists() and new.exists()


def test_evict_for_disk_failure_deletes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = _cache(tmp_path, fake_fetch(), cap=250, min_free_bytes=1000)
    old = _entry(cache, "old", 100, 1)
    new = _entry(cache, "new", 100, 2)
    monkeypatch.setattr(shutil, "disk_usage", lambda _p: _Usage(1000, 0, 150))
    with pytest.raises(ModelFetchError) as ei:
        cache.evict_for(100)
    assert ei.value.code == MODEL_CACHE_FULL and ei.value.release and ei.value.local
    assert old.exists() and new.exists()


async def test_staging_overflow_kills_the_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("minion_executor.model_cache._STAGING_MARGIN_BYTES", 100)
    monkeypatch.setattr("minion_executor.model_cache._STAGING_POLL_S", 0.05)

    def flood(spec: FetchSpec, conn: Connection) -> None:
        conn.send(("plan", 10, {"w.safetensors": [10, _sha(b"x" * 10), None]}))
        assert conn.recv() == "go"
        (Path(spec.staging) / "w.safetensors").write_bytes(b"x" * 10_000)
        time.sleep(60)

    t0 = time.monotonic()
    with pytest.raises(ModelFetchError) as ei:
        await _cache(tmp_path, flood).ensure(_model())
    assert ei.value.code == MODEL_INTEGRITY and ei.value.release
    assert time.monotonic() - t0 < 20
    assert not (tmp_path / "c" / ".staging").exists()


async def test_fetch_child_self_test_exit_maps_to_sandbox_failed(tmp_path: Path) -> None:
    from minion_executor.worker.sandbox import SANDBOX_FAILED, SELF_TEST_EXIT_CODE

    def die(spec: FetchSpec, conn: Connection) -> None:
        os._exit(SELF_TEST_EXIT_CODE)

    with pytest.raises(ModelFetchError) as ei:
        await _cache(tmp_path, die).ensure(_model())
    assert ei.value.code == SANDBOX_FAILED and ei.value.release and ei.value.local


def test_pin_sets_in_use_only_after_the_flock_succeeds(tmp_path: Path) -> None:
    import fcntl

    cache = _cache(tmp_path, fake_fetch())
    entry = _entry(cache, "a", 10, 1)
    fd = os.open(entry / MANIFEST_NAME, os.O_RDONLY)
    fcntl.flock(fd, fcntl.LOCK_EX)  # e.g. another process evicting it
    try:
        cache._pin(entry)
        assert cache._in_use is None and cache._pin_fd is None
    finally:
        os.close(fd)
    cache._pin(entry)
    assert cache._in_use == entry and cache._pin_fd is not None
    cache.release_in_use()


def test_confine_spec_tcp_is_https_plus_proxy_only(tmp_path: Path) -> None:
    def tcp(env: dict[str, str]) -> list[str]:
        c = _cache(tmp_path, fake_fetch(), sandbox=True, fetch_env=FetchEnv.capture(env))
        spec = c._confine_spec(tmp_path / "st")
        assert spec is not None
        return list(spec["net_tcp"])

    assert tcp({}) == ["*:443"]
    assert tcp({"HF_ENDPOINT": "https://hub.example:8443"}) == ["*:443", "*:8443"]
    # with a proxy only the proxy port is allowed, not *:443
    assert tcp({"HTTPS_PROXY": "http://user:pw@proxy.corp:3128"}) == ["*:3128"]
    assert tcp({"HTTPS_PROXY": "http://127.0.0.1:8888"}) == ["localhost:8888"]
    # NO_PROXY set next to a proxy: direct hub access is possible, so *:443 (and the
    # HF_ENDPOINT port) is allowed as well
    both = {"HTTPS_PROXY": "http://proxy.corp:3128", "NO_PROXY": "hub.example"}
    assert tcp(both) == ["*:3128", "*:443"]
    assert tcp({**both, "HF_ENDPOINT": "https://hub.example:8443"}) == [
        "*:3128",
        "*:443",
        "*:8443",
    ]
    assert tcp({"ALL_PROXY": "proxy.corp:8080"}) == ["*:8080"]
    # HTTP_PROXY is not used for https: never opens *:80
    assert tcp({"HTTP_PROXY": "http://proxy.corp"}) == ["*:443"]


def test_confine_spec_ca_files_are_literals_and_home_cert_dirs_refused(tmp_path: Path) -> None:
    ca = tmp_path / "ca.pem"
    ca.write_text("x")
    c = _cache(
        tmp_path, fake_fetch(), sandbox=True, fetch_env=FetchEnv.capture({"SSL_CERT_FILE": str(ca)})
    )
    spec = c._confine_spec(tmp_path / "st")
    assert spec is not None and spec["read_files"] == [str(ca)] and str(ca) not in spec["read"]

    homedir = Path.home() / "certs"
    bad = _cache(
        tmp_path,
        fake_fetch(),
        sandbox=True,
        fetch_env=FetchEnv.capture({"SSL_CERT_DIR": str(homedir)}),
    )
    with pytest.raises(ModelFetchError) as ei:
        bad._confine_spec(tmp_path / "st")
    assert ei.value.local and ei.value.release and "SSL_CERT_DIR" in ei.value.message
    ok = _cache(
        tmp_path,
        fake_fetch(),
        sandbox=True,
        fetch_env=FetchEnv.capture({"SSL_CERT_DIR": "/etc/ssl/certs"}),
    )
    spec = ok._confine_spec(tmp_path / "st")
    assert spec is not None and os.path.realpath("/etc/ssl/certs") in spec["read"]


async def test_staging_disk_reserve_kills_the_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("minion_executor.model_cache._STAGING_POLL_S", 0.05)
    monkeypatch.setattr(
        ModelCache, "_reserve", lambda _self, total: total + 2**30
    )  # never met, even with the margin
    monkeypatch.setattr(ModelCache, "_check_disk", lambda *_a, **_k: None)

    def slow(spec: FetchSpec, conn: Connection) -> None:
        time.sleep(60)

    t0 = time.monotonic()
    with pytest.raises(ModelFetchError) as ei:
        await _cache(tmp_path, slow).ensure(_model())
    assert ei.value.code == MODEL_CACHE_FULL and ei.value.release and ei.value.local
    assert time.monotonic() - t0 < 20


async def test_staging_poll_tolerates_overshoot_within_the_margin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Free space between reserve - margin and reserve (normal staging overshoot) is fine."""
    monkeypatch.setattr("minion_executor.model_cache._STAGING_POLL_S", 0.05)
    free = shutil.disk_usage(tmp_path).free
    reserve = free + 1024  # free is 1 KiB below the reserve, well within the margin
    monkeypatch.setattr(ModelCache, "_reserve", lambda _self, _total: reserve)
    monkeypatch.setattr(ModelCache, "_check_disk", lambda *_a, **_k: None)

    def slow(spec: FetchSpec, conn: Connection) -> None:
        time.sleep(1)
        conn.send(("err", "X", "x", False, False))

    with pytest.raises(ModelFetchError) as ei:
        await _cache(tmp_path, slow).ensure(_model())
    assert ei.value.code != MODEL_CACHE_FULL


async def test_staging_entry_cap_applies_before_the_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("minion_executor.model_cache._STAGING_POLL_S", 0.05)
    monkeypatch.setattr("minion_executor.model_cache._STAGING_MAX_ENTRIES", 5)

    def spam(spec: FetchSpec, conn: Connection) -> None:
        for i in range(50):
            (Path(spec.staging) / f"f{i}").write_bytes(b"x")
        time.sleep(60)

    with pytest.raises(ModelFetchError) as ei:
        await _cache(tmp_path, spam).ensure(_model())
    assert ei.value.code == MODEL_INTEGRITY and ei.value.release


def test_child_env_disables_xet_chunk_cache(tmp_path: Path) -> None:
    env = _cache(tmp_path, fake_fetch())._child_env(tmp_path / "st")
    assert env["HF_XET_CHUNK_CACHE_SIZE_BYTES"] == "0"
