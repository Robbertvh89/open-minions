"""Local model cache and the killable fetch subprocess (TASK-4.2 S4, D5-D7).

Weights are downloaded by the *control side* so the inference worker can stay offline:

* The download runs in a short-lived `spawn` subprocess (not an inference process). Cancelling
  `ModelCache.ensure` (subtask cancel, SIGTERM/stop) kills it, so shutdown never waits on a
  download thread (invariant 6).
* Only the pinned `ModelEntry.revision` is fetched, and only files matching `SAFE_PATTERNS`
  (safetensors, json, tokenizer files); never `*.py`, `*.bin`, `*.pt`, `*.pkl` or `*.gguf`.
* The child reports the file list from the hub's `files_metadata` (size, LFS sha256 or, for
  non-LFS files, the git blob id). The expected digests therefore come *from the hub, via the
  child*; the *parent* re-hashes every downloaded file against them (sha256 for LFS files, the
  git blob sha1 `sha1("blob <size>\0" + content)` for the small non-LFS ones) and records the
  sha256 in the manifest. A compromised child can at most make the parent accept what the
  child says the hub said, which is why the child is also sandboxed (Seatbelt: writes only to
  its staging dir, no HOME reads, no exec).
* `HF_TOKEN`, proxy and CA settings (`FetchEnv`) reach only the fetch subprocess, never the
  worker, and are never logged.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import errno
import fcntl
import fnmatch
import hashlib
import json
import multiprocessing as mp
import os
import shutil
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import structlog
from minion_common.enums import Engine
from minion_common.schemas import ModelEntry
from pydantic import SecretStr

from minion_executor.runners.base import MODEL_INTEGRITY, RunnerError
from minion_executor.worker import sandbox as seatbelt

log = structlog.get_logger(__name__)

# Allow-list of files fetched from the hub. Deliberately excludes *.py (remote code), *.bin,
# *.pt, *.pkl (pickle) and *.gguf.
SAFE_PATTERNS: tuple[str, ...] = (
    "*.safetensors",
    "*.json",
    "tokenizer.model",
    "*.tiktoken",
    "vocab.txt",
    "merges.txt",
    "*.jinja",
)

MODEL_DOWNLOAD_FAILED = "MODEL_DOWNLOAD_FAILED"
MODEL_CACHE_FULL = "MODEL_CACHE_FULL"
MODEL_UNSUPPORTED = "MODEL_UNSUPPORTED"

MANIFEST_NAME = ".minion-manifest.json"
_HASH_CHUNK = 1024 * 1024
_GIB = 1024**3

_PROXY_NAMES = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
)
_CA_NAMES = ("SSL_CERT_FILE", "SSL_CERT_DIR", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE")
# Never forwarded even if set: would make the fetch fail or leak telemetry / change the cache.
_HF_HUB_EXCLUDED = frozenset({"HF_HUB_OFFLINE", "HF_HUB_CACHE", "HF_HUB_DISABLE_TELEMETRY"})
_BASE_CHILD_NAMES = ("PATH", "HOME", "LANG")

# Local-only failures are remembered per (repo, revision) so repeated assignments of the same
# model are released without touching the network or the disk again.
_LOCAL_MEMORY_MAX = 64
_LOCAL_TTL_S = 3600.0
_DISK_FULL_TTL_S = 600.0  # the owner may free space, so look again sooner
DEFAULT_MIN_FREE_BYTES = 10 * _GIB
_DISK_RESERVE_FRACTION = 0.10

# Extra Mach services the sandboxed fetch child may look up on top of the worker's list
# (`sandbox.MACH_ALLOW`). Ablation on macOS 27 with the real `-m model` download: DNS and TCP
# need nothing extra; `huggingface_hub` -> `httpx2` verifies TLS through `truststore` (the
# Security framework), which fails with OSStatus -26276 unless `trustd.agent` is reachable.
# Not allowed (download works without): trustd, SecurityServer, configd, nsurlsessiond.
FETCH_MACH_ALLOW: tuple[str, ...] = ("com.apple.trustd.agent",)

# A compromised fetch child could fill the disk past what the hub plan announced. While it
# downloads, the parent measures the staging dir (hub client scratch included) and kills it above
# `plan total + margin`. Margin: hub-client bookkeeping, partial-file overhead.
_STAGING_MARGIN_BYTES = 64 * 1024 * 1024
_STAGING_MARGIN_FRACTION = 0.05


def _staging_margin(needed: int) -> int:
    return max(_STAGING_MARGIN_BYTES, int(needed * _STAGING_MARGIN_FRACTION))


_STAGING_POLL_S = 1.0
_STAGING_MAX_ENTRIES = 10_000  # files + dirs in a staging dir, before and after the plan


class ModelFetchError(RunnerError):
    """Model could not be provisioned. `release=True` hands the subtask back without burning an
    attempt (executor-local trouble such as the network being down). Messages carry no URLs,
    credentials or payload."""

    def __init__(
        self,
        code: str,
        message: str = "",
        *,
        retryable: bool = True,
        release: bool = False,
        blocked_by_in_use: bool = False,
        local: bool = False,
    ) -> None:
        super().__init__(code, message, retryable=retryable)
        self.release = release
        # Executor-local condition that will recur for this model on this Mac (cache cap,
        # disk full, hub access without a token): the cache remembers it and releases fast.
        self.local = local
        # MODEL_CACHE_FULL only because the protected in-use model occupies the space; the
        # caller may free it (unload the worker) and retry once.
        self.blocked_by_in_use = blocked_by_in_use


def allowed_file(rel: str) -> bool:
    """True if `rel` (posix path inside the repo) matches `SAFE_PATTERNS`."""
    parts = rel.split("/")
    if rel.startswith("/") or any(p in ("", ".", "..") for p in parts):
        return False
    return any(fnmatch.fnmatchcase(rel, pat) for pat in SAFE_PATTERNS)


@dataclass(frozen=True, repr=False)
class FetchEnv:
    """Proxy / CA / hub settings captured from the environment before `restrict_environ()`.

    Values may be credentials (token, `http://user:pw@proxy`), so they are `SecretStr` and the
    repr lists names only. They are handed to the fetch subprocess as an explicit env dict.
    """

    values: Mapping[str, SecretStr] = field(default_factory=dict)

    @classmethod
    def capture(cls, environ: Mapping[str, str] | None = None) -> FetchEnv:
        env = os.environ if environ is None else environ
        out: dict[str, SecretStr] = {}
        for name, value in env.items():
            if not value:
                continue
            if (
                name in _PROXY_NAMES
                or name in _CA_NAMES
                or name in ("HF_TOKEN", "HF_ENDPOINT")
                or (name.startswith("HF_HUB_") and name not in _HF_HUB_EXCLUDED)
            ):
                out[name] = SecretStr(value)
        endpoint = out.get("HF_ENDPOINT")
        if endpoint is not None and not endpoint.get_secret_value().startswith("https://"):
            raise ValueError("HF_ENDPOINT must be an https URL")
        return cls(out)

    @property
    def names(self) -> list[str]:
        return sorted(self.values)

    def to_env(self) -> dict[str, str]:
        return {k: v.get_secret_value() for k, v in self.values.items()}

    def __repr__(self) -> str:
        return f"FetchEnv(names={self.names})"

    __str__ = __repr__


@dataclass(frozen=True, repr=False)
class FetchSpec:
    """What the fetch subprocess needs. `env` may hold secrets, hence the redacting repr."""

    repo: str
    revision: str
    staging: str
    env: Mapping[str, str]

    def __repr__(self) -> str:
        return f"FetchSpec(repo={self.repo!r}, revision={self.revision!r})"

    __str__ = __repr__


# fetch(spec, conn) runs in the child. Protocol over `conn`:
#   child  -> ("plan", total_bytes, {rel: [size, sha256 | None, git_blob_sha1 | None]})
#   parent -> "go"          (after making room; anything else / a kill aborts)
#   child  -> ("done",)     (files are in spec.staging)
#   child  -> ("err", code, message, retryable, release, local)   at any point
FetchFn = Callable[[FetchSpec, Connection], None]


def _is_enospc(exc: BaseException | None) -> bool:
    seen = 0
    while exc is not None and seen < 8:
        if isinstance(exc, OSError) and exc.errno == errno.ENOSPC:
            return True
        exc = exc.__cause__ or exc.__context__
        seen += 1
    return False


def _send_err(conn: Connection, exc: BaseException) -> None:
    if isinstance(exc, ModelFetchError):
        conn.send(("err", exc.code, exc.message, exc.retryable, exc.release, exc.local))
    elif _is_enospc(exc):
        conn.send(("err", MODEL_CACHE_FULL, "disk full", True, True, True))
    elif isinstance(exc, RunnerError):  # e.g. SANDBOX_FAILED: this Mac cannot fetch at all
        conn.send(("err", exc.code, exc.message, False, True, True))
    else:
        # Hub verdict (HTTP status) -> failure, except 401/403 which only mean this Mac has no
        # (valid) token: local, released. Anything else is network/local -> release.
        status = getattr(getattr(exc, "response", None), "status_code", None)
        # 401 with a token set is a bad/expired/revoked credential on THIS Mac: released but not
        # remembered, so a rotated token works next time.
        if status == 401 and os.environ.get("HF_TOKEN"):
            conn.send(("err", MODEL_DOWNLOAD_FAILED, "hub rejected token 401", True, True, False))
        elif status == 403 or status == 401:
            conn.send(
                ("err", MODEL_DOWNLOAD_FAILED, f"hub access denied {status}", True, True, True)
            )
        elif status is not None:
            conn.send(("err", MODEL_DOWNLOAD_FAILED, f"hub error {status}", True, False, False))
        else:
            conn.send(("err", MODEL_DOWNLOAD_FAILED, type(exc).__name__, True, True, False))


def _child_main(
    fetch: FetchFn,
    spec: FetchSpec,
    conn: Connection,
    parent_pid: int | None = None,
    nice: int = 0,
    confine: dict[str, Any] | None = None,
) -> None:
    from minion_executor.worker.process import _watch_parent

    if parent_pid is not None:  # the control process died: never keep downloading
        threading.Thread(target=_watch_parent, args=(parent_pid, 1.0), daemon=True).start()
    if nice:
        with contextlib.suppress(OSError):
            os.nice(nice)
    os.environ.clear()
    os.environ.update(spec.env)
    try:
        if confine is not None:  # fail closed: any error is reported, fetch never runs
            seatbelt.apply_from_spec(confine)
        fetch(spec, conn)
    except BaseException as exc:
        with contextlib.suppress(Exception):
            _send_err(conn, exc)
    finally:
        conn.close()


def _hf_fetch(spec: FetchSpec, conn: Connection) -> None:
    """Child: resolve the pinned revision, report the plan, wait for room, download."""
    from huggingface_hub import HfApi, snapshot_download

    api = HfApi()
    info = api.model_info(spec.repo, revision=spec.revision, files_metadata=True)
    if info.sha != spec.revision:
        raise ModelFetchError(MODEL_INTEGRITY, "hub returned a different revision", retryable=True)
    files: dict[str, list[Any]] = {}
    for sib in info.siblings or []:
        if allowed_file(sib.rfilename):
            sha = sib.lfs.sha256 if sib.lfs is not None else None
            files[sib.rfilename] = [sib.size, sha, None if sha else sib.blob_id]
    if not any(name.endswith(".safetensors") for name in files):
        raise ModelFetchError(MODEL_UNSUPPORTED, "no safetensors weights", retryable=False)
    total = sum(int(v[0] or 0) for v in files.values())
    conn.send(("plan", total, files))
    if conn.recv() != "go":
        return
    snapshot_download(
        spec.repo,
        revision=spec.revision,
        allow_patterns=list(SAFE_PATTERNS),
        local_dir=spec.staging,
        max_workers=4,
    )
    conn.send(("done",))


def _revision(model: ModelEntry) -> str:
    if model.revision is None:  # ModelEntry validation guarantees it for mlx/hf
        raise ModelFetchError(MODEL_UNSUPPORTED, "model has no pinned revision", retryable=False)
    return model.revision


class _HashCancelledError(Exception):
    """The awaiting task was cancelled; stop hashing (the thread cannot be interrupted)."""


def _hash_file(path: Path, cancel: threading.Event, *, git_blob: bool) -> tuple[str, str | None]:
    """`(sha256, git blob sha1 | None)` of a file, checking `cancel` per chunk.

    Runs in a worker thread (`asyncio.to_thread`) at the default scheduling priority: a
    per-thread `nice` is not possible on macOS (`setpriority` applies per process), so the
    chunked read plus the cancel flag is what keeps the Mac responsive.
    """
    sha = hashlib.sha256()
    blob = None
    if git_blob:
        blob = hashlib.sha1(usedforsecurity=False)
        blob.update(b"blob %d\0" % path.stat().st_size)
    with path.open("rb") as f:
        while chunk := f.read(_HASH_CHUNK):
            if cancel.is_set():
                raise _HashCancelledError
            sha.update(chunk)
            if blob is not None:
                blob.update(chunk)
    return sha.hexdigest(), None if blob is None else blob.hexdigest()


def _tree_files(root: Path) -> Iterator[tuple[str, Path]]:
    """Every file under `root`, plus symlinked directories (listed so callers reject them)."""
    for dirpath, dirs, names in os.walk(root):
        for name in [*names, *(d for d in dirs if (Path(dirpath) / d).is_symlink())]:
            p = Path(dirpath) / name
            yield p.relative_to(root).as_posix(), p


def _dir_bytes(path: Path) -> int:
    return sum(p.stat().st_size for _rel, p in _tree_files(path) if not p.is_symlink())


def _staging_usage(path: Path) -> tuple[int, int]:
    """`(bytes, entries)` under `path`, tolerant of files appearing and vanishing mid-walk (a live
    download). Bytes are the larger of apparent size and allocated blocks (`st_blocks * 512`)."""
    total = entries = 0
    for dirpath, dirs, names in os.walk(path):
        entries += len(dirs)
        for name in names:
            entries += 1
            with contextlib.suppress(OSError):
                st = (Path(dirpath) / name).lstat()
                total += max(st.st_size, st.st_blocks * 512)
    return total, entries


def _died(proc: Any) -> ModelFetchError:
    """The fetch process is gone without a protocol message. Exit code `SELF_TEST_EXIT_CODE` means
    its sandbox self-test failed: this Mac cannot confine a fetch at all."""
    if proc.exitcode == seatbelt.SELF_TEST_EXIT_CODE:
        return ModelFetchError(
            seatbelt.SANDBOX_FAILED, "sandbox self-test failed", release=True, local=True
        )
    return ModelFetchError(MODEL_DOWNLOAD_FAILED, "fetch process died", release=True)


def _rmtree(path: Path) -> None:
    """Remove `path` quickly out of the namespace first so readers never see a half-deleted dir."""
    trash = path.with_name(f".trash-{uuid.uuid4().hex}")
    try:
        path.rename(trash)
    except FileNotFoundError:
        return
    shutil.rmtree(trash, ignore_errors=True)


def _has_key(obj: Any, key: str) -> bool:
    if isinstance(obj, dict):
        return key in obj or any(_has_key(v, key) for v in obj.values())
    if isinstance(obj, list):
        return any(_has_key(v, key) for v in obj)
    return False


class ModelCache:
    """Pinned, verified, LRU-bounded on-disk cache of Hugging Face model snapshots.

    Layout: `<root>/hf/<org>--<name>/<revision>/` with `.minion-manifest.json`. Mutations are
    serialised by an `fcntl.flock` on `<root>/.lock` (plus an in-process asyncio lock). The
    most recently ensured model is the "in-use" one: this process keeps a shared `flock` on its
    manifest, so neither this nor another executor process sharing the cache evicts it.
    """

    def __init__(
        self,
        root: Path,
        cap_bytes: int,
        *,
        fetch_env: FetchEnv | None = None,
        fetch: FetchFn = _hf_fetch,
        download_timeout_s: float = 3600.0,
        mp_context: str = "spawn",
        min_free_bytes: int = DEFAULT_MIN_FREE_BYTES,
        nice: int = 0,
        sandbox: bool = True,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._root = root
        self._cap = cap_bytes
        self._fetch_env = fetch_env or FetchEnv()
        self._fetch = fetch
        self._timeout_s = download_timeout_s
        self._ctx: Any = mp.get_context(mp_context)
        self._alock = asyncio.Lock()
        self._in_use: Path | None = None
        self._pin_fd: int | None = None
        self._min_free = min_free_bytes
        self._nice = nice
        self._sandbox = sandbox
        self._clock = clock
        # (repo, revision) -> (expires_at, code, message); bounded, oldest first
        self._local: collections.OrderedDict[tuple[str, str], tuple[float, str, str]] = (
            collections.OrderedDict()
        )

    @property
    def root(self) -> Path:
        return self._root

    def path_for(self, model: ModelEntry) -> Path:
        return self._root / "hf" / model.ref.replace("/", "--") / _revision(model)

    # -- public API --

    async def ensure(self, model: ModelEntry) -> Path | None:
        """Local snapshot directory for `model`, fetching it if needed. `None` for engines
        that manage their own store (Ollama pulls by tag; fake has no weights)."""
        if model.engine not in (Engine.MLX, Engine.HF):
            return None
        final = self.path_for(model)
        key = (model.ref, _revision(model))
        known = self._recall(key)
        if known is not None and not self.verify(final):
            log.info("model_cache_local_failure_recalled", repo=model.ref, code=known[0])
            raise ModelFetchError(known[0], known[1], release=True, local=True)
        try:
            async with self._alock:
                self._root.mkdir(parents=True, exist_ok=True, mode=0o700)
                async with self._flock():
                    await asyncio.to_thread(self._sweep)
                    if self.verify(final):
                        self.touch(final)
                    else:
                        if final.exists():
                            log.warning("model_cache_entry_invalid", repo=model.ref)
                            await asyncio.to_thread(_rmtree, final)
                        await self._fetch_into(model, final)
                    self._pin(final)
        except ModelFetchError as exc:
            if exc.local:
                self._remember(key, exc)
            raise
        return final

    def release_in_use(self) -> None:
        """Forget the protected in-use model (call after the worker holding it was unloaded)
        and drop the cross-process pin."""
        self._in_use = None
        self._unpin()

    def verify(self, path: Path) -> bool:
        """Cheap check: manifest present and every listed file exists with the recorded size."""
        manifest = self._read_manifest(path)
        if manifest is None:
            return False
        try:
            for rel, meta in manifest["files"].items():
                if not allowed_file(rel):
                    return False
                f = path / rel
                if f.is_symlink() or f.stat().st_size != meta["size"]:
                    return False
        except (OSError, KeyError, TypeError):
            return False
        return True

    def touch(self, path: Path) -> None:
        with contextlib.suppress(OSError):
            os.utime(path / MANIFEST_NAME)

    def used_bytes(self) -> int:
        return sum(size for _p, size, _m in self._entries())

    def evict_for(self, needed: int, keep: Path | None = None) -> list[Path]:
        """Delete least-recently-used entries until `needed` more bytes fit under the cap.

        Never deletes `keep`, nor an entry some process pins (shared `flock` on its manifest).
        Raises `MODEL_CACHE_FULL` (deleting nothing) if that is impossible.
        """
        entries = sorted(self._entries(), key=lambda e: e[2])  # oldest manifest mtime first
        locks: dict[Path, int] = {}
        try:
            pinned = 0
            for path, size, _m in entries:
                if path == keep:
                    continue
                fd = self._try_exclusive(path)
                if fd is None:
                    pinned += size
                else:
                    locks[path] = fd
            kept = sum(s for p, s, _m in entries if p == keep)
            if needed + kept + pinned > self._cap:
                blocked = kept > 0 and needed + pinned <= self._cap
                log.error("model_cache_too_small", needed=needed, cap=self._cap, in_use=blocked)
                raise ModelFetchError(
                    MODEL_CACHE_FULL,
                    "model does not fit the configured cache size",
                    retryable=True,
                    # Nothing more to try on this Mac unless unloading our own model helps:
                    # then the caller retries; otherwise hand the subtask back.
                    release=not blocked,
                    blocked_by_in_use=blocked,
                    local=not blocked and pinned == 0,
                )
            # Plan first, check the disk with what the plan would free, delete only if it fits.
            used = sum(s for _p, s, _m in entries)
            plan: list[tuple[Path, int]] = []
            for path, size, _mtime in entries:
                if used + needed <= self._cap:
                    break
                if path not in locks:
                    continue
                used -= size
                plan.append((path, size))
            self._check_disk(needed, sum(size for _p, size in plan))
            evicted: list[Path] = []
            for path, _size in plan:
                _rmtree(path)
                evicted.append(path)
                log.info("model_cache_evicted", entry=path.parent.name)
            return evicted
        finally:
            for fd in locks.values():
                os.close(fd)

    # -- internals --

    def _recall(self, key: tuple[str, str]) -> tuple[str, str] | None:
        hit = self._local.get(key)
        if hit is None:
            return None
        if hit[0] <= self._clock():
            del self._local[key]
            return None
        return hit[1], hit[2]

    def _remember(self, key: tuple[str, str], exc: ModelFetchError) -> None:
        ttl = _DISK_FULL_TTL_S if exc.code == MODEL_CACHE_FULL else _LOCAL_TTL_S
        self._local.pop(key, None)
        self._local[key] = (self._clock() + ttl, exc.code, exc.message)
        while len(self._local) > _LOCAL_MEMORY_MAX:
            self._local.popitem(last=False)

    def _pin(self, path: Path) -> None:
        """Hold a shared flock on `path`'s manifest and drop the previous pin."""
        try:
            fd = os.open(path / MANIFEST_NAME, os.O_RDONLY)
        except OSError:
            self._unpin()
            self._in_use = None
            return
        try:
            fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            log.warning("model_cache_pin_failed")
            return
        self._unpin()
        self._pin_fd = fd
        self._in_use = path  # only once the cross-process pin is held

    def _unpin(self) -> None:
        if self._pin_fd is not None:
            with contextlib.suppress(OSError):
                os.close(self._pin_fd)
            self._pin_fd = None

    @staticmethod
    def _try_exclusive(entry: Path) -> int | None:
        try:
            fd = os.open(entry / MANIFEST_NAME, os.O_RDONLY)
        except OSError:
            return None
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            return None
        return fd

    def _sweep(self) -> None:
        """Under the root flock: remove leftovers of crashed fetches / half-done deletes."""
        shutil.rmtree(self._root / ".staging", ignore_errors=True)
        base = self._root / "hf"
        if base.is_dir():
            for trash in base.glob("*/.trash-*"):
                shutil.rmtree(trash, ignore_errors=True)

    def _read_manifest(self, path: Path) -> dict[str, Any] | None:
        try:
            data = json.loads((path / MANIFEST_NAME).read_text())
            if not isinstance(data, dict) or not isinstance(data["files"], dict):
                return None
            return data
        except (OSError, ValueError, KeyError):
            return None

    def _entries(self) -> list[tuple[Path, int, float]]:
        out: list[tuple[Path, int, float]] = []
        base = self._root / "hf"
        if not base.is_dir():
            return out
        for repo_dir in base.iterdir():
            if repo_dir.name.startswith(".") or not repo_dir.is_dir():
                continue
            for rev_dir in repo_dir.iterdir():
                if rev_dir.name.startswith("."):
                    continue
                m = rev_dir / MANIFEST_NAME
                try:
                    mtime = m.stat().st_mtime
                    size = sum(v["size"] for v in json.loads(m.read_text())["files"].values())
                except (OSError, ValueError, KeyError, TypeError):
                    continue
                out.append((rev_dir, int(size), mtime))
        return out

    @contextlib.asynccontextmanager
    async def _flock(self) -> Any:
        fd = os.open(self._root / ".lock", os.O_CREAT | os.O_RDWR, 0o600)
        try:
            while True:  # non-blocking + poll so a cancel is never stuck in a thread
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    await asyncio.sleep(0.05)
            yield
        finally:
            os.close(fd)  # closing releases the flock

    def _child_env(self, staging: Path) -> dict[str, str]:
        env = {k: os.environ[k] for k in _BASE_CHILD_NAMES if k in os.environ}
        env.update({k: v for k, v in os.environ.items() if k.startswith("LC_")})
        env.update(self._fetch_env.to_env())
        # Everything the hub client caches lives inside the staging dir (the only writable
        # place for the sandboxed child) and is deleted with it: nothing persists outside
        # sealed entries.
        env["HF_HOME"] = str(staging / ".hf-home")
        env["HF_XET_CACHE"] = str(staging / ".hf-home" / "xet")
        env["TMPDIR"] = str(staging / ".tmp")
        env["HF_XET_CHUNK_CACHE_SIZE_BYTES"] = "0"
        env["HF_HUB_DISABLE_TELEMETRY"] = "1"
        env["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
        return env

    def _confine_spec(self, staging: Path) -> dict[str, Any] | None:
        if not self._sandbox:
            return None
        read: list[str] = seatbelt.python_read_paths()
        read_files: list[str] = []
        for name in _CA_NAMES:
            value = self._fetch_env.values.get(name)
            raw = value.get_secret_value() if value is not None else ""
            if not raw:
                continue
            if name == "SSL_CERT_DIR":
                # A directory under HOME would expose everything next to the certs.
                for part in raw.split(os.pathsep):
                    if not part:
                        continue
                    real = os.path.realpath(part)
                    if any(seatbelt._under(real, h) for h in seatbelt.home_dirs()):
                        raise ModelFetchError(
                            MODEL_DOWNLOAD_FAILED,
                            "SSL_CERT_DIR under the home directory is not supported; "
                            "use SSL_CERT_FILE",
                            release=True,
                            local=True,
                        )
                    read.append(real)
            else:  # a bundle: grant exactly that file
                read_files.append(raw)
        return {
            "read": read,
            "read_files": read_files,
            "write": [str(staging)],
            "engines": ["fetch"],
            "network": True,
            "net_tcp": self._fetch_tcp(),
            "metal": False,
            "mach_extra": list(FETCH_MACH_ALLOW),
        }

    def _fetch_tcp(self) -> list[str]:
        """Outbound TCP the fetch child may open. Direct: HTTPS (the hub endpoint's port, 443 by
        default) to anywhere. With a proxy (`HTTPS_PROXY` / `ALL_PROXY`; `HTTP_PROXY` is not used
        for https): only the proxy's `*:<port>`, nothing else. Loopback stays denied unless the
        proxy itself is local."""
        proxies: list[str] = []
        for name in ("HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"):
            value = self._fetch_env.values.get(name)
            if value is None:
                continue
            raw = value.get_secret_value()
            try:
                u = urlsplit(raw if "://" in raw else "http://" + raw)
                port = u.port or {"https": 443, "socks5": 1080, "socks5h": 1080}.get(u.scheme, 80)
                host = (u.hostname or "").lower()
            except ValueError:
                continue
            local = host in ("localhost", "127.0.0.1", "::1")
            proxies.append(f"{'localhost' if local else '*'}:{port}")
        no_proxy = any(self._fetch_env.values.get(n) for n in ("NO_PROXY", "no_proxy"))
        if proxies and not no_proxy:
            return list(dict.fromkeys(proxies))
        ports = [443]
        endpoint = self._fetch_env.values.get("HF_ENDPOINT")
        if endpoint is not None:
            with contextlib.suppress(ValueError):
                ports.append(urlsplit(endpoint.get_secret_value()).port or 443)
        direct = [f"*:{p}" for p in dict.fromkeys(ports)]
        return list(dict.fromkeys(proxies + direct))

    async def _fetch_into(self, model: ModelEntry, final: Path) -> None:
        staging_root = self._root / ".staging"
        staging = staging_root / uuid.uuid4().hex
        staging.mkdir(parents=True, mode=0o700)
        (staging / ".tmp").mkdir(mode=0o700)
        spec = FetchSpec(model.ref, _revision(model), str(staging), self._child_env(staging))
        cancel = threading.Event()
        try:
            plan = await self._run_child(spec, final)
            await asyncio.to_thread(self._verify_and_seal, staging, plan, model, cancel)
            final.parent.mkdir(parents=True, exist_ok=True)
            staging.rename(final)
        except OSError as exc:
            if _is_enospc(exc):
                log.error("model_cache_disk_full", repo=model.ref)
                raise ModelFetchError(
                    MODEL_CACHE_FULL, "disk full", release=True, local=True
                ) from None
            raise
        finally:
            cancel.set()  # a hashing thread still running after a cancel stops at its next chunk
            await asyncio.to_thread(shutil.rmtree, staging_root, True)

    def _reserve(self, total: int) -> int:
        return max(self._min_free, int(total * _DISK_RESERVE_FRACTION))

    def _check_disk(self, needed: int, planned_bytes: int = 0) -> None:
        usage = shutil.disk_usage(self._root)
        reserve = self._reserve(usage.total)
        if usage.free + planned_bytes - needed < reserve:
            log.error("model_cache_disk_reserve", needed=needed, free=usage.free, reserve=reserve)
            raise ModelFetchError(
                MODEL_CACHE_FULL, "not enough free disk space", release=True, local=True
            )

    async def _run_child(self, spec: FetchSpec, final: Path) -> dict[str, list[Any]]:
        parent, child = self._ctx.Pipe()
        proc = self._ctx.Process(
            target=_child_main,
            args=(
                self._fetch,
                spec,
                child,
                os.getpid(),
                self._nice,
                self._confine_spec(Path(spec.staging)),
            ),
            daemon=True,
        )
        proc.start()
        child.close()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._timeout_s
        plan: dict[str, list[Any]] | None = None
        # Before the plan arrives only the margin is allowed (the child just lists the hub).
        limit = _STAGING_MARGIN_BYTES
        limit_base = 0
        last_size_check = loop.time()
        try:
            while True:
                if loop.time() > deadline:
                    raise ModelFetchError(MODEL_DOWNLOAD_FAILED, "download timed out", release=True)
                if not parent.poll(0):
                    if not proc.is_alive() and not parent.poll(0):
                        raise _died(proc)
                    if loop.time() - last_size_check >= _STAGING_POLL_S:
                        last_size_check = loop.time()
                        usage = await asyncio.to_thread(shutil.disk_usage, self._root)
                        # Normal overshoot (the margin the staging limit allows) must not trip it.
                        if usage.free < self._reserve(usage.total) - _staging_margin(limit_base):
                            # unlinked-but-open files and fast writes show up here, not in a walk
                            log.error("model_staging_disk_reserve", repo=spec.repo)
                            raise ModelFetchError(
                                MODEL_CACHE_FULL,
                                "not enough free disk space",
                                release=True,
                                local=True,
                            )
                        size, entries = await asyncio.to_thread(_staging_usage, Path(spec.staging))
                        if size > limit or entries > _STAGING_MAX_ENTRIES:
                            log.error("model_staging_overflow", repo=spec.repo)
                            raise ModelFetchError(
                                MODEL_INTEGRITY, "download exceeds the hub listing", release=True
                            )
                    await asyncio.sleep(0.05)
                    continue
                try:
                    msg = parent.recv()
                except (EOFError, OSError):
                    await asyncio.to_thread(proc.join, 1)  # let the exit code settle
                    raise _died(proc) from None
                kind = msg[0]
                if kind == "plan":
                    plan = msg[2]
                    needed = int(msg[1])
                    await asyncio.to_thread(self.evict_for, needed, self._in_use)
                    parent.send("go")
                    limit_base = needed
                    limit = needed + _staging_margin(needed)
                    last_size_check = loop.time()
                elif kind == "done":
                    if plan is None:
                        raise ModelFetchError(MODEL_DOWNLOAD_FAILED, "protocol error")
                    return plan
                elif kind == "err":
                    code, message, retryable, release = msg[1:5]
                    local = bool(msg[5]) if len(msg) > 5 else False
                    raise ModelFetchError(
                        str(code)[:64],
                        str(message)[:200],
                        retryable=retryable,
                        release=release,
                        local=local,
                    )
        finally:  # also on CancelledError: stop/cancel must kill the download (invariant 6)
            parent.close()
            if proc.is_alive():
                proc.kill()
            proc.join(5)

    def _verify_and_seal(
        self,
        staging: Path,
        plan: dict[str, list[Any]],
        model: ModelEntry,
        cancel: threading.Event,
    ) -> None:
        for bookkeeping in (".cache", ".hf-home", ".tmp"):  # hub client scratch, never an entry
            shutil.rmtree(staging / bookkeeping, ignore_errors=True)
        found: dict[str, Path] = {}
        for rel, p in _tree_files(staging):
            if p.is_symlink():
                raise ModelFetchError(MODEL_INTEGRITY, "unexpected symlink")
            if not allowed_file(rel):
                raise ModelFetchError(MODEL_UNSUPPORTED, "disallowed file", retryable=False)
            found[rel] = p
        if set(found) != set(plan):
            raise ModelFetchError(MODEL_INTEGRITY, "downloaded files differ from hub listing")
        files: dict[str, dict[str, Any]] = {}
        for rel, p in found.items():
            size, want, *rest = plan[rel]
            blob_id = rest[0] if rest else None
            if want is None and blob_id is None:  # nothing from the hub to check against
                raise ModelFetchError(MODEL_INTEGRITY, "no digest for file")
            digest, blob = _hash_file(p, cancel, git_blob=want is None)
            if (
                (size is not None and p.stat().st_size != size)
                or (want is not None and digest != want)
                or (want is None and blob != blob_id)
            ):
                log.error("model_integrity_failed", repo=model.ref, file=rel)
                raise ModelFetchError(MODEL_INTEGRITY, "sha256/size mismatch")
            files[rel] = {"size": p.stat().st_size, "sha256": digest}
        for rel, p in found.items():
            if not rel.endswith(".json"):
                continue
            if cancel.is_set():
                raise _HashCancelledError
            try:
                has_auto_map = _has_key(json.loads(p.read_text()), "auto_map")
            except (OSError, ValueError):
                raise ModelFetchError(MODEL_UNSUPPORTED, "bad json", retryable=False) from None
            if has_auto_map:  # would need trust_remote_code
                raise ModelFetchError(
                    MODEL_UNSUPPORTED, "model requires remote code", retryable=False
                )
        manifest = {"repo": model.ref, "revision": model.revision, "files": files}
        (staging / MANIFEST_NAME).write_text(json.dumps(manifest))
        log.info("model_cached", repo=model.ref, revision=model.revision, files=len(files))


def default_cache_dir() -> Path:
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Caches" / "open-minions" / "models"
    base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    return Path(base) / "open-minions" / "models"
