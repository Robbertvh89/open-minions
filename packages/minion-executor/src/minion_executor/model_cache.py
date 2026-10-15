"""Local model cache and the killable fetch subprocess (TASK-4.2 S4, D5-D7).

Weights are downloaded by the *control side* so the inference worker can stay offline:

* The download runs in a short-lived `spawn` subprocess (not an inference process). Cancelling
  `ModelCache.ensure` (subtask cancel, SIGTERM/stop) kills it, so shutdown never waits on a
  download thread (invariant 6).
* Only the pinned `ModelEntry.revision` is fetched, and only files matching `SAFE_PATTERNS`
  (safetensors, json, tokenizer files); never `*.py`, `*.bin`, `*.pt`, `*.pkl` or `*.gguf`.
* The child reports the file list from the hub's `files_metadata` (size + LFS sha256). The
  *parent* re-hashes every downloaded file against it, so verification does not depend on the
  child. Non-LFS files (small json) carry no sha256 on the hub: their size is checked and their
  sha256 is recorded in the manifest.
* `HF_TOKEN`, proxy and CA settings (`FetchEnv`) reach only the fetch subprocess, never the
  worker, and are never logged.
"""

from __future__ import annotations

import asyncio
import contextlib
import fcntl
import fnmatch
import hashlib
import json
import multiprocessing as mp
import os
import shutil
import sys
import uuid
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

import structlog
from minion_common.enums import Engine
from minion_common.schemas import ModelEntry
from pydantic import SecretStr

from minion_executor.runners.base import RunnerError

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
MODEL_INTEGRITY = "MODEL_INTEGRITY"
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
_BASE_CHILD_NAMES = ("PATH", "HOME", "TMPDIR", "LANG")


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
    ) -> None:
        super().__init__(code, message, retryable=retryable)
        self.release = release
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
#   child  -> ("plan", total_bytes, {rel: [size, sha256 | None]})
#   parent -> "go"          (after making room; anything else / a kill aborts)
#   child  -> ("done",)     (files are in spec.staging)
#   child  -> ("err", code, message, retryable, release)   at any point
FetchFn = Callable[[FetchSpec, Connection], None]


def _send_err(conn: Connection, exc: BaseException) -> None:
    if isinstance(exc, ModelFetchError):
        conn.send(("err", exc.code, exc.message, exc.retryable, exc.release))
        return
    # Hub verdict (HTTP status) -> retryable failure; anything else is network/local -> release.
    status = getattr(getattr(exc, "response", None), "status_code", None)
    if status is not None:
        conn.send(("err", MODEL_DOWNLOAD_FAILED, f"hub error {status}", True, False))
    else:
        conn.send(("err", MODEL_DOWNLOAD_FAILED, type(exc).__name__, True, True))


def _child_main(fetch: FetchFn, spec: FetchSpec, conn: Connection) -> None:
    os.environ.clear()
    os.environ.update(spec.env)
    try:
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
            files[sib.rfilename] = [sib.size, sha]
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


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(_HASH_CHUNK):
            h.update(chunk)
    return h.hexdigest()


def _tree_files(root: Path) -> Iterator[tuple[str, Path]]:
    for dirpath, _dirs, names in os.walk(root):
        for name in names:
            p = Path(dirpath) / name
            yield p.relative_to(root).as_posix(), p


def _dir_bytes(path: Path) -> int:
    return sum(p.stat().st_size for _rel, p in _tree_files(path) if not p.is_symlink())


def _rmtree(path: Path) -> None:
    """Remove `path` quickly out of the namespace first so readers never see a half-deleted dir."""
    trash = path.with_name(f".trash-{uuid.uuid4().hex}")
    try:
        path.rename(trash)
    except FileNotFoundError:
        return
    shutil.rmtree(trash, ignore_errors=True)


class ModelCache:
    """Pinned, verified, LRU-bounded on-disk cache of Hugging Face model snapshots.

    Layout: `<root>/hf/<org>--<name>/<revision>/` with `.minion-manifest.json`. Mutations are
    serialised by an `fcntl.flock` on `<root>/.lock` (plus an in-process asyncio lock). The
    most recently ensured model is the "in-use" one and is never evicted.
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
    ) -> None:
        self._root = root
        self._cap = cap_bytes
        self._fetch_env = fetch_env or FetchEnv()
        self._fetch = fetch
        self._timeout_s = download_timeout_s
        self._ctx: Any = mp.get_context(mp_context)
        self._alock = asyncio.Lock()
        self._in_use: Path | None = None

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
        async with self._alock:
            self._root.mkdir(parents=True, exist_ok=True, mode=0o700)
            async with self._flock():
                if self.verify(final):
                    self.touch(final)
                else:
                    if final.exists():
                        log.warning("model_cache_entry_invalid", repo=model.ref)
                        await asyncio.to_thread(_rmtree, final)
                    await self._fetch_into(model, final)
                self._in_use = final
        return final

    def release_in_use(self) -> None:
        """Forget the protected in-use model (call after the worker holding it was unloaded)."""
        self._in_use = None

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

        Never deletes `keep`. Raises `MODEL_CACHE_FULL` (deleting nothing) if that is impossible.
        """
        entries = sorted(self._entries(), key=lambda e: e[2])  # oldest manifest mtime first
        protected = sum(s for p, s, _m in entries if p == keep)
        if needed + protected > self._cap:
            blocked = protected > 0 and needed <= self._cap
            log.error("model_cache_too_small", needed=needed, cap=self._cap, in_use=blocked)
            raise ModelFetchError(
                MODEL_CACHE_FULL,
                "model does not fit the configured cache size",
                retryable=True,
                blocked_by_in_use=blocked,
            )
        used = sum(s for _p, s, _m in entries)
        evicted: list[Path] = []
        for path, size, _mtime in entries:
            if used + needed <= self._cap:
                break
            if path == keep:
                continue
            _rmtree(path)
            used -= size
            evicted.append(path)
            log.info("model_cache_evicted", entry=path.parent.name)
        return evicted

    # -- internals --

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
            if not repo_dir.is_dir():
                continue
            for rev_dir in repo_dir.iterdir():
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

    def _child_env(self, root: Path) -> dict[str, str]:
        env = {k: os.environ[k] for k in _BASE_CHILD_NAMES if k in os.environ}
        env.update({k: v for k, v in os.environ.items() if k.startswith("LC_")})
        env.update(self._fetch_env.to_env())
        env["HF_HOME"] = str(root / ".hf-home")
        env["HF_HUB_DISABLE_TELEMETRY"] = "1"
        env["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
        return env

    async def _fetch_into(self, model: ModelEntry, final: Path) -> None:
        staging_root = self._root / ".staging"
        shutil.rmtree(staging_root, ignore_errors=True)  # under the lock: all leftovers are stale
        staging = staging_root / uuid.uuid4().hex
        staging.mkdir(parents=True, mode=0o700)
        spec = FetchSpec(model.ref, _revision(model), str(staging), self._child_env(self._root))
        try:
            plan = await self._run_child(spec, final)
            await asyncio.to_thread(self._verify_and_seal, staging, plan, model)
            final.parent.mkdir(parents=True, exist_ok=True)
            staging.rename(final)
        finally:
            await asyncio.to_thread(shutil.rmtree, staging_root, True)

    async def _run_child(self, spec: FetchSpec, final: Path) -> dict[str, list[Any]]:
        parent, child = self._ctx.Pipe()
        proc = self._ctx.Process(target=_child_main, args=(self._fetch, spec, child), daemon=True)
        proc.start()
        child.close()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._timeout_s
        plan: dict[str, list[Any]] | None = None
        try:
            while True:
                if loop.time() > deadline:
                    raise ModelFetchError(MODEL_DOWNLOAD_FAILED, "download timed out", release=True)
                if not parent.poll(0):
                    if not proc.is_alive() and not parent.poll(0):
                        raise ModelFetchError(
                            MODEL_DOWNLOAD_FAILED, "fetch process died", release=True
                        )
                    await asyncio.sleep(0.05)
                    continue
                try:
                    msg = parent.recv()
                except (EOFError, OSError):
                    raise ModelFetchError(
                        MODEL_DOWNLOAD_FAILED, "fetch process died", release=True
                    ) from None
                kind = msg[0]
                if kind == "plan":
                    plan = msg[2]
                    self.evict_for(int(msg[1]), keep=self._in_use)
                    parent.send("go")
                elif kind == "done":
                    if plan is None:
                        raise ModelFetchError(MODEL_DOWNLOAD_FAILED, "protocol error")
                    return plan
                elif kind == "err":
                    _, code, message, retryable, release = msg
                    raise ModelFetchError(
                        str(code)[:64], str(message)[:200], retryable=retryable, release=release
                    )
        finally:  # also on CancelledError: stop/cancel must kill the download (invariant 6)
            parent.close()
            if proc.is_alive():
                proc.kill()
            proc.join(5)

    def _verify_and_seal(
        self, staging: Path, plan: dict[str, list[Any]], model: ModelEntry
    ) -> None:
        shutil.rmtree(staging / ".cache", ignore_errors=True)  # huggingface_hub bookkeeping
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
            size, want = plan[rel]
            digest = _sha256(p)
            if (size is not None and p.stat().st_size != size) or (
                want is not None and digest != want
            ):
                log.error("model_integrity_failed", repo=model.ref, file=rel)
                raise ModelFetchError(MODEL_INTEGRITY, "sha256/size mismatch")
            files[rel] = {"size": p.stat().st_size, "sha256": digest}
        cfg = found.get("config.json")
        if cfg is not None:
            try:
                has_auto_map = "auto_map" in json.loads(cfg.read_text())
            except (OSError, ValueError):
                raise ModelFetchError(
                    MODEL_UNSUPPORTED, "bad config.json", retryable=False
                ) from None
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
