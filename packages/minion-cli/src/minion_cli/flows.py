"""Submit / wait / fetch flows (async, no CLI). Never prints or logs line content."""

from __future__ import annotations

import asyncio
import fcntl
import json
import math
import os
import random
import re
import shutil
import tempfile
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from minion_common.client import ApiError, UserClient
from minion_common.enums import SubtaskStatus, TaskStatus, TaskType
from minion_common.schemas import SubtaskOut, SubtaskPage, TaskCounts, TaskCreate, TaskOut
from pydantic import ValidationError

from minion_cli.errors import EXIT_UNREACHABLE, EXIT_USAGE, CliError, clean
from minion_cli.settings import CliSettings
from minion_cli.store import ObjectInfo, ObjectStore, md5_file, plain_md5, sha256_file

TERMINAL = frozenset(
    {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.PARTIAL, TaskStatus.CANCELLED}
)
POLL_START_S = 2.0
POLL_FACTOR = 1.5
POLL_MAX_S = 30.0
MAX_TRANSIENT = 10
MAX_PAGE = 100  # server cap on `limit` for GET /tasks/.../subtasks
MAX_INPUT_FILES = 9999  # keys are `{i:04d}-name`
_PART_RE = re.compile(r"^(\d{6,})\.jsonl$")
_UNSAFE = re.compile(r"[^A-Za-z0-9._-]")


@dataclass(frozen=True)
class Target:
    team: str
    project: str
    name: str

    def input_prefix(self, s: CliSettings) -> str:
        return f"{s.input_prefix}{self.team}/{self.project}/{self.name}/"

    def output_prefix(self, s: CliSettings) -> str:
        return f"{s.output_prefix}{self.team}/{self.project}/{self.name}/"


def slugify(stem: str) -> str:
    slug = re.sub(r"[^a-z0-9_-]+", "-", stem.lower()).strip("-_")[:40].strip("-_")
    return slug or "task"


def default_name(first_file: Path, now: datetime | None = None) -> str:
    stamp = (now or datetime.now(UTC)).strftime("%Y%m%d%H%M%S")
    return f"{slugify(first_file.stem)}-{stamp}"


def api_failure(exc: Exception) -> CliError:
    """Map a server/transport failure to a CliError (no request bodies, no secrets)."""
    if isinstance(exc, ApiError):
        if exc.status_code in (401, 403):
            return CliError(f"server refused the API key ({exc.status_code})", EXIT_USAGE)
        if exc.status_code >= 500:
            return CliError(f"server error: {clean(exc)}", EXIT_UNREACHABLE)
        return CliError(f"server rejected the request: {clean(exc)}", EXIT_USAGE)
    return CliError(f"server unreachable: {type(exc).__name__}", EXIT_UNREACHABLE)


# ---- upload / submit ----------------------------------------------------------------------


def _file_sizes(files: list[Path]) -> list[int]:
    sizes: list[int] = []
    for f in files:
        if not f.is_file():
            raise CliError(f"not a file: {f.name}", EXIT_USAGE)
        sizes.append(f.stat().st_size)
    return sizes


async def _same_content(store: ObjectStore, key: str, path: Path) -> bool:
    info = await store.head(key)
    if info is None:
        return False
    if info.sha256 is not None:
        return info.sha256 == await asyncio.to_thread(sha256_file, path)
    md5 = plain_md5(info.etag)
    return md5 is not None and md5 == await asyncio.to_thread(md5_file, path)


async def upload_inputs(
    store: ObjectStore, settings: CliSettings, target: Target, files: list[Path]
) -> str:
    """Upload `files` under the task's input prefix (argv order) and return the input URI.

    Resumes when the prefix holds only our own keys whose content matches the local file (sha256
    metadata we set at upload, else a plain-md5 ETag); any other object there is foreign or
    different and aborts (exit 2) rather than being mixed into the task input.
    """
    if not files:
        raise CliError("no input files", EXIT_USAGE)
    if len(files) > MAX_INPUT_FILES:
        raise CliError(f"too many input files ({len(files)} > {MAX_INPUT_FILES})", EXIT_USAGE)
    sizes = await asyncio.to_thread(_file_sizes, files)
    prefix = target.input_prefix(settings)
    expected = {
        f"{prefix}{i:04d}-{_UNSAFE.sub('_', f.name)}": size
        for i, (f, size) in enumerate(zip(files, sizes, strict=True))
    }
    if len(expected) != len(files):  # pragma: no cover - index prefix makes keys unique
        raise CliError("duplicate input keys", EXIT_USAGE)
    existing = {o.key: o.size for o in await store.list_objects(prefix)}
    local = dict(zip(expected, files, strict=True))
    for key, size in existing.items():
        if expected.get(key) != size:
            raise CliError(
                f"input prefix {prefix!r} already holds other objects; choose another --name",
                EXIT_USAGE,
            )
        if not await _same_content(store, key, local[key]):
            raise CliError(
                f"input object {key.rsplit('/', 1)[-1]!r} differs from the local file; "
                "choose another --name",
                EXIT_USAGE,
            )
    for key, f in local.items():
        if key not in existing:
            await store.upload_file(f, key, sha256=await asyncio.to_thread(sha256_file, f))
    return f"s3://{settings.s3_bucket}/{prefix}"


async def submit_task(
    client: UserClient,
    store: ObjectStore,
    settings: CliSettings,
    target: Target,
    files: list[Path],
    *,
    type: TaskType,
    model: str,
    params: dict[str, Any] | None = None,
    records_per_subtask: int = 500,
    priority: int = 0,
    allowed_pool: str | None = None,
) -> TaskOut:
    try:
        await client.get_task(target.team, target.project, target.name)
    except ApiError as exc:
        if exc.status_code != 404:
            raise api_failure(exc) from None
    except httpx.HTTPError as exc:
        raise api_failure(exc) from None
    else:
        raise CliError(
            f"task {target.name!r} already exists; use `minion fetch {target.name}` or --name",
            EXIT_USAGE,
        )
    # Validate the request before uploading anything (the input URI is deterministic).
    try:
        req = TaskCreate(
            team=target.team,
            project=target.project,
            task_name=target.name,
            type=type,
            model=model,
            input_uri=f"s3://{settings.s3_bucket}/{target.input_prefix(settings)}",
            output_uri=f"s3://{settings.s3_bucket}/{target.output_prefix(settings)}",
            params=params or {},
            records_per_subtask=records_per_subtask,
            priority=priority,
            allowed_pool=allowed_pool,
        )
    except ValidationError as exc:
        # Error text can embed the offending value; report field names only.
        fields = ", ".join(sorted({str(e["loc"][0]) for e in exc.errors() if e["loc"]}))
        raise CliError(f"invalid task request ({fields or 'see options'})", EXIT_USAGE) from None
    input_uri = await upload_inputs(store, settings, target, files)
    if input_uri != req.input_uri:  # pragma: no cover - same expression
        raise CliError("input URI mismatch", EXIT_USAGE)
    try:
        return await client.create_task(req)
    except (ApiError, httpx.HTTPError) as exc:
        raise api_failure(exc) from None


# ---- wait ---------------------------------------------------------------------------------


def _progress_key(t: TaskOut) -> tuple[object, ...]:
    return (t.status, *t.counts.model_dump().values())


async def wait_task(
    client: UserClient,
    target: Target,
    *,
    timeout_s: float | None = None,
    on_progress: Callable[[TaskOut], None] | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    clock: Callable[[], float] = time.monotonic,
    jitter: Callable[[], float] = lambda: random.uniform(0.9, 1.1),  # noqa: S311
) -> TaskOut:
    """Poll until the task is terminal. Backoff 2 s x1.5 up to 30 s; resets on progress."""
    deadline = None if timeout_s is None else clock() + timeout_s
    delay = POLL_START_S
    last: tuple[object, ...] | None = None
    transient = 0
    while True:
        wait = delay * jitter()
        try:
            task = await client.get_task(target.team, target.project, target.name)
        except ApiError as exc:
            if exc.status_code == 429:
                wait = max(exc.retry_after or 0.0, wait)
            elif exc.status_code >= 500:
                transient += 1
                delay = min(delay * POLL_FACTOR, POLL_MAX_S)
            else:
                raise api_failure(exc) from None
            if transient > MAX_TRANSIENT:
                raise api_failure(exc) from None
        except httpx.HTTPError as exc:
            transient += 1
            delay = min(delay * POLL_FACTOR, POLL_MAX_S)
            if transient > MAX_TRANSIENT:
                raise api_failure(exc) from None
        else:
            transient = 0
            key = _progress_key(task)
            if key != last:
                last, delay = key, POLL_START_S
                if on_progress is not None:
                    on_progress(task)
            if task.status in TERMINAL:
                return task
            delay = min(delay * POLL_FACTOR, POLL_MAX_S)
            wait = delay * jitter()
        if deadline is not None:
            remaining = deadline - clock()
            if remaining <= 0:  # the poll above was the last one
                raise CliError("timed out waiting for the task", EXIT_UNREACHABLE)
            wait = min(wait, remaining)
        await sleep(wait)


# ---- fetch --------------------------------------------------------------------------------


@dataclass(frozen=True)
class FetchResult:
    parts: int
    downloaded: int
    records: int
    not_succeeded: int
    results_path: Path
    manifest_path: Path


async def _retrying[T](
    call: Callable[[], Awaitable[T]],
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    """Run a server call, retrying 5xx / transport errors / 429 like `wait_task` does."""
    delay = POLL_START_S
    for attempt in range(MAX_TRANSIENT + 1):
        wait = delay
        try:
            return await call()
        except ApiError as exc:
            if exc.status_code == 429:
                wait = max(exc.retry_after or 0.0, wait)
            elif exc.status_code < 500:
                raise api_failure(exc) from None
            if attempt == MAX_TRANSIENT:
                raise api_failure(exc) from None
        except httpx.HTTPError as exc:
            if attempt == MAX_TRANSIENT:
                raise api_failure(exc) from None
        delay = min(delay * POLL_FACTOR, POLL_MAX_S)
        await sleep(wait)
    raise AssertionError("unreachable")  # pragma: no cover


async def _list_all_subtasks(
    client: UserClient,
    target: Target,
    total: int,
    sleep: Callable[[float], Awaitable[None]],
) -> list[SubtaskOut]:
    """All subtasks, bounded by the task's `counts.total`, a page cap and cursor-cycle detection."""
    items: dict[int, SubtaskOut] = {}
    seen: set[str] = set()
    cursor: str | None = None
    max_pages = math.ceil(total / MAX_PAGE) + 2
    for _ in range(max_pages):

        async def one(c: str | None = cursor) -> SubtaskPage:
            return await client.list_subtasks(
                target.team, target.project, target.name, cursor=c, limit=MAX_PAGE
            )

        page = await _retrying(one, sleep)
        for st in page.items:
            items[st.subtask_id] = st
        cursor = page.next_cursor
        if cursor is None or len(items) >= total:
            return sorted(items.values(), key=lambda s: s.subtask_id)
        if cursor in seen:
            raise CliError("server pagination loops (repeated cursor)", EXIT_UNREACHABLE)
        seen.add(cursor)
    raise CliError("server returned more subtask pages than expected", EXIT_UNREACHABLE)


def _size(p: Path) -> int | None:
    try:
        return p.stat().st_size
    except FileNotFoundError:
        return None


def _atomic_write(dest: Path, write: Callable[[Any], None]) -> None:
    """Write via a 0600 temp file in the same directory, then rename over `dest`."""
    fd, tmp = tempfile.mkstemp(dir=dest.parent, prefix=f".{dest.name}.")
    try:
        with os.fdopen(fd, "wb") as out:
            write(out)
        Path(tmp).replace(dest)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def _merge(parts: list[Path], dest: Path) -> None:
    def write(out: Any) -> None:
        for p in parts:
            with p.open("rb") as f:
                shutil.copyfileobj(f, out)
                if f.tell() > 0:
                    f.seek(-1, os.SEEK_END)
                    if f.read(1) != b"\n":
                        out.write(b"\n")

    _atomic_write(dest, write)


def _write_manifest(path: Path, doc: dict[str, Any]) -> None:
    data = (json.dumps(doc, indent=2, sort_keys=True) + "\n").encode()
    _atomic_write(path, lambda out: out.write(data))


def _identity(target: Target, task: TaskOut) -> dict[str, str]:
    return {
        "team": target.team,
        "project": target.project,
        "task_name": target.name,
        "output_uri": task.output_uri,
        "created_at": task.created_at.astimezone(UTC).isoformat(),
    }


_LOCK_NAME = ".minion.lock"
_STALE_TMP = ".*.jsonl.*"


def _check_claim(out_dir: Path, identity: dict[str, str]) -> dict[int, dict[str, Any]] | None:
    """Prior part records if `out_dir` already belongs to this task, None if it is free to claim.

    Refuses (exit 2) another task's dir, an unreadable manifest, a `parts/` without manifest and
    a non-empty dir without manifest (e.g. ~/Desktop).
    """
    manifest = out_dir / "manifest.json"
    if manifest.exists():
        try:
            doc = json.loads(manifest.read_text())
            if not isinstance(doc, dict):
                raise ValueError
            if any(doc.get(k) != v for k, v in identity.items()):
                raise CliError(
                    f"{out_dir} holds the results of another task; use another -o", EXIT_USAGE
                )
            parts = doc.get("parts", [])
            if not isinstance(parts, list):
                raise ValueError
            return {int(p["subtask_id"]): p for p in parts if isinstance(p, dict)}
        except (ValueError, KeyError, TypeError):
            raise CliError(
                f"{out_dir} has an unreadable manifest.json; use another -o", EXIT_USAGE
            ) from None
    if (out_dir / "parts").exists():
        raise CliError(f"{out_dir} has parts/ but no manifest.json; use another -o", EXIT_USAGE)
    if any(e.name != _LOCK_NAME for e in out_dir.iterdir()):
        raise CliError(f"{out_dir} is not empty and not a results dir; use another -o", EXIT_USAGE)
    return None


def _lock_out_dir(out_dir: Path) -> int:
    fd = os.open(out_dir / _LOCK_NAME, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        raise CliError(
            f"another `minion fetch` is writing to {out_dir}; wait for it or use another -o",
            EXIT_USAGE,
        ) from None
    return fd


def _create_manifest(path: Path, doc: dict[str, Any]) -> None:
    """Create `path` only if it does not exist (temp file + hard link, never overwrites)."""
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "wb") as out:
            out.write((json.dumps(doc, indent=2, sort_keys=True) + "\n").encode())
        try:
            os.link(tmp, path)
        except FileExistsError:
            raise CliError(f"{path.parent} was claimed concurrently; use another -o") from None
    finally:
        Path(tmp).unlink(missing_ok=True)


def _prepare_out_dir(
    out_dir: Path, identity: dict[str, str]
) -> tuple[dict[int, dict[str, Any]], int]:
    """Create/claim the output dir for this task and lock it; returns (prior parts, lock fd).

    The caller closes the fd (releasing the flock) when the fetch is done.
    """
    if out_dir.exists() and not out_dir.is_dir():
        raise CliError(f"{out_dir} is not a directory; use another -o", EXIT_USAGE)
    created = not out_dir.exists()
    if created:
        out_dir.mkdir(mode=0o700, parents=True)
    else:
        _check_claim(out_dir, identity)  # refuse before we drop a lock file into a foreign dir
    fd = _lock_out_dir(out_dir)
    try:
        prior = _check_claim(out_dir, identity)  # again, now that nobody else can claim
        manifest = out_dir / "manifest.json"
        if prior is None:
            out_dir.chmod(0o700)  # empty (or ours): tighten whatever mode it had
            _create_manifest(manifest, {**identity, "parts": []})
            prior = {}
        parts_dir = out_dir / "parts"
        parts_dir.mkdir(mode=0o700, exist_ok=True)
        parts_dir.chmod(0o700)
        for stale in (*out_dir.glob(".*.jsonl.*"), *parts_dir.glob(_STALE_TMP)):
            stale.unlink(missing_ok=True)  # temp files of an interrupted run
        for stale in out_dir.glob(".manifest.json.*"):
            stale.unlink(missing_ok=True)
    except BaseException:
        os.close(fd)
        raise
    return prior, fd


def _part_is_current(dest: Path, remote: ObjectInfo, prior: dict[str, Any] | None) -> bool:
    """A local part is reusable only if its content provably equals the remote object."""
    if _size(dest) != remote.size or remote.etag is None:
        return False
    if plain_md5(remote.etag) is not None:
        return md5_file(dest) == remote.etag
    # Multipart ETag: trust the earlier fetch's record only if the object is unchanged.
    return (
        prior is not None
        and prior.get("etag") == remote.etag
        and prior.get("sha256") == sha256_file(dest)
    )


def _remote_parts(listing: list[ObjectInfo], prefix: str) -> dict[int, ObjectInfo]:
    remote: dict[int, ObjectInfo] = {}
    for o in listing:
        m = _PART_RE.match(o.key[len(prefix) :]) if o.key.startswith(prefix) else None
        if m and f"{int(m.group(1)):06d}" == m.group(1):  # canonical name only
            remote[int(m.group(1))] = o
    return remote


async def fetch_results(
    client: UserClient,
    store: ObjectStore,
    settings: CliSettings,
    target: Target,
    task: TaskOut,
    out_dir: Path,
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> FetchResult:
    """Download SUCCEEDED subtask outputs, merge to results.jsonl (ascending subtask_id).

    The output dir is claimed by a manifest naming this task (another task's dir is refused).
    Idempotent: a part on disk is skipped only when its content matches the remote object (md5
    for a plain ETag, else the ETag + sha256 recorded in the manifest). Listing is non-recursive,
    so the `_attempts/` namespace is never listed.
    """
    subtasks = await _list_all_subtasks(client, target, task.counts.total, sleep)
    records = {
        st.subtask_id: st.record_count for st in subtasks if st.status is SubtaskStatus.SUCCEEDED
    }
    failed = [st for st in subtasks if st.status is not SubtaskStatus.SUCCEEDED]

    prefix = target.output_prefix(settings)
    remote = _remote_parts(await store.list_objects(prefix, recursive=False), prefix)
    identity = _identity(target, task)
    prior, lock_fd = await asyncio.to_thread(_prepare_out_dir, out_dir, identity)
    try:
        parts_dir = out_dir / "parts"
        downloaded = 0
        ids = sorted(records)
        for sid in ids:
            if sid not in remote:
                raise CliError(f"result object for subtask {sid} is missing", EXIT_USAGE)
            dest = parts_dir / f"{sid:06d}.jsonl"
            if await asyncio.to_thread(_part_is_current, dest, remote[sid], prior.get(sid)):
                continue
            await store.download_file(remote[sid].key, dest)
            downloaded += 1

        part_paths = [parts_dir / f"{sid:06d}.jsonl" for sid in ids]
        results = out_dir / "results.jsonl"
        await asyncio.to_thread(_merge, part_paths, results)
        manifest = out_dir / "manifest.json"
        manifest_parts = [
            {
                "subtask_id": sid,
                "file": f"parts/{sid:06d}.jsonl",
                "records": records[sid],
                "bytes": remote[sid].size,
                "etag": remote[sid].etag,
                "sha256": await asyncio.to_thread(sha256_file, parts_dir / f"{sid:06d}.jsonl"),
            }
            for sid in ids
        ]
        counts: TaskCounts = task.counts
        await asyncio.to_thread(
            _write_manifest,
            manifest,
            {
                **identity,
                "status": task.status.value,
                "counts": counts.model_dump(),
                "records": sum(records.values()),
                "not_succeeded_subtasks": len(failed),
                "not_succeeded": [
                    {
                        "subtask_id": st.subtask_id,
                        "status": st.status.value,
                        "record_count": st.record_count,
                        "attempt": st.attempt,
                        "error_code": st.error_code,
                    }
                    for st in failed
                ],
                "parts": manifest_parts,
            },
        )
        return FetchResult(
            parts=len(ids),
            downloaded=downloaded,
            records=sum(records.values()),
            not_succeeded=len(failed),
            results_path=results,
            manifest_path=manifest,
        )

    finally:
        os.close(lock_fd)
