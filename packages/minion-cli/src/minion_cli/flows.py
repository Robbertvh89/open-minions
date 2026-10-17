"""Submit / wait / fetch flows (async, no CLI). Never prints or logs line content."""

from __future__ import annotations

import asyncio
import hashlib
import json
import random
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from minion_common.client import ApiError, UserClient
from minion_common.enums import SubtaskStatus, TaskStatus, TaskType
from minion_common.schemas import TaskCounts, TaskCreate, TaskOut

from minion_cli.errors import EXIT_UNREACHABLE, EXIT_USAGE, CliError
from minion_cli.settings import CliSettings
from minion_cli.store import ObjectStore

TERMINAL = frozenset(
    {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.PARTIAL, TaskStatus.CANCELLED}
)
POLL_START_S = 2.0
POLL_FACTOR = 1.5
POLL_MAX_S = 30.0
MAX_TRANSIENT = 10
_PART_RE = re.compile(r"^(\d{6})\.jsonl$")
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
            return CliError(f"server error: {exc}", EXIT_UNREACHABLE)
        return CliError(f"server rejected the request: {exc}", EXIT_USAGE)
    return CliError(f"server unreachable: {type(exc).__name__}", EXIT_UNREACHABLE)


# ---- upload / submit ----------------------------------------------------------------------


def _file_sizes(files: list[Path]) -> list[int]:
    sizes: list[int] = []
    for f in files:
        if not f.is_file():
            raise CliError(f"not a file: {f.name}", EXIT_USAGE)
        sizes.append(f.stat().st_size)
    return sizes


async def upload_inputs(
    store: ObjectStore, settings: CliSettings, target: Target, files: list[Path]
) -> str:
    """Upload `files` under the task's input prefix (argv order) and return the input URI.

    Resumes when the prefix holds only our own keys with matching sizes; any other object there
    is foreign and aborts (exit 2) rather than being mixed into the task input.
    """
    if not files:
        raise CliError("no input files", EXIT_USAGE)
    sizes = await asyncio.to_thread(_file_sizes, files)
    prefix = target.input_prefix(settings)
    expected = {
        f"{prefix}{i:04d}-{_UNSAFE.sub('_', f.name)}": size
        for i, (f, size) in enumerate(zip(files, sizes, strict=True))
    }
    if len(expected) != len(files):  # pragma: no cover - index prefix makes keys unique
        raise CliError("duplicate input keys", EXIT_USAGE)
    existing = {o.key: o.size for o in await store.list_objects(prefix)}
    for key, size in existing.items():
        if expected.get(key) != size:
            raise CliError(
                f"input prefix {prefix!r} already holds other objects; choose another --name",
                EXIT_USAGE,
            )
    for f, key in zip(files, expected, strict=True):
        if key not in existing:
            await store.upload_file(f, key)
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
    input_uri = await upload_inputs(store, settings, target, files)
    req = TaskCreate(
        team=target.team,
        project=target.project,
        task_name=target.name,
        type=type,
        model=model,
        input_uri=input_uri,
        output_uri=f"s3://{settings.s3_bucket}/{target.output_prefix(settings)}",
        params=params or {},
        records_per_subtask=records_per_subtask,
        priority=priority,
        allowed_pool=allowed_pool,
    )
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
        if deadline is not None and clock() + wait > deadline:
            raise CliError("timed out waiting for the task", EXIT_UNREACHABLE)
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


def _size(p: Path) -> int | None:
    try:
        return p.stat().st_size
    except FileNotFoundError:
        return None


def _merge(parts: list[Path], dest: Path) -> None:
    tmp = dest.with_name(dest.name + ".tmp")
    with tmp.open("wb") as out:
        for p in parts:
            data = p.read_bytes()
            out.write(data)
            if data and not data.endswith(b"\n"):
                out.write(b"\n")
    tmp.replace(dest)


def _write_manifest(path: Path, doc: dict[str, Any]) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
    tmp.replace(path)


def _sha256(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


async def fetch_results(
    client: UserClient,
    store: ObjectStore,
    settings: CliSettings,
    target: Target,
    task: TaskOut,
    out_dir: Path,
) -> FetchResult:
    """Download SUCCEEDED subtask outputs, merge to results.jsonl (ascending subtask_id).

    Idempotent: a part already on disk with the stored size is skipped. Only
    `{out}/{id:06d}.jsonl` objects are read; the `_attempts/` namespace is never listed.
    """
    records: dict[int, int] = {}
    not_succeeded = 0
    cursor: str | None = None
    try:
        while True:
            page = await client.list_subtasks(
                target.team, target.project, target.name, cursor=cursor
            )
            for st in page.items:
                if st.status is SubtaskStatus.SUCCEEDED:
                    records[st.subtask_id] = st.record_count
                else:
                    not_succeeded += 1
            cursor = page.next_cursor
            if cursor is None:
                break
    except (ApiError, httpx.HTTPError) as exc:
        raise api_failure(exc) from None

    prefix = target.output_prefix(settings)
    remote = {
        int(m.group(1)): o.size
        for o in await store.list_objects(prefix)
        if o.key.startswith(prefix) and (m := _PART_RE.match(o.key[len(prefix) :]))
    }
    parts_dir = out_dir / "parts"
    await asyncio.to_thread(lambda: parts_dir.mkdir(parents=True, exist_ok=True))
    downloaded = 0
    ids = sorted(records)
    for sid in ids:
        if sid not in remote:
            raise CliError(f"result object for subtask {sid} is missing", EXIT_USAGE)
        dest = parts_dir / f"{sid:06d}.jsonl"
        if await asyncio.to_thread(_size, dest) == remote[sid]:
            continue
        await store.download_file(f"{prefix}{sid:06d}.jsonl", dest)
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
            "bytes": remote[sid],
            "sha256": await asyncio.to_thread(_sha256, parts_dir / f"{sid:06d}.jsonl"),
        }
        for sid in ids
    ]
    counts: TaskCounts = task.counts
    await asyncio.to_thread(
        _write_manifest,
        manifest,
        {
            "team": target.team,
            "project": target.project,
            "task_name": target.name,
            "status": task.status.value,
            "counts": counts.model_dump(),
            "records": sum(records.values()),
            "not_succeeded_subtasks": not_succeeded,
            "parts": manifest_parts,
        },
    )
    return FetchResult(
        parts=len(ids),
        downloaded=downloaded,
        records=sum(records.values()),
        not_succeeded=not_succeeded,
        results_path=results,
        manifest_path=manifest,
    )
