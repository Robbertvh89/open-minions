"""Object store access for the submitter. Credentials live here and nowhere else (S-14)."""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
import tempfile
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

import boto3
from boto3.exceptions import RetriesExceededError, S3TransferFailedError, S3UploadFailedError
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from minion_common.client import require_secure_url

from minion_cli.errors import EXIT_UNREACHABLE, EXIT_USAGE, CliError
from minion_cli.settings import CliSettings

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

_PLAIN_MD5 = re.compile(r"^[0-9a-f]{32}$")


@dataclass(frozen=True)
class ObjectInfo:
    key: str
    size: int
    etag: str | None = None  # without quotes
    sha256: str | None = None  # x-amz-meta-sha256, set by our uploads (only from `head`)


def plain_md5(etag: str | None) -> str | None:
    """The ETag if it is a plain md5 (single-part upload), else None."""
    return etag if etag is not None and _PLAIN_MD5.match(etag) else None


def sha256_file(path: Path) -> str:
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def md5_file(path: Path) -> str:
    with path.open("rb") as f:
        return hashlib.file_digest(f, lambda: hashlib.md5(usedforsecurity=False)).hexdigest()


class ObjectStore(Protocol):
    async def list_objects(self, prefix: str, *, recursive: bool = True) -> list[ObjectInfo]:
        """Objects under `prefix`, sorted by key. `recursive=False` lists only the direct
        children (Delimiter="/"), so deeper namespaces are never listed."""
        ...

    async def head(self, key: str) -> ObjectInfo | None:
        """Size, ETag and `sha256` metadata of one object, or None when absent."""
        ...

    async def upload_file(self, path: Path, key: str, *, sha256: str) -> None:
        """Upload `path`, recording `sha256` as object metadata (`x-amz-meta-sha256`)."""
        ...

    async def download_file(self, key: str, dest: Path) -> None:
        """Write the object to `dest` atomically (temp file in the same directory + replace)."""
        ...


# ---- dedicated transfer pool (so Ctrl-C does not wait for boto3 threads) -----------------------

_lock = threading.Lock()
_pool: ThreadPoolExecutor | None = None
_inflight = 0


def _get_pool() -> ThreadPoolExecutor:
    global _pool
    with _lock:
        if _pool is None:
            _pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="minion-s3")
        return _pool


def _release(_: object) -> None:
    global _inflight
    with _lock:
        _inflight -= 1


async def _in_pool[T](fn: Callable[..., T], *args: object) -> T:
    """Run `fn` in the transfer pool. The in-flight count covers queued work too and is dropped
    by a done-callback, so it is exact even if the awaiting task is cancelled (Ctrl-C)."""
    global _inflight
    with _lock:
        _inflight += 1
    try:
        fut = _get_pool().submit(fn, *args)
    except BaseException:
        _release(None)
        raise
    fut.add_done_callback(_release)
    return await asyncio.wrap_future(fut)


def abort_transfers() -> bool:
    """Drop queued transfers without waiting for the running ones.

    Returns True when a transfer thread is still running (the caller must then exit the process
    without joining it).
    """
    global _pool
    with _lock:
        pool, _pool = _pool, None
        running = _inflight > 0
    if pool is not None:
        pool.shutdown(wait=False, cancel_futures=True)
    return running


def _client(settings: CliSettings) -> S3Client:
    client = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=(
            settings.s3_access_key.get_secret_value() if settings.s3_access_key else None
        ),
        aws_secret_access_key=(
            settings.s3_secret_key.get_secret_value() if settings.s3_secret_key else None
        ),
        region_name=settings.s3_region,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=10,
            read_timeout=60,
            retries={"max_attempts": 3},
            # SeaweedFS does not accept the default trailing CRC32 checksums of recent boto3.
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )
    if settings.s3_endpoint_url is None:
        # The endpoint came from the environment (AWS_ENDPOINT_URL...) or config files: apply the
        # same https-only rule as for MINION_CLI_S3_ENDPOINT_URL.
        try:
            require_secure_url(client.meta.endpoint_url)
        except ValueError as exc:
            raise CliError(f"object store endpoint: {exc}", EXIT_USAGE) from None
    return client


def _wrap(exc: Exception, what: str) -> CliError:
    """Map an S3 failure to a CliError without echoing keys, bodies or credentials."""
    if isinstance(exc, ClientError):
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        if status in (401, 403):
            return CliError(f"object store denied {what} (check S3 credentials)", EXIT_USAGE)
        if status == 404:
            return CliError(f"object store: not found while trying to {what}", EXIT_USAGE)
        if status < 500:
            return CliError(f"object store rejected {what} (HTTP {status})", EXIT_USAGE)
    elif isinstance(exc, RetriesExceededError | S3TransferFailedError | S3UploadFailedError):
        return CliError(f"object store transfer failed while trying to {what}", EXIT_UNREACHABLE)
    elif isinstance(exc, OSError) and not isinstance(exc, BotoCoreError):
        return CliError(
            f"local file error while trying to {what}: {type(exc).__name__}", EXIT_USAGE
        )
    return CliError(f"object store unreachable while trying to {what}", EXIT_UNREACHABLE)


_S3_ERRORS = (
    BotoCoreError,
    ClientError,
    RetriesExceededError,
    S3TransferFailedError,
    S3UploadFailedError,
    OSError,
)


class S3Store:
    def __init__(self, settings: CliSettings) -> None:
        self._bucket = settings.s3_bucket
        self._s3 = _client(settings)

    def _list(self, prefix: str, recursive: bool) -> list[ObjectInfo]:
        out: list[ObjectInfo] = []
        pager = self._s3.get_paginator("list_objects_v2")
        delimiter = "" if recursive else "/"
        for page in pager.paginate(Bucket=self._bucket, Prefix=prefix, Delimiter=delimiter):
            out.extend(
                ObjectInfo(o["Key"], o["Size"], o.get("ETag", "").strip('"') or None)
                for o in page.get("Contents", [])
            )
        return sorted(out, key=lambda o: o.key)

    async def list_objects(self, prefix: str, *, recursive: bool = True) -> list[ObjectInfo]:
        try:
            return await _in_pool(self._list, prefix, recursive)
        except _S3_ERRORS as exc:
            raise _wrap(exc, "list objects") from None

    def _head(self, key: str) -> ObjectInfo | None:
        try:
            r = self._s3.head_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            if exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
                return None
            raise
        meta = {k.lower(): v for k, v in r.get("Metadata", {}).items()}
        return ObjectInfo(
            key, r["ContentLength"], r.get("ETag", "").strip('"') or None, meta.get("sha256")
        )

    async def head(self, key: str) -> ObjectInfo | None:
        try:
            return await _in_pool(self._head, key)
        except _S3_ERRORS as exc:
            raise _wrap(exc, "inspect an object") from None

    async def upload_file(self, path: Path, key: str, *, sha256: str) -> None:
        try:
            await _in_pool(
                lambda: self._s3.upload_file(
                    str(path), self._bucket, key, ExtraArgs={"Metadata": {"sha256": sha256}}
                )
            )
        except _S3_ERRORS as exc:
            raise _wrap(exc, "upload an input file") from None

    def _download(self, key: str, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=dest.parent, prefix=f".{dest.name}.")
        os.close(fd)
        try:
            self._s3.download_file(self._bucket, key, tmp)
            Path(tmp).replace(dest)
            dest.chmod(0o600)
        finally:
            Path(tmp).unlink(missing_ok=True)

    async def download_file(self, key: str, dest: Path) -> None:
        try:
            await _in_pool(self._download, key, dest)
        except _S3_ERRORS as exc:
            raise _wrap(exc, "download a result part") from None


def make_store(settings: CliSettings) -> ObjectStore:
    return S3Store(settings)
