"""Object store access for the submitter. Credentials live here and nowhere else (S-14)."""

from __future__ import annotations

import asyncio
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from minion_cli.errors import EXIT_UNREACHABLE, EXIT_USAGE, CliError
from minion_cli.settings import CliSettings

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


@dataclass(frozen=True)
class ObjectInfo:
    key: str
    size: int


class ObjectStore(Protocol):
    async def list_objects(self, prefix: str) -> list[ObjectInfo]:
        """Objects under `prefix`, sorted by key."""
        ...

    async def upload_file(self, path: Path, key: str) -> None: ...

    async def download_file(self, key: str, dest: Path) -> None:
        """Write the object to `dest` atomically (temp file in the same directory + replace)."""
        ...


def _client(settings: CliSettings) -> S3Client:
    return boto3.client(
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
    return CliError(f"object store unreachable while trying to {what}", EXIT_UNREACHABLE)


class S3Store:
    def __init__(self, settings: CliSettings) -> None:
        self._bucket = settings.s3_bucket
        self._s3 = _client(settings)

    def _list(self, prefix: str) -> list[ObjectInfo]:
        out: list[ObjectInfo] = []
        pager = self._s3.get_paginator("list_objects_v2")
        for page in pager.paginate(Bucket=self._bucket, Prefix=prefix):
            out.extend(ObjectInfo(o["Key"], o["Size"]) for o in page.get("Contents", []))
        return sorted(out, key=lambda o: o.key)

    async def list_objects(self, prefix: str) -> list[ObjectInfo]:
        try:
            return await asyncio.to_thread(self._list, prefix)
        except (BotoCoreError, ClientError) as exc:
            raise _wrap(exc, "list objects") from None

    async def upload_file(self, path: Path, key: str) -> None:
        try:
            await asyncio.to_thread(self._s3.upload_file, str(path), self._bucket, key)
        except (BotoCoreError, ClientError, boto3.exceptions.S3UploadFailedError) as exc:
            raise _wrap(exc, "upload an input file") from None

    def _download(self, key: str, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=dest.parent, prefix=f".{dest.name}.")
        os.close(fd)
        try:
            self._s3.download_file(self._bucket, key, tmp)
            Path(tmp).replace(dest)
        finally:
            Path(tmp).unlink(missing_ok=True)

    async def download_file(self, key: str, dest: Path) -> None:
        try:
            await asyncio.to_thread(self._download, key, dest)
        except (BotoCoreError, ClientError) as exc:
            raise _wrap(exc, "download a result part") from None


def make_store(settings: CliSettings) -> ObjectStore:
    return S3Store(settings)
