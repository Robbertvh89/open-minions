"""Object storage access. Sync boto3 wrapped in `asyncio.to_thread` (plan D3)."""

import asyncio
from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING

import boto3
from botocore.config import Config

from minion_server.settings import ServerSettings

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

MAX_PRESIGN_TTL_S = 3600
# Per-subtask outputs are small: promoting one must not hold a DB transaction for minutes.
COPY_CONNECT_TIMEOUT_S = 5
COPY_READ_TIMEOUT_S = 30
COPY_MAX_ATTEMPTS = 2


class PreconditionFailedError(Exception):
    """The source object changed between HEAD and COPY (CopySourceIfMatch mismatch)."""


@dataclass(frozen=True)
class ObjectStat:
    size: int
    etag: str


class S3Storage:
    def __init__(self, settings: ServerSettings) -> None:
        self._bucket = settings.s3_bucket
        self._ttl = min(settings.presign_ttl_s, MAX_PRESIGN_TTL_S)

        def cfg(connect_timeout: int, read_timeout: int, max_attempts: int) -> Config:
            return Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                retries={"max_attempts": max_attempts},
            )

        def make(endpoint: str, config: Config | None = None) -> "S3Client":
            return boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=settings.s3_access_key.get_secret_value(),
                aws_secret_access_key=settings.s3_secret_key.get_secret_value(),
                region_name=settings.s3_region,
                config=config or cfg(10, 10, 2),
            )

        self._client = make(settings.s3_endpoint_url)
        self._copy_client = make(
            settings.s3_endpoint_url,
            cfg(COPY_CONNECT_TIMEOUT_S, COPY_READ_TIMEOUT_S, COPY_MAX_ATTEMPTS),
        )
        # Presigning is local; it uses the endpoint executors can reach.
        self._signer = make(settings.public_s3_endpoint_url)

    @property
    def bucket(self) -> str:
        return self._bucket

    def _presign(self, op: str, key: str, ttl_s: int | None) -> str:
        ttl = min(ttl_s or self._ttl, self._ttl)
        url: str = self._signer.generate_presigned_url(
            op, Params={"Bucket": self._bucket, "Key": key}, ExpiresIn=ttl
        )
        return url

    async def presign_get(self, key: str, ttl_s: int | None = None) -> str:
        return await asyncio.to_thread(self._presign, "get_object", key, ttl_s)

    async def presign_put(self, key: str, ttl_s: int | None = None) -> str:
        return await asyncio.to_thread(self._presign, "put_object", key, ttl_s)

    def _list_keys(self, prefix: str, max_keys: int | None) -> list[tuple[str, int]]:
        out: list[tuple[str, int]] = []
        pages = self._client.get_paginator("list_objects_v2").paginate(
            Bucket=self._bucket, Prefix=prefix
        )
        for page in pages:
            out.extend((o["Key"], o["Size"]) for o in page.get("Contents", []))
            if max_keys is not None and len(out) > max_keys:
                break  # caller sees > max_keys and rejects; never hold an unbounded listing
        return sorted(out)

    async def list_keys(self, prefix: str, max_keys: int | None = None) -> list[tuple[str, int]]:
        """Sorted `(key, size)` pairs under `prefix`.

        With `max_keys`, listing stops once more than `max_keys` keys were seen (the result then
        has more than `max_keys` entries and is not complete).
        """
        return await asyncio.to_thread(self._list_keys, prefix, max_keys)

    async def any_key(self, prefix: str) -> bool:
        """True if at least one object exists under `prefix` (single `MaxKeys=1` request)."""

        def _any() -> bool:
            resp = self._client.list_objects_v2(Bucket=self._bucket, Prefix=prefix, MaxKeys=1)
            return bool(resp.get("Contents"))

        return await asyncio.to_thread(_any)

    async def stat(self, key: str) -> ObjectStat | None:
        """Size and ETag, or None if the object does not exist."""

        def _head() -> ObjectStat | None:
            try:
                resp = self._client.head_object(Bucket=self._bucket, Key=key)
            except self._client.exceptions.ClientError as exc:
                if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
                    return None
                raise
            return ObjectStat(int(resp["ContentLength"]), str(resp.get("ETag", "")))

        return await asyncio.to_thread(_head)

    async def head(self, key: str) -> int | None:
        """Object size in bytes, or None if it does not exist."""
        st = await self.stat(key)
        return None if st is None else st.size

    def iter_object(self, key: str, chunk_size: int = 1 << 20) -> Iterator[bytes]:
        """Blocking chunk iterator; call from a worker thread (splitter)."""
        body = self._client.get_object(Bucket=self._bucket, Key=key)["Body"]
        try:
            yield from body.iter_chunks(chunk_size)
        finally:
            body.close()

    async def copy(self, src_key: str, dst_key: str, *, if_match: str | None = None) -> None:
        """Server-side copy. With `if_match` (an ETag) the copy fails if the source changed."""

        def _copy() -> None:
            extra = {"CopySourceIfMatch": if_match} if if_match else {}
            try:
                self._copy_client.copy_object(
                    Bucket=self._bucket,
                    Key=dst_key,
                    CopySource={"Bucket": self._bucket, "Key": src_key},
                    **extra,  # type: ignore[arg-type]
                )
            except self._copy_client.exceptions.ClientError as exc:
                if exc.response.get("Error", {}).get("Code") == "PreconditionFailed":
                    raise PreconditionFailedError from exc
                raise

        await asyncio.to_thread(_copy)
