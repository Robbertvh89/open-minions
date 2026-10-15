"""Object storage access. Sync boto3 wrapped in `asyncio.to_thread` (plan D3)."""

import asyncio
from collections.abc import Iterator
from typing import Any

import boto3
from botocore.config import Config

from minion_server.settings import ServerSettings

MAX_PRESIGN_TTL_S = 3600


class S3Storage:
    def __init__(self, settings: ServerSettings) -> None:
        self._bucket = settings.s3_bucket
        self._ttl = min(settings.presign_ttl_s, MAX_PRESIGN_TTL_S)
        cfg = Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=10,
            read_timeout=10,
            retries={"max_attempts": 2},
        )

        def make(endpoint: str) -> Any:
            return boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=settings.s3_access_key,
                aws_secret_access_key=settings.s3_secret_key,
                region_name=settings.s3_region,
                config=cfg,
            )

        self._client = make(settings.s3_endpoint_url)
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

    def _list_keys(self, prefix: str) -> list[tuple[str, int]]:
        out: list[tuple[str, int]] = []
        pages = self._client.get_paginator("list_objects_v2").paginate(
            Bucket=self._bucket, Prefix=prefix
        )
        for page in pages:
            out.extend((o["Key"], o["Size"]) for o in page.get("Contents", []))
        return sorted(out)

    async def list_keys(self, prefix: str) -> list[tuple[str, int]]:
        """Sorted `(key, size)` pairs under `prefix`."""
        return await asyncio.to_thread(self._list_keys, prefix)

    async def head(self, key: str) -> int | None:
        """Object size in bytes, or None if it does not exist."""

        def _head() -> int | None:
            try:
                resp = self._client.head_object(Bucket=self._bucket, Key=key)
            except self._client.exceptions.ClientError as exc:
                if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
                    return None
                raise
            return int(resp["ContentLength"])

        return await asyncio.to_thread(_head)

    def iter_object(self, key: str, chunk_size: int = 1 << 20) -> Iterator[bytes]:
        """Blocking chunk iterator; call from a worker thread (splitter)."""
        body = self._client.get_object(Bucket=self._bucket, Key=key)["Body"]
        try:
            yield from body.iter_chunks(chunk_size)
        finally:
            body.close()

    async def copy(self, src_key: str, dst_key: str) -> None:
        def _copy() -> None:
            self._client.copy_object(
                Bucket=self._bucket,
                Key=dst_key,
                CopySource={"Bucket": self._bucket, "Key": src_key},
            )

        await asyncio.to_thread(_copy)
