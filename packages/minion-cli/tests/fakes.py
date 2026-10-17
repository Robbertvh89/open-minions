"""In-memory ObjectStore for unit tests (no boto3, no network); models ETags and metadata."""

from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
from pathlib import Path

from minion_cli.store import ObjectInfo


class FakeStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.meta: dict[str, dict[str, str]] = {}
        self.etags: dict[str, str] = {}  # overrides (e.g. a multipart-style "abc-2")
        self.uploads: list[str] = []
        self.downloads: list[str] = []
        self.listed: list[tuple[str, bool]] = []

    def put(self, key: str, data: bytes, *, etag: str | None = None) -> None:
        self.objects[key] = data
        if etag is not None:
            self.etags[key] = etag

    def _info(self, key: str, with_meta: bool = False) -> ObjectInfo:
        data = self.objects[key]
        etag = self.etags.get(key) or hashlib.md5(data, usedforsecurity=False).hexdigest()
        sha = self.meta.get(key, {}).get("sha256") if with_meta else None
        return ObjectInfo(key, len(data), etag, sha)

    async def list_objects(self, prefix: str, *, recursive: bool = True) -> list[ObjectInfo]:
        self.listed.append((prefix, recursive))
        return sorted(
            (
                self._info(k)
                for k in self.objects
                if k.startswith(prefix) and (recursive or "/" not in k[len(prefix) :])
            ),
            key=lambda o: o.key,
        )

    async def head(self, key: str) -> ObjectInfo | None:
        return self._info(key, with_meta=True) if key in self.objects else None

    async def upload_file(self, path: Path, key: str, *, sha256: str) -> None:
        self.uploads.append(key)
        self.objects[key] = await asyncio.to_thread(path.read_bytes)
        self.meta[key] = {"sha256": sha256}
        self.etags.pop(key, None)

    async def download_file(self, key: str, dest: Path) -> None:
        self.downloads.append(key)

        def write() -> None:
            dest.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=dest.parent, prefix=f".{dest.name}.")
            with os.fdopen(fd, "wb") as f:
                f.write(self.objects[key])
            Path(tmp).replace(dest)

        await asyncio.to_thread(write)
