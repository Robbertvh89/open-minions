"""In-memory ObjectStore for unit tests (no boto3, no network)."""

from __future__ import annotations

import asyncio
from pathlib import Path

from minion_cli.store import ObjectInfo


class FakeStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.uploads: list[str] = []
        self.downloads: list[str] = []

    async def list_objects(self, prefix: str) -> list[ObjectInfo]:
        return sorted(
            (ObjectInfo(k, len(v)) for k, v in self.objects.items() if k.startswith(prefix)),
            key=lambda o: o.key,
        )

    async def upload_file(self, path: Path, key: str) -> None:
        self.uploads.append(key)
        self.objects[key] = await asyncio.to_thread(path.read_bytes)

    async def download_file(self, key: str, dest: Path) -> None:
        self.downloads.append(key)

        def write() -> None:
            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest.with_name(dest.name + ".tmp")
            tmp.write_bytes(self.objects[key])
            tmp.replace(dest)

        await asyncio.to_thread(write)
