"""Presigned-URL data path: ranged GET to a temp file, streaming PUT, sha256 (invariant 4)."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator
from pathlib import Path

import httpx

from minion_executor.runners.base import Record

CHUNK = 1024 * 1024


class BadInputError(Exception):
    """Input bytes are not valid JSONL records. Message never contains payload content."""


async def download_range(
    http: httpx.AsyncClient, url: str, start: int, end_exclusive: int, dest: Path
) -> int:
    """GET bytes [start, end_exclusive) of the presigned `url` into `dest`; return bytes written."""
    expected = end_exclusive - start
    headers = {"Range": f"bytes={start}-{end_exclusive - 1}"}
    written = 0
    async with http.stream("GET", url, headers=headers) as resp:
        resp.raise_for_status()
        with dest.open("wb") as fh:
            async for chunk in resp.aiter_bytes(CHUNK):
                written += len(chunk)
                if written > expected:
                    raise BadInputError("ranged GET returned more bytes than requested")
                fh.write(chunk)
    return written


def parse_jsonl(path: Path, expected_records: int) -> list[Record]:
    """Parse a JSONL file of objects; blank lines are skipped. Raises BadInputError."""
    records: list[Record] = []
    with path.open("rb") as fh:
        for lineno, line in enumerate(fh, start=1):
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                raise BadInputError(f"line {lineno} is not valid JSON") from None
            if not isinstance(rec, dict):
                raise BadInputError(f"line {lineno} is not a JSON object")
            records.append(rec)
    if len(records) != expected_records:
        raise BadInputError(f"expected {expected_records} records, found {len(records)}")
    return records


def write_jsonl(path: Path, records: list[Record]) -> tuple[int, str]:
    """Write records as JSONL; return (byte_size, sha256 hex)."""
    digest = hashlib.sha256()
    size = 0
    with path.open("wb") as fh:
        for rec in records:
            line = (
                json.dumps(rec, separators=(",", ":"), ensure_ascii=False).encode("utf-8") + b"\n"
            )
            digest.update(line)
            size += len(line)
            fh.write(line)
    return size, digest.hexdigest()


async def _file_chunks(path: Path) -> AsyncIterator[bytes]:
    with path.open("rb") as fh:
        while chunk := fh.read(CHUNK):
            yield chunk


async def upload_file(http: httpx.AsyncClient, url: str, path: Path) -> None:
    """Stream `path` to the presigned PUT `url` with an explicit Content-Length."""
    size = await asyncio.to_thread(lambda: path.stat().st_size)
    resp = await http.put(
        url,
        content=_file_chunks(path),
        headers={"Content-Length": str(size)},
    )
    resp.raise_for_status()
