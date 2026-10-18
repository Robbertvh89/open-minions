"""Presigned-URL data path: ranged GET to a temp file, streaming PUT, sha256 (invariant 4)."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
from minion_common.s3post import encode_post_form
from minion_common.schemas import PresignedPost, range_header

from minion_executor.runners.base import Record

CHUNK = 1024 * 1024


class BadInputError(Exception):
    """Input bytes are not valid JSONL records. Message never contains payload content."""


class InputUnavailableError(Exception):
    """The ranged GET did not yield exactly the requested bytes (executor-local/transient)."""


class OutputTooLargeError(Exception):
    """S3 refused the upload because it exceeds the signed size range (deterministic)."""


async def download_range(
    http: httpx.AsyncClient, url: str, start: int, end_exclusive: int, dest: Path
) -> int:
    """GET bytes [start, end_exclusive) of the presigned `url` into `dest`; return bytes written."""
    expected = end_exclusive - start
    headers = {"Range": range_header(start, end_exclusive)}
    written = 0
    async with http.stream("GET", url, headers=headers) as resp:
        resp.raise_for_status()
        if resp.status_code != 206:  # a 200 means the Range header was ignored
            raise InputUnavailableError(f"ranged GET returned {resp.status_code}, expected 206")
        fh = await asyncio.to_thread(dest.open, "wb")
        try:
            async for chunk in resp.aiter_bytes(CHUNK):
                written += len(chunk)
                if written > expected:
                    raise InputUnavailableError("ranged GET returned more bytes than requested")
                await asyncio.to_thread(fh.write, chunk)
        finally:
            await asyncio.to_thread(fh.close)
    if written != expected:
        raise InputUnavailableError(f"ranged GET returned {written} bytes, expected {expected}")
    return written


def parse_jsonl(path: Path, expected_records: int) -> list[Record]:
    """Parse a JSONL file of objects; blank lines are skipped. Raises BadInputError.

    Blocking; call via `asyncio.to_thread`.
    """
    records: list[Record] = []
    with path.open("rb") as fh:
        for lineno, line in enumerate(fh, start=1):
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except RecursionError:  # pathologically nested JSON
                raise BadInputError(f"line {lineno} is nested too deeply") from None
            except ValueError:
                raise BadInputError(f"line {lineno} is not valid JSON") from None
            if not isinstance(rec, dict):
                raise BadInputError(f"line {lineno} is not a JSON object")
            records.append(rec)
    if len(records) != expected_records:
        raise BadInputError(f"expected {expected_records} records, found {len(records)}")
    return records


def write_jsonl(path: Path, records: list[Record]) -> tuple[int, str]:
    """Write records as JSONL; return (byte_size, sha256 hex). Blocking; use `to_thread`."""
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
    fh = await asyncio.to_thread(path.open, "rb")
    try:
        while chunk := await asyncio.to_thread(fh.read, CHUNK):
            yield chunk
    finally:
        await asyncio.to_thread(fh.close)


async def upload_file(http: httpx.AsyncClient, url: str, path: Path) -> None:
    """Stream `path` to the presigned PUT `url` with an explicit Content-Length."""
    size = await asyncio.to_thread(lambda: path.stat().st_size)
    resp = await http.put(
        url,
        content=_file_chunks(path),
        headers={"Content-Length": str(size)},
    )
    resp.raise_for_status()


async def _post_body(preamble: bytes, path: Path, epilogue: bytes) -> AsyncIterator[bytes]:
    yield preamble
    async for chunk in _file_chunks(path):
        yield chunk
    yield epilogue


async def upload_post(http: httpx.AsyncClient, post: PresignedPost, path: Path) -> None:
    """Stream `path` as the `file` part of a presigned POST with an exact Content-Length.

    Any 2xx is success. A 400 EntityTooLarge raises `OutputTooLargeError`; other non-2xx
    (including a 3xx, which is not followed) raise `httpx.HTTPStatusError`. The form fields are
    credentials: never logged.
    """
    size = await asyncio.to_thread(lambda: path.stat().st_size)
    parts = encode_post_form(post.fields)
    length = len(parts.preamble) + size + len(parts.epilogue)
    async with http.stream(
        "POST",
        post.url,
        content=_post_body(parts.preamble, path, parts.epilogue),
        headers={"Content-Type": parts.content_type, "Content-Length": str(length)},
    ) as resp:
        if resp.is_success:
            return
        if resp.status_code == 400:
            head = b""
            async for chunk in resp.aiter_bytes():
                head += chunk
                if len(head) >= 4096:
                    break
            if b"<Code>EntityTooLarge</Code>" in head[:4096]:
                raise OutputTooLargeError
        await resp.aread()
        resp.raise_for_status()
