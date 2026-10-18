"""io.upload_post: multipart framing, exact Content-Length, streaming, error mapping."""

from collections.abc import AsyncIterator
from email.parser import BytesParser
from email.policy import HTTP
from pathlib import Path

import httpx
import pytest
from minion_common.schemas import PresignedPost
from minion_executor import io

POST = PresignedPost(url="https://s3.test/bkt", fields={"key": "a/b", "policy": "pp", "x": "y"})


async def _upload(path: Path, handler: object) -> None:
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:  # type: ignore[arg-type]
        await io.upload_post(http, POST, path)


async def test_form_fields_in_order_file_last_exact_length_streamed(tmp_path: Path) -> None:
    data = b"x" * (io.CHUNK * 2 + 5)  # several read chunks
    f = tmp_path / "o.jsonl"
    f.write_bytes(data)
    seen: dict[str, object] = {}

    async def handler(req: httpx.Request) -> httpx.Response:
        raw = await req.aread()
        assert int(req.headers["Content-Length"]) == len(raw)
        msg = BytesParser(policy=HTTP).parsebytes(
            b"Content-Type: " + req.headers["Content-Type"].encode() + b"\r\n\r\n" + raw
        )
        parts = list(msg.iter_parts())
        seen["names"] = [p.get_param("name", header="content-disposition") for p in parts]
        seen["file"] = parts[-1].get_payload(decode=True)
        return httpx.Response(204)

    await _upload(f, handler)
    assert seen["names"] == ["key", "policy", "x", "file"]
    assert seen["file"] == data


@pytest.mark.parametrize("status", [307, 403, 500])
async def test_non_2xx_raises_status_error(tmp_path: Path, status: int) -> None:
    f = tmp_path / "o"
    f.write_bytes(b"{}\n")
    with pytest.raises(httpx.HTTPStatusError):
        await _upload(f, lambda req: httpx.Response(status))


async def test_entity_too_large(tmp_path: Path) -> None:
    f = tmp_path / "o"
    f.write_bytes(b"{}\n")
    with pytest.raises(io.OutputTooLargeError):
        await _upload(f, lambda req: httpx.Response(400, content=b"<Code>EntityTooLarge</Code>"))


async def test_connect_error_propagates(tmp_path: Path) -> None:
    f = tmp_path / "o"
    f.write_bytes(b"{}\n")

    def boom(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("x")

    with pytest.raises(httpx.ConnectError):
        await _upload(f, boom)


class _Chunks(httpx.AsyncByteStream):
    def __init__(self, *chunks: bytes) -> None:
        self._chunks = chunks

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for c in self._chunks:
            yield c


async def test_streaming_other_400_raises_status_error_not_stream_consumed(
    tmp_path: Path,
) -> None:
    f = tmp_path / "o"
    f.write_bytes(b"{}\n")
    with pytest.raises(httpx.HTTPStatusError):
        await _upload(f, lambda req: httpx.Response(400, stream=_Chunks(b"<Code>X", b"</Code>")))


async def test_streaming_entity_too_large(tmp_path: Path) -> None:
    f = tmp_path / "o"
    f.write_bytes(b"{}\n")
    stream = _Chunks(b"<Error><Code>Entity", b"TooLarge</Code></Error>")
    with pytest.raises(io.OutputTooLargeError):
        await _upload(f, lambda req: httpx.Response(400, stream=stream))
