import urllib.error
import urllib.request

import pytest
from minion_server.settings import ServerSettings
from minion_server.storage import S3Storage


def _http(method: str, url: str, data: bytes | None = None) -> bytes:
    req = urllib.request.Request(url, data=data, method=method)  # noqa: S310 - test URL
    with urllib.request.urlopen(req, timeout=10) as r:  # noqa: S310
        return bytes(r.read())


@pytest.mark.integration
async def test_presigned_roundtrip(s3) -> None:  # type: ignore[no-untyped-def]
    cfg = ServerSettings(
        database_url="postgresql+asyncpg://x@h/db",
        s3_endpoint_url=s3.endpoint_url,
        s3_access_key=s3.access_key,
        s3_secret_key=s3.secret_key,
        s3_bucket=s3.bucket,
        s3_region=s3.region,
    )
    st = S3Storage(cfg)
    put = await st.presign_put("outputs/t/x.jsonl")
    _http("PUT", put, b'{"a":1}\n')
    assert await st.head("outputs/t/x.jsonl") == 8
    assert await st.head("outputs/t/missing") is None
    assert _http("GET", await st.presign_get("outputs/t/x.jsonl")) == b'{"a":1}\n'
    # presigned PUT is scoped to one object
    with pytest.raises(urllib.error.HTTPError):
        _http("PUT", put.replace("x.jsonl", "y.jsonl"), b"z")
    await st.copy("outputs/t/x.jsonl", "outputs/t/z.jsonl")
    assert await st.list_keys("outputs/t/") == [("outputs/t/x.jsonl", 8), ("outputs/t/z.jsonl", 8)]
    assert b"".join(st.iter_object("outputs/t/z.jsonl")) == b'{"a":1}\n'
