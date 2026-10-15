import urllib.error
import urllib.request
from urllib.parse import urlsplit

import pytest
from minion_server.settings import ServerSettings
from minion_server.storage import S3Storage
from pydantic import SecretStr

from conftest import S3Env


def _http(method: str, url: str, data: bytes | None = None) -> bytes:
    req = urllib.request.Request(url, data=data, method=method)  # noqa: S310 - test URL
    with urllib.request.urlopen(req, timeout=10) as r:  # noqa: S310
        return bytes(r.read())


@pytest.mark.integration
async def test_presigned_roundtrip(s3: S3Env) -> None:
    cfg = ServerSettings(
        database_url=SecretStr("postgresql+asyncpg://x@h/db"),
        s3_endpoint_url=s3.endpoint_url,
        s3_access_key=SecretStr(s3.access_key),
        s3_secret_key=SecretStr(s3.secret_key),
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


def test_presigned_host_uses_public_endpoint() -> None:
    cfg = ServerSettings(
        database_url=SecretStr("postgresql+asyncpg://x@h/db"),
        s3_endpoint_url="http://s3.internal:9000",
        s3_public_endpoint_url="https://s3.public.example.com",
        s3_access_key=SecretStr("ak"),
        s3_secret_key=SecretStr("sk"),
    )
    st = S3Storage(cfg)
    url = st._presign("get_object", "inputs/a", None)
    assert urlsplit(url).netloc == "s3.public.example.com"
    assert urlsplit(url).scheme == "https"


@pytest.mark.integration
async def test_any_key_stat_and_conditional_copy(s3: S3Env) -> None:
    from minion_server.storage import PreconditionFailedError

    cfg = ServerSettings(
        database_url=SecretStr("postgresql+asyncpg://x@h/db"),
        s3_endpoint_url=s3.endpoint_url,
        s3_access_key=SecretStr(s3.access_key),
        s3_secret_key=SecretStr(s3.secret_key),
        s3_bucket=s3.bucket,
        s3_region=s3.region,
    )
    st = S3Storage(cfg)
    assert not await st.any_key("cond/")
    s3.client.put_object(Bucket=s3.bucket, Key="cond/src", Body=b"one")
    assert await st.any_key("cond/")
    stat = await st.stat("cond/src")
    assert stat is not None and stat.size == 3 and stat.etag
    await st.copy("cond/src", "cond/dst", if_match=stat.etag)
    s3.client.put_object(Bucket=s3.bucket, Key="cond/src", Body=b"changed")  # TOCTOU
    with pytest.raises(PreconditionFailedError):
        await st.copy("cond/src", "cond/dst2", if_match=stat.etag)
    assert not await st.any_key("cond/dst2")
    big = await st.list_keys("cond/", max_keys=1)
    assert len(big) > 1  # truncated listing signals "more than max_keys"
