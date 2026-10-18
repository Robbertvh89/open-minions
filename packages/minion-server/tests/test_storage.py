import urllib.error
import urllib.request
import warnings
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
    s3.client.put_object(Bucket=s3.bucket, Key="outputs/t/x.jsonl", Body=b'{"a":1}\n')
    assert await st.head("outputs/t/x.jsonl") == 8
    assert await st.head("outputs/t/missing") is None
    assert _http("GET", await st.presign_get("outputs/t/x.jsonl")) == b'{"a":1}\n'
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


def _unit_storage() -> S3Storage:
    return S3Storage(
        ServerSettings(
            database_url=SecretStr("postgresql+asyncpg://x@h/db"),
            s3_access_key=SecretStr("ak"),
            s3_secret_key=SecretStr("sk"),
        )
    )


async def test_presign_get_signs_the_range_header() -> None:
    from urllib.parse import parse_qs

    st = _unit_storage()
    ranged = parse_qs(urlsplit(await st.presign_get("inputs/a", 60, (10, 20))).query)
    assert "range" in ranged["X-Amz-SignedHeaders"][0].split(";")
    plain = parse_qs(urlsplit(await st.presign_get("inputs/a", 60)).query)
    assert "range" not in plain["X-Amz-SignedHeaders"][0].split(";")
    # a different slice yields a different signature
    other = parse_qs(urlsplit(await st.presign_get("inputs/a", 60, (10, 21))).query)
    assert other["X-Amz-Signature"] != ranged["X-Amz-Signature"]


@pytest.mark.integration
async def test_signed_range_get_enforced_by_s3(s3: S3Env) -> None:
    """The signed-range GET returns exactly the slice; other requests are refused (403) where the
    backend enforces SigV4 signed headers (AWS S3, MinIO). SeaweedFS may not: then warn loudly."""
    cfg = ServerSettings(
        database_url=SecretStr("postgresql+asyncpg://x@h/db"),
        s3_endpoint_url=s3.endpoint_url,
        s3_access_key=SecretStr(s3.access_key),
        s3_secret_key=SecretStr(s3.secret_key),
        s3_bucket=s3.bucket,
        s3_region=s3.region,
    )
    st = S3Storage(cfg)
    s3.client.put_object(Bucket=s3.bucket, Key="rng/in", Body=b"0123456789abcdef")
    url = await st.presign_get("rng/in", None, (4, 10))

    def get(headers: dict[str, str]) -> tuple[int, bytes]:
        req = urllib.request.Request(url, headers=headers)  # noqa: S310
        try:
            with urllib.request.urlopen(req, timeout=10) as r:  # noqa: S310
                return r.status, bytes(r.read())
        except urllib.error.HTTPError as exc:
            return exc.code, b""

    assert get({"Range": "bytes=4-9"}) == (206, b"456789")
    other, none = get({"Range": "bytes=0-3"})[0], get({})[0]
    # If-Range is an unsigned header: SigV4 backends may accept it (403 or the same slice is
    # fine); the risk is a backend that drops the Range on an ETag mismatch (200 / wider body)
    if_range = get({"Range": "bytes=4-9", "If-Range": '"bogus"'})
    observed = (other, none, if_range[0])  # (other range, no range, extra If-Range header)
    if (other, none) != (403, 403) or if_range not in ((403, b""), (206, b"456789")):
        msg = (
            "backend does NOT enforce the signed Range / signed-header set "
            f"(other range, no range, extra If-Range) = {observed}; input GET URLs are not "
            "slice-scoped on this store (SPEC S-2)"
        )
        print(f"WARNING: {msg}")
        warnings.warn(msg, stacklevel=1)


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


@pytest.mark.integration
async def test_presigned_post_enforces_size_cap(s3: S3Env) -> None:
    import httpx

    cfg = ServerSettings(
        database_url=SecretStr("postgresql+asyncpg://x@h/db"),
        s3_endpoint_url=s3.endpoint_url,
        s3_access_key=SecretStr(s3.access_key),
        s3_secret_key=SecretStr(s3.secret_key),
        s3_bucket=s3.bucket,
        s3_region=s3.region,
    )
    st = S3Storage(cfg)
    cap = 1000

    async def post(key: str, data: bytes, mutate: dict[str, str] | None = None) -> httpx.Response:
        p = await st.presign_post(key, cap)
        fields = {**p.fields, **(mutate or {})}
        async with httpx.AsyncClient(timeout=30) as c:
            return await c.post(p.url, data=fields, files={"file": ("o.jsonl", data)})

    # (a) exactly at the cap is accepted
    r = await post("post/a", b"x" * cap)
    assert r.is_success, r.status_code
    assert await st.head("post/a") == cap
    # (b) one byte over is refused and nothing is stored
    r = await post("post/b", b"x" * (cap + 1))
    assert not r.is_success
    assert await st.head("post/b") is None
    # (c) an oversize POST onto an existing key leaves the old object intact
    s3.client.put_object(Bucket=s3.bucket, Key="post/c", Body=b"old")
    r = await post("post/c", b"y" * (cap + 1))
    assert not r.is_success
    assert s3.client.get_object(Bucket=s3.bucket, Key="post/c")["Body"].read() == b"old"
    # (d) a tampered key is not accepted (SeaweedFS answers 307, AWS 403) and nothing is stored
    r = await post("post/d", b"z", {"key": "post/d-evil"})
    assert not r.is_success
    assert await st.head("post/d-evil") is None and await st.head("post/d") is None
    # (e) a bad signature is 403
    r = await post("post/e", b"z", {"x-amz-signature": "0" * 64})
    assert r.status_code == 403
    assert await st.head("post/e") is None
