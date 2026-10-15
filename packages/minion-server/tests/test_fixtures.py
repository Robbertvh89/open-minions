"""Smoke tests for the shared Postgres + SeaweedFS fixtures."""

import urllib.request

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from conftest import S3Env

pytestmark = pytest.mark.integration


async def test_postgres_fixture_is_migrated(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        col = await conn.execute(
            text(
                "SELECT is_nullable FROM information_schema.columns "
                "WHERE table_name='minion_executor' AND column_name='token_expires_at'"
            )
        )
    assert col.scalar_one() == "YES"


def test_s3_fixture_roundtrip_and_presigned_get(s3: S3Env) -> None:
    s3.client.put_object(Bucket=s3.bucket, Key="smoke/a.txt", Body=b"hello")
    url = s3.client.generate_presigned_url(
        "get_object", Params={"Bucket": s3.bucket, "Key": "smoke/a.txt"}, ExpiresIn=60
    )
    with urllib.request.urlopen(url, timeout=10) as resp:  # noqa: S310
        assert resp.read() == b"hello"
