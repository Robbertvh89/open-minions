"""Repo-wide fixtures. Integration fixtures start containers via testcontainers (needs Docker)."""

import time
import urllib.error
import urllib.request
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import boto3
import pytest
from alembic import command
from alembic.config import Config
from botocore.config import Config as BotoConfig
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

REPO_ROOT = Path(__file__).resolve().parent
S3_IMAGE = "chrislusf/seaweedfs:4.05"  # keep in sync with deploy/compose.yml
S3_ACCESS_KEY = "minion"  # matches deploy/seaweedfs/s3.json
S3_SECRET_KEY = "minion-secret"  # noqa: S105
S3_BUCKET = "minion"
S3_REGION = "us-east-1"


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine", driver="asyncpg") as pg:
        yield pg.get_connection_url()


@pytest.fixture(scope="session")
def migrated_url(postgres_url: str) -> str:
    cfg = Config(str(REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", postgres_url)
    command.upgrade(cfg, "head")
    return postgres_url


@pytest.fixture
async def engine(migrated_url: str) -> AsyncIterator[AsyncEngine]:
    eng = create_async_engine(migrated_url)
    yield eng
    await eng.dispose()


@dataclass(frozen=True)
class S3Env:
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket: str
    region: str
    client: "S3Client"


def _wait_http(url: str, timeout_s: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2):  # noqa: S310 - fixed local URL
                return
        except (urllib.error.URLError, OSError):
            time.sleep(0.5)
    raise TimeoutError(f"{url} not ready after {timeout_s}s")


@pytest.fixture(scope="session")
def s3() -> Iterator[S3Env]:
    """SeaweedFS (same image/command as deploy/compose.yml) with s3.json mounted, bucket created."""
    from testcontainers.core.container import DockerContainer

    container = (
        DockerContainer(S3_IMAGE)
        .with_command(
            "server -dir=/data -s3 -s3.port=9000 -s3.config=/etc/seaweedfs/s3.json -ip.bind=0.0.0.0"
        )
        .with_volume_mapping(
            str(REPO_ROOT / "deploy" / "seaweedfs" / "s3.json"), "/etc/seaweedfs/s3.json", "ro"
        )
        .with_exposed_ports(9000)
    )
    with container:
        endpoint = f"http://{container.get_container_host_ip()}:{container.get_exposed_port(9000)}"
        _wait_http(f"{endpoint}/status")
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            region_name=S3_REGION,
            config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
        )
        deadline = time.monotonic() + 30
        while True:
            try:
                client.create_bucket(Bucket=S3_BUCKET)
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.5)
        yield S3Env(endpoint, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET, S3_REGION, client)
