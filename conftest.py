"""Repo-wide fixtures. Integration fixtures start containers via testcontainers (needs Docker)."""

import asyncio
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import boto3
import pytest
from alembic import command
from alembic.config import Config
from botocore.config import Config as BotoConfig
from minion_common.enums import Engine
from minion_common.schemas import AcquireRequest, Budget, ModelEntry, TaskCreate
from minion_server.admin import issue_executor_token
from minion_server.auth import ExecutorPrincipal
from minion_server.db.models import Subtask
from minion_server.repositories import tasks as tasks_repo
from minion_server.settings import ServerSettings
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

REPO_ROOT = Path(__file__).resolve().parent
S3_IMAGE = "chrislusf/seaweedfs:4.05"  # keep in sync with deploy/compose.yml
S3_ACCESS_KEY = "minion"  # matches deploy/seaweedfs/s3.json
S3_SECRET_KEY = "minion-secret"  # noqa: S105
S3_BUCKET = "minion"
S3_REGION = "us-east-1"
TEST_MODELS_FILE = str(REPO_ROOT / "packages" / "minion-server" / "tests" / "models.test.yaml")


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


@pytest.fixture
async def fresh_url(postgres_url: str) -> AsyncIterator[str]:
    """A brand-new, migrated database on the shared Postgres server, dropped after the test.

    Use when a test needs an empty queue/tables regardless of what other tests left behind.
    """
    name = f"t_{uuid.uuid4().hex[:16]}"
    base = make_url(postgres_url)
    admin = create_async_engine(base, isolation_level="AUTOCOMMIT")
    try:
        async with admin.connect() as conn:
            await conn.execute(text(f'CREATE DATABASE "{name}"'))
        url = base.set(database=name).render_as_string(hide_password=False)
        cfg = Config(str(REPO_ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", url)
        await asyncio.to_thread(command.upgrade, cfg, "head")
        yield url
    finally:
        async with admin.connect() as conn:
            await conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        await admin.dispose()


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
def s3(request: pytest.FixtureRequest) -> Iterator[S3Env]:
    """SeaweedFS (same image/command as deploy/compose.yml) with s3.json mounted, bucket created."""
    from testcontainers.core.container import DockerContainer

    container = (
        DockerContainer(S3_IMAGE)
        .with_command(
            "server -dir=/data -s3 -s3.port=9000 -s3.config=/etc/seaweedfs/s3.json -ip.bind=0.0.0.0"
            # Small runners: the default 30 GB volume size limit with auto volume.max (derived
            # from free disk) can yield zero writable volumes -> PutObject InternalError.
            " -master.volumeSizeLimitMB=64 -volume.max=200 -master.volumePreallocate=false"
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
        # Warm-up: the first PutObject triggers volume growth, which can take >30 s to settle;
        # absorb that here (bounded) so no test's first raw PUT times out.
        deadline = time.monotonic() + 90
        while True:
            try:
                client.put_object(Bucket=S3_BUCKET, Key="_warmup", Body=b"x")
                client.delete_object(Bucket=S3_BUCKET, Key="_warmup")
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise
                time.sleep(1)
        try:
            yield S3Env(endpoint, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET, S3_REGION, client)
        finally:
            if request.session.testsfailed:  # CI diagnostics for S3 InternalError etc.
                out, err = container.get_logs()
                tail = (out + err).decode(errors="replace").splitlines()[-80:]
                print("\n--- seaweedfs log tail ---\n" + "\n".join(tail))


# --- server test helpers (seeding, executors, settings) ---


def _settings(url: str) -> ServerSettings:
    return ServerSettings(
        database_url=SecretStr(url),
        s3_access_key=SecretStr("ak"),
        s3_secret_key=SecretStr("sk"),
        models_file=TEST_MODELS_FILE,
        splitter_poll_s=60,
        heartbeat_min_interval_s=0,  # tests beat back-to-back; the limit has its own test
    )


def _budget(mem: float = 8.0) -> Budget:
    return Budget.model_validate(
        {"profile": "balanced", "memory_gb": mem, "gpu_duty_cycle": 0.5, "max_batch_size": 8}
    )


def _req(mem: float = 8.0, engines: tuple[Engine, ...] = (Engine.MLX,)) -> AcquireRequest:
    return AcquireRequest(engines=list(engines), budget=_budget(mem))


async def seed_task(
    sm: async_sessionmaker[AsyncSession],
    name: str,
    n: int,
    *,
    engine: Engine = Engine.MLX,
    memory_gb: float = 0.1,
    pool: str | None = None,
    priority: int = 0,
    status: str = "PENDING",
    team: str = "acq",
    model_id: str = "fake-embed",
) -> None:
    body = TaskCreate.model_validate(
        {
            "team": team,
            "project": "p",
            "task_name": name,
            "type": "embedding",
            "model": model_id,
            "input_uri": f"s3://minion/inputs/{team}/{name}/",
            "output_uri": f"s3://minion/outputs/{team}/{name}/",
            "priority": priority,
            "allowed_pool": pool,
        }
    )
    model = ModelEntry(
        model_id=model_id,
        type="embedding",
        engine=engine,
        ref={Engine.OLLAMA: "m:latest", Engine.FAKE: "fake"}.get(engine, "o/m"),
        revision={Engine.OLLAMA: "b" * 64, Engine.FAKE: None}.get(engine, "a" * 40),
        memory_gb=memory_gb,
    )
    async with sm() as s, s.begin():
        inserted = await tasks_repo.insert_task(
            s,
            body,
            model=model,
            input_uri=body.input_uri,
            output_uri=body.output_uri,
            created_by="t",
        )
        if not inserted:
            raise RuntimeError("seed task already exists")
        for i in range(n):
            s.add(
                Subtask(
                    team=team,
                    project="p",
                    task_name=name,
                    subtask_id=i,
                    input_object_key=f"inputs/{team}/{name}/a.jsonl",
                    byte_start=i * 100,
                    byte_end=i * 100 + 100,
                    first_record=i * 10,
                    record_count=10,
                )
            )
        await s.flush()
        await s.execute(
            text("UPDATE minion_task SET status=:st, total_subtasks=:n WHERE task_name=:name"),
            {"st": status, "n": n, "name": name},
        )


async def make_executors(
    sm: async_sessionmaker[AsyncSession], n: int, pool: str = "default"
) -> list[tuple[ExecutorPrincipal, str]]:
    out = []
    for i in range(n):
        eid, token = await issue_executor_token(sm, owner=f"o{i}", pool=pool, ttl_days=1)
        out.append((ExecutorPrincipal(uuid.UUID(eid), f"o{i}", pool), token))
    return out


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}
