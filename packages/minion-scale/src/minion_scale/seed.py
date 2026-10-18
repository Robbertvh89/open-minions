"""Operator-side bulk seeding (DEV ONLY).

Writes N tasks x M PENDING subtasks straight into PostgreSQL with `INSERT ... SELECT
generate_series`, bypassing the splitter (a splitter run per task would dominate the setup time).
Every slice points at one small input object uploaded once. This module is operator tooling, not
the server: the "SQL only in repositories/" rule is for the server's request path. Executor
tokens are issued through the admin path and kept in memory or a 0600 file, never printed.
"""

import json
import os
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from minion_common.enums import ExecutorStatus
from minion_common.schemas import SLUG_PATTERN
from minion_server.admin import issue_executor_token
from minion_server.db.models import Executor
from minion_server.models_registry import load_models
from pydantic import BaseModel, SecretStr, field_validator
from sqlalchemy import text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

DEFAULT_TEAM = "scale"
DEFAULT_PROJECT = "load"
DEFAULT_PREFIX = "scale"
DEFAULT_MODEL = "scale-fake"
DEFAULT_POOL = "scale"  # tasks and issued tokens live in their own pool: no real executor matches
CREATED_BY = "minion-scale"
RECORDS_PER_SUBTASK = 10
MAX_TASKS = 99_999
MAX_SUBTASKS_PER_TASK = 1_000_000
TOKENS_SCHEMA = "minion-scale-tokens/1"

_SLUG = re.compile(SLUG_PATTERN)

Sm = async_sessionmaker[AsyncSession]


class SeedError(Exception):
    """Invalid seed request. Never carries credentials."""


@dataclass(frozen=True)
class SeedSpec:
    tasks: int
    subtasks: int  # per task
    models_file: Path
    bucket: str
    team: str = DEFAULT_TEAM
    project: str = DEFAULT_PROJECT
    prefix: str = DEFAULT_PREFIX
    model_id: str = DEFAULT_MODEL
    records: int = RECORDS_PER_SUBTASK
    max_attempts: int = 5
    pool: str = DEFAULT_POOL

    def validate(self) -> None:
        if not 1 <= self.tasks <= MAX_TASKS:
            raise SeedError(f"tasks must be 1..{MAX_TASKS}")
        if not 1 <= self.subtasks <= MAX_SUBTASKS_PER_TASK:
            raise SeedError(f"subtasks must be 1..{MAX_SUBTASKS_PER_TASK}")
        if self.records < 1 or self.max_attempts < 1:
            raise SeedError("records and max_attempts must be >= 1")
        for what, value in (("team", self.team), ("project", self.project), ("pool", self.pool)):
            if not _SLUG.fullmatch(value):
                raise SeedError(f"{what} must be a lowercase slug")
        if not _SLUG.fullmatch(f"{self.prefix}-{self.tasks:05d}"):
            raise SeedError("prefix must give a lowercase slug task name")

    @property
    def input_key(self) -> str:
        return f"inputs/{self.team}/_scale/input.jsonl"

    @property
    def input_prefix_uri(self) -> str:
        return f"s3://{self.bucket}/inputs/{self.team}/_scale/"

    @property
    def output_root(self) -> str:
        return f"outputs/{self.team}/"


@dataclass(frozen=True)
class SeedResult:
    tasks: int
    subtasks: int
    input_bytes: int


def input_object(records: int = RECORDS_PER_SUBTASK) -> bytes:
    """The one shared, tiny input object: `records` synthetic JSONL lines (no real data)."""
    return b"".join(b'{"text":"scale-%d"}\n' % i for i in range(records))


def upload_input(client: "S3Client", spec: SeedSpec) -> int:
    body = input_object(spec.records)
    client.put_object(Bucket=spec.bucket, Key=spec.input_key, Body=body)
    return len(body)


_INSERT_TASKS = text(
    """
    WITH p AS (
        SELECT CAST(:team AS text) AS team, CAST(:project AS text) AS project,
               CAST(:prefix AS text) AS prefix, CAST(:bucket AS text) AS bucket,
               CAST(:type AS text) AS type, CAST(:model_id AS text) AS model_id,
               CAST(:engine AS text) AS engine, CAST(:input_uri AS text) AS input_uri
    )
    INSERT INTO minion_task (
        team, project, task_name, type, model_id, engine, model_memory_gb, input_uri, output_uri,
        records_per_subtask, max_attempts, allowed_pool, status, total_subtasks, created_by
    )
    SELECT p.team, p.project, p.prefix || '-' || lpad(g::text, 5, '0'), p.type, p.model_id,
           p.engine, :memory_gb, p.input_uri,
           's3://' || p.bucket || '/outputs/' || p.team || '/' || p.prefix || '-'
               || lpad(g::text, 5, '0') || '/',
           :records, :max_attempts, CAST(:pool AS text), 'PENDING', :subtasks,
           CAST(:created_by AS text)
    FROM p CROSS JOIN generate_series(1, :tasks) AS g
    """
)

_INSERT_SUBTASKS = text(
    """
    INSERT INTO minion_subtask (
        team, project, task_name, subtask_id, status, attempt, max_attempts, input_object_key,
        byte_start, byte_end, first_record, record_count
    )
    SELECT t.team, t.project, t.task_name, s, 'PENDING', 0, t.max_attempts,
           CAST(:input_key AS text), 0, :input_bytes, CAST(s AS bigint) * :records, :records
    FROM minion_task t
    CROSS JOIN generate_series(0, :subtasks - 1) AS s
    WHERE t.team = CAST(:team AS text) AND t.project = CAST(:project AS text)
      AND t.task_name LIKE CAST(:prefix AS text) || '-_____'
      AND t.created_by = CAST(:created_by AS text)
    """
)


async def seed_queue(sm: Sm, spec: SeedSpec, *, input_bytes: int) -> SeedResult:
    """Insert the tasks and their PENDING subtasks in one transaction."""
    spec.validate()
    model = load_models(spec.models_file).get(spec.model_id)
    if model is None:
        raise SeedError(f"model {spec.model_id!r} is not in {spec.models_file}")
    common = {"team": spec.team, "project": spec.project, "prefix": spec.prefix}
    async with sm() as session, session.begin():
        await session.execute(
            _INSERT_TASKS,
            {
                **common,
                "type": model.type.value,
                "model_id": model.model_id,
                "engine": model.engine.value,
                "memory_gb": model.memory_gb,
                "input_uri": spec.input_prefix_uri,
                "bucket": spec.bucket,
                "records": spec.records,
                "max_attempts": spec.max_attempts,
                "pool": spec.pool,
                "subtasks": spec.subtasks,
                "tasks": spec.tasks,
                "created_by": CREATED_BY,
            },
        )
        await session.execute(
            _INSERT_SUBTASKS,
            {
                **common,
                "input_key": spec.input_key,
                "input_bytes": input_bytes,
                "records": spec.records,
                "subtasks": spec.subtasks,
                "created_by": CREATED_BY,
            },
        )
    return SeedResult(spec.tasks, spec.tasks * spec.subtasks, input_bytes)


class ExecutorCred(BaseModel):
    executor_id: str
    token: SecretStr

    @field_validator("executor_id")
    @classmethod
    def _uuid(cls, v: str) -> str:
        return str(uuid.UUID(v))  # ValueError (-> a clean CLI error) on anything else

    def __repr__(self) -> str:  # never show the token, even via a stray print/log of the object
        return f"ExecutorCred(executor_id={self.executor_id!r})"


async def seed_executors(
    sm: Sm, n: int, *, pool: str = DEFAULT_POOL, ttl_days: int = 1, owner_prefix: str = "scale"
) -> list[ExecutorCred]:
    """Issue `n` executor tokens through the admin path (only hashes reach the database)."""
    if n < 1:
        raise SeedError("executors must be >= 1")
    out: list[ExecutorCred] = []
    for i in range(n):
        eid, token = await issue_executor_token(
            sm, owner=f"{owner_prefix}-{i:04d}", pool=pool, ttl_days=ttl_days
        )
        out.append(ExecutorCred(executor_id=eid, token=SecretStr(token)))
    return out


async def revoke_executors(sm: Sm, executor_ids: list[str], *, pool: str = DEFAULT_POOL) -> int:
    """Revoke the given executors, but only those in `pool` (a tampered tokens file cannot revoke
    a real executor); returns how many were revoked."""
    n = 0
    async with sm() as session, session.begin():
        for eid in executor_ids:
            result = await session.execute(
                update(Executor)
                .where(Executor.executor_id == uuid.UUID(eid), Executor.pool == pool)
                .values(status=ExecutorStatus.REVOKED.value)
            )
            n += int(result.rowcount)  # type: ignore[attr-defined]
    return n


def write_tokens_file(path: Path, creds: list[ExecutorCred]) -> None:
    """Write credentials to a new 0600 file (refuses to overwrite or follow a symlink)."""
    doc = {
        "schema": TOKENS_SCHEMA,
        "executors": [
            {"executor_id": c.executor_id, "token": c.token.get_secret_value()} for c in creds
        ],
    }
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as fh:
        json.dump(doc, fh)


def read_tokens_file(path: Path) -> list[ExecutorCred]:
    st = path.stat()
    if st.st_mode & 0o077:
        raise SeedError("tokens file must be owner-only (chmod 600)")
    doc = json.loads(path.read_text())
    if doc.get("schema") != TOKENS_SCHEMA:
        raise SeedError("not a minion-scale tokens file")
    return [ExecutorCred.model_validate(e) for e in doc["executors"]]


async def count_remaining(sm: Sm, *, team: str = DEFAULT_TEAM) -> int:
    """Subtasks of the seeded team that are not terminal yet (the drain condition)."""
    async with sm() as session:
        row = await session.execute(
            text(
                "SELECT count(*) FROM minion_subtask WHERE team = :team"
                " AND status IN ('PENDING', 'RUNNING')"
            ),
            {"team": team},
        )
        return int(row.scalar_one())


async def sum_remaining(sm: Sm, *, team: str = DEFAULT_TEAM) -> int:
    """Subtasks of the seeded team not yet terminal, from the task counters (one row per task,
    no scan of `minion_subtask`): the cheap progress poll of `run`."""
    async with sm() as session:
        row = await session.execute(
            text(
                "SELECT coalesce(sum(total_subtasks - succeeded_subtasks - failed_subtasks), 0)"
                " FROM minion_task WHERE team = :team AND status IN ('PENDING', 'RUNNING')"
            ),
            {"team": team},
        )
        return int(row.scalar_one())
