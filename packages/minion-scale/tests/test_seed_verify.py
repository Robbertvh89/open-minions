"""Seed and verify (TASK-7.2 a): unit checks plus a migrated Postgres + SeaweedFS."""

import stat
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from minion_scale.seed import (
    SeedError,
    SeedSpec,
    count_remaining,
    input_object,
    read_tokens_file,
    revoke_executors,
    seed_executors,
    seed_queue,
    upload_input,
    write_tokens_file,
)
from minion_scale.verify import VerifyReport, verify
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from conftest import REPO_ROOT, S3_BUCKET, S3Env

MODELS = REPO_ROOT / "deploy" / "scale" / "models.scale.yaml"
Sm = async_sessionmaker[AsyncSession]


def _spec(tasks: int = 2, subtasks: int = 5, **kw: Any) -> SeedSpec:
    return SeedSpec(tasks=tasks, subtasks=subtasks, models_file=MODELS, bucket=S3_BUCKET, **kw)


# --- unit ----------------------------------------------------------------------------------------


def test_models_file_is_a_fake_model() -> None:
    from minion_server.models_registry import load_models

    m = load_models(MODELS)["scale-fake"]
    assert m.engine.value == "fake" and m.type.value == "embedding"


@pytest.mark.parametrize(
    "kw",
    [{"tasks": 0}, {"subtasks": 0}, {"team": "Bad Team"}, {"prefix": "UP"}, {"records": 0}],
)
def test_spec_rejects_bad_input(kw: dict[str, Any]) -> None:
    with pytest.raises(SeedError):
        _spec(**kw).validate()


def test_input_object_is_tiny_and_line_delimited() -> None:
    body = input_object(10)
    assert body.count(b"\n") == 10 and len(body) < 1024


def test_tokens_file_is_owner_only_and_never_shown(tmp_path: Path) -> None:
    from minion_scale.seed import ExecutorCred

    creds = [ExecutorCred(executor_id="e1", token=SecretStr("s3cr3t-token"))]
    assert "s3cr3t-token" not in repr(creds) and "s3cr3t-token" not in str(creds[0])
    path = tmp_path / "tokens.json"
    write_tokens_file(path, creds)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    with pytest.raises(FileExistsError):
        write_tokens_file(path, creds)
    back = read_tokens_file(path)
    assert back[0].token.get_secret_value() == "s3cr3t-token"
    path.chmod(0o644)
    with pytest.raises(SeedError, match="owner-only"):
        read_tokens_file(path)


# --- integration ---------------------------------------------------------------------------------


@pytest.fixture
async def sm(fresh_url: str) -> AsyncIterator[Sm]:
    eng = create_async_engine(fresh_url)
    yield async_sessionmaker(eng, expire_on_commit=False)
    await eng.dispose()


async def _one(sm: Sm, sql: str) -> Any:
    async with sm() as s:
        return (await s.execute(text(sql))).scalar_one()


@pytest.mark.integration
async def test_seed_creates_pending_queue_and_hashed_tokens(sm: Sm, s3: S3Env) -> None:
    spec = _spec(tasks=3, subtasks=7)
    size = upload_input(s3.client, spec)
    res = await seed_queue(sm, spec, input_bytes=size)
    assert (res.tasks, res.subtasks) == (3, 21)
    assert await _one(sm, "SELECT count(*) FROM minion_task WHERE status='PENDING'") == 3
    assert await _one(sm, "SELECT sum(total_subtasks) FROM minion_task") == 21
    assert await _one(sm, "SELECT count(*) FROM minion_subtask WHERE status='PENDING'") == 21
    assert await _one(sm, "SELECT count(DISTINCT input_object_key) FROM minion_subtask") == 1
    assert await _one(sm, "SELECT min(byte_end) FROM minion_subtask") == size
    assert await count_remaining(sm) == 21
    assert await _one(sm, "SELECT count(*) FROM minion_task WHERE allowed_pool = 'scale'") == 3
    creds = await seed_executors(sm, 4)
    assert len({c.executor_id for c in creds}) == 4
    assert await _one(sm, "SELECT count(*) FROM minion_executor WHERE pool = 'scale'") == 4
    assert await _one(sm, "SELECT max(first_record) FROM minion_subtask") == 6 * spec.records
    assert await revoke_executors(sm, [c.executor_id for c in creds]) == 4
    assert await _one(sm, "SELECT count(*) FROM minion_executor WHERE status = 'REVOKED'") == 4
    tokens = [c.token.get_secret_value() for c in creds]
    hashes = {r[0] for r in await _rows(sm, "SELECT token_hash FROM minion_executor")}
    assert hashes.isdisjoint(tokens)
    with pytest.raises(Exception):  # noqa: B017 - same output prefix: unique violation
        await seed_queue(sm, spec, input_bytes=size)


async def _rows(sm: Sm, sql: str) -> list[Any]:
    async with sm() as s:
        return list((await s.execute(text(sql))).all())


async def _drain_cleanly(sm: Sm, s3: S3Env, spec: SeedSpec) -> None:
    """What a perfect run leaves behind: every subtask SUCCEEDED once, its output promoted."""
    async with sm() as s, s.begin():
        await s.execute(
            text(
                """
                INSERT INTO minion_subtask_attempt
                  (team, project, task_name, subtask_id, attempt, acquirement_id, outcome,
                   started_at)
                SELECT team, project, task_name, subtask_id, 1, gen_random_uuid(),
                       'SUCCEEDED', now()
                FROM minion_subtask
                """
            )
        )
        await s.execute(
            text(
                """
                UPDATE minion_subtask SET status='SUCCEEDED', attempt=1,
                  output_uri='s3://' || :b || '/outputs/' || team || '/' || task_name || '/'
                             || lpad(subtask_id::text, 6, '0') || '.jsonl'
                """
            ),
            {"b": S3_BUCKET},
        )
        await s.execute(
            text("UPDATE minion_task SET status='SUCCEEDED', succeeded_subtasks=total_subtasks")
        )
    for r in await _rows(sm, "SELECT output_uri FROM minion_subtask"):
        key = r[0].removeprefix(f"s3://{S3_BUCKET}/")
        s3.client.put_object(Bucket=S3_BUCKET, Key=key, Body=b"{}")


def _failed(rep: VerifyReport) -> list[str]:
    return rep.failed


@pytest.mark.integration
async def test_verify_passes_clean_and_detects_double_success_and_orphan(sm: Sm, s3: S3Env) -> None:
    spec = _spec(tasks=2, subtasks=4, prefix="vfy", team="vfyteam")
    size = upload_input(s3.client, spec)
    await seed_queue(sm, spec, input_bytes=size)
    undrained = await verify(sm, team="vfyteam", s3=s3.client, bucket=S3_BUCKET)
    assert "nf3_all_subtasks_terminal" in _failed(undrained)  # PENDING work is not drained

    await _drain_cleanly(sm, s3, spec)
    clean = await verify(sm, team="vfyteam", s3=s3.client, bucket=S3_BUCKET)
    assert clean.ok, clean.to_dict()
    assert clean.counts["succeeded"] == 8

    # a second SUCCEEDED attempt for one subtask (double success)
    async with sm() as s, s.begin():
        await s.execute(
            text(
                """
                INSERT INTO minion_subtask_attempt
                  (team, project, task_name, subtask_id, attempt, acquirement_id, outcome,
                   started_at)
                SELECT team, project, task_name, subtask_id, 2, gen_random_uuid(),
                       'SUCCEEDED', now()
                FROM minion_subtask WHERE task_name='vfy-00001' AND subtask_id=0
                """
            )
        )
    dbl = await verify(sm, team="vfyteam", s3=s3.client, bucket=S3_BUCKET)
    assert _failed(dbl) == ["nf2_one_succeeded_attempt_per_succeeded_subtask"]
    assert dbl.checks[0].detail["samples"] == ["vfyteam/load/vfy-00001#0"]
    async with sm() as s, s.begin():
        await s.execute(text("DELETE FROM minion_subtask_attempt WHERE attempt=2"))

    # an acquirement that outlived its (already SUCCEEDED) subtask, with no heartbeat row
    async with sm() as s, s.begin():
        eid = (await s.execute(text("SELECT executor_id FROM minion_executor LIMIT 1"))).scalar()
        if eid is None:
            await s.execute(
                text(
                    "INSERT INTO minion_executor (executor_id, token_hash, hostname, owner)"
                    " VALUES (gen_random_uuid(), 'h', 'h', 'o')"
                )
            )
        await s.execute(
            text(
                """
                INSERT INTO minion_subtask_acquirement
                  (acquirement_id, team, project, task_name, subtask_id, executor_id, attempt)
                SELECT gen_random_uuid(), team, project, task_name, subtask_id,
                       (SELECT executor_id FROM minion_executor LIMIT 1), 1
                FROM minion_subtask WHERE task_name='vfy-00002' AND subtask_id=3
                """
            )
        )
    orphan = await verify(sm, team="vfyteam", s3=s3.client, bucket=S3_BUCKET)
    assert _failed(orphan) == ["nf3_no_orphan_acquirements"]
    assert orphan.checks[3].detail["samples"] == ["vfyteam/load/vfy-00002#3"]
    async with sm() as s, s.begin():
        await s.execute(text("DELETE FROM minion_subtask_acquirement"))

    # counters drift and a missing promoted object
    async with sm() as s, s.begin():
        await s.execute(
            text("UPDATE minion_task SET succeeded_subtasks=1 WHERE task_name='vfy-00001'")
        )
    assert _failed(await verify(sm, team="vfyteam")) == ["task_counters_match_subtasks"]
    async with sm() as s, s.begin():
        await s.execute(text("UPDATE minion_task SET succeeded_subtasks=4"))
    s3.client.delete_object(Bucket=S3_BUCKET, Key="outputs/vfyteam/vfy-00001/000002.jsonl")
    gone = await verify(sm, team="vfyteam", s3=s3.client, bucket=S3_BUCKET)
    assert _failed(gone) == ["nf2_final_objects_match_succeeded"]
    assert gone.checks[1].detail["missing"] == 1


def test_server_image_only_copies_the_scale_pyproject() -> None:
    """The workspace sync needs the member's metadata; the image must not carry its code."""
    lines = (REPO_ROOT / "deploy" / "Dockerfile").read_text().splitlines()
    copies = [ln for ln in lines if ln.startswith("COPY ") and "minion-scale" in ln]
    assert copies == [
        "COPY packages/minion-scale/pyproject.toml packages/minion-scale/pyproject.toml"
    ]
    sync = [ln for ln in lines if "uv sync" in ln]
    assert sync and all("--package minion-watchdog" in ln for ln in sync)
