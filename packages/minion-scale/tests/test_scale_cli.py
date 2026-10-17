"""CLI guard rails: explicit settings, scale-DB refusal, exit codes, cleanup (TASK-7.2 fixes)."""

import asyncio
import os
import sys
from pathlib import Path

import pytest
from minion_common.enums import Engine
from minion_scale.cli import app, check_scale_db, check_scale_host
from minion_scale.seed import read_tokens_file
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from typer.testing import CliRunner

from conftest import REPO_ROOT, S3Env, seed_task, serve_status

MODELS = REPO_ROOT / "deploy" / "scale" / "models.scale.yaml"
REMOTE = "postgresql+asyncpg://u:secret-pw@db.example.com:5432/prod"


def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for k in list(os.environ):
        if k.startswith("MINION_"):
            monkeypatch.delenv(k)


def test_non_loopback_database_is_refused_without_the_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    _clean_env(monkeypatch)
    monkeypatch.setenv("MINION_DATABASE_URL", REMOTE)
    runner = CliRunner()
    for cmd in (["seed"], ["run", "--server=https://x", "--tokens-file=t"], ["explain"]):
        res = runner.invoke(app, cmd)
        assert res.exit_code == 2, cmd
        assert "refusing database host 'db.example.com'" in res.output
        assert "secret-pw" not in res.output
    # with the flag the host check passes (the next problem is the missing S3 settings)
    res = runner.invoke(app, ["seed", "--yes-this-is-a-scale-db"])
    assert res.exit_code == 2 and "MINION_S3_ENDPOINT_URL" in res.output


def test_host_guard_checks_query_hosts_and_loopback_forms() -> None:
    import typer

    for url in (
        "postgresql+asyncpg://u:pw@localhost/db?host=remote.example.com",
        "postgresql+asyncpg://u:pw@127.0.0.1/db?hostaddr=10.0.0.5",
        "postgresql+asyncpg://u:pw@10.1.2.3/db",
    ):
        with pytest.raises(typer.Exit):
            check_scale_host(url, False)
    for url in (
        "postgresql+asyncpg://u:pw@127.0.0.2/db",
        "postgresql+asyncpg://u:pw@[::1]/db",
        "postgresql+asyncpg://u:pw@localhost/db",
        "postgresql+asyncpg://u:pw@/db?host=/var/run/postgresql",
    ):
        check_scale_host(url, False)
    check_scale_host("postgresql+asyncpg://u:pw@remote/db?host=remote", True)


def test_settings_come_from_the_environment_only(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _clean_env(monkeypatch)
    (tmp_path / ".env").write_text(f"MINION_DATABASE_URL={REMOTE}\n")
    monkeypatch.chdir(tmp_path)
    res = CliRunner().invoke(app, ["verify"])
    assert res.exit_code == 2 and "MINION_DATABASE_URL" in res.output


@pytest.mark.integration
async def test_a_database_with_foreign_tasks_is_refused(fresh_url: str) -> None:
    import typer

    eng = create_async_engine(fresh_url)
    try:
        sm = async_sessionmaker(eng, expire_on_commit=False)
        await check_scale_db(sm, False)  # empty: fine
        await seed_task(sm, "real-work", 1, engine=Engine.FAKE)
        with pytest.raises(typer.Exit):
            await check_scale_db(sm, False)
        await check_scale_db(sm, True)
    finally:
        await eng.dispose()


async def _cli(env: dict[str, str], cwd: Path, *args: str) -> tuple[int, str]:
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "minion_scale.cli",
        *args,
        env=env,
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    out, _ = await proc.communicate()
    return proc.returncode or 0, out.decode()


@pytest.mark.integration
async def test_run_exit_codes_and_cleanup(fresh_url: str, s3: S3Env, tmp_path: Path) -> None:
    env = {
        **{k: v for k, v in os.environ.items() if not k.startswith("MINION_")},
        "MINION_DATABASE_URL": fresh_url,
        "MINION_S3_ENDPOINT_URL": s3.endpoint_url,
        "MINION_S3_ACCESS_KEY": s3.access_key,
        "MINION_S3_SECRET_KEY": s3.secret_key,
        "MINION_S3_BUCKET": s3.bucket,
    }
    tokens = tmp_path / "tokens.json"
    rc, out = await _cli(
        env,
        tmp_path,
        "seed",
        "--tasks=1",
        "--subtasks=3",
        "--executors=2",
        f"--tokens-file={tokens}",
        f"--models-file={MODELS}",
    )
    assert rc == 0, out
    # all sims are rejected by the server: the run must end by itself with exit 1
    for srv in serve_status(401):
        rc, out = await asyncio.wait_for(
            _cli(
                env, tmp_path, "run", f"--server={srv}", f"--tokens-file={tokens}", "--poll-s=0.1"
            ),
            120,
        )
    assert rc == 1 and "did not drain" in out, out
    # an insecure server URL is a usage error, not a hang
    rc, out = await _cli(
        env, tmp_path, "run", "--server=http://example.com", f"--tokens-file={tokens}"
    )
    assert rc == 2 and "insecure" in out, out

    creds = read_tokens_file(tokens)
    eng = create_async_engine(fresh_url)
    try:
        async with eng.connect() as c:
            pools = (await c.execute(text("SELECT DISTINCT pool FROM minion_executor"))).all()
            tasks = (await c.execute(text("SELECT DISTINCT allowed_pool FROM minion_task"))).all()
        assert [r[0] for r in pools] == ["scale"] and [r[0] for r in tasks] == ["scale"]
        rc, out = await _cli(env, tmp_path, "cleanup", f"--tokens-file={tokens}")
        assert rc == 0 and "revoked 2" in out, out
        assert not tokens.exists()
        async with eng.connect() as c:
            revoked = (
                await c.execute(text("SELECT count(*) FROM minion_executor WHERE status='REVOKED'"))
            ).scalar_one()
        assert revoked == len(creds) == 2
    finally:
        await eng.dispose()
    for cred in creds:
        assert cred.token.get_secret_value() not in out
