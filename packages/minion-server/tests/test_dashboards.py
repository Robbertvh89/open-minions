"""Grafana dashboards read only the minion_ro views, as minion_readonly (TASK-6.5, FR-40).

The static tests need no database. The integration test runs every panel's SQL (Grafana macros
substituted) over a real login that is a member of `minion_readonly`.
"""

import json
import re
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

REPO_ROOT = Path(__file__).resolve().parents[3]
DASHBOARDS = sorted((REPO_ROOT / "deploy" / "grafana" / "dashboards").glob("*.json"))
RELATION = re.compile(r"\b(?:FROM|JOIN)\s+([A-Za-z_][\w.\"]*)", re.IGNORECASE)
_STEP = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def panels() -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for path in DASHBOARDS:
        for p in json.loads(path.read_text())["panels"]:
            for t in p["targets"]:
                out.append((f"{path.stem}/{p['title']}", t["rawSql"]))
    return out


def substitute_macros(sql: str) -> str:
    """The subset of Grafana's PostgreSQL macros the dashboards use, for a 6 h range."""

    def group(m: re.Match[str]) -> str:
        n, unit = int(m.group(2)), m.group(3)
        secs = n * _STEP[unit]
        return f'floor(extract(epoch from {m.group(1)})/{secs})*{secs} AS "time"'

    sql = re.sub(r"\$__timeGroupAlias\(\s*([\w.]+)\s*,\s*'(\d+)([smhd])'\s*\)", group, sql)
    sql = re.sub(
        r"\$__timeFilter\(\s*([\w.]+)\s*\)",
        r"\1 BETWEEN now() - interval '6 hours' AND now()",
        sql,
    )
    assert "$__" not in sql, sql
    return sql


def test_dashboards_exist_and_have_panels() -> None:
    assert [p.stem for p in DASHBOARDS] == ["fleet", "tasks"]
    assert len(panels()) >= 12
    for path in DASHBOARDS:
        d = json.loads(path.read_text())
        assert d["uid"] == f"minion-{path.stem}"
        ids = [p["id"] for p in d["panels"]]
        assert len(ids) == len(set(ids))
        assert all(p["datasource"]["uid"] == "minion-postgres" for p in d["panels"])


@pytest.mark.parametrize(("name", "sql"), panels())
def test_panel_queries_only_the_minion_ro_schema(name: str, sql: str) -> None:
    relations = RELATION.findall(sql)
    assert relations, name
    for rel in relations:
        assert rel.startswith("minion_ro."), f"{name}: reads {rel}"
    # no way around the views: no public.* / catalog / raw table names
    assert not re.search(r"\bminion_(task|subtask|executor|subtask_attempt)\b", sql), name
    assert not re.search(r"token_hash|public\.|pg_", sql, re.IGNORECASE), name


def test_static_check_catches_a_raw_table() -> None:
    assert RELATION.findall("SELECT 1 FROM minion_executor e JOIN minion_ro.task t ON true") == [
        "minion_executor",
        "minion_ro.task",
    ]


def test_macro_substitution_covers_every_panel() -> None:
    for _, sql in panels():
        substitute_macros(sql)


@pytest.fixture
async def ro_engine(fresh_url: str) -> AsyncIterator[AsyncEngine]:
    """A real LOGIN role that is a member of minion_readonly, connected to a migrated database."""
    login = f"t_ro_{uuid.uuid4().hex[:10]}"
    base = make_url(fresh_url)
    admin = create_async_engine(base)
    ro = create_async_engine(base.set(username=login, password="pw"))
    try:
        async with admin.begin() as conn:
            await conn.execute(
                text(f"CREATE ROLE {login} LOGIN PASSWORD 'pw' IN ROLE minion_readonly")
            )
            await conn.execute(
                text(
                    "INSERT INTO minion_executor (executor_id, token_hash, hostname, owner, pool,"
                    " engines, profile, budget, paused_reason, last_seen_at) VALUES"
                    " (gen_random_uuid(), 'secret-hash', 'h', 'alice', 'default', '{mlx}',"
                    " 'balanced', '{\"memory_gb\": 8}', 'thermal', now())"
                )
            )
        yield ro
    finally:
        await ro.dispose()
        await admin.dispose()
        cleanup = create_async_engine(base.set(database="postgres"), isolation_level="AUTOCOMMIT")
        async with cleanup.connect() as conn:
            await conn.execute(text(f"DROP DATABASE IF EXISTS {base.database} WITH (FORCE)"))
            await conn.execute(text(f"DROP ROLE IF EXISTS {login}"))
        await cleanup.dispose()


@pytest.mark.integration
async def test_every_panel_runs_as_minion_readonly(ro_engine: AsyncEngine) -> None:
    async with ro_engine.connect() as conn:
        for name, sql in panels():
            try:
                await conn.execute(text(substitute_macros(sql)))
            except DBAPIError as exc:  # pragma: no cover - failure detail
                raise AssertionError(f"{name}: {exc.orig}") from exc
            await conn.rollback()


@pytest.mark.integration
async def test_minion_readonly_cannot_read_secrets_or_write(ro_engine: AsyncEngine) -> None:
    denied: list[Any] = [
        "SELECT token_hash FROM minion_executor",
        "SELECT * FROM public.minion_task",
        "SELECT * FROM public.minion_subtask",
        "SELECT * FROM public.minion_subtask_attempt",
        "INSERT INTO minion_ro.task (team) VALUES ('x')",
        "UPDATE minion_ro.executor SET owner = 'x'",
        "DELETE FROM minion_ro.subtask",
        "CREATE TABLE public.t_ro_x (a int)",
        "CREATE TABLE minion_ro.t_ro_x (a int)",
    ]
    for sql in denied:
        async with ro_engine.connect() as conn:
            with pytest.raises(DBAPIError, match="permission denied"):
                await conn.execute(text(sql))
    # the views expose no secret column at all
    async with ro_engine.connect() as conn:
        cols = {
            (r[0], r[1])
            for r in await conn.execute(
                text(
                    "SELECT table_name, column_name FROM information_schema.columns"
                    " WHERE table_schema = 'minion_ro'"
                )
            )
        }
    names = {c for _, c in cols}
    assert not names & {"token_hash", "params", "input_object_key", "input_uri", "output_uri"}
    assert not names & {"output_uri", "acquirement_id", "budget", "message"}
    async with ro_engine.connect() as conn:
        res = await conn.execute(text("SELECT owner, paused_reason FROM minion_ro.executor"))
        row = res.one()
    assert tuple(row) == ("alice", "thermal")
