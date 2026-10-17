"""Property test: `list_tasks` paging equals a plain DB `ORDER BY`, under C and ICU collations.

Team slugs are chosen so their order differs between byte-wise (C) and ICU collation, and
`created_at` has heavy ties, so the cross-team cursor comparison is exercised in both orders.
"""

import asyncio
import random
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from alembic import command
from alembic.config import Config
from minion_common.enums import TaskStatus
from minion_server.repositories import tasks as repo
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from conftest import REPO_ROOT, seed_task

TEAMS = ["a-c", "ab", "a_b", "a-b", "a0b", "a0", "b"]
PROJECTS = ["p", "p-q", "pq"]
ORDER = "ORDER BY created_at DESC, team DESC, project DESC, task_name DESC"


@pytest.fixture(params=["default", "icu"])
async def collated(request: pytest.FixtureRequest, postgres_url: str) -> AsyncIterator[str]:
    name = f"c_{uuid.uuid4().hex[:16]}"
    base = make_url(postgres_url)
    admin = create_async_engine(base, isolation_level="AUTOCOMMIT")
    try:
        async with admin.connect() as conn:
            if request.param == "icu":
                try:
                    await conn.execute(
                        text(
                            f'CREATE DATABASE "{name}" TEMPLATE template0 LOCALE_PROVIDER icu'
                            " ICU_LOCALE 'en-US' LOCALE 'C'"
                        )
                    )
                except DBAPIError:
                    pytest.skip("postgres image without ICU support")
            else:
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


@pytest.mark.integration
async def test_paging_matches_order_by(collated: str) -> None:
    rng = random.Random(1234)  # noqa: S311
    eng = create_async_engine(collated)
    sm: async_sessionmaker[AsyncSession] = async_sessionmaker(eng, expire_on_commit=False)
    try:
        for i, team in enumerate(t for t in TEAMS for _ in range(rng.randint(3, 8))):
            await seed_task(
                sm,
                f"t{rng.choice(['', '-', '_'])}{i}",
                0,
                team=team,
                project=rng.choice(PROJECTS),
                status=rng.choice(["PENDING", "SUCCEEDED"]),
            )
        t0 = datetime(2026, 1, 1, tzinfo=UTC)
        async with sm() as s, s.begin():
            for team, project, name in (
                await s.execute(text("SELECT team, project, task_name FROM minion_task"))
            ).all():
                await s.execute(
                    text(
                        "UPDATE minion_task SET created_at = :c"
                        " WHERE team = :t AND project = :p AND task_name = :n"
                    ),
                    {
                        "c": t0 + timedelta(microseconds=rng.choice([0, 0, 1, 1, 2, 5])),
                        "t": team,
                        "p": project,
                        "n": name,
                    },
                )
        for _ in range(25):
            teams = rng.sample(TEAMS, rng.randint(1, len(TEAMS)))
            project = rng.choice([None, None, "p", "p-q"])
            status = rng.choice([None, None, TaskStatus.PENDING])
            limit = rng.choice([1, 2, 3, 5, 7, 50])
            where = ["team = ANY(:teams)"]
            params: dict[str, object] = {"teams": teams}
            if project:
                where.append("project = :proj")
                params["proj"] = project
            if status:
                where.append("status = :st")
                params["st"] = status.value
            cond = " AND ".join(where)
            sql = f"SELECT team, project, task_name FROM minion_task WHERE {cond} {ORDER}"  # noqa: S608
            async with sm() as s:
                expected = [
                    tuple(r)
                    for r in (
                        await s.execute(
                            text(sql),
                            params,
                        )
                    ).all()
                ]
            seen: list[tuple[str, str, str]] = []
            after = None
            for _page in range(1000):
                async with sm() as s, s.begin():
                    got, more = await repo.list_tasks(
                        s, teams=teams, project=project, status=status, after=after, limit=limit
                    )
                seen += [(t.team, t.project, t.task_name) for t, _ in got]
                if not more:
                    break
                last = got[-1][0]
                after = (last.created_at, last.team, last.project, last.task_name)
            assert seen == expected, (teams, project, status, limit)
    finally:
        await eng.dispose()
