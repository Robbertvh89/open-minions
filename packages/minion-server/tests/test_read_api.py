"""Read API: task list, subtasks, fleet (TASK-6.4, SPEC §6.1)."""

import asyncio
import hashlib
import re
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from minion_common.client import ApiError, UserClient
from minion_common.enums import SubtaskStatus
from minion_common.schemas import ExecutorPage
from minion_server.app import create_app
from minion_server.cursor import (
    ExecutorCursor,
    InvalidCursorError,
    SubtaskCursor,
    TaskCursor,
    decode_executor_cursor,
    decode_subtask_cursor,
    decode_task_cursor,
    encode_cursor,
)
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from conftest import _auth, _req, _settings, make_executors, seed_task

AGE_ONE = (
    "UPDATE minion_executor SET last_seen_at = now() - make_interval(mins => :m)"
    " WHERE executor_id = :i"
)
ALICE, BOB, OP = "k-alice", "k-bob", "k-op"


def _h(k: str) -> str:
    return hashlib.sha256(k.encode()).hexdigest()


# --- unit: cursor ------------------------------------------------------------------------------


def test_cursor_roundtrip_and_rejects_garbage() -> None:
    from datetime import UTC, datetime

    c = TaskCursor(created_at=datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=UTC),
                   team="t", project="p", task_name="n")  # fmt: skip
    assert decode_task_cursor(encode_cursor(c)) == c
    s = SubtaskCursor(subtask_id=7)
    assert decode_subtask_cursor(encode_cursor(s)) == s
    for bad in ["", "!!!", "e30", "A" * 600, encode_cursor(s), "é"]:
        with pytest.raises(InvalidCursorError):
            decode_task_cursor(bad)
    with pytest.raises(InvalidCursorError):
        decode_subtask_cursor(encode_cursor(c))
    import base64

    e = ExecutorCursor(registered_at=datetime(2026, 1, 2, tzinfo=UTC), executor_id=uuid.uuid4())
    assert decode_executor_cursor(encode_cursor(e)) == e
    with pytest.raises(InvalidCursorError):
        decode_executor_cursor(encode_cursor(s))
    legacy = base64.urlsafe_b64encode(
        b'{"k":"executor","executor_id":"%s"}' % str(e.executor_id).encode()
    )
    with pytest.raises(InvalidCursorError):
        decode_executor_cursor(legacy.decode())
    forged = base64.urlsafe_b64encode(b'{"k":"subtask","subtask_id":-1}').decode()
    with pytest.raises(InvalidCursorError):
        decode_subtask_cursor(forged)
    forged = base64.urlsafe_b64encode(b'{"k":"subtask","subtask_id":"1"}').decode()
    with pytest.raises(InvalidCursorError):
        decode_subtask_cursor(forged)


# --- integration -------------------------------------------------------------------------------


@pytest.fixture
async def http(fresh_url: str, tmp_path: Path) -> AsyncIterator[tuple[AsyncClient, Any]]:
    keys = tmp_path / "keys.yaml"
    keys.write_text(
        "keys:\n"
        f"  - {{name: alice, sha256: {_h(ALICE)}, teams: [ra, rb]}}\n"
        f"  - {{name: bob, sha256: {_h(BOB)}, teams: [rc]}}\n"
        f"  - {{name: op, sha256: {_h(OP)}, teams: [ra], operator: true}}\n"
    )
    cfg = _settings(fresh_url).model_copy(update={"api_keys_file": str(keys)})
    app = create_app(cfg)
    async with app.router.lifespan_context(app):
        app.state.splitter_task.cancel()
        await asyncio.gather(app.state.splitter_task, return_exceptions=True)
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            yield c, app


def _a(key: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {key}"}


@pytest.mark.integration
async def test_list_tasks_scoped_to_callers_teams(http: tuple[AsyncClient, Any]) -> None:
    c, app = http
    sm = app.state.sessionmaker
    await seed_task(sm, "a1", 3, team="ra")
    await seed_task(sm, "b1", 1, team="rb")
    await seed_task(sm, "c1", 1, team="rc")
    r = await c.get("/api/v1/tasks", headers=_a(ALICE))
    assert r.status_code == 200
    got = {(t["team"], t["task_name"]) for t in r.json()["items"]}
    assert got == {("ra", "a1"), ("rb", "b1")}
    assert r.json()["next_cursor"] is None
    by_name = {t["task_name"]: t for t in r.json()["items"]}
    assert by_name["a1"]["counts"]["pending"] == 3
    # another team -> 403, own team filter works
    assert (await c.get("/api/v1/tasks?team=rc", headers=_a(ALICE))).status_code == 403
    r = await c.get("/api/v1/tasks?team=rb", headers=_a(ALICE))
    assert [t["task_name"] for t in r.json()["items"]] == ["b1"]
    r = await c.get("/api/v1/tasks?status=SUCCEEDED", headers=_a(ALICE))
    assert r.json()["items"] == []
    assert (await c.get("/api/v1/tasks?status=bogus", headers=_a(ALICE))).status_code == 422
    assert (await c.get("/api/v1/tasks?limit=101", headers=_a(ALICE))).status_code == 422
    assert (await c.get("/api/v1/tasks?limit=0", headers=_a(ALICE))).status_code == 422
    assert (await c.get("/api/v1/tasks")).status_code == 401


@pytest.mark.integration
async def test_list_tasks_pagination_stable_under_inserts_and_ties(
    http: tuple[AsyncClient, Any],
) -> None:
    c, app = http
    sm: async_sessionmaker[AsyncSession] = app.state.sessionmaker
    for i in range(7):
        await seed_task(sm, f"t{i}", 1, team="ra")
    # force created_at ties across the page boundary: tie-breaker must keep the order total
    async with sm() as s, s.begin():
        await s.execute(
            text("UPDATE minion_task SET created_at = '2026-01-01T00:00:00Z' "
                 "WHERE task_name IN ('t2','t3','t4')")
        )  # fmt: skip
    r = await c.get("/api/v1/tasks?limit=3", headers=_a(ALICE))
    p1 = r.json()
    assert len(p1["items"]) == 3 and p1["next_cursor"]
    # concurrent inserts between pages must not shift or duplicate the remaining pages
    for i in range(3):
        await seed_task(sm, f"new{i}", 1, team="ra")
    seen = [t["task_name"] for t in p1["items"]]
    cursor = p1["next_cursor"]
    while cursor:
        r = await c.get(f"/api/v1/tasks?limit=3&cursor={cursor}", headers=_a(ALICE))
        assert r.status_code == 200, r.text
        seen += [t["task_name"] for t in r.json()["items"]]
        cursor = r.json()["next_cursor"]
    assert len(seen) == len(set(seen)) == 7
    assert set(seen) == {f"t{i}" for i in range(7)}
    # exactly-full last page: no dangling cursor
    r = await c.get("/api/v1/tasks?limit=100&team=ra", headers=_a(ALICE))
    assert r.json()["next_cursor"] is None and len(r.json()["items"]) == 10


@pytest.mark.integration
async def test_tampered_cursor_is_422(http: tuple[AsyncClient, Any]) -> None:
    c, app = http
    await seed_task(app.state.sessionmaker, "x", 1, team="ra")
    await seed_task(app.state.sessionmaker, "y", 1, team="ra")
    cursor = (await c.get("/api/v1/tasks?limit=1", headers=_a(ALICE))).json()["next_cursor"]
    for bad in [
        cursor[:-3] + "AAA",
        "garbage",
        cursor + "x",
        encode_cursor(SubtaskCursor(subtask_id=1)),
    ]:
        r = await c.get("/api/v1/tasks", params={"cursor": bad}, headers=_a(ALICE))
        assert r.status_code == 422, bad
        assert r.json()["detail"]["code"] == "INVALID_CURSOR"
    r = await c.get("/api/v1/tasks/ra/p/x/subtasks?cursor=garbage", headers=_a(ALICE))
    assert r.status_code == 422
    # oversized cursors are rejected by the decoder, not by a Query length limit
    r = await c.get("/api/v1/tasks", params={"cursor": "A" * 5000}, headers=_a(ALICE))
    assert r.status_code == 422 and r.json()["detail"]["code"] == "INVALID_CURSOR"
    r = await c.get("/api/v1/executors", params={"cursor": "A" * 5000}, headers=_a(OP))
    assert r.status_code == 422 and r.json()["detail"]["code"] == "INVALID_CURSOR"


@pytest.mark.integration
async def test_list_tasks_counts_one_statement(http: tuple[AsyncClient, Any]) -> None:
    from minion_server.repositories import tasks as repo
    from sqlalchemy import event

    _, app = http
    sm = app.state.sessionmaker
    for i in range(50):
        await seed_task(sm, f"q{i:02d}", 2, team="ra")
    stmts: list[str] = []
    engine = app.state.engine.sync_engine
    event.listen(engine, "before_cursor_execute", lambda *a: stmts.append(a[2]))
    for n in (5, 50):
        stmts.clear()
        async with sm() as s, s.begin():
            rows, _ = await repo.list_tasks(
                s, teams=["ra"], project=None, status=None, after=None, limit=n
            )
        assert len(rows) == n and len(stmts) == 1  # page and counts in one statement
        assert not re.search(r"\bminion_subtask\b(?!_)", stmts[0])
    stmts.clear()
    async with sm() as s, s.begin():
        got = await repo.get_task(s, "ra", "p", "q00")
    assert got is not None and len(stmts) == 1
    assert got[1][SubtaskStatus.PENDING] == 2


def test_derive_counts_pending_is_remainder_clamped() -> None:
    from minion_server.repositories.tasks import derive_counts

    c = derive_counts(10, 2, 3, 1, 1)
    assert c[SubtaskStatus.PENDING] == 3 and c[SubtaskStatus.RUNNING] == 2
    # SPLITTING / unfinished split: total is 0, so pending clamps at 0
    c = derive_counts(0, 0, 0, 0, 3)
    assert c[SubtaskStatus.PENDING] == 0 and c[SubtaskStatus.CANCELLED] == 3


def _relations(plan: Any) -> set[str]:
    out: set[str] = set()
    if isinstance(plan, dict):
        if "Relation Name" in plan:
            out.add(plan["Relation Name"])
        for v in plan.values():
            out |= _relations(v)
    elif isinstance(plan, list):
        for v in plan:
            out |= _relations(v)
    return out


@pytest.mark.integration
async def test_counts_plans_never_touch_subtasks(http: tuple[AsyncClient, Any]) -> None:
    import json

    from minion_server.db.models import Task
    from minion_server.repositories import tasks as repo
    from sqlalchemy import select
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.orm import aliased

    _, app = http
    sm = app.state.sessionmaker
    await seed_task(sm, "big", 1, team="ra")
    async with sm() as s, s.begin():
        await s.execute(
            text(
                "INSERT INTO minion_subtask (team, project, task_name, subtask_id, status,"
                " input_object_key, byte_start, byte_end, first_record, record_count)"
                " SELECT 'ra', 'p', 'big', g, 'PENDING', 'k', 0, 1, g, 1"
                " FROM generate_series(1, 20000) g"
            )
        )
        await s.execute(text("UPDATE minion_task SET total_subtasks = 20001 WHERE task_name='big'"))
        await s.execute(text("ANALYZE minion_subtask"))
    t = aliased(Task, repo._list_tasks_stmt(["ra"], None, None, None, 10).subquery("page"))
    list_stmt = select(t, *repo._count_columns(t))
    detail_stmt = select(Task, *repo._count_columns(Task)).where(
        Task.team == "ra", Task.project == "p", Task.task_name == "big"
    )
    for seqscan in ("on", "off"):
        for stmt in (list_stmt, detail_stmt):
            dialect = postgresql.dialect()  # type: ignore[no-untyped-call]
            sql = str(stmt.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))
            async with sm() as s, s.begin():
                await s.execute(text(f"SET LOCAL enable_seqscan = {seqscan}"))
                raw = (await s.execute(text(f"EXPLAIN (FORMAT JSON) {sql}"))).scalar_one()
            plan = raw if isinstance(raw, list) else json.loads(raw)
            rels = _relations(plan)
            assert "minion_subtask" not in rels, (seqscan, rels)
            assert "minion_task_counter" in rels


@pytest.mark.integration
async def test_list_subtasks_pagination_filter_and_rules(http: tuple[AsyncClient, Any]) -> None:
    c, app = http
    sm = app.state.sessionmaker
    await seed_task(sm, "s", 5, team="ra")
    async with sm() as s, s.begin():
        await s.execute(
            text(
                "UPDATE minion_subtask SET status='FAILED', attempt=3, "
                'error=\'{"code":"OOM","message":"secret"}\'::jsonb WHERE subtask_id IN (1,3)'
            )
        )
    base = "/api/v1/tasks/ra/p/s/subtasks"
    r = await c.get(f"{base}?limit=2", headers=_a(ALICE))
    p = r.json()
    assert [i["subtask_id"] for i in p["items"]] == [0, 1] and p["next_cursor"]
    assert set(p["items"][0]) == {
        *("subtask_id", "status", "attempt", "max_attempts"),
        *("record_count", "error_code", "updated_at"),
    }
    assert p["items"][1]["error_code"] == "OOM" and "secret" not in r.text
    ids = [0, 1]
    cur = p["next_cursor"]
    while cur:
        p = (await c.get(f"{base}?limit=2&cursor={cur}", headers=_a(ALICE))).json()
        ids += [i["subtask_id"] for i in p["items"]]
        cur = p["next_cursor"]
    assert ids == [0, 1, 2, 3, 4]
    r = await c.get(f"{base}?status=FAILED", headers=_a(ALICE))
    assert [i["subtask_id"] for i in r.json()["items"]] == [1, 3]
    assert (await c.get(f"{base}?status=nope", headers=_a(ALICE))).status_code == 422
    # same rules as get_task: other team 403, unknown task 404, bad slug 404
    assert (await c.get(f"{base}", headers=_a(BOB))).status_code == 403
    assert (await c.get("/api/v1/tasks/ra/p/zz/subtasks", headers=_a(ALICE))).status_code == 404
    assert (await c.get("/api/v1/tasks/ra/P!/s/subtasks", headers=_a(ALICE))).status_code == 404
    assert (await c.get(base)).status_code == 401


@pytest.mark.integration
async def test_executors_operator_only_no_token_hash_busy_online(
    http: tuple[AsyncClient, Any],
) -> None:
    c, app = http
    sm = app.state.sessionmaker
    assert (await c.get("/api/v1/executors", headers=_a(ALICE))).status_code == 403
    assert (await c.get("/api/v1/executors")).status_code == 401
    await seed_task(sm, "f", 1, team="ra")
    (busy_p, busy_tok), (idle_p, _), (stale_p, _), (never_p, _), (rev_p, _) = await make_executors(
        sm, 5
    )
    r = await c.post("/api/v1/executor/acquire", json=_req().model_dump(mode="json"),
                     headers=_auth(busy_tok))  # fmt: skip
    assert r.status_code == 200, r.text

    async def sql(stmt: str, **kw: Any) -> None:
        async with sm() as s, s.begin():
            await s.execute(text(stmt), kw)

    async def fleet() -> dict[str, dict[str, Any]]:
        r = await c.get("/api/v1/executors", headers=_a(OP))
        assert r.status_code == 200
        assert "token_hash" not in r.text and "token_hash" not in r.text.lower()
        assert r.json()["next_cursor"] is None
        return {e["executor_id"]: e for e in r.json()["items"]}

    def flags(f: dict[str, dict[str, Any]], p: Any) -> tuple[bool, bool]:
        e = f[str(p.executor_id)]
        return e["online"], e["busy"]

    await sql("UPDATE minion_executor SET last_seen_at = now() WHERE executor_id = :i",
              i=idle_p.executor_id)  # fmt: skip
    await sql(AGE_ONE, m=5,
              i=stale_p.executor_id)  # fmt: skip
    await sql("UPDATE minion_executor SET last_seen_at = NULL WHERE executor_id = :i",
              i=never_p.executor_id)  # fmt: skip
    await sql("UPDATE minion_executor SET status='REVOKED', last_seen_at = now() "
              "WHERE executor_id = :i", i=rev_p.executor_id)  # fmt: skip
    f = await fleet()
    assert flags(f, busy_p) == (True, True)  # acquire touched last_seen_at + heartbeat row
    assert flags(f, idle_p) == (True, False)
    assert flags(f, stale_p) == (False, False)  # idle 5 min > 120 s window
    assert flags(f, never_p) == (False, False)
    assert flags(f, rev_p) == (False, False)
    assert f[str(busy_p.executor_id)]["last_heartbeat_at"] is not None
    # a busy executor stays online up to the heartbeat window (10 min), not the idle one
    await sql(AGE_ONE, m=5,
              i=busy_p.executor_id)  # fmt: skip
    await sql(
        "UPDATE minion_executor_heartbeat SET last_seen_at = now() - make_interval(mins => :m)", m=5
    )
    assert flags(await fleet(), busy_p) == (True, True)
    await sql(AGE_ONE, m=11,
              i=busy_p.executor_id)  # fmt: skip
    await sql(
        "UPDATE minion_executor_heartbeat SET last_seen_at = now() - make_interval(mins => :m)",
        m=11,
    )
    assert flags(await fleet(), busy_p) == (False, True)
    # finishing the subtask (release) drops the heartbeat row: no longer busy
    acq = r.json()["acquirement_id"]
    r = await c.post(f"/api/v1/executor/subtasks/{acq}/release", headers=_auth(busy_tok))
    assert r.status_code == 204, r.text
    assert flags(await fleet(), busy_p)[1] is False


@pytest.mark.integration
async def test_client_methods(http: tuple[AsyncClient, Any]) -> None:
    _c, app = http
    await seed_task(app.state.sessionmaker, "cl", 3, team="ra")
    transport = ASGITransport(app=app)
    async with UserClient("http://localhost", SecretStr(OP), transport=transport) as uc:
        page = await uc.list_tasks(team="ra", limit=1)
        assert [t.task_name for t in page.items] == ["cl"]
        sp = await uc.list_subtasks("ra", "p", "cl", status=SubtaskStatus.PENDING, limit=2)
        assert len(sp.items) == 2 and sp.next_cursor
        fleet = await uc.list_executors(limit=5)
        assert isinstance(fleet, ExecutorPage) and fleet.next_cursor is None
        with pytest.raises(ApiError) as ei:
            await uc.list_tasks(team="rc")
        assert ei.value.status_code == 403


@pytest.mark.integration
async def test_list_tasks_status_and_project_filters(http: tuple[AsyncClient, Any]) -> None:
    c, app = http
    sm = app.state.sessionmaker
    await seed_task(sm, "ok", 1, team="ra")
    await seed_task(sm, "wait", 1, team="ra")
    await seed_task(sm, "other", 1, team="ra", project="q")
    async with sm() as s, s.begin():
        await s.execute(text("UPDATE minion_task SET status='SUCCEEDED' WHERE task_name='ok'"))

    async def names(qs: str) -> set[str]:
        r = await c.get(f"/api/v1/tasks?{qs}", headers=_a(ALICE))
        assert r.status_code == 200, r.text
        return {t["task_name"] for t in r.json()["items"]}

    assert await names("status=SUCCEEDED") == {"ok"}
    assert await names("status=PENDING") == {"wait", "other"}
    assert await names("project=q") == {"other"}
    assert await names("project=p") == {"ok", "wait"}
    assert await names("project=p&status=PENDING") == {"wait"}


@pytest.mark.integration
async def test_multi_team_tie_across_page_boundary(http: tuple[AsyncClient, Any]) -> None:
    c, app = http
    sm: async_sessionmaker[AsyncSession] = app.state.sessionmaker
    for team, n in [("ra", 3), ("rb", 3)]:
        for i in range(n):
            await seed_task(sm, f"{team}{i}", 1, team=team)
    # every row shares one created_at except one: ties span each page boundary and both teams
    async with sm() as s, s.begin():
        await s.execute(text("UPDATE minion_task SET created_at = '2026-01-01T00:00:00Z'"))
        await s.execute(text("UPDATE minion_task SET created_at = '2026-02-01T00:00:00Z' "
                             "WHERE task_name = 'ra0'"))  # fmt: skip
        expected = [
            (r[0], r[1])
            for r in (
                await s.execute(
                    text(
                        "SELECT team, task_name FROM minion_task ORDER BY created_at DESC,"
                        " team DESC, project DESC, task_name DESC"
                    )
                )
            ).all()
        ]
    for limit in (1, 2, 3, 4):
        seen: list[tuple[str, str]] = []
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {"limit": limit}
            if cursor:
                params["cursor"] = cursor
            r = await c.get("/api/v1/tasks", params=params, headers=_a(ALICE))
            assert r.status_code == 200, r.text
            seen += [(t["team"], t["task_name"]) for t in r.json()["items"]]
            cursor = r.json()["next_cursor"]
            if not cursor:
                break
        assert seen == expected, limit


@pytest.mark.integration
@pytest.mark.parametrize("with_cursor", [False, True])
@pytest.mark.parametrize("cursor_team", ["aa", "rb", "zz"])  # below, equal to, above branch teams
async def test_multi_team_branches_use_team_created_index(
    http: tuple[AsyncClient, Any], with_cursor: bool, cursor_team: str
) -> None:
    from minion_server.repositories import tasks as repo
    from sqlalchemy.dialects import postgresql

    if not with_cursor and cursor_team != "rb":
        pytest.skip("cursor team is irrelevant without a cursor")
    _, app = http
    sm: async_sessionmaker[AsyncSession] = app.state.sessionmaker
    await seed_task(sm, "e1", 1, team="ra")
    await seed_task(sm, "e2", 1, team="rb")
    after = (datetime(2030, 1, 1, tzinfo=UTC), cursor_team, "p", "zzz") if with_cursor else None
    stmt = repo._list_tasks_stmt(["ra", "rb"], None, None, after, 10)
    dialect = postgresql.dialect()  # type: ignore[no-untyped-call]
    sql = str(stmt.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))
    async with sm() as s, s.begin():
        await s.execute(text("SET LOCAL enable_seqscan = off"))
        plan = "\n".join(r[0] for r in (await s.execute(text("EXPLAIN " + sql))).all())
    # One scan per team branch; the final join lookup of the page rows may use any index (on this
    # 2-row table the planner ties between the PK, team_created and ix_minion_task_team_status).
    assert "Seq Scan" not in plan, plan
    lines = plan.splitlines()
    for team in ("ra", "rb"):
        conds = [i for i, ln in enumerate(lines) if f"(team = '{team}'" in ln]
        assert len(conds) == 1, plan
        # the Index Cond line sits right under its scan node
        assert "using ix_minion_task_team_created" in lines[conds[0] - 1], plan


@pytest.mark.integration
async def test_executors_pagination_and_filters(http: tuple[AsyncClient, Any]) -> None:
    c, app = http
    sm = app.state.sessionmaker
    made = await make_executors(sm, 5)
    async with sm() as s, s.begin():
        await s.execute(
            text("UPDATE minion_executor SET status='REVOKED' WHERE executor_id = :i"),
            {"i": made[0][0].executor_id},
        )
        await s.execute(
            text("UPDATE minion_executor SET pool='gpu' WHERE executor_id = :i"),
            {"i": made[1][0].executor_id},
        )
        # Heavy registered_at ties: the executor_id tiebreak must carry the paging.
        await s.execute(
            text(
                "UPDATE minion_executor SET registered_at = TIMESTAMPTZ '2026-01-01 00:00:00+00'"
                " WHERE executor_id <> :i"
            ),
            {"i": made[4][0].executor_id},
        )
        expected = [
            str(r[0])
            for r in (
                await s.execute(
                    text(
                        "SELECT executor_id FROM minion_executor"
                        " ORDER BY registered_at, executor_id"
                    )
                )
            ).all()
        ]
    seen: list[str] = []
    cursor: str | None = None
    while True:
        params: dict[str, Any] = {"limit": 2}
        if cursor:
            params["cursor"] = cursor
        r = await c.get("/api/v1/executors", params=params, headers=_a(OP))
        assert r.status_code == 200, r.text
        seen += [e["executor_id"] for e in r.json()["items"]]
        cursor = r.json()["next_cursor"]
        if not cursor:
            break
    assert seen == expected and len(set(seen)) == 5
    r = await c.get("/api/v1/executors?status=REVOKED", headers=_a(OP))
    assert [e["executor_id"] for e in r.json()["items"]] == [str(made[0][0].executor_id)]
    r = await c.get("/api/v1/executors?pool=gpu", headers=_a(OP))
    assert [e["executor_id"] for e in r.json()["items"]] == [str(made[1][0].executor_id)]
    for bad in ("limit=0", "limit=501", "status=nope", "pool=BAD!"):
        assert (await c.get(f"/api/v1/executors?{bad}", headers=_a(OP))).status_code == 422


@pytest.mark.integration
async def test_executors_budget_valid_populated_invalid_null(
    http: tuple[AsyncClient, Any],
) -> None:
    c, app = http
    sm = app.state.sessionmaker
    (good, _), (empty, _), (wrong, _), (none, _) = await make_executors(sm, 4)
    stored = {
        good.executor_id: '{"profile":"balanced","memory_gb":8,"gpu_duty_cycle":0.5,'
        '"max_batch_size":8}',
        empty.executor_id: "{}",
        wrong.executor_id: '{"profile":"balanced","memory_gb":"lots","gpu_duty_cycle":0.5,'
        '"max_batch_size":8}',
    }
    async with sm() as s, s.begin():
        for eid, b in stored.items():
            await s.execute(
                text("UPDATE minion_executor SET budget=CAST(:b AS jsonb) WHERE executor_id=:id"),
                {"b": b, "id": eid},
            )
    r = await c.get("/api/v1/executors", headers=_a(OP))
    assert r.status_code == 200, r.text
    by_id = {e["executor_id"]: e["budget"] for e in r.json()["items"]}
    assert by_id[str(good.executor_id)]["memory_gb"] == 8
    assert by_id[str(empty.executor_id)] is None
    assert by_id[str(wrong.executor_id)] is None
    assert by_id[str(none.executor_id)] is None


@pytest.mark.integration
async def test_fleet_summary_aggregate_ignores_unreadable_budgets(
    http: tuple[AsyncClient, Any],
) -> None:
    from minion_server.api.fleet import fleet_summary

    _c, app = http
    sm = app.state.sessionmaker
    execs = await make_executors(sm, 5)
    budgets = [
        '{"memory_gb": 8}',
        '{"memory_gb": 4.5}',
        '{"memory_gb": "abc"}',
        '{"memory_gb": 5000}',
        "{}",
    ]
    async with sm() as s, s.begin():
        for (p, _), b in zip(execs, budgets, strict=True):
            await s.execute(
                text(
                    "UPDATE minion_executor SET profile='balanced', last_seen_at=now(),"
                    " budget=CAST(:b AS jsonb), paused_reason=:r WHERE executor_id=:id"
                ),
                {"b": b, "r": "thermal" if b == "{}" else None, "id": p.executor_id},
            )
    async with sm() as s:
        summary = await fleet_summary(s, app.state.settings)
    assert summary.total == 5 and summary.online == 5
    assert summary.profiles == [("balanced", 5, 12.5)]
    assert summary.paused == [("thermal", 1)]
