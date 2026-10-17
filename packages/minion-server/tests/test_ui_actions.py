"""Web UI part 2: create, cancel, fleet (TASK-6.7, ADR-0010). Same semantics as the REST API."""

import asyncio
import hashlib
import re
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient, Response
from minion_server.app import create_app
from minion_server.settings import ServerSettings
from pydantic import SecretStr
from sqlalchemy import text

from conftest import TEST_MODELS_FILE, S3Env, make_executors, seed_task

ALICE, OPER, BOB = "k-alice-ui", "k-oper-ui", "k-bob-ui"
BASE = "https://ui.example"
ORIGIN = {"Origin": BASE}


def _h(k: str) -> str:
    return hashlib.sha256(k.encode()).hexdigest()


KEYS = f"""keys:
  - {{name: alice, sha256: {_h(ALICE)}, teams: [ui1, ui2]}}
  - {{name: oper, sha256: {_h(OPER)}, teams: [ui1], operator: true}}
  - {{name: bob, sha256: {_h(BOB)}, teams: [ui3]}}
teams:
  ui2: {{default_pool: pa, allowed_pools: [pa, pb]}}
"""


@pytest.fixture
async def ui(fresh_url: str, s3: S3Env, tmp_path: Path) -> AsyncIterator[tuple[AsyncClient, Any]]:
    keys = tmp_path / "keys.yaml"
    keys.write_text(KEYS)
    cfg = ServerSettings(
        database_url=SecretStr(fresh_url),
        s3_endpoint_url=s3.endpoint_url,
        s3_access_key=SecretStr(s3.access_key),
        s3_secret_key=SecretStr(s3.secret_key),
        s3_bucket=s3.bucket,
        s3_region=s3.region,
        api_keys_file=str(keys),
        api_keys_reload_s=0,
        models_file=TEST_MODELS_FILE,
        splitter_poll_s=60,
        ui_session_secret=SecretStr("s3cret-" * 6),
    )
    app = create_app(cfg)
    async with app.router.lifespan_context(app):
        app.state.splitter_task.cancel()
        await asyncio.gather(app.state.splitter_task, return_exceptions=True)
        async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE) as c:
            yield c, app


async def _login(c: AsyncClient, key: str) -> str:
    """Log in; return the session's CSRF token (as rendered on a page)."""
    c.cookies.clear()
    page = (await c.get("/ui/login")).text
    token = re.search(r'name="csrf" value="([^"]+)"', page)
    assert token
    r = await c.post("/ui/login", data={"api_key": key, "csrf": token.group(1)}, headers=ORIGIN)
    assert r.status_code == 303
    return _page_csrf(await c.get("/ui/tasks/new"))


def _page_csrf(r: Response) -> str:
    m = re.search(r'name="csrf" value="([^"]+)"', r.text)
    assert m, r.text[:300]
    return m.group(1)


def _put(s3: S3Env, key: str) -> None:
    s3.client.put_object(Bucket=s3.bucket, Key=key, Body=b'{"id":"a","text":"hi"}\n')


def _form(csrf: str, name: str, **kw: str) -> dict[str, str]:
    return {
        "csrf": csrf,
        "team": "ui1",
        "project": "p",
        "task_name": name,
        "type": "embedding",
        "model": "fake-embed",
        "input_uri": f"s3://minion/inputs/ui1/{name}",
        "output_uri": f"s3://minion/outputs/ui1/{name}",
        "params": "{}",
        "records_per_subtask": "10",
        "priority": "0",
        "allowed_pool": "",
    } | kw


async def _task_row(app: Any, team: str, name: str) -> dict[str, Any] | None:
    async with app.state.sessionmaker() as s:
        row = (
            (
                await s.execute(
                    text(
                        "SELECT type, model_id, params, records_per_subtask, priority,"
                        " allowed_pool, status, created_by FROM minion_task"
                        " WHERE team=:t AND task_name=:n"
                    ),
                    {"t": team, "n": name},
                )
            )
            .mappings()
            .first()
        )
    return dict(row) if row else None


@pytest.mark.integration
async def test_ui_create_matches_api_row(ui: tuple[AsyncClient, Any], s3: S3Env) -> None:
    c, app = ui
    for n in ("via-ui", "via-api"):
        _put(s3, f"inputs/ui1/{n}/a.jsonl")
    csrf = await _login(c, ALICE)
    r = await c.post(
        "/ui/tasks",
        data=_form(csrf, "via-ui", params='{"normalize": true}', priority="3"),
        headers=ORIGIN,
    )
    assert r.status_code == 303 and r.headers["location"] == "/ui/tasks/ui1/p/via-ui"
    api = await c.post(
        "/api/v1/tasks",
        headers={"Authorization": f"Bearer {ALICE}"},
        json={
            "team": "ui1", "project": "p", "task_name": "via-api", "type": "embedding",
            "model": "fake-embed", "input_uri": "s3://minion/inputs/ui1/via-api",
            "output_uri": "s3://minion/outputs/ui1/via-api", "params": {"normalize": True},
            "records_per_subtask": 10, "priority": 3,
        },
    )  # fmt: skip
    assert api.status_code == 202
    assert await _task_row(app, "ui1", "via-ui") == await _task_row(app, "ui1", "via-api")
    detail = await c.get("/ui/tasks/ui1/p/via-ui")
    assert detail.status_code == 200 and "Cancel task" in detail.text


@pytest.mark.integration
async def test_ui_create_shows_api_error_codes(ui: tuple[AsyncClient, Any], s3: S3Env) -> None:
    c, app = ui
    _put(s3, "inputs/ui1/ok/a.jsonl")
    _put(s3, "inputs/ui2/pol/a.jsonl")
    csrf = await _login(c, ALICE)

    async def post(name: str, **kw: str) -> Response:
        return await c.post("/ui/tasks", data=_form(csrf, name, **kw), headers=ORIGIN)

    cases: list[tuple[dict[str, str], int, str]] = [
        ({"model": "nope"}, 422, "UNKNOWN_MODEL"),
        ({"type": "completion"}, 422, "MODEL_TYPE_MISMATCH"),
        ({"params": "{not json"}, 422, "INVALID_PARAMS"),
        ({"params": "[1]"}, 422, "INVALID_PARAMS"),
        ({"params": '{"trust_remote_code": true}'}, 422, "INVALID_PARAMS"),
        ({"input_uri": "s3://minion/inputs/other/x"}, 422, "INVALID_URI"),
        ({"input_uri": "s3://minion/inputs/ui1/empty"}, 422, "EMPTY_INPUT"),
        ({"team": "ui3"}, 403, "FORBIDDEN"),
        ({"records_per_subtask": "0"}, 422, "INVALID_REQUEST"),
        ({"records_per_subtask": "many"}, 422, "INVALID_REQUEST"),
        ({"team": "ui2", "input_uri": "s3://minion/inputs/ui2/pol",
          "output_uri": "s3://minion/outputs/ui2/pol", "allowed_pool": "pz"},
         403, "POOL_NOT_ALLOWED"),
    ]  # fmt: skip
    for i, (kw, status, code) in enumerate(cases):
        r = await post(f"e{i}", **kw)
        assert r.status_code == status and f"{code}:" in r.text, (kw, r.text[:400])
        assert await _task_row(app, kw.get("team", "ui1"), f"e{i}") is None
    # a default pool from the team policy is applied exactly as in the API
    ok = await post(
        "pol",
        team="ui2",
        input_uri="s3://minion/inputs/ui2/pol",
        output_uri="s3://minion/outputs/ui2/pol",
    )
    assert ok.status_code == 303
    row = await _task_row(app, "ui2", "pol")
    assert row is not None and row["allowed_pool"] == "pa"
    ok2 = await post("ok")
    assert ok2.status_code == 303
    dup = await post("ok")
    assert dup.status_code == 409 and "TASK_EXISTS:" in dup.text


@pytest.mark.integration
async def test_ui_create_escapes_and_does_not_echo_into_errors(
    ui: tuple[AsyncClient, Any], s3: S3Env
) -> None:
    c, _ = ui
    csrf = await _login(c, ALICE)
    evil = "</textarea><script>alert(1)</script>"
    r = await c.post(
        "/ui/tasks",
        data=_form(csrf, "x", project=evil, params=evil, model=evil),
        headers=ORIGIN,
    )
    assert r.status_code == 422
    assert "<script>" not in r.text and "&lt;script&gt;" in r.text  # refilled but escaped
    assert "alert(1)" not in r.text.split('role="alert">')[1].split("</p>")[0]  # not in the error


@pytest.mark.integration
async def test_ui_posts_need_csrf_and_origin(ui: tuple[AsyncClient, Any], s3: S3Env) -> None:
    c, app = ui
    _put(s3, "inputs/ui1/csrf/a.jsonl")
    csrf = await _login(c, ALICE)
    form = _form(csrf, "csrf")
    assert (
        await c.post("/ui/tasks", data=form, headers={"Origin": "https://evil"})
    ).status_code == 403
    assert (await c.post("/ui/tasks", data=form)).status_code == 403
    assert (
        await c.post("/ui/tasks", data=form | {"csrf": "bad"}, headers=ORIGIN)
    ).status_code == 403
    assert await _task_row(app, "ui1", "csrf") is None
    await seed_task(app.state.sessionmaker, "cx", 2, team="ui1", status="RUNNING")
    url = "/ui/tasks/ui1/p/cx/cancel"
    for kw in (
        {"headers": {"Origin": "https://evil"}},
        {},
        {"headers": ORIGIN, "data": {"csrf": "bad"}},
    ):
        kw.setdefault("data", {"csrf": csrf})
        assert (await c.post(url, **kw)).status_code == 403  # type: ignore[arg-type]
    assert (await _task_row(app, "ui1", "cx"))["status"] == "RUNNING"  # type: ignore[index]


@pytest.mark.integration
async def test_ui_cancel_follows_fr8(ui: tuple[AsyncClient, Any]) -> None:
    c, app = ui
    sm = app.state.sessionmaker
    await seed_task(sm, "run", 3, team="ui1", status="RUNNING")
    await seed_task(sm, "done", 1, team="ui1", status="SUCCEEDED")
    await seed_task(sm, "theirs", 1, team="ui3", status="RUNNING")
    csrf = await _login(c, ALICE)

    confirm = await c.get("/ui/tasks/ui1/p/run/cancel")
    assert confirm.status_code == 200 and "Yes, cancel this task" in confirm.text
    assert "onclick" not in confirm.text and "confirm(" not in confirm.text
    assert (await _task_row(app, "ui1", "run"))["status"] == "RUNNING"  # type: ignore[index]  # GET is safe

    for _ in range(2):  # idempotent
        r = await c.post("/ui/tasks/ui1/p/run/cancel", data={"csrf": csrf}, headers=ORIGIN)
        assert r.status_code == 303 and r.headers["location"] == "/ui/tasks/ui1/p/run"
    assert (await _task_row(app, "ui1", "run"))["status"] == "CANCELLED"  # type: ignore[index]
    async with sm() as s:
        n = (
            await s.execute(
                text(
                    "SELECT count(*) FROM minion_subtask"
                    " WHERE task_name='run' AND status='CANCELLED'"
                )
            )
        ).scalar_one()
    assert n == 3
    assert "Cancel task" not in (await c.get("/ui/tasks/ui1/p/run")).text

    fin = await c.post("/ui/tasks/ui1/p/done/cancel", data={"csrf": csrf}, headers=ORIGIN)
    assert fin.status_code == 409 and "TASK_FINISHED:" in fin.text
    other = await c.post("/ui/tasks/ui3/p/theirs/cancel", data={"csrf": csrf}, headers=ORIGIN)
    assert other.status_code == 403
    assert (await c.get("/ui/tasks/ui3/p/theirs/cancel")).status_code == 403
    assert (
        await c.post("/ui/tasks/ui1/p/nope/cancel", data={"csrf": csrf}, headers=ORIGIN)
    ).status_code == 404
    assert (await _task_row(app, "ui3", "theirs"))["status"] == "RUNNING"  # type: ignore[index]


@pytest.mark.integration
async def test_ui_fleet_is_operator_only_with_summary_and_paging(
    ui: tuple[AsyncClient, Any],
) -> None:
    c, app = ui
    sm = app.state.sessionmaker
    execs = await make_executors(sm, 3)
    async with sm() as s, s.begin():
        for i, (p, _) in enumerate(execs):
            host = "<b>evil</b>" if i == 0 else f"mac{i}"
            await s.execute(
                text(
                    "UPDATE minion_executor SET hostname=:h, owner=:o, profile='balanced',"
                    " budget=CAST(:b AS jsonb), last_seen_at=now(), paused_reason=:r"
                    " WHERE executor_id=:id"
                ),
                {
                    "h": host,
                    "o": f"own{i}",
                    "b": '{"profile":"balanced","memory_gb":8,"gpu_duty_cycle":0.5,'
                    '"max_batch_size":8}',
                    "r": "thermal" if i < 2 else None,
                    "id": p.executor_id,
                },
            )

    await _login(c, ALICE)
    r = await c.get("/ui/fleet")
    assert r.status_code == 403 and "OPERATOR_REQUIRED" in r.text
    assert "Fleet</a>" not in (await c.get("/ui/tasks")).text

    await _login(c, OPER)
    assert "Fleet</a>" in (await c.get("/ui/tasks")).text
    r = await c.get("/ui/fleet")
    assert r.status_code == 200
    assert "<b>evil</b>" not in r.text and "&lt;b&gt;evil&lt;/b&gt;" in r.text
    assert "3 online of 3" in r.text
    assert re.search(r"<td>balanced</td><td>3</td><td>24\.0</td>", r.text)
    assert re.search(r"<td>thermal</td><td>2</td>", r.text)
    p1 = await c.get("/ui/fleet?limit=2")
    assert p1.text.count("<td>own") == 2
    nxt = re.search(r'href="(/ui/fleet\?[^"]+)">Next page', p1.text)
    assert nxt
    p2 = await c.get(nxt.group(1).replace("&amp;", "&"))
    assert p2.text.count("<td>own") == 1 and "Next page" not in p2.text
    assert (await c.get("/ui/fleet?limit=0")).status_code == 422
    assert (await c.get("/ui/fleet?status=bogus")).status_code == 422
    assert (await c.get("/ui/fleet?cursor=zzz")).status_code == 422
