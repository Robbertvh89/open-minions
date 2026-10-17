"""Complete / fail / release (TASK-2.2 B5). Each test uses its own team for isolation."""

import hashlib
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from minion_common.enums import TaskStatus
from minion_server.app import create_app
from minion_server.repositories import executors as executors_repo
from minion_server.services.completion import derive_task_status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from conftest import S3Env, _auth, _req, _settings, make_executors, seed_task

BASE = "/api/v1/executor"
DATA = b'{"id":"a"}\n{"id":"b"}\n'


@pytest.mark.parametrize(
    ("total", "ok", "bad", "expected"),
    [
        (3, 2, 0, None),
        (3, 3, 0, TaskStatus.SUCCEEDED),
        (3, 2, 1, TaskStatus.PARTIAL),
        (3, 0, 3, TaskStatus.FAILED),
        (0, 0, 0, None),
    ],
)
def test_derive_task_status(total: int, ok: int, bad: int, expected: TaskStatus | None) -> None:
    assert derive_task_status(total, ok, bad) == expected


@pytest.fixture
async def http(migrated_url: str, s3: S3Env) -> AsyncIterator[tuple[AsyncClient, Any]]:
    cfg = _settings(migrated_url).model_copy(
        update={
            "s3_endpoint_url": s3.endpoint_url,
            "public_s3_endpoint_url": s3.endpoint_url,
        }
    )
    from pydantic import SecretStr

    cfg = cfg.model_copy(
        update={
            "s3_access_key": SecretStr(s3.access_key),
            "s3_secret_key": SecretStr(s3.secret_key),
            "s3_bucket": s3.bucket,
        }
    )
    app = create_app(cfg)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
    ):
        yield c, app


async def _own(
    c: AsyncClient, sm: async_sessionmaker[AsyncSession], team: str, token: str, eid: uuid.UUID
) -> dict[str, Any]:
    """Prefer `team`, acquire one subtask over HTTP."""
    async with sm() as s, s.begin():
        await executors_repo.set_preference(s, eid, team=team, project=None, task_name=None)
    r = await c.post(f"{BASE}/acquire", json=_req().model_dump(mode="json"), headers=_auth(token))
    assert r.status_code == 200, r.text
    a: dict[str, Any] = r.json()
    return a


def _put(s3: S3Env, team: str, name: str, a: dict[str, Any], body: bytes = DATA) -> tuple[str, str]:
    base = f"outputs/{team}/{name}"
    src = f"{base}/_attempts/{a['subtask_id']:06d}/{a['attempt']}.jsonl"
    s3.client.put_object(Bucket=s3.bucket, Key=src, Body=body)
    return src, f"{base}/{a['subtask_id']:06d}.jsonl"


def _complete_body(body: bytes = DATA, records: int = 10) -> dict[str, Any]:
    return {
        "record_count": records,
        "byte_size": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
    }


def _exists(s3: S3Env, key: str) -> bool:
    return bool(s3.client.list_objects_v2(Bucket=s3.bucket, Prefix=key).get("Contents"))


async def _one(sm: async_sessionmaker[AsyncSession], sql: str, **p: Any) -> Any:
    async with sm() as s:
        return (await s.execute(text(sql), p)).one()


async def _all(sm: async_sessionmaker[AsyncSession], sql: str, **p: Any) -> list[Any]:
    async with sm() as s:
        return list((await s.execute(text(sql), p)).all())


FAIL = {"retryable": True, "code": "OOM", "message": "boom"}


@pytest.mark.integration
async def test_complete_promotes_and_is_idempotent(
    http: tuple[AsyncClient, Any], s3: S3Env
) -> None:
    c, app = http
    sm = app.state.sessionmaker
    await seed_task(sm, "done", 1, team="cmp1")
    (p, tok), (_q, qtok) = await make_executors(sm, 2)
    a = await _own(c, sm, "cmp1", tok, p.executor_id)
    src, dst = _put(s3, "cmp1", "done", a)
    assert not _exists(s3, dst)
    url = f"{BASE}/subtasks/{a['acquirement_id']}/complete"
    assert (await c.post(url, json=_complete_body(), headers=_auth(tok))).status_code == 204
    assert s3.client.get_object(Bucket=s3.bucket, Key=dst)["Body"].read() == DATA
    assert _exists(s3, src)  # attempt-scoped object is kept
    st = await _one(
        sm,
        "SELECT s.status, s.output_uri, t.status, t.succeeded_subtasks, t.finished_at IS NOT NULL,"
        " (SELECT count(*) FROM minion_subtask_acquirement WHERE task_name='done'),"
        " (SELECT count(*) FROM minion_subtask_attempt WHERE task_name='done'"
        "  AND outcome='SUCCEEDED')"
        " FROM minion_subtask s JOIN minion_task t USING (team, project, task_name)"
        " WHERE s.task_name='done'",
    )
    assert tuple(st) == ("SUCCEEDED", f"s3://minion/{dst}", "SUCCEEDED", 1, True, 0, 1)
    # lost-response retry by the same owner is a no-op; another executor still gets 409
    assert (await c.post(url, json=_complete_body(), headers=_auth(tok))).status_code == 204
    assert (await c.post(url, json=_complete_body(), headers=_auth(qtok))).status_code == 409
    again = await _one(sm, "SELECT succeeded_subtasks FROM minion_task WHERE task_name='done'")
    assert again[0] == 1


@pytest.mark.integration
async def test_complete_validation_keeps_ownership(
    http: tuple[AsyncClient, Any], s3: S3Env
) -> None:
    c, app = http
    sm = app.state.sessionmaker
    await seed_task(sm, "val", 1, team="cmp2")
    ((p, tok),) = await make_executors(sm, 1)
    a = await _own(c, sm, "cmp2", tok, p.executor_id)
    url = f"{BASE}/subtasks/{a['acquirement_id']}/complete"
    r = await c.post(url, json=_complete_body(), headers=_auth(tok))
    assert (r.status_code, r.json()["detail"]["code"]) == (422, "OUTPUT_MISSING")
    _, dst = _put(s3, "cmp2", "val", a)
    r = await c.post(url, json=_complete_body(b"short"), headers=_auth(tok))
    assert (r.status_code, r.json()["detail"]["code"]) == (422, "OUTPUT_SIZE_MISMATCH")
    r = await c.post(url, json=_complete_body(records=9), headers=_auth(tok))
    assert (r.status_code, r.json()["detail"]["code"]) == (422, "RECORD_COUNT_MISMATCH")
    assert not _exists(s3, dst)
    row = await _one(sm, "SELECT count(*) FROM minion_subtask_acquirement WHERE task_name='val'")
    assert row[0] == 1
    assert (await c.post(url, json=_complete_body(), headers=_auth(tok))).status_code == 204


@pytest.mark.integration
async def test_oversize_attempt_never_promoted_then_failed(
    http: tuple[AsyncClient, Any], s3: S3Env
) -> None:
    c, app = http
    sm = app.state.sessionmaker
    app.state.settings = app.state.settings.model_copy(update={"max_output_bytes": len(DATA) - 1})
    await seed_task(sm, "big", 1, team="cmp8")
    async with sm() as s, s.begin():
        await s.execute(text("UPDATE minion_subtask SET max_attempts=2 WHERE task_name='big'"))
    ((p, tok),) = await make_executors(sm, 1)
    for n in (1, 2):
        a = await _own(c, sm, "cmp8", tok, p.executor_id)
        assert a["attempt"] == n
        _, dst = _put(s3, "cmp8", "big", a)
        base = f"{BASE}/subtasks/{a['acquirement_id']}"
        r = await c.post(f"{base}/complete", json=_complete_body(), headers=_auth(tok))
        assert (r.status_code, r.json()["detail"]["code"]) == (422, "OUTPUT_TOO_LARGE")
        assert not _exists(s3, dst)  # never promoted
        # the executor reports a rejected complete as a retryable fail (COMPLETE_REJECTED)
        body = FAIL | {"code": "COMPLETE_REJECTED"}
        assert (await c.post(f"{base}/fail", json=body, headers=_auth(tok))).status_code == 204
    row = await _one(sm, "SELECT status FROM minion_subtask WHERE task_name='big'")
    assert row[0] == "FAILED"


@pytest.mark.integration
@pytest.mark.parametrize("route", ["complete", "fail", "release"])
async def test_stale_and_foreign_acquirement_409(
    http: tuple[AsyncClient, Any], s3: S3Env, route: str
) -> None:
    c, app = http
    sm = app.state.sessionmaker
    team = f"cmp3{route[0]}"
    name = f"stale-{route}"
    await seed_task(sm, name, 1, team=team)
    (p, tok), (_, other) = await make_executors(sm, 2)
    a = await _own(c, sm, team, tok, p.executor_id)
    body = {"complete": _complete_body(), "fail": FAIL, "release": None}[route]
    _, dst = _put(s3, team, name, a)

    def post(acq: object, token: str) -> Any:
        return c.post(f"{BASE}/subtasks/{acq}/{route}", json=body, headers=_auth(token))

    for acq, token in [(a["acquirement_id"], other), (uuid.uuid4(), tok)]:
        r = await post(acq, token)
        assert (r.status_code, r.json()["detail"]["code"]) == (409, "STALE_ACQUIREMENT")
    assert not _exists(s3, dst)
    row = await _one(sm, "SELECT status FROM minion_subtask WHERE task_name=:n", n=name)
    assert row[0] == "RUNNING"  # untouched by the rejected calls
    # owner releases; a late complete/fail/release with the old id is now stale too
    assert (
        await c.post(f"{BASE}/subtasks/{a['acquirement_id']}/release", headers=_auth(tok))
    ).status_code == 204
    r = await post(a["acquirement_id"], tok)
    assert r.status_code == 409
    assert not _exists(s3, dst)  # late complete after release did not promote


@pytest.mark.integration
async def test_release_does_not_consume_retry_budget(http: tuple[AsyncClient, Any]) -> None:
    c, app = http
    sm = app.state.sessionmaker
    await seed_task(sm, "rel", 1, team="cmp4")
    async with sm() as s, s.begin():
        await s.execute(text("UPDATE minion_subtask SET max_attempts=2 WHERE task_name='rel'"))
    ((p, tok),) = await make_executors(sm, 1)
    for n in range(1, 5):  # more releases than max_attempts
        a = await _own(c, sm, "cmp4", tok, p.executor_id)
        assert a["attempt"] == n
        r = await c.post(f"{BASE}/subtasks/{a['acquirement_id']}/release", headers=_auth(tok))
        assert r.status_code == 204
        row = await _one(
            sm,
            "SELECT s.status, t.status, (SELECT count(*) FROM minion_subtask_acquirement"
            " WHERE task_name='rel'), (SELECT count(*) FROM minion_executor_heartbeat h JOIN"
            " minion_subtask_acquirement a USING (acquirement_id) WHERE a.task_name='rel')"
            " FROM minion_subtask s JOIN minion_task t USING (team, project, task_name)"
            " WHERE s.task_name='rel'",
        )
        assert tuple(row) == ("PENDING", "RUNNING", 0, 0)
    a = await _own(c, sm, "cmp4", tok, p.executor_id)
    assert a["attempt"] == 5
    rows = await _one(
        sm,
        "SELECT count(*) FROM minion_subtask_attempt WHERE task_name='rel' AND outcome='RELEASED'",
    )
    assert rows[0] == 4


@pytest.mark.integration
async def test_release_code_is_stored_in_attempt_history(http: tuple[AsyncClient, Any]) -> None:
    c, app = http
    sm = app.state.sessionmaker
    await seed_task(sm, "relcode", 1, team="cmp4c")
    ((p, tok), (_q, other)) = await make_executors(sm, 2)
    a = await _own(c, sm, "cmp4c", tok, p.executor_id)
    url = f"{BASE}/subtasks/{a['acquirement_id']}/release"
    # a malformed code is rejected before anything changes; so is a foreign executor (409)
    for bad in ({"code": "lower"}, {"code": "X" * 65}, {"code": ""}, {"code": "A", "x": 1}):
        assert (await c.post(url, json=bad, headers=_auth(tok))).status_code == 422
    r = await c.post(url, json={"code": "BUDGET_DOWNGRADE"}, headers=_auth(other))
    assert (r.status_code, r.json()["detail"]["code"]) == (409, "STALE_ACQUIREMENT")
    row = await _one(sm, "SELECT status FROM minion_subtask WHERE task_name='relcode'")
    assert row[0] == "RUNNING"
    r = await c.post(url, json={"code": "BUDGET_DOWNGRADE"}, headers=_auth(tok))
    assert r.status_code == 204
    row = await _one(
        sm,
        "SELECT outcome, error_code FROM minion_ro.attempt WHERE task_name='relcode'",
    )
    assert tuple(row) == ("RELEASED", "BUDGET_DOWNGRADE")
    sub = await _one(sm, "SELECT status, error FROM minion_subtask WHERE task_name='relcode'")
    assert tuple(sub) == ("PENDING", None)  # the reason is history, not the subtask's error
    # a release without a body (old executors) still works and has no code
    b = await _own(c, sm, "cmp4c", tok, p.executor_id)
    r = await c.post(f"{BASE}/subtasks/{b['acquirement_id']}/release", headers=_auth(tok))
    assert r.status_code == 204
    rows = await _one(
        sm,
        "SELECT count(*) FILTER (WHERE error_code IS NULL), count(*)"
        " FROM minion_ro.attempt WHERE task_name='relcode'",
    )
    assert tuple(rows) == (1, 2)


@pytest.mark.integration
async def test_release_on_cancelled_task_records_cancelled_with_code(
    http: tuple[AsyncClient, Any],
) -> None:
    c, app = http
    sm = app.state.sessionmaker
    await seed_task(sm, "relcan", 1, team="cmp4d")
    ((p, tok),) = await make_executors(sm, 1)
    a = await _own(c, sm, "cmp4d", tok, p.executor_id)
    async with sm() as s, s.begin():
        await s.execute(text("UPDATE minion_task SET status='CANCELLED' WHERE task_name='relcan'"))
    r = await c.post(
        f"{BASE}/subtasks/{a['acquirement_id']}/release",
        json={"code": "STOP"},
        headers=_auth(tok),
    )
    assert r.status_code == 204
    row = await _one(
        sm, "SELECT outcome, error_code FROM minion_ro.attempt WHERE task_name='relcan'"
    )
    assert tuple(row) == ("CANCELLED", "STOP")


@pytest.mark.integration
async def test_views_expose_only_code_shaped_error_codes(http: tuple[AsyncClient, Any]) -> None:
    _c, app = http
    sm = app.state.sessionmaker
    await seed_task(sm, "badcode", 2, team="cmp4e")
    async with sm() as s, s.begin():
        await s.execute(
            text(
                "UPDATE minion_subtask SET error = CAST(:e AS jsonb)"
                " WHERE task_name='badcode' AND subtask_id=0"
            ),
            {"e": '{"code": "secret free text", "message": "m"}'},
        )
        await s.execute(
            text(
                "UPDATE minion_subtask SET error = CAST(:e AS jsonb)"
                " WHERE task_name='badcode' AND subtask_id=1"
            ),
            {"e": '{"code": "OOM"}'},
        )
    rows = await _all(
        sm,
        "SELECT subtask_id, error_code FROM minion_ro.subtask WHERE task_name='badcode'"
        " ORDER BY subtask_id",
    )
    assert [tuple(r) for r in rows] == [(0, None), (1, "OOM")]


@pytest.mark.integration
async def test_ro_executor_online_matches_the_fleet_api(http: tuple[AsyncClient, Any]) -> None:
    """minion_ro.executor.online must equal GET /executors online (same thresholds, 120/600 s)."""
    c, app = http
    sm = app.state.sessionmaker
    await seed_task(sm, "onl", 2, team="cmp4g")
    execs = await make_executors(sm, 6)
    ages = [
        (None, 10),  # idle, fresh
        (None, 200),  # idle, past 120 s
        (None, 130),  # idle, past 120 s
        ("hb", 300),  # busy: last_seen old, heartbeat 300 s ago -> within 600 s
        ("hb", 700),  # busy, past 600 s
        (None, 10),  # REVOKED below
    ]
    owned = {}
    for idx in (3, 4):
        (p, tok) = execs[idx]
        owned[idx] = await _own(c, sm, "cmp4g", tok, p.executor_id)
    async with sm() as s, s.begin():
        for idx, ((p, _), (kind, age)) in enumerate(zip(execs, ages, strict=True)):
            if kind == "hb":
                await s.execute(
                    text(
                        "UPDATE minion_executor_heartbeat SET last_seen_at = now() - :a * interval"
                        " '1 second' WHERE acquirement_id = :q"
                    ),
                    {"a": age, "q": uuid.UUID(owned[idx]["acquirement_id"])},
                )
                age = 5000
            await s.execute(
                text(
                    "UPDATE minion_executor SET last_seen_at = now() - :a * interval '1 second',"
                    " status = CASE WHEN :i = 5 THEN 'REVOKED' ELSE status END"
                    " WHERE executor_id = :e"
                ),
                {"a": age, "e": p.executor_id, "i": idx},
            )
    async with sm() as s, s.begin():
        api = {
            r.executor.executor_id: r.online
            for r in (
                await executors_repo.list_fleet(
                    s, idle_online_s=120.0, busy_online_s=600.0, limit=50
                )
            )[0]
        }
    rows = await _all(sm, "SELECT executor_id, online FROM minion_ro.executor")
    view = {r[0]: r[1] for r in rows}
    assert view == api
    assert [view[p.executor_id] for p, _ in execs] == [True, False, False, True, False, False]


@pytest.mark.integration
async def test_attempt_error_is_sql_null_without_a_code(http: tuple[AsyncClient, Any]) -> None:
    c, app = http
    sm = app.state.sessionmaker
    await seed_task(sm, "nullerr", 1, team="cmp4f")
    ((p, tok),) = await make_executors(sm, 1)
    a = await _own(c, sm, "cmp4f", tok, p.executor_id)
    r = await c.post(f"{BASE}/subtasks/{a['acquirement_id']}/release", headers=_auth(tok))
    assert r.status_code == 204
    row = await _one(
        sm, "SELECT error IS NULL FROM minion_subtask_attempt WHERE task_name='nullerr'"
    )
    assert row[0] is True


@pytest.mark.integration
async def test_fail_consumes_budget_then_terminal(http: tuple[AsyncClient, Any]) -> None:
    c, app = http
    sm = app.state.sessionmaker
    await seed_task(sm, "fl", 1, team="cmp5")
    async with sm() as s, s.begin():
        await s.execute(text("UPDATE minion_subtask SET max_attempts=2 WHERE task_name='fl'"))
    ((p, tok),) = await make_executors(sm, 1)

    async def fail(a: dict[str, Any], **kw: Any) -> None:
        r = await c.post(
            f"{BASE}/subtasks/{a['acquirement_id']}/fail", json=FAIL | kw, headers=_auth(tok)
        )
        assert r.status_code == 204

    async def state() -> tuple[Any, ...]:
        row = await _one(
            sm,
            "SELECT (SELECT string_agg(status, ',' ORDER BY subtask_id) FROM minion_subtask"
            " WHERE task_name='fl'), t.status, t.failed_subtasks FROM minion_task t"
            " WHERE task_name='fl'",
        )
        return tuple(row)

    a = await _own(c, sm, "cmp5", tok, p.executor_id)
    await fail(a)  # 1st failure of 2: retryable -> PENDING
    assert await state() == ("PENDING", "RUNNING", 0)
    a = await _own(c, sm, "cmp5", tok, p.executor_id)
    assert a["attempt"] == 2
    await fail(a)  # budget exhausted -> FAILED; all subtasks terminal -> task FAILED
    assert await state() == ("FAILED", "FAILED", 1)
    # non-retryable fails at once, despite budget left
    await seed_task(sm, "fl-nr", 1, team="cmp5")
    b = await _own(c, sm, "cmp5", tok, p.executor_id)
    assert b["attempt"] == 1
    await fail(b, retryable=False)
    row = await _one(sm, "SELECT status FROM minion_subtask WHERE task_name='fl-nr'")
    assert row[0] == "FAILED"
    err = await _one(
        sm,
        "SELECT error->>'code', error->>'message' FROM minion_subtask_attempt"
        " WHERE task_name='fl' AND outcome='FAILED' LIMIT 1",
    )
    assert tuple(err) == ("OOM", "boom")


@pytest.mark.integration
async def test_task_partial_when_mixed(http: tuple[AsyncClient, Any], s3: S3Env) -> None:
    c, app = http
    sm = app.state.sessionmaker
    await seed_task(sm, "mix", 2, team="cmp6")
    ((p, tok),) = await make_executors(sm, 1)
    a = await _own(c, sm, "cmp6", tok, p.executor_id)
    _put(s3, "cmp6", "mix", a)
    r = await c.post(
        f"{BASE}/subtasks/{a['acquirement_id']}/complete",
        json=_complete_body(),
        headers=_auth(tok),
    )
    assert r.status_code == 204
    mid = await _one(sm, "SELECT status, finished_at FROM minion_task WHERE task_name='mix'")
    assert tuple(mid) == ("RUNNING", None)
    b = await _own(c, sm, "cmp6", tok, p.executor_id)
    r = await c.post(
        f"{BASE}/subtasks/{b['acquirement_id']}/fail",
        json=FAIL | {"retryable": False},
        headers=_auth(tok),
    )
    assert r.status_code == 204
    end = await _one(
        sm,
        "SELECT status, finished_at IS NOT NULL, succeeded_subtasks, failed_subtasks"
        " FROM minion_task WHERE task_name='mix'",
    )
    assert tuple(end) == ("PARTIAL", True, 1, 1)


@pytest.mark.integration
async def test_fail_code_is_normalized_server_side(http: tuple[AsyncClient, Any]) -> None:
    c, app = http
    sm = app.state.sessionmaker
    await seed_task(sm, "normcode", 1, team="cmp6")
    async with sm() as s, s.begin():
        await s.execute(text("UPDATE minion_subtask SET max_attempts=5 WHERE task_name='normcode'"))
    ((p, tok),) = await make_executors(sm, 1)
    for sent, stored in (("oom_killed", "OOM_KILLED"), ("not a code!", "RUNNER_ERROR")):
        a = await _own(c, sm, "cmp6", tok, p.executor_id)
        r = await c.post(
            f"{BASE}/subtasks/{a['acquirement_id']}/fail",
            json=FAIL | {"code": sent},
            headers=_auth(tok),
        )
        assert r.status_code == 204, sent
        row = await _one(
            sm,
            "SELECT error ->> 'code' FROM minion_subtask WHERE task_name='normcode'",
        )
        assert row[0] == stored
        row = await _one(
            sm,
            "SELECT error ->> 'code' FROM minion_subtask_attempt WHERE task_name='normcode'"
            " AND attempt = :n",
            n=a["attempt"],
        )
        assert row[0] == stored
