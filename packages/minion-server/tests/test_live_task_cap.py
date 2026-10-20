"""Per-team live-task cap on create: 429 TOO_MANY_LIVE_TASKS, exact under concurrency
(TASK-30, FR-9, ADR-0020)."""

import asyncio
import hashlib
import os
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import text

from conftest import API_KEY, TEAM, Live, S3Env, _serve

pytestmark = pytest.mark.integration

H = {"Authorization": f"Bearer {API_KEY}"}
OTHER = "e2eother"


def _keys(teams: str, policy: str = "") -> str:
    digest = hashlib.sha256(API_KEY.encode()).hexdigest()
    return f"keys:\n  - name: u\n    sha256: {digest}\n    teams: [{teams}]\n{policy}"


@pytest.fixture
async def capped(fresh_url: str, s3: S3Env, tmp_path: Path) -> Any:
    extra = {"max_live_tasks_per_team": 2, "api_keys_reload_s": 0}
    async with _serve(fresh_url, s3, tmp_path, extra) as lv:
        keys = tmp_path / "keys.yaml"
        keys.write_text(_keys(f"{TEAM}, {OTHER}"))
        st = keys.stat()
        os.utime(keys, ns=(st.st_atime_ns, st.st_mtime_ns + 5))
        yield lv


def _put(lv: Live, team: str, name: str) -> None:
    lv.s3.client.put_object(
        Bucket=lv.s3.bucket, Key=f"inputs/{team}/{name}/a.jsonl", Body=b'{"id": "1", "text": "x"}\n'
    )


def _body(lv: Live, team: str, name: str) -> dict[str, Any]:
    _put(lv, team, name)
    return {
        "team": team,
        "project": "p",
        "task_name": name,
        "type": "embedding",
        "model": "fake-embed",
        "input_uri": f"s3://{lv.s3.bucket}/inputs/{team}/{name}/",
        "output_uri": f"s3://{lv.s3.bucket}/outputs/{team}/{name}/",
    }


async def _post(c: httpx.AsyncClient, lv: Live, team: str, name: str) -> httpx.Response:
    return await c.post("/api/v1/tasks", json=_body(lv, team, name), headers=H)


async def _live_count(lv: Live, team: str) -> int:
    rows = await lv.rows(
        "SELECT count(*) FROM minion_task WHERE team=:t"
        " AND status IN ('SPLITTING','PENDING','RUNNING')",
        t=team,
    )
    return int(rows[0][0])


async def test_cap_rejects_third_and_other_team_unaffected(capped: Live) -> None:
    async with httpx.AsyncClient(base_url=capped.url) as c:
        assert (await _post(c, capped, TEAM, "a")).status_code == 202
        assert (await _post(c, capped, TEAM, "b")).status_code == 202
        r = await _post(c, capped, TEAM, "c")
        assert r.status_code == 429
        detail = r.json()["detail"]
        assert detail["code"] == "TOO_MANY_LIVE_TASKS"
        assert (
            "limit (2)" in detail["message"]
            and "max_live_tasks" in detail["message"]
            and "Retry-After" not in r.headers
        )
        assert await capped.rows("SELECT 1 FROM minion_task WHERE task_name='c'") == []
        assert (await _post(c, capped, OTHER, "x")).status_code == 202


async def test_slots_free_up_and_splitting_counts(capped: Live) -> None:
    async with httpx.AsyncClient(base_url=capped.url) as c:
        for n in ("a", "b"):
            assert (await _post(c, capped, TEAM, n)).status_code == 202
        assert (await _post(c, capped, TEAM, "c")).status_code == 429
        r = await c.post(f"/api/v1/tasks/{TEAM}/p/a/cancel", headers=H)
        assert r.status_code == 200, r.text
        assert (await _post(c, capped, TEAM, "c")).status_code == 202
        assert (await _post(c, capped, TEAM, "d")).status_code == 429
        # live now: b, c. Each terminal status frees a slot; the next create fills it again.
        live = ["b", "c"]
        for k, st in enumerate(("SUCCEEDED", "FAILED", "PARTIAL")):
            async with capped.sm() as sess, sess.begin():
                await sess.execute(
                    text("UPDATE minion_task SET status=:s WHERE task_name=:n"),
                    {"s": st, "n": live.pop(0)},
                )
            new = f"n{k}"
            assert (await _post(c, capped, TEAM, new)).status_code == 202
            live.append(new)
            assert (await _post(c, capped, TEAM, f"x{k}")).status_code == 429
        # a SPLITTING task counts: the new tasks above are still SPLITTING (no splitter run)
        assert await _live_count(capped, TEAM) == 2


async def test_keys_file_override_after_reload(fresh_url: str, s3: S3Env, tmp_path: Path) -> None:
    extra = {"max_live_tasks_per_team": 2, "api_keys_reload_s": 0}
    async with _serve(fresh_url, s3, tmp_path, extra) as lv:
        keys = tmp_path / "keys.yaml"
        keys.write_text(_keys(TEAM, f"teams:\n  {TEAM}: {{max_live_tasks: 3}}\n"))
        st = keys.stat()
        os.utime(keys, ns=(st.st_atime_ns, st.st_mtime_ns + 5))
        async with httpx.AsyncClient(base_url=lv.url) as c:
            codes = [(await _post(c, lv, TEAM, f"o{i}")).status_code for i in range(4)]
            assert codes == [202, 202, 202, 429]
            keys.write_text(_keys(TEAM, f"teams:\n  {TEAM}: {{max_live_tasks: 4}}\n"))
            os.utime(keys, ns=(st.st_atime_ns, st.st_mtime_ns + 10))
            assert (await _post(c, lv, TEAM, "o9")).status_code == 202


@pytest.mark.parametrize("rep", range(3))
async def test_concurrent_creates_hold_cap_exactly(
    fresh_url: str, s3: S3Env, tmp_path: Path, rep: int
) -> None:
    async with _serve(fresh_url, s3, tmp_path, {"max_live_tasks_per_team": 5}) as lv:
        bodies = [_body(lv, TEAM, f"c{i}") for i in range(20)]
        async with httpx.AsyncClient(base_url=lv.url, timeout=60) as c:
            rs = await asyncio.gather(*(c.post("/api/v1/tasks", json=b, headers=H) for b in bodies))
        codes = sorted(r.status_code for r in rs)
        assert codes == [202] * 5 + [429] * 15, codes
        assert await _live_count(lv, TEAM) == 5


async def test_count_plan_uses_team_status_index(capped: Live) -> None:
    async with capped.sm() as s, s.begin():
        await s.execute(
            text(
                "INSERT INTO minion_task (team, project, task_name, type, model_id, engine,"
                " model_memory_gb, input_uri, output_uri, created_by, status)"
                " SELECT CASE WHEN g < 400 OR g >= 5400 THEN 'big' ELSE 'other' || (g % 20) END,"
                " 'p', 'n' || g, 'embedding', 'm', 'mlx', 1, 's3://i',"
                " 's3://o/' || g, 'u', CASE WHEN g < 5400 THEN 'RUNNING' ELSE 'SUCCEEDED' END"
                " FROM generate_series(0, 25399) g"
            )
        )
    async with capped.sm() as s:
        await s.execute(text("ANALYZE minion_task"))
        base = (
            "SELECT count(*) FROM (SELECT 1 FROM minion_task WHERE team={t}"
            " AND status IN ('SPLITTING','PENDING','RUNNING') LIMIT 500) s"
        )
        plan = "\n".join(
            r[0] for r in (await s.execute(text("EXPLAIN " + base.format(t="'big'")))).all()
        )
        await s.execute(text("SET plan_cache_mode = force_generic_plan"))
        await s.execute(text("PREPARE cnt(text) AS " + base.format(t="$1")))
        generic = "\n".join(
            r[0] for r in (await s.execute(text("EXPLAIN EXECUTE cnt('big')"))).all()
        )
    for p in (plan, generic):
        assert "ix_minion_task_team_status" in p, p
        assert "Seq Scan on minion_task" not in p, p


async def test_lock_timeout_answers_503_busy(capped: Live) -> None:
    import time

    async with capped.sm() as holder, holder.begin():
        await holder.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended('minion_task_live:' || :t, 0))"),
            {"t": TEAM},
        )
        async with httpx.AsyncClient(base_url=capped.url, timeout=30) as c:
            t0 = time.monotonic()
            r = await _post(c, capped, TEAM, "locked")
            waited = time.monotonic() - t0
        assert r.status_code == 503, r.text
        assert r.json()["detail"]["code"] == "SERVER_BUSY"
        assert int(r.headers["Retry-After"]) >= 1
        assert 1.5 <= waited < 4, waited  # the 2 s lock_timeout, not a hang
    assert await capped.rows("SELECT 1 FROM minion_task WHERE task_name='locked'") == []
    async with httpx.AsyncClient(base_url=capped.url) as c:  # lock released: works again
        assert (await _post(c, capped, TEAM, "free")).status_code == 202
