"""Governor e2e (TASK-5.5 AC #6): real server + Postgres + S3 and a real `minion-executor run`
subprocess (fake runner), steered through the dev signal file (`MINION_EXECUTOR_FAKE_SIGNALS`)
and the owner CLI (`pause` / `resume` / `profile`). Covers pause / resume, the 60 s pause release,
memory-critical release, the FR-54 downgrade and status.json, plus (macOS) ADR-0006 D14: the
running executor holds no power assertion that keeps the Mac awake.

Timers are shrunk with the DEV-only `MINION_EXECUTOR_*_S` knobs. Synchronisation is by polling a
condition with a bounded timeout (DB rows, status.json, process tree), never by fixed sleeps.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import signal
import socket
import subprocess
import sys
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import uvicorn
from minion_common.client import UserClient
from minion_common.schemas import TaskCreate
from minion_executor.capacity.safefile import SafeFileError
from minion_executor.capacity.status import StatusFile, read_status
from minion_server.admin import issue_executor_token
from minion_server.app import create_app
from pydantic import SecretStr
from sqlalchemy import text

from conftest import S3Env, _settings

pytestmark = pytest.mark.integration

API_KEY = "gov-e2e-user-key"
TEAM = "gov"
HB_S = 1.0
SMALL, BIG = 0.5, 2.0  # light / balanced memory budgets (GB) written to executor.toml
BIG_MODEL_GB = 1.0  # fits balanced (2.0), not light (0.5): the downgrade model

MODELS = f"""\
models:
  - {{model_id: fake-embed, type: embedding, engine: fake, ref: fake, memory_gb: 0.1,
      revision: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}}
  - {{model_id: fake-big, type: embedding, engine: fake, ref: fake, memory_gb: {BIG_MODEL_GB},
      revision: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}}
"""
TOML = f"""\
[profiles.light]
memory_gb = {SMALL}
[profiles.balanced]
memory_gb = {BIG}
"""
QUIET: dict[str, Any] = {"hid_idle_s": 0, "on_ac": True, "battery_percent": 100}


async def eventually(
    cond: Callable[[], Awaitable[bool] | bool], limit_s: float = 60.0, what: str = "condition"
) -> None:
    deadline = time.monotonic() + limit_s
    while True:
        r = cond()
        if await r if isinstance(r, Awaitable) else r:
            return
        if time.monotonic() > deadline:
            raise TimeoutError(f"{what} not met within {limit_s:g}s")
        await asyncio.sleep(0.05)


@dataclass
class Live:
    url: str
    sm: Any
    s3: S3Env

    async def token(self, owner: str) -> str:
        return (await issue_executor_token(self.sm, owner=owner, pool="default", ttl_days=1))[1]

    async def submit(
        self, name: str, body: bytes, *, rps: int = 10, model: str = "fake-embed"
    ) -> None:
        self.s3.client.put_object(
            Bucket=self.s3.bucket, Key=f"inputs/{TEAM}/{name}/a.jsonl", Body=body
        )
        async with UserClient(self.url, SecretStr(API_KEY)) as uc:
            await uc.create_task(
                TaskCreate.model_validate(
                    {
                        "team": TEAM,
                        "project": "p",
                        "task_name": name,
                        "type": "embedding",
                        "model": model,
                        "input_uri": f"s3://{self.s3.bucket}/inputs/{TEAM}/{name}/",
                        "output_uri": f"s3://{self.s3.bucket}/outputs/{TEAM}/{name}/",
                        "records_per_subtask": rps,
                    }
                )
            )

    async def rows(self, sql: str, **p: Any) -> list[Any]:
        async with self.sm() as s:
            return list((await s.execute(text(sql), p)).all())

    async def task_status(self, name: str) -> str:
        async with UserClient(self.url, SecretStr(API_KEY)) as uc:
            return str((await uc.get_task(TEAM, "p", name)).status)

    async def attempts(self, name: str) -> list[tuple[int, str]]:
        rows = await self.rows(
            "SELECT attempt, outcome FROM minion_subtask_attempt WHERE task_name=:n"
            " ORDER BY subtask_id, attempt",
            n=name,
        )
        return [(r[0], r[1]) for r in rows]

    async def inflight(self) -> bool:
        return bool((await self.rows("SELECT count(*) FROM minion_subtask_acquirement"))[0][0])

    async def exec_row(self) -> Any:
        rows = await self.rows(
            "SELECT paused_reason, profile, budget, last_seen_at FROM minion_executor"
        )
        return rows[0] if rows else None

    def final_keys(self, name: str) -> list[str]:
        resp = self.s3.client.list_objects_v2(
            Bucket=self.s3.bucket, Prefix=f"outputs/{TEAM}/{name}/"
        )
        return sorted(o["Key"] for o in resp.get("Contents", []) if "_attempts" not in o["Key"])

    def out_ids(self, name: str) -> list[str]:
        ids: list[str] = []
        for k in self.final_keys(name):
            body = self.s3.client.get_object(Bucket=self.s3.bucket, Key=k)["Body"].read()
            ids.extend(json.loads(line)["id"] for line in body.splitlines())
        return ids


@pytest.fixture
async def live(fresh_url: str, s3: S3Env, tmp_path: Path) -> AsyncIterator[Live]:
    keys = tmp_path / "keys.yaml"
    digest = hashlib.sha256(API_KEY.encode()).hexdigest()
    keys.write_text(f"keys:\n  - name: u\n    sha256: {digest}\n    teams: [{TEAM}]\n")
    models = tmp_path / "models.yaml"
    models.write_text(MODELS)
    cfg = _settings(fresh_url).model_copy(
        update={
            "s3_endpoint_url": s3.endpoint_url,
            "s3_access_key": SecretStr(s3.access_key),
            "s3_secret_key": SecretStr(s3.secret_key),
            "s3_bucket": s3.bucket,
            "api_keys_file": str(keys),
            "models_file": str(models),
            "splitter_poll_s": 0.1,
        }
    )
    app = create_app(cfg)
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level="warning", lifespan="on"))
    serve = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        await eventually(lambda: server.started, 30, "server start")
        yield Live(f"http://127.0.0.1:{port}", app.state.sessionmaker, s3)
    finally:
        server.should_exit = True
        await asyncio.wait_for(serve, 30)
        sock.close()


# --- the executor under test ---


def _cli(*args: str) -> tuple[str, ...]:
    return (sys.executable, "-c", "from minion_executor.main import app; app()", *args)


def descendants(pid: int, *, tracker: bool = False) -> list[int]:
    """Live descendants of `pid`. Without `tracker`, multiprocessing's resource-tracker helper
    (it lives as long as the executor, not the model) is left out, leaving the worker."""
    out = subprocess.run(
        ["/bin/ps", "-axo", "pid=,ppid=,command="], capture_output=True, text=True, check=True
    ).stdout
    kids: dict[int, list[tuple[int, str]]] = {}
    for line in out.splitlines():
        p, pp, cmd = line.split(None, 2)
        kids.setdefault(int(pp), []).append((int(p), cmd))
    found: list[int] = []
    todo = [pid]
    while todo:
        for child, cmd in kids.get(todo.pop(), []):
            if tracker or "resource_tracker" not in cmd:
                found.append(child)
            todo.append(child)
    return found


class Exec:
    """One `minion-executor run` subprocess with a dev signal file and a private state dir."""

    def __init__(self, live: Live, tmp: Path) -> None:
        self.live = live
        self.state_dir = tmp / "state"
        self.sig_dir = tmp / "sig"
        self.sig_dir.mkdir(mode=0o700)
        self.sig_path = self.sig_dir / "signals.json"
        self.cfg = tmp / "cfg"
        self.cfg.mkdir(mode=0o700)
        self.toml = self.cfg / "executor.toml"
        self.toml.write_text(TOML)
        self.toml.chmod(0o600)
        self.log_path = tmp / "executor.log"
        self.proc: asyncio.subprocess.Process | None = None
        self.env: dict[str, str] = {}

    def signals(self, **fields: Any) -> None:
        """Replace the signal file atomically (the executor may read it at any moment)."""
        tmp = self.sig_dir / "signals.tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump({**QUIET, **fields}, f)
        tmp.replace(self.sig_path)

    def _env(self, token: str, extra: dict[str, str]) -> dict[str, str]:
        env = {
            **os.environ,
            "MINION_EXECUTOR_TOKEN": token,
            "MINION_EXECUTOR_DEV": "1",
            "MINION_EXECUTOR_TEST_HOOKS": "1",
            "MINION_EXECUTOR_FAKE_SIGNALS": str(self.sig_path),
            "MINION_EXECUTOR_STATE_DIR": str(self.state_dir),
            "MINION_EXECUTOR_CONFIG": str(self.toml),
            "MINION_EXECUTOR_HEARTBEAT_INTERVAL_S": str(HB_S),
            "MINION_EXECUTOR_SIGNAL_POLL_S": "0.5",
            "MINION_EXECUTOR_PRESENCE_INTERVAL_S": "1",
            "MINION_EXECUTOR_RESUME_AFTER_S": "1",
            "MINION_EXECUTOR_ESCALATE_AFTER_S": "1",
            "MINION_EXECUTOR_PAUSE_RELEASE_AFTER_S": "300",
            "MINION_EXECUTOR_DOWNGRADE_BATCH_S": "60",
            "MINION_EXECUTOR_STOP_GRACE_S": "0",
        }
        env.update(extra)
        self.env = env
        return env

    async def start(self, *, signals: dict[str, Any] | None = None, **extra: str) -> None:
        self.signals(**(signals or {}))
        env = self._env(await self.live.token("gov-exec"), extra)
        with self.log_path.open("wb") as log:
            self.proc = await asyncio.create_subprocess_exec(
                *_cli(
                    "run", "--server", self.live.url, "--profile", "balanced", "--runner", "fake"
                ),
                env=env,
                stdout=log,
                stderr=log,
                cwd=str(self.state_dir.parent),
            )

    async def stop(self) -> None:
        p = self.proc
        if p is None or p.returncode is not None:
            return
        p.send_signal(signal.SIGTERM)
        try:
            await asyncio.wait_for(p.wait(), 20)
        except TimeoutError:
            p.kill()
            await p.wait()

    def cli(self, *args: str) -> subprocess.CompletedProcess[str]:
        env = {k: v for k, v in self.env.items() if k != "MINION_EXECUTOR_TOKEN"}
        return subprocess.run(  # noqa: S603
            _cli(*args), env=env, capture_output=True, text=True, timeout=60, check=False
        )

    @property
    def pid(self) -> int:
        assert self.proc is not None
        return self.proc.pid

    def status(self) -> StatusFile | None:
        try:
            return read_status(self.state_dir)
        except SafeFileError:
            return None

    def workers(self) -> list[int]:
        return descendants(self.pid)

    def logs(self) -> str:
        return self.log_path.read_text(errors="replace")[-3000:] if self.log_path.exists() else ""


@pytest.fixture
async def ex(live: Live, tmp_path: Path) -> AsyncIterator[Exec]:
    e = Exec(live, tmp_path)
    try:
        yield e
    finally:
        await e.stop()


def slow_body(steps: int, n_fast: int = 3, *, prefix: str = "r") -> bytes:
    """One record that ticks for `steps` x 0.1 s (it pauses at its checkpoints) plus fast ones."""
    lines = [{"id": f"{prefix}slow", "text": "x", "sleep_steps": steps, "step_s": 0.1}]
    lines += [{"id": f"{prefix}{i}", "text": f"t{i}"} for i in range(n_fast)]
    return b"".join(json.dumps(r).encode() + b"\n" for r in lines)


def fast_body(n: int, prefix: str = "f") -> bytes:
    return b"".join(f'{{"id":"{prefix}{i}","text":"t{i}"}}\n'.encode() for i in range(n))


async def running_subtask(ex: Exec) -> None:
    """A subtask is in flight and its worker subprocess is up."""

    def up() -> bool:
        st = ex.status()
        return st is not None and st.subtask is not None and bool(ex.workers())

    await eventually(up, 60, f"subtask running\n{ex.logs()}")


async def paused_row(live: Live, reason: str) -> None:
    async def ok() -> bool:
        row = await live.exec_row()
        return row is not None and row[0] == reason

    await eventually(ok, 30, f"executor row paused_reason={reason}")


def assert_exactly_once(ids: list[str], expected: list[str]) -> None:
    assert sorted(ids) == sorted(expected)
    assert len(set(ids)) == len(ids)


# --- (a) pause gates acquires, presence reports it, clearing resumes ---


@pytest.mark.parametrize(
    ("signal_fields", "reason"),
    [({"thermal": "serious"}, "thermal"), ({"memory_pressure": "warning"}, "memory_pressure")],
    ids=["thermal", "memory_warning"],
)
async def test_signal_pause_blocks_acquires_and_clear_resumes(
    live: Live, ex: Exec, signal_fields: dict[str, Any], reason: str
) -> None:
    """AC #1/#6: a paused executor never acquires but reports `paused_reason` (presence); once
    the signal is clear for the resume window it acquires and finishes the task."""
    await live.submit("gate", fast_body(10))
    await ex.start(signals=signal_fields)
    await paused_row(live, reason)
    row = await live.exec_row()
    assert row[1] == "balanced"
    assert row[2]["memory_gb"] == BIG
    # the loop keeps cycling while paused (presence refreshes last_seen_at), yet never acquires
    first = row[3]
    seen: list[Any] = []

    async def cycled() -> bool:
        cur = (await live.exec_row())[3]
        if cur != first and cur not in seen:
            seen.append(cur)
        return len(seen) >= 2

    await eventually(cycled, 30, "two presence refreshes while paused")
    assert await live.attempts("gate") == []
    assert not await live.inflight()
    st = ex.status()
    assert st is not None and st.paused_reason == reason and st.subtask is None

    ex.signals()  # clear
    await eventually(lambda: _done(live, "gate"), 60, f"task done\n{ex.logs()}")
    assert [o for _, o in await live.attempts("gate")] == ["SUCCEEDED"]
    assert (await live.exec_row())[0] is None  # acquire clears the paused reason
    assert_exactly_once(live.out_ids("gate"), [f"f{i}" for i in range(10)])


async def _done(live: Live, name: str) -> bool:
    return await live.task_status(name) in ("SUCCEEDED", "FAILED", "PARTIAL")


# --- (b) a short pause lets the same attempt continue ---


async def test_short_pause_mid_subtask_same_attempt_succeeds(live: Live, ex: Exec) -> None:
    """AC #2: a pause shorter than pause-release freezes the worker (heartbeats go on) and the
    SAME attempt resumes and completes; nothing is released."""
    body = slow_body(50)  # ~5 s of work in 0.1 s ticks
    await live.submit("short", body)
    await ex.start(MINION_EXECUTOR_PAUSE_RELEASE_AFTER_S="120")
    await running_subtask(ex)
    ex.signals(thermal="serious")
    await paused_row(live, "thermal")  # via heartbeat: the loop is inside the subtask
    pid_before = ex.workers()
    # held paused: the subtask cannot complete (hold covers a few heartbeats and polls)
    t0 = time.monotonic()
    while time.monotonic() - t0 < 2.5:
        assert await live.attempts("short") == []
        assert await live.inflight()
        await asyncio.sleep(0.25)
    ex.signals()  # clear
    await eventually(lambda: _done(live, "short"), 60, f"task done\n{ex.logs()}")
    assert await live.attempts("short") == [(1, "SUCCEEDED")]
    assert pid_before  # the worker was never replaced during the pause
    assert_exactly_once(live.out_ids("short"), ["rslow", "r0", "r1", "r2"])


# --- (c) a long pause releases; the re-run is exactly-once ---


async def test_long_pause_releases_and_rerun_has_no_duplicate_output(live: Live, ex: Exec) -> None:
    """AC #2 / invariant 5: paused >= pause-release -> worker killed, subtask RELEASED, nothing
    promoted; after the pause it is re-acquired (attempt 2) and the output is promoted once."""
    await live.submit("long", slow_body(50))
    await ex.start(MINION_EXECUTOR_PAUSE_RELEASE_AFTER_S="2")
    await running_subtask(ex)
    ex.signals(thermal="serious")

    async def released() -> bool:
        return await live.attempts("long") == [(1, "RELEASED")]

    await eventually(released, 30, f"released\n{ex.logs()}")
    assert not await live.inflight()
    assert live.final_keys("long") == []  # the interrupted attempt promoted nothing
    assert (await live.rows("SELECT status FROM minion_subtask WHERE task_name='long'"))[0][0] == (
        "PENDING"
    )
    await eventually(lambda: not ex.workers(), 15, "worker killed on release")
    assert (await live.exec_row())[0] == "thermal"  # still paused: no re-acquire yet

    ex.signals()
    await eventually(lambda: _done(live, "long"), 60, f"task done\n{ex.logs()}")
    assert await live.attempts("long") == [(1, "RELEASED"), (2, "SUCCEEDED")]
    assert live.final_keys("long") == [f"outputs/{TEAM}/long/000000.jsonl"]
    assert_exactly_once(live.out_ids("long"), ["rslow", "r0", "r1", "r2"])


# --- (d) memory pressure critical releases at once ---


async def test_memory_critical_releases_immediately(live: Live, ex: Exec) -> None:
    """AC #2: critical memory pressure interrupts + releases at once (pause-release is 300 s here,
    so only the critical rule can have done it); the model memory is back (worker killed)."""
    await live.submit("crit", slow_body(50))
    await ex.start()
    await running_subtask(ex)
    ex.signals(memory_pressure="critical")

    async def released() -> bool:
        return await live.attempts("crit") == [(1, "RELEASED")]

    await eventually(released, 15, f"released\n{ex.logs()}")
    await eventually(lambda: not ex.workers(), 15, "worker killed")
    assert live.final_keys("crit") == []
    st = ex.status()
    assert st is not None and st.paused_reason == "memory_pressure" and st.subtask is None
    assert not await live.inflight()

    ex.signals()
    await eventually(lambda: _done(live, "crit"), 60, f"task done\n{ex.logs()}")
    assert await live.attempts("crit") == [(1, "RELEASED"), (2, "SUCCEEDED")]
    assert_exactly_once(live.out_ids("crit"), ["rslow", "r0", "r1", "r2"])


# --- (e) downgrade: finish the batch, release, unload, status ---


async def test_owner_downgrade_finishes_batch_releases_and_unloads(live: Live, ex: Exec) -> None:
    """FR-54 / AC #3: `profile light` drops the budget below the loaded model. The running batch
    is allowed to finish, then the subtask is released (BUDGET_DOWNGRADE), the worker unloaded and
    status.json shows the new budget. Back on `profile balanced` the re-run succeeds."""
    # batch 1 (8 records, balanced batch size) holds an 8 s non-ticking record; 4 more follow
    recs = [{"id": "slow", "text": "x", "sleep_s": 8}] + [
        {"id": f"d{i}", "text": "t"} for i in range(11)
    ]
    body = b"".join(json.dumps(r).encode() + b"\n" for r in recs)
    await live.submit("down", body, rps=12, model="fake-big")
    await ex.start()
    await running_subtask(ex)
    st = ex.status()
    assert st is not None and st.budget.memory_gb == BIG

    cp = ex.cli("profile", "light")
    assert cp.returncode == 0, cp.stderr

    def lowered() -> bool:
        s = ex.status()
        return s is not None and s.budget.memory_gb == SMALL

    await eventually(lowered, 15, f"status shows light budget\n{ex.logs()}")
    s = ex.status()
    assert s is not None and s.profile.value == "light" and s.profile_source == "owner"
    # the batch is still running: it was allowed to finish, not cut off
    assert await live.attempts("down") == []
    assert await live.inflight()
    assert ex.workers()

    async def released() -> bool:
        return await live.attempts("down") == [(1, "RELEASED")]

    await eventually(released, 30, f"released\n{ex.logs()}")
    assert live.final_keys("down") == []  # batch 2 never ran; nothing promoted
    await eventually(lambda: not ex.workers(), 15, "worker unloaded")
    s = ex.status()
    assert s is not None and s.subtask is None and s.budget.memory_gb == SMALL
    assert "BUDGET_DOWNGRADE" in ex.logs() or "budget_downgrade" in ex.logs()
    # the server does not hand a 1 GB model to a 0.5 GB budget: no churn while on light
    assert not await live.inflight()

    cp = ex.cli("profile", "balanced")
    assert cp.returncode == 0, cp.stderr
    await eventually(lambda: _done(live, "down"), 60, f"task done\n{ex.logs()}")
    assert await live.attempts("down") == [(1, "RELEASED"), (2, "SUCCEEDED")]
    assert_exactly_once(live.out_ids("down"), ["slow"] + [f"d{i}" for i in range(11)])


# --- (f) owner pause via the CLI ---


async def test_owner_pause_cli_pauses_and_resume_continues(live: Live, ex: Exec) -> None:
    """FR-56: `minion-executor pause` (control.json in the state dir) stops acquires and shows in
    `status` and presence as user_paused; `resume` lifts it at once."""
    await live.submit("owner", fast_body(10))
    # park the executor first so the pause cannot lose a race with the acquire
    await ex.start(signals={"thermal": "serious"})
    await paused_row(live, "thermal")
    cp = ex.cli("pause")
    assert cp.returncode == 0, cp.stderr
    assert (ex.state_dir / "control.json").is_file()
    ex.signals()  # signals clear: only the owner pause remains

    await paused_row(live, "user_paused")  # reason priority: user_paused over thermal
    st = ex.cli("status")
    assert st.returncode == 0, st.stderr
    assert "paused (user_paused)" in st.stdout
    # several governor polls + presence rounds later it still has not acquired
    row = await live.exec_row()
    first, seen = row[3], 0
    while seen < 3:
        cur = (await live.exec_row())[3]
        if cur != first:
            first, seen = cur, seen + 1
        await asyncio.sleep(0.1)
    assert await live.attempts("owner") == []
    assert not await live.inflight()

    cp = ex.cli("resume")
    assert cp.returncode == 0, cp.stderr
    await eventually(lambda: _done(live, "owner"), 60, f"task done\n{ex.logs()}")
    assert [o for _, o in await live.attempts("owner")] == ["SUCCEEDED"]
    assert_exactly_once(live.out_ids("owner"), [f"f{i}" for i in range(10)])


# --- D14: no power assertion (macOS) ---

SLEEP_BLOCKERS = (
    "PreventUserIdleSystemSleep",
    "PreventSystemSleep",
    "NoIdleSleep",
    "PreventUserIdleDisplaySleep",
    "NoDisplaySleep",
)


def assertions_held_by(pids: set[int]) -> list[str]:
    """`pmset -g assertions` detail lines of the form `pid N(name): ... <Type> named: ...` whose
    pid is in `pids` and whose type keeps the Mac (or its display) awake."""
    out = subprocess.run(
        ["/usr/bin/pmset", "-g", "assertions"], capture_output=True, text=True, check=True
    ).stdout
    held: list[str] = []
    for line in out.splitlines():
        m = re.match(r"\s*pid (\d+)\(", line)
        if m and int(m.group(1)) in pids and any(t in line for t in SLEEP_BLOCKERS):
            held.append(line.strip())
    return held


@pytest.mark.macos
async def test_running_executor_holds_no_sleep_preventing_power_assertion(
    live: Live, ex: Exec
) -> None:
    """ADR-0006 D14 / invariant 6: while a subtask runs, neither the executor nor its worker
    holds a PreventUserIdleSystemSleep / PreventSystemSleep / NoIdleSleep assertion. A control
    (`caffeinate -i`) proves the pmset filter would see one."""
    control = await asyncio.create_subprocess_exec("/usr/bin/caffeinate", "-i", "-t", "120")
    try:
        await eventually(lambda: bool(assertions_held_by({control.pid})), 15, "control assertion")
        assert any("PreventUserIdleSystemSleep" in h for h in assertions_held_by({control.pid}))

        await live.submit("sleepy", slow_body(80))  # ~8 s of running work
        await ex.start()
        await running_subtask(ex)
        for _ in range(3):
            mine = {ex.pid, *descendants(ex.pid, tracker=True)}
            assert len(mine) >= 2  # the executor and a live worker
            assert assertions_held_by(mine) == []
            await asyncio.sleep(0.5)
        # and the assertion table was readable and non-trivial the whole time
        assert assertions_held_by({control.pid})
        await eventually(lambda: _done(live, "sleepy"), 60, f"task done\n{ex.logs()}")
    finally:
        if control.returncode is None:
            control.terminate()
            await control.wait()
