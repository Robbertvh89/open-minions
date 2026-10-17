"""CI smoke (TASK-7.2 c): 20 sims, 2000 subtasks, faults on, through the CLI; verify must pass."""

import asyncio
import json
import os
import sys
import time
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from minion_scale.report import SCHEMA, SECTIONS
from minion_scale.seed import read_tokens_file
from minion_watchdog.main import run_watchdog
from minion_watchdog.settings import WatchdogSettings
from pydantic import SecretStr

from conftest import REPO_ROOT, Live, S3Env, _serve

pytestmark = pytest.mark.integration

MODELS = REPO_ROOT / "deploy" / "scale" / "models.scale.yaml"
BUDGET_S = 90.0


@pytest.fixture
async def lv(fresh_url: str, s3: S3Env, tmp_path: Path) -> AsyncIterator[Live]:
    async with _serve(fresh_url, s3, tmp_path, {"models_file": str(MODELS)}) as live:
        yield live


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


async def test_smoke_20_sims_2k_subtasks_with_faults(
    lv: Live, fresh_url: str, s3: S3Env, tmp_path: Path
) -> None:
    work = tmp_path / "cwd"  # no .env here: settings come from the env below only
    work.mkdir()
    env = {
        **os.environ,
        "MINION_DATABASE_URL": fresh_url,
        "MINION_S3_ENDPOINT_URL": s3.endpoint_url,
        "MINION_S3_ACCESS_KEY": s3.access_key,
        "MINION_S3_SECRET_KEY": s3.secret_key,
        "MINION_S3_BUCKET": s3.bucket,
    }
    tokens_path = tmp_path / "tokens.json"
    outputs: list[str] = []

    async def cli(*args: str) -> str:
        rc, out = await _cli(env, work, *args)
        outputs.append(out)
        diag = ""
        if rc != 0 and (work / "run.json").exists():  # what the run saw, for a CI post-mortem
            doc = json.loads((work / "run.json").read_text()).get("run", {})
            diag = "\n" + json.dumps(
                {
                    k: doc.get(k)
                    for k in ("assigned", "completed", "drained", "duration_s", "events", "faults")
                }
            )
            diag += "\nacquire_assigned " + json.dumps(
                {
                    k: v
                    for k, v in doc.get("calls", {}).get("acquire_assigned", {}).items()
                    if k.startswith(("p", "count", "mean"))
                }
            )
        assert rc == 0, out[-2000:] + diag
        return out

    await cli(
        "seed",
        "--tasks=20",
        "--subtasks=100",
        "--executors=20",
        f"--tokens-file={tokens_path}",
        f"--models-file={MODELS}",
    )
    assert tokens_path.stat().st_mode & 0o077 == 0
    await cli("explain", f"--models-file={MODELS}", "--heartbeat-timeout-s=2", "--out=explain.json")

    watchdog_cfg = WatchdogSettings(
        database_url=SecretStr("unused"), heartbeat_timeout_s=2.0, watchdog_interval_s=0.5
    )
    stop = asyncio.Event()
    watchdog = asyncio.create_task(run_watchdog(lv.sm, watchdog_cfg, stop))
    started = time.monotonic()
    try:
        await cli(
            "run",
            f"--server={lv.url}",
            f"--tokens-file={tokens_path}",
            "--procs=2",
            "--work-s=0.05",
            "--heartbeat-interval-s=0.5",
            "--backoff-lo=0.05",
            "--backoff-hi=0.2",
            "--acquire-pace-s=0",
            "--max-consecutive-failures=5",
            "--failure-pause-s=2",
            "--vanish=0.01",
            "--release=0.02",
            "--fail=0.02",
            "--late=0.005",
            "--late-wait-s=4",
            "--poll-s=0.5",
            f"--max-duration-s={BUDGET_S}",
            "--out=run.json",
        )
    finally:
        stop.set()
        await watchdog
    elapsed = time.monotonic() - started
    print(f"\nsmoke run phase: {elapsed:.1f} s")
    assert elapsed < BUDGET_S

    await cli("verify", "--out=verify.json")
    await cli("report", "run.json", "explain.json", "verify.json", "--out=report.json")

    report = json.loads((work / "report.json").read_text())
    assert report["schema"] == SCHEMA and set(report) <= {"schema", "generated_at", *SECTIONS}
    run = report["run"]
    assert run["server_errors_5xx"] == 0 and run["transport_errors"] == 0
    assert run["sims"] == 20 and run["procs"] == 2
    assert run["completed"] >= 1900  # the rest ended FAILED after exhausting retries or 409s
    assert sum(run["faults"].values()) > 0 and run["conflicts_409"] >= 0
    assert report["verify"]["ok"], report["verify"]
    assert report["verify"]["counts"]["subtasks"] == 2000
    assert {e["name"] for e in report["explain"]} == {
        "candidate",
        "candidate_preference",
        "stale_heartbeat",
        "over_lease",
    }
    assert all(e["execution_ms"] is not None for e in report["explain"])
    assert run["calls"]["acquire_assigned"]["count"] >= 2000

    # no credentials or payload anywhere in what the harness printed or wrote
    blob = "\n".join(outputs) + (work / "report.json").read_text()
    for cred in read_tokens_file(tokens_path):
        assert cred.token.get_secret_value() not in blob
    for needle in ("X-Amz-Signature", "X-Amz-Credential", '"text"', s3.secret_key, "Bearer"):
        assert needle not in blob
