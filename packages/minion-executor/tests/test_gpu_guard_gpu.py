"""GPU duty guard against a real worker on the Apple GPU (TASK-15 step 5; opt in: `pytest -m gpu`).

Real IOKit reader, Seatbelt-sandboxed `WorkerHandle`, real `ExecutorLoop` with the default guard
thresholds (10 s window, +10 pp margin, 30 s kill) and the 2 s guard interval. SIGSTOP duty gating
(TASK-15.1) is not part of this suite: a GATE verdict is only logged, so the rogue is stopped by
the KILL rung.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from minion_common.enums import CapacityProfile, Engine, PausedReason
from minion_common.schemas import (
    Assignment,
    Budget,
    HeartbeatResponse,
    ModelEntry,
    PresignedPost,
)
from minion_executor.capacity.governor import Decision, ProfileSource
from minion_executor.capacity.gpu_guard import GpuAction, GpuGuardConfig
from minion_executor.capacity.gpu_meter import default_gpu_reader
from minion_executor.capacity.profiles import PROFILES, budget_for
from minion_executor.capacity.runtime import FixedGovernor
from minion_executor.loop import ExecutorLoop
from minion_executor.model_cache import FetchEnv, ModelCache
from minion_executor.worker.process import WorkerHandle
from minion_executor.worker.sandbox import SandboxPolicy

pytestmark = [pytest.mark.gpu, pytest.mark.macos]

GIB = 1024**3
REPO_ROOT = Path(__file__).resolve().parents[3]
CACHE_DIR = Path(
    os.environ.get("MINION_TEST_MODEL_CACHE", Path.home() / ".cache" / "open-minions-test-models")
)
DEFAULTS = GpuGuardConfig()  # the shipped thresholds: this suite is what justifies them
HONEST_S = 40.0
PROMPTS = ["Say hi.", "What is 2+2?", "Name a color.", "Reply with the word yes."] * 2


class Client:
    """Stub server: hands out `make()` assignments until `deadline`, then stops the loop."""

    def __init__(self, make: Any, deadline: float | None = None) -> None:
        self.make, self.deadline = make, deadline
        self.completed: list[Any] = []
        self.failed: list[Any] = []
        self.released: list[str] = []
        self.loop: ExecutorLoop | None = None

    async def register(self, req: Any) -> None: ...

    async def acquire(self, req: Any) -> Assignment | None:
        if self.deadline is not None and time.monotonic() >= self.deadline:
            assert self.loop is not None
            self.loop.stop()
            return None
        if self.deadline is None and (self.completed or self.released):
            assert self.loop is not None
            self.loop.stop()
            return None
        return self.make()  # type: ignore[no-any-return]

    async def complete(self, aid: uuid.UUID, req: Any) -> None:
        self.completed.append(req)

    async def fail(self, aid: uuid.UUID, req: Any) -> None:
        self.failed.append(req)

    async def release(self, aid: uuid.UUID, req: Any = None) -> None:
        self.released.append(getattr(req, "code", None) or str(req))

    async def heartbeat(self, aid: uuid.UUID, req: Any) -> HeartbeatResponse:
        return HeartbeatResponse(
            cancel=False,
            output_post=PresignedPost(
                url="https://s3.test/out", fields={"key": "k", "policy": "p"}
            ),
            expires_at=datetime.now(UTC) + timedelta(minutes=5),
        )


def _assignment(model: ModelEntry, body: bytes, count: int, params: dict[str, Any]) -> Assignment:
    return Assignment.model_validate(
        {
            "acquirement_id": str(uuid.uuid4()),
            "attempt": 1,
            "team": "t",
            "project": "p",
            "task_name": "n",
            "subtask_id": 0,
            "type": model.type.value,
            "model": model.model_dump(mode="json"),
            "params": params,
            "input_url": "https://s3.test/in",
            "byte_start": 0,
            "byte_end": len(body),
            "record_count": count,
            "output_post": {"url": "https://s3.test/out", "fields": {"key": "k", "policy": "p"}},
            "expires_at": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
        }
    )


def _http(body: bytes) -> httpx.AsyncClient:
    def s3(req: httpx.Request) -> httpx.Response:
        if req.method == "GET":
            return httpx.Response(206, content=body)
        req.read()
        return httpx.Response(200)

    return httpx.AsyncClient(transport=httpx.MockTransport(s3))


def _assert_no_children() -> None:
    """Every test tears its worker (and the burn thread inside it) down."""
    import multiprocessing

    deadline = time.monotonic() + 5
    while multiprocessing.active_children() and time.monotonic() < deadline:
        time.sleep(0.1)
    assert multiprocessing.active_children() == []


@pytest.fixture
def quiet_gpu() -> None:
    """Rogue tests need a GPU the host is not using (a video wallpaper or a busy WindowServer
    time-slices it and caps the burn's measured share): skip, never flake, on a busy host."""
    read = default_gpu_reader()
    assert read is not None
    me = os.getpid()

    def others() -> float:
        return float(sum(sum(c.values()) for pid, c in read().items() if pid != me))

    a, t0 = others(), time.monotonic()
    time.sleep(3)
    busy = (others() - a) / 1e9 / (time.monotonic() - t0)
    if busy > 0.15:
        pytest.skip(f"host GPU is busy ({busy:.0%} used by other processes)")


def test_burn_hook_saturates_the_gpu(quiet_gpu: None) -> None:
    """The rogue hook must measure far above any limit (standalone, real IOKit reader)."""
    pytest.importorskip("mlx.core")
    import subprocess
    import sys

    from minion_executor.worker.footprint import process_footprint_bytes

    read = default_gpu_reader()
    assert read is not None
    # A child process (TASK-44): the burn's Metal buffers must never live in the pytest process.
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "from minion_executor.runners.fake import _gpu_burn; _gpu_burn(10.0)",
        ]
    )
    try:
        time.sleep(1.5)
        shares: list[float] = []
        peak = 0
        for _ in range(4):  # 2 s windows: a transient host dip must not hide the saturation
            a, t0 = sum(read().get(child.pid, {}).values()), time.monotonic()
            time.sleep(2)
            b, t1 = sum(read().get(child.pid, {}).values()), time.monotonic()
            shares.append((b - a) / 1e9 / (t1 - t0))
            peak = max(peak, process_footprint_bytes(child.pid) or 0)
        rc = child.poll()
        if rc not in (None, 0):
            err = child.stderr.read().decode(errors="replace")[-2000:] if child.stderr else ""
            raise AssertionError(f"burn child died rc={rc}: {err}")
        assert max(shares) >= 0.8, shares
        assert peak <= 2 << 30, f"burn footprint {peak / 2**30:.2f} GiB"
    finally:
        child.kill()
        child.wait(10)


class CountingReader:
    """The real IOKit reader, counting calls (proves the guard was watching)."""

    def __init__(self) -> None:
        read = default_gpu_reader()
        assert read is not None
        self._read = read
        self.calls = 0

    def __call__(self) -> dict[int, dict[str, int]]:
        self.calls += 1
        return self._read()


def _duty(profile: CapacityProfile) -> Budget:
    return budget_for(profile, total_gb=16.0)


# --- (a) rogue worker ---------------------------------------------------------------------------


async def test_rogue_unpaced_worker_is_killed_and_released(tmp_path: Path, quiet_gpu: None) -> None:
    """A hook that ignores pacing burns the GPU at light (duty 0.3): the control side kills the
    worker and releases the subtask with GPU_DUTY_VIOLATION within detection + 30 s."""
    pytest.importorskip("mlx.core")
    model = ModelEntry(
        model_id="fake-embed", type="embedding", engine=Engine.FAKE,
        ref="o/m", revision="a" * 40, memory_gb=0.1,
    )  # fmt: skip
    body = json.dumps({"id": "rogue", "text": "x", "gpu_burn_s": 300}).encode() + b"\n"
    budget = _duty(CapacityProfile.LIGHT)
    assert budget.gpu_duty_cycle == pytest.approx(0.3)
    client = Client(lambda: _assignment(model, body, 1, {}))
    worker = WorkerHandle(
        [Engine.FAKE], test_hooks=True, sandbox=SandboxPolicy(cache_root=tmp_path),
        tick_interval_s=0.2, step_timeout_s=300,
    )  # fmt: skip
    reader = CountingReader()
    loop = ExecutorLoop(
        client,  # type: ignore[arg-type]
        worker, budget, [Engine.FAKE], http=_http(body), tmp_root=tmp_path,
        governor=FixedGovernor(budget), gpu_reader=reader, gpu_guard=DEFAULTS,
    )  # fmt: skip
    client.loop = loop
    guard = loop._gpu
    assert guard is not None
    observe = guard.observe
    seen: dict[str, float] = {}

    def spy(*a: Any, **kw: Any) -> Any:
        v = observe(*a, **kw)
        if v.action is not GpuAction.OK:
            seen.setdefault("detected", time.monotonic())
        if v.action is GpuAction.KILL:
            seen["killed"] = time.monotonic()
        return v

    guard.observe = spy  # type: ignore[method-assign]
    t0 = time.monotonic()
    try:
        await asyncio.wait_for(loop.run(max_subtasks=1), 120)
    finally:
        worker.close()
        _assert_no_children()
    elapsed = time.monotonic() - t0
    print(f"GPUTIMES detect={seen['detected'] - t0:.1f} kill={seen['killed'] - t0:.1f}")
    assert client.released == ["GPU_DUTY_VIOLATION"], (client.released, client.failed)
    assert client.completed == [] and client.failed == []  # nothing uploaded on a verdict
    assert loop._status_snapshot().gpu_violation == "over_duty"
    assert reader.calls >= 5
    # detection (first full window, about 10 s after the burn starts), then the 20 GPU-s bucket
    assert (
        seen["killed"] - seen["detected"]
        <= DEFAULTS.kill_budget_gpu_s / 0.4 + 2 * loop._guard_s + 1.0
    )
    assert seen["detected"] - t0 <= DEFAULTS.window_s + 10.0  # spawn + first window + a sample
    assert elapsed <= DEFAULTS.window_s + DEFAULTS.kill_budget_gpu_s / 0.4 + 20.0, elapsed


# --- (b) honest, paced worker with real MLX inference ---------------------------------------


def _models() -> dict[str, ModelEntry]:
    raw = yaml.safe_load((REPO_ROOT / "deploy" / "models.yaml").read_text())["models"]
    return {m["model_id"]: ModelEntry(**m) for m in raw}


QWEN = _models()["qwen2.5-0.5b-instruct"]


@pytest.fixture(scope="module")
async def qwen_cache() -> ModelCache:
    await asyncio.to_thread(CACHE_DIR.mkdir, parents=True, exist_ok=True)
    cache = ModelCache(CACHE_DIR, 5 * GIB, fetch_env=FetchEnv.capture())
    assert await cache.ensure(QWEN) is not None
    return cache


RECORDS = {CapacityProfile.LIGHT: 100, CapacityProfile.BALANCED: 150, CapacityProfile.FULL: 300}


def _record_shares(loop: ExecutorLoop) -> list[float]:
    """Every judged share the guard computes (None = not yet judged is skipped)."""
    shares: list[float] = []
    guard = loop._gpu
    assert guard is not None
    observe = guard.observe

    def spy(*a: Any, **kw: Any) -> Any:
        v = observe(*a, **kw)
        if v.share is not None:
            shares.append(v.share)
        return v

    guard.observe = spy  # type: ignore[method-assign]
    return shares


@pytest.mark.model
@pytest.mark.parametrize(
    "profile", [CapacityProfile.LIGHT, CapacityProfile.BALANCED, CapacityProfile.FULL]
)
async def test_honest_mlx_worker_is_never_killed(
    profile: CapacityProfile, qwen_cache: ModelCache, tmp_path: Path
) -> None:
    """Real Qwen inference through the paced runner for ~40 s: the guard watches (real IOKit
    meter, shipped thresholds) and never gates, kills or releases."""
    # long subtasks: the guard judges full windows (the window also spans subtask boundaries)
    prompts = (PROMPTS * 40)[: RECORDS[profile]]
    body = b"".join(
        json.dumps({"id": f"r{i}", "prompt": p}).encode() + b"\n" for i, p in enumerate(prompts)
    )
    budget = _duty(profile)
    assert budget.gpu_duty_cycle == PROFILES[profile].gpu_duty_cycle
    client = Client(
        lambda: _assignment(QWEN, body, len(prompts), {"max_tokens": 48}),
        deadline=time.monotonic() + HONEST_S,
    )
    worker = WorkerHandle(
        [Engine.MLX], sandbox=SandboxPolicy(cache_root=CACHE_DIR),
        tick_interval_s=0.2, step_timeout_s=300,
    )  # fmt: skip
    reader = CountingReader()
    loop = ExecutorLoop(
        client,  # type: ignore[arg-type]
        worker, budget, [Engine.MLX], http=_http(body), tmp_root=tmp_path,
        model_cache=qwen_cache, governor=FixedGovernor(budget),
        gpu_reader=reader, gpu_guard=DEFAULTS,
    )  # fmt: skip
    client.loop = loop
    shares = _record_shares(loop)
    try:
        done = await asyncio.wait_for(loop.run(), 300)
    finally:
        worker.close()
        _assert_no_children()
    assert client.failed == [] and client.released == [], (client.failed, client.released)
    assert done >= 1 and len(client.completed) == done
    assert loop._status_snapshot().gpu_violation is None
    assert reader.calls >= 5 and shares, "the guard never judged a window"
    assert max(shares) <= budget.gpu_duty_cycle + DEFAULTS.margin, shares


class OneShotClient(Client):
    """One assignment, then an empty queue (the loop idles with a warm worker)."""

    def __init__(self, make: Any) -> None:
        super().__init__(make)
        self.given = False

    async def acquire(self, req: Any) -> Assignment | None:
        if self.given:
            return None
        self.given = True
        return self.make()  # type: ignore[no-any-return]


@pytest.mark.model
async def test_honest_worker_paused_mid_batch_then_idle_is_never_killed(
    qwen_cache: ModelCache, tmp_path: Path
) -> None:
    """Real Qwen: paused mid-batch (the in-flight step finishes, then the worker acks and goes
    quiet), resumed, finished, then a warm idle worker for more than one guard window: no
    gpu_duty_kill, no violation in status."""
    profile = CapacityProfile.BALANCED
    prompts = (PROMPTS * 40)[:150]
    body = b"".join(
        json.dumps({"id": f"r{i}", "prompt": p}).encode() + b"\n" for i, p in enumerate(prompts)
    )
    budget = _duty(profile)
    client = OneShotClient(lambda: _assignment(QWEN, body, len(prompts), {"max_tokens": 48}))
    worker = WorkerHandle(
        [Engine.MLX], sandbox=SandboxPolicy(cache_root=CACHE_DIR),
        tick_interval_s=0.2, step_timeout_s=300,
    )  # fmt: skip
    gov = FixedGovernor(budget)

    def decision(paused: bool) -> Decision:
        return Decision(
            profile=profile, budget=budget,
            paused_reason=PausedReason.THERMAL if paused else None,
            release_now=False, pause_expired=False, trim=False,
            profile_source=ProfileSource.PINNED, candidate=None, candidate_since=None,
        )  # fmt: skip

    loop = ExecutorLoop(
        client,  # type: ignore[arg-type]
        worker, budget, [Engine.MLX], http=_http(body), tmp_root=tmp_path,
        model_cache=qwen_cache, governor=gov, gpu_reader=CountingReader(), gpu_guard=DEFAULTS,
    )  # fmt: skip
    guard = loop._gpu
    assert guard is not None
    kills: list[str] = []
    observe = guard.observe

    def spy(*a: Any, **kw: Any) -> Any:
        v = observe(*a, **kw)
        if v.action is GpuAction.KILL:
            kills.append(v.reason)
        return v

    guard.observe = spy  # type: ignore[method-assign]
    task = asyncio.create_task(loop.run())
    try:
        deadline = time.monotonic() + 120
        while loop._batch_budget is None and time.monotonic() < deadline:  # noqa: ASYNC110
            await asyncio.sleep(0.2)
        await asyncio.sleep(4)  # mid-batch
        gov.update(decision(True))
        await asyncio.sleep(12)  # longer than a window: acked and quiet
        gov.update(decision(False))
        while not client.completed and time.monotonic() < deadline + 120:  # noqa: ASYNC110
            await asyncio.sleep(0.5)
        await asyncio.sleep(DEFAULTS.window_s + 6)  # warm and idle for more than one window
        loop.stop()
        await asyncio.wait_for(task, 60)
    finally:
        worker.close()
        _assert_no_children()
    assert client.completed, "the subtask never completed"
    assert kills == [] and client.released == [] and client.failed == []
    assert loop._status_snapshot().gpu_violation is None


async def test_rogue_in_short_subtasks_is_killed(tmp_path: Path, quiet_gpu: None) -> None:
    """Back-to-back ~1 s subtasks (honestly paced) while a background thread of the worker keeps
    the GPU busy: no subtask is long enough to be judged alone, the guard window must span them
    and still kill (release GPU_DUTY_VIOLATION)."""
    pytest.importorskip("mlx.core")
    model = ModelEntry(
        model_id="fake-embed", type="embedding", engine=Engine.FAKE,
        ref="o/m", revision="a" * 40, memory_gb=0.1,
    )  # fmt: skip
    body = (
        json.dumps({"id": "rogue", "text": "x", "gpu_burn_s": 70, "no_pace": True}).encode() + b"\n"
    )
    budget = _duty(CapacityProfile.LIGHT)
    client = Client(lambda: _assignment(model, body, 1, {}), deadline=time.monotonic() + 80)
    worker = WorkerHandle(
        [Engine.FAKE], test_hooks=True, sandbox=SandboxPolicy(cache_root=tmp_path),
        tick_interval_s=0.2, step_timeout_s=300,
    )  # fmt: skip
    loop = ExecutorLoop(
        client,  # type: ignore[arg-type]
        worker, budget, [Engine.FAKE], http=_http(body), tmp_root=tmp_path,
        governor=FixedGovernor(budget), gpu_reader=CountingReader(), gpu_guard=DEFAULTS,
    )  # fmt: skip
    client.loop = loop
    try:
        await asyncio.wait_for(loop.run(), 200)
    finally:
        worker.close()
        _assert_no_children()
    assert "GPU_DUTY_VIOLATION" in client.released, (
        client.released,
        len(client.completed),
        loop._status_snapshot().gpu_share,
    )
