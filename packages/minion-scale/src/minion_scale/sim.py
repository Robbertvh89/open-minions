"""Simulated executors (DEV ONLY): the real REST protocol, no inference, no database.

Each sim is a pared-down executor loop: register, acquire, sleep `work_s`, heartbeat, PUT a tiny
output to the presigned URL, complete. It talks to the server through `minion_common.ExecutorClient`
and to object storage through presigned URLs only (invariants 1 and 4): this module and its
imports must stay free of sqlalchemy/asyncpg (a test enforces that). Back-off after an empty
acquire (204), a 429 or a failure follows the real executor's loop; a test compares the constants
and the delay formulas with `minion_executor.loop`.

Injected faults, drawn per assignment: `vanish` (stop beating and walk away: the watchdog must
reclaim), `release`, `fail` (retryable), `late` (go silent past the heartbeat timeout, then
complete: the server must answer 409).
"""

import asyncio
import contextlib
import enum
import hashlib
import math
import multiprocessing as mp
import queue
import random
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, NamedTuple, TypeVar

import httpx
from minion_common.client import ApiError, ExecutorClient, StaleAcquirementError
from minion_common.enums import CapacityProfile, Engine
from minion_common.schemas import (
    AcquireRequest,
    Assignment,
    Budget,
    CompleteRequest,
    FailRequest,
    HeartbeatRequest,
    RegisterRequest,
    ReleaseRequest,
)
from pydantic import SecretStr

from minion_scale.metrics import TRANSPORT, Recorder, merge_dicts, monitor_lag

T = TypeVar("T")

# Mirrors of the executor's constants (checked against minion_executor in the unit tests).
EXECUTOR_BACKOFF: tuple[float, float] = (10.0, 30.0)  # loop.py `backoff` default
MAX_RETRY_AFTER_S = 300.0  # loop.py MAX_RETRY_AFTER_S
ACQUIRE_PACE_S = 0.4  # ExecutorSettings.acquire_pace_s default
MAX_CONSECUTIVE_FAILURES = 5  # ExecutorLoop max_consecutive_failures default
FAILURE_PAUSE_S = 300.0  # ExecutorLoop failure_pause_s default
FAULT_CODE = "SCALE_FAULT"


def backoff_delay(n: int, backoff: tuple[float, float], rng: random.Random) -> float:
    """Exponential delay for the n-th consecutive empty/failed call (executor `_delay`)."""
    lo, hi = backoff
    return float(min(hi, lo * (2 ** min(n, 16))) * rng.uniform(0.5, 1.0))


def rate_limit_delay(
    retry_after: float | None,
    n: int,
    backoff: tuple[float, float],
    rng: random.Random,
    *,
    repeated: bool = False,
) -> float:
    """Back-off after a 429 (executor `_rate_limit_delay`): wait `Retry-After` (cap 300 s) plus
    0-20 % jitter; repeated 429s escalate to max(back-off, Retry-After)."""
    if retry_after is None:
        return backoff_delay(n, backoff, rng)
    ra = min(retry_after, MAX_RETRY_AFTER_S)
    if repeated:
        return max(backoff_delay(n, backoff, rng), ra * rng.uniform(1.0, 1.2))
    return float(ra * rng.uniform(1.0, 1.2))


class Outcome(enum.Enum):
    """What one assignment came to (the executor's `_Outcome`): only DONE resets the failure
    streak, only FAILED backs off."""

    DONE = enum.auto()
    FAILED = enum.auto()
    NEUTRAL = enum.auto()  # discarded, cancelled, vanished, stopped, released: neither


def _ok(done: bool) -> Outcome:
    return Outcome.NEUTRAL if done else Outcome.FAILED


class Fault(enum.StrEnum):
    NONE = "none"
    VANISH = "vanish"
    RELEASE = "release"
    FAIL = "fail"
    LATE = "late"


@dataclass(frozen=True)
class FaultRates:
    """Per-assignment probabilities; the rest of the mass is a normal run."""

    vanish: float = 0.0
    release: float = 0.0
    fail: float = 0.0
    late: float = 0.0

    def __post_init__(self) -> None:
        rates = (self.vanish, self.release, self.fail, self.late)
        if any(r < 0 for r in rates) or sum(rates) > 1:
            raise ValueError("fault rates must be >= 0 and sum to <= 1")

    def pick(self, rng: random.Random) -> Fault:
        x = rng.random()
        for fault, rate in (
            (Fault.VANISH, self.vanish),
            (Fault.RELEASE, self.release),
            (Fault.FAIL, self.fail),
            (Fault.LATE, self.late),
        ):
            if x < rate:
                return fault
            x -= rate
        return Fault.NONE


@dataclass(frozen=True)
class SimConfig:
    base_url: str
    work_s: float = 1.0
    heartbeat_interval_s: float = 5.0
    backoff: tuple[float, float] = EXECUTOR_BACKOFF
    acquire_pace_s: float = ACQUIRE_PACE_S
    faults: FaultRates = field(default_factory=FaultRates)
    # How long a `late` sim stays silent: must exceed the watchdog's timeout + a tick.
    late_wait_s: float = 30.0
    timeout_s: float = 30.0
    max_consecutive_failures: int = MAX_CONSECUTIVE_FAILURES
    failure_pause_s: float = FAILURE_PAUSE_S
    seed: int = 0


def sim_budget() -> Budget:
    return Budget.model_validate(
        {"profile": "balanced", "memory_gb": 8.0, "gpu_duty_cycle": 0.5, "max_batch_size": 8}
    )


def register_request(index: int) -> RegisterRequest:
    return RegisterRequest(
        hostname=f"scale-sim-{index:04d}",
        chip="sim",
        total_memory_gb=16.0,
        os_version="sim",
        engines=[Engine.FAKE],
        profile=CapacityProfile.BALANCED,
        budget=sim_budget(),
    )


def output_body(record_count: int) -> bytes:
    """Tiny synthetic output: one short line per record."""
    return b"{}\n" * record_count


class SimExecutor:
    def __init__(
        self,
        index: int,
        token: SecretStr,
        cfg: SimConfig,
        rec: Recorder,
        stop: asyncio.Event,
        put_http: httpx.AsyncClient,
    ) -> None:
        self.index = index
        self.cfg = cfg
        self.rec = rec
        self.stop = stop
        self._put = put_http
        self._rng = random.Random(f"{cfg.seed}:{index}")  # noqa: S311 - jitter, not security
        self._client = ExecutorClient(cfg.base_url, token, timeout=cfg.timeout_s)
        self._idle = 0
        self._fails = 0
        self._limited = 0
        self._cancelled = asyncio.Event()  # the server asked this sim to stop (task cancelled)

    async def _call(
        self,
        op: str,
        aw: Awaitable[T],
        *,
        ok_op: Callable[[T], str] | None = None,
        ok_status: Callable[[T], int] | None = None,
    ) -> T:
        """Await one API call, recording latency under `op` (or `ok_op(result)` on success, with
        `ok_status(result)` instead of 200, e.g. 204 for an empty acquire)."""
        t0 = time.perf_counter()
        try:
            result = await aw
        except ApiError as exc:
            self.rec.record(op, time.perf_counter() - t0, exc.status_code)
            raise
        except httpx.HTTPError:
            self.rec.record(op, time.perf_counter() - t0, TRANSPORT)
            raise
        self.rec.record(
            ok_op(result) if ok_op else op,
            time.perf_counter() - t0,
            ok_status(result) if ok_status else 200,
        )
        return result

    async def _sleep(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self.stop.wait(), timeout=seconds)

    async def run(self) -> None:
        try:
            if not await self._register():
                return
            last_acquire: float | None = None
            while not self.stop.is_set():
                if last_acquire is not None and self.cfg.acquire_pace_s > 0:
                    gap = last_acquire + self.cfg.acquire_pace_s - time.monotonic()
                    if gap > 0:
                        await self._sleep(gap)
                        if self.stop.is_set():
                            break
                last_acquire = time.monotonic()
                try:
                    assignment = await self._call(
                        "acquire_error",
                        self._client.acquire(
                            AcquireRequest(engines=[Engine.FAKE], budget=sim_budget())
                        ),
                        ok_op=lambda a: "acquire_empty" if a is None else "acquire_assigned",
                        ok_status=lambda a: 204 if a is None else 200,
                    )
                except (ApiError, httpx.HTTPError, ValueError) as exc:  # as loop.py acquire
                    if not await self._after_acquire_error(exc):
                        return
                    continue
                self._limited = 0
                if assignment is None:
                    await self._sleep(backoff_delay(self._idle, self.cfg.backoff, self._rng))
                    self._idle += 1
                    continue
                self._idle = 0
                self.rec.count("assigned")
                outcome = await self._handle(assignment)
                if outcome is Outcome.DONE:
                    self._fails = 0
                elif outcome is Outcome.FAILED:
                    await self._back_off_after_failure()
        finally:
            await self._client.aclose()

    async def _back_off_after_failure(self) -> None:
        """Executor `_back_off_after_failure`: exponential, and a long pause after
        `max_consecutive_failures` failures in a row."""
        self._fails += 1
        if self._fails >= self.cfg.max_consecutive_failures:
            self.rec.count("failure_pause")
            self._fails = 0
            await self._sleep(self.cfg.failure_pause_s)
        else:
            await self._sleep(backoff_delay(self._fails - 1, self.cfg.backoff, self._rng))

    async def _register(self) -> bool:
        attempt = 0
        limited_n = 0
        while not self.stop.is_set():
            try:
                await self._call("register", self._client.register(register_request(self.index)))
                return True
            except (ApiError, httpx.HTTPError) as exc:
                if isinstance(exc, ApiError) and exc.status_code < 500 and exc.status_code != 429:
                    self.rec.count("sim_aborted")
                    return False
                retry_after = exc.retry_after if isinstance(exc, ApiError) else None
                limited = isinstance(exc, ApiError) and exc.status_code == 429
                if limited:  # 429s are counted separately, like the executor's register loop
                    limited_n += 1
                await self._sleep(
                    rate_limit_delay(
                        retry_after if limited else None,
                        attempt,
                        self.cfg.backoff,
                        self._rng,
                        repeated=limited and limited_n > 1,
                    )
                )
                attempt += 1
        return False

    async def _after_acquire_error(self, exc: Exception) -> bool:
        """Executor loop semantics: 429 waits Retry-After, 5xx/transport back off, other 4xx end
        the executor. Returns False when this sim must stop."""
        limited = isinstance(exc, ApiError) and exc.status_code == 429
        if isinstance(exc, ApiError) and exc.status_code < 500 and not limited:
            self.rec.count("sim_aborted")
            return False
        if limited:
            self._limited += 1
            delay = rate_limit_delay(
                exc.retry_after if isinstance(exc, ApiError) else None,
                self._limited - 1,
                self.cfg.backoff,
                self._rng,
                repeated=self._limited > 1,
            )
        else:
            delay = backoff_delay(self._idle, self.cfg.backoff, self._rng)
            self._idle += 1
        await self._sleep(delay)
        return True

    async def _handle(self, a: Assignment) -> Outcome:
        """Run one assignment and say how it ended."""
        fault = self.cfg.faults.pick(self._rng)
        if fault is not Fault.NONE:
            self.rec.count(f"fault_{fault.value}")
        if fault is Fault.VANISH:
            return Outcome.NEUTRAL  # walk away: no beat, no complete; the watchdog reclaims it
        stale = asyncio.Event()
        self._cancelled.clear()
        beat: asyncio.Task[None] | None = None
        if fault is not Fault.LATE:
            beat = asyncio.create_task(self._heartbeats(a, stale))
        try:
            await self._wait(
                self.cfg.late_wait_s if fault is Fault.LATE else self.cfg.work_s, stale
            )
            if self._cancelled.is_set():  # executor: release with CANCELLED, no failure back-off
                self.rec.count("cancelled")
                await self._release(a, "CANCELLED")
                return Outcome.NEUTRAL
            if stale.is_set():
                self.rec.count("discarded_stale")
                return Outcome.NEUTRAL
            if self.stop.is_set():
                return _ok(await self._release(a, "STOP"))
            if fault is Fault.RELEASE:
                return _ok(await self._release(a, FAULT_CODE))
            if fault is Fault.FAIL:
                return _ok(await self._fail(a))
            if not await self._upload(a):
                return _ok(await self._fail(a, code="UPLOAD_FAILED"))
            return await self._complete(a, expect_stale=fault is Fault.LATE)
        finally:
            if beat is not None:
                beat.cancel()
                await asyncio.gather(beat, return_exceptions=True)

    async def _wait(self, seconds: float, stale: asyncio.Event) -> None:
        waiters = {asyncio.create_task(self.stop.wait()), asyncio.create_task(stale.wait())}
        try:
            await asyncio.wait(waiters, timeout=seconds, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for w in waiters:
                w.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)

    async def _heartbeats(self, a: Assignment, stale: asyncio.Event) -> None:
        while True:
            await self._sleep(self.cfg.heartbeat_interval_s)
            if self.stop.is_set():
                return
            try:
                resp = await self._call(
                    "heartbeat", self._client.heartbeat(a.acquirement_id, HeartbeatRequest())
                )
            except StaleAcquirementError:
                stale.set()
                return
            except (ApiError, httpx.HTTPError):
                continue  # a missed beat is retried at the next interval
            if resp.cancel:
                self._cancelled.set()
                stale.set()
                return

    async def _upload(self, a: Assignment) -> bool:
        body = output_body(a.record_count)
        t0 = time.perf_counter()
        try:
            resp = await self._put.put(a.output_url, content=body)
        except httpx.HTTPError:
            self.rec.record("output_put", time.perf_counter() - t0, TRANSPORT)
            return False
        self.rec.record("output_put", time.perf_counter() - t0, resp.status_code)
        return resp.is_success

    async def _complete(self, a: Assignment, *, expect_stale: bool) -> Outcome:
        body = output_body(a.record_count)
        req = CompleteRequest(
            record_count=a.record_count,
            byte_size=len(body),
            sha256=hashlib.sha256(body).hexdigest(),
        )
        try:
            await self._call("complete", self._client.complete(a.acquirement_id, req))
        except StaleAcquirementError:
            self.rec.count("late_complete_409" if expect_stale else "complete_409")
            return Outcome.NEUTRAL  # someone else owns it now: nothing to report
        except (ApiError, httpx.HTTPError):
            self.rec.count("complete_failed")
            return Outcome.FAILED
        self.rec.count("completed")
        if expect_stale:
            self.rec.count("late_complete_accepted")  # the watchdog had not reclaimed it yet
        return Outcome.DONE

    async def _release(self, a: Assignment, code: str) -> bool:
        try:
            await self._call(
                "release", self._client.release(a.acquirement_id, ReleaseRequest(code=code))
            )
        except StaleAcquirementError:
            return True
        except (ApiError, httpx.HTTPError):
            return False
        self.rec.count("released")
        return True

    async def _fail(self, a: Assignment, code: str = FAULT_CODE) -> bool:
        req = FailRequest(retryable=True, code=code, message="injected by minion-scale")
        try:
            await self._call("fail", self._client.fail(a.acquirement_id, req))
        except StaleAcquirementError:
            return True
        except (ApiError, httpx.HTTPError):
            return False
        self.rec.count("failed_retryable")
        return False  # the executor backs off after a failed subtask


async def run_sims(
    cfg: SimConfig,
    tokens: list[SecretStr],
    stop: asyncio.Event,
    *,
    first_index: int = 0,
    rec: Recorder | None = None,
) -> Recorder:
    """Run one sim per token until `stop` is set; the recorder includes event-loop lag."""
    rec = rec or Recorder()
    limits = httpx.Limits(
        max_connections=max(10, len(tokens)), max_keepalive_connections=len(tokens)
    )
    async with httpx.AsyncClient(timeout=cfg.timeout_s, limits=limits) as put_http:
        sims = [
            SimExecutor(first_index + i, tok, cfg, rec, stop, put_http)
            for i, tok in enumerate(tokens)
        ]
        lag = asyncio.create_task(monitor_lag(rec, stop))
        try:
            await asyncio.gather(*(s.run() for s in sims))
        finally:
            stop.set()
            await asyncio.gather(lag, return_exceptions=True)
    return rec


def _proc_main(
    cfg: SimConfig,
    tokens: list[str],
    first_index: int,
    stop_flag: Any,
    out: Any,
) -> None:
    async def main() -> dict[str, Any]:
        stop = asyncio.Event()

        async def watch() -> None:
            while not stop.is_set():
                if stop_flag.is_set():
                    stop.set()
                    return
                await asyncio.sleep(0.1)

        watcher = asyncio.create_task(watch())
        try:
            rec = await run_sims(cfg, [SecretStr(t) for t in tokens], stop, first_index=first_index)
        finally:
            watcher.cancel()
        return rec.to_dict()

    crashed_exit = False
    try:
        result = asyncio.run(main())
    except Exception as exc:
        crashed_exit = True
        crashed = Recorder()
        crashed.count("proc_crashed")
        crashed.count(f"proc_crashed_{type(exc).__name__}")  # the type only, never the message
        result = crashed.to_dict()
    out.put(result)
    if crashed_exit:
        sys.exit(3)  # non-zero so run_fleet sees the death at once instead of waiting for the rest


class FleetResult(NamedTuple):
    rec: Recorder
    duration_s: float
    drained: bool  # `until()` became true (False: time ran out, every sim ended, or a crash)


DEFAULT_STALL_S = 60.0


def join_timeout_s(cfg: SimConfig) -> float:
    """How long to wait for the sim processes to report after the stop flag: a call in flight may
    take `timeout_s`, and a sim can be in two (the call, then its release)."""
    return 2 * cfg.timeout_s + 10


async def run_fleet(
    cfg: SimConfig,
    tokens: list[SecretStr],
    *,
    procs: int,
    until: Callable[[], Awaitable[bool]],
    poll_s: float = 1.0,
    max_duration_s: float | None = None,
    progress: Callable[[], Awaitable[int]] | None = None,
    stall_s: float | None = DEFAULT_STALL_S,
) -> FleetResult:
    """Run the sims (spread over `procs` processes) until `until()` is true or time runs out.

    Fails fast instead of hanging: when every sim has ended (all aborted) or a sim process died,
    the run stops with `drained=False`. An exception of the in-process runner (e.g. an insecure
    server URL) is re-raised. With `progress` (the remaining-work count) the run also stops, with
    a `stalled` event, when that count has not changed for `stall_s` seconds of polls (e.g. a
    queue nobody can acquire from); `stall_s` None or 0 disables it. Keep `stall_s` above the
    longest expected subtask (`work_s`, `late_wait_s`).
    Returns the merged recorder, the duration and whether it drained.
    """
    started = time.monotonic()
    stalled = False
    stall_polls = math.ceil(stall_s / poll_s) if progress is not None and stall_s else 0

    async def drained() -> bool:
        nonlocal stalled
        last: int | None = None
        same = 0
        while True:
            if max_duration_s is not None and time.monotonic() - started >= max_duration_s:
                return False
            if await until():
                return True
            if stall_polls and progress is not None:
                now = await progress()
                same = same + 1 if now == last else 0
                last = now
                if same >= stall_polls:
                    stalled = True
                    return False
            await asyncio.sleep(poll_s)

    if procs <= 1:
        stop = asyncio.Event()
        runner = asyncio.create_task(run_sims(cfg, tokens, stop))
        watch = asyncio.create_task(drained())
        try:
            await asyncio.wait({runner, watch}, return_when=asyncio.FIRST_COMPLETED)
            ok = watch.done() and not watch.cancelled() and watch.result()
        finally:
            stop.set()
            watch.cancel()
            await asyncio.gather(watch, return_exceptions=True)
        rec = await runner  # re-raises what the runner died of
        if stalled:
            rec.count("stalled")
        return FleetResult(rec, time.monotonic() - started, ok)

    ctx = mp.get_context("spawn")
    stop_flag = ctx.Event()
    out = ctx.Queue()
    chunks = [tokens[i::procs] for i in range(procs)]
    offsets = [sum(len(c) for c in chunks[:i]) for i in range(procs)]
    workers = [
        ctx.Process(
            target=_proc_main,
            args=(cfg, [t.get_secret_value() for t in chunk], off, stop_flag, out),
        )
        for chunk, off in zip(chunks, offsets, strict=True)
        if chunk
    ]
    for w in workers:
        w.start()
    ok = False
    watch = asyncio.create_task(drained())
    try:
        while not watch.done():
            await asyncio.wait({watch}, timeout=min(poll_s, 0.5))
            if not watch.done() and (
                any(not w.is_alive() and w.exitcode not in (0, None) for w in workers)
                or all(not w.is_alive() for w in workers)
            ):
                break  # a process died, or every sim process ended: nothing left to wait for
        ok = watch.done() and not watch.cancelled() and watch.result()
    finally:
        stop_flag.set()
        watch.cancel()
        await asyncio.gather(watch, return_exceptions=True)
    parts: list[dict[str, Any]] = []
    deadline = time.monotonic() + join_timeout_s(cfg)
    while len(parts) < len(workers) and time.monotonic() < deadline:
        try:
            parts.append(await asyncio.to_thread(out.get, True, 0.5))
        except queue.Empty:
            if not any(w.is_alive() for w in workers):
                try:  # a result may still be in the pipe after the process exited
                    parts.append(await asyncio.to_thread(out.get, True, 1.0))
                except queue.Empty:
                    break
    missing = len(workers) - len(parts)
    for w in workers:
        if w.is_alive():
            await asyncio.to_thread(w.join, 1.0)
        if w.is_alive():
            w.terminate()
        await asyncio.to_thread(w.join)
    rec = merge_dicts(parts)
    if stalled:
        rec.count("stalled")
    if missing:  # a process that never reported (killed, segfault): count it, fail the run
        rec.count("proc_crashed", missing)
        ok = False
    if rec.events.get("proc_crashed"):
        ok = False
    return FleetResult(rec, time.monotonic() - started, ok)
