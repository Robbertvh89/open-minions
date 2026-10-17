"""Frame-latency probe for the NF-7 foreground proxy (TASK-6.9).

A small child process issues a fixed fp16 matmul on the Apple GPU on an absolute 30 fps schedule
and records, per frame, completion minus *scheduled* start. It stands in for a compositor / video
call sharing the GPU with the inference worker; it is a scheduling-latency proxy, not a measure
of real frame drops. It is not an inference worker (invariant 7): it handles no task data, has
no network, and only floats cross the pipe back. MLX is imported lazily inside the child only.
"""

from __future__ import annotations

import math
import multiprocessing as mp
import struct
import time
from collections.abc import Callable
from dataclasses import dataclass
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from typing import Protocol

from minion_executor.worker.process import restrict_environ

FRAME_PERIOD_S = 1.0 / 30.0
MIN_DURATION_S = 1.0
MAX_DURATION_S = 600.0
MAX_FRAMES = int(MAX_DURATION_S * 30)
KILL_GRACE_S = 30.0
CALIBRATE_TARGET_MS = 5.0
MIN_SIZE = 64
MAX_SIZE = 8192
_F64 = struct.Struct("<d")


class ProbeError(RuntimeError):
    """The probe could not run (no mlx, child died, bad reply)."""


@dataclass(frozen=True)
class ProbeResult:
    latencies_ms: list[float]
    missed: int


class FrameProbe(Protocol):
    def calibrate(self) -> int: ...

    def run(self, duration_s: float, size: int) -> ProbeResult: ...


def check_duration(duration_s: float) -> float:
    if not MIN_DURATION_S <= duration_s <= MAX_DURATION_S:
        raise ValueError(f"duration must be within {MIN_DURATION_S}..{MAX_DURATION_S} s")
    return duration_s


def run_schedule(
    duration_s: float,
    step: Callable[[], None],
    *,
    period_s: float = FRAME_PERIOD_S,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> ProbeResult:
    """Run `step` on an absolute schedule t0 + i * period for `duration_s`.

    Latency = completion - scheduled start (queueing behind other GPU work counts). When a step
    runs past the end of later slots, those slots are skipped and counted as missed; the overrun
    frame itself is counted once, as late."""
    check_duration(duration_s)
    n_slots = min(int(duration_s / period_s), MAX_FRAMES)
    t0 = clock()
    latencies: list[float] = []
    missed = 0
    i = 0
    while i < n_slots:
        scheduled = t0 + i * period_s
        wait = scheduled - clock()
        if wait > 0:
            sleep(wait)
        step()
        end = clock()
        latencies.append((end - scheduled) * 1000.0)
        nxt = i + 1
        while nxt < n_slots and t0 + (nxt + 1) * period_s <= end:
            nxt += 1
            missed += 1
        i = nxt
    return ProbeResult(latencies, missed)


def calibrate_size(
    measure_ms: Callable[[int], float],
    *,
    target_ms: float = CALIBRATE_TARGET_MS,
    lo: int = MIN_SIZE,
    hi: int = MAX_SIZE,
) -> int:
    """Binary-search the matmul edge whose median step time is closest to `target_ms`."""
    best = lo
    best_err = abs(measure_ms(lo) - target_ms)
    while hi - lo > 16:
        mid = (lo + hi) // 2
        t = measure_ms(mid)
        if abs(t - target_ms) < best_err:
            best, best_err = mid, abs(t - target_ms)
        if t < target_ms:
            lo = mid
        else:
            hi = mid
    return best


def _make_step(size: int) -> Callable[[], None]:
    import mlx.core as mx

    a = mx.random.normal((size, size)).astype(mx.float16)
    mx.eval(a)

    def step() -> None:
        mx.eval(a @ a)

    return step


def _median_step_ms(size: int, n: int = 20) -> float:
    step = _make_step(size)
    for _ in range(3):
        step()
    times: list[float] = []
    for _ in range(n):
        t = time.perf_counter()
        step()
        times.append((time.perf_counter() - t) * 1000.0)
    times.sort()
    return times[len(times) // 2]


def _pack(values: list[float]) -> bytes:
    return b"".join(_F64.pack(v) for v in values)


def _child_main(conn: Connection, mode: str, duration_s: float, size: int) -> None:
    restrict_environ()
    try:
        if mode == "calibrate":
            conn.send_bytes(_pack([float(calibrate_size(_median_step_ms))]))
        else:
            res = run_schedule(duration_s, _make_step(size))
            conn.send_bytes(_pack([float(res.missed), *res.latencies_ms]))
    finally:
        conn.close()


def _hang_child_for_tests(conn: Connection, mode: str, duration_s: float, size: int) -> None:
    """Importable stand-in child that never replies (spawn needs a top-level target)."""
    time.sleep(120)


def decode_reply(raw: bytes, *, max_values: int = MAX_FRAMES + 1) -> list[float]:
    if not raw or len(raw) % _F64.size or len(raw) // _F64.size > max_values:
        raise ProbeError("bad probe reply")
    vals = [_F64.unpack_from(raw, o)[0] for o in range(0, len(raw), _F64.size)]
    if not all(math.isfinite(v) and v >= 0.0 for v in vals):
        raise ProbeError("bad probe reply")
    return vals


class MlxFrameProbe:
    """Spawns a fresh child per call; the parent kills it `KILL_GRACE_S` after the deadline."""

    def __init__(self, target: Callable[[Connection, str, float, int], None] = _child_main) -> None:
        self._target = target
        self._last_proc: BaseProcess | None = None

    @property
    def last_proc(self) -> BaseProcess | None:
        return self._last_proc

    def _call(self, mode: str, duration_s: float, size: int, timeout_s: float) -> list[float]:
        ctx = mp.get_context("spawn")
        parent, child = ctx.Pipe(duplex=False)
        proc = ctx.Process(target=self._target, args=(child, mode, duration_s, size), daemon=True)
        self._last_proc = proc
        proc.start()
        child.close()
        try:
            if not parent.poll(timeout_s):
                raise ProbeError("probe timed out")
            try:
                raw = parent.recv_bytes(8 * (MAX_FRAMES + 1))
            except (EOFError, OSError) as exc:
                raise ProbeError("probe child died") from exc
            proc.join(5.0)
            return decode_reply(raw)
        finally:
            if proc.is_alive():
                proc.kill()
            proc.join(5.0)
            parent.close()

    def calibrate(self) -> int:
        vals = self._call("calibrate", MIN_DURATION_S, MIN_SIZE, 120.0)
        if len(vals) != 1:
            raise ProbeError("bad calibrate reply")
        return max(MIN_SIZE, min(MAX_SIZE, int(vals[0])))

    def run(self, duration_s: float, size: int) -> ProbeResult:
        check_duration(duration_s)
        if not MIN_SIZE <= size <= MAX_SIZE:
            raise ValueError("size out of range")
        vals = self._call("run", duration_s, size, duration_s + KILL_GRACE_S)
        missed = vals[0]
        if missed < 0 or missed != int(missed):
            raise ProbeError("bad probe reply")
        return ProbeResult(vals[1:], int(missed))
