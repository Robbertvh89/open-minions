"""Worker subprocess hosting the Runner (invariant 7).

The control process talks to it over a `multiprocessing.Pipe` using length-framed
messages (`send_bytes`/`recv_bytes`, bounded by `max_message_bytes`) carrying JSON. The child is
started with the `spawn` context so no Metal/GPU state is inherited via fork, receives an
allowlisted environment (no tokens/secrets), and exits on its own if the control process dies.
"""

from __future__ import annotations

import contextlib
import json
import multiprocessing as mp
import os
import resource
import sys
import threading
import time
from collections.abc import Callable, Collection, Mapping
from dataclasses import asdict
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from pathlib import Path
from typing import Any

from minion_common.enums import Engine
from minion_common.schemas import Budget, ModelEntry, TaskParams, parse_params

from minion_executor.runners.base import (
    ENGINE_UNAVAILABLE,
    EngineConfig,
    Record,
    Runner,
    RunnerError,
)
from minion_executor.worker.throttle import DutyCyclePacer

DEFAULT_MAX_MESSAGE_BYTES = 64 * 1024 * 1024
# Allowlist (not a blocklist): only these names ever reach a worker. `__PYVENV_LAUNCHER__` is the
# macOS framework-launcher variable the interpreter bootstrap may consult for venv detection.
# TODO(TASK-4.2): proxy/CA/HF_HUB_* handling for real engines (credential-bearing proxy URLs
# only ever reach the fetch subprocess, never the worker).
_ALLOWED_NAMES = frozenset(
    {
        "PATH",
        "HOME",
        "TMPDIR",
        "LANG",
        "HF_HOME",
        "TOKENIZERS_PARALLELISM",
        "MINION_EXECUTOR_DEV",
        "MINION_EXECUTOR_TEST_HOOKS",
        "__PYVENV_LAUNCHER__",
    }
)
_ALLOWED_PREFIXES = ("LC_", "MLX_")


class WorkerCrashed(Exception):  # noqa: N818 - name fixed by plan/API
    """The worker process died or its pipe closed unexpectedly."""


def env_allowed(name: str) -> bool:
    return name in _ALLOWED_NAMES or name.startswith(_ALLOWED_PREFIXES)


def scrub_env(env: Mapping[str, str]) -> dict[str, str]:
    """Copy of `env` keeping only allowlisted names (so unknown secrets never leak by default)."""
    return {k: v for k, v in env.items() if env_allowed(k)}


def restrict_environ() -> None:
    """Reduce this process's own `os.environ` to the worker allowlist.

    The `spawn` start method hands the parent's environment to the child before `worker_main`
    runs, so the control process must not hold secrets (HF_TOKEN, AWS_*, DATABASE_URL, ...) in
    its environ when it spawns. Call after everything that reads env at construction time
    (httpx proxy / SSL_CERT_* settings are read when the client is built) and before any spawn.
    """
    for name in [k for k in os.environ if not env_allowed(k)]:
        del os.environ[name]


def _send(conn: Connection, msg: dict[str, Any]) -> None:
    conn.send_bytes(json.dumps(msg).encode("utf-8"))


def _recv(conn: Connection, maxlength: int) -> dict[str, Any]:
    msg: dict[str, Any] = json.loads(conn.recv_bytes(maxlength).decode("utf-8"))
    return msg


def _watch_parent(parent_pid: int, interval_s: float) -> None:
    """Hard-exit if the control process disappears (even while a long `run` is in progress)."""
    while True:
        time.sleep(interval_s)
        if os.getppid() != parent_pid:
            os._exit(1)


class _Ctl:
    """`StepControl` inside the worker: static duty-cycle pacing plus rate-limited ticks."""

    def __init__(self, conn: Connection, duty: float, tick_interval_s: float) -> None:
        self._conn = conn
        self._tick_s = tick_interval_s
        self._last_tick = 0.0
        self._done: int | None = None  # last records_done seen; re-sent on pacer ticks
        self._pacer = DutyCyclePacer(duty, time.monotonic, self._sleep)

    def start(self) -> None:
        self._pacer.reset()

    def _tick(self, records_done: int | None) -> None:
        now = time.monotonic()
        if now - self._last_tick >= self._tick_s:
            self._last_tick = now
            _send(self._conn, {"tick": records_done})

    def _sleep(self, seconds: float) -> None:
        """Pace sleep in tick-sized chunks so a long pause never looks like a hang."""
        end = time.monotonic() + seconds
        while (left := end - time.monotonic()) > 0:
            time.sleep(min(left, self._tick_s))
            self._tick(self._done)

    def checkpoint(self, records_done: int | None = None) -> None:
        if records_done is not None:
            self._done = records_done
        self._tick(self._done)  # before pacing: the pacer's own ticks share the rate limit
        self._pacer.step_done()


def _maxrss_bytes() -> int:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss if sys.platform == "darwin" else rss * 1024  # macOS: bytes, Linux: KiB


def worker_main(
    conn: Connection,
    engines: list[str],
    nice: int = 0,
    env: dict[str, str] | None = None,
    test_hooks: bool = False,
    max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
    parent_pid: int | None = None,
    parent_watch_interval_s: float = 1.0,
    tick_interval_s: float = 1.0,
) -> None:
    """Child entry point (top level so it is picklable under spawn). The runner is created at
    `load` from the model's engine, which must be in `engines`."""
    from minion_executor.runners import create_runner, probe_engines

    if env is not None:  # explicit, scrubbed environment replaces whatever was inherited
        os.environ.clear()
        os.environ.update(env)
    if parent_pid is not None:
        threading.Thread(
            target=_watch_parent, args=(parent_pid, parent_watch_interval_s), daemon=True
        ).start()
    if nice:
        with contextlib.suppress(OSError):
            os.nice(nice)
    allowed = {Engine(e) for e in engines}
    runner: Runner | None = None
    model: ModelEntry | None = None
    budget: Budget | None = None
    while True:
        try:
            msg = _recv(conn, max_message_bytes)
        except (EOFError, OSError):  # parent gone, or oversize message: pipe is unusable
            break
        op = msg["op"]
        if op == "shutdown":
            if runner is not None:
                runner.unload()
            _send(conn, {"ok": True})
            break
        try:
            result: Any = None
            if op == "load":
                if runner is not None:
                    runner.unload()
                    runner = None
                model = ModelEntry(**msg["model"])
                budget = Budget(**msg["budget"])
                cfg = EngineConfig(**msg.get("cfg", {}))
                if model.engine not in allowed:
                    raise RunnerError("ENGINE_NOT_ALLOWED", model.engine.value)
                path = Path(msg["model_path"]) if msg.get("model_path") else None
                try:
                    new_runner = create_runner(model.engine, model.type, test_hooks=test_hooks)
                    new_runner.load(model, path, budget, cfg)
                except ImportError:  # extra not installed in this venv
                    raise RunnerError(ENGINE_UNAVAILABLE, model.engine.value) from None
                runner = new_runner
            elif op == "run":
                if runner is None or model is None or budget is None:
                    raise RunnerError("not_loaded")
                params: TaskParams = parse_params(model.type, msg.get("params") or {})
                ctl = _Ctl(conn, budget.gpu_duty_cycle, tick_interval_s)
                ctl.start()
                result = runner.run(msg["batch"], params, ctl)
            elif op == "stats":
                mem = runner.memory_in_use() if runner is not None else None
                result = {
                    "active_bytes": mem["active_bytes"] if mem else 0,
                    "peak_bytes": mem["peak_bytes"] if mem else 0,
                    "max_rss_bytes": _maxrss_bytes(),
                }
            elif op == "probe":
                result = probe_engines(allowed, EngineConfig(**msg.get("cfg", {})))
            else:
                raise RunnerError("bad_op", op)
            _send(conn, {"ok": True, "result": result})
        except RunnerError as e:
            _send(
                conn,
                {"ok": False, "code": e.code, "message": e.message, "retryable": e.retryable},
            )
        except Exception as e:
            _send(conn, {"ok": False, "code": "runner_error", "message": type(e).__name__})
    conn.close()


class WorkerHandle:
    """Control-side handle. Keeps the model warm across same-model subtasks.

    Thread-safety: calls are serialised by a lock. `kill()` deliberately does NOT take the lock
    (the loop calls it from another thread while a call is in flight): it marks the handle closed
    and kills the process; the in-flight call then fails with `WorkerCrashed` and cleans up, and
    no new worker is ever spawned afterwards.
    """

    def __init__(
        self,
        engines: Collection[Engine],
        nice: int = 0,
        *,
        load_timeout_s: float = 1800.0,
        step_timeout_s: float = 300.0,
        max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
        test_hooks: bool = False,
        parent_watch_interval_s: float = 1.0,
        tick_interval_s: float = 1.0,
        engine_config: EngineConfig | None = None,
    ) -> None:
        self._engines = sorted(e.value for e in engines)
        self._nice = nice
        self._load_timeout_s = load_timeout_s
        self._step_timeout_s = step_timeout_s
        self._tick_interval_s = tick_interval_s
        self._cfg = engine_config or EngineConfig()
        self._max_message_bytes = max_message_bytes
        self._test_hooks = test_hooks
        self._watch_s = parent_watch_interval_s
        self._ctx = mp.get_context("spawn")
        self._lock = threading.RLock()
        self._closed = False
        self._seen_gen = 0  # last generation ensure_model() reset for
        self._interrupt_gen = 0  # bumped by interrupt(); a change means 'abort what you started'
        self._proc: BaseProcess | None = None
        self._conn: Connection | None = None
        self._model_id: str | None = None
        self._after_spawn_hook: Callable[[BaseProcess], None] | None = None  # test seam

    @property
    def pid(self) -> int | None:
        proc = self._proc
        return proc.pid if proc else None

    @property
    def loaded_model_id(self) -> str | None:
        return self._model_id

    def _ensure_started(self) -> Connection:
        if self._proc is None or self._conn is None:
            if self._closed:
                raise WorkerCrashed("worker handle is stopped")
            gen = self._seen_gen
            parent, child = self._ctx.Pipe()
            proc = self._ctx.Process(
                target=worker_main,
                args=(
                    child,
                    self._engines,
                    self._nice,
                    scrub_env(os.environ),
                    self._test_hooks,
                    self._max_message_bytes,
                    os.getpid(),
                    self._watch_s,
                    self._tick_interval_s,
                ),
                daemon=True,
            )
            proc.start()
            child.close()
            if self._after_spawn_hook is not None:
                self._after_spawn_hook(proc)
            self._proc, self._conn = proc, parent
            if self._closed or self._interrupt_gen != gen:  # kill()/interrupt() raced the spawn
                self._reset()
                raise WorkerCrashed("worker handle is stopped")
        return self._conn

    def _call(
        self,
        msg: dict[str, Any],
        timeout_s: float,
        on_progress: Callable[[int], None] | None = None,
    ) -> dict[str, Any]:
        """Send `msg`, wait for the reply. `timeout_s` is an inactivity limit: every tick from
        the worker resets it, and ticks are forwarded to `on_progress`."""
        with self._lock:
            gen = self._seen_gen
            conn = self._ensure_started()
            proc = self._proc
            if proc is None:  # pragma: no cover - _ensure_started guarantees it
                raise WorkerCrashed("worker not started")
            try:
                _send(conn, msg)
                deadline = time.monotonic() + timeout_s
                abortable = msg.get("op") != "shutdown"
                while True:
                    if not conn.poll(0.1):
                        if abortable and (self._closed or self._interrupt_gen != gen):
                            raise WorkerCrashed("worker handle is stopped")
                        if not proc.is_alive() and not conn.poll(0):
                            raise WorkerCrashed(f"worker exited with code {proc.exitcode}")
                        if time.monotonic() > deadline:
                            raise WorkerCrashed("worker timed out")
                        continue
                    reply = _recv(conn, self._max_message_bytes)
                    if "tick" not in reply:
                        break
                    deadline = time.monotonic() + timeout_s  # liveness: restart the clock
                    done = reply["tick"]
                    if on_progress is not None and isinstance(done, int):
                        with contextlib.suppress(Exception):  # a bad callback must not kill a run
                            on_progress(done)
            except (EOFError, BrokenPipeError, ConnectionResetError, OSError) as e:
                code = proc.exitcode
                self._reset()
                raise WorkerCrashed(f"worker pipe closed (exit code {code})") from e
            except WorkerCrashed:
                self._reset()
                raise
            if not reply["ok"]:
                raise RunnerError(
                    reply["code"], reply.get("message", ""), retryable=reply.get("retryable", True)
                )
            return reply

    def ensure_model(
        self, model: ModelEntry, budget: Budget, model_path: Path | None = None
    ) -> None:
        """Load `model`; no-op if already warm, kill+restart on a model switch."""
        with self._lock:
            g = self._interrupt_gen  # read once: an interrupt landing now must not be swallowed
            if g != self._seen_gen:
                self._seen_gen = g  # re-arm: later interrupts abort this call
                self._reset()
            if self._model_id == model.model_id and self._proc and self._proc.is_alive():
                return
            if self._model_id is not None:
                self._reset()
            self._call(
                {
                    "op": "load",
                    "model": model.model_dump(mode="json"),
                    "budget": budget.model_dump(mode="json"),
                    "model_path": str(model_path) if model_path else None,
                    "cfg": asdict(self._cfg),
                },
                self._load_timeout_s,
            )
            self._model_id = model.model_id

    def run(
        self,
        batch: list[Record],
        params: TaskParams | None = None,
        on_progress: Callable[[int], None] | None = None,
    ) -> list[Record]:
        """Run `batch` on the loaded model. `params=None` means the type's defaults."""
        msg = {
            "op": "run",
            "batch": batch,
            "params": params.model_dump(mode="json") if params is not None else {},
        }
        reply = self._call(msg, self._step_timeout_s, on_progress)
        result: list[Record] = reply["result"]
        return result

    def stats(self) -> dict[str, int]:
        """Worker memory: runner `active_bytes`/`peak_bytes` plus process `max_rss_bytes`."""
        stats: dict[str, int] = self._call({"op": "stats"}, self._step_timeout_s)["result"]
        return stats

    def probe(self, timeout_s: float = 90.0) -> dict[str, str | None]:
        """Spawn a worker, report `{engine: version | None}` for this handle's engines (imports
        happen in the child only), then shut it down. Do not call with a model loaded."""
        with self._lock:
            try:
                reply = self._call({"op": "probe", "cfg": asdict(self._cfg)}, timeout_s)
                result: dict[str, str | None] = reply["result"]
                return result
            finally:
                self._reset()

    def unload(self) -> None:
        """Drop the model and free its memory by resetting the worker; the handle stays usable."""
        with self._lock:
            self._reset()

    def kill(self) -> None:
        """Stop for good: hard-kill the worker (reclaiming model memory), never respawn.

        Lock-free so it can interrupt a call that holds the lock; safe from any thread.
        """
        self._closed = True
        proc = self._proc
        if proc is not None:
            with contextlib.suppress(OSError, ValueError, AttributeError):
                proc.kill()

    def interrupt(self) -> None:
        """Abort the current work: hard-kill the worker but allow a respawn on the next call.

        Unlike `kill()` this leaves the handle open. An in-flight call fails with `WorkerCrashed`
        and resets; a dead idle worker is replaced by `ensure_model` (which reloads the model).
        Lock-free, safe from any thread.
        """
        self._interrupt_gen += 1  # kill() is async: don't trust is_alive() right after it
        proc = self._proc
        if proc is not None:
            with contextlib.suppress(OSError, ValueError, AttributeError):
                proc.kill()

    def _reset(self) -> None:
        """Kill (if alive) and forget the worker. Callers hold the lock."""
        proc, conn = self._proc, self._conn
        self._proc = self._conn = None
        self._model_id = None
        if proc is not None:
            with contextlib.suppress(OSError, ValueError):
                if proc.is_alive():
                    proc.kill()
        if conn is not None:
            conn.close()
        if proc is not None:
            proc.join(timeout=5)

    def close(self) -> None:
        self._closed = True
        with self._lock:
            if self._proc is not None and self._proc.is_alive():
                with contextlib.suppress(WorkerCrashed, RunnerError):
                    self._call({"op": "shutdown"}, 10.0)
            self._reset()
