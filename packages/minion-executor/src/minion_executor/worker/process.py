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
import threading
import time
from collections.abc import Callable, Mapping
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from typing import Any

from minion_common.schemas import Budget, ModelEntry

from minion_executor.runners.base import Record, RunnerError

DEFAULT_MAX_MESSAGE_BYTES = 64 * 1024 * 1024
# Allowlist (not a blocklist): only these names ever reach a worker. `__PYVENV_LAUNCHER__` is the
# macOS framework-launcher variable the interpreter bootstrap may consult for venv detection.
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


def worker_main(
    conn: Connection,
    runner_name: str,
    nice: int = 0,
    env: dict[str, str] | None = None,
    test_hooks: bool = False,
    max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
    parent_pid: int | None = None,
    parent_watch_interval_s: float = 1.0,
) -> None:
    """Child entry point (top level so it is picklable under spawn)."""
    from minion_executor.runners import create_runner

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
    runner = create_runner(runner_name, test_hooks=test_hooks)
    while True:
        try:
            msg = _recv(conn, max_message_bytes)
        except (EOFError, OSError):  # parent gone, or oversize message: pipe is unusable
            break
        op = msg["op"]
        if op == "shutdown":
            runner.unload()
            _send(conn, {"ok": True})
            break
        try:
            result: list[Record] | None = None
            if op == "load":
                runner.load(ModelEntry(**msg["model"]), Budget(**msg["budget"]))
            elif op == "run":
                result = runner.run(msg["batch"])
            elif op == "unload":
                runner.unload()
            else:
                raise RunnerError("bad_op", op)
            _send(conn, {"ok": True, "result": result})
        except RunnerError as e:
            _send(conn, {"ok": False, "code": e.code, "message": e.message})
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
        runner_name: str,
        nice: int = 0,
        *,
        load_timeout_s: float = 900.0,
        run_timeout_s: float = 600.0,
        max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
        test_hooks: bool = False,
        parent_watch_interval_s: float = 1.0,
    ) -> None:
        self._runner_name = runner_name
        self._nice = nice
        self._load_timeout_s = load_timeout_s
        self._run_timeout_s = run_timeout_s
        self._max_message_bytes = max_message_bytes
        self._test_hooks = test_hooks
        self._watch_s = parent_watch_interval_s
        self._ctx = mp.get_context("spawn")
        self._lock = threading.RLock()
        self._closed = False
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
            parent, child = self._ctx.Pipe()
            proc = self._ctx.Process(
                target=worker_main,
                args=(
                    child,
                    self._runner_name,
                    self._nice,
                    scrub_env(os.environ),
                    self._test_hooks,
                    self._max_message_bytes,
                    os.getpid(),
                    self._watch_s,
                ),
                daemon=True,
            )
            proc.start()
            child.close()
            if self._after_spawn_hook is not None:
                self._after_spawn_hook(proc)
            self._proc, self._conn = proc, parent
            if self._closed:  # kill() ran between the check above and the assignment
                self._reset()
                raise WorkerCrashed("worker handle is stopped")
        return self._conn

    def _call(self, msg: dict[str, Any], timeout_s: float) -> dict[str, Any]:
        with self._lock:
            conn = self._ensure_started()
            proc = self._proc
            if proc is None:  # pragma: no cover - _ensure_started guarantees it
                raise WorkerCrashed("worker not started")
            try:
                _send(conn, msg)
                deadline = time.monotonic() + timeout_s
                abortable = msg.get("op") != "shutdown"
                while not conn.poll(0.1):
                    if abortable and self._closed:
                        raise WorkerCrashed("worker handle is stopped")
                    if not proc.is_alive():
                        if conn.poll(0):
                            break
                        raise WorkerCrashed(f"worker exited with code {proc.exitcode}")
                    if time.monotonic() > deadline:
                        raise WorkerCrashed("worker timed out")
                reply = _recv(conn, self._max_message_bytes)
            except (EOFError, BrokenPipeError, ConnectionResetError, OSError) as e:
                code = proc.exitcode
                self._reset()
                raise WorkerCrashed(f"worker pipe closed (exit code {code})") from e
            except WorkerCrashed:
                self._reset()
                raise
            if not reply["ok"]:
                raise RunnerError(reply["code"], reply.get("message", ""))
            return reply

    def ensure_model(self, model: ModelEntry, budget: Budget) -> None:
        """Load `model`; no-op if already warm, kill+restart on a model switch."""
        with self._lock:
            if self._model_id == model.model_id and self._proc and self._proc.is_alive():
                return
            if self._model_id is not None:
                self._reset()
            self._call(
                {
                    "op": "load",
                    "model": model.model_dump(mode="json"),
                    "budget": budget.model_dump(mode="json"),
                },
                self._load_timeout_s,
            )
            self._model_id = model.model_id

    def run(self, batch: list[Record]) -> list[Record]:
        reply = self._call({"op": "run", "batch": batch}, self._run_timeout_s)
        result: list[Record] = reply["result"]
        return result

    def kill(self) -> None:
        """Stop for good: hard-kill the worker (reclaiming model memory), never respawn.

        Lock-free so it can interrupt a call that holds the lock; safe from any thread.
        """
        self._closed = True
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
