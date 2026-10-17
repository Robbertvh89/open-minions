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
import shutil
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Collection, Mapping
from dataclasses import asdict, replace
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from minion_common.enums import CpuPriority, Engine
from minion_common.schemas import Budget, ModelEntry, TaskParams, parse_params

from minion_executor.capacity.priority import apply_cpu_priority, current_priority, satisfies
from minion_executor.capacity.profiles import nice_for
from minion_executor.runners.base import (
    ENGINE_UNAVAILABLE,
    MODEL_INTEGRITY,
    EngineConfig,
    Record,
    Runner,
    RunnerError,
)
from minion_executor.worker import sandbox as sbx
from minion_executor.worker.control import (
    WorkerAck,
    WorkerCtl,
    WorkerUsage,
    parse_ack,
    parse_usage,
)
from minion_executor.worker.footprint import process_footprint_bytes
from minion_executor.worker.sandbox import SandboxPolicy
from minion_executor.worker.throttle import DutyCyclePacer

DEFAULT_MAX_MESSAGE_BYTES = 64 * 1024 * 1024
CTL_SEND_TIMEOUT_S = 1.0  # bounds BOTH the wait for the pipe write lock and the write itself
UNLOAD_TIMEOUT_S = 5.0  # total wall-clock cap on the graceful unload (> runner keep_alive POST)
# Allowlist (not a blocklist): only these names ever reach a worker. `__PYVENV_LAUNCHER__` is the
# macOS framework-launcher variable the interpreter bootstrap may consult for venv detection.
# Proxy, CA and HF_TOKEN are deliberately not on it: they go only to the model-fetch subprocess
# (`model_cache.FetchEnv`). The worker is offline and loads from the local cache.
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
# Forced into every worker regardless of the parent's environment (D7).
FORCED_WORKER_ENV: Mapping[str, str] = {
    "HF_HUB_OFFLINE": "1",
    "TRANSFORMERS_OFFLINE": "1",
    "HF_DATASETS_OFFLINE": "1",
    "HF_HUB_DISABLE_TELEMETRY": "1",
}


class _SelfTestFailedError(Exception):
    """The worker exited with `SELF_TEST_EXIT_CODE`: its sandbox did not confine it."""


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


def _self_test_error() -> RunnerError:
    """Retryable (another Mac may sandbox fine) and released by the loop: this Mac cannot."""
    return RunnerError(sbx.SANDBOX_FAILED, "sandbox self-test failed", retryable=True)


def _send(conn: Connection, msg: dict[str, Any]) -> None:
    conn.send_bytes(json.dumps(msg).encode("utf-8"))


def _recv(conn: Connection, maxlength: int) -> dict[str, Any]:
    msg: dict[str, Any] = json.loads(conn.recv_bytes(maxlength).decode("utf-8"))
    return msg


def _kill_quietly(proc: BaseProcess) -> None:
    with contextlib.suppress(OSError, ValueError, AttributeError):
        proc.kill()


def _watch_parent(parent_pid: int, interval_s: float) -> None:
    """Hard-exit if the control process disappears (even while a long `run` is in progress)."""
    while True:
        time.sleep(interval_s)
        if os.getppid() != parent_pid:
            os._exit(1)


class _ParentGone(Exception):  # noqa: N818 - internal control flow
    """The control process closed the pipe (or sent something unreadable) during a run."""


class _Ctl:
    """`StepControl` inside the worker: duty-cycle pacing, rate-limited ticks, and the live-control
    channel. Control messages (`WorkerCtl`) are read at every checkpoint and during pacer sleeps
    (so a pause or a new duty acts at once); while paused the worker blocks here and keeps ticking
    so the control side's inactivity timeout never fires. Each applied message is acked.

    The duty actually paced is `min(ctl.duty, budget duty)`: a live duty can only be stricter than
    the budget in force (invariant 6), never looser."""

    def __init__(
        self,
        conn: Connection,
        ctl: WorkerCtl,
        tick_interval_s: float,
        max_message_bytes: int,
        on_trim: Callable[[], None],
        duty_cap: float = 1.0,
    ) -> None:
        self._conn = conn
        self._cap = duty_cap  # the budget's duty: the governor may lower it, never raise it
        self._tick_s = tick_interval_s
        self._max = max_message_bytes
        self._on_trim = on_trim
        self._last_tick = 0.0
        self._done: int | None = None  # last records_done seen; re-sent on pacer ticks
        self.ctl = ctl
        self._pacer = DutyCyclePacer(min(ctl.duty, duty_cap), time.monotonic, self._wait)

    @property
    def paced(self) -> bool:
        """The pacer will sleep at checkpoints: runners must drain async GPU work first."""
        return self._pacer.duty < 1.0

    def start(self) -> None:
        """Ack the starting state; a run that begins paused does no work until resumed."""
        self._pacer.reset()
        self._send_tick(ack=True)
        self._last_tick = 0.0  # progress ticks resume at the first checkpoint
        self._block_while_paused()
        self._pacer.reset()

    def finish(self) -> None:
        """Final usage totals, so the control side can read the run's GPU share."""
        self._send_tick()

    def _send_tick(self, *, ack: bool = False) -> None:
        msg: dict[str, Any] = {
            "tick": self._done,
            "busy_s": round(self._pacer.busy_s, 6),
            "wall_s": round(self._pacer.wall_s, 6),
        }
        if ack:
            msg.update(ack=self.ctl.gen, paused=self.ctl.paused)
        self._last_tick = time.monotonic()
        _send(self._conn, msg)

    def _tick(self) -> None:
        if time.monotonic() - self._last_tick >= self._tick_s:
            self._send_tick()

    def _read_ctl(self, timeout: float) -> bool:
        """Wait up to `timeout` for one message and apply it. True if a message was consumed."""
        try:
            if not self._conn.poll(max(0.0, timeout)):
                return False
            msg = _recv(self._conn, self._max)
        except (EOFError, OSError):
            raise _ParentGone from None
        except ValueError:  # undecodable frame: drop it
            return True
        self._apply(msg)
        return True

    def _apply(self, msg: object) -> None:
        if not isinstance(msg, dict) or msg.get("op") != "ctl":
            return
        try:
            new = WorkerCtl.from_msg(msg)
        except ValueError:
            return
        if new.gen <= self.ctl.gen:  # latest wins; stale or duplicate
            return
        self.ctl = new
        self._pacer.set_duty(min(new.duty, self._cap))
        if new.trim:
            self._on_trim()
        self._send_tick(ack=True)

    def _block_while_paused(self) -> None:
        while self.ctl.paused:
            t0 = time.monotonic()
            self._read_ctl(self._tick_s)
            self._pacer.exclude(time.monotonic() - t0)
            self._tick()

    def _wait(self, seconds: float) -> bool:
        """Pacer sleep that a control message cuts short (True: re-derive what is still owed)."""
        end = time.monotonic() + seconds
        while (left := end - time.monotonic()) > 0:
            if self._read_ctl(min(left, self._tick_s)):
                self._block_while_paused()
                return True
            self._tick()
        return False

    def checkpoint(self, records_done: int | None = None) -> None:
        if records_done is not None:
            self._done = records_done
        while self._read_ctl(0):
            pass
        self._block_while_paused()
        self._tick()  # before pacing: the pacer's own ticks share the rate limit
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
    cur_ctl: WorkerCtl | None = None  # newest live-control state seen (latest generation wins)
    sandboxed_reads: list[str] | None = None  # set once the sandbox is on (irreversible)
    trim_errors = 0  # runner.trim() failures: counted, never fatal, never carry a message

    def safe_trim() -> None:
        nonlocal trim_errors
        if runner is None:
            return
        try:
            runner.trim()
        except Exception:
            trim_errors += 1

    def confine(spec: dict[str, Any] | None, model_path: Path | None) -> None:
        """Apply the sandbox before any engine import / model load (fail closed)."""
        nonlocal sandboxed_reads
        if sandboxed_reads is None:
            if spec is None:
                return
            sbx.apply_from_spec(spec)
            sandboxed_reads = [os.path.realpath(p) for p in spec.get("read", [])]
        if model_path is not None:  # a later load may not reach outside what was granted
            real = os.path.realpath(model_path)
            if not any(real == r or real.startswith(r.rstrip("/") + "/") for r in sandboxed_reads):
                raise RunnerError(sbx.SANDBOX_FAILED, "model path outside the sandbox grant")

    while True:
        try:
            msg = _recv(conn, max_message_bytes)
        except (EOFError, OSError):  # parent gone, or oversize message: pipe is unusable
            break
        op = msg["op"]
        if op == "ctl":
            # Live control outside a run: apply silently. A reply here would be read by the next
            # `_call` as its own and desync the protocol.
            try:
                new_ctl = WorkerCtl.from_msg(msg)
            except ValueError:
                continue
            if cur_ctl is None or new_ctl.gen > cur_ctl.gen:
                cur_ctl = new_ctl
                if new_ctl.trim:
                    safe_trim()
            continue
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
                apply_cpu_priority(budget.cpu_priority)  # before the sandbox (irreversible)
                cfg = EngineConfig(**msg.get("cfg", {}))
                if model.engine not in allowed:
                    raise RunnerError("ENGINE_NOT_ALLOWED", model.engine.value)
                path = Path(msg["model_path"]) if msg.get("model_path") else None
                confine(msg.get("sandbox"), path)
                try:
                    new_runner = create_runner(model.engine, model.type, test_hooks=test_hooks)
                    new_runner.load(model, path, budget, cfg)
                except ImportError:  # extra not installed in this venv
                    raise RunnerError(ENGINE_UNAVAILABLE, model.engine.value) from None
                runner = new_runner
            elif op == "run":
                if runner is None or model is None or budget is None:
                    raise RunnerError("not_loaded")
                if msg.get("budget") is not None:  # the budget in force for this batch (FR-54)
                    run_budget = Budget(**msg["budget"])
                    if run_budget != budget:
                        runner.set_budget(run_budget)
                        apply_cpu_priority(run_budget.cpu_priority)
                        budget = run_budget
                params: TaskParams = parse_params(model.type, msg.get("params") or {})
                if msg.get("ctl") is not None:
                    sent = WorkerCtl.from_msg(msg["ctl"])
                    if sent.trim:  # a trim whose own send failed rides on the run (idempotent)
                        safe_trim()
                    if cur_ctl is None or sent.gen > cur_ctl.gen:
                        cur_ctl = sent
                start = cur_ctl or WorkerCtl(gen=0, duty=budget.gpu_duty_cycle, paused=False)
                ctl = _Ctl(
                    conn,
                    start,
                    tick_interval_s,
                    max_message_bytes,
                    safe_trim,
                    budget.gpu_duty_cycle,
                )
                try:
                    ctl.start()
                    result = runner.run(msg["batch"], params, ctl)
                    ctl.finish()
                finally:
                    if ctl.ctl.gen > 0:  # gen 0 is the budget default, not a governor decision
                        cur_ctl = ctl.ctl
            elif op == "unload":  # graceful: let the runner free what lives outside this process
                if runner is not None:
                    runner.unload()
                    runner = None
            elif op == "stats":
                mem = runner.memory_in_use() if runner is not None else None
                result = {
                    "active_bytes": mem["active_bytes"] if mem else 0,
                    "peak_bytes": mem["peak_bytes"] if mem else 0,
                    "max_rss_bytes": _maxrss_bytes(),
                    "trim_errors": trim_errors,
                }
            elif op == "probe":
                confine(msg.get("sandbox"), None)
                result = probe_engines(allowed, EngineConfig(**msg.get("cfg", {})))
            else:
                raise RunnerError("bad_op", op)
            _send(conn, {"ok": True, "result": result})
        except _ParentGone:
            break
        except RunnerError as e:
            _send(
                conn,
                {
                    "ok": False,
                    "code": e.code,
                    "message": e.message,
                    "retryable": e.retryable,
                    "release": e.release,
                },
            )
        except Exception as e:
            _send(
                conn,
                {
                    "ok": False,
                    "code": "runner_error",
                    "message": type(e).__name__,
                    "retryable": True,
                },
            )
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
        sandbox: SandboxPolicy | None = None,
    ) -> None:
        self._sandbox = sandbox  # None: unsandboxed (dev/tests); main passes it unless dev opt-out
        self._tmp_dir: Path | None = None  # per-spawn writable dir, the only one the sandbox allows
        self._engines = sorted(e.value for e in engines)
        self._nice = nice  # spawn niceness until a budget names a priority
        self._cpu_priority: CpuPriority | None = None  # of the latest budget seen by this handle
        self._prio_seen = False
        self._load_timeout_s = load_timeout_s
        self._step_timeout_s = step_timeout_s
        self._tick_interval_s = tick_interval_s
        self._cfg = engine_config or EngineConfig()
        self._max_message_bytes = max_message_bytes
        self._test_hooks = test_hooks
        self._watch_s = parent_watch_interval_s
        self._ctx = mp.get_context("spawn")
        self._lock = threading.RLock()
        self._send_lock = threading.Lock()  # one writer at a time on the pipe (control + call)
        self._ctl_lock = threading.Lock()
        self._ctl_gen = 0  # last live-control generation issued
        self._ctl: WorkerCtl | None = None  # latest-wins state, re-sent with every run
        self._wire_gen = 0  # newest generation handed to the pipe (or carried by a run): ack bound
        self._sent_gen = 0  # newest generation known to be written to the current worker's pipe
        self._unsent: WorkerCtl | None = None  # newest state whose send failed (+ trim)
        self._op: str | None = None  # op of the call in flight (None: worker idle in its main loop)
        self._ack: WorkerAck | None = None
        self._usage: WorkerUsage | None = None
        self._closed = False
        self._seen_gen = 0  # last generation ensure_model() reset for
        self._interrupt_gen = 0  # bumped by interrupt(); a change means 'abort what you started'
        self._proc: BaseProcess | None = None
        self._conn: Connection | None = None
        self._model_id: str | None = None
        self._ollama_ref: str | None = None  # set while an Ollama model may be resident
        self._engine_env: dict[str, str] = {}  # per-engine env for the next spawn
        self._after_spawn_hook: Callable[[BaseProcess], None] | None = None  # test seam

    @property
    def pid(self) -> int | None:
        proc = self._proc
        return proc.pid if proc else None

    @property
    def loaded_model_id(self) -> str | None:
        return self._model_id

    def _note_budget(self, budget: Budget) -> None:
        """Remember the priority of the budget in force (drives nice / OMP at the next spawn and
        the control-side `priority_ok` check)."""
        self._cpu_priority, self._prio_seen = budget.cpu_priority, True

    def _spawn_nice(self) -> int:
        """Niceness for the next worker: that of the latest budget seen, else the constructor's."""
        return nice_for(self._cpu_priority) if self._prio_seen else self._nice

    def _ensure_started(self) -> Connection:
        if self._proc is None or self._conn is None:
            if self._closed:
                raise WorkerCrashed("worker handle is stopped")
            gen = self._seen_gen
            env = {**scrub_env(os.environ), **FORCED_WORKER_ENV, **self._engine_env}
            nice = self._spawn_nice()
            if nice > 0:  # low/background: a throttled worker must not fan out BLAS/OpenMP threads
                env["OMP_NUM_THREADS"] = "2"
            if self._sandbox is not None:
                # `minion-exec-<pid>-` so `cleanup_stale_tmp` sweeps it if the control dies
                prefix = f"minion-exec-{os.getpid()}-worker-"
                self._tmp_dir = Path(os.path.realpath(tempfile.mkdtemp(prefix=prefix)))
                env["TMPDIR"] = str(self._tmp_dir)
            parent, child = self._ctx.Pipe()
            proc = self._ctx.Process(
                target=worker_main,
                args=(
                    child,
                    self._engines,
                    nice,
                    env,
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
        *,
        hard_deadline: float | None = None,
    ) -> dict[str, Any]:
        """Send `msg`, wait for the reply. `timeout_s` is an inactivity limit: every tick from
        the worker resets it, and ticks are forwarded to `on_progress`. `hard_deadline` (absolute
        `time.monotonic()`) is a total cap that ticks cannot extend."""
        with self._lock:
            gen = self._seen_gen
            conn = self._ensure_started()
            proc = self._proc
            if proc is None:  # pragma: no cover - _ensure_started guarantees it
                raise WorkerCrashed("worker not started")
            op = msg.get("op")
            self._op = op if isinstance(op, str) else ""
            try:
                with self._send_lock:  # serialises every _wire_gen writer with _send_ctl
                    if op == "run" and isinstance(msg.get("ctl"), dict):
                        self._wire_gen = max(self._wire_gen, int(msg["ctl"].get("gen", 0)))
                    _send(conn, msg)
                deadline = time.monotonic() + timeout_s
                abortable = msg.get("op") != "shutdown"
                while True:
                    if self._unsent is not None and abortable and op == "run":
                        self._flush_unsent(conn, proc)
                    if not conn.poll(0.1):
                        if abortable and (self._closed or self._interrupt_gen != gen):
                            raise WorkerCrashed("worker handle is stopped")
                        if not proc.is_alive() and not conn.poll(0):
                            if proc.exitcode == sbx.SELF_TEST_EXIT_CODE:
                                raise _SelfTestFailedError
                            raise WorkerCrashed(f"worker exited with code {proc.exitcode}")
                        if time.monotonic() > deadline:
                            raise WorkerCrashed("worker timed out")
                        if hard_deadline is not None and time.monotonic() > hard_deadline:
                            raise WorkerCrashed("worker exceeded its deadline")
                        continue
                    # `poll` only says a byte arrived; `recv_bytes` then blocks for the whole
                    # frame. A watchdog kills the worker at the deadline so a partial or dripped
                    # frame ends in EOF (-> WorkerCrashed) instead of hanging this thread.
                    limit = deadline if hard_deadline is None else min(deadline, hard_deadline)
                    watchdog = threading.Timer(
                        max(0.0, limit - time.monotonic()), _kill_quietly, (proc,)
                    )
                    watchdog.daemon = True
                    watchdog.start()
                    try:
                        reply = _recv(conn, self._max_message_bytes)
                    finally:
                        watchdog.cancel()
                    if hard_deadline is not None and time.monotonic() > hard_deadline:
                        raise WorkerCrashed("worker exceeded its deadline")
                    if not isinstance(reply, dict):
                        raise WorkerCrashed("malformed worker reply")
                    if "tick" not in reply:
                        break
                    if op != "run":  # ticks only exist inside a run: anything else is a lie
                        raise WorkerCrashed("unexpected tick outside a run")
                    try:  # new tick fields are validated strictly (a lie is a protocol violation)
                        usage = parse_usage(reply)
                        ack = parse_ack(reply, issued=self._wire_gen, at=time.monotonic())
                    except ValueError:
                        raise WorkerCrashed("malformed worker reply") from None
                    deadline = time.monotonic() + timeout_s  # liveness: restart the clock
                    done = reply["tick"]
                    if on_progress is not None and isinstance(done, int):
                        with contextlib.suppress(Exception):  # a bad callback must not kill a run
                            on_progress(done)
                    # after the callback: whoever sees this ack also sees the progress before it
                    if usage is not None:
                        self._usage = usage
                    if ack is not None:
                        self._ack = ack
            except _SelfTestFailedError:
                self._reset()
                raise _self_test_error() from None
            except (EOFError, BrokenPipeError, ConnectionResetError, OSError) as e:
                proc.join(1)  # let the exit code settle
                code = proc.exitcode
                self._reset()
                if code == sbx.SELF_TEST_EXIT_CODE:
                    raise _self_test_error() from None
                raise WorkerCrashed(f"worker pipe closed (exit code {code})") from e
            except WorkerCrashed:
                self._reset()
                raise
            except (ValueError, RecursionError, MemoryError):  # hostile JSON / bad utf-8
                self._reset()
                raise WorkerCrashed("malformed worker reply") from None
            finally:
                self._op = None
            ok = reply.get("ok")
            code, message = reply.get("code"), reply.get("message", "")
            valid = isinstance(ok, bool) and (
                isinstance(reply.get("result"), (dict, list, type(None)))
                if ok
                else isinstance(code, str) and isinstance(message, str)
            )
            if not valid:
                self._reset()
                raise WorkerCrashed("malformed worker reply")
            if op not in ("run", "shutdown"):  # back in the worker's main loop, reading
                self._flush_unsent(conn, proc)  # state saved during this op, without a run
            if not ok:
                raise RunnerError(
                    str(code),
                    str(message),
                    retryable=reply.get("retryable") is True,
                    release=reply.get("release") is True,
                )
            return reply

    def ensure_model(
        self, model: ModelEntry, budget: Budget, model_path: Path | None = None
    ) -> None:
        """Load `model`; no-op if already warm, kill+restart on a model switch."""
        with self._lock:
            self._note_budget(budget)
            g = self._interrupt_gen  # read once: an interrupt landing now must not be swallowed
            if g != self._seen_gen:
                self._seen_gen = g  # re-arm: later interrupts abort this call
                self._reset()
            if self._model_id == model.model_id and self._proc and self._proc.is_alive():
                return
            if self._model_id is not None and self._proc and self._proc.is_alive():
                self._graceful_reset()  # model switch: let the runner free external memory
            elif self._model_id is not None or (self._sandbox is not None and self._proc):
                self._reset()  # a sandboxed worker's grants are fixed: load into a fresh one
            # MLX workers never need torch: keep `transformers` from importing it (smaller surface).
            self._ollama_ref = model.ref if model.engine == Engine.OLLAMA else None
            self._engine_env = {"USE_TORCH": "0"} if model.engine == Engine.MLX else {}
            self._call(
                {
                    "op": "load",
                    "sandbox": self._sandbox_spec(model_path, model),
                    "model": model.model_dump(mode="json"),
                    "budget": budget.model_dump(mode="json"),
                    "model_path": str(model_path) if model_path else None,
                    "cfg": asdict(replace(self._cfg, load_timeout_s=self._load_timeout_s)),
                },
                self._load_timeout_s,
            )
            self._model_id = model.model_id

    def _sandbox_spec(
        self, model_path: Path | None, model: ModelEntry | None
    ) -> dict[str, Any] | None:
        if self._sandbox is None:
            return None
        self._ensure_started()  # the tmp dir is created with the process
        if self._tmp_dir is None:  # pragma: no cover - _ensure_started guarantees it
            raise WorkerCrashed("worker tmp dir missing")
        # Loopback access to Ollama only for an Ollama load, from the same config the runner uses.
        ollama = (
            model.engine == Engine.OLLAMA
            if model is not None
            else Engine.OLLAMA.value in self._engines
        )
        return sbx.sandbox_spec(
            self._sandbox,
            model_path=model_path,
            tmp_dir=self._tmp_dir,
            engines=[model.engine.value] if model is not None else self._engines,
            ollama_port=urlsplit(self._cfg.ollama_url).port if ollama else None,
        )

    def run(
        self,
        batch: list[Record],
        params: TaskParams | None = None,
        on_progress: Callable[[int], None] | None = None,
        budget: Budget | None = None,
    ) -> list[Record]:
        """Run `batch` on the loaded model. `params=None` means the type's defaults.

        `budget` is the budget in force for this batch: the worker re-applies memory limits, batch
        caps and CPU priority before it starts (None keeps the one given at load). The latest
        `control()` state (duty, pause) travels with the batch, so it is never out of date.
        """
        msg: dict[str, Any] = {
            "op": "run",
            "batch": batch,
            "params": params.model_dump(mode="json") if params is not None else {},
        }
        if budget is not None:
            msg["budget"] = budget.model_dump(mode="json")
            self._note_budget(budget)
        with self._ctl_lock:
            ctl = self._ctl
            unsent = self._unsent
            if ctl is not None and unsent is not None and unsent.gen <= ctl.gen:
                self._unsent = None  # this run carries it ...
                if unsent.trim:
                    ctl = replace(ctl, trim=True)  # ... including a trim whose send failed
        if ctl is not None:
            msg["ctl"] = ctl.to_msg()
        self._usage = None
        try:
            reply = self._call(msg, self._step_timeout_s, on_progress)
        except RunnerError as exc:
            if exc.code == MODEL_INTEGRITY:  # a bad tag must not stay warm and churn
                with self._lock:
                    self._graceful_reset()
            raise
        result = reply["result"]
        if not isinstance(result, list) or not all(isinstance(r, dict) for r in result):
            with self._lock:
                self._reset()
            raise WorkerCrashed("malformed worker reply")
        return result

    def control(self, *, duty: float, paused: bool, trim: bool = False) -> int:
        """Change the duty cycle / pause state of the running batch (and of later ones).

        Applied by the worker at its next checkpoint, or at once during a pacer sleep or a pause.
        Latest wins: each call gets a new generation, and the worker ignores anything not newer
        than what it has. Returns the generation; `last_ack()` reports when it was applied.
        `trim=True` additionally asks the runner to free cached memory once. Never blocks on the
        call lock, so it works while a batch is running; with no worker alive the state is just
        stored and sent with the next `run`. Raises `ValueError` for an invalid duty.
        """
        with self._ctl_lock:
            gen = self._ctl_gen + 1
            # strict round-trip: a bad duty / paused / trim type is rejected before anything moves
            ctl = WorkerCtl.from_msg(WorkerCtl(gen, duty, paused, trim).to_msg())
            self._ctl_gen = gen
            self._ctl = replace(ctl, trim=False)  # trim is a one-shot, not state
            if self._unsent is not None and self._unsent.trim:
                ctl = replace(ctl, trim=True)  # an earlier unsent trim is not lost
            self._unsent = ctl  # until a send succeeds
        conn, proc = self._conn, self._proc
        # While a non-run op (load, stats, probe) is in flight the worker is not reading: writing
        # would only fill the pipe and trip the watchdog on a healthy worker. The state stays in
        # `_unsent` and the next run carries it.
        busy = self._op not in (None, "run")
        if conn is not None and proc is not None and not self._closed and not busy:
            self._send_ctl(conn, proc, ctl, wait=True)
        return ctl.gen

    def _send_ctl(self, conn: Connection, proc: BaseProcess, ctl: WorkerCtl, *, wait: bool) -> bool:
        """Write one ctl frame without ever wedging the caller. True if it was written.

        The pipe write lock is waited for at most `CTL_SEND_TIMEOUT_S` (`wait=False`: not at all),
        and the write itself is bounded by a watchdog: a worker that has stopped reading would
        otherwise block `send` forever once the pipe buffer is full. At the deadline the worker is
        killed (a partial frame must never be followed by another one, and a worker that ignores
        its control channel is not honouring the budget); the next call sees it dead.
        """
        if not self._send_lock.acquire(blocking=wait, timeout=CTL_SEND_TIMEOUT_S if wait else -1):
            return False  # a newer state will be re-sent by the in-flight call (_flush_unsent)
        done = threading.Event()  # set before cancel(): a timer already firing must stand down
        watchdog = threading.Timer(CTL_SEND_TIMEOUT_S, self._abort_stuck_send, (proc, done))
        watchdog.daemon = True
        watchdog.start()
        try:
            self._wire_gen = max(self._wire_gen, ctl.gen)  # before the write: the ack may race it
            _send(conn, ctl.to_msg())
        except (OSError, ValueError, TypeError):
            # worker gone / closing: the next run carries the state
            return False
        finally:
            done.set()
            watchdog.cancel()
            self._send_lock.release()
        with self._ctl_lock:
            self._sent_gen = max(self._sent_gen, ctl.gen)
            if self._unsent is not None and self._unsent.gen <= ctl.gen:
                self._unsent = None
        return True

    def _abort_stuck_send(self, proc: BaseProcess, done: threading.Event) -> None:
        """Watchdog: a ctl write did not complete in time. Treated as a protocol violation by the
        worker: kill it and abort any call in flight (the handle stays usable, like `interrupt`).
        No-op if the send finished meanwhile. The `done` check narrows the gap but cannot close it:
        a write completing exactly at the deadline may still be killed, which is acceptable (the
        next call sees a dead worker and respawns)."""
        if done.is_set():
            return
        self._interrupt_gen += 1
        _kill_quietly(proc)

    def _flush_unsent(self, conn: Connection, proc: BaseProcess) -> None:
        """Re-send a control state whose send found the pipe busy (called by the in-flight call
        between replies; never blocks)."""
        with self._ctl_lock:
            pending = self._unsent
        if pending is not None and pending.gen > self._sent_gen and not self._closed:
            self._send_ctl(conn, proc, pending, wait=False)

    def last_ack(self) -> WorkerAck | None:
        """The worker's most recent acknowledgement of a control generation (None before any)."""
        return self._ack

    def gpu_share(self) -> float | None:
        """Busy share of wall time in the current/last run (pauses excluded); None before data."""
        usage = self._usage
        return usage.share if usage is not None else None

    def footprint_bytes(self) -> int | None:
        """Memory the OS charges the worker, or None if there is no live worker. Safe to call
        during a run (takes no handle lock). The sample is taken first and the child is checked
        alive after: an unreaped child keeps its pid, so a still-alive child proves the number is
        the worker's and not a recycled pid's."""
        proc = self._proc
        pid = proc.pid if proc is not None else None
        if proc is None or pid is None:
            return None
        value = process_footprint_bytes(pid)
        with contextlib.suppress(ValueError, AssertionError):
            if proc.is_alive():
                return value
        return None

    def priority_ok(self) -> bool | None:
        """Control-side check that the worker honours the CPU priority of the budget in force:
        its nice is at least the target and, for `background` on macOS, Darwin background QoS is
        still set. False = the worker is less throttled than its budget (kill / respawn it).
        None = nothing to check (no worker, no budget seen yet, or it just exited). A budget that
        was only just raised is applied by the worker when its batch starts: callers should treat
        a False right after a budget change as a grace window, not a violation.
        """
        proc = self._proc
        pid = proc.pid if proc is not None else None
        if proc is None or pid is None or not self._prio_seen:
            return None
        priority = self._cpu_priority
        try:
            state = current_priority(pid)
            if not proc.is_alive():  # same sample-then-verify as footprint_bytes
                return None
        except (OSError, ValueError, AssertionError):
            return None
        return satisfies(state, priority)

    def stats(self) -> dict[str, int]:
        """Worker memory: runner `active_bytes`/`peak_bytes` plus process `max_rss_bytes`."""
        stats: dict[str, int] = self._call({"op": "stats"}, self._step_timeout_s)["result"]
        return stats

    def probe(self, timeout_s: float = 90.0) -> dict[str, str | None]:
        """Spawn a worker, report `{engine: version | None}` for this handle's engines (imports
        happen in the child only), then shut it down. Do not call with a model loaded."""
        with self._lock:
            try:
                reply = self._call(
                    {
                        "op": "probe",
                        "cfg": asdict(self._cfg),
                        "sandbox": self._sandbox_spec(None, None),
                    },
                    timeout_s,
                )
                result: dict[str, str | None] = reply["result"]
                return result
            finally:
                self._reset()

    def unload(self) -> None:
        """Drop the model and free its memory by resetting the worker; the handle stays usable.

        A live worker is first asked to unload (short timeout) so a runner can release memory
        held outside the process (Ollama `keep_alive: 0`); then it is killed regardless.
        """
        with self._lock:
            self._graceful_reset()

    def _graceful_reset(self) -> None:
        """Ask a live loaded worker to unload under a total deadline, then ALWAYS kill it.

        Any failure of the polite step (crash, protocol violation, tick flood, bug) is
        swallowed; the `finally` guarantees the worker never outlives this call.
        """
        try:
            proc = self._proc
            if proc is not None and proc.is_alive() and self._model_id is not None:
                with contextlib.suppress(Exception):
                    self._call(
                        {"op": "unload"},
                        UNLOAD_TIMEOUT_S,
                        hard_deadline=time.monotonic() + UNLOAD_TIMEOUT_S,
                    )
                    self._ollama_ref = None  # the runner released it; skip the second request
        finally:
            self._reset()

    def kill(self) -> None:
        """Stop for good: hard-kill the worker (reclaiming model memory), never respawn.

        Lock-free so it can interrupt a call that holds the lock; safe from any thread.
        """
        self._closed = True
        ref = self._ollama_ref
        proc = self._proc
        if proc is not None:
            with contextlib.suppress(OSError, ValueError, AttributeError):
                proc.kill()
            self._release_ollama(ref)

    def _release_ollama(self, ref: str | None) -> None:
        """After a hard kill the daemon still holds the weights for up to `keep_alive`: send a
        best-effort `keep_alive: 0` from the control side (short timeout, no env proxies). Callers
        run off the event loop (`asyncio.to_thread`)."""
        if ref is not None:
            from minion_executor.runners.ollama import release_model

            release_model(self._cfg.ollama_url, ref)

    def interrupt(self) -> None:
        """Abort the current work: hard-kill the worker but allow a respawn on the next call.

        Unlike `kill()` this leaves the handle open. An in-flight call fails with `WorkerCrashed`
        and resets; a dead idle worker is replaced by `ensure_model` (which reloads the model).
        Lock-free, safe from any thread.
        """
        self._interrupt_gen += 1  # kill() is async: don't trust is_alive() right after it
        ref = self._ollama_ref
        proc = self._proc
        if proc is not None:
            with contextlib.suppress(OSError, ValueError, AttributeError):
                proc.kill()
            self._release_ollama(ref)

    def _reset(self) -> None:
        """Kill (if alive) and forget the worker. Callers hold the lock."""
        proc, conn = self._proc, self._conn
        self._proc = self._conn = None
        self._model_id = None
        self._ack = None
        with self._ctl_lock:
            self._sent_gen, self._unsent = self._ctl_gen, None  # the next run carries the state
            self._wire_gen = 0
        ref, self._ollama_ref = self._ollama_ref, None
        if proc is not None:
            with contextlib.suppress(OSError, ValueError):
                if proc.is_alive():
                    proc.kill()
        if conn is not None:
            # a control() write may be mid-flight on this conn (the kill above unblocks it): let it
            # finish before closing the fd; the watchdog bounds it, so proceed anyway on timeout
            got = self._send_lock.acquire(timeout=CTL_SEND_TIMEOUT_S + 1)
            if got:
                try:
                    conn.close()
                finally:
                    self._send_lock.release()
            # else: a writer may still hold the fd; closing it could hit a recycled descriptor,
            # so leave the connection to the garbage collector
        if proc is not None:
            proc.join(timeout=5)
        tmp, self._tmp_dir = self._tmp_dir, None
        if tmp is not None:
            shutil.rmtree(tmp, ignore_errors=True)
        if proc is not None:  # hard kill: the daemon may still hold the weights (off-loop callers)
            self._release_ollama(ref)

    def close(self) -> None:
        self._closed = True
        with self._lock:
            if self._proc is not None and self._proc.is_alive():
                with contextlib.suppress(WorkerCrashed, RunnerError):
                    self._call({"op": "shutdown"}, 10.0)
            self._reset()
