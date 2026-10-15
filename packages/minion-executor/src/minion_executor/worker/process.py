"""Worker subprocess hosting the Runner (invariant 7).

The control process talks to it over a `multiprocessing.Pipe` using length-framed
messages (`send_bytes`/`recv_bytes`) carrying JSON. The child is started with the
`spawn` context so no Metal/GPU state is inherited via fork.
"""

from __future__ import annotations

import contextlib
import json
import multiprocessing as mp
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from typing import Any

from minion_common.schemas import Budget, ModelEntry

from minion_executor.runners.base import Record, RunnerError


class WorkerCrashed(Exception):  # noqa: N818 - name fixed by plan/API
    """The worker process died or its pipe closed unexpectedly."""


def _send(conn: Connection, msg: dict[str, Any]) -> None:
    conn.send_bytes(json.dumps(msg).encode("utf-8"))


def _recv(conn: Connection) -> dict[str, Any]:
    msg: dict[str, Any] = json.loads(conn.recv_bytes().decode("utf-8"))
    return msg


def worker_main(conn: Connection, runner_name: str, nice: int = 0) -> None:
    """Child entry point (top level so it is picklable under spawn)."""
    import os

    from minion_executor.runners import create_runner

    if nice:
        with contextlib.suppress(OSError):
            os.nice(nice)
    runner = create_runner(runner_name)
    while True:
        try:
            msg = _recv(conn)
        except EOFError:
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
    """Control-side handle. Keeps the model warm across same-model subtasks."""

    def __init__(self, runner_name: str = "fake", nice: int = 0, timeout_s: float = 600.0) -> None:
        self._runner_name = runner_name
        self._nice = nice
        self._timeout_s = timeout_s
        self._ctx = mp.get_context("spawn")
        self._proc: BaseProcess | None = None
        self._conn: Connection | None = None
        self._model_id: str | None = None

    @property
    def pid(self) -> int | None:
        return self._proc.pid if self._proc else None

    @property
    def loaded_model_id(self) -> str | None:
        return self._model_id

    def _ensure_started(self) -> Connection:
        if self._proc is None or self._conn is None:
            parent, child = self._ctx.Pipe()
            proc = self._ctx.Process(
                target=worker_main, args=(child, self._runner_name, self._nice), daemon=True
            )
            proc.start()
            child.close()
            self._proc, self._conn = proc, parent
        return self._conn

    def _call(self, msg: dict[str, Any]) -> dict[str, Any]:
        conn = self._ensure_started()
        proc = self._proc
        if proc is None:  # pragma: no cover - _ensure_started guarantees it
            raise WorkerCrashed("worker not started")
        try:
            _send(conn, msg)
            waited = 0.0
            while not conn.poll(0.1):
                waited += 0.1
                if not proc.is_alive():
                    if conn.poll(0):
                        break
                    raise WorkerCrashed(f"worker exited with code {proc.exitcode}")
                if waited > self._timeout_s:
                    self.kill()
                    raise WorkerCrashed("worker timed out")
            reply = _recv(conn)
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
        if self._model_id == model.model_id and self._proc and self._proc.is_alive():
            return
        if self._model_id is not None:
            self.kill()
        self._call(
            {
                "op": "load",
                "model": model.model_dump(mode="json"),
                "budget": budget.model_dump(mode="json"),
            }
        )
        self._model_id = model.model_id

    def run(self, batch: list[Record]) -> list[Record]:
        reply = self._call({"op": "run", "batch": batch})
        result: list[Record] = reply["result"]
        return result

    def kill(self) -> None:
        """Hard-stop the worker, reclaiming all model memory."""
        proc = self._proc
        if proc is not None and proc.is_alive():
            proc.kill()
        self._reset()

    def _reset(self) -> None:
        proc, conn = self._proc, self._conn
        self._proc = self._conn = None
        self._model_id = None
        if conn is not None:
            conn.close()
        if proc is not None:
            proc.join(timeout=5)

    def close(self) -> None:
        if self._proc is not None and self._proc.is_alive():
            with contextlib.suppress(WorkerCrashed, RunnerError):
                self._call({"op": "shutdown"})
        self.kill()
