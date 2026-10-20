"""Worker IPC hardening (TASK-12): one deadline thread per call, bounded request write, socket
shutdown on a watchdog fire. In-process socketpairs and fakes only; nothing here waits for real
time to pass except the (sub-second) deadlines under test."""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import socket
import threading
import time
from collections.abc import Callable
from multiprocessing.connection import Connection
from typing import Any

import pytest
from minion_common.enums import Engine
from minion_executor.worker import process as wp
from minion_executor.worker.process import (
    WorkerCrashed,
    WorkerHandle,
    WorkerTimeout,
    _Deadline,
    _shutdown_quietly,
)


class _FakeProc:
    """Stands in for the worker process; `kill` is a no-op when `ignores_kill` (a grandchild
    holding the peer fd keeps the pipe open after the worker pid is gone)."""

    pid = 4242

    def __init__(self, *, ignores_kill: bool = False) -> None:
        self.ignores_kill = ignores_kill
        self.killed = False
        self._dead = False
        self.exitcode: int | None = None

    def is_alive(self) -> bool:
        return not self._dead

    def kill(self) -> None:
        self.killed = True
        if not self.ignores_kill:
            self._dead = True
            self.exitcode = -9

    def join(self, timeout: float | None = None) -> None:
        return None


def _pair() -> tuple[Connection, Connection]:
    return mp.get_context("spawn").Pipe()


def _handle(proc: _FakeProc, conn: Connection, **kw: Any) -> WorkerHandle:
    w = WorkerHandle([Engine.FAKE], **kw)
    w._proc, w._conn, w._model_id = proc, conn, "m"  # type: ignore[assignment]
    return w


def _frame(msg: dict[str, Any]) -> bytes:
    return json.dumps(msg).encode("utf-8")


def test_pipe_is_a_socketpair() -> None:
    """R1: `_shutdown_quietly` relies on Pipe(duplex=True) being a socket on POSIX."""
    a, b = _pair()
    try:
        s = socket.socket(fileno=os.dup(a.fileno()))
        s.close()
    finally:
        a.close()
        b.close()


def test_deadline_fires_once_for_a_past_deadline() -> None:
    fired = threading.Event()
    calls: list[int] = []

    def on_fire() -> None:
        calls.append(1)
        fired.set()

    dl = _Deadline(on_fire)
    dl.arm(time.monotonic() - 1)
    assert fired.wait(5)
    assert dl.disarm() is True and dl.fired
    dl.arm(time.monotonic() - 1)  # a fired deadline stays fired: no second action
    dl.close()
    assert calls == [1]


def test_deadline_disarmed_in_time_never_fires_and_close_joins_the_thread() -> None:
    dl = _Deadline(lambda: pytest.fail("must not fire"))
    dl.arm(time.monotonic() + 3600)
    assert dl.disarm() is False
    thread = dl._thread
    assert thread is not None
    dl.close()
    assert not thread.is_alive()


def test_deadline_arm_disarm_cycles_use_a_single_thread() -> None:
    baseline = threading.active_count()
    dl = _Deadline(lambda: pytest.fail("must not fire"))
    for _ in range(1000):
        dl.arm(time.monotonic() + 3600)
        assert dl.disarm() is False
    assert threading.active_count() == baseline + 1
    dl.close()
    assert threading.active_count() == baseline


def test_shutdown_quietly_ends_blocked_io_despite_a_leaked_peer_fd() -> None:
    parent, child = _pair()
    leaked = os.dup(child.fileno())  # the "grandchild" holding the peer
    try:
        errors: list[BaseException] = []

        def writer() -> None:
            try:
                parent.send_bytes(b"x" * (8 * 1024 * 1024))  # nobody reads: blocks
            except OSError as e:
                errors.append(e)

        t = threading.Thread(target=writer, daemon=True)
        t.start()
        _shutdown_quietly(parent)
        t.join(5)
        assert not t.is_alive() and errors

        reader_conn, other = _pair()
        leaked2 = os.dup(other.fileno())
        try:
            res: list[BaseException] = []

            def reader() -> None:
                try:
                    reader_conn.recv_bytes()
                except (EOFError, OSError) as e:
                    res.append(e)

            r = threading.Thread(target=reader, daemon=True)
            r.start()
            _shutdown_quietly(reader_conn)
            r.join(5)
            assert not r.is_alive() and res
        finally:
            os.close(leaked2)
            reader_conn.close()
            other.close()
    finally:
        os.close(leaked)
        parent.close()
        child.close()


def test_shutdown_quietly_ignores_a_closed_conn_and_a_plain_pipe() -> None:
    a, b = _pair()
    a.close()
    _shutdown_quietly(a)
    b.close()
    r, w = mp.get_context("spawn").Pipe(duplex=False)
    _shutdown_quietly(r)
    _shutdown_quietly(w)
    r.close()
    w.close()


class _FiredDeadline(_Deadline):
    """Reports a fire on every disarm, as if the watchdog had killed the worker meanwhile."""

    def disarm(self) -> bool:
        super().disarm()
        return True


def test_a_fire_during_the_request_write_is_a_timeout() -> None:
    parent, child = _pair()
    try:
        child.send_bytes(_frame({"ok": True, "result": {}}))  # already in the socket buffer
        proc = _FakeProc()
        w = _handle(proc, parent)
        w._deadline_factory = lambda on_fire: _FiredDeadline(on_fire)
        # the request write also reports a fire, so this exercises the send-side check
        with pytest.raises(WorkerTimeout):
            w.stats()
        assert w.pid is None
    finally:
        child.close()


def test_recv_side_fire_after_a_complete_frame_is_a_timeout() -> None:
    parent, child = _pair()
    try:
        child.send_bytes(_frame({"ok": True, "result": {}}))
        proc = _FakeProc()
        w = _handle(proc, parent)

        class _RecvOnly(_Deadline):
            def __init__(self, on_fire: Callable[[], None]) -> None:
                super().__init__(on_fire)
                self.n = 0

            def disarm(self) -> bool:
                super().disarm()
                self.n += 1
                return self.n >= 2  # first disarm = after the request write

        w._deadline_factory = lambda on_fire: _RecvOnly(on_fire)
        with pytest.raises(WorkerTimeout):
            w.stats()
        assert w.pid is None
    finally:
        child.close()


def test_non_reading_peer_and_ignored_kill_cannot_wedge_the_request_write() -> None:
    parent, child = _pair()
    proc = _FakeProc(ignores_kill=True)
    w = _handle(proc, parent, step_timeout_s=0.2)
    out: list[BaseException] = []

    def go() -> None:
        try:
            w.run([{"id": i, "pad": "x" * 1024} for i in range(8 * 1024)])  # ~8 MiB
        except BaseException as e:
            out.append(e)

    t = threading.Thread(target=go, daemon=True)
    t.start()
    t.join(10)
    try:
        assert not t.is_alive()
        assert len(out) == 1 and isinstance(out[0], WorkerTimeout)
        assert proc.killed
    finally:
        child.close()


def test_a_busy_send_lock_fails_the_call_instead_of_waiting_forever(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(wp, "CTL_SEND_TIMEOUT_S", 0.01)
    parent, child = _pair()
    proc = _FakeProc()
    w = _handle(proc, parent)
    w._send_lock.acquire()
    try:
        with pytest.raises(WorkerCrashed, match="pipe busy"):
            w.stats()
        assert w.pid is None
    finally:
        w._send_lock.release()
        child.close()
