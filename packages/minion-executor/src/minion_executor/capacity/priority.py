"""CPU priority of the worker, applied from inside the worker (ADR-0006).

Lives in `capacity/` (not `worker/`) so runners and the control side can read it without importing
the worker process module.

Two levers, both unprivileged:
  * `nice`: portable, monotonic. Raising the niceness works live; lowering it is `EACCES`, so a
    lower-priority-to-higher change takes effect only when the worker is next respawned.
  * Darwin background QoS (`setpriority(PRIO_DARWIN_PROCESS, 0, PRIO_DARWIN_BG)`, what
    `taskpolicy -b` does): throttles CPU, I/O and GPU scheduling of every thread in the process,
    and is reversible. Used for `background` only.
"""

from __future__ import annotations

import contextlib
import ctypes
import functools
import os
import struct
import sys
from dataclasses import dataclass
from typing import Any

from minion_common.enums import CpuPriority

from minion_executor.capacity.profiles import nice_for


@dataclass(frozen=True)
class PriorityState:
    nice: int
    background: bool  # Darwin background QoS is on (always False elsewhere)


_PROC_PIDTBSDINFO = 3  # proc_pidinfo flavor -> struct proc_bsdinfo
_BSDINFO_BYTES = 136
_BSDINFO_NICE_OFFSET = 116  # int32 pbi_nice
_PROC_FLAG_DARWINBG = 0x8000  # pbi_flags (offset 0): process is in Darwin background


@functools.cache
def _proc_pidinfo() -> Any:
    fn = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True).proc_pidinfo
    fn.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_uint64, ctypes.c_void_p, ctypes.c_int]
    fn.restype = ctypes.c_int
    return fn


def _darwin_state_of(pid: int) -> PriorityState:
    """Another process's nice and Darwin-BG flag. `getpriority(PRIO_DARWIN_PROCESS, other)` reads
    0 for any pid but our own, so the control side reads `proc_bsdinfo` instead."""
    buf = ctypes.create_string_buffer(_BSDINFO_BYTES)
    if _proc_pidinfo()(pid, _PROC_PIDTBSDINFO, 0, buf, _BSDINFO_BYTES) != _BSDINFO_BYTES:
        raise ProcessLookupError(pid)
    (flags,) = struct.unpack_from("I", buf.raw, 0)
    (nice,) = struct.unpack_from("i", buf.raw, _BSDINFO_NICE_OFFSET)
    return PriorityState(nice=nice, background=bool(flags & _PROC_FLAG_DARWINBG))


def current_priority(pid: int = 0) -> PriorityState:
    """Read back what the OS actually applied (never trust the request). `pid` 0 is this process;
    another pid (the control side checking its worker) raises `OSError` if it is gone."""
    if pid not in (0, os.getpid()) and sys.platform == "darwin":
        return _darwin_state_of(pid)
    bg = False
    if sys.platform == "darwin":
        with contextlib.suppress(OSError):
            bg = os.getpriority(os.PRIO_DARWIN_PROCESS, 0) == 1
    return PriorityState(nice=os.getpriority(os.PRIO_PROCESS, pid), background=bg)


def satisfies(state: PriorityState, priority: CpuPriority | None) -> bool:
    """True if `state` is at least as throttled as `priority` demands (more throttled is fine:
    a mismatch is only the worker being *less* restricted than its budget)."""
    if state.nice < nice_for(priority):
        return False
    return not (
        sys.platform == "darwin" and priority is CpuPriority.BACKGROUND and not state.background
    )


def apply_cpu_priority(priority: CpuPriority | None) -> PriorityState:
    """Move this process toward `priority` as far as an unprivileged process can; returns the
    state afterwards. Failures are swallowed (priority is best effort, never a reason to fail a
    batch)."""
    want_nice = nice_for(priority)
    now = current_priority()
    if want_nice > now.nice:
        with contextlib.suppress(OSError):
            os.nice(want_nice - now.nice)
    if sys.platform == "darwin":
        want_bg = priority is CpuPriority.BACKGROUND
        if want_bg != now.background:
            with contextlib.suppress(OSError):
                os.setpriority(os.PRIO_DARWIN_PROCESS, 0, os.PRIO_DARWIN_BG if want_bg else 0)
    return current_priority()
