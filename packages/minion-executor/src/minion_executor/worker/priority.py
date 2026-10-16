"""CPU priority of the worker, applied from inside the worker (ADR-0006).

Two levers, both unprivileged:
  * `nice`: portable, monotonic. Raising the niceness works live; lowering it is `EACCES`, so a
    lower-priority-to-higher change takes effect only when the worker is next respawned.
  * Darwin background QoS (`setpriority(PRIO_DARWIN_PROCESS, 0, PRIO_DARWIN_BG)`, what
    `taskpolicy -b` does): throttles CPU, I/O and GPU scheduling of every thread in the process,
    and is reversible. Used for `background` only.
"""

from __future__ import annotations

import contextlib
import os
import sys
from dataclasses import dataclass

from minion_common.enums import CpuPriority

from minion_executor.capacity.profiles import nice_for

_DARWIN = sys.platform == "darwin"


@dataclass(frozen=True)
class PriorityState:
    nice: int
    background: bool  # Darwin background QoS is on (always False elsewhere)


def current_priority() -> PriorityState:
    """Read back what the OS actually applied (never trust the request)."""
    bg = False
    if _DARWIN:
        with contextlib.suppress(OSError):
            bg = os.getpriority(os.PRIO_DARWIN_PROCESS, 0) == 1
    return PriorityState(nice=os.getpriority(os.PRIO_PROCESS, 0), background=bg)


def apply_cpu_priority(priority: CpuPriority | None) -> PriorityState:
    """Move this process toward `priority` as far as an unprivileged process can; returns the
    state afterwards. Failures are swallowed (priority is best effort, never a reason to fail a
    batch)."""
    want_nice = nice_for(priority)
    now = current_priority()
    if want_nice > now.nice:
        with contextlib.suppress(OSError):
            os.nice(want_nice - now.nice)
    if _DARWIN:
        want_bg = priority is CpuPriority.BACKGROUND
        if want_bg != now.background:
            with contextlib.suppress(OSError):
                os.setpriority(os.PRIO_DARWIN_PROCESS, 0, os.PRIO_DARWIN_BG if want_bg else 0)
    return current_priority()
