"""Process memory footprint, sampled from the control process (never inside the worker).

macOS: `phys_footprint` from `proc_pid_rusage` (what Activity Monitor charges a process, so it
includes compressed and GPU-wired unified memory that RSS misses). Linux: resident set from
`/proc/<pid>/statm`. Both need no privilege for a child of ours and no extra dependency.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import functools
import os
import sys
from pathlib import Path
from typing import Any

_RUSAGE_INFO_V0 = 0
_V0_PHYS_FOOTPRINT_OFFSET = 72  # rusage_info_v0: uuid[16] then 7 u64 (user..resident), then this
_BUF_BYTES = 512  # >= sizeof(rusage_info_current); V0 fills the first 96 bytes


@functools.cache
def _proc_pid_rusage() -> Any:
    """The libproc function with its signature set, resolved once (a poll calls it every second)."""
    path = ctypes.util.find_library("proc") or "/usr/lib/libproc.dylib"
    fn = ctypes.CDLL(path, use_errno=True).proc_pid_rusage
    fn.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
    fn.restype = ctypes.c_int
    return fn


def _darwin_footprint(pid: int) -> int | None:
    buf = ctypes.create_string_buffer(_BUF_BYTES)
    if _proc_pid_rusage()(pid, _RUSAGE_INFO_V0, buf) != 0:
        return None
    return int.from_bytes(
        buf.raw[_V0_PHYS_FOOTPRINT_OFFSET : _V0_PHYS_FOOTPRINT_OFFSET + 8], sys.byteorder
    )


def _linux_footprint(pid: int) -> int | None:
    fields = Path(f"/proc/{pid}/statm").read_text().split()
    return int(fields[1]) * os.sysconf("SC_PAGE_SIZE")  # resident pages


def process_footprint_bytes(pid: int) -> int | None:
    """Memory the OS charges to `pid`, or None if unknown (gone, not ours, unsupported OS)."""
    if pid <= 0:
        return None
    try:
        if sys.platform == "darwin":
            return _darwin_footprint(pid)
        if sys.platform.startswith("linux"):
            return _linux_footprint(pid)
    except (OSError, ValueError, IndexError, AttributeError):
        return None
    return None
