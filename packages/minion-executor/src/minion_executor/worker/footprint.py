"""Process memory footprint, sampled from the control process (never inside the worker).

macOS: `phys_footprint` from `proc_pid_rusage` (what Activity Monitor charges a process, so it
includes compressed and GPU-wired unified memory that RSS misses). Linux: resident set from
`/proc/<pid>/statm`. Both need no privilege for a child of ours and no extra dependency.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import os
import sys
from pathlib import Path

_RUSAGE_INFO_V0 = 0
_V0_PHYS_FOOTPRINT_OFFSET = 72  # rusage_info_v0: uuid[16] then 7 u64 (user..resident), then this
_BUF_BYTES = 512  # >= sizeof(rusage_info_current); V0 fills the first 96 bytes


def _darwin_footprint(pid: int) -> int | None:
    path = ctypes.util.find_library("proc") or "/usr/lib/libproc.dylib"
    lib = ctypes.CDLL(path, use_errno=True)
    lib.proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
    lib.proc_pid_rusage.restype = ctypes.c_int
    buf = ctypes.create_string_buffer(_BUF_BYTES)
    if lib.proc_pid_rusage(pid, _RUSAGE_INFO_V0, buf) != 0:
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
