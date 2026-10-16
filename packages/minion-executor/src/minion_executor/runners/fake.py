"""Deterministic fake runner for tests and the e2e path (no GPU, no engine imports)."""

from __future__ import annotations

import hashlib
import math
import os
import time
from pathlib import Path

from minion_common.enums import TaskType
from minion_common.schemas import Budget, CompletionParams, EmbeddingParams, ModelEntry, TaskParams

from minion_executor.runners.base import EngineConfig, MemoryStats, Record, RunnerError, StepControl

EMBED_DIM = 8
PROMPT_TOO_LONG = "PROMPT_TOO_LONG"


def fake_vector(text: str, normalize: bool = True) -> list[float]:
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    raw = [b / 255.0 for b in digest[:EMBED_DIM]]
    if normalize:
        norm = math.sqrt(sum(x * x for x in raw)) or 1.0
        raw = [x / norm for x in raw]
    return [round(x, 6) for x in raw]


INJECTED_FAILURE = "INJECTED_FAILURE"


def _text_of(rec: Record) -> str:
    """Prompt text from `prompt`, `messages` (all message contents) or legacy `text`."""
    if isinstance(rec.get("prompt"), str):
        return str(rec["prompt"])
    msgs = rec.get("messages")
    if isinstance(msgs, list):
        return "\n".join(str(m.get("content", "")) for m in msgs if isinstance(m, dict))
    return str(rec.get("text", ""))


_SANDBOX_PROBES = (
    "read_path",
    "write_path",
    "list_dir",
    "connect",
    "exec",
    "mach_lookup",
    "sandbox_check",
    "cfprefs",
    "signal_parent",
    "mlx_matmul",
)


def _sandbox_probe(rec: Record) -> dict[str, object]:
    """Test hook: attempt one OS action from inside the (possibly sandboxed) worker and report
    whether it succeeded. Never returns file contents; only reachable with test hooks on."""
    try:
        if "read_path" in rec:
            with Path(str(rec["read_path"])).open("rb") as f:
                return {"ok": True, "bytes": len(f.read(64))}
        if "write_path" in rec:
            with Path(str(rec["write_path"])).open("wb") as f:
                f.write(b"x")
            return {"ok": True}
        if "connect" in rec:
            import socket

            host, _, port = str(rec["connect"]).rpartition(":")
            with socket.create_connection((host, int(port)), timeout=3):
                return {"ok": True}
        if "exec" in rec:
            import subprocess

            argv = [str(a) for a in rec["exec"]]
            r = subprocess.run(argv, capture_output=True, timeout=10, check=False)  # noqa: S603
            return {"ok": r.returncode == 0, "returncode": r.returncode}
        if "mach_lookup" in rec:
            return _mach_lookup(str(rec["mach_lookup"]))
        if "list_dir" in rec:
            names = os.listdir(str(rec["list_dir"]))  # noqa: PTH208
            return {"ok": True, "entries": len(names)}
        if "sandbox_check" in rec:
            return _sandbox_check(str(rec["sandbox_check"]), rec.get("path"))
        if "cfprefs" in rec:
            return _cfprefs_keys(str(rec["cfprefs"]))
        if "signal_parent" in rec:
            os.kill(os.getppid(), 0)
            return {"ok": True}
        if "mlx_matmul" in rec:
            return _mlx_matmul(int(rec["mlx_matmul"]))
    except Exception as exc:
        return {"ok": False, "error": type(exc).__name__}
    return {"ok": False, "error": "unknown"}


def _sandbox_check(op: str, path: object) -> dict[str, object]:
    """`sandbox_check(getpid(), op, [PATH filter, path])`: 0 = the operation would be allowed.
    Asks the kernel policy without performing the operation (nothing is ever written). `path`
    must exist: for a missing one the call reports 1 whatever the policy says."""
    import ctypes

    lib = ctypes.CDLL("/usr/lib/libsandbox.1.dylib")
    lib.sandbox_check.restype = ctypes.c_int
    # variadic: only the fixed args are declared so ctypes uses the variadic (stack) ABI on arm64
    lib.sandbox_check.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int]
    if path is None:
        rc = lib.sandbox_check(os.getpid(), op.encode(), 0)
    else:  # SANDBOX_FILTER_PATH = 1
        rc = lib.sandbox_check(os.getpid(), op.encode(), 1, ctypes.c_char_p(str(path).encode()))
    return {"ok": rc == 0, "rc": rc}


def _cfprefs_keys(domain: str) -> dict[str, object]:
    """Number of keys CFPreferences returns for `domain` (0 / failure when preferences are
    denied)."""
    import ctypes

    cf = ctypes.CDLL("/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation")
    cf.CFStringCreateWithCString.restype = ctypes.c_void_p
    cf.CFStringCreateWithCString.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint32]
    cf.CFPreferencesCopyKeyList.restype = ctypes.c_void_p
    cf.CFPreferencesCopyKeyList.argtypes = [ctypes.c_void_p] * 3
    cf.CFArrayGetCount.restype = ctypes.c_long
    cf.CFArrayGetCount.argtypes = [ctypes.c_void_p]
    user = ctypes.c_void_p.in_dll(cf, "kCFPreferencesCurrentUser")
    host = ctypes.c_void_p.in_dll(cf, "kCFPreferencesAnyHost")
    dom = cf.CFStringCreateWithCString(None, domain.encode(), 0x08000100)
    arr = cf.CFPreferencesCopyKeyList(dom, user, host)
    n = int(cf.CFArrayGetCount(arr)) if arr else 0
    return {"ok": n > 0, "keys": n}


def _mlx_matmul(n: int) -> dict[str, object]:
    """GPU matmul + compiled fusion with MLX (the sandbox must not break Metal)."""
    import mlx.core as mx  # engine import, test hook only

    a = mx.ones((n, n))
    b = a @ a
    c = mx.compile(lambda x: mx.softmax(x * 0.5 + mx.sin(x), axis=-1))(b)
    mx.eval(b, c)
    return {
        "ok": True,
        "device": str(mx.default_device()),
        "value": float(b[0, 0].item().real),
        "softmax_sum": float(c[0].sum().item().real),
    }


def _mach_lookup(name: str) -> dict[str, object]:
    """`bootstrap_look_up(bootstrap_port, name)`; KERN_SUCCESS (0) = reachable."""
    import ctypes

    libc = ctypes.CDLL(None)
    port = ctypes.c_uint32.in_dll(libc, "bootstrap_port")
    out = ctypes.c_uint32(0)
    libc.bootstrap_look_up.argtypes = [
        ctypes.c_uint32,
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_uint32),
    ]
    libc.bootstrap_look_up.restype = ctypes.c_int
    rc = libc.bootstrap_look_up(port.value, name.encode(), ctypes.byref(out))
    return {"ok": rc == 0, "kern_return": rc}


class FakeRunner:
    """Deterministic runner. Per-record `fail` / `retryable` / `sleep_s` / `sleep_steps` /
    `echo_env` hooks are honoured only when constructed with `test_hooks=True` (tests); input
    data can never trigger them otherwise."""

    def __init__(self, *, test_hooks: bool = False) -> None:
        self._model: ModelEntry | None = None
        self._cfg = EngineConfig()
        self._hooks = test_hooks

    def load(
        self, model: ModelEntry, model_path: Path | None, budget: Budget, cfg: EngineConfig
    ) -> None:
        if model.memory_gb > budget.memory_gb:
            raise RunnerError("oom", "model exceeds budget")
        self._model = model
        self._cfg = cfg

    def run(self, batch: list[Record], params: TaskParams, ctl: StepControl) -> list[Record]:
        if self._model is None:
            raise RunnerError("not_loaded")
        out: list[Record] = []
        for rec in batch:
            if self._hooks:
                if rec.get("fail"):
                    raise RunnerError(  # fixed code
                        INJECTED_FAILURE,
                        "injected failure",
                        retryable=rec.get("retryable", True) is not False,
                    )
                if rec.get("sleep_s"):  # simulate a slow subtask (capped); no ticks
                    time.sleep(min(float(rec["sleep_s"]), 60.0))
                if rec.get("sleep_steps"):  # slow subtask that ticks via ctl between steps
                    for _ in range(min(int(rec["sleep_steps"]), 1000)):
                        time.sleep(min(float(rec.get("step_s", 0.1)), 10.0))
                        ctl.checkpoint(len(out))
                if any(k in rec for k in _SANDBOX_PROBES):
                    out.append({"id": rec.get("id"), **_sandbox_probe(rec)})
                    ctl.checkpoint(len(out))
                    continue
                if rec.get("echo_env"):  # report whether an env var is visible to the worker
                    out.append({"id": rec.get("id"), "present": str(rec["echo_env"]) in os.environ})
                    ctl.checkpoint(len(out))
                    continue
            text = _text_of(rec)
            if self._model.type is TaskType.EMBEDDING:
                normalize = params.normalize if isinstance(params, EmbeddingParams) else True
                out.append({"id": rec.get("id"), "embedding": fake_vector(text, normalize)})
            else:
                limit = self._cfg.max_prompt_tokens
                if isinstance(params, CompletionParams):
                    limit -= params.max_tokens
                if len(text) > limit:  # fake tokenizer: one char = one token
                    out.append({"id": rec.get("id"), "error": PROMPT_TOO_LONG})
                else:
                    out.append({"id": rec.get("id"), "completion": text})
            ctl.checkpoint(len(out))
        return out

    def memory_in_use(self) -> MemoryStats:
        return {"active_bytes": 0, "peak_bytes": 0}

    def unload(self) -> None:
        self._model = None
