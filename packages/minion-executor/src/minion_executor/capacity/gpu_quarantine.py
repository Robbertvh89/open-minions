"""Persisted GPU-kill book behind the per-engine quarantine (ADR-0006 D17, TASK-43.1).

The loop keeps kill times on its monotonic clock, which does not survive a restart. This file
stores them as UTC wall-clock instants in `gpu_quarantine.json` (0600, atomic replace, same
unsafe-state-dir refusal as status.json), so a restart cannot reset a quarantine. Only the control
process writes it (the worker sandbox allows writes to its own tmp dir only).

Loading never extends a quarantine and never raises: a missing file is empty, a corrupt or
unsafe one is reported to the caller and treated as empty, entries older than the window or dated
in the future (clock set back, hand-edited file) are dropped, and the list lengths are capped.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from minion_executor.capacity.safefile import SafeFileError, atomic_write_private, read_private_file

QUARANTINE_FILE = "gpu_quarantine.json"
MAX_QUARANTINE_BYTES = 64 * 1024
MAX_KILLS_PER_LIST = 64  # far above what the quarantine points can use; bounds the file


class EngineKills(BaseModel):
    model_config = ConfigDict(extra="forbid")

    duty: list[float] = Field(default_factory=list, max_length=MAX_KILLS_PER_LIST)
    meter: list[float] = Field(default_factory=list, max_length=MAX_KILLS_PER_LIST)


class QuarantineFile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = 1
    engines: dict[str, EngineKills] = Field(default_factory=dict, max_length=16)


# engine value -> (duty kill ages, meter kill ages) in seconds before now
Ages = dict[str, tuple[list[float], list[float]]]


def save_kills(state_dir: Path, kills: dict[str, tuple[list[float], list[float]]]) -> None:
    """Write `kills` (engine value -> (duty, meter) wall-clock epoch seconds). Raises
    SafeFileError."""
    body = QuarantineFile(
        engines={
            e: EngineKills(
                duty=sorted(d)[-MAX_KILLS_PER_LIST:], meter=sorted(m)[-MAX_KILLS_PER_LIST:]
            )
            for e, (d, m) in kills.items()
            if d or m
        }
    )
    atomic_write_private(state_dir / QUARANTINE_FILE, body.model_dump_json().encode("utf-8"))


def load_ages(state_dir: Path, wall_now: float, window_s: float) -> tuple[Ages, str | None]:
    """(engine value -> (duty ages, meter ages) in seconds, problem note or None).

    Ages are within [0, window_s); everything else is dropped. A problem note (no file contents)
    means the file was unusable and the result is empty.
    """
    try:
        data = read_private_file(
            state_dir / QUARANTINE_FILE, "gpu quarantine file", MAX_QUARANTINE_BYTES
        )
        if data is None:
            return {}, None
        parsed = QuarantineFile.model_validate_json(data)
    except SafeFileError as exc:
        return {}, str(exc)
    except ValidationError:
        return {}, "gpu quarantine file has an invalid schema"
    out: Ages = {}
    for engine, k in parsed.engines.items():
        duty = [wall_now - t for t in k.duty if 0 <= wall_now - t < window_s]
        meter = [wall_now - t for t in k.meter if 0 <= wall_now - t < window_s]
        if duty or meter:
            out[engine] = (duty, meter)
    return out, None
