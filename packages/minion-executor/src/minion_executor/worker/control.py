"""Live-control messages between the control process and the worker (ADR-0006).

`{"op": "ctl", "gen": n, "duty": d, "paused": b, "trim": b}` flows control -> worker on the
existing duplex pipe. It is latest-wins: a message with a generation not newer than the one
already applied is dropped. In a run the worker answers with an ack tick (`ack` = the applied
generation); outside a run it applies the message silently (a reply would desync `_call`).
Every field is validated strictly on both sides: an invalid message is rejected, never coerced.

The worker's acks and usage numbers are advisory (it is the party being limited): enforcement is
control-side, by sampling the worker's footprint and OS priority and killing it on a violation.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, TypeGuard


def _is_int(v: object) -> TypeGuard[int]:
    return isinstance(v, int) and not isinstance(v, bool)


def _is_num(v: object) -> TypeGuard[int | float]:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


@dataclass(frozen=True)
class WorkerCtl:
    """What the governor wants of a running batch. `trim` is a one-shot (free caches now)."""

    gen: int
    duty: float
    paused: bool
    trim: bool = False

    def to_msg(self) -> dict[str, Any]:
        return {
            "op": "ctl",
            "gen": self.gen,
            "duty": self.duty,
            "paused": self.paused,
            "trim": self.trim,
        }

    @staticmethod
    def from_msg(msg: Mapping[str, Any]) -> WorkerCtl:
        """Strict parse of a ctl message (or the `ctl` in a `run`); `ValueError` if bad."""
        gen, duty, paused, trim = (
            msg.get("gen"),
            msg.get("duty"),
            msg.get("paused"),
            msg.get("trim"),
        )
        if (
            not _is_int(gen)
            or gen < 0
            or not _is_num(duty)
            or not 0 < duty <= 1
            or not isinstance(paused, bool)
            or not isinstance(trim, bool)
        ):
            raise ValueError("invalid ctl message")
        return WorkerCtl(gen=gen, duty=float(duty), paused=paused, trim=trim)


@dataclass(frozen=True)
class WorkerAck:
    """The worker's last acknowledgement: it applied generation `gen` at control-side `at`."""

    gen: int
    at: float  # time.monotonic() on the control side when the ack was received
    paused: bool


@dataclass(frozen=True)
class WorkerUsage:
    """Pacer totals of the current run (pauses excluded): `busy_s / wall_s` is the GPU share."""

    busy_s: float
    wall_s: float

    @property
    def share(self) -> float | None:
        return self.busy_s / self.wall_s if self.wall_s > 0 else None


def parse_usage(reply: Mapping[str, Any]) -> WorkerUsage | None:
    """`busy_s`/`wall_s` of a tick, or None if absent. `ValueError` if present but invalid."""
    if "busy_s" not in reply and "wall_s" not in reply:
        return None
    busy, wall = reply.get("busy_s"), reply.get("wall_s")
    if not (_is_num(busy) and _is_num(wall)) or busy < 0 or wall < 0 or busy > wall + 1e-3:
        raise ValueError(
            "invalid tick usage"
        )  # busy time cannot exceed the wall time it is part of
    return WorkerUsage(float(busy), float(wall))


def parse_ack(
    reply: Mapping[str, Any],
    *,
    issued: int,
    at: float,
    prev: WorkerAck | None = None,
    expect_paused: tuple[int, bool] | None = None,
) -> WorkerAck | None:
    """The ack inside a tick, or None if the tick carries none. `ValueError` if invalid, if it
    acknowledges a generation the control side never issued, if it goes backwards relative to
    `prev` (the same worker's previous ack), or if it contradicts what the control side sent:
    `expect_paused=(gen, paused)` is the state of the newest generation issued, and generation 0
    (the unpaused budget default) is never paused. Older generations are not checked (no history
    is kept, so a healthy worker can never be rejected for a lost entry)."""
    if "ack" not in reply:
        if "paused" in reply:
            raise ValueError("paused without ack")
        return None
    gen, paused = reply["ack"], reply.get("paused")
    if not (_is_int(gen) and isinstance(paused, bool)) or gen < 0 or gen > issued:
        raise ValueError("invalid ack")
    if prev is not None and gen < prev.gen:
        raise ValueError("ack went backwards")
    if gen == 0 and paused:
        raise ValueError("ack pauses the default generation")
    if expect_paused is not None and gen == expect_paused[0] and paused != expect_paused[1]:
        raise ValueError("ack contradicts the issued state")
    return WorkerAck(gen=gen, at=at, paused=paused)
