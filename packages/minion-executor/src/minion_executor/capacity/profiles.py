"""Capacity profiles (SPEC FR-50/51): built-ins, `executor.toml` overrides, the `custom` profile.

Live enforcement (governor, pause, downgrade) is M4.3 onwards; this module only resolves a profile
name to a `Budget`. The 75 % RAM cap is applied here, to every profile, whatever the config says.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, replace

from minion_common.enums import CapacityProfile, CpuPriority
from minion_common.schemas import Budget

RAM_CAP_FRACTION = 0.75
GIB = 1024**3

# POSIX nice increments for the worker / fetch subprocess. `background` additionally gets the
# Darwin background QoS in the worker (M4.3); nice is the portable floor.
NICE_BY_PRIORITY: dict[CpuPriority, int] = {
    CpuPriority.BACKGROUND: 15,
    CpuPriority.LOW: 10,
    CpuPriority.NORMAL: 0,
}


@dataclass(frozen=True)
class ProfileSpec:
    """One profile. Exactly one of `memory_fraction` (of RAM) and `memory_gb` is set."""

    gpu_duty_cycle: float
    max_batch_size: int
    cpu_priority: CpuPriority
    memory_fraction: float | None = None
    memory_gb: float | None = None

    def __post_init__(self) -> None:
        if (self.memory_fraction is None) == (self.memory_gb is None):
            raise ValueError("exactly one of memory_fraction and memory_gb must be set")

    def wanted_gb(self, total_gb: float) -> float:
        if self.memory_fraction is not None:
            return total_gb * self.memory_fraction
        return float(self.memory_gb or 0.0)


PROFILES: dict[CapacityProfile, ProfileSpec] = {
    CapacityProfile.LIGHT: ProfileSpec(0.30, 4, CpuPriority.BACKGROUND, memory_fraction=0.25),
    CapacityProfile.BALANCED: ProfileSpec(0.50, 8, CpuPriority.LOW, memory_fraction=0.40),
    CapacityProfile.FULL: ProfileSpec(1.00, 32, CpuPriority.NORMAL, memory_fraction=0.70),
}


def total_memory_gb() -> float:
    return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / GIB


def nice_for(priority: CpuPriority | None) -> int:
    """Nice increment for a CPU priority (None, an old budget, counts as normal)."""
    return NICE_BY_PRIORITY[priority or CpuPriority.NORMAL]


def parse_profile(
    name: str, specs: dict[CapacityProfile, ProfileSpec] | None = None
) -> CapacityProfile:
    specs = PROFILES if specs is None else specs
    try:
        profile = CapacityProfile(name)
    except ValueError:
        raise ValueError(f"unknown profile: {name!r}") from None
    if profile not in specs:
        raise ValueError(
            f"profile {name!r} is not defined (add [profiles.{name}] to executor.toml)"
        )
    return profile


def merge_spec(
    base: ProfileSpec,
    *,
    memory_fraction: float | None = None,
    memory_gb: float | None = None,
    gpu_duty_cycle: float | None = None,
    max_batch_size: int | None = None,
    cpu_priority: CpuPriority | None = None,
) -> ProfileSpec:
    """Apply a partial override. A memory override replaces whichever memory form the base used."""
    mem: dict[str, float | None] = {
        "memory_fraction": base.memory_fraction,
        "memory_gb": base.memory_gb,
    }
    if memory_fraction is not None:
        mem = {"memory_fraction": memory_fraction, "memory_gb": None}
    elif memory_gb is not None:
        mem = {"memory_fraction": None, "memory_gb": memory_gb}
    return replace(
        base,
        gpu_duty_cycle=base.gpu_duty_cycle if gpu_duty_cycle is None else gpu_duty_cycle,
        max_batch_size=base.max_batch_size if max_batch_size is None else max_batch_size,
        cpu_priority=base.cpu_priority if cpu_priority is None else cpu_priority,
        **mem,
    )


def budget_for(
    profile: CapacityProfile,
    total_gb: float | None = None,
    specs: dict[CapacityProfile, ProfileSpec] | None = None,
) -> Budget:
    spec = (PROFILES if specs is None else specs)[profile]
    total = total_memory_gb() if total_gb is None else total_gb
    cap = total * RAM_CAP_FRACTION
    return Budget(
        profile=profile,
        memory_gb=round(min(spec.wanted_gb(total), cap), 3),
        gpu_duty_cycle=spec.gpu_duty_cycle,
        max_batch_size=spec.max_batch_size,
        cpu_priority=spec.cpu_priority,
    )
