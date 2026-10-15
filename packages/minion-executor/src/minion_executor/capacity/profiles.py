"""Static capacity profiles (SPEC FR-51). Live governor enforcement is M4."""

from __future__ import annotations

import os
from dataclasses import dataclass

from minion_common.enums import CapacityProfile
from minion_common.schemas import Budget

RAM_CAP_FRACTION = 0.75
GIB = 1024**3


@dataclass(frozen=True)
class ProfileSpec:
    memory_fraction: float
    gpu_duty_cycle: float
    max_batch_size: int
    nice: int  # CPU priority as a POSIX nice increment for the worker


PROFILES: dict[CapacityProfile, ProfileSpec] = {
    CapacityProfile.LIGHT: ProfileSpec(0.25, 0.30, 4, 15),
    CapacityProfile.BALANCED: ProfileSpec(0.40, 0.50, 8, 10),
    CapacityProfile.FULL: ProfileSpec(0.70, 1.00, 32, 0),
}


def total_memory_gb() -> float:
    return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / GIB


def parse_profile(name: str) -> CapacityProfile:
    try:
        profile = CapacityProfile(name)
    except ValueError:
        raise ValueError(f"unknown profile: {name!r}") from None
    if profile not in PROFILES:
        raise ValueError(f"profile {name!r} needs explicit values (not built in)")
    return profile


def budget_for(profile: CapacityProfile, total_gb: float | None = None) -> Budget:
    spec = PROFILES[profile]
    total = total_memory_gb() if total_gb is None else total_gb
    fraction = min(spec.memory_fraction, RAM_CAP_FRACTION)
    return Budget(
        profile=profile,
        memory_gb=round(total * fraction, 3),
        gpu_duty_cycle=spec.gpu_duty_cycle,
        max_batch_size=spec.max_batch_size,
    )
