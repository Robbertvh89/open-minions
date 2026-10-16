"""`executor.toml`: the owner's capacity configuration (SPEC FR-51/52/53, ADR-0006 D9).

The file is trusted input that shapes how much of the Mac the executor takes, so it is refused
unless it belongs to the current user and nobody else can write it (invariant 6). Unknown keys and
bad values are errors, never silently ignored. The 75 % RAM cap is applied in `budget_for`, to
every profile including `custom`.
"""

from __future__ import annotations

import os
import re
import stat
import tomllib
from datetime import time
from pathlib import Path
from typing import Annotated, Self

from minion_common.enums import CapacityProfile, CpuPriority
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    ValidationError,
    model_validator,
)

from minion_executor.capacity.profiles import PROFILES, ProfileSpec, merge_spec

MAX_CONFIG_BYTES = 64 * 1024
DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
_HHMM = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")


class ConfigError(ValueError):
    """Invalid or unsafe executor.toml. The CLI maps it to exit 2."""


def default_config_path() -> Path:
    return Path.home() / ".config" / "minion" / "executor.toml"


def _hhmm(v: str) -> str:
    if not _HHMM.fullmatch(v):
        raise ValueError("expected a 24 h time as HH:MM")
    return v


HHMM = Annotated[str, AfterValidator(_hhmm)]


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ProfileOverride(_Strict):
    """Partial override of a built-in profile; for `custom` every dimension is required."""

    memory_fraction: float | None = Field(default=None, gt=0, le=1, allow_inf_nan=False)
    memory_gb: float | None = Field(default=None, gt=0, le=1024, allow_inf_nan=False)
    gpu_duty_cycle: float | None = Field(default=None, gt=0, le=1, allow_inf_nan=False)
    max_batch_size: int | None = Field(default=None, ge=1, le=4096)
    cpu_priority: CpuPriority | None = None

    @model_validator(mode="after")
    def _one_memory_form(self) -> Self:
        if self.memory_fraction is not None and self.memory_gb is not None:
            raise ValueError("set only one of memory_fraction and memory_gb")
        return self


class ProfilesConfig(_Strict):
    light: ProfileOverride | None = None
    balanced: ProfileOverride | None = None
    full: ProfileOverride | None = None
    custom: ProfileOverride | None = None

    @model_validator(mode="after")
    def _custom_complete(self) -> Self:
        c = self.custom
        if c is None:
            return self
        if c.memory_fraction is None and c.memory_gb is None:
            raise ValueError("profiles.custom needs memory_fraction or memory_gb")
        if c.gpu_duty_cycle is None or c.max_batch_size is None or c.cpu_priority is None:
            raise ValueError("profiles.custom needs gpu_duty_cycle, max_batch_size, cpu_priority")
        return self


class ScheduleEntry(_Strict):
    """Local wall-clock window (a human calendar, the one documented exception to UTC-only)."""

    profile: CapacityProfile
    start: HHMM
    end: HHMM
    days: list[str] | None = None

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.start == self.end:
            raise ValueError("schedule start and end must differ")
        if self.days is not None:
            if not self.days or len(set(self.days)) != len(self.days):
                raise ValueError("days must be a non-empty list without duplicates")
            if any(d not in DAYS for d in self.days):
                raise ValueError(f"unknown day; use {', '.join(DAYS)}")
        return self

    @property
    def start_time(self) -> time:
        return time.fromisoformat(self.start)

    @property
    def end_time(self) -> time:
        return time.fromisoformat(self.end)


class ExecutorConfig(_Strict):
    active_profile: CapacityProfile = CapacityProfile.LIGHT
    idle_profile: CapacityProfile = CapacityProfile.FULL
    idle_after_min: float = Field(default=10.0, ge=1, le=1440, allow_inf_nan=False)
    allow_on_battery: StrictBool = False
    min_battery_percent: int = Field(default=30, ge=0, le=100)
    profiles: ProfilesConfig = Field(default_factory=ProfilesConfig)
    schedule: list[ScheduleEntry] = Field(default_factory=list, max_length=64)

    @model_validator(mode="after")
    def _custom_referenced_only_if_defined(self) -> Self:
        if self.profiles.custom is None:
            used = {self.active_profile, self.idle_profile, *(s.profile for s in self.schedule)}
            if CapacityProfile.CUSTOM in used:
                raise ValueError("'custom' is used but [profiles.custom] is not defined")
        return self


def resolve_profiles(cfg: ExecutorConfig) -> dict[CapacityProfile, ProfileSpec]:
    """Built-in profiles with the config's overrides applied, plus `custom` when defined."""
    specs = dict(PROFILES)
    for name in (CapacityProfile.LIGHT, CapacityProfile.BALANCED, CapacityProfile.FULL):
        ov: ProfileOverride | None = getattr(cfg.profiles, name.value)
        if ov is not None:
            specs[name] = merge_spec(PROFILES[name], **ov.model_dump(exclude_none=True))
    c = cfg.profiles.custom
    if c is not None:
        if c.gpu_duty_cycle is None or c.max_batch_size is None or c.cpu_priority is None:
            raise ConfigError("profiles.custom is incomplete")  # ProfilesConfig rejects this first
        specs[CapacityProfile.CUSTOM] = ProfileSpec(
            c.gpu_duty_cycle,
            c.max_batch_size,
            c.cpu_priority,
            memory_fraction=c.memory_fraction,
            memory_gb=c.memory_gb,
        )
    return specs


def _check_mode(st: os.stat_result, what: str, path: Path) -> None:
    if st.st_uid != os.geteuid():
        raise ConfigError(f"{what} {path} is not owned by the current user")
    if st.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise ConfigError(f"{what} {path} is group- or world-writable (use chmod go-w)")


def _read_safely(path: Path) -> bytes:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    except OSError as exc:
        raise ConfigError(f"cannot read config {path}: {exc.strerror}") from None
    try:
        st = os.fstat(fd)  # the opened file, so no check/use race
        if not stat.S_ISREG(st.st_mode):
            raise ConfigError(f"config {path} is not a regular file")
        _check_mode(st, "config", path)
        try:
            # Symlinks are followed on purpose (dotfile managers); check the link's directory
            # and the target's directory.
            real_parent = Path(os.path.realpath(path)).parent
            for d in dict.fromkeys((path.parent, real_parent)):
                _check_mode(d.stat(), "config directory", d)
        except OSError as exc:
            raise ConfigError(f"cannot stat config directory: {exc.strerror}") from None
        with os.fdopen(fd, "rb") as f:
            fd = -1  # now owned by f
            data = f.read(MAX_CONFIG_BYTES + 1)
    finally:
        if fd >= 0:
            os.close(fd)
    if len(data) > MAX_CONFIG_BYTES:
        raise ConfigError(f"config {path} is larger than {MAX_CONFIG_BYTES} bytes")
    return data


def _format(exc: ValidationError) -> str:
    # loc + message only: pydantic's default text echoes the offending input value
    return "; ".join(
        f"{'.'.join(str(p) for p in e['loc']) or '<root>'}: {e['msg']}" for e in exc.errors()
    )


def load_config(path: Path | None = None) -> ExecutorConfig:
    """Load and validate the config. An explicit `path` must exist; the default may be absent."""
    explicit = path is not None
    p = path if path is not None else default_config_path()
    if not explicit and not os.path.lexists(p):
        return ExecutorConfig()
    data = _read_safely(p)
    try:
        raw = tomllib.loads(data.decode("utf-8"))
    except RecursionError:
        raise ConfigError(f"invalid TOML in {p}: nesting too deep") from None
    except (tomllib.TOMLDecodeError, UnicodeDecodeError) as exc:
        raise ConfigError(f"invalid TOML in {p}: {exc}") from None
    try:
        return ExecutorConfig.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(f"invalid config {p}: {_format(exc)}") from None
