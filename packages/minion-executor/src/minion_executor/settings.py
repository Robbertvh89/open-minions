"""Executor settings from `MINION_EXECUTOR_*` / `MINION_*` environment variables."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Self

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from minion_executor.capacity.control import default_state_dir
from minion_executor.model_cache import default_cache_dir


class ExecutorSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MINION_EXECUTOR_", extra="ignore")

    # Seconds without work before the worker is unloaded to free model memory (invariant 6).
    # 0 disables idle unload.
    idle_unload_s: float = Field(default=600.0, ge=0, allow_inf_nan=False)

    # FR-17: seconds an in-flight subtask may keep running after SIGTERM/SIGINT before the worker
    # is killed and the subtask released. 0 = stop at once. A second signal stops immediately.
    stop_grace_s: float = Field(default=30.0, ge=0, le=300, allow_inf_nan=False)

    # Model cache (D6). Ollama models live in Ollama's own store and are not counted.
    model_cache_dir: Path = Field(default_factory=default_cache_dir)
    model_cache_gb: float = Field(default=50.0, gt=0, allow_inf_nan=False)
    # Free space that must remain on the cache volume after a download (the larger of this and
    # 10% of the disk); otherwise the subtask is released with MODEL_CACHE_FULL.
    min_free_disk_gb: float = Field(default=10.0, ge=0, allow_inf_nan=False)
    # Wall-clock limit for one model download (fetch subprocess); on expiry the subtask is released.
    download_timeout_s: float = Field(default=3600.0, gt=0, allow_inf_nan=False)

    # executor.toml (FR-51/52/53). Unset: ~/.config/minion/executor.toml, which may be absent.
    # An explicit path must exist and be absolute (~ expanded). Env var:
    # MINION_EXECUTOR_CONFIG; an empty value counts as unset.
    config_path: Path | None = Field(default=None, validation_alias="MINION_EXECUTOR_CONFIG")

    # Owner control (`pause`/`resume`/`profile` write control.json here; ADR-0006 D11). Created
    # 0700; refused when not ours or writable by others.
    state_dir: Path = Field(default_factory=default_state_dir)

    # Dev/tests only: read host signals from this JSON file instead of macOS. Needs
    # MINION_EXECUTOR_DEV=1; an empty value counts as unset.
    fake_signals: Path | None = None

    # Governor (ADR-0006 D1/D12): seconds between signal polls (bounds NF-9 detection), and between
    # presence pings while paused (the server polls "< 2 min", ARCHITECTURE §7).
    signal_poll_s: float = Field(default=2.0, ge=0.5, le=4.0, allow_inf_nan=False)
    presence_interval_s: float = Field(default=60.0, ge=1.0, le=110.0, allow_inf_nan=False)

    # Governor timers (ADR-0006 D5/D6). The defaults are the spec; any other value is a test knob
    # and needs MINION_EXECUTOR_DEV=1.
    pause_release_after_s: float = Field(default=60.0, gt=0, le=3600, allow_inf_nan=False)
    resume_after_s: float = Field(default=10.0, ge=0, le=3600, allow_inf_nan=False)
    escalate_after_s: float = Field(default=60.0, ge=0, le=3600, allow_inf_nan=False)
    downgrade_batch_s: float = Field(default=60.0, gt=0, le=3600, allow_inf_nan=False)

    # Run the inference worker inside the Seatbelt sandbox (ADR-0009). Turning it off is a dev
    # escape hatch only and requires MINION_EXECUTOR_DEV=1.
    sandbox: bool = True
    # Same for the model-fetch subprocess (network allowed, writes only to its staging dir).
    fetch_sandbox: bool = True

    @field_validator("state_dir")
    @classmethod
    def _state_dir_absolute(cls, v: Path) -> Path:
        v = v.expanduser()
        if not v.is_absolute():
            raise ValueError("MINION_EXECUTOR_STATE_DIR must be an absolute path")
        return v

    @field_validator("config_path", "fake_signals", mode="before")
    @classmethod
    def _empty_config_path_is_unset(cls, v: object) -> object:
        return None if isinstance(v, str) and not v.strip() else v

    @field_validator("config_path")
    @classmethod
    def _config_path_absolute(cls, v: Path | None) -> Path | None:
        if v is None:
            return None
        v = v.expanduser()
        if not v.is_absolute():
            raise ValueError("MINION_EXECUTOR_CONFIG must be an absolute path")
        return v

    @field_validator("fake_signals")
    @classmethod
    def _fake_signals_absolute(cls, v: Path | None) -> Path | None:
        if v is None:
            return None
        v = v.expanduser()
        if not v.is_absolute():
            raise ValueError("MINION_EXECUTOR_FAKE_SIGNALS must be an absolute path")
        return v

    @field_validator("model_cache_dir")
    @classmethod
    def _cache_dir_absolute(cls, v: Path) -> Path:
        v = v.expanduser()
        if not v.is_absolute():
            raise ValueError("MINION_EXECUTOR_MODEL_CACHE_DIR must be an absolute path")
        return v

    @model_validator(mode="after")
    def _sandbox_off_needs_dev(self) -> Self:
        dev = os.environ.get("MINION_EXECUTOR_DEV") == "1"
        if not self.sandbox and not dev:
            raise ValueError("MINION_EXECUTOR_SANDBOX=0 is only allowed with MINION_EXECUTOR_DEV=1")
        if not self.fetch_sandbox and not dev:
            raise ValueError(
                "MINION_EXECUTOR_FETCH_SANDBOX=0 is only allowed with MINION_EXECUTOR_DEV=1"
            )
        timers = (
            (self.pause_release_after_s, 60.0, "PAUSE_RELEASE_AFTER_S"),
            (self.resume_after_s, 10.0, "RESUME_AFTER_S"),
            (self.escalate_after_s, 60.0, "ESCALATE_AFTER_S"),
            (self.downgrade_batch_s, 60.0, "DOWNGRADE_BATCH_S"),
        )
        for value, default, name in timers:
            if value != default and not dev:
                raise ValueError(
                    f"MINION_EXECUTOR_{name} is only allowed with MINION_EXECUTOR_DEV=1"
                )
        if self.fake_signals is not None and not dev:
            raise ValueError(
                "MINION_EXECUTOR_FAKE_SIGNALS is only allowed with MINION_EXECUTOR_DEV=1"
            )
        return self
