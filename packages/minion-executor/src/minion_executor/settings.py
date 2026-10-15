"""Executor settings from `MINION_EXECUTOR_*` / `MINION_*` environment variables."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from minion_executor.model_cache import default_cache_dir


class ExecutorSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MINION_EXECUTOR_", extra="ignore")

    # Seconds without work before the worker is unloaded to free model memory (invariant 6).
    # 0 disables idle unload.
    idle_unload_s: float = Field(default=600.0, ge=0, allow_inf_nan=False)

    # Model cache (D6). Ollama models live in Ollama's own store and are not counted.
    model_cache_dir: Path = Field(default_factory=default_cache_dir)
    model_cache_gb: float = Field(default=50.0, gt=0, allow_inf_nan=False)
    # Wall-clock limit for one model download (fetch subprocess); on expiry the subtask is released.
    download_timeout_s: float = Field(default=3600.0, gt=0, allow_inf_nan=False)
