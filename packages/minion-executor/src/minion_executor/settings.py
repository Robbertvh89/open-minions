"""Executor settings from `MINION_EXECUTOR_*` / `MINION_*` environment variables."""

from __future__ import annotations

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ExecutorSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MINION_EXECUTOR_", extra="ignore")

    # Seconds without work before the worker is unloaded to free model memory (invariant 6).
    # 0 disables idle unload.
    idle_unload_s: float = Field(default=600.0, ge=0, allow_inf_nan=False)
    # Executor-side releases (e.g. MODEL_NOT_ADMISSIBLE) per subtask before failing it instead.
    release_limit: int = Field(
        default=10, ge=1, validation_alias=AliasChoices("MINION_RELEASE_LIMIT")
    )
