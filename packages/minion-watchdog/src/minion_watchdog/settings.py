from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class WatchdogSettings(BaseSettings):
    """Watchdog configuration, read from `MINION_*` environment variables.

    Reclaim never touches S3, so only the database is required.
    """

    model_config = SettingsConfigDict(env_prefix="MINION_", env_file=".env", extra="ignore")

    database_url: SecretStr  # required; see .env.example
    db_pool_size: int = Field(default=2, ge=1)
    heartbeat_timeout_s: float = Field(default=900.0, gt=0)  # silent this long -> reclaimed
    # Hard cap on one acquirement, however fresh its heartbeat (a looping executor cannot hold a
    # subtask forever). Must exceed the slowest legitimate subtask.
    max_lease_s: float = Field(default=6 * 3600.0, gt=0)
    watchdog_interval_s: float = Field(default=60.0, gt=0)
    watchdog_batch_size: int = Field(default=500, ge=1)
    cancel_sweep_window_s: float = Field(default=3600.0, gt=0)  # how far back to heal cancels
    watchdog_max_backoff_s: float = Field(default=300.0, gt=0)  # cap after repeated DB errors

    @model_validator(mode="after")
    def _lease_exceeds_timeout(self) -> "WatchdogSettings":
        if self.max_lease_s <= self.heartbeat_timeout_s:
            raise ValueError("max_lease_s must exceed heartbeat_timeout_s")
        return self
