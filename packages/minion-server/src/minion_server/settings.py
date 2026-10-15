from pydantic_settings import BaseSettings, SettingsConfigDict


class ServerSettings(BaseSettings):
    """Server configuration, read from `MINION_*` environment variables."""

    model_config = SettingsConfigDict(env_prefix="MINION_", env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://minion:minion@localhost:5432/minion"
    host: str = "127.0.0.1"
    port: int = 8080
