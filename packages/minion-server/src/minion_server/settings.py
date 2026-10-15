from typing import Self

from minion_common.client import require_secure_url
from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _check_prefixes(values: list[str]) -> list[str]:
    if not values:
        raise ValueError("at least one prefix required")
    for p in values:
        if not p or not p.endswith("/"):
            raise ValueError(f"prefix {p!r} must be non-empty and end with '/'")
    return values


class ServerSettings(BaseSettings):
    """Server configuration, read from `MINION_*` environment variables."""

    model_config = SettingsConfigDict(env_prefix="MINION_", env_file=".env", extra="ignore")

    database_url: SecretStr  # required; see .env.example
    host: str = "127.0.0.1"
    port: int = 8080

    # Object storage (S3 API). Executors get presigned URLs built against the public endpoint.
    s3_endpoint_url: str = "http://localhost:9000"
    s3_public_endpoint_url: str | None = None  # defaults to s3_endpoint_url
    s3_access_key: SecretStr  # required; see .env.example
    s3_secret_key: SecretStr  # required; see .env.example
    s3_bucket: str = "minion"
    s3_region: str = "us-east-1"
    s3_input_prefixes: list[str] = ["inputs/"]
    s3_output_prefixes: list[str] = ["outputs/"]
    presign_ttl_s: int = Field(default=900, gt=0, le=3600)

    # Config files and background work.
    models_file: str = "deploy/models.yaml"
    api_keys_file: str | None = None
    splitter_poll_s: float = Field(default=2.0, gt=0)
    executor_token_ttl_days: int = Field(default=90, gt=0)

    @field_validator("s3_input_prefixes", "s3_output_prefixes")
    @classmethod
    def _prefixes(cls, v: list[str]) -> list[str]:
        return _check_prefixes(v)

    @model_validator(mode="after")
    def _public_endpoint_secure(self) -> Self:
        # Presigned URLs carry credentials-derived signatures to executors: require TLS.
        require_secure_url(self.public_s3_endpoint_url)
        return self

    @property
    def public_s3_endpoint_url(self) -> str:
        return self.s3_public_endpoint_url or self.s3_endpoint_url
