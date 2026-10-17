"""Submitter settings from `MINION_CLI_*` environment variables (no env file)."""

from __future__ import annotations

from minion_common.client import require_secure_url
from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class CliSettings(BaseSettings):
    # No env_file: S3 credentials must come from the real environment or the boto default chain.
    model_config = SettingsConfigDict(env_prefix="MINION_CLI_", extra="ignore")

    server: str | None = None
    team: str | None = None
    project: str = "default"

    s3_endpoint_url: str | None = None
    s3_bucket: str = "minion"
    s3_region: str = "us-east-1"
    # Unset: the boto3 default credential chain. Scope them to inputs/{team}/ and
    # outputs/{team}/ (S-14).
    s3_access_key: SecretStr | None = None
    s3_secret_key: SecretStr | None = None
    input_prefix: str = Field(default="inputs/", pattern=r"^([A-Za-z0-9._-]+/)*$")
    output_prefix: str = Field(default="outputs/", pattern=r"^([A-Za-z0-9._-]+/)*$")

    @field_validator("server", "s3_endpoint_url")
    @classmethod
    def _secure(cls, value: str | None) -> str | None:
        return require_secure_url(value) if value else None

    @field_validator("server", "team", "s3_endpoint_url", mode="before")
    @classmethod
    def _empty_is_unset(cls, value: object) -> object:
        return None if value == "" else value
