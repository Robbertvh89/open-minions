from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ServerSettings(BaseSettings):
    """Server configuration, read from `MINION_*` environment variables."""

    model_config = SettingsConfigDict(env_prefix="MINION_", env_file=".env", extra="ignore")

    database_url: str  # required; see .env.example
    host: str = "127.0.0.1"
    port: int = 8080

    # Object storage (S3 API). Executors get presigned URLs built against the public endpoint.
    s3_endpoint_url: str = "http://localhost:9000"
    s3_public_endpoint_url: str | None = None  # defaults to s3_endpoint_url
    s3_access_key: str = "minion"
    s3_secret_key: str = "minion-secret"  # noqa: S105 - dev default, matches deploy/compose.yml
    s3_bucket: str = "minion"
    s3_region: str = "us-east-1"
    input_prefix: str = "inputs/"
    output_prefix: str = "outputs/"
    presign_ttl_s: int = Field(default=900, gt=0, le=3600)

    # Config files and background work.
    models_file: str = "deploy/models.yaml"
    api_keys_file: str | None = None
    splitter_poll_s: float = Field(default=2.0, gt=0)
    executor_token_ttl_days: int = Field(default=90, gt=0)

    @property
    def public_s3_endpoint_url(self) -> str:
        return self.s3_public_endpoint_url or self.s3_endpoint_url
