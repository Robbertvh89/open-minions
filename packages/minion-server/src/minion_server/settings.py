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
    # Sizing: pool_size + max_overflow bounds concurrent DB connections per server process.
    db_pool_size: int = Field(default=10, ge=1)
    db_max_overflow: int = Field(default=10, ge=0)
    max_request_body_bytes: int = Field(default=1 << 20, gt=0)  # 413 above this

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

    # Prometheus /metrics (FR-41): off by default. The same port serves it, so with a token set
    # only `Authorization: Bearer <token>` scrapes it (>= 32 chars; empty = unset); without one,
    # restrict it at the proxy (a warning is logged at startup when the host is not loopback).
    metrics_enabled: bool = False
    metrics_token: SecretStr | None = None

    # Web UI (ADR-0010): off (every /ui route is 404) until a session secret is set. The secret
    # signs the stateless `minion_session` cookie; rotating it logs everyone out.
    ui_session_secret: SecretStr | None = None
    ui_session_ttl_s: int = Field(default=8 * 3600, ge=60, le=7 * 24 * 3600)
    # Optional comma list of host[:port] the UI may be served under; others get 400 (ADR-0010).
    ui_allowed_hosts: str = ""

    # Config files and background work.
    models_file: str = "deploy/models.yaml"
    api_keys_file: str | None = None
    api_keys_reload_s: float = Field(default=5.0, ge=0)  # keys file change check; 0 = every request
    splitter_poll_s: float = Field(default=2.0, gt=0)
    executor_token_ttl_days: int = Field(default=90, gt=0)

    # Retry budget guard: RELEASED attempts per subtask before a release counts as a FAILED one.
    release_limit: int = Field(default=10, ge=0)

    # Minimum seconds between heartbeats of one acquirement; faster beats get 429 (guards write
    # amplification: a rogue executor cannot hammer the heartbeat row). 0 disables. Executors
    # beat every ~30 s, so 1 s leaves ample headroom.
    heartbeat_min_interval_s: float = Field(default=1.0, ge=0)
    # Fleet view (ARCHITECTURE §7): online = heard from within this window.
    executor_online_idle_s: float = Field(default=120.0, gt=0)  # acquire poll / presence ping
    executor_online_busy_s: float = Field(default=600.0, gt=0)  # running: heartbeat

    # Splitter bounds (memory/DoS protection) and retry policy.
    split_max_line_bytes: int = Field(default=16 << 20, gt=0)  # LINE_TOO_LONG above this
    split_max_input_bytes: int = Field(default=50 << 30, gt=0)  # total per task
    split_max_objects: int = Field(default=10_000, gt=0)  # input objects per task
    split_max_subtasks: int = Field(default=100_000, gt=0)  # subtasks per task
    split_max_attempts: int = Field(default=5, gt=0)  # transient failures before FAILED
    split_backoff_base_s: float = Field(default=2.0, ge=0)  # doubles per attempt
    split_backoff_max_s: float = Field(default=300.0, ge=0)
    split_lease_s: float = Field(default=900.0, gt=0)  # claim lease; crashed splits retry after
    split_max_seconds: float = Field(default=3600.0, gt=0)  # wall-clock budget per split attempt

    @field_validator("metrics_token", mode="before")
    @classmethod
    def _metrics_token(cls, v: object) -> object:
        """Empty means unset (a blank `MINION_METRICS_TOKEN=` line); a set token must be strong."""
        raw = v.get_secret_value() if isinstance(v, SecretStr) else v
        if isinstance(raw, str):
            raw = raw.strip()  # a trailing newline/space must neither count nor break the match
        if raw is None or raw == "":
            return None
        if isinstance(raw, str) and len(raw) < 32:
            raise ValueError("metrics_token must be at least 32 characters (or unset)")
        return raw

    @field_validator("ui_session_secret", mode="before")
    @classmethod
    def _ui_session_secret(cls, v: object) -> object:
        """Empty means unset (blank env line); a set secret must be >= 32 bytes."""
        raw = v.get_secret_value() if isinstance(v, SecretStr) else v
        if isinstance(raw, str):
            raw = raw.strip()
        if raw is None or raw == "":
            return None
        if isinstance(raw, str) and len(raw.encode("utf-8")) < 32:
            raise ValueError("ui_session_secret must be at least 32 bytes (or unset)")
        if isinstance(raw, str) and len(set(raw)) < 2:
            raise ValueError("ui_session_secret must not be a single repeated character")
        return raw

    @field_validator("ui_allowed_hosts")
    @classmethod
    def _ui_allowed_hosts(cls, v: str) -> str:
        """Bare `host[:port]` items only: no scheme, path, userinfo, whitespace, empty items."""
        if not v.strip():
            return ""
        for item in v.split(","):
            entry = item.strip()
            if not entry:
                raise ValueError("ui_allowed_hosts has an empty item (stray comma)")
            if "://" in entry or any(c in entry for c in "/@") or any(c.isspace() for c in entry):
                raise ValueError(
                    f"ui_allowed_hosts entry {entry!r} must be a bare host[:port] "
                    "(no scheme, '/', '@' or whitespace)"
                )
        return v

    @property
    def ui_allowed_host_set(self) -> frozenset[str]:
        return frozenset(h.strip().lower() for h in self.ui_allowed_hosts.split(",") if h.strip())

    @field_validator("s3_input_prefixes", "s3_output_prefixes")
    @classmethod
    def _prefixes(cls, v: list[str]) -> list[str]:
        return _check_prefixes(v)

    @model_validator(mode="after")
    def _prefixes_disjoint(self) -> Self:
        # Outputs written under an input prefix would be re-split as input (and vice versa).
        for i in self.s3_input_prefixes:
            for o in self.s3_output_prefixes:
                if i.startswith(o) or o.startswith(i):
                    raise ValueError(f"input prefix {i!r} and output prefix {o!r} overlap")
        return self

    @model_validator(mode="after")
    def _public_endpoint_secure(self) -> Self:
        # Presigned URLs carry credentials-derived signatures to executors: require TLS.
        require_secure_url(self.public_s3_endpoint_url)
        return self

    @property
    def public_s3_endpoint_url(self) -> str:
        return self.s3_public_endpoint_url or self.s3_endpoint_url
