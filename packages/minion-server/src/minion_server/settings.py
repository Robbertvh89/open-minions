from typing import Self

from minion_common.client import DEFAULT_TIMEOUT_S, require_secure_url
from minion_common.schemas import MAX_OUTPUT_BYTES_DEFAULT, MAX_OUTPUT_BYTES_LIMIT
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
    # Overload (TASK-7.4.4, ADR-0013): a request waits at most db_pool_timeout_s for a connection
    # (then 503 SERVER_BUSY, never 500). An acquire that has used up acquire_deadline_s before its
    # claim INSERT answers 503 ACQUIRE_DEADLINE with nothing committed, so a client that timed out
    # (executor HTTP timeout 30 s) is not left with a ghost acquirement. At most acquire_concurrency
    # acquires run per process (default pool + overflow - 4, min 1); the rest get 503 SERVER_BUSY
    # and the remaining connections stay free for heartbeat / complete / release.
    db_pool_timeout_s: float = Field(default=5.0, ge=0.5, le=30.0)
    acquire_deadline_s: float = Field(default=10.0, ge=1.0, le=20.0)
    acquire_concurrency: int | None = Field(default=None, ge=1)
    # Executor token cache + auth lane (TASK-32, ADR-0018). A revoked/rotated token keeps working
    # for at most executor_auth_cache_ttl_s per worker (0 = revalidate every request); expiry is
    # checked on every hit. Unknown tokens are looked up on a separate pool of db_auth_pool_size
    # connections (no overflow); no connection within auth_lookup_timeout_s -> 503 SERVER_BUSY.
    executor_auth_cache_ttl_s: float = Field(default=10.0, ge=0, le=60)
    executor_auth_cache_max_entries: int = Field(default=50_000, gt=0)
    db_auth_pool_size: int = Field(default=2, ge=1, le=8)
    auth_lookup_timeout_s: float = Field(default=1.0, ge=0.1, le=5.0)
    max_request_body_bytes: int = Field(default=1 << 20, gt=0)  # 413 above this

    # Multi-worker (TASK-7.4.6): `workers` uvicorn processes, each with its own pool, acquire
    # semaphore, per-executor in-flight cap, rate limiter and splitter loop (ARCHITECTURE §6).
    # db_max_connections is the Postgres max_connections the budget check compares against
    # (startup warning only; PG default 100). The watchdog pool is added to the total.
    server_workers: int = Field(default=1, ge=1, le=32)
    db_max_connections: int = Field(default=100, ge=1)
    # Connections kept for others: superuser_reserved_connections (3) + Grafana (4).
    db_reserved_connections: int = Field(default=7, ge=0)
    # Same MINION_ var the watchdog reads (ge=1 like there); set it on the server service too,
    # otherwise the budget check counts the default.
    watchdog_db_pool_size: int = Field(default=2, ge=1)
    # Prometheus multiprocess directory, used only when workers > 1 and metrics are enabled.
    # Unset (default): the parent creates a fresh private 0700 dir (mkdtemp) and removes it on
    # exit. Set: must be an existing real directory (no symlink) owned by the server user with no
    # group/other access, else the server exits 2; only its counter_/histogram_/gauge_ *.db files
    # are deleted at startup.
    metrics_multiproc_dir: str | None = None

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
    # An attempt object above this is never promoted (422 OUTPUT_TOO_LARGE on complete, S-2).
    max_output_bytes: int = Field(default=MAX_OUTPUT_BYTES_DEFAULT, gt=0, le=MAX_OUTPUT_BYTES_LIMIT)

    # uvicorn access log (off by default): lines carry client IP, method, path (ids) and status;
    # query strings are stripped by logging_config.
    server_access_log: bool = False

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

    # Acquire claim window (ADR-0013, FR-22): the claim picks at random among the top N candidates
    # of the highest priority present. 1 = strict FR-22 order (head contention returns).
    acquire_window: int = Field(default=16, ge=1, le=256)

    # Open acquirements one executor may hold (TASK-33, ADR-0019); above it acquire answers
    # 429 TOO_MANY_ACQUIREMENTS. Per executor, enforced in the DB so it holds across workers.
    max_open_acquirements: int = Field(default=4, ge=1, le=64)

    # Live (SPLITTING/PENDING/RUNNING) tasks one team may hold (TASK-30, FR-9, ADR-0020); above it
    # create answers 429 TOO_MANY_LIVE_TASKS. A team can override it in the keys file.
    max_live_tasks_per_team: int = Field(default=500, ge=1, le=10_000)

    # Retry budget guard: RELEASED attempts per subtask before a release counts as a FAILED one.
    # Upper bound 100; the watchdog reads the same MINION_RELEASE_LIMIT and must use the same value.
    release_limit: int = Field(default=10, ge=0, le=100)

    # Minimum seconds between heartbeats of one acquirement; faster beats get 429 (guards write
    # amplification: a rogue executor cannot hammer the heartbeat row). 0 disables. Executors
    # beat every ~30 s, so 1 s leaves ample headroom.
    heartbeat_min_interval_s: float = Field(default=1.0, ge=0)
    # Minimum seconds between calls of one executor to register / acquire / presence; faster calls
    # get 429 TOO_MANY_REQUESTS + Retry-After before any DB work (NF-5). In-memory, per replica
    # (soft state, plan D4); 0 disables. Healthy executors acquire >= ~5 s apart when idle, so
    # the defaults leave headroom; the executor paces busy-loop acquires >= 0.25 s apart.
    register_min_interval_s: float = Field(default=5.0, ge=0)
    acquire_min_interval_s: float = Field(default=0.2, ge=0)
    presence_min_interval_s: float = Field(default=5.0, ge=0)
    preference_min_interval_s: float = Field(default=1.0, ge=0)
    # Limiter LRU bound: 10k executors x 4 limited ops; eviction only admits one extra call.
    rate_limit_max_entries: int = Field(default=40_000, gt=0)
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
    def _overload_limits(self) -> Self:
        if self.acquire_deadline_s >= DEFAULT_TIMEOUT_S:
            raise ValueError(
                f"acquire_deadline_s must be below the executor HTTP timeout ({DEFAULT_TIMEOUT_S} s)"  # noqa: E501
            )
        # worst case a request waits db_pool_timeout_s for a connection that arrives right at the
        # deadline: both must fit in the client's timeout with a margin for the response
        if self.db_pool_timeout_s + self.acquire_deadline_s > DEFAULT_TIMEOUT_S - 5:
            raise ValueError(
                "db_pool_timeout_s + acquire_deadline_s must be at most "
                f"{DEFAULT_TIMEOUT_S - 5:g} s (executor HTTP timeout {DEFAULT_TIMEOUT_S:g} s - 5 s)"
            )
        cap = self.db_pool_size + self.db_max_overflow
        if self.acquire_concurrency is not None and self.acquire_concurrency > cap:
            raise ValueError(f"acquire_concurrency must be <= pool_size + max_overflow ({cap})")
        return self

    @property
    def acquire_concurrency_limit(self) -> int:
        """Concurrent acquires per process: leave 4 connections for owner operations."""
        if self.acquire_concurrency is not None:
            return self.acquire_concurrency
        return max(1, self.db_pool_size + self.db_max_overflow - 4)

    @property
    def connection_limit(self) -> int:
        """Connections available to this deployment: max_connections minus the reserved ones."""
        return self.db_max_connections - self.db_reserved_connections

    @property
    def connection_budget(self) -> int:
        """Worst-case DB connections: every worker's full pool plus its auth pool, plus the
        watchdog's pool."""
        per_worker = self.db_pool_size + self.db_max_overflow + self.db_auth_pool_size
        return self.server_workers * per_worker + self.watchdog_db_pool_size

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
