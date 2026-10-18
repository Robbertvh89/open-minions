import pytest
from minion_server.settings import ServerSettings
from pydantic import ValidationError


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_DATABASE_URL", "postgresql+asyncpg://x@h/db")
    monkeypatch.setenv("MINION_S3_ACCESS_KEY", "ak")
    monkeypatch.setenv("MINION_S3_SECRET_KEY", "sk")


def test_public_endpoint_defaults_to_internal(monkeypatch: pytest.MonkeyPatch) -> None:
    s = ServerSettings(_env_file=None)
    assert s.public_s3_endpoint_url == s.s3_endpoint_url
    monkeypatch.setenv("MINION_S3_PUBLIC_ENDPOINT_URL", "https://s3.example.com")
    assert ServerSettings(_env_file=None).public_s3_endpoint_url == "https://s3.example.com"


def test_public_endpoint_must_be_https(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_S3_PUBLIC_ENDPOINT_URL", "http://s3.example.com")
    with pytest.raises(ValidationError):
        ServerSettings(_env_file=None)
    monkeypatch.delenv("MINION_S3_PUBLIC_ENDPOINT_URL")
    monkeypatch.setenv("MINION_S3_ENDPOINT_URL", "http://s3.internal:9000")
    with pytest.raises(ValidationError):  # effective public endpoint falls back to internal
        ServerSettings(_env_file=None)


def test_presign_ttl_capped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_PRESIGN_TTL_S", "3601")
    with pytest.raises(ValidationError):
        ServerSettings(_env_file=None)


def test_s3_credentials_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MINION_S3_SECRET_KEY")
    with pytest.raises(ValidationError):
        ServerSettings(_env_file=None)


def test_secrets_not_in_repr() -> None:
    s = ServerSettings(_env_file=None)
    assert "postgresql+asyncpg://x@h/db" not in repr(s)
    assert "'sk'" not in repr(s) and "'ak'" not in repr(s)


@pytest.mark.parametrize("value", ['["inputs"]', '[""]', "[]"])
def test_prefixes_validated(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("MINION_S3_INPUT_PREFIXES", value)
    with pytest.raises(ValidationError):
        ServerSettings(_env_file=None)


def test_prefixes_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_S3_OUTPUT_PREFIXES", '["o1/", "o2/"]')
    assert ServerSettings(_env_file=None).s3_output_prefixes == ["o1/", "o2/"]


def test_pool_defaults() -> None:
    s = ServerSettings(_env_file=None)
    assert (s.db_pool_size, s.db_max_overflow) == (10, 10)


@pytest.mark.parametrize(
    ("inputs", "outputs"),
    [
        ('["data/"]', '["data/out/"]'),  # output inside input
        ('["data/in/"]', '["data/"]'),  # input inside output
        ('["a/"]', '["a/"]'),  # identical
    ],
)
def test_overlapping_prefixes_refused(
    monkeypatch: pytest.MonkeyPatch, inputs: str, outputs: str
) -> None:
    monkeypatch.setenv("MINION_S3_INPUT_PREFIXES", inputs)
    monkeypatch.setenv("MINION_S3_OUTPUT_PREFIXES", outputs)
    with pytest.raises(ValidationError, match="overlap"):
        ServerSettings(_env_file=None)


def test_max_output_bytes_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_MAX_OUTPUT_BYTES", str(5 << 30))
    assert ServerSettings(_env_file=None).max_output_bytes == 5 << 30
    for bad in ("0", str((5 << 30) + 1)):
        monkeypatch.setenv("MINION_MAX_OUTPUT_BYTES", bad)
        with pytest.raises(ValidationError):
            ServerSettings(_env_file=None)


def test_overload_defaults_and_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    s = ServerSettings(_env_file=None)
    assert (s.db_pool_timeout_s, s.acquire_deadline_s) == (5.0, 10.0)
    assert s.acquire_concurrency_limit == max(1, s.db_pool_size + s.db_max_overflow - 4)
    for name, bad in [
        ("DB_POOL_TIMEOUT_S", "0.1"),
        ("DB_POOL_TIMEOUT_S", "31"),
        ("ACQUIRE_DEADLINE_S", "0.5"),
        ("ACQUIRE_DEADLINE_S", "30"),  # not below the executor's 30 s HTTP timeout
        ("ACQUIRE_CONCURRENCY", "0"),
        ("ACQUIRE_CONCURRENCY", "999"),  # above pool_size + max_overflow
    ]:
        monkeypatch.setenv(f"MINION_{name}", bad)
        with pytest.raises(ValidationError):
            ServerSettings(_env_file=None)
        monkeypatch.delenv(f"MINION_{name}")


def test_acquire_concurrency_default_floors_at_one(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_DB_POOL_SIZE", "1")
    monkeypatch.setenv("MINION_DB_MAX_OVERFLOW", "0")
    assert ServerSettings(_env_file=None).acquire_concurrency_limit == 1


def test_pool_timeout_plus_deadline_must_fit_the_executor_http_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MINION_DB_POOL_TIMEOUT_S", "10")
    monkeypatch.setenv("MINION_ACQUIRE_DEADLINE_S", "15")  # 25 s: fits with the 5 s margin
    ServerSettings(_env_file=None)
    monkeypatch.setenv("MINION_ACQUIRE_DEADLINE_S", "16")  # 26 s: a client would time out first
    with pytest.raises(ValidationError):
        ServerSettings(_env_file=None)
