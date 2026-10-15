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
