import pytest
from minion_server.settings import ServerSettings
from pydantic import ValidationError


def test_public_endpoint_defaults_to_internal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_DATABASE_URL", "postgresql+asyncpg://x@h/db")
    s = ServerSettings(_env_file=None)
    assert s.public_s3_endpoint_url == s.s3_endpoint_url
    monkeypatch.setenv("MINION_S3_PUBLIC_ENDPOINT_URL", "https://s3.example.com")
    assert ServerSettings(_env_file=None).public_s3_endpoint_url == "https://s3.example.com"


def test_presign_ttl_capped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_DATABASE_URL", "postgresql+asyncpg://x@h/db")
    monkeypatch.setenv("MINION_PRESIGN_TTL_S", "3601")
    with pytest.raises(ValidationError):
        ServerSettings(_env_file=None)
