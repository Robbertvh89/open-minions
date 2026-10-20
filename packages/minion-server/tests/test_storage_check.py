import pytest
from minion_server import storage_check
from minion_server.admin import app
from minion_server.settings import ServerSettings
from minion_server.storage import S3Storage
from minion_server.storage_check import PROBE_BODY, SLICE_BODY, run_check
from pydantic import SecretStr
from typer.testing import CliRunner

from conftest import S3Env


def _enforcing(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
    return (206, SLICE_BODY) if headers.get("Range") == "bytes=4-9" else (403, b"")


def _lax(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
    """Ignores the signed headers: serves whatever Range is asked, the whole body without one."""
    return (200, PROBE_BODY) if "Range" not in headers else (206, SLICE_BODY)


def _widening(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
    if "If-Range" in headers:
        return 200, PROBE_BODY
    return _enforcing(url, headers)


class FakeStorage(S3Storage):
    def __init__(self) -> None:
        self.puts: list[str] = []
        self.deletes: list[str] = []
        self.fail_put = False

    async def put(self, key: str, data: bytes) -> None:
        if self.fail_put:
            raise OSError("boom")
        self.puts.append(key)

    async def presign_get(
        self, key: str, ttl_s: int | None = None, byte_range: tuple[int, int] | None = None
    ) -> str:
        assert byte_range == (4, 10)
        return "http://s3.invalid/b/k?X-Amz-Signature=secret"

    async def delete(self, key: str) -> None:
        self.deletes.append(key)


async def test_enforcing_backend_passes_and_probe_deleted() -> None:
    st = FakeStorage()
    checks = await run_check(st, _enforcing)
    assert all(c.ok for c in checks) and len(checks) == 4
    assert st.puts == st.deletes and st.puts[0].startswith(storage_check.PROBE_PREFIX)


async def test_non_enforcing_backend_fails_with_observed_tuples() -> None:
    checks = await run_check(FakeStorage(), _lax)
    by = {c.name: c for c in checks}
    assert by["signed Range"].ok
    assert by["no Range"].observed == (200, len(PROBE_BODY)) and not by["no Range"].ok
    assert not by["different Range"].ok


async def test_if_range_widening_fails() -> None:
    checks = await run_check(FakeStorage(), _widening)
    assert [c.ok for c in checks] == [True, True, True, False]


async def test_probe_deleted_when_fetch_raises() -> None:
    def boom(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
        raise ConnectionError

    st = FakeStorage()
    with pytest.raises(ConnectionError):
        await run_check(st, boom)
    assert len(st.deletes) == 1


def _patch(monkeypatch: pytest.MonkeyPatch, fetch: storage_check.Fetch) -> FakeStorage:
    st = FakeStorage()
    monkeypatch.setattr("minion_server.storage.S3Storage", lambda settings: st)
    orig = storage_check.run_check

    async def run(storage: S3Storage, f: storage_check.Fetch = fetch) -> list[storage_check.Check]:
        return await orig(storage, f)

    monkeypatch.setattr(storage_check, "run_check", run)
    return st


def test_cli_exit_codes_and_no_secret_output(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_DATABASE_URL", "postgresql+asyncpg://x@h/db")
    monkeypatch.setenv("MINION_S3_ACCESS_KEY", "AKSECRET")
    monkeypatch.setenv("MINION_S3_SECRET_KEY", "SKSECRET")
    _patch(monkeypatch, _enforcing)
    ok = CliRunner().invoke(app, ["check-storage"])
    assert ok.exit_code == 0, ok.output
    _patch(monkeypatch, _lax)
    bad = CliRunner().invoke(app, ["check-storage"])
    assert bad.exit_code == 1
    assert "(200, 16)" in bad.output
    for out in (ok.output, bad.output):
        assert "secret" not in out.lower() and "http" not in out and "X-Amz" not in out


def test_cli_connectivity_error_exits_2(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINION_DATABASE_URL", "postgresql+asyncpg://x@h/db")
    monkeypatch.setenv("MINION_S3_ACCESS_KEY", "ak")
    monkeypatch.setenv("MINION_S3_SECRET_KEY", "sk")
    st = _patch(monkeypatch, _enforcing)
    st.fail_put = True
    assert CliRunner().invoke(app, ["check-storage"]).exit_code == 2


def test_cli_config_error_exits_2(monkeypatch: pytest.MonkeyPatch) -> None:
    for k in ("MINION_DATABASE_URL", "MINION_S3_ACCESS_KEY", "MINION_S3_SECRET_KEY"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.chdir("/")
    assert CliRunner().invoke(app, ["check-storage"]).exit_code == 2


@pytest.mark.integration
async def test_seaweedfs_observed_behaviour(s3: S3Env) -> None:
    """Records what the dev-stack SeaweedFS does; the assertions pin the observed behaviour."""
    cfg = ServerSettings(
        database_url=SecretStr("postgresql+asyncpg://x@h/db"),
        s3_endpoint_url=s3.endpoint_url,
        s3_access_key=SecretStr(s3.access_key),
        s3_secret_key=SecretStr(s3.secret_key),
        s3_bucket=s3.bucket,
        s3_region=s3.region,
    )
    checks = await run_check(S3Storage(cfg))
    print({c.name: (c.observed, c.ok) for c in checks})
    leftovers = s3.client.list_objects_v2(Bucket=s3.bucket, Prefix=storage_check.PROBE_PREFIX)
    assert not leftovers.get("Contents")
    # SeaweedFS (dev stack) enforces signed Range (observed): 206 slice / 403 / 403 / 206 slice
    assert {c.name: c.observed for c in checks} == {
        "signed Range": (206, 6),
        "no Range": (403, 0),
        "different Range": (403, 0),
        "If-Range does not widen": (206, 6),
    }
    assert all(c.ok for c in checks)
