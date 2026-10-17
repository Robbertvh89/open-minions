"""Unit tests (no DB) for the web UI form parsing and fleet summary shaping (TASK-6.7)."""

import pytest
from fastapi import HTTPException
from minion_common.schemas import Budget
from minion_server.api.fleet import FleetSummary, _budget, shape_summary
from minion_server.repositories.executors import FleetGroup, fleet_summary_stmt
from minion_server.ui.routes import _build_create, _message
from sqlalchemy.dialects import postgresql


def _values(**kw: str) -> dict[str, str]:
    base = {
        "team": "t", "project": "p", "task_name": "n", "type": "embedding", "model": "m",
        "input_uri": "s3://b/in", "output_uri": "s3://b/out", "params": "",
        "records_per_subtask": "", "priority": "", "allowed_pool": "",
    }  # fmt: skip
    return base | kw


def _code(exc: HTTPException) -> str:
    assert isinstance(exc.detail, dict)
    return str(exc.detail["code"])


def test_build_create_coerces_strings_and_defaults_empty_optionals() -> None:
    body = _build_create(_values(records_per_subtask=" 250 ", priority="-3", allowed_pool=" gpu "))
    assert (body.records_per_subtask, body.priority, body.allowed_pool) == (250, -3, "gpu")
    assert body.params == {}
    dflt = _build_create(_values())
    assert (dflt.records_per_subtask, dflt.priority, dflt.allowed_pool) == (500, 0, None)


def test_build_create_params_must_be_a_json_object() -> None:
    assert _build_create(_values(params='{"a": 1}')).params == {"a": 1}
    for bad in ("[1]", '"s"', "3", "{not json"):
        with pytest.raises(HTTPException) as ei:
            _build_create(_values(params=bad))
        assert ei.value.status_code == 422 and _code(ei.value) == "INVALID_PARAMS"


def test_build_create_deep_nesting_is_invalid_params_not_a_crash() -> None:
    with pytest.raises(HTTPException) as ei:
        _build_create(_values(params="[" * 100_000 + "]" * 100_000))
    assert ei.value.status_code == 422 and _code(ei.value) == "INVALID_PARAMS"


def test_build_create_validation_error_names_fields_not_values() -> None:
    with pytest.raises(HTTPException) as ei:
        _build_create(_values(records_per_subtask="secret-value"))
    assert _code(ei.value) == "INVALID_REQUEST"
    assert "records_per_subtask" in _message(ei.value) and "secret-value" not in _message(ei.value)


def test_message_formats_code_and_falls_back() -> None:
    assert _message(HTTPException(422, {"code": "X", "message": "m"})) == "X: m"
    assert _message(HTTPException(422, {"message": "m"})) == "m"
    assert _message(HTTPException(500, "plain")) == "Request failed."
    assert _message(HTTPException(500, {"code": "X", "message": 3})) == "Request failed."


def test_budget_valid_empty_and_wrong_type() -> None:
    good = {"profile": "balanced", "memory_gb": 8, "gpu_duty_cycle": 0.5, "max_batch_size": 8}
    b = _budget(good)
    assert isinstance(b, Budget) and b.memory_gb == 8
    assert _budget(None) is None
    assert _budget({}) is None
    assert _budget({**good, "memory_gb": "lots"}) is None
    assert _budget({**good, "memory_gb": -1}) is None


def test_shape_summary_counts_online_only_and_totals_everyone() -> None:
    g = [
        FleetGroup(True, "balanced", None, 2, 16.0),
        FleetGroup(True, "balanced", "thermal", 1, 8.0),
        FleetGroup(True, None, "battery", 1, 0.0),
        FleetGroup(False, "full", "thermal", 5, 100.0),
    ]
    assert shape_summary(g) == FleetSummary(
        profiles=[("balanced", 3, 24.0), ("unknown", 1, 0.0)],
        paused=[("battery", 1), ("thermal", 1)],
        online=4,
        total=9,
    )
    assert shape_summary([]) == FleetSummary([], [], 0, 0)


def test_fleet_summary_sql_groups_and_guards_the_cast() -> None:
    sql = str(
        fleet_summary_stmt(120, 600).compile(dialect=postgresql.dialect())  # type: ignore[no-untyped-call]
    ).lower()
    assert "group by" in sql and "count(*)" in sql
    assert "jsonb_typeof" in sql and "numeric" in sql
    assert "make_interval" in sql  # same online expression as list_fleet


async def test_deeply_nested_rest_body_is_a_client_error_not_500() -> None:
    import httpx
    from minion_server.app import create_app

    transport = httpx.ASGITransport(app=create_app(), raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.post(
            "/api/v1/tasks",
            content="[" * 100_000 + "]" * 100_000,
            headers={"content-type": "application/json"},
        )
    # FastAPI maps any body-parse failure (incl. RecursionError) to 400, never 500.
    assert r.status_code == 400
