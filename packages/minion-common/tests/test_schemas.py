import uuid
from datetime import UTC, datetime

import pytest
from minion_common.enums import CapacityProfile, Engine, TaskType
from minion_common.schemas import (
    AcquireRequest,
    Assignment,
    Budget,
    CompleteRequest,
    FailRequest,
    ModelEntry,
    PreferenceRequest,
    TaskCreate,
)
from pydantic import ValidationError

BUDGET = Budget(profile=CapacityProfile.BALANCED, memory_gb=8, gpu_duty_cycle=0.5, max_batch_size=8)


def _task(**kw: object) -> TaskCreate:
    base: dict[str, object] = {
        "team": "ml",
        "project": "p1",
        "task_name": "emb-1",
        "type": "embedding",
        "model": "fake-embed",
        "input_uri": "s3://b/inputs/a/",
        "output_uri": "s3://b/outputs/a/",
    }
    base.update(kw)
    return TaskCreate.model_validate(base)


def test_task_defaults() -> None:
    t = _task()
    assert t.records_per_subtask == 500 and t.priority == 0 and t.allowed_pool is None


@pytest.mark.parametrize("bad", ["Has Upper", "-lead", "trail-", "a/b", "", "x" * 70])
def test_task_name_slug_rejected(bad: str) -> None:
    with pytest.raises(ValidationError):
        _task(task_name=bad)


@pytest.mark.parametrize("n", [0, -1, 100_001])
def test_records_per_subtask_bounds(n: int) -> None:
    with pytest.raises(ValidationError):
        _task(records_per_subtask=n)


def test_records_per_subtask_edges() -> None:
    assert _task(records_per_subtask=1).records_per_subtask == 1
    assert _task(records_per_subtask=100_000).records_per_subtask == 100_000


def test_extra_fields_forbidden() -> None:
    with pytest.raises(ValidationError):
        _task(bogus=1)


def test_budget_bounds() -> None:
    with pytest.raises(ValidationError):
        Budget(profile=CapacityProfile.FULL, memory_gb=0, gpu_duty_cycle=1, max_batch_size=1)
    with pytest.raises(ValidationError):
        Budget(profile=CapacityProfile.FULL, memory_gb=1, gpu_duty_cycle=1.1, max_batch_size=1)
    with pytest.raises(ValidationError):
        Budget(profile=CapacityProfile.FULL, memory_gb=1, gpu_duty_cycle=1, max_batch_size=0)


def test_acquire_requires_engines() -> None:
    with pytest.raises(ValidationError):
        AcquireRequest(engines=[], budget=BUDGET)
    assert AcquireRequest(engines=[Engine.MLX], budget=BUDGET).engines == [Engine.MLX]


def test_preference_all_none_and_slug() -> None:
    assert PreferenceRequest().team is None
    with pytest.raises(ValidationError):
        PreferenceRequest(team="Bad Team")


def test_fail_message_limit() -> None:
    FailRequest(retryable=True, code="OOM", message="x" * 1000)
    with pytest.raises(ValidationError):
        FailRequest(retryable=True, code="OOM", message="x" * 1001)


def test_complete_sha256_format() -> None:
    CompleteRequest(record_count=1, byte_size=1, sha256="a" * 64)
    with pytest.raises(ValidationError):
        CompleteRequest(record_count=1, byte_size=1, sha256="xyz")


def test_assignment_roundtrip() -> None:
    a = Assignment(
        acquirement_id=uuid.uuid4(),
        attempt=1,
        team="ml",
        project="p",
        task_name="t",
        subtask_id=0,
        type=TaskType.EMBEDDING,
        model=ModelEntry(
            model_id="m", type=TaskType.EMBEDDING, engine=Engine.MLX, ref="r", memory_gb=1
        ),
        input_url="http://x",
        byte_start=0,
        byte_end=10,
        record_count=2,
        output_url="http://y",
        expires_at=datetime.now(UTC),
    )
    assert Assignment.model_validate_json(a.model_dump_json()) == a
    for end in (0, 10):  # byte_end must exceed byte_start
        with pytest.raises(ValidationError):
            Assignment.model_validate({**a.model_dump(), "byte_start": 10, "byte_end": end})


def test_allowed_pool_and_priority_bounds() -> None:
    assert _task(allowed_pool="x" * 64).allowed_pool == "x" * 64
    with pytest.raises(ValidationError):
        _task(allowed_pool="x" * 65)
    for bad in (-1001, 1001):
        with pytest.raises(ValidationError):
            _task(priority=bad)


def test_engines_list_is_bounded() -> None:
    import pytest
    from minion_common.schemas import AcquireRequest
    from pydantic import ValidationError

    budget = {"profile": "balanced", "memory_gb": 8, "gpu_duty_cycle": 0.5, "max_batch_size": 8}
    AcquireRequest.model_validate({"engines": ["mlx"] * 16, "budget": budget})
    with pytest.raises(ValidationError):
        AcquireRequest.model_validate({"engines": ["mlx"] * 17, "budget": budget})


def test_heartbeat_request_validation() -> None:
    from minion_common.schemas import HeartbeatRequest, Progress

    assert HeartbeatRequest().progress is None
    HeartbeatRequest(progress=Progress(records_done=0, records_total=5), paused_reason="on_battery")
    for bad in ({"paused_reason": "Bad Reason"}, {"paused_reason": "a" * 65}, {"extra": 1}):
        with pytest.raises(ValidationError):
            HeartbeatRequest.model_validate(bad)
    with pytest.raises(ValidationError):
        Progress(records_done=-1, records_total=0)


def test_budget_and_progress_hardening() -> None:
    from minion_common.schemas import Progress

    for bad in (float("inf"), float("nan"), 1025.0):
        with pytest.raises(ValidationError):
            Budget(profile=CapacityProfile.FULL, memory_gb=bad, gpu_duty_cycle=1, max_batch_size=1)
    with pytest.raises(ValidationError):
        Budget(
            profile=CapacityProfile.FULL, memory_gb=1, gpu_duty_cycle=float("nan"), max_batch_size=1
        )
    with pytest.raises(ValidationError):
        Progress(records_done=0, records_total=2**31)
    with pytest.raises(ValidationError):
        Progress(records_done=3, records_total=2)
    Progress(records_done=2, records_total=2)
