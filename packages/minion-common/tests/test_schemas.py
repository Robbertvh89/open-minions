import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from minion_common.enums import CapacityProfile, Engine, TaskType
from minion_common.schemas import (
    AcquireRequest,
    Assignment,
    Budget,
    CompleteRequest,
    CompletionParams,
    EmbeddingParams,
    ErrorBody,
    FailRequest,
    ModelEntry,
    PreferenceRequest,
    TaskCreate,
    parse_params,
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
    for bad in ("", "X" * 65):
        with pytest.raises(ValidationError):
            FailRequest(retryable=True, code=bad, message="m")
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
            model_id="m",
            type=TaskType.EMBEDDING,
            engine=Engine.MLX,
            ref="o/m",
            revision="a" * 40,
            memory_gb=1,
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


SHA1 = "a" * 40
SHA256 = "b" * 64


def _entry(**kw: Any) -> ModelEntry:
    base: dict[str, Any] = {
        "model_id": "m",
        "type": "embedding",
        "engine": "mlx",
        "ref": "o/m",
        "revision": SHA1,
        "memory_gb": 1,
    }
    return ModelEntry.model_validate(base | kw)


def test_revision_required_per_engine() -> None:
    assert _entry().revision == SHA1
    assert _entry(engine="hf").revision == SHA1
    for engine in ("mlx", "hf"):
        for bad in (None, "main", SHA256, "A" * 40, SHA1[:-1]):
            with pytest.raises(ValidationError, match="revision"):
                _entry(engine=engine, revision=bad)
    with pytest.raises(ValidationError, match="revision"):
        _entry(engine="mlx", revision="sha256:" + SHA256)


def test_ollama_digest_prefix_normalised() -> None:
    for rev in (SHA256, "sha256:" + SHA256):
        assert _entry(engine="ollama", ref="qwen2.5:0.5b", revision=rev).revision == SHA256
    for bad in (None, SHA1, "sha256:" + SHA1):
        with pytest.raises(ValidationError, match="revision"):
            _entry(engine="ollama", ref="qwen2.5:0.5b", revision=bad)


def test_fake_engine_revision_optional() -> None:
    assert _entry(engine="fake", ref="fake", revision=None).revision is None


def test_bad_refs_rejected() -> None:
    for ref in ("noslash", "o/m/x", "../x/y", "o/..", "-o/m", "o/m a"):
        with pytest.raises(ValidationError, match="ref"):
            _entry(ref=ref)
    with pytest.raises(ValidationError, match="ref"):
        _entry(engine="ollama", ref="Bad Tag", revision=SHA256)


def test_ref_and_revision_bypasses_rejected() -> None:
    for ref in ("o/m\n", "\u043e/m", "o/\u043c", "o/.", "./m", "o//m"):
        with pytest.raises(ValidationError, match="ref"):
            _entry(ref=ref)
    for ref in ("qwen\n", "q\u0435n:1b", "a/./b:1", "a//b:1", "a/../b:1", "a:\u0663", "a:."):
        with pytest.raises(ValidationError, match="ref"):
            _entry(engine="ollama", ref=ref, revision=SHA256)
    for rev in (SHA1 + "\n", "\uff10" * 40):
        with pytest.raises(ValidationError, match="revision"):
            _entry(revision=rev)
    with pytest.raises(ValidationError, match="revision"):
        _entry(engine="ollama", ref="qwen2.5:0.5b", revision=SHA256 + "\n")


def test_memory_gb_bounds() -> None:
    for bad in (0, 1025, float("inf"), float("nan")):
        with pytest.raises(ValidationError):
            _entry(memory_gb=bad)


def test_parse_params_defaults_and_valid() -> None:
    c = parse_params(TaskType.COMPLETION, {})
    assert isinstance(c, CompletionParams)
    assert (c.temperature, c.top_p, c.max_tokens, c.seed, c.system) == (0.0, 1.0, 256, None, None)
    e = parse_params(TaskType.EMBEDDING, {"normalize": False})
    assert isinstance(e, EmbeddingParams) and e.normalize is False
    assert parse_params(TaskType.EMBEDDING, {}) == EmbeddingParams()


def test_parse_params_rejects_unknown_and_cross_type() -> None:
    for t, raw in (
        (TaskType.COMPLETION, {"stop": ["x"]}),
        (TaskType.COMPLETION, {"trust_remote_code": True}),
        (TaskType.COMPLETION, {"normalize": True}),
        (TaskType.EMBEDDING, {"temperature": 0.1}),
        (TaskType.EMBEDDING, {"x": 1}),
    ):
        with pytest.raises(ValidationError):
            parse_params(t, raw)


def test_parse_params_bounds() -> None:
    bad: list[dict[str, Any]] = [
        {"temperature": -0.1},
        {"temperature": 2.1},
        {"temperature": float("nan")},
        {"top_p": 0},
        {"top_p": 1.1},
        {"max_tokens": 0},
        {"max_tokens": 4097},
        {"max_tokens": 1.5},
        {"max_tokens": True},
        {"seed": -1},
        {"seed": "1"},
        {"system": "x" * 8001},
    ]
    for raw in bad:
        with pytest.raises(ValidationError):
            parse_params(TaskType.COMPLETION, raw)
    ok = parse_params(
        TaskType.COMPLETION,
        {"temperature": 2, "top_p": 1, "max_tokens": 4096, "seed": 0, "system": "x" * 8000},
    )
    assert isinstance(ok, CompletionParams) and ok.max_tokens == 4096


def test_ollama_ref_requires_tag() -> None:
    for ref in ("qwen2.5", "library/qwen2.5", "user/model"):
        with pytest.raises(ValidationError, match="tag"):
            _entry(engine="ollama", ref=ref, revision=SHA256)
    assert _entry(engine="ollama", ref="user/model:7b", revision=SHA256).ref == "user/model:7b"


def test_ollama_ref_rejects_registry_host() -> None:
    for ref in (
        "hf.co/user/model:q4",
        "registry.example.com/user/model:1",
        "localhost/model:1",
        "localhost:5000/model:1",
        "127.0.0.1/model:1",
        "10.0.0.5:11434/model:1",
        "example.com:443/a/b:1",
        "evilhost/ns/model:7b",
        "a/b/c/d:1",
    ):
        with pytest.raises(ValidationError, match="ref"):
            _entry(engine="ollama", ref=ref, revision=SHA256)


def test_budget_cpu_priority_optional_and_validated() -> None:
    from minion_common.enums import CpuPriority
    from minion_common.schemas import Budget, PresenceRequest

    base = {"profile": "light", "memory_gb": 4, "gpu_duty_cycle": 0.3, "max_batch_size": 4}
    assert Budget.model_validate(base).cpu_priority is None  # older clients
    b = Budget.model_validate({**base, "cpu_priority": "background"})
    assert b.cpu_priority is CpuPriority.BACKGROUND
    assert Budget.model_validate_json(b.model_dump_json()) == b
    with pytest.raises(ValidationError):
        Budget.model_validate({**base, "cpu_priority": "turbo"})
    assert PresenceRequest(budget=b, paused_reason="on_battery").paused_reason == "on_battery"
    for bad in ("Bad", "x" * 65, "a b"):
        with pytest.raises(ValidationError):
            PresenceRequest(budget=b, paused_reason=bad)
    with pytest.raises(ValidationError):
        PresenceRequest.model_validate({"budget": base, "extra": 1})


@pytest.mark.parametrize("code", ["UNAUTHORIZED", "A_1", "X" * 64])
def test_error_body_accepts_server_style_codes(code: str) -> None:
    assert ErrorBody(code=code, message="m").code == code


@pytest.mark.parametrize("code", ["", "lower", "A B", "A\n", "X" * 65, "\x1b[31m", "A-B"])
def test_error_body_rejects_odd_codes(code: str) -> None:
    with pytest.raises(ValidationError):
        ErrorBody(code=code, message="m")


def test_assignment_and_responses_ignore_unknown_fields() -> None:
    from minion_common.schemas import Assignment, HeartbeatResponse, RegisterResponse

    hb = HeartbeatResponse.model_validate(
        {"cancel": False, "output_url": "https://s/o", "expires_at": "2030-01-01T00:00:00Z", "x": 1}
    )
    assert hb.cancel is False
    eid = str(uuid.uuid4())
    assert str(RegisterResponse.model_validate({"executor_id": eid, "new": 1}).executor_id) == eid
    model = {
        "model_id": "fake",
        "type": "embedding",
        "engine": "fake",
        "ref": "fake",
        "memory_gb": 1,
    }
    a = Assignment.model_validate(
        {
            "acquirement_id": eid, "attempt": 1, "team": "t", "project": "p", "task_name": "n",
            "subtask_id": 0, "type": "embedding", "model": model, "input_url": "https://s/i",
            "byte_start": 0, "byte_end": 5, "record_count": 1, "output_url": "https://s/o",
            "expires_at": "2030-01-01T00:00:00Z", "future_field": {"a": 1},
        }
    )  # fmt: skip
    assert a.byte_end == 5 and not hasattr(a, "future_field")


def test_requests_stay_strict() -> None:
    from minion_common.schemas import CompleteRequest

    with pytest.raises(ValidationError):
        bad = {"record_count": 1, "byte_size": 1, "sha256": "0" * 64, "x": 1}
        CompleteRequest.model_validate(bad)


def test_range_header_is_inclusive_end() -> None:
    from minion_common.schemas import range_header

    assert range_header(0, 1) == "bytes=0-0"
    assert range_header(10, 21) == "bytes=10-20"


@pytest.mark.parametrize("start,end", [(5, 5), (6, 5), (-1, 5)])
def test_range_header_rejects_empty_or_inverted(start: int, end: int) -> None:
    from minion_common.schemas import range_header

    with pytest.raises(ValueError):
        range_header(start, end)
