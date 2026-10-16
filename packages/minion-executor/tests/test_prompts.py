"""Pure prompt/record helpers (no engine, runs in CI)."""

from __future__ import annotations

from typing import Any

import pytest
from minion_common.schemas import CompletionParams, EmbeddingParams
from minion_executor.runners.base import RunnerError
from minion_executor.runners.prompts import (
    BAD_INPUT,
    MAX_MESSAGES,
    build_messages,
    embedding_text,
    validate_messages,
)

P = CompletionParams()


def _bad(rec: Any, params: Any = P) -> None:
    with pytest.raises(RunnerError) as ei:
        build_messages(rec, params)
    assert ei.value.code == BAD_INPUT
    assert ei.value.retryable is False


def test_prompt_is_wrapped_as_user_message() -> None:
    assert build_messages({"id": 1, "prompt": "hi"}, P) == [{"role": "user", "content": "hi"}]


def test_messages_pass_through_with_system_prepended() -> None:
    rec = {"id": 1, "messages": [{"role": "user", "content": "q"}]}
    out = build_messages(rec, CompletionParams(system="be brief"))
    assert out == [
        {"role": "system", "content": "be brief"},
        {"role": "user", "content": "q"},
    ]


def test_system_param_ignored_for_embedding_params_and_empty() -> None:
    assert build_messages({"prompt": "x"}, EmbeddingParams()) == [{"role": "user", "content": "x"}]
    assert build_messages({"prompt": "x"}, CompletionParams(system="")) == [
        {"role": "user", "content": "x"}
    ]


def test_multi_turn_chat_is_kept_in_order() -> None:
    msgs = [
        {"role": "system", "content": "s"},
        {"role": "user", "content": "a"},
        {"role": "assistant", "content": "b"},
        {"role": "user", "content": "c"},
    ]
    assert build_messages({"messages": msgs}, P) == msgs


@pytest.mark.parametrize(
    "rec",
    [
        "not a dict",
        {},
        {"id": 1},
        {"prompt": "a", "messages": [{"role": "user", "content": "b"}]},
        {"prompt": ""},
        {"prompt": 5},
        {"prompt": None},
        {"messages": []},
        {"messages": "hi"},
        {"messages": [{"role": "user", "content": "x"}, "oops"]},
        {"messages": [{"role": "tool", "content": "x"}]},
        {"messages": [{"role": "user", "content": ""}]},
        {"messages": [{"role": "user", "content": ["x"]}]},
        {"messages": [{"role": "user"}]},
        {"messages": [{"role": "user", "content": "x", "name": "n"}]},
        {"messages": [{"role": "user", "content": "x"}, {"role": "assistant", "content": "y"}]},
        {"messages": [{"role": "user", "content": "x"}] * (MAX_MESSAGES + 1)},
    ],
)
def test_malformed_records_are_bad_input(rec: Any) -> None:
    _bad(rec)


def test_error_message_never_contains_payload() -> None:
    with pytest.raises(RunnerError) as ei:
        build_messages({"messages": [{"role": "user", "content": "SECRET-TEXT", "x": 1}]}, P)
    assert "SECRET-TEXT" not in str(ei.value)


def test_validate_messages_returns_normalised_copy() -> None:
    src = [{"role": "user", "content": "x"}]
    out = validate_messages(src)
    assert out == src and out is not src


def test_embedding_text() -> None:
    assert embedding_text({"id": 1, "text": "abc"}) == "abc"
    for bad in ({}, {"text": ""}, {"text": 3}, {"prompt": "x"}, "x"):
        with pytest.raises(RunnerError) as ei:
            embedding_text(bad)  # type: ignore[arg-type]
        assert ei.value.code == BAD_INPUT and not ei.value.retryable
