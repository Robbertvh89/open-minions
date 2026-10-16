"""Record -> chat messages / embedding text. Pure (no engine imports), shared by all runners.

Malformed records raise `RunnerError(BAD_INPUT, retryable=False)`: another attempt cannot fix the
input. Messages carry the record's shape only, never its content (security: no payload in logs).
"""

from __future__ import annotations

from typing import Any, Literal, TypedDict, cast

from minion_common.schemas import CompletionParams, TaskParams

from minion_executor.runners.base import Record, RunnerError

BAD_INPUT = "BAD_INPUT"
ROLES = frozenset({"system", "user", "assistant"})
MAX_MESSAGES = 256  # a chat longer than this is not a prompt; also bounds template work


class Message(TypedDict):
    role: Literal["system", "user", "assistant"]
    content: str


def _bad(reason: str) -> RunnerError:
    return RunnerError(BAD_INPUT, reason, retryable=False)


def validate_messages(messages: object) -> list[Message]:
    """Check a `messages` value: non-empty list of `{role, content}` with a known role and a
    non-empty string content, ending in a non-assistant turn."""
    if not isinstance(messages, list) or not messages:
        raise _bad("messages must be a non-empty list")
    if len(messages) > MAX_MESSAGES:
        raise _bad("too many messages")
    out: list[Message] = []
    for m in messages:
        if not isinstance(m, dict) or set(m) - {"role", "content"}:
            raise _bad("message must be an object with role and content")
        role, content = m.get("role"), m.get("content")
        if not isinstance(role, str) or role not in ROLES:
            raise _bad("unknown message role")
        if not isinstance(content, str) or not content:
            raise _bad("message content must be a non-empty string")
        out.append(cast(Message, {"role": role, "content": content}))
    if out[-1]["role"] == "assistant":
        raise _bad("last message must not be from the assistant")
    return out


def build_messages(record: Record, params: TaskParams) -> list[Message]:
    """Chat messages for a completion record: `prompt` (wrapped as a user message) xor
    `messages`, with `params.system` prepended as a system message."""
    if not isinstance(record, dict):
        raise _bad("record must be an object")
    has_prompt, has_messages = "prompt" in record, "messages" in record
    if has_prompt == has_messages:
        raise _bad("record needs exactly one of prompt or messages")
    if has_prompt:
        prompt: Any = record["prompt"]
        if not isinstance(prompt, str) or not prompt:
            raise _bad("prompt must be a non-empty string")
        msgs: list[Message] = [{"role": "user", "content": prompt}]
    else:
        msgs = validate_messages(record["messages"])
    system = params.system if isinstance(params, CompletionParams) else None
    if system:
        msgs.insert(0, {"role": "system", "content": system})
    return msgs


def embedding_text(record: Record) -> str:
    """Text to embed: the record's `text` field (FR-4)."""
    if not isinstance(record, dict):
        raise _bad("record must be an object")
    text: Any = record.get("text")
    if not isinstance(text, str) or not text:
        raise _bad("text must be a non-empty string")
    return text
