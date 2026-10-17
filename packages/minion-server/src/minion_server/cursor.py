"""Opaque keyset cursors (SPEC §6.1).

A cursor is url-safe base64 of a small JSON object, validated strictly on decode (kind, field
types, slug patterns, bounds). It carries only a sort position, never authority: every query
re-applies the caller's team filter, so a forged but well-formed cursor can at most move the
page window within what the caller may already see.
"""

import base64
import binascii
from datetime import datetime
from typing import Annotated, Literal

from minion_common.schemas import SLUG_PATTERN
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints, ValidationError

MAX_CURSOR_CHARS = 512
_Slug = Annotated[str, StringConstraints(pattern=SLUG_PATTERN)]


class InvalidCursorError(ValueError):
    """The cursor is malformed, tampered with, or of the wrong kind."""


class TaskCursor(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    k: Literal["task"] = "task"
    created_at: AwareDatetime
    team: _Slug
    project: _Slug
    task_name: _Slug


class SubtaskCursor(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    k: Literal["subtask"] = "subtask"
    subtask_id: int = Field(ge=0, le=2**31 - 1)


def encode_cursor(cursor: TaskCursor | SubtaskCursor) -> str:
    raw = cursor.model_dump_json().encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_json(token: str) -> bytes:
    if not token or len(token) > MAX_CURSOR_CHARS or not token.isascii():
        raise InvalidCursorError("invalid cursor")
    try:
        raw = base64.b64decode(
            token.replace("-", "+").replace("_", "/") + "=" * (-len(token) % 4), validate=True
        )
    except (binascii.Error, ValueError) as exc:
        raise InvalidCursorError("invalid cursor") from exc
    return raw


def decode_task_cursor(token: str) -> TaskCursor:
    try:
        cur = TaskCursor.model_validate_json(_decode_json(token))
    except ValidationError as exc:
        raise InvalidCursorError("invalid cursor") from exc
    # Strict JSON mode accepts ISO strings for datetime; re-encoding must round-trip exactly.
    if encode_cursor(cur) != token.rstrip("="):
        raise InvalidCursorError("invalid cursor")
    return cur


def decode_subtask_cursor(token: str) -> SubtaskCursor:
    try:
        cur = SubtaskCursor.model_validate_json(_decode_json(token))
    except ValidationError as exc:
        raise InvalidCursorError("invalid cursor") from exc
    if encode_cursor(cur) != token.rstrip("="):
        raise InvalidCursorError("invalid cursor")
    return cur


def task_cursor_from(created_at: datetime, team: str, project: str, task_name: str) -> str:
    return encode_cursor(
        TaskCursor(created_at=created_at, team=team, project=project, task_name=task_name)
    )
