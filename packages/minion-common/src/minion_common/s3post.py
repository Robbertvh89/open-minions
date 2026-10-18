"""Multipart/form-data encoding for S3 presigned POST uploads (pure; no HTTP client).

The caller sends `preamble + <file bytes> + epilogue` with an exact `Content-Length`, so a large
file can be streamed in chunks. S3 requires the `file` part last.
"""

from __future__ import annotations

import secrets
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class FormParts:
    preamble: bytes
    epilogue: bytes
    content_type: str


def encode_post_form(
    fields: Mapping[str, str],
    *,
    filename: str = "output.jsonl",
    boundary: str | None = None,
) -> FormParts:
    """Encode `fields` (in order) then the head of the `file` part; see module docstring."""
    boundary = boundary or secrets.token_hex(16)
    if any(boundary in v or boundary in k for k, v in fields.items()):
        raise ValueError("boundary occurs in a form field")
    if any(c in filename for c in '\r\n"'):
        raise ValueError("invalid filename")
    head = bytearray()
    for name, value in fields.items():
        if any(c in name for c in '\r\n"'):
            raise ValueError("invalid form field name")
        head += f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n'.encode()
        head += value.encode() + b"\r\n"
    head += (
        f'--{boundary}\r\nContent-Disposition: form-data; name="file"; '
        f'filename="{filename}"\r\nContent-Type: application/octet-stream\r\n\r\n'
    ).encode()
    return FormParts(
        preamble=bytes(head),
        epilogue=f"\r\n--{boundary}--\r\n".encode(),
        content_type=f"multipart/form-data; boundary={boundary}",
    )
