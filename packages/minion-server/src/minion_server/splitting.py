"""Pure JSONL slicing on newline boundaries (ADR-0004, ARCHITECTURE §5.2)."""

from collections.abc import Generator, Iterable
from dataclasses import dataclass


class LineTooLongError(ValueError):
    """A single line exceeded the configured maximum (no newline within `max_line_bytes`)."""


@dataclass(frozen=True)
class Slice:
    byte_start: int
    byte_end: int  # exclusive; always just past a newline, or EOF
    record_count: int


def slice_jsonl(
    chunks: Iterable[bytes],
    start_offset: int,
    records_per_subtask: int,
    max_line_bytes: int | None = None,
) -> Generator[Slice]:
    """Yield byte-range slices of `records_per_subtask` records each (last may be smaller).

    `start_offset` is the object offset of the first byte of `chunks`. Blank (whitespace-only)
    lines are skipped, not counted; they fall inside a slice or are dropped at the tail. A slice
    starts where the previous one ended. The final line may lack a trailing newline. CRLF is
    handled because `\\r` stays part of its line.

    Memory is bounded by `max_line_bytes` (excluding the newline): a longer line raises
    `LineTooLongError`. The partial line is kept as a list of parts and joined once, so a line
    spanning many chunks is not re-copied per chunk.
    """
    if records_per_subtask < 1:
        raise ValueError("records_per_subtask must be >= 1")

    def check(n: int) -> None:
        if max_line_bytes is not None and n > max_line_bytes:
            raise LineTooLongError(f"line exceeds {max_line_bytes} bytes")

    slice_start = start_offset
    pos = start_offset  # absolute offset of the end of the last complete line seen
    count = 0
    parts: list[bytes] = []  # the unterminated line so far
    plen = 0
    for chunk in chunks:
        if b"\n" not in chunk:
            parts.append(chunk)
            plen += len(chunk)
            check(plen)
            continue
        lines = chunk.split(b"\n")
        tail = lines.pop()
        for i, line in enumerate(lines):
            if i == 0 and parts:
                parts.append(line)
                line = b"".join(parts)
                parts, plen = [], 0
            check(len(line))
            pos += len(line) + 1
            if not line.strip():
                continue
            count += 1
            if count == records_per_subtask:
                yield Slice(slice_start, pos, count)
                slice_start, count = pos, 0
        if tail:
            parts, plen = [tail], len(tail)
            check(plen)
        else:
            parts, plen = [], 0
    last = b"".join(parts)
    if last.strip():
        pos += len(last)
        count += 1
    if count:
        yield Slice(slice_start, pos, count)
