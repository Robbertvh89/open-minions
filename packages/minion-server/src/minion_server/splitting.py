"""Pure JSONL slicing on newline boundaries (ADR-0004, ARCHITECTURE §5.2)."""

from collections.abc import Iterable, Iterator
from dataclasses import dataclass


@dataclass(frozen=True)
class Slice:
    byte_start: int
    byte_end: int  # exclusive; always just past a newline, or EOF
    record_count: int


def slice_jsonl(
    chunks: Iterable[bytes], start_offset: int, records_per_subtask: int
) -> Iterator[Slice]:
    """Yield byte-range slices of `records_per_subtask` records each (last may be smaller).

    `start_offset` is the object offset of the first byte of `chunks`. Blank (whitespace-only)
    lines are skipped, not counted; they fall inside a slice or are dropped at the tail. A slice
    starts where the previous one ended. The final line may lack a trailing newline. CRLF is
    handled because `\\r` stays part of its line.
    """
    if records_per_subtask < 1:
        raise ValueError("records_per_subtask must be >= 1")
    slice_start = start_offset
    pos = start_offset  # absolute offset of the end of the last complete line seen
    count = 0
    buf = b""
    for chunk in chunks:
        lines = (buf + chunk).split(b"\n")
        buf = lines.pop()
        for line in lines:
            pos += len(line) + 1
            if not line.strip():
                continue
            count += 1
            if count == records_per_subtask:
                yield Slice(slice_start, pos, count)
                slice_start, count = pos, 0
    if buf.strip():
        pos += len(buf)
        count += 1
    if count:
        yield Slice(slice_start, pos, count)
