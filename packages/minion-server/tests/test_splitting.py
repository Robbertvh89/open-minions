import itertools

import pytest
from minion_server.splitting import Slice, slice_jsonl


def _chunked(data: bytes, size: int) -> list[bytes]:
    return [data[i : i + size] for i in range(0, len(data), size)]


def _recs(n: int, prefix: str = "") -> bytes:
    return b"".join(f'{{"id":"{prefix}{i}"}}\n'.encode() for i in range(n))


def test_boundaries_are_newline_aligned_and_exclusive() -> None:
    data = _recs(5)
    slices = list(slice_jsonl([data], 0, 2))
    assert [s.record_count for s in slices] == [2, 2, 1]
    assert slices[0].byte_start == 0
    for a, b in itertools.pairwise(slices):
        assert a.byte_end == b.byte_start
    assert slices[-1].byte_end == len(data)
    for s in slices:
        assert s.byte_end == len(data) or data[s.byte_end - 1 : s.byte_end] == b"\n"
        assert data[s.byte_start : s.byte_end].count(b"\n") == s.record_count


@pytest.mark.parametrize("size", [1, 3, 7, 1000])
def test_chunking_does_not_change_result(size: int) -> None:
    data = _recs(11)
    assert list(slice_jsonl(_chunked(data, size), 0, 4)) == list(slice_jsonl([data], 0, 4))


def test_crlf() -> None:
    data = b'{"id":1}\r\n{"id":2}\r\n{"id":3}\r\n'
    slices = list(slice_jsonl([data], 0, 2))
    assert slices == [Slice(0, 20, 2), Slice(20, len(data), 1)]


def test_no_trailing_newline() -> None:
    data = b'{"id":1}\n{"id":2}'
    assert list(slice_jsonl(_chunked(data, 4), 0, 5)) == [Slice(0, len(data), 2)]


def test_single_record_slices() -> None:
    data = _recs(3)
    slices = list(slice_jsonl([data], 0, 1))
    assert [s.record_count for s in slices] == [1, 1, 1]
    assert b"".join(data[s.byte_start : s.byte_end] for s in slices) == data


def test_blank_lines_skipped_not_counted() -> None:
    data = b'\n{"id":1}\n\n  \n{"id":2}\n\n'
    slices = list(slice_jsonl([data], 0, 1))
    assert [s.record_count for s in slices] == [1, 1]
    # trailing blank lines belong to no slice; every slice holds exactly one record
    assert slices[-1].byte_end == len(data) - 1
    for s in slices:
        assert data[s.byte_start : s.byte_end].strip().startswith(b"{")


def test_empty_and_blank_only_inputs() -> None:
    assert list(slice_jsonl([], 0, 3)) == []
    assert list(slice_jsonl([b"\n\n \n"], 0, 3)) == []


def test_start_offset_shifts_ranges() -> None:
    assert list(slice_jsonl([b'{"a":1}\n'], 100, 1)) == [Slice(100, 108, 1)]


def test_invalid_n() -> None:
    with pytest.raises(ValueError):
        list(slice_jsonl([b"x\n"], 0, 0))


def test_line_cap_across_chunks() -> None:
    from minion_server.splitting import LineTooLongError

    with pytest.raises(LineTooLongError):
        list(slice_jsonl(_chunked(b"x" * 5000, 100), 0, 2, max_line_bytes=1024))
    with pytest.raises(LineTooLongError):  # terminated, but too long
        list(slice_jsonl([b"x" * 2000 + b"\n"], 0, 2, max_line_bytes=1024))
    ok = b"y" * 1024 + b"\n" + b"z" * 1024  # exactly at the cap (newline excluded)
    assert [s.record_count for s in slice_jsonl(_chunked(ok, 7), 0, 5, max_line_bytes=1024)] == [2]


@pytest.mark.parametrize("size", [1, 5, 64])
def test_cap_does_not_change_slicing(size: int) -> None:
    data = _recs(30) + b"\n \n" + _recs(3, "t").rstrip(b"\n")
    capped = list(slice_jsonl(_chunked(data, size), 0, 4, max_line_bytes=100))
    assert capped == list(slice_jsonl([data], 0, 4))
