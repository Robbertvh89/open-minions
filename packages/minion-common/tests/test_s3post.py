from email.parser import BytesParser
from email.policy import HTTP

import pytest
from minion_common.s3post import encode_post_form
from minion_common.schemas import Assignment, PresignedPost
from pydantic import ValidationError

FIELDS = {"key": "a/b", "policy": "cG9saWN5", "x-amz-signature": "abc"}


def test_encode_roundtrip_fields_in_order_file_last() -> None:
    parts = encode_post_form(FIELDS)
    body = parts.preamble + b'{"a":1}\n' + parts.epilogue
    msg = BytesParser(policy=HTTP).parsebytes(
        b"Content-Type: " + parts.content_type.encode() + b"\r\n\r\n" + body
    )
    names = [p.get_param("name", header="content-disposition") for p in msg.iter_parts()]
    assert names == [*FIELDS, "file"]
    got = {
        p.get_param("name", header="content-disposition"): p.get_content() for p in msg.iter_parts()
    }
    assert got["key"] == "a/b"
    assert list(msg.iter_parts())[-1].get_payload(decode=True) == b'{"a":1}\n'
    assert len(body) == len(parts.preamble) + 8 + len(parts.epilogue)


def test_boundary_in_value_rejected() -> None:
    with pytest.raises(ValueError):
        encode_post_form({"key": "xBNDx"}, boundary="BND")


def test_presigned_post_validation() -> None:
    assert PresignedPost(url="http://s3/b", fields=FIELDS).fields["key"] == "a/b"
    bad: list[dict[str, str]] = [
        {"key": "a"},
        {"policy": "p"},
        {**FIELDS, "x": "a\r\nb"},
        {**FIELDS, "x": 'a"b'},
        {**FIELDS, "bad name": "v"},
        {**FIELDS, **{f"f{i}": "v" for i in range(20)}},
        {},
    ]
    for fields in bad:
        with pytest.raises(ValidationError):
            PresignedPost(url="http://s3/b", fields=fields)


def test_assignment_ignores_unknown_fields_and_output_post_optional() -> None:
    assert "output_post" in Assignment.model_fields
    assert Assignment.model_fields["output_post"].default is None
