import pytest
from minion_server.uris import InvalidUriError, parse_s3_uri

P = ("inputs/",)


def test_valid() -> None:
    u = parse_s3_uri("s3://minion/inputs/team/a.jsonl", bucket="minion", prefixes=P)
    assert (u.bucket, u.key) == ("minion", "inputs/team/a.jsonl")


@pytest.mark.parametrize(
    "uri",
    [
        "s3://other/inputs/a",
        "http://minion/inputs/a",
        "s3://minion/outputs/a",
        "s3://minion/inputs/../outputs/a",
        "s3://minion/inputs/a/../../x",
        "s3://minion/inputs//a",
        "s3://minion//inputs/a",
        "s3://minion/",
        "s3://minion",
        "s3://minion/inputs/a\nb",
        "s3://minion/inputs/a?x=1",
        "s3://minion/inputs/./a",
        "s3://minion/inputs\\a",
    ],
)
def test_rejected(uri: str) -> None:
    with pytest.raises(InvalidUriError):
        parse_s3_uri(uri, bucket="minion", prefixes=P)
