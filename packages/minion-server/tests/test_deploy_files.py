"""Static checks of deploy/Dockerfile and .dockerignore (TASK-6.11)."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DOCKERFILE = ROOT / "deploy" / "Dockerfile"
DOCKERIGNORE = ROOT / ".dockerignore"


def _lines(path: Path) -> list[str]:
    return [ln.strip() for ln in path.read_text().splitlines() if ln.strip()]


def test_every_base_image_is_pinned_by_digest() -> None:
    froms = [ln for ln in _lines(DOCKERFILE) if ln.upper().startswith("FROM ")]
    assert len(froms) >= 2
    for ln in froms:
        assert re.search(r"@sha256:[0-9a-f]{64}", ln), ln


def test_runtime_user_is_numeric_non_root() -> None:
    users = [ln.split(None, 1)[1] for ln in _lines(DOCKERFILE) if ln.upper().startswith("USER ")]
    assert users
    uid = users[-1].split(":")[0]
    assert uid.isdigit()
    assert int(uid) != 0


def test_install_is_locked_and_without_dev_or_executor() -> None:
    text = DOCKERFILE.read_text()
    assert "uv sync --locked --no-dev" in text
    assert "minion-executor/src" not in text
    for copy in re.findall(r"^COPY .*$", text, re.M):
        assert ".env" not in copy
        assert "secrets" not in copy


def test_dockerignore_is_an_allow_list_without_secrets() -> None:
    lines = [ln for ln in _lines(DOCKERIGNORE) if not ln.startswith("#")]
    assert lines[0] == "*"
    for ln in lines:
        if ln.startswith("!"):
            assert ".env" not in ln
            assert "secrets" not in ln
