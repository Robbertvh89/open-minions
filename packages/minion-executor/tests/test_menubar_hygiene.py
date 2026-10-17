"""The menu bar must stay a local-files-only surface: no network, no token (TASK-7.5 AC2).

These are source and import checks of the menubar package's own code. They do not claim process
isolation: the `minion-executor` entry point (main.py) loads httpx at module level.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import minion_executor.menubar as pkg
import pytest

FORBIDDEN_MODULES = ("httpx", "minion_common.client", "keyring", "minion_executor.secrets")
FORBIDDEN_IMPORTS = (
    *FORBIDDEN_MODULES,
    "subprocess",
    "socket",
    "urllib",
    "requests",
    "http",
    "ctypes",
    "asyncio",
)
FORBIDDEN_OS_PREFIXES = ("system", "popen", "spawn", "exec", "posix_spawn")
FORBIDDEN_NAME_PREFIXES = ("NSTask", "NSURL")
SRC = Path(pkg.__file__).parent
SOURCES = sorted(SRC.glob("*.py"))


def _import_in_fresh_interpreter(module: str) -> str:
    code = (
        "import sys\n"
        f"import {module}\n"
        f"bad = [m for m in {FORBIDDEN_MODULES!r} if m in sys.modules]\n"
        "print(','.join(bad))\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)  # noqa: S603
    assert out.returncode == 0, out.stderr
    return out.stdout.strip()


def test_importing_model_loads_no_network_or_secret_modules() -> None:
    assert _import_in_fresh_interpreter("minion_executor.menubar.model") == ""


@pytest.mark.macos
def test_importing_app_loads_no_network_or_secret_modules() -> None:
    assert _import_in_fresh_interpreter("minion_executor.menubar.app") == ""


def test_the_sources_are_found() -> None:
    assert {p.name for p in SOURCES} >= {"app.py", "model.py"}


def _ident(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


@pytest.mark.parametrize("path", SOURCES, ids=lambda p: p.name)
def test_menubar_sources_do_not_reference_secrets_network_or_processes(path: Path) -> None:
    text = path.read_text()
    for node in ast.walk(ast.parse(text)):
        names: list[str] = []
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module or ""]
            for alias in node.names:
                assert not alias.name.startswith(FORBIDDEN_NAME_PREFIXES), (path.name, alias.name)
                if node.module == "os":
                    assert not alias.name.startswith(FORBIDDEN_OS_PREFIXES), (path.name, alias.name)
        for name in names:
            for bad in FORBIDDEN_IMPORTS:
                assert name != bad and not name.startswith(bad + "."), (path.name, name)
        if isinstance(node, ast.Attribute) and _ident(node.value) == "os":
            assert not node.attr.startswith(FORBIDDEN_OS_PREFIXES), (path.name, node.attr)
        assert not _ident(node).startswith(FORBIDDEN_NAME_PREFIXES), (path.name, _ident(node))
    for needle in ("MINION_EXECUTOR_TOKEN", "ENV_VAR", "load_token", "Keychain", "keyring"):
        assert needle not in text, (path.name, needle)


@pytest.mark.parametrize("path", SOURCES, ids=lambda p: p.name)
def test_docstrings_do_not_claim_process_isolation(path: Path) -> None:
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef):
            doc = (ast.get_docstring(node) or "").lower()
            assert "isolat" not in doc and "sandbox" not in doc, (path.name, doc[:60])
