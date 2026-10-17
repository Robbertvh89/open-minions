"""The menu bar must stay a local-files-only surface: no network, no token (TASK-7.5 AC2)."""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import minion_executor.menubar as pkg

FORBIDDEN_MODULES = ("httpx", "minion_common.client", "keyring", "minion_executor.secrets")
SRC = Path(pkg.__file__).parent


def test_importing_model_loads_no_network_or_secret_modules() -> None:
    code = (
        "import sys\n"
        "import minion_executor.menubar.model\n"
        f"bad = [m for m in {FORBIDDEN_MODULES!r} if m in sys.modules]\n"
        "print(','.join(bad))\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)  # noqa: S603
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == ""


def test_menubar_sources_do_not_reference_secrets_or_network() -> None:
    for path in SRC.glob("*.py"):
        text = path.read_text()
        for node in ast.walk(ast.parse(text)):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            for name in names:
                for bad in (*FORBIDDEN_MODULES, "subprocess", "socket", "urllib", "requests"):
                    assert name != bad and not name.startswith(bad + "."), (path.name, name)
        for needle in ("MINION_EXECUTOR_TOKEN", "ENV_VAR", "load_token", "Keychain", "keyring"):
            assert needle not in text, (path.name, needle)
