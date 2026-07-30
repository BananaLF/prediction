from __future__ import annotations

import ast
from pathlib import Path
import subprocess
import sys


SDK_MODULES = {"polymarket"}


def sdk_imports_outside(gateway: Path) -> list[Path]:
    offenders: list[Path] = []
    for source in Path("predmarket").rglob("*.py"):
        if source.resolve() == gateway.resolve():
            continue
        tree = ast.parse(source.read_text(), filename=str(source))
        for node in ast.walk(tree):
            module = None
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                modules = [node.module] if node.module is not None else []
            else:
                continue
            if any(name.split(".", 1)[0] in SDK_MODULES for name in modules):
                offenders.append(source)
                break
    return offenders


def test_only_gateway_imports_polymarket_sdk() -> None:
    offenders = sdk_imports_outside(Path("predmarket/polymarket/gateway.py"))

    assert offenders == []


def test_module_help_exits_without_network_access() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "predmarket", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "usage:" in result.stdout
