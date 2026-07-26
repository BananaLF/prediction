"""Regression guard for the production package's public, read-only boundary."""

from __future__ import annotations

import ast
from importlib import metadata
from pathlib import Path
import re

import predmarket


PACKAGE = Path(__file__).parents[2] / "predmarket"


def _production_sources() -> list[Path]:
    return sorted(PACKAGE.rglob("*.py"))


def test_production_has_no_trade_wallet_auth_or_shell_surface() -> None:
    forbidden_route = re.compile(
        r"""(?ix)
        ["']/
        (?:
            order(?:s)?|cancel(?:-all)?|balance(?:-allowance)?|
            allowance|wallet|sign(?:ature)?
        )
        (?:[/?{]["']|["'])
        """
    )
    forbidden_text = (
        "Authorization",
        "POLY_",
        "shell=True",
        "/ws/user",
        "/user/ws",
    )
    forbidden_import_roots = {
        "eth_account",
        "web3",
        "py_clob_client",
        "brownie",
    }

    violations: list[str] = []
    for path in _production_sources():
        source = path.read_text(encoding="utf-8")
        relative = path.relative_to(PACKAGE.parent)
        if match := forbidden_route.search(source):
            violations.append(f"{relative}: forbidden route {match.group(0)!r}")
        for token in forbidden_text:
            if token in source:
                violations.append(f"{relative}: forbidden token {token!r}")
        tree = ast.parse(source, filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                roots = {alias.name.split(".", 1)[0] for alias in node.names}
            elif isinstance(node, ast.ImportFrom):
                roots = {node.module.split(".", 1)[0]} if node.module else set()
            else:
                continue
            prohibited = roots & forbidden_import_roots
            if prohibited:
                violations.append(
                    f"{relative}:{node.lineno}: forbidden import "
                    f"{sorted(prohibited)[0]}"
                )

    assert violations == []


def test_network_origins_and_websocket_channel_are_public_and_fixed() -> None:
    gamma = (PACKAGE / "polymarket" / "gamma.py").read_text(encoding="utf-8")
    clob = (PACKAGE / "polymarket" / "clob.py").read_text(encoding="utf-8")
    websocket = (PACKAGE / "polymarket" / "ws.py").read_text(encoding="utf-8")

    assert '"https://gamma-api.polymarket.com"' in gamma
    assert 'f"{self.base_url}/markets/keyset"' in gamma
    assert '"https://clob.polymarket.com"' in clob
    assert all(
        token in clob for token in ('"/books"', '"/fee-rate"', 'f"/clob-markets/')
    )
    assert (
        '"wss://ws-subscriptions-clob.polymarket.com/ws/market"' in websocket
    )
    assert "wss://ws-subscriptions-clob.polymarket.com/ws/user" not in websocket


def test_legacy_modules_are_absent() -> None:
    for relative in (
        "predmarket/core.py",
        "predmarket/api.py",
        "predmarket/ledger.py",
        "tests/test_core.py",
    ):
        assert not (PACKAGE.parent / relative).exists()


def test_package_and_distribution_versions_match() -> None:
    assert predmarket.__version__ == "0.2.0"
    assert predmarket.__version__ == metadata.version("predmarket")
