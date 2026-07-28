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


def _assignment(tree: ast.AST, name: str) -> object:
    matches = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == name for target in node.targets)
    ]
    assert len(matches) == 1
    return ast.literal_eval(matches[0])


def _http_calls(tree: ast.AST) -> list[ast.Call]:
    calls: list[ast.Call] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        receiver = node.func.value
        if (
            isinstance(receiver, ast.Attribute)
            and isinstance(receiver.value, ast.Name)
            and receiver.value.id == "self"
            and receiver.attr == "http"
            and node.func.attr
            in {"get", "post", "put", "patch", "delete", "request", "stream"}
        ):
            calls.append(node)
    return calls


def _looks_like_direct_http_call(node: ast.Call) -> bool:
    if not isinstance(node.func, ast.Attribute) or node.func.attr not in {
        "get", "post", "put", "patch", "delete", "request", "stream"
    }:
        return False
    receiver = node.func.value
    if isinstance(receiver, ast.Name):
        return receiver.id == "httpx" or receiver.id.endswith(("http", "client"))
    return (
        isinstance(receiver, ast.Attribute)
        and receiver.attr in {"http", "client"}
    )


def test_network_origins_and_endpoint_builders_are_an_exact_allowlist() -> None:
    gamma_path = PACKAGE / "polymarket" / "gamma.py"
    clob_path = PACKAGE / "polymarket" / "clob.py"
    ws_path = PACKAGE / "polymarket" / "ws.py"
    gamma = ast.parse(gamma_path.read_text(encoding="utf-8"))
    clob = ast.parse(clob_path.read_text(encoding="utf-8"))
    websocket = ast.parse(ws_path.read_text(encoding="utf-8"))

    assert _assignment(gamma, "GAMMA_PUBLIC_ORIGIN") == (
        "https://gamma-api.polymarket.com"
    )
    assert _assignment(clob, "CLOB_PUBLIC_ORIGIN") == "https://clob.polymarket.com"
    assert _assignment(websocket, "MARKET_CHANNEL_URL") == (
        "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    )

    gamma_calls = _http_calls(gamma)
    assert len(gamma_calls) == 1
    assert gamma_calls[0].func.attr == "stream"
    assert [ast.unparse(value) for value in gamma_calls[0].args[:2]] == [
        "'GET'",
        "f'{self.base_url}/markets/keyset'",
    ]

    clob_calls = _http_calls(clob)
    assert len(clob_calls) == 1
    assert clob_calls[0].func.attr == "stream"
    assert [ast.unparse(value) for value in clob_calls[0].args[:2]] == [
        "method",
        "f'{self.base_url}{path}'",
    ]

    request_builders: set[tuple[str, str]] = set()
    for node in ast.walk(clob):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "_request"
        ):
            assert len(node.args) >= 2
            method = ast.literal_eval(node.args[0])
            route = node.args[1]
            if isinstance(route, ast.Constant):
                path = ast.literal_eval(route)
            else:
                assert isinstance(route, ast.JoinedStr)
                assert route.values[0].value == "/clob-markets/"
                formatted = route.values[1]
                assert isinstance(formatted, ast.FormattedValue)
                assert ast.unparse(formatted.value) == "quote(condition, safe='')"
                path = "/clob-markets/{urlencoded_condition}"
            request_builders.add((method, path))
    assert request_builders == {
        ("POST", "/books"),
        ("GET", "/fee-rate"),
        ("GET", "/clob-markets/{urlencoded_condition}"),
    }

    ws_connector_calls = [
        node
        for node in ast.walk(websocket)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "connector"
    ]
    assert len(ws_connector_calls) == 1
    assert ast.unparse(ws_connector_calls[0].args[0]) == "MARKET_CHANNEL_URL"

    enumerated: list[tuple[str, int]] = []
    for source_path in _production_sources():
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and _looks_like_direct_http_call(node):
                enumerated.append((str(source_path.relative_to(PACKAGE.parent)), node.lineno))
    assert enumerated == [
        ("predmarket/polymarket/clob.py", clob_calls[0].lineno),
        ("predmarket/polymarket/gamma.py", gamma_calls[0].lineno),
    ]


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
