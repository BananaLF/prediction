import argparse
import json

from predmarket.cli import build_parser, main


def test_validate_opportunity_command_exists():
    parser = build_parser()
    args = parser.parse_args(["validate-opportunity", "opp_123"])
    assert args.command == "validate-opportunity"
    assert args.opportunity_id == "opp_123"


def test_validate_opportunity_unknown_id_is_json_error(capsys):
    async def missing_opportunity(_args):
        raise KeyError("opp_missing")

    assert main(
        ["--json", "validate-opportunity", "opp_missing"],
        dispatcher=missing_opportunity,
    ) == 0

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["status"] == "fail"
    assert payload["errors"][0]["code"] == "NOT_FOUND"
    assert captured.err == ""


def test_validate_opportunity_emits_json_only(capsys):
    async def successful_validation(_args):
        return {"status": "pass", "errors": []}

    assert main(
        ["validate-opportunity", "opp_123"],
        dispatcher=successful_validation,
    ) == 0

    captured = capsys.readouterr()
    assert json.loads(captured.out) == {"status": "pass", "errors": []}
    assert captured.err == ""


def test_validate_opportunity_missing_argument_is_json_error(capsys):
    async def successful_validation(_args):
        return {"status": "pass", "errors": []}

    assert main(
        ["validate-opportunity"],
        dispatcher=successful_validation,
    ) == 0

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["status"] == "fail"
    assert payload["errors"][0]["code"] == "INVALID_INPUT"
    assert captured.err == ""
