import json
from pathlib import Path

from scripts.replay_signal_evidence import replay


def test_committed_signal_evidence_replays_all_persisted_revisions() -> None:
    evidence = Path(__file__).parents[2] / "docs/signal-execution-diagnosis-evidence-2026-08-10.json"

    bundle = json.loads(evidence.read_text(encoding="utf-8"))
    results = replay(bundle)

    assert len(results) == 4
    assert [row["replayed_expected_profit"] for row in results] == [
        "1.12821",
        "-0.222895",
        "4.48684",
        "-0.222645",
    ]
    assert bundle["capability_boundary"]["execution_tables_present"] == []
