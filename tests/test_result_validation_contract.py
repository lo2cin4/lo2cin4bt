from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator


REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = (
    REPO_ROOT
    / "backtester"
    / "contracts"
    / "runtime"
    / "result-validation-report-v1.schema.json"
)


def test_rust_result_validation_report_matches_public_json_schema() -> None:
    bridge = __import__("backtester.RustCoreBridge_backtester", fromlist=["dummy"])
    summary = bridge.run_accounting_via_cli(
        {
            "config": {
                "starting_equity": 100.0,
                "cost_rate": 0.0,
                "max_gross_exposure": 1.0,
                "allow_short": False,
            },
            "checkpoints": [
                {
                    "time": "2024-01-02",
                    "rebalance": True,
                    "returns": {},
                    "target_weights": {"AAA": 1.0},
                }
            ],
        },
        timeout=60,
    )

    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(summary["result_validation"])
    assert summary["result_validation"]["status"] == "valid"
    assert summary["result_validation"]["table_row_counts"]["equity_curve"] == 1
