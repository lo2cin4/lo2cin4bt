import importlib
import json
from pathlib import Path

import pytest


FIXTURE_PATH = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "backtester"
    / "rust_accounting_golden_v1.json"
)


def _cases() -> list[dict]:
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "rust_accounting_golden.v1"
    return list(payload["cases"])


@pytest.mark.golden
@pytest.mark.parametrize("case", _cases(), ids=lambda case: case["case_id"])
def test_rust_accounting_matches_immutable_golden_fixture(case: dict) -> None:
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    actual = bridge.run_accounting_via_cli(case["input"], timeout=60)
    expected = case["expected"]

    assert actual["final_equity"] == pytest.approx(expected["final_equity"], abs=1e-12)
    assert actual["active_rebalances"] == expected["active_rebalances"]
    assert [row["turnover"] for row in actual["events"]] == pytest.approx(
        expected["turnover"], abs=1e-12
    )
    assert [row["cash_weight"] for row in actual["events"]] == pytest.approx(
        expected["cash_weight"], abs=1e-12
    )
    assert [row["gross_exposure"] for row in actual["events"]] == pytest.approx(
        expected["gross_exposure"], abs=1e-12
    )
