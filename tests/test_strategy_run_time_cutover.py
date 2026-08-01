from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from backtester.StrategyRunConfig_backtester import (
    StrategyRunConfigError,
    normalize_strategy_run_config,
    plan_strategy_execution,
)
from backtester.ops.support_checker import strategy_run_support_report


REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_PATH = (
    REPO_ROOT
    / "backtester"
    / "contracts"
    / "strategy"
    / "examples"
    / "strategy-run-us-etf-yfinance-daily-selection-timing-momentum-sma-example.json"
)
SCHEMA_PATH = (
    REPO_ROOT / "backtester" / "contracts" / "strategy" / "strategy-run.schema.json"
)
LEGACY_TIME_FIELDS = ("frequency", "interval", "calendar", "timezone")


def _config() -> dict[str, object]:
    return json.loads(EXAMPLE_PATH.read_text(encoding="utf-8"))


def test_strategy_run_requires_typed_time_contract_and_preserves_it_in_plan() -> None:
    config = _config()
    data = config["data"]

    assert all(field not in data for field in LEGACY_TIME_FIELDS)
    assert data["stream_binding"] == {
        "execution_stream_id": "execution_daily",
        "decision_stream_id": "execution_daily",
    }
    Draft202012Validator(
        json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    ).validate(config)

    normalized = normalize_strategy_run_config(config)
    snapshot = plan_strategy_execution(normalized)["resolved_strategy_snapshot"]

    assert normalized["data"]["bar_time"] == data["bar_time"]
    assert normalized["data"]["stream_binding"] == data["stream_binding"]
    assert snapshot["bar_time"] == data["bar_time"]
    assert snapshot["stream_binding"] == data["stream_binding"]
    assert all(field not in snapshot for field in LEGACY_TIME_FIELDS)


@pytest.mark.parametrize("legacy_field", LEGACY_TIME_FIELDS)
def test_strategy_run_rejects_each_legacy_time_field(legacy_field: str) -> None:
    config = _config()
    config["data"][legacy_field] = "legacy"

    with pytest.raises(StrategyRunConfigError, match="legacy time fields"):
        normalize_strategy_run_config(config)

    report = strategy_run_support_report(config)
    assert report["supported"] is False
    assert report["issues"][0]["path"] == f"data.{legacy_field}"


@pytest.mark.parametrize("missing_field", ("bar_time", "stream_binding"))
def test_strategy_run_rejects_missing_typed_time_contract(missing_field: str) -> None:
    config = _config()
    del config["data"][missing_field]

    with pytest.raises(StrategyRunConfigError, match=missing_field):
        normalize_strategy_run_config(config)


def test_strategy_run_rejects_stream_binding_that_does_not_match_graph() -> None:
    config = deepcopy(_config())
    config["data"]["stream_binding"]["decision_stream_id"] = "missing_stream"

    with pytest.raises(StrategyRunConfigError, match="decision_stream_id"):
        normalize_strategy_run_config(config)


@pytest.mark.parametrize("legacy_field", LEGACY_TIME_FIELDS)
def test_strategy_run_rejects_nested_benchmark_time_authority(
    legacy_field: str,
) -> None:
    config = _config()
    config["data"]["benchmark"][legacy_field] = "legacy"

    with pytest.raises(
        StrategyRunConfigError,
        match=r"data\.benchmark.*bound execution stream.*legacy time fields",
    ):
        normalize_strategy_run_config(config)

    report = strategy_run_support_report(config)
    assert report["supported"] is False
    assert report["issues"][0]["path"] == f"data.benchmark.{legacy_field}"
