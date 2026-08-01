from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE = (
    REPO_ROOT
    / "backtester"
    / "contracts"
    / "strategy"
    / "examples"
    / "strategy-run-voo-gld-yfinance-daily-momentum90-sma250-rotation-example.json"
)


def _daily_bar_time() -> dict[str, object]:
    return {
        "schema_version": "bar_time_contract.v1",
        "contract_id": "lo2cin4bt.bar_time_contract.v1",
        "session_model": {
            "calendar_id": "XNYS",
            "timezone": "America/New_York",
            "session_scope": "regular",
            "session_label_policy": "exchange_local_date",
            "non_session_bar_policy": "reject",
        },
        "timestamp_model": {
            "time_standard": "UTC",
            "precision": "nanosecond",
            "clock": "historical_available_time",
            "ordering": (
                "available_time_then_event_time_then_external_execution_sequence"
                "_then_lifecycle_stage_then_stream_id_then_source_sequence"
            ),
        },
        "price_model": {
            "price_basis": "split_dividend_adjusted",
            "corporate_action_policy": "provider_applied",
        },
        "streams": [
            {
                "stream_id": "execution_daily",
                "role": "execution",
                "source": {"kind": "external", "provider_id": "yfinance"},
                "bar_spec": {
                    "aggregation": "time",
                    "step": 1,
                    "unit": "day",
                    "price_type": "last",
                    "alignment": "session_open",
                },
                "timestamp_semantics": {
                    "timestamp_convention": "bar_close",
                    "interval_boundary": "left_open_right_closed",
                    "bar_open_time_column": "bar_open_timestamp",
                    "bar_close_time_column": "bar_close_timestamp",
                    "available_time_column": "available_timestamp",
                    "session_label_column": "session_label",
                    "availability_policy": "bar_close",
                },
            }
        ],
    }


def _v2_config() -> dict[str, object]:
    payload = json.loads(SOURCE.read_text(encoding="utf-8"))
    data = payload["data"]
    data.pop("frequency", None)
    data.pop("interval", None)
    data.pop("calendar", None)
    data.pop("timezone", None)
    data["bar_time"] = _daily_bar_time()
    data["stream_binding"] = {
        "execution_stream_id": "execution_daily",
        "decision_stream_id": "execution_daily",
    }
    return payload


def test_engine_request_v2_carries_typed_bar_time_and_stream_binding() -> None:
    mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])

    request = mod.build_engine_request(_v2_config())

    assert request["schema_version"] == "engine_request.v2"
    assert request["contract_id"] == "lo2cin4bt.engine_request.v2"
    assert request["data_requirements"]["bundle_schema_version"] == (
        "market_data_bundle.v2"
    )
    assert request["data_requirements"]["bar_time"] == _daily_bar_time()
    assert request["strategy"]["stream_binding"] == {
        "execution_stream_id": "execution_daily",
        "decision_stream_id": "execution_daily",
    }
    assert "frequency" not in request["data_requirements"]
    assert "calendar" not in request["data_requirements"]
    assert "timezone" not in request["data_requirements"]
    assert "bar_time" not in request["data_requirements"]["provider_config"]
    assert "stream_binding" not in request["data_requirements"]["provider_config"]


def test_engine_request_v2_rejects_legacy_frequency_without_mapping() -> None:
    mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    legacy = json.loads(SOURCE.read_text(encoding="utf-8"))
    legacy["data"]["frequency"] = "1D"

    with pytest.raises(ValueError, match="bar_time.*frequency"):
        mod.build_engine_request(legacy)


def test_stream_binding_must_reference_declared_streams_and_execution_role() -> None:
    mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    config = _v2_config()

    missing = copy.deepcopy(config)
    missing["data"]["stream_binding"]["decision_stream_id"] = "missing"
    with pytest.raises(ValueError, match="decision_stream_id"):
        mod.build_engine_request(missing)

    wrong_execution = copy.deepcopy(config)
    wrong_execution["data"]["bar_time"]["streams"][0]["role"] = "decision"
    with pytest.raises(ValueError, match="execution_stream_id"):
        mod.build_engine_request(wrong_execution)

    wrong_provider = copy.deepcopy(config)
    wrong_provider["data"]["bar_time"]["streams"][0]["source"]["provider_id"] = "files"
    with pytest.raises(ValueError, match="provider_id"):
        mod.build_engine_request(wrong_provider)

    unbound_external_decision = copy.deepcopy(config)
    decision_stream = copy.deepcopy(
        unbound_external_decision["data"]["bar_time"]["streams"][0]
    )
    decision_stream["stream_id"] = "decision_daily"
    decision_stream["role"] = "decision"
    unbound_external_decision["data"]["bar_time"]["streams"].append(decision_stream)
    unbound_external_decision["data"]["stream_binding"][
        "decision_stream_id"
    ] = "decision_daily"
    with pytest.raises(ValueError, match="derive from execution_stream_id"):
        mod.build_engine_request(unbound_external_decision)


def test_engine_request_v2_accepts_one_minute_execution_and_derived_five_minute_decision() -> None:
    mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    config = _v2_config()
    execution = config["data"]["bar_time"]["streams"][0]
    execution["stream_id"] = "execution_1m"
    execution["bar_spec"]["unit"] = "minute"
    decision = copy.deepcopy(execution)
    decision["stream_id"] = "decision_5m"
    decision["role"] = "decision"
    decision["source"] = {
        "kind": "derived",
        "parent_stream_id": "execution_1m",
        "aggregation_engine": "shared_rust",
        "empty_bar_policy": "omit",
        "partial_first_bar_policy": "omit",
        "partial_final_bar_policy": "omit",
    }
    decision["bar_spec"]["step"] = 5
    config["data"]["bar_time"]["streams"].append(decision)
    config["data"]["stream_binding"] = {
        "execution_stream_id": "execution_1m",
        "decision_stream_id": "decision_5m",
    }

    request = mod.build_engine_request(config)

    assert request["strategy"]["stream_binding"]["execution_stream_id"] == "execution_1m"
    assert request["strategy"]["stream_binding"]["decision_stream_id"] == "decision_5m"
