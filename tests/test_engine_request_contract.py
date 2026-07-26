from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator


REPO_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_CONTRACT_ROOT = REPO_ROOT / "backtester" / "contracts" / "runtime"
PROFILE_SOURCES = {
    "selection_timing_portfolio": (
        "backtester/contracts/strategy/examples/"
        "strategy-run-us-etf-yfinance-daily-selection-timing-momentum-sma-example.json"
    ),
    "allocation_portfolio": (
        "backtester/contracts/strategy/examples/"
        "strategy-run-vti-avuv-vxus-sgol-dbmf-yfinance-yearly-rebalance-example.json"
    ),
    "rotation_portfolio": (
        "backtester/contracts/strategy/examples/"
        "strategy-run-voo-gld-yfinance-daily-momentum90-sma250-rotation-example.json"
    ),
    "calendar_event_portfolio": (
        "backtester/contracts/strategy/examples/"
        "strategy-run-btcusdt-binance-monthly-nth-weekday-same-session-matrix-example.json"
    ),
    "pair_spread_portfolio": (
        "backtester/contracts/strategy/examples/"
        "strategy-run-spy-qqq-yfinance-monthly-pair-spread-example.json"
    ),
    "multi_leg_event_portfolio": (
        "backtester/contracts/strategy/examples/"
        "strategy-run-qqq-tlt-gld-yfinance-monthly-hedge-overlay-example.json"
    ),
}


def _load(relative_path: str) -> dict:
    return json.loads((REPO_ROOT / relative_path).read_text(encoding="utf-8"))


def test_six_profiles_build_one_engine_request_contract() -> None:
    contract_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    schema = json.loads(
        (RUNTIME_CONTRACT_ROOT / "engine-request-v1.schema.json").read_text(encoding="utf-8")
    )
    fixture_payload = json.loads(
        (
            RUNTIME_CONTRACT_ROOT
            / "examples"
            / "engine-request-profile-fixtures-v1.json"
        ).read_text(encoding="utf-8")
    )
    fixtures = {
        item["strategy"]["strategy_profile_id"]: item
        for item in fixture_payload["requests"]
    }

    assert fixture_payload["schema_version"] == "engine_request_profile_fixtures.v1"
    assert set(fixtures) == set(PROFILE_SOURCES)
    for profile_id, source_path in PROFILE_SOURCES.items():
        request = contract_mod.build_engine_request(_load(source_path))

        Draft202012Validator(schema).validate(request)
        assert request == fixtures[profile_id]
        assert request["schema_version"] == "engine_request.v1"
        assert request["contract_id"] == "lo2cin4bt.engine_request.v1"
        assert request["strategy"]["strategy_mode_id"] == "multi_asset_portfolio"
        assert request["strategy"]["strategy_profile_id"] == profile_id
        assert request["request_hash"] == contract_mod.engine_request_hash(request)
        assert "producer_family" not in json.dumps(request)
        assert "portfolio_config" not in json.dumps(request)


def test_engine_request_uses_canonical_ops_and_typed_actions() -> None:
    contract_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    request = contract_mod.build_engine_request(
        _load(PROFILE_SOURCES["selection_timing_portfolio"])
    )

    operations = request["strategy"]["decision_plan"]["required_operations"]
    actions = request["strategy"]["decision_plan"]["required_actions"]
    assert "indicator.momentum" in operations
    assert set(actions).issubset({"enter", "exit", "flatten", "set_target_weights"})


def test_engine_request_materializes_explicit_account_venue_and_clock_defaults() -> None:
    config_mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    contract_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])

    normalized = config_mod.normalize_strategy_run_config(
        _load(PROFILE_SOURCES["allocation_portfolio"])
    )
    request = contract_mod.build_engine_request(normalized)

    assert normalized["simulation"]["account"] == {
        "base_currency": "USD",
        "balance_mode": "normalized_equity",
        "starting_balance": 100.0,
        "position_mode": "netting",
        "account_type": "cash",
        "leverage_limit": 1.0,
    }
    assert normalized["simulation"]["venue"] == {
        "venue_id": "SIM",
        "oms_type": "netting",
        "book_type": "bar",
        "routing": "simulated",
        "settlement_days": 0,
    }
    assert normalized["simulation"]["clock"] == {
        "mode": "historical_event_time",
        "event_ordering": "event_time_then_sequence",
        "tie_breaker": "source_then_sequence",
    }
    assert request["simulation"]["account"] == normalized["simulation"]["account"]
    assert request["simulation"]["venue"] == normalized["simulation"]["venue"]
    assert request["simulation"]["clock"] == normalized["simulation"]["clock"]


def test_engine_request_uses_user_account_and_venue_values_instead_of_hardcoded_values() -> None:
    contract_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    config = _load(PROFILE_SOURCES["pair_spread_portfolio"])
    config["simulation"] = {
        "account": {
            "base_currency": "HKD",
            "balance_mode": "cash",
            "starting_balance": 1_000_000.0,
            "position_mode": "hedging",
            "account_type": "margin",
            "leverage_limit": 2.5,
        },
        "venue": {
            "venue_id": "SIM-HK",
            "oms_type": "hedging",
            "book_type": "bar",
            "routing": "simulated",
            "settlement_days": 2,
        },
        "clock": {
            "mode": "historical_event_time",
            "event_ordering": "event_time_then_sequence",
            "tie_breaker": "source_then_sequence",
        },
    }

    request = contract_mod.build_engine_request(config)

    assert request["simulation"]["account"]["base_currency"] == "HKD"
    assert request["simulation"]["account"]["starting_balance"] == 1_000_000.0
    assert request["simulation"]["account"]["position_mode"] == "hedging"
    assert request["simulation"]["venue"]["venue_id"] == "SIM-HK"
    assert request["simulation"]["venue"]["settlement_days"] == 2


def test_strategy_config_rejects_account_and_venue_position_mode_mismatch() -> None:
    contract_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    config = _load(PROFILE_SOURCES["allocation_portfolio"])
    config["simulation"] = {
        "account": {"position_mode": "hedging"},
        "venue": {"oms_type": "netting"},
    }

    with pytest.raises(ValueError, match="position_mode must match venue.oms_type"):
        contract_mod.build_engine_request(config)


def test_engine_request_rejects_unknown_operation_without_exception_path() -> None:
    contract_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    config = _load(PROFILE_SOURCES["selection_timing_portfolio"])
    config["computed_fields"][0]["op"] = "indicator.magic"

    with pytest.raises(ValueError, match="indicator.magic"):
        contract_mod.build_engine_request(config)


def test_engine_request_rejects_unknown_profile() -> None:
    contract_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    config = _load(PROFILE_SOURCES["selection_timing_portfolio"])
    config["platform"]["strategy_profile_id"] = "ai_magic_strategy"

    with pytest.raises(ValueError, match="ai_magic_strategy"):
        contract_mod.build_engine_request(config)


def test_candidate_binding_does_not_mutate_public_strategy_config() -> None:
    contract_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    config = _load(PROFILE_SOURCES["calendar_event_portfolio"])
    original = copy.deepcopy(config)

    request = contract_mod.build_engine_request(
        config,
        request_id="calendar-window-001",
        run_scope="validation_test_window",
        resolved_parameters={"month_week": 2, "weekday": "monday"},
        window={"start": "2024-01-01", "end": "2024-03-31"},
    )

    assert config == original
    assert request["request_id"] == "calendar-window-001"
    assert request["workflow"]["run_scope"] == "validation_test_window"
    assert request["workflow"]["resolved_parameters"] == {
        "month_week": 2,
        "weekday": "monday",
    }
    assert "resolved_parameters" not in config


@pytest.mark.parametrize(
    "uncompiled_payload",
    [
        _load(PROFILE_SOURCES["selection_timing_portfolio"]),
        {
            "dataloader": {"source": "multi_asset"},
            "backtester": {
                "strategy_mode": "multi_asset_portfolio",
                "portfolio_config": {},
            },
        },
    ],
)
def test_unified_runtime_rejects_every_uncompiled_config_shape(
    uncompiled_payload: dict,
) -> None:
    runner_mod = __import__("backtester.UnifiedBacktestRunner_backtester", fromlist=["dummy"])

    with pytest.raises(ValueError, match="EngineRequest"):
        runner_mod.UnifiedBacktestRunnerBacktester().run(
            market_data_bundle=object(),
            engine_request=uncompiled_payload,
        )


def test_validation_window_adapter_is_one_resolved_internal_run() -> None:
    contract_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    request = contract_mod.build_engine_request(
        _load(PROFILE_SOURCES["calendar_event_portfolio"]),
        request_id="calendar-train-001",
        run_scope="validation_train_window",
        resolved_parameters={"month_week": 2, "weekday": "monday"},
        window={"start": "2024-01-01", "end": "2024-03-31"},
    )

    internal_run = contract_mod.strategy_run_from_engine_request(request)

    assert request["workflow"]["run_scope"] == "validation_train_window"
    assert internal_run["platform"]["workflow_id"] == "single_backtest"
    assert internal_run["parameter_domains"] == {}
