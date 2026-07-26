import json
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator


_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from dataloader.market_data_loader import market_data_spec_from_requirements  # noqa: E402

QQQ_SMA_EXAMPLE = "backtester/contracts/strategy/examples/strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json"
BTCUSDT_MONTHLY_NTH_WEEKDAY_EXAMPLE = "backtester/contracts/strategy/examples/strategy-run-btcusdt-binance-monthly-nth-weekday-same-session-matrix-example.json"
VOO_GLD_ROTATION_EXAMPLE = "backtester/contracts/strategy/examples/strategy-run-voo-gld-yfinance-daily-momentum90-sma250-rotation-example.json"
ANNUAL_FIXED_ETF_EXAMPLE = "backtester/contracts/strategy/examples/strategy-run-vti-avuv-vxus-sgol-dbmf-yfinance-yearly-rebalance-example.json"
SELECTION_TIMING_EXAMPLE = "backtester/contracts/strategy/examples/strategy-run-us-etf-yfinance-daily-selection-timing-momentum-sma-example.json"
PAIR_SPREAD_EXAMPLE = "backtester/contracts/strategy/examples/strategy-run-spy-qqq-yfinance-monthly-pair-spread-example.json"
MULTI_LEG_EVENT_EXAMPLE = "backtester/contracts/strategy/examples/strategy-run-qqq-tlt-gld-yfinance-monthly-hedge-overlay-example.json"
QQQ_SMA_WFA_EXAMPLE = "backtester/contracts/strategy/examples/wfa-run-qqq-yfinance-daily-sma-cross-example.json"
VOO_GLD_ROTATION_WFA_EXAMPLE = "backtester/contracts/strategy/examples/wfa-run-voo-gld-yfinance-daily-momentum90-sma250-rotation-example.json"
BTCUSDT_MONTHLY_NTH_WEEKDAY_WFA_EXAMPLE = "backtester/contracts/strategy/examples/wfa-run-btcusdt-binance-monthly-nth-weekday-same-session-example.json"
SECTOR_LONG_SHORT_EXAMPLE = "backtester/contracts/strategy/examples/strategy-run-us-sector-etf-yfinance-monthly-12-1-long-short-rotation-example.json"
ADJUSTED_LONG_SHORT_TEST = "tests/fixtures/strategy-run-us-sector-etf-yfinance-monthly-adjusted-12-1-long-short-test.json"


def _load(path: str):
    return json.loads((_REPO_ROOT / path).read_text(encoding="utf-8-sig"))


def test_strategy_run_rejects_nested_risk_gates_compatibility_shape() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = _load(VOO_GLD_ROTATION_EXAMPLE)
    config["risk"]["gates"] = {"max_positions": 1}

    with pytest.raises(mod.StrategyRunConfigError, match="risk.gates"):
        mod.normalize_strategy_run_config(config)


@pytest.mark.parametrize("action", ["pause_trading", "paper_stop_until_recovery"])
def test_strategy_run_rejects_ambiguous_or_retired_risk_actions(action: str) -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = _load(VOO_GLD_ROTATION_EXAMPLE)
    config["risk"]["max_drawdown"] = 0.1
    config["risk"]["gate_action"] = action

    with pytest.raises(mod.StrategyRunConfigError, match="Unknown risk.gate_action"):
        mod.normalize_strategy_run_config(config)


@pytest.mark.parametrize(
    "action",
    ["flatten", "permanent_stop", "shadow_until_recovery"],
)
def test_strategy_run_accepts_explicit_drawdown_routes(action: str) -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = _load(VOO_GLD_ROTATION_EXAMPLE)
    config["risk"]["max_drawdown"] = 0.1
    config["risk"]["gate_action"] = action

    normalized = mod.normalize_strategy_run_config(config)

    assert normalized["risk"]["gate_action"] == action


def test_strategy_run_materializes_registry_computed_field_defaults() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = _load(SELECTION_TIMING_EXAMPLE)
    config["computed_fields"] = [
        {"name": "macd", "op": "indicator.macd"},
        {"name": "percentile", "op": "indicator.percentile"},
        {"name": "bollinger", "op": "indicator.bollinger"},
        {"name": "volatility", "op": "indicator.volatility", "period": 20},
        {"name": "atr", "op": "indicator.atr"},
        {"name": "rank", "op": "cross_section.rank", "source": "macd"},
        {"name": "winsor", "op": "cross_section.winsorize", "source": "macd"},
    ]

    normalized = mod.normalize_strategy_run_config(config)
    fields = {item["name"]: item for item in normalized["computed_fields"]}

    assert fields["macd"] == {
        "name": "macd",
        "op": "indicator.macd",
        "source": "close",
        "fastperiod": 12,
        "slowperiod": 26,
        "signalperiod": 9,
        "output": "line",
    }
    assert fields["percentile"]["period"] == 14
    assert fields["percentile"]["percentile"] == 50
    assert fields["bollinger"]["period"] == 20
    assert fields["bollinger"]["stddev"] == 2
    assert fields["bollinger"]["band"] == "middle"
    assert fields["volatility"]["source"] == "close"
    assert fields["volatility"]["annualize"] is True
    assert fields["atr"]["high_source"] == "high"
    assert fields["atr"]["low_source"] == "low"
    assert fields["atr"]["close_source"] == "close"
    assert fields["atr"]["period"] == 14
    assert fields["atr"]["method"] == "wilder"
    assert fields["rank"]["method"] == "average"
    assert fields["rank"]["ascending"] is True
    assert fields["winsor"]["lower"] == 0.01
    assert fields["winsor"]["upper"] == 0.99


def test_strategy_run_materializes_explicit_risk_gate_contract() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = _load(VOO_GLD_ROTATION_EXAMPLE)
    config["risk"]["max_drawdown"] = 0.1
    config["risk"].pop("gate_action", None)

    normalized = mod.normalize_strategy_run_config(config)

    assert normalized["risk"]["gate_action"] == "flatten"

    config["risk"]["gate_action"] = "reduce_exposure"
    reduced = mod.normalize_strategy_run_config(config)
    assert reduced["risk"]["reduce_exposure_factor"] == 0.5


def _external_breadth_timer_example() -> dict:
    return {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "workflow_id": "parameter_matrix",
            "run_type": "example",
            "display_label": "SPY-IEF | External Breadth Timer | yfinance | Test",
        },
        "data": {
            "provider": "yfinance",
            "frequency": "1D",
            "interval": "1d",
            "calendar": "XNYS",
            "timezone": "America/New_York",
            "start_date": "2010-01-01",
            "start_policy": "common_available",
            "external_features": [
                {
                    "name": "market_breadth",
                    "path": "workspace/datasets/MARKET_BREADTH_1D.csv",
                    "time_column": "time",
                    "value_column": "close",
                    "scope": "market",
                }
            ],
            "benchmark": {
                "provider": "yfinance",
                "symbol": "SPY",
                "label": "SPY buy and hold",
                "interval": "1d",
            },
        },
        "universe": {
            "symbols": ["SPY", "IEF"],
            "universe_policy": "configured_symbols",
        },
        "computed_fields": [],
        "signals": {
            "entry": {
                "field": "market_breadth",
                "op": "crosses_below",
                "value": {"param_ref": "breadth_threshold"},
            },
            "conflict_policy": "reset_timer_on_reentry_signal",
        },
        "selection": {},
        "allocation": {
            "method": "fixed_weights",
            "weights": {"SPY": 1},
            "cash_policy": "keep_unallocated_cash",
        },
        "rebalance": {"trigger": {"op": "calendar.first_session"}},
        "fill_model": {
            "timing": "timeline",
            "position_policy": {"on_entry_signal_while_holding": "reset_timer"},
            "actions": [
                {
                    "signal": "rebalance",
                    "offset_bars": 0,
                    "price": "open",
                    "action": "set_target_weights",
                    "weights": {"SPY": 1},
                },
                {
                    "signal": "entry",
                    "offset_bars": 1,
                    "price": "open",
                    "action": "set_target_weights",
                    "weights": {"IEF": 1},
                },
                {
                    "signal": "entry",
                    "offset_bars": {"param_ref": "delay_bars"},
                    "price": "close",
                    "action": "set_target_weights",
                    "weights": {"SPY": 1},
                },
            ],
            "matrix_workers": 4,
            "matrix_result_retention": 50,
            "cost": {"transaction_cost": 0.001, "slippage": 0.0005},
        },
        "risk": {
            "max_positions": 1,
            "max_gross_exposure": 1,
            "long_short": "long_only",
            "allow_short": False,
        },
        "parameter_domains": {
            "breadth_threshold": {"type": "range", "start": 10, "end": 35, "step": 1},
            "delay_bars": {"type": "range", "start": 100, "end": 200, "step": 10},
        },
        "combo_limits": {"warn_combos": 11},
        "metricstracker": {
            "enable_metrics_analysis": True,
            "time_unit": 252,
            "risk_free_rate": 0.04,
        },
        "outputs": {
            "equity_curve": True,
            "trade_summary": True,
            "entry_exit_markers": True,
            "rebalance_audit": True,
            "holdings": True,
            "asset_contribution": True,
        },
        "metadata": {
            "example": True,
            "local_research_only": True,
            "strategy_id": "external_breadth_timer_contract_probe",
            "notes": [
                "This synthetic contract probe validates external-feature loading and timer semantics.",
                "It uses placeholder assets and parameter ranges rather than a research strategy.",
                "It is not a profitability claim, WFA pass, live-trading signal, broker instruction, or investment advice.",
            ],
        },
    }


def test_strategy_run_schema_declares_universe_provenance_fields():
    schema = _load("backtester/contracts/strategy/strategy-run.schema.json")
    platform_props = schema["properties"]["platform"]["properties"]
    assert "strategy_profile_id" in platform_props
    assert "strategy_preset_id" in platform_props
    universe_props = schema["properties"]["universe"]["properties"]

    for key in [
        "universe_policy",
        "survivorship_policy",
        "historical_constituents_path",
        "universe_constituents_path",
        "as_of_date",
        "delisted_policy",
        "point_in_time_constituents",
        "historical_constituents_hash",
        "universe_constituents_hash",
    ]:
        assert key in universe_props

    metric_props = schema["properties"]["metricstracker"]["properties"]
    assert "time_unit" in metric_props
    assert "risk_free_rate" in metric_props


def test_strategy_and_wfa_schemas_reject_unknown_top_level_and_path_escape():
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    strategy_schema = _load("backtester/contracts/strategy/strategy-run.schema.json")
    wfa_schema = _load("backtester/contracts/strategy/wfa-run.schema.json")

    strategy = _load(QQQ_SMA_EXAMPLE)
    strategy["typo_field"] = True
    assert list(Draft202012Validator(strategy_schema).iter_errors(strategy))

    wfa = _load(QQQ_SMA_WFA_EXAMPLE)
    wfa["strategy_run_path"] = "../../outside.json"
    assert list(Draft202012Validator(wfa_schema).iter_errors(wfa))
    with pytest.raises(mod.StrategyRunConfigError, match="parent-directory"):
        mod.normalize_wfa_run_config(wfa)

    wfa = _load(QQQ_SMA_WFA_EXAMPLE)
    wfa["strategy_run_path"] = "D:/outside.json"
    assert list(Draft202012Validator(wfa_schema).iter_errors(wfa))
    with pytest.raises(mod.StrategyRunConfigError, match="repo-relative"):
        mod.normalize_wfa_run_config(wfa)

    wfa = _load(QQQ_SMA_WFA_EXAMPLE)
    wfa["typo_field"] = True
    assert list(Draft202012Validator(wfa_schema).iter_errors(wfa))


def test_strategy_and_wfa_examples_validate_against_public_schemas():
    strategy_schema = _load("backtester/contracts/strategy/strategy-run.schema.json")
    wfa_schema = _load("backtester/contracts/strategy/wfa-run.schema.json")

    for example in [
        QQQ_SMA_EXAMPLE,
        BTCUSDT_MONTHLY_NTH_WEEKDAY_EXAMPLE,
        VOO_GLD_ROTATION_EXAMPLE,
        ANNUAL_FIXED_ETF_EXAMPLE,
        SECTOR_LONG_SHORT_EXAMPLE,
        SELECTION_TIMING_EXAMPLE,
        PAIR_SPREAD_EXAMPLE,
        MULTI_LEG_EVENT_EXAMPLE,
    ]:
        Draft202012Validator(strategy_schema).validate(_load(example))

    for example in [
        QQQ_SMA_WFA_EXAMPLE,
        VOO_GLD_ROTATION_WFA_EXAMPLE,
        BTCUSDT_MONTHLY_NTH_WEEKDAY_WFA_EXAMPLE,
    ]:
        Draft202012Validator(wfa_schema).validate(_load(example))


def test_strategy_schema_accepts_computed_fields_without_legacy_aliases() -> None:
    strategy_schema = _load("backtester/contracts/strategy/strategy-run.schema.json")
    strategy = _load(ANNUAL_FIXED_ETF_EXAMPLE)
    strategy.pop("features", None)
    strategy["computed_fields"] = [
        {"name": "ema_20", "op": "indicator.ema", "source": "close", "period": 20}
    ]

    Draft202012Validator(strategy_schema).validate(strategy)


def test_strategy_run_normalizer_rejects_legacy_aliases_in_current_config() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    strategy = _load(VOO_GLD_ROTATION_EXAMPLE)
    strategy["indicators"] = strategy.pop("computed_fields")
    strategy["execution"] = strategy.pop("fill_model")

    with pytest.raises(mod.StrategyRunConfigError, match="must use computed_fields"):
        mod.normalize_strategy_run_config(strategy)


def test_strategy_run_normalizer_rejects_metadata_legacy_backtester_in_current_config() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    strategy = _load(VOO_GLD_ROTATION_EXAMPLE)
    strategy["metadata"]["legacy_backtester"] = {"Backtest_id": "legacy_probe"}

    with pytest.raises(mod.StrategyRunConfigError, match="legacy_backtester"):
        mod.normalize_strategy_run_config(strategy)


def test_strategy_run_normalizer_rejects_legacy_multi_asset_runtime_wrapper() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    legacy_runtime_config = {
        "dataloader": {"source": "multi_asset", "start_date": "2020-01-01", "frequency": "1D"},
        "backtester": {
            "strategy_mode": "multi_asset_portfolio",
            "Backtest_id": "legacy_wrapper_probe",
            "market_data": {
                "provider": "yfinance",
                "symbols": ["VOO", "GLD"],
                "start": "2020-01-01",
                "interval": "1d",
            },
            "portfolio_config": {
                "strategy_id": "legacy_wrapper_probe",
                "universe": {"symbols": ["VOO", "GLD"]},
                "selection": {"rank_by": "close", "rank_order": "desc", "top_n": 1},
                "allocation": {"method": "equal_weight", "position_limit": 1.0},
                "rebalance": {"trigger": {"op": "calendar.month_start"}},
                "execution": {"cost": {"transaction_cost": 0.0, "slippage": 0.0}},
            },
        },
    }

    with pytest.raises(mod.StrategyRunConfigError, match="canonical strategy run config"):
        mod.normalize_strategy_run_config(legacy_runtime_config)


def test_normalizes_single_asset_ma_config_to_strategy_run():
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    path = _REPO_ROOT / QQQ_SMA_EXAMPLE
    normalized = mod.normalize_strategy_run_config(_load(str(path.relative_to(_REPO_ROOT))), source_path=path)

    assert normalized["schema_version"] == "strategy_run"
    assert normalized["platform"]["strategy_mode_id"] == "multi_asset_portfolio"
    assert normalized["platform"]["strategy_profile_id"] == "selection_timing_portfolio"
    assert normalized["platform"]["strategy_preset_id"] == "single_asset_signal"
    assert normalized["platform"]["workflow_id"] == "parameter_matrix"
    assert normalized["universe"]["symbols"] == ["QQQ"]
    assert normalized["selection"] == {}
    assert normalized["allocation"]["method"] == "position_state"
    assert normalized["allocation"]["target_weight"] == 1.0
    assert normalized["rebalance"] == {}
    assert set(normalized["parameter_domains"]) == {"short_ma", "long_ma"}
    assert normalized["parameter_domains"]["short_ma"] == {"type": "range", "start": 20, "end": 100, "step": 10}
    assert normalized["parameter_domains"]["long_ma"] == {"type": "range", "start": 120, "end": 300, "step": 10}
    assert normalized["signals"]["entry"]["op"] == "crosses_above"
    assert normalized["signals"]["exit"]["op"] == "crosses_below"
    assert normalized["metadata"]["local_research_only"] is True
    assert any("not a profitability claim" in note for note in normalized["metadata"]["notes"])


def test_single_asset_signal_preset_compiles_into_selection_timing_profile():
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_preset_id": "single_asset_signal",
            "workflow_id": "single_backtest",
        },
        "data": {"provider": "yfinance", "frequency": "1D", "calendar": "XNYS"},
        "universe": {"symbols": ["QQQ"]},
        "computed_fields": [],
        "signals": {
            "entry": {"field": "close", "op": "gt", "value": 100},
            "exit": {"field": "close", "op": "lt", "value": 100},
        },
        "selection": {},
        "allocation": {},
        "rebalance": {},
        "fill_model": {
            "timing": "timeline",
            "actions": [
                {"signal": "entry", "offset_bars": 1, "price": "open", "action": "enter"},
                {"signal": "exit", "offset_bars": 1, "price": "open", "action": "exit"},
            ],
        },
        "risk": {"max_positions": 1, "max_gross_exposure": 1.0},
        "parameter_domains": {},
        "outputs": {"equity_curve": True},
        "metadata": {"strategy_id": "preset_probe"},
    }

    normalized = mod.normalize_strategy_run_config(config)
    plan = mod.plan_strategy_execution(normalized)

    assert normalized["platform"]["strategy_mode_id"] == "multi_asset_portfolio"
    assert normalized["platform"]["strategy_profile_id"] == "selection_timing_portfolio"
    assert normalized["platform"]["strategy_preset_id"] == "single_asset_signal"
    assert normalized["selection"] == {}
    assert normalized["allocation"]["method"] == "position_state"
    assert plan["strategy_preset_id"] == "single_asset_signal"
    assert plan["result_type"] == "single_asset"
    assert plan["canonical_runtime_plan"]["preset_id"] == "single_asset_signal"
    assert plan["canonical_runtime_plan"]["profile_contract"]["contract_kind"] == "selection_timing"


@pytest.mark.parametrize(
    "mutator",
    [
        lambda config: config["platform"].update({"strategy_mode_id": "single_asset_signal"}),
        lambda config: config["platform"].update({"workflow_id": "matrix"}),
        lambda config: config["allocation"].update({"method": "signal_state"}),
        lambda config: config["rebalance"].update({"trigger": {"op": "signal.change"}}),
    ],
)
def test_strategy_run_normalizer_rejects_retired_runtime_terms(mutator):
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    strategy = _load(QQQ_SMA_EXAMPLE)
    mutator(strategy)

    with pytest.raises(mod.StrategyRunConfigError):
        mod.normalize_strategy_run_config(strategy)


def test_public_sector_long_short_example_is_strategy_run_contract():
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    path = _REPO_ROOT / SECTOR_LONG_SHORT_EXAMPLE
    config = _load(str(path.relative_to(_REPO_ROOT)))
    normalized = mod.normalize_strategy_run_config(config, source_path=path)
    plan = mod.plan_strategy_execution(normalized)

    assert normalized["schema_version"] == "strategy_run"
    assert normalized["platform"]["strategy_mode_id"] == "multi_asset_portfolio"
    assert normalized["platform"]["strategy_profile_id"] == "rotation_portfolio"
    assert normalized["platform"]["workflow_id"] == "single_backtest"
    assert normalized["data"]["provider"] == "yfinance"
    assert normalized["data"]["frequency"] == "1D"
    assert normalized["data"]["calendar"] == "XNYS"
    assert len(normalized["universe"]["symbols"]) == 11
    assert normalized["selection"]["long_top_n"] == 2
    assert normalized["selection"]["short_bottom_n"] == 2
    assert normalized["allocation"]["method"] == "equal_weight_long_short"
    assert normalized["allocation"]["long_gross_exposure"] == 0.5
    assert normalized["allocation"]["short_gross_exposure"] == 0.5
    assert normalized["fill_model"]["timing"] == "signal_close_for_next_bar"
    assert normalized["fill_model"]["price"] == "next_open"
    assert normalized["fill_model"]["cost"]["transaction_cost"] == 0.001
    assert normalized["fill_model"]["cost"]["short_borrow_rate_annual"] == 0.003
    assert normalized["metadata"]["public_example"] is True
    assert normalized["metadata"]["local_research_only"] is True
    assert normalized["metricstracker"]["time_unit"] == 252
    assert normalized["metricstracker"]["risk_free_rate"] == 0.04
    assert plan["strategy_profile_id"] == "rotation_portfolio"
    assert plan["result_type"] == "portfolio"


def test_adjusted_momentum_research_config_uses_generic_computed_field_chain():
    config_mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    request_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    schema = _load("backtester/contracts/strategy/strategy-run.schema.json")
    request_schema = _load("backtester/contracts/runtime/engine-request-v1.schema.json")
    config = _load(ADJUSTED_LONG_SHORT_TEST)

    Draft202012Validator(schema).validate(config)
    normalized = config_mod.normalize_strategy_run_config(config)
    request = request_mod.build_engine_request(normalized)

    Draft202012Validator(request_schema).validate(request)
    assert normalized["selection"]["rank_by"] == "adjusted_momentum_score"
    assert [field["name"] for field in normalized["computed_fields"]] == [
        "recent_1m_return",
        "momentum_12_1",
        "one_plus_recent_return",
        "adjusted_momentum_score",
    ]
    assert set(request["strategy"]["decision_plan"]["required_operations"]) >= {
        "indicator.calendar_return",
        "math.add",
        "math.multiply",
    }
    assert request["strategy"]["decision_plan"]["computed_fields"] == normalized["computed_fields"]


def test_all_default_runnable_strategies_charge_point_one_percent_transaction_cost():
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    roots = [
        _REPO_ROOT / "workspace" / "runs",
        _REPO_ROOT / "backtester" / "contracts" / "strategy" / "examples",
    ]
    offenders = []
    for root in roots:
        for path in root.glob("strategy-run-*.json"):
            config = _load(str(path.relative_to(_REPO_ROOT)))
            normalized = mod.normalize_strategy_run_config(config, source_path=path)
            transaction_cost = normalized["fill_model"]["cost"]["transaction_cost"]
            if transaction_cost != pytest.approx(0.001):
                offenders.append((str(path.relative_to(_REPO_ROOT)), transaction_cost))

    assert offenders == []


def test_normalizes_calendar_matrix_config_and_preserves_same_session_contract():
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    path = _REPO_ROOT / BTCUSDT_MONTHLY_NTH_WEEKDAY_EXAMPLE
    normalized = mod.normalize_strategy_run_config(_load(str(path.relative_to(_REPO_ROOT))), source_path=path)

    assert normalized["platform"]["strategy_mode_id"] == "multi_asset_portfolio"
    assert normalized["platform"]["workflow_id"] == "parameter_matrix"
    assert normalized["universe"]["symbols"] == ["BTC-USD"]
    assert normalized["parameter_domains"]["month_week"] == {"type": "range", "start": 1, "end": 4, "step": 1}
    assert normalized["parameter_domains"]["weekday"]["values"] == [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ]
    assert normalized["signals"]["entry"]["op"] == "calendar.nth_weekday_of_month"
    assert normalized["signals"]["entry"]["months"] == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    assert normalized["signals"]["entry"]["ordinal"] == {"param_ref": "month_week"}
    assert normalized["signals"]["entry"]["weekday"] == {"param_ref": "weekday"}
    assert normalized["signals"]["exit"]["op"] == "session.same_session_close"
    assert normalized["signals"]["side"] == "long"
    assert normalized["allocation"]["method"] == "position_state"
    assert normalized["allocation"]["target_weight"] == 1
    assert normalized["fill_model"]["session_scope"] == "same_session"
    assert normalized["fill_model"]["entry_price"] == "open"
    assert normalized["fill_model"]["exit_price"] == "close"
    assert normalized["fill_model"]["actions"] == [
        {
            "signal": "entry",
            "offset_bars": 0,
            "price": "open",
            "action": "enter",
        },
        {
            "signal": "entry",
            "offset_bars": 0,
            "price": "close",
            "action": "exit",
        },
    ]
    assert normalized["risk"]["allow_short"] is False
    assert normalized["metadata"]["local_research_only"] is True
    assert any("not a profitability claim" in note for note in normalized["metadata"]["notes"])


def test_normalizes_multi_asset_portfolio_matrix_config():
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    path = _REPO_ROOT / VOO_GLD_ROTATION_EXAMPLE
    normalized = mod.normalize_strategy_run_config(_load(str(path.relative_to(_REPO_ROOT))), source_path=path)

    assert normalized["platform"]["strategy_mode_id"] == "multi_asset_portfolio"
    assert normalized["platform"]["workflow_id"] == "single_backtest"
    assert normalized["universe"]["symbols"] == ["VOO", "GLD"]
    assert normalized["parameter_domains"] == {}
    assert normalized["computed_fields"][0]["period"] == 90
    assert normalized["computed_fields"][1]["period"] == 250
    assert normalized["selection"]["eligible"]["right_field"] == "sma_filter"
    assert normalized["selection"]["rank_by"] == "return_momentum"
    assert normalized["data"]["benchmark"]["symbol"] == "SPY"
    assert normalized["metadata"]["local_research_only"] is True
    assert any("not a profitability claim" in note for note in normalized["metadata"]["notes"])


def test_market_data_spec_preserves_external_features():
    config = {
        "data": {
            "provider": "yfinance",
            "start_date": "2020-01-01",
            "external_features": [
                {
                    "name": "market_breadth",
                    "path": "workspace/datasets/MARKET_BREADTH_1D.csv",
                    "time_column": "Date",
                    "value_column": "breadth",
                }
            ],
        },
        "universe": {"symbols": ["SPY", "GLD"]},
    }

    spec = market_data_spec_from_requirements(
        {
            "provider": config["data"]["provider"],
            "provider_config": config["data"],
            "symbols": config["universe"]["symbols"],
            "external_features": config["data"]["external_features"],
        }
    )

    assert spec["provider"] == "yfinance"
    assert spec["symbols"] == ["SPY", "GLD"]
    assert spec["external_features"][0]["name"] == "market_breadth"


def test_external_breadth_timer_example_uses_raw_external_feature_file():
    config = _external_breadth_timer_example()
    strategy_mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    checker_mod = __import__("backtester.ops.support_checker", fromlist=["dummy"])
    normalized = strategy_mod.normalize_strategy_run_config(
        config,
        source_path=_REPO_ROOT / "tests" / "fixtures" / "external_breadth_timer_strategy_run.json",
    )

    feature = normalized["data"]["external_features"][0]
    spec = market_data_spec_from_requirements(
        {
            "provider": normalized["data"]["provider"],
            "provider_config": normalized["data"],
            "symbols": normalized["universe"]["symbols"],
            "external_features": normalized["data"]["external_features"],
        }
    )
    restore_action = normalized["fill_model"]["actions"][2]

    assert feature["path"] == "workspace/datasets/MARKET_BREADTH_1D.csv"
    assert feature["time_column"] == "time"
    assert feature["value_column"] == "close"
    assert normalized["rebalance"]["trigger"]["op"] == "calendar.first_session"
    assert restore_action["offset_bars"] == {"param_ref": "delay_bars"}
    assert normalized["fill_model"]["position_policy"]["on_entry_signal_while_holding"] == "reset_timer"
    assert spec["external_features"][0]["value_column"] == "close"
    assert checker_mod.strategy_run_support_report(normalized)["supported"] is True


def test_fixed_allocation_without_parameters_plans_as_explicit_rolling_validation():
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    path = _REPO_ROOT / VOO_GLD_ROTATION_EXAMPLE
    normalized = mod.normalize_strategy_run_config(_load(str(path.relative_to(_REPO_ROOT))), source_path=path)
    normalized["platform"]["workflow_id"] = "rolling_validation"
    plan = mod.plan_strategy_execution(normalized)

    assert normalized["parameter_domains"] == {}
    assert plan["is_rolling_validation"] is True
    assert plan["workflow_id"] == "rolling_validation"
    assert plan["execution_backend"] == "vector_hybrid"
    assert plan["accounting_backend"] == "sequential"


def test_annual_fixed_etf_allocation_example_is_strategy_run_contract():
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    path = _REPO_ROOT / ANNUAL_FIXED_ETF_EXAMPLE
    normalized = mod.normalize_strategy_run_config(_load(str(path.relative_to(_REPO_ROOT))), source_path=path)
    plan = mod.plan_strategy_execution(normalized)

    assert normalized["platform"]["strategy_mode_id"] == "multi_asset_portfolio"
    assert normalized["platform"]["workflow_id"] == "single_backtest"
    assert normalized["data"]["provider"] == "yfinance"
    assert normalized["data"]["calendar"] == "XNYS"
    assert normalized["data"]["start_policy"] == "common_available"
    assert normalized["data"]["benchmark"]["symbol"] == "VTI"
    assert normalized["universe"]["symbols"] == ["VTI", "AVUV", "VXUS", "SGOL", "DBMF"]
    assert normalized["universe"]["universe_policy"] == "static_public_etf_list"
    assert normalized["allocation"]["method"] == "fixed_weights"
    assert normalized["allocation"]["weights"] == {
        "VTI": 0.3,
        "AVUV": 0.1,
        "VXUS": 0.2,
        "SGOL": 0.2,
        "DBMF": 0.2,
    }
    assert sum(normalized["allocation"]["weights"].values()) == pytest.approx(1.0)
    assert normalized["rebalance"]["trigger"]["op"] == "calendar.year_start"
    assert normalized["outputs"]["rebalance_audit"] is True
    assert normalized["outputs"]["holdings"] is True
    assert normalized["outputs"]["asset_contribution"] is True
    assert normalized["metadata"]["local_research_only"] is True
    assert any("not a profitability claim" in note for note in normalized["metadata"]["notes"])
    assert plan["result_type"] == "portfolio"
    assert plan["requires_portfolio_accounting"] is True


def test_execution_planner_uses_vector_hybrid_for_single_and_portfolio():
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    single_path = _REPO_ROOT / QQQ_SMA_EXAMPLE
    portfolio_path = _REPO_ROOT / VOO_GLD_ROTATION_EXAMPLE

    single_plan = mod.plan_strategy_execution(
        mod.normalize_strategy_run_config(_load(str(single_path.relative_to(_REPO_ROOT))), source_path=single_path)
    )
    portfolio_plan = mod.plan_strategy_execution(
        mod.normalize_strategy_run_config(_load(str(portfolio_path.relative_to(_REPO_ROOT))), source_path=portfolio_path)
    )

    assert single_plan["result_type"] == "single_asset"
    assert portfolio_plan["result_type"] == "portfolio"
    assert single_plan["requires_portfolio_accounting"] is True
    assert portfolio_plan["requires_portfolio_accounting"] is True
    assert single_plan["stages"][-1]["id"] == "portfolio_accounting"
    assert portfolio_plan["stages"][-1]["backend"] == "sequential"
    assert single_plan["normalized_strategy_plan"]["schema_version"] == "normalized_strategy_plan.v1"
    assert single_plan["normalized_strategy_plan"]["canonical_runtime_plan"]["schema_version"] == "canonical_runtime_plan.v1"
    assert single_plan["normalized_strategy_plan"]["output_contracts"]["result_bundle"] == "canonical_result_bundle.v1"
    assert single_plan["canonical_runtime_plan"]["schema_version"] == "canonical_runtime_plan.v1"
    assert portfolio_plan["resolved_strategy_snapshot"]["schema_version"] == "resolved_strategy_snapshot.v1"
    assert single_plan["engine_capability_requirements"]["schema_version"] == "engine_capability_requirements.v1"
    assert isinstance(single_plan["param_axes"], list)
    assert single_plan["param_axes"][0]["name"] == "long_ma"
    assert isinstance(single_plan["plan_hash"], str) and len(single_plan["plan_hash"]) == 64
    assert single_plan["canonical_runtime_plan"]["universe_shape"]["scope"] == "single_asset"
    assert portfolio_plan["canonical_runtime_plan"]["universe_shape"]["scope"] == "portfolio"
    assert single_plan["engine_capability_requirements"]["workflow_support"]["parameter_matrix"] is True
    assert portfolio_plan["engine_capability_requirements"]["workflow_support"]["single_backtest"] is True
    assert portfolio_plan["combo_guard"]["estimated_total_combos"] == 0


def test_factor_pipeline_is_first_class_contract_stage():
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "selection_timing_portfolio",
            "workflow_id": "parameter_matrix",
        },
        "data": {
            "provider": "local_parquet",
            "frequency": "1D",
            "calendar": "XNYS",
            "benchmark": "SPY",
        },
        "universe": {"symbols": ["AAA", "BBB", "CCC"]},
        "factor_pipeline": {
            "schema_version": "factor_pipeline.v1",
            "data_requirements": {
                "price_fields": ["close", "volume"],
                "fundamental_fields": ["book_value", "market_cap"],
                "classification_fields": ["sector"],
                "point_in_time_required": True,
            },
            "construction": [
                {"name": "value", "family": "value", "op": "factor.book_to_market"},
                {"name": "momentum", "family": "momentum", "op": "factor.price_momentum"},
            ],
            "preprocessing": [
                {"op": "winsorize", "scope": "cross_section"},
                {"op": "standardize", "scope": "cross_section"},
                {"op": "neutralize", "scope": "cross_section", "group_by": ["sector"]},
                {"op": "lag_audit", "scope": "point_in_time"},
            ],
            "composite": {
                "method": "equal_weight",
                "inputs": ["value", "momentum"],
                "output": "factor_score",
            },
            "point_in_time": {
                "known_at_field": "known_at",
                "fail_on_lookahead": True,
            },
            "cache": {
                "enabled": True,
                "namespace": "lo2cin4bt.factor_pipeline",
                "storage": "local_parquet",
            },
            "outputs": {"factor_score_frame": True, "statanalyser": True},
        },
        "computed_fields": [],
        "selection": {
            "eligible": {"field": "factor_score", "op": "gt", "value": -999999},
            "rank_by": "factor_score",
            "rank_order": "desc",
            "top_n": 10,
        },
        "allocation": {"method": "equal_weight", "position_limit": 0.1},
        "rebalance": {"trigger": {"op": "calendar.month_start"}},
        "fill_model": {"timing": "signal_close_for_next_bar", "price": "close_to_close"},
        "risk": {"max_positions": 10, "max_gross_exposure": 1.0, "long_short": "long_only"},
        "parameter_domains": {
            "value_weight": {"type": "range", "start": 0.0, "end": 1.0, "step": 0.25}
        },
        "outputs": {"equity_curve": True, "asset_contribution": True},
    }

    normalized = mod.normalize_strategy_run_config(config)
    plan = mod.plan_strategy_execution(normalized)

    assert normalized["factor_pipeline"]["schema_version"] == "factor_pipeline.v1"
    assert plan["uses_factor_pipeline"] is True


def test_selection_timing_portfolio_profile_normalizes_under_multi_asset_portfolio() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "selection_timing_portfolio",
            "workflow_id": "single_backtest",
        },
        "data": {
            "provider": "yfinance",
            "frequency": "1D",
            "calendar": "XNYS",
        },
        "universe": {"symbols": ["AAA", "BBB", "CCC"]},
        "computed_fields": [
            {"name": "momentum_20", "op": "indicator.momentum", "source": "close", "period": 20}
        ],
        "selection": {
            "eligible": {"field": "close", "op": "gt", "value": 0},
            "rank_by": "momentum_20",
            "rank_order": "desc",
            "top_n": 2,
        },
        "allocation": {"method": "equal_weight", "position_limit": 0.5},
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "fill_model": {"timing": "signal_close_for_next_bar", "price": "close_to_close"},
        "risk": {"max_positions": 2, "max_gross_exposure": 1.0, "long_short": "long_only"},
        "parameter_domains": {},
        "outputs": {"equity_curve": True},
        "metadata": {"strategy_id": "selection_timing_profile_probe"},
    }

    normalized = mod.normalize_strategy_run_config(config)
    plan = mod.plan_strategy_execution(normalized)

    assert normalized["platform"]["strategy_profile_id"] == "selection_timing_portfolio"
    assert plan["strategy_profile_id"] == "selection_timing_portfolio"
    assert plan["strategy_mode_id"] == "multi_asset_portfolio"
    assert plan["canonical_runtime_plan"]["profile_id"] == "selection_timing_portfolio"
    assert plan["canonical_runtime_plan"]["decision_shape"]["has_selection"] is True
    assert plan["engine_capability_requirements"]["producer_requirements"] == [
        "selection_and_ranking",
        "rebalance_trigger_evaluation",
        "selection_timing_compile",
    ]
    assert plan["resolved_strategy_snapshot"]["selection"]["top_n"] == 2
    assert plan["canonical_runtime_plan"]["profile_contract"]["contract_kind"] == "selection_timing"
    assert plan["canonical_runtime_plan"]["profile_contract"]["holdings_cap"] == 2
    assert plan["resolved_strategy_snapshot"]["profile_contract"]["ranking_key"] == "momentum_20"


def test_selection_timing_portfolio_profile_applies_authoring_defaults() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "selection_timing_portfolio",
            "workflow_id": "single_backtest",
        },
        "data": {"provider": "yfinance", "frequency": "1D", "calendar": "XNYS"},
        "universe": {"symbols": ["AAA", "BBB", "CCC", "DDD"]},
        "computed_fields": [
            {"name": "momentum_90", "op": "indicator.momentum", "source": "close", "period": 90}
        ],
        "selection": {
            "eligible": {"field": "close", "op": "gt", "value": 0},
            "rank_by": "momentum_90",
            "top_n": 4,
        },
        "allocation": {},
        "risk": {},
        "parameter_domains": {},
        "outputs": {"equity_curve": True},
        "metadata": {"strategy_id": "selection_timing_defaults_probe"},
    }

    normalized = mod.normalize_strategy_run_config(config)
    plan = mod.plan_strategy_execution(normalized)

    assert normalized["selection"]["rank_order"] == "desc"
    assert normalized["allocation"]["method"] == "equal_weight"
    assert normalized["allocation"]["position_limit"] == 0.25
    assert normalized["allocation"]["cash_policy"] == "keep_unallocated_cash"
    assert normalized["rebalance"]["trigger"]["op"] == "calendar.every_session"
    assert normalized["fill_model"]["timing"] == "signal_close_for_next_bar"
    assert normalized["fill_model"]["price"] == "close_to_close"
    assert normalized["fill_model"]["cost"] == {
        "transaction_cost": 0.001,
        "slippage": 0.0,
        "short_borrow_rate_annual": 0.0,
        "borrow_day_count": 252,
    }
    assert normalized["fill_model"]["liquidity"] == {
        "max_fill_fraction": 1.0,
    }
    assert normalized["risk"]["max_positions"] == 4
    assert normalized["risk"]["max_gross_exposure"] == 1.0
    assert normalized["risk"]["long_short"] == "long_only"
    assert normalized["risk"]["allow_short"] is False
    assert plan["canonical_runtime_plan"]["allocation_shape"]["method"] == "equal_weight"
    assert plan["resolved_strategy_snapshot"]["rebalance"]["trigger_op"] == "calendar.every_session"


def test_selection_timing_portfolio_profile_requires_holdings_cap() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "selection_timing_portfolio",
            "workflow_id": "single_backtest",
        },
        "data": {"provider": "yfinance", "frequency": "1D", "calendar": "XNYS"},
        "universe": {"symbols": ["AAA", "BBB", "CCC"]},
        "computed_fields": [],
        "selection": {
            "eligible": {"field": "close", "op": "gt", "value": 0},
            "rank_by": "close",
            "rank_order": "desc",
        },
        "allocation": {"method": "equal_weight", "position_limit": 0.5},
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "fill_model": {"timing": "signal_close_for_next_bar", "price": "close_to_close"},
        "risk": {"max_gross_exposure": 1.0, "long_short": "long_only"},
        "parameter_domains": {},
        "outputs": {"equity_curve": True},
        "metadata": {"strategy_id": "selection_timing_profile_missing_cap"},
    }

    with pytest.raises(mod.StrategyRunConfigError, match="holdings cap"):
        mod.normalize_strategy_run_config(config)


def test_selection_timing_portfolio_profile_accepts_per_asset_signals_for_shared_compiler() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "selection_timing_portfolio",
            "workflow_id": "single_backtest",
        },
        "data": {"provider": "yfinance", "frequency": "1D", "calendar": "XNYS"},
        "universe": {"symbols": ["AAA", "BBB", "CCC"]},
        "computed_fields": [
            {"name": "momentum_20", "op": "indicator.momentum", "source": "close", "period": 20}
        ],
        "signals": {
            "entry": {"field": "close", "op": "gt", "value": 0},
            "exit": {"field": "close", "op": "lt", "value": 0},
        },
        "selection": {
            "eligible": {"field": "close", "op": "gt", "value": 0},
            "rank_by": "momentum_20",
            "top_n": 2,
        },
        "allocation": {"method": "equal_weight", "position_limit": 0.5},
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "fill_model": {"timing": "signal_close_for_next_bar", "price": "close_to_close"},
        "risk": {"max_positions": 2, "max_gross_exposure": 1.0, "long_short": "long_only"},
        "parameter_domains": {},
        "outputs": {"equity_curve": True},
        "metadata": {"strategy_id": "selection_timing_profile_signal_gate"},
    }

    normalized = mod.normalize_strategy_run_config(config)

    assert normalized["platform"]["strategy_profile_id"] == "selection_timing_portfolio"
    assert normalized["signals"]["entry"]["field"] == "close"
    assert normalized["signals"]["exit"]["field"] == "close"


def _monthly_long_short_rotation_config() -> dict:
    return {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "rotation_portfolio",
            "workflow_id": "single_backtest",
        },
        "data": {"provider": "yfinance", "frequency": "1D", "calendar": "XNYS"},
        "universe": {"symbols": ["SPY", "QQQ", "IWM", "EFA", "EEM", "TLT"]},
        "computed_fields": [
            {
                "name": "momentum_12_1",
                "op": "indicator.calendar_return",
                "source": "close",
                "sampling": "month_end",
                "start_lag": 12,
                "end_lag": 1,
            }
        ],
        "selection": {
            "eligible": {"field": "close", "op": "gt", "value": 0},
            "rank_by": "momentum_12_1",
            "rank_order": "desc",
            "long_top_n": 2,
            "short_bottom_n": 2,
            "tie_breaker": "symbol",
        },
        "allocation": {
            "method": "equal_weight_long_short",
            "long_gross_exposure": 0.5,
            "short_gross_exposure": 0.5,
        },
        "rebalance": {"trigger": {"op": "calendar.month_end"}},
        "fill_model": {
            "timing": "signal_close_for_next_bar",
            "price": "next_open",
            "cost": {
                "transaction_cost": 0.001,
                "slippage": 0.0,
                "short_borrow_rate_annual": 0.003,
                "borrow_day_count": 252,
            },
        },
        "risk": {
            "max_positions": 4,
            "max_gross_exposure": 1.0,
            "target_net_exposure": 0.0,
            "long_short": "market_neutral",
            "allow_short": True,
        },
        "parameter_domains": {},
        "outputs": {"equity_curve": True, "rebalance_audit": True},
        "metadata": {"strategy_id": "monthly_long_short_rotation_probe"},
    }


def test_rotation_profile_accepts_monthly_long_short_building_blocks() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])

    normalized = mod.normalize_strategy_run_config(_monthly_long_short_rotation_config())

    assert normalized["computed_fields"][0]["op"] == "indicator.calendar_return"
    assert normalized["selection"]["long_top_n"] == 2
    assert normalized["selection"]["short_bottom_n"] == 2
    assert normalized["allocation"]["method"] == "equal_weight_long_short"
    assert normalized["simulation"]["account"]["account_type"] == "margin"


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda config: config["risk"].update({"allow_short": False}), "allow_short"),
        (lambda config: config["risk"].update({"max_gross_exposure": 0.75}), "gross exposure"),
        (lambda config: config["computed_fields"][0].update({"start_lag": 1}), "start_lag"),
        (
            lambda config: config["fill_model"]["cost"].pop("short_borrow_rate_annual"),
            "short_borrow_rate_annual",
        ),
    ],
)
def test_monthly_long_short_contract_rejects_inconsistent_config(mutate, message: str) -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = _monthly_long_short_rotation_config()
    mutate(config)

    with pytest.raises(mod.StrategyRunConfigError, match=message):
        mod.normalize_strategy_run_config(config)


def test_pair_spread_portfolio_profile_normalizes_under_multi_asset_portfolio() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "pair_spread_portfolio",
            "workflow_id": "single_backtest",
        },
        "data": {"provider": "yfinance", "frequency": "1D", "calendar": "XNYS"},
        "universe": {"symbols": ["SPY", "QQQ"]},
        "computed_fields": [],
        "signals": {
            "entry": {"field": "close", "op": "gt", "value": 0},
            "exit": {"field": "close", "op": "lt", "value": 0},
        },
        "allocation": {"method": "fixed_weights"},
        "rebalance": {},
        "fill_model": {
            "timing": "timeline",
            "actions": [
                {
                    "signal": "entry",
                    "offset_bars": 1,
                    "price": "open",
                    "action": "set_target_weights",
                    "weights": {"SPY": 1.0, "QQQ": -1.0},
                },
                {
                    "signal": "exit",
                    "offset_bars": 1,
                    "price": "open",
                    "action": "flatten",
                },
            ],
        },
        "risk": {"allow_short": True, "long_short": "market_neutral", "max_gross_exposure": 2.0},
        "parameter_domains": {},
        "outputs": {"equity_curve": True},
        "metadata": {"strategy_id": "pair_spread_profile_probe"},
    }

    normalized = mod.normalize_strategy_run_config(config)
    plan = mod.plan_strategy_execution(normalized)

    assert normalized["platform"]["strategy_profile_id"] == "pair_spread_portfolio"
    assert plan["strategy_profile_id"] == "pair_spread_portfolio"
    assert plan["canonical_runtime_plan"]["profile_id"] == "pair_spread_portfolio"
    assert plan["canonical_runtime_plan"]["universe_shape"]["symbol_count"] == 2
    assert "pair_spread_compile" in plan["engine_capability_requirements"]["producer_requirements"]
    assert plan["canonical_runtime_plan"]["profile_contract"]["contract_kind"] == "pair_spread"
    assert plan["canonical_runtime_plan"]["profile_contract"]["has_negative_weight_leg"] is True
    assert plan["canonical_runtime_plan"]["profile_contract"]["event_phases"] == ["open"]


def test_multi_leg_event_profile_requires_non_empty_timeline_actions() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "multi_leg_event_portfolio",
            "workflow_id": "single_backtest",
        },
        "data": {"provider": "yfinance", "frequency": "1D", "calendar": "XNYS"},
        "universe": {"symbols": ["QQQ", "TLT", "GLD"]},
        "computed_fields": [],
        "signals": {"entry": {"field": "close", "op": "gt", "value": 0}},
        "allocation": {"method": "fixed_weights"},
        "rebalance": {},
        "fill_model": {"timing": "timeline", "actions": []},
        "risk": {"max_gross_exposure": 1.0},
        "parameter_domains": {},
        "outputs": {"equity_curve": True},
        "metadata": {"strategy_id": "multi_leg_profile_missing_actions"},
    }

    with pytest.raises(mod.StrategyRunConfigError, match="non-empty fill_model.actions"):
        mod.normalize_strategy_run_config(config)


def test_multi_leg_event_profile_emits_profile_contract() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "multi_leg_event_portfolio",
            "workflow_id": "single_backtest",
        },
        "data": {"provider": "yfinance", "frequency": "1D", "calendar": "XNYS"},
        "universe": {"symbols": ["QQQ", "TLT", "GLD"]},
        "computed_fields": [],
        "signals": {
            "entry": {
                "op": "calendar.nth_weekday_of_month",
                "ordinal": 3,
                "weekday": "friday",
            }
        },
        "allocation": {"method": "fixed_weights"},
        "fill_model": {
            "timing": "timeline",
            "actions": [
                {
                    "signal": "entry",
                    "offset_bars": 0,
                    "price": "open",
                    "action": "set_target_weights",
                    "weights": {"TLT": 0.5, "GLD": 0.5},
                },
                {
                    "signal": "entry",
                    "offset_bars": 0,
                    "price": "close",
                    "action": "set_target_weights",
                    "weights": {"QQQ": 1.0},
                },
            ],
        },
        "risk": {"max_gross_exposure": 1.0, "long_short": "long_only"},
        "parameter_domains": {},
        "outputs": {"equity_curve": True},
        "metadata": {"strategy_id": "multi_leg_profile_contract_probe"},
    }

    normalized = mod.normalize_strategy_run_config(config)
    plan = mod.plan_strategy_execution(normalized)

    assert "multi_leg_timeline_compile" in plan["engine_capability_requirements"]["producer_requirements"]
    assert plan["canonical_runtime_plan"]["profile_contract"]["contract_kind"] == "multi_leg_event"
    assert plan["canonical_runtime_plan"]["profile_contract"]["timeline_actions_count"] == 2
    assert plan["canonical_runtime_plan"]["profile_contract"]["event_phases"] == ["open", "close"]
    assert plan["canonical_runtime_plan"]["profile_contract"]["restore_actions_defined"] is True


def test_rotation_example_runtime_packet_describes_rolling_validation_support() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    path = _REPO_ROOT / VOO_GLD_ROTATION_EXAMPLE
    normalized = mod.normalize_strategy_run_config(_load(str(path.relative_to(_REPO_ROOT))), source_path=path)
    plan = mod.plan_strategy_execution(normalized)

    assert plan["canonical_runtime_plan"]["allocation_shape"]["method"] == "equal_weight"
    assert plan["canonical_runtime_plan"]["decision_shape"]["rank_by"] == "return_momentum"
    assert plan["engine_capability_requirements"]["supports_parameter_matrix"] is False
    assert plan["engine_capability_requirements"]["supports_rolling_validation"] is True
    assert plan["resolved_strategy_snapshot"]["allocation"]["method"] == "equal_weight"
    assert plan["resolved_strategy_snapshot"]["benchmark_symbol"] == "SPY"


def test_wfa_without_parameter_domains_is_rejected_instead_of_silently_reclassified() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    strategy_path = _REPO_ROOT / VOO_GLD_ROTATION_EXAMPLE
    strategy = mod.normalize_strategy_run_config(
        _load(str(strategy_path.relative_to(_REPO_ROOT))),
        source_path=strategy_path,
    )
    strategy["platform"]["workflow_id"] = "walk_forward_analysis"

    with pytest.raises(mod.StrategyRunConfigError, match="requires parameter_domains"):
        mod.plan_strategy_execution(strategy)


def test_wfa_config_normalizes_to_wfa_run_with_strategy_reference():
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    cases = [
        (QQQ_SMA_WFA_EXAMPLE, QQQ_SMA_EXAMPLE, 60, "research diagnostics only", "walk_forward_analysis"),
        (VOO_GLD_ROTATION_WFA_EXAMPLE, VOO_GLD_ROTATION_EXAMPLE, 1, "rolling validation", "rolling_validation"),
        (BTCUSDT_MONTHLY_NTH_WEEKDAY_WFA_EXAMPLE, BTCUSDT_MONTHLY_NTH_WEEKDAY_EXAMPLE, 28, "month_week 1-4", "walk_forward_analysis"),
    ]

    for wfa_path, strategy_path, candidate_limit, expected_note, workflow_id in cases:
        path = _REPO_ROOT / wfa_path
        normalized = mod.normalize_wfa_run_config(_load(str(path.relative_to(_REPO_ROOT))), source_path=path)

        assert normalized["schema_version"] == "wfa_run"
        assert normalized["engine"] == "unified_portfolio_wfa"
        assert normalized["optimizer"]["objectives"] == ["sharpe", "calmar"]
        assert normalized["optimizer"]["candidate_limit"] == candidate_limit
        assert normalized["platform"]["workflow_id"] == workflow_id
        assert normalized["outputs"]["selected_optimum"] is True
        assert normalized["strategy_run_path"].endswith(Path(strategy_path).name)
        assert normalized["metadata"]["local_research_only"] is True
        assert any(expected_note in note for note in normalized["metadata"]["notes"])


def test_wfa_config_accepts_strategy_run_path_as_primary_reference_field() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])

    normalized = mod.normalize_wfa_run_config(
        {
            "schema_version": "wfa_run",
            "strategy_run_path": "workspace/runs/strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json",
            "platform": {"workflow_id": "walk_forward_analysis"},
            "windowing": {"mode": "rolling", "train_ratio": 0.6, "test_ratio": 0.2, "step_size": 126},
            "optimizer": {"objectives": ["sharpe", "calmar"]},
            "acceptance": {},
            "outputs": {"selected_optimum": True, "candidate_diagnostics": True, "window_backtests": False},
        }
    )

    assert normalized["strategy_run_path"] == "workspace/runs/strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json"


def test_wfa_config_requires_an_explicit_validation_workflow_id() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])

    with pytest.raises(mod.StrategyRunConfigError, match="platform.workflow_id"):
        mod.normalize_wfa_run_config(
            {
                "schema_version": "wfa_run",
                "strategy_run_path": "workspace/runs/strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json",
                "windowing": {"mode": "rolling"},
                "optimizer": {},
                "acceptance": {},
                "outputs": {},
            }
        )


def test_wfa_normalizer_rejects_retired_runtime_shell() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])

    with pytest.raises(mod.StrategyRunConfigError, match="canonical wfa_run"):
        mod.normalize_wfa_run_config(
            {
                "wfa_config": {
                    "strategy_run_path": "workspace/runs/strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json",
                    "mode": "rolling",
                }
            }
        )


def test_resolve_wfa_strategy_run_path_uses_strategy_run_path_only() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])

    resolved = mod.resolve_wfa_strategy_run_path(
        {
            "schema_version": "wfa_run",
            "strategy_run_path": "workspace/runs/strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json",
            "platform": {"workflow_id": "walk_forward_analysis"},
        }
    )

    assert resolved == "workspace/runs/strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json"


def test_wfa_config_rejects_legacy_strategy_config_path_alias() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])

    with pytest.raises(mod.StrategyRunConfigError, match="must use strategy_run_path"):
        mod.normalize_wfa_run_config(
            {
                "schema_version": "wfa_run",
                "strategy_config_path": "workspace/runs/strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json",
                "platform": {"workflow_id": "walk_forward_analysis"},
                "windowing": {"mode": "rolling"},
                "optimizer": {},
                "acceptance": {},
                "outputs": {},
            }
        )


def test_wfa_loader_embeds_execution_plan_for_strategy_run_shell() -> None:
    loader_mod = __import__("validation_workflow.ConfigLoader_validation_workflow", fromlist=["dummy"])
    path = _REPO_ROOT / VOO_GLD_ROTATION_WFA_EXAMPLE

    loaded = loader_mod.ConfigLoader().load_config(str(path))

    assert loaded is not None
    execution_plan = loaded.backtester_config.get("execution_plan")
    assert isinstance(execution_plan, dict)
    assert execution_plan["schema_version"] == "execution_plan.v1"
    assert execution_plan["workflow_id"] == "rolling_validation"
    assert execution_plan["canonical_runtime_plan"]["workflow_shape"]["is_rolling_validation"] is True
    assert execution_plan["resolved_strategy_snapshot"]["strategy_mode_id"] == "multi_asset_portfolio"


def test_invalid_strategy_mode_fails_fast():
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    with pytest.raises(mod.StrategyRunConfigError, match="Unknown strategy_mode_id"):
        mod.validate_strategy_run_config(
            {
                "schema_version": "strategy_run",
                "platform": {"strategy_mode_id": "walk_forward_analysis", "workflow_id": "single_backtest"},
                "data": {},
                "universe": {"symbols": ["QQQ"]},
                "computed_fields": [],
                "allocation": {},
                "fill_model": {},
                "risk": {},
                "parameter_domains": {},
                "outputs": {},
            }
        )


def test_autorunner_validator_accepts_strategy_run_primary_config(tmp_path):
    validator_mod = __import__("autorunner.ConfigValidator_autorunner", fromlist=["dummy"])
    config = {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "selection_timing_portfolio",
            "workflow_id": "parameter_matrix",
        },
        "data": {"provider": "local", "frequency": "1D", "benchmark": "SPY"},
        "universe": {"symbols": ["VOO", "GLD"]},
        "computed_fields": [
            {"name": "return_momentum", "op": "indicator.momentum", "source": "close", "period": 20}
        ],
        "selection": {
            "eligible": {"field": "close", "op": "gt", "value": 0},
            "rank_by": "return_momentum",
            "rank_order": "desc",
            "top_n": 1,
        },
        "allocation": {"method": "equal_weight", "position_limit": 1.0},
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "fill_model": {"timing": "signal_close_for_next_bar", "price": "close_to_close"},
        "risk": {"max_positions": 1, "max_gross_exposure": 1.0, "long_short": "long_only"},
        "parameter_domains": {
            "return_lookback": {"type": "range", "start": 10, "end": 20, "step": 10}
        },
        "outputs": {"equity_curve": True, "holdings": True},
    }
    path = tmp_path / "strategy_run.json"
    path.write_text(json.dumps(config), encoding="utf-8")

    validator = validator_mod.ConfigValidator()

    assert validator.validate_config(str(path)) is True


def test_autorunner_validator_rejects_legacy_runtime_shell(tmp_path):
    validator_mod = __import__("autorunner.ConfigValidator_autorunner", fromlist=["dummy"])
    config = {
        "dataloader": {"source": "multi_asset", "frequency": "1D", "start_date": "2024-01-02"},
        "backtester": {
            "strategy_mode": "multi_asset_portfolio",
            "Backtest_id": "legacy_subdaily_probe",
            "market_data": {"interval": "1D"},
            "portfolio_config": {
                "strategy_id": "legacy_subdaily_probe",
                "universe": {"symbols": ["AAA", "BBB"]},
                "indicators": [],
                "rebalance": {"trigger": {"op": "calendar.every_session"}},
                "selection": {},
                "allocation": {"position_limit": 0.5},
                "execution": {"cost": {"transaction_cost": 0.0, "slippage": 0.0}},
            },
        },
        "metricstracker": {"enable_metrics_analysis": False},
    }
    path = tmp_path / "legacy_multi_asset_subdaily.json"
    path.write_text(json.dumps(config), encoding="utf-8")

    validator = validator_mod.ConfigValidator()

    assert validator.validate_config(str(path)) is False
    assert any(
        "schema_version=strategy_run" in message
        for message in validator.get_validation_errors(str(path))
    )


def test_autorunner_loader_accepts_strategy_run_primary_config(tmp_path):
    loader_mod = __import__("autorunner.ConfigLoader_autorunner", fromlist=["dummy"])
    config = {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "selection_timing_portfolio",
            "workflow_id": "parameter_matrix",
        },
        "data": {"provider": "yfinance", "frequency": "1D", "start_date": "2020-01-01"},
        "universe": {"symbols": ["VOO", "GLD"]},
        "computed_fields": [
            {"name": "return_momentum", "op": "indicator.momentum", "source": "close", "period": 20}
        ],
        "selection": {
            "eligible": {"field": "close", "op": "gt", "value": 0},
            "rank_by": "return_momentum",
            "rank_order": "desc",
            "top_n": 1,
        },
        "allocation": {"method": "equal_weight", "position_limit": 1.0},
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "fill_model": {"timing": "signal_close_for_next_bar", "price": "close_to_close"},
        "risk": {"max_positions": 1, "max_gross_exposure": 1.0, "long_short": "long_only"},
        "parameter_domains": {
            "return_lookback": {"type": "range", "start": 10, "end": 20, "step": 10}
        },
        "outputs": {"equity_curve": True, "holdings": True},
    }
    path = tmp_path / "strategy_run.json"
    path.write_text(json.dumps(config), encoding="utf-8")

    loaded = loader_mod.ConfigLoader().load_config(str(path))

    assert loaded is not None
    assert loaded.dataloader_config["source"] == "multi_asset"
    assert loaded.dataloader_config["asset_symbols"] == ["VOO", "GLD"]
    assert loaded.engine_request["schema_version"] == "engine_request.v1"
    assert loaded.engine_request["strategy"]["strategy_mode_id"] == "multi_asset_portfolio"
    assert loaded.engine_request["strategy"]["strategy_profile_id"] == "selection_timing_portfolio"
    assert "engine_request" not in loaded.backtester_config
    assert "strategy_run_config" not in loaded.backtester_config
    assert "condition_pairs" not in loaded.backtester_config
    assert "trading_params" not in loaded.backtester_config


def test_autorunner_loader_uses_internal_market_loader_for_calendar_example():
    loader_mod = __import__("autorunner.ConfigLoader_autorunner", fromlist=["dummy"])
    path = _REPO_ROOT / BTCUSDT_MONTHLY_NTH_WEEKDAY_EXAMPLE

    loaded = loader_mod.ConfigLoader().load_config(str(path))

    assert loaded is not None
    assert loaded.dataloader_config["source"] == "multi_asset"
    assert loaded.dataloader_config["asset_symbols"] == ["BTC-USD"]
    assert loaded.engine_request["strategy"]["strategy_mode_id"] == "multi_asset_portfolio"
    assert loaded.engine_request["data_requirements"]["provider"] == "coinbase"
    assert "engine_request" not in loaded.backtester_config
    assert "strategy_run_config" not in loaded.backtester_config
    assert "condition_pairs" not in loaded.backtester_config
    assert "trading_params" not in loaded.backtester_config


def test_wfa_loader_keeps_strategy_run_shell_free_of_legacy_defaults():
    loader_mod = __import__("validation_workflow.ConfigLoader_validation_workflow", fromlist=["dummy"])
    path = _REPO_ROOT / VOO_GLD_ROTATION_WFA_EXAMPLE

    loaded = loader_mod.ConfigLoader().load_config(str(path))

    assert loaded is not None
    assert loaded.backtester_config["strategy_run_config"]["schema_version"] == "strategy_run"
    assert "strategy_config" not in loaded.backtester_config
    assert "portfolio_config" not in loaded.backtester_config
    assert "condition_pairs" not in loaded.backtester_config
    assert "trading_params" not in loaded.backtester_config


def test_wfa_validator_rejects_referenced_subdaily_strategy_run(tmp_path):
    validator_mod = __import__("validation_workflow.ConfigValidator_validation_workflow", fromlist=["dummy"])
    strategy_path = tmp_path / "strategy_subdaily.json"
    wfa_path = tmp_path / "wfa_subdaily.json"
    strategy_config = {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "workflow_id": "single_backtest",
        },
        "data": {"provider": "yfinance", "frequency": "5m", "start_date": "2020-01-01"},
        "universe": {"symbols": ["VOO", "GLD"]},
        "computed_fields": [],
        "selection": {},
        "allocation": {"method": "equal_weight", "position_limit": 1.0},
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "fill_model": {"timing": "signal_close_for_next_bar", "price": "close_to_close"},
        "risk": {"max_positions": 1, "max_gross_exposure": 1.0, "long_short": "long_only"},
        "parameter_domains": {},
        "outputs": {"equity_curve": True},
        "metadata": {"strategy_id": "subdaily_strategy_probe"},
    }
    wfa_config = {
        "schema_version": "wfa_run",
        "strategy_run_path": strategy_path.name,
        "platform": {"workflow_id": "rolling_validation"},
        "windowing": {"mode": "rolling", "train_ratio": 0.6, "test_ratio": 0.2, "step_size": 30},
        "optimizer": {"objectives": ["sharpe", "calmar"]},
        "acceptance": {},
        "outputs": {"selected_optimum": True, "candidate_diagnostics": True, "window_backtests": False},
    }
    strategy_path.write_text(json.dumps(strategy_config), encoding="utf-8")
    wfa_path.write_text(json.dumps(wfa_config), encoding="utf-8")

    validator = validator_mod.ConfigValidator()

    assert validator.validate_config(str(wfa_path)) is False
    assert any("session-level bars only" in message for message in validator.get_validation_errors(str(wfa_path)))


def test_wfa_loader_rejects_referenced_subdaily_strategy_run(tmp_path):
    loader_mod = __import__("validation_workflow.ConfigLoader_validation_workflow", fromlist=["dummy"])
    strategy_path = tmp_path / "strategy_subdaily.json"
    wfa_path = tmp_path / "wfa_subdaily.json"
    strategy_config = {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "workflow_id": "single_backtest",
        },
        "data": {"provider": "yfinance", "interval": "1h", "start_date": "2020-01-01"},
        "universe": {"symbols": ["VOO", "GLD"]},
        "computed_fields": [],
        "selection": {},
        "allocation": {"method": "equal_weight", "position_limit": 1.0},
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "fill_model": {"timing": "signal_close_for_next_bar", "price": "close_to_close"},
        "risk": {"max_positions": 1, "max_gross_exposure": 1.0, "long_short": "long_only"},
        "parameter_domains": {},
        "outputs": {"equity_curve": True},
        "metadata": {"strategy_id": "subdaily_loader_probe"},
    }
    wfa_config = {
        "schema_version": "wfa_run",
        "strategy_run_path": strategy_path.name,
        "windowing": {"mode": "rolling", "train_ratio": 0.6, "test_ratio": 0.2, "step_size": 30},
        "optimizer": {"objectives": ["sharpe", "calmar"]},
        "acceptance": {},
        "outputs": {"selected_optimum": True, "candidate_diagnostics": True, "window_backtests": False},
    }
    strategy_path.write_text(json.dumps(strategy_config), encoding="utf-8")
    wfa_path.write_text(json.dumps(wfa_config), encoding="utf-8")

    assert loader_mod.ConfigLoader().load_config(str(wfa_path)) is None
