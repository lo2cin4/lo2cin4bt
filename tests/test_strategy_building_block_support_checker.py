import sys
from copy import deepcopy
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _typed_daily_data(provider: str = "local") -> dict:
    return {
        "provider": provider,
        "bar_time": {
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
                    "source": {"kind": "external", "provider_id": provider},
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
        },
        "stream_binding": {
            "execution_stream_id": "execution_daily",
            "decision_stream_id": "execution_daily",
        },
    }


def _base_strategy_config():
    return {
        "schema_version": "strategy_run",
        "platform": {"strategy_mode_id": "multi_asset_portfolio", "workflow_id": "single_backtest"},
        "data": _typed_daily_data(),
        "universe": {"symbols": ["QQQ"]},
        "computed_fields": [],
        "signals": {},
        "selection": {},
        "allocation": {"method": "equal_weight", "position_limit": 1.0},
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "fill_model": {"timing": "signal_close_for_next_bar", "price": "close_to_close"},
        "risk": {"max_positions": 1, "max_gross_exposure": 1.0, "long_short": "long_only"},
        "parameter_domains": {},
        "outputs": {"equity_curve": True},
    }


def test_strategy_run_support_checker_accepts_top_level_rsi_computed_field() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    checker_mod = __import__("backtester.ops.support_checker", fromlist=["dummy"])
    config = _base_strategy_config()
    config["computed_fields"] = [{"name": "rsi_14", "op": "indicator.rsi", "source": "close", "period": 14}]
    config["signals"] = {
        "entry": {"field": "rsi_14", "op": "lt", "value": 20},
        "exit": {"field": "rsi_14", "op": "gt", "value": 80},
        "target_weight": 1.0,
    }
    config["rebalance"] = {"trigger": {"op": "calendar.first_session"}}

    mod.validate_strategy_run_config(config)
    assert checker_mod.strategy_run_support_report(config)["supported"] is True


def test_strategy_run_support_checker_rejects_inline_rsi_feature() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = _base_strategy_config()
    config["selection"] = {
        "eligible": {
            "field": {"feature": "indicator.rsi", "source": "close", "period": 14},
            "op": "lt",
            "value": 30,
        },
        "rank_by": "close",
        "top_n": 1,
    }

    with pytest.raises(mod.StrategyRunConfigError, match="selection\\.eligible\\.left\\.feature"):
        mod.validate_strategy_run_config(config)


def test_strategy_run_support_checker_rejects_all_public_inline_features() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = _base_strategy_config()
    config["selection"] = {
        "eligible": {
            "field": {"feature": "indicator.sma", "source": "close", "period": 20},
            "op": "gt",
            "value": 100,
        },
        "rank_by": "close",
        "top_n": 1,
    }

    with pytest.raises(mod.StrategyRunConfigError, match="inline feature nodes are not part of the public"):
        mod.validate_strategy_run_config(config)


def test_strategy_run_support_checker_accepts_public_computed_fields() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = _base_strategy_config()
    config["computed_fields"] = [
        {"name": "ema_20", "op": "indicator.ema", "source": "close", "period": 20},
        {"name": "z_20", "op": "indicator.zscore", "source": "close", "period": 20},
        {"name": "p90_20", "op": "indicator.percentile", "source": "close", "period": 20, "percentile": 90},
        {"name": "bb_upper_20", "op": "indicator.bollinger", "source": "close", "period": 20, "band": "upper"},
        {"name": "atr_14", "op": "indicator.atr", "period": 14},
        {"name": "macd_line", "op": "indicator.macd", "source": "close", "output": "line"},
        {"name": "macd_signal", "op": "indicator.macd", "source": "close", "output": "signal"},
        {"name": "macd_histogram", "op": "indicator.macd", "source": "close", "output": "histogram"},
    ]
    config["selection"] = {
        "eligible": {"field": "close", "op": "gt", "right_field": "ema_20"},
        "rank_by": "z_20",
        "top_n": 1,
    }

    normalized = mod.normalize_strategy_run_config(config)

    assert "features" not in normalized
    assert [item["name"] for item in normalized["computed_fields"]] == [
        "ema_20",
        "z_20",
        "p90_20",
        "bb_upper_20",
        "atr_14",
        "macd_line",
        "macd_signal",
        "macd_histogram",
    ]


def test_strategy_run_support_checker_rejects_combined_computed_fields_and_legacy_aliases() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = _base_strategy_config()
    config["features"] = [{"name": "sma_3", "op": "indicator.sma", "source": "close", "period": 3}]
    config["computed_fields"] = [{"name": "ema_3", "op": "indicator.ema", "source": "close", "period": 3}]

    with pytest.raises(mod.StrategyRunConfigError, match="computed_fields\\[\\]"):
        mod.validate_strategy_run_config(config)


@pytest.mark.parametrize(
    ("section", "expected"),
    [
        ("computed_fields", "computed_fields[] entries must be objects"),
        ("indicators", "indicators[] has been retired; use computed_fields[]"),
        ("features", "features[] has been retired; use computed_fields[]"),
    ],
)
def test_strategy_run_support_checker_reports_malformed_alias_section(section, expected) -> None:
    checker_mod = __import__("backtester.ops.support_checker", fromlist=["dummy"])
    config = _base_strategy_config()
    config[section] = ["not-an-object"]

    report = checker_mod.strategy_run_support_report(config)

    assert report["supported"] is False
    expected_path = f"{section}[0]" if section == "computed_fields" else section
    assert report["issues"][0]["path"] == expected_path
    assert report["issues"][0]["reason"] == expected


def test_strategy_run_support_checker_rejects_retired_execution_section() -> None:
    checker_mod = __import__("backtester.ops.support_checker", fromlist=["dummy"])
    config = _base_strategy_config()
    config["execution"] = {"timing": "signal_close_for_next_bar"}

    report = checker_mod.strategy_run_support_report(config)

    assert report["supported"] is False
    assert report["issues"][0]["path"] == "execution"
    assert report["issues"][0]["reason"] == "execution{} has been retired; use fill_model{}"


def test_strategy_run_support_checker_rejects_unknown_op_before_runtime() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = _base_strategy_config()
    config["computed_fields"] = [{"name": "vcp", "op": "indicator.vcp", "source": "close"}]

    with pytest.raises(mod.StrategyRunConfigError, match="computed_fields\\[0\\]\\.op"):
        mod.validate_strategy_run_config(config)


@pytest.mark.parametrize("field_name", ["frequency", "interval", "calendar", "timezone"])
def test_strategy_run_support_checker_rejects_legacy_data_time_fields(field_name: str) -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = _base_strategy_config()
    config["data"][field_name] = "legacy"

    with pytest.raises(mod.StrategyRunConfigError, match="legacy time fields"):
        mod.validate_strategy_run_config(config)


def test_strategy_run_support_checker_rejects_subdaily_market_data_timeframes() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = _base_strategy_config()
    config["data"]["market_data"] = {"provider": "binance", "interval": "300"}

    with pytest.raises(mod.StrategyRunConfigError, match="legacy time fields"):
        mod.validate_strategy_run_config(config)


def test_strategy_run_support_checker_rejects_invalid_indicator_enum_before_runtime() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = _base_strategy_config()
    config["computed_fields"] = [
        {"name": "bad_macd", "op": "indicator.macd", "source": "close", "output": "macd_signal"}
    ]

    with pytest.raises(mod.StrategyRunConfigError, match="computed_fields\\[0\\]\\.output"):
        mod.validate_strategy_run_config(config)


@pytest.mark.parametrize(
    "op_name",
    [
        "sma",
        "ta.sma",
        "ema",
        "ta.ema",
        "momentum",
        "return",
        "volatility",
        "rolling_volatility",
        "atr",
        "ta.atr",
        "average_true_range",
        "rsi",
        "ta.rsi",
        "macd",
        "ta.macd",
        "indicator.macd_signal",
        "macd_signal",
        "ta.macd_signal",
        "zscore",
        "ta.zscore",
        "percentile",
        "rolling_percentile",
        "bollinger",
        "bollinger_band",
        "ta.bollinger",
    ],
)
def test_strategy_run_support_checker_rejects_indicator_alias_ops(op_name) -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = _base_strategy_config()
    config["computed_fields"] = [{"name": "alias_probe", "op": op_name, "source": "close", "period": 20}]

    with pytest.raises(mod.StrategyRunConfigError, match="computed_fields\\[0\\]\\.op"):
        mod.validate_strategy_run_config(config)


def test_strategy_run_support_checker_respects_runtime_site_spelling() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    ops_mod = __import__("backtester.ops", fromlist=["dummy"])
    config = _base_strategy_config()
    config["signals"] = {
        "entry": {"field": "close", "op": ">", "value": 0},
        "exit": {"field": "close", "op": "<", "value": 999999},
        "target_weight": 1.0,
    }

    mod.validate_strategy_run_config(config)

    registry = ops_mod.build_registry()
    assert registry.is_supported(">", ops_mod.MULTI_ASSET_CONDITION)
    assert not registry.is_supported(">", ops_mod.NODE_IR_CONDITION)

    bad = deepcopy(config)
    bad["signals"]["entry"] = {"field": "close", "op": "and", "value": 0}
    with pytest.raises(mod.StrategyRunConfigError, match="spelling is not accepted"):
        mod.validate_strategy_run_config(bad)


def test_strategy_run_support_checker_accepts_timeline_offset_param_ref() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    checker_mod = __import__("backtester.ops.support_checker", fromlist=["dummy"])
    config = _base_strategy_config()
    config["platform"] = {"strategy_mode_id": "multi_asset_portfolio", "workflow_id": "parameter_matrix"}
    config["universe"] = {"symbols": ["SPY", "IEF"]}
    config["signals"] = {"entry": {"field": "market_breadth", "op": "crosses_below", "value": 10}}
    config["allocation"] = {"method": "fixed_weights", "weights": {"SPY": 1.0}}
    config["rebalance"] = {"trigger": {"op": "target.change"}}
    config["fill_model"] = {
        "timing": "timeline",
        "position_policy": {"on_entry_signal_while_holding": "reset_timer"},
        "actions": [
            {
                "signal": "entry",
                "offset_bars": 1,
                "price": "open",
                "action": "set_target_weights",
                "weights": {"IEF": 1.0},
            },
            {
                "signal": "entry",
                "offset_bars": {"param_ref": "delay_bars"},
                "price": "close",
                "action": "set_target_weights",
                "weights": {"SPY": 1.0},
            },
        ],
    }
    config["parameter_domains"] = {"delay_bars": {"type": "range", "start": 10, "end": 30, "step": 10}}

    mod.validate_strategy_run_config(config)
    assert checker_mod.strategy_run_support_report(config)["supported"] is True


def test_strategy_run_support_checker_rejects_zero_delay_dynamic_selection() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = _base_strategy_config()
    config["computed_fields"] = [
        {"name": "momentum_20", "op": "indicator.momentum", "source": "close", "period": 20}
    ]
    config["selection"] = {"rank_by": "momentum_20", "rank_order": "desc", "top_n": 1}
    config["fill_model"] = {
        "timing": "timeline",
        "actions": [
            {
                "signal": "rebalance",
                "offset_bars": 0,
                "price": "close",
                "action": "set_target_weights",
            }
        ],
    }

    with pytest.raises(mod.StrategyRunConfigError, match="pre-session-known calendar or static action"):
        mod.validate_strategy_run_config(config)


def test_strategy_run_support_checker_accepts_zero_delay_preknown_calendar_action() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    checker_mod = __import__("backtester.ops.support_checker", fromlist=["dummy"])
    config = _base_strategy_config()
    config["signals"] = {
        "entry": {
            "op": "calendar.nth_weekday_of_month",
            "ordinal": 3,
            "weekday": "friday",
        }
    }
    config["selection"] = {}
    config["allocation"] = {"method": "fixed_weights"}
    config["fill_model"] = {
        "timing": "timeline",
        "actions": [
            {
                "signal": "entry",
                "offset_bars": 0,
                "price": "open",
                "action": "set_target_weights",
                "weights": {"QQQ": 1.0},
            }
        ],
    }

    mod.validate_strategy_run_config(config)
    assert checker_mod.strategy_run_support_report(config)["supported"] is True


def test_strategy_run_support_checker_rejects_zero_delay_data_signal() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = _base_strategy_config()
    config["signals"] = {"entry": {"field": "close", "op": "gt", "value": 100}}
    config["selection"] = {}
    config["allocation"] = {"method": "fixed_weights", "weights": {"QQQ": 1.0}}
    config["fill_model"] = {
        "timing": "timeline",
        "actions": [
            {
                "signal": "entry",
                "offset_bars": 0,
                "price": "close",
                "action": "set_target_weights",
                "weights": {"QQQ": 1.0},
            }
        ],
    }

    with pytest.raises(mod.StrategyRunConfigError, match="data-dependent signals"):
        mod.validate_strategy_run_config(config)


def test_strategy_run_support_checker_rejects_template_ops_at_runtime() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])
    config = _base_strategy_config()
    config["computed_fields"] = [{"name": "starter", "op": "template.single_asset_ma_cross"}]

    with pytest.raises(mod.StrategyRunConfigError, match="template\\.\\* building blocks"):
        mod.validate_strategy_run_config(config)


def test_strategy_run_support_checker_rejects_malformed_condition_shapes() -> None:
    mod = __import__("backtester.StrategyRunConfig_backtester", fromlist=["dummy"])

    string_node = _base_strategy_config()
    string_node["signals"] = {"entry": "indicator.future_magic"}
    with pytest.raises(mod.StrategyRunConfigError, match="condition nodes must be objects or lists"):
        mod.validate_strategy_run_config(string_node)

    malformed_all = _base_strategy_config()
    malformed_all["selection"] = {"eligible": {"all": {"field": "close", "op": "gt", "value": 0}}}
    with pytest.raises(mod.StrategyRunConfigError, match="all must be a list"):
        mod.validate_strategy_run_config(malformed_all)

    malformed_any = _base_strategy_config()
    malformed_any["selection"] = {"eligible": {"any": {"field": "close", "op": "gt", "value": 0}}}
    with pytest.raises(mod.StrategyRunConfigError, match="any must be a list"):
        mod.validate_strategy_run_config(malformed_any)
