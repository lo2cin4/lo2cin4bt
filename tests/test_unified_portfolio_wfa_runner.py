import importlib
import json
import atexit
import shutil
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest


_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backtester.EngineRequest_backtester import build_engine_request  # noqa: E402
from dataloader.market_data_bundle import build_market_data_bundle  # noqa: E402
from dataloader.market_data_loader import market_data_spec_from_requirements  # noqa: E402


_BUNDLE_ROOT = Path(tempfile.mkdtemp(prefix="lo2cin4bt-wfa-bundles-"))
atexit.register(shutil.rmtree, _BUNDLE_ROOT, True)


def _wfa_runner(runner_mod, *, market_data, strategy_config, wfa_config):
    request = build_engine_request(
        strategy_config,
        request_id="test:wfa:market_data",
        run_scope="single",
    )
    spec = market_data_spec_from_requirements(request["data_requirements"])
    close_index = pd.DatetimeIndex(market_data["close"].index)
    if len(close_index) > 1 and close_index.sort_values().to_series().diff().dropna().min() < pd.Timedelta(days=1):
        spec["index_kind"] = "event_timestamp"
    bundle = build_market_data_bundle(
        market_data,
        spec=spec,
        output_root=_BUNDLE_ROOT,
    )
    return runner_mod.UnifiedPortfolioWFARunner(
        market_data_bundle=bundle,
        strategy_config=strategy_config,
        wfa_config=wfa_config,
    )


def test_wfa_equity_contract_rejects_missing_contribution_column():
    runner_mod = importlib.import_module(
        "validation_workflow.UnifiedPortfolioWFARunner_validation_workflow"
    )
    equity = pd.DataFrame(
            {
                "Equity_value": [100.0, 101.0],
                "Portfolio_return": [0.0, 0.01],
                "Turnover": [0.0, 1.0],
            "Trade_cost": [0.0, 0.1],
            "Gross_exposure": [1.0, 1.0],
            "Selected_count": [1, 1],
            "Weight_AAA": [1.0, 1.0],
        }
    )

    with pytest.raises(ValueError, match="missing Contribution columns"):
        runner_mod.UnifiedPortfolioWFARunner._validated_equity_contract(equity)


def test_wfa_equity_contract_rejects_nan_instead_of_treating_it_as_zero():
    runner_mod = importlib.import_module(
        "validation_workflow.UnifiedPortfolioWFARunner_validation_workflow"
    )
    equity = pd.DataFrame(
            {
                "Equity_value": [100.0, 101.0],
                "Portfolio_return": [0.0, 0.01],
                "Turnover": [0.0, float("nan")],
            "Trade_cost": [0.0, 0.1],
            "Gross_exposure": [1.0, 1.0],
            "Selected_count": [1, 1],
            "Weight_AAA": [1.0, 1.0],
            "Contribution_AAA": [0.0, 0.01],
        }
    )

    with pytest.raises(ValueError, match="invalid numeric values in Turnover"):
        runner_mod.UnifiedPortfolioWFARunner._validated_equity_contract(equity)


def _market_data():
    dates = pd.date_range("2023-01-02", periods=90, freq="B")
    close = pd.DataFrame(
        {
            "AAA": [100.0 + idx * 0.4 for idx in range(len(dates))],
            "BBB": [130.0 - idx * 0.15 + max(0, idx - 45) * 0.8 for idx in range(len(dates))],
        },
        index=dates,
    )
    return {"close": close}


def _intraday_market_data():
    dates = pd.date_range("2023-01-02 09:30", periods=12, freq="h")
    close = pd.DataFrame(
        {
            "AAA": [100.0 + idx * 0.2 for idx in range(len(dates))],
            "BBB": [105.0 + idx * 0.1 for idx in range(len(dates))],
        },
        index=dates,
    )
    return {"close": close}


def _canonical_strategy_config(body: dict) -> dict:
    config = dict(body)
    metadata = config.get("metadata")
    if not isinstance(metadata, dict) or not str(metadata.get("strategy_id") or ""):
        raise AssertionError("Canonical WFA test config requires metadata.strategy_id")
    universe = config.get("universe") if isinstance(config.get("universe"), dict) else {}
    symbols = [str(item) for item in universe.get("symbols", [])]
    domains = config.get("parameter_domains")
    has_domains = isinstance(domains, dict) and bool(domains)
    has_decision_logic = bool(config.get("selection") or config.get("signals"))
    platform = {
        "strategy_mode_id": "multi_asset_portfolio",
        "strategy_profile_id": (
            "selection_timing_portfolio"
            if has_decision_logic
            else "allocation_portfolio"
        ),
        "workflow_id": (
            "walk_forward_analysis" if has_domains else "rolling_validation"
        ),
    }
    if len(symbols) == 1 and config.get("signals"):
        platform["strategy_preset_id"] = "single_asset_signal"

    config["schema_version"] = "strategy_run"
    config["platform"] = platform
    config.setdefault(
        "data",
        {"provider": "synthetic", "frequency": "1D", "calendar": "XNYS"},
    )
    config.setdefault("computed_fields", [])
    config.setdefault("signals", {})
    config.setdefault("selection", {})
    config.setdefault("allocation", {})
    config.setdefault("rebalance", {})
    config.setdefault("fill_model", {})
    config.setdefault(
        "risk",
        {
            "max_positions": max(1, len(symbols)),
            "max_gross_exposure": 1.0,
            "allow_short": False,
        },
    )
    config.setdefault("parameter_domains", {})
    config.setdefault("outputs", {"equity_curve": True})
    return config


def test_unified_portfolio_wfa_rejects_subdaily_strategy_frequency():
    runner_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFARunner_validation_workflow")
    strategy_config = {
        "metadata": {"strategy_id": "subdaily_wfa_probe"},
        "data": {"provider": "yfinance", "frequency": "1h"},
        "universe": {"symbols": ["AAA", "BBB"]},
        "parameter_domains": {},
        "selection": {},
        "allocation": {"method": "equal_weight", "position_limit": 1.0},
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "fill_model": {"cost": {"transaction_cost": 0.0, "slippage": 0.0}},
    }

    with pytest.raises(ValueError, match="session-level bars only"):
        _wfa_runner(runner_mod,
            market_data=_market_data(),
            strategy_config=_canonical_strategy_config(strategy_config),
            wfa_config={},
        )


def test_unified_portfolio_wfa_rejects_subdaily_market_data_index_spacing():
    runner_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFARunner_validation_workflow")
    strategy_config = {
        "metadata": {"strategy_id": "subdaily_wfa_index_probe"},
        "data": {"provider": "yfinance", "frequency": "1D"},
        "universe": {"symbols": ["AAA", "BBB"]},
        "parameter_domains": {},
        "selection": {},
        "allocation": {"method": "equal_weight", "position_limit": 1.0},
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "fill_model": {"cost": {"transaction_cost": 0.0, "slippage": 0.0}},
    }

    with pytest.raises(ValueError, match="index spacing"):
        _wfa_runner(runner_mod,
            market_data=_intraday_market_data(),
            strategy_config=_canonical_strategy_config(strategy_config),
            wfa_config={},
        )


def test_unified_portfolio_wfa_exports_selected_optimum_per_window():
    runner_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFARunner_validation_workflow")
    strategy_config = {
        "metadata": {"strategy_id": "unified_wfa_probe"},
        "universe": {"symbols": ["AAA", "BBB"]},
        "parameter_domains": {"lookback": {"type": "range", "start": 2, "end": 4, "step": 2}},
        "computed_fields": [
            {
                "name": "momentum",
                "op": "indicator.momentum",
                "source": "close",
                "period": {"param_ref": "lookback"},
            }
        ],
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "selection": {
            "eligible": {"field": "close", "op": "gt", "value": 0},
            "rank_by": "momentum",
            "rank_order": "desc",
            "top_n": 1,
        },
        "allocation": {"method": "equal_weight", "position_limit": 1.0},
        "fill_model": {"cost": {"transaction_cost": 0.0, "slippage": 0.0}},
    }
    wfa_config = {
        "windowing": {"train_size": 35, "test_size": 10, "step_size": 20},
        "optimizer": {"objectives": ["sharpe"]},
    }

    result = _wfa_runner(runner_mod,
        market_data=_market_data(),
        strategy_config=_canonical_strategy_config(strategy_config),
        wfa_config=wfa_config,
    ).run()

    assert result.metadata["workflow"] == "walk_forward_analysis"
    assert result.metadata["row_contract"] == "selected_optimum_per_window"
    assert result.metadata["candidate_count"] == 2
    assert result.metadata["candidate_budget_policy"] == "full_grid"
    assert result.metadata["candidate_budget_method"] == "full_grid"
    assert result.metadata["candidate_budget_seed"] is None
    assert result.selected_optimum["wfa_row_type"].unique().tolist() == ["selected_optimum"]
    assert result.selected_optimum.groupby(["window_id", "objective"]).size().max() == 1
    assert set(result.selected_optimum["candidate_count"].unique().tolist()) == {2}
    assert set(result.selected_optimum["candidate_budget_policy"].unique().tolist()) == {"full_grid"}
    assert set(result.selected_optimum["candidate_budget_method"].unique().tolist()) == {"full_grid"}
    assert result.selected_optimum["candidate_budget_seed"].isna().all()
    assert {"accepted", "review_status", "acceptance_reasons", "oos_is_ratio"}.issubset(
        result.selected_optimum.columns
    )
    assert "oos_portfolio_json" in result.selected_optimum.columns
    assert {
        "is_risk_gate_event_count",
        "oos_risk_gate_event_count",
        "oos_risk_gate_summary_json",
    }.issubset(result.selected_optimum.columns)
    assert set(result.selected_optimum["oos_risk_gate_event_count"].unique().tolist()) == {0}
    portfolio_snapshot = json.loads(result.selected_optimum["oos_portfolio_json"].iloc[0])
    assert portfolio_snapshot["asset_count"] == 2
    assert portfolio_snapshot["allocation"]
    assert portfolio_snapshot["contribution"]
    assert portfolio_snapshot["risk_gate_event_count"] == 0
    assert set(result.candidate_diagnostics["wfa_row_type"].unique().tolist()) == {
        "candidate_diagnostic"
    }
    assert result.window_backtests
    first_window_backtest = result.window_backtests[0]
    assert first_window_backtest["backtest_id"].startswith("wfa_window_001_sharpe_")
    assert first_window_backtest["oos_result"].strategy_id == first_window_backtest["backtest_id"]
    assert first_window_backtest["oos_result"].config["metadata"]["source_workflow"] == "walk_forward_analysis"
    assert first_window_backtest["oos_result"].config["metadata"]["wfa_window_id"] == 1
    assert first_window_backtest["oos_result"].config["engine_run_scope"] == "validation_test_window"
    assert first_window_backtest["oos_result"].config["engine_request_id"]
    assert len(first_window_backtest["oos_result"].config["engine_request_hash"]) == 64


def test_unified_portfolio_wfa_candidate_budget_is_reported():
    runner_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFARunner_validation_workflow")
    strategy_config = {
        "metadata": {"strategy_id": "budget_probe"},
        "universe": {"symbols": ["AAA", "BBB"]},
        "parameter_domains": {"lookback": [2, 4, 6, 8, 10]},
        "computed_fields": [
            {
                "name": "momentum",
                "op": "indicator.momentum",
                "source": "close",
                "period": {"param_ref": "lookback"},
            }
        ],
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "selection": {
            "eligible": {"field": "close", "op": "gt", "value": 0},
            "rank_by": "momentum",
            "rank_order": "desc",
            "top_n": 1,
        },
        "allocation": {"method": "equal_weight", "position_limit": 1.0},
        "fill_model": {"cost": {"transaction_cost": 0.0, "slippage": 0.0}},
    }

    result = _wfa_runner(runner_mod,
        market_data=_market_data(),
        strategy_config=_canonical_strategy_config(strategy_config),
        wfa_config={
            "windowing": {"train_size": 35, "test_size": 10, "step_size": 20},
            "optimizer": {"objectives": ["sharpe"], "n_trials": 2, "random_seed": 7},
        },
    ).run()

    assert result.metadata["candidate_count"] == 2
    assert result.metadata["total_candidate_count"] == 5
    assert result.metadata["candidate_budget"] == 2
    assert result.metadata["candidate_budget_applied"] is True
    assert result.metadata["candidate_budget_policy"] == "seeded_random_sample"
    assert result.metadata["candidate_budget_method"] == "seeded_random_sample"
    assert result.metadata["candidate_budget_seed"] == 7
    assert set(result.selected_optimum["candidate_count"].unique().tolist()) == {2}
    assert set(result.selected_optimum["total_candidate_count"].unique().tolist()) == {5}
    assert result.selected_optimum["candidate_budget_applied"].unique().tolist() == [True]
    assert set(result.selected_optimum["candidate_budget_policy"].unique().tolist()) == {"seeded_random_sample"}
    assert set(result.selected_optimum["candidate_budget_method"].unique().tolist()) == {"seeded_random_sample"}
    assert set(result.selected_optimum["candidate_budget_seed"].unique().tolist()) == {7}
    assert "sampled 2/5 candidates" in result.selected_optimum["selection_evidence"].iloc[0]


def test_unified_portfolio_wfa_sampled_single_candidate_remains_wfa_and_preserves_zero_seed():
    runner_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFARunner_validation_workflow")
    strategy_config = {
        "metadata": {"strategy_id": "budget_one_probe"},
        "universe": {"symbols": ["AAA", "BBB"]},
        "parameter_domains": {"lookback": [2, 4, 6]},
        "computed_fields": [
            {
                "name": "momentum",
                "op": "indicator.momentum",
                "source": "close",
                "period": {"param_ref": "lookback"},
            }
        ],
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "selection": {
            "eligible": {"field": "close", "op": "gt", "value": 0},
            "rank_by": "momentum",
            "rank_order": "desc",
            "top_n": 1,
        },
        "allocation": {"method": "equal_weight", "position_limit": 1.0},
        "fill_model": {"cost": {"transaction_cost": 0.0, "slippage": 0.0}},
    }

    result = _wfa_runner(runner_mod,
        market_data=_market_data(),
        strategy_config=_canonical_strategy_config(strategy_config),
        wfa_config={
            "windowing": {"train_size": 35, "test_size": 10, "step_size": 20},
            "optimizer": {"objectives": ["sharpe"], "max_candidates": 1, "random_seed": 0},
        },
    ).run()

    assert result.metadata["workflow"] == "walk_forward_analysis"
    assert result.metadata["candidate_count"] == 1
    assert result.metadata["total_candidate_count"] == 3
    assert result.metadata["candidate_budget_policy"] == "seeded_random_sample"
    assert result.metadata["candidate_budget_seed"] == 0
    assert result.selected_optimum["workflow"].unique().tolist() == ["walk_forward_analysis"]
    assert result.selected_optimum["candidate_budget_seed"].unique().tolist() == [0]


def test_unified_portfolio_wfa_full_grid_budget_policy_is_reported():
    runner_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFARunner_validation_workflow")
    strategy_config = {
        "metadata": {"strategy_id": "full_grid_budget_probe"},
        "universe": {"symbols": ["AAA", "BBB"]},
        "parameter_domains": {"lookback": [2, 4]},
        "computed_fields": [
            {
                "name": "momentum",
                "op": "indicator.momentum",
                "source": "close",
                "period": {"param_ref": "lookback"},
            }
        ],
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "selection": {
            "eligible": {"field": "close", "op": "gt", "value": 0},
            "rank_by": "momentum",
            "rank_order": "desc",
            "top_n": 1,
        },
        "allocation": {"method": "equal_weight", "position_limit": 1.0},
        "fill_model": {"cost": {"transaction_cost": 0.0, "slippage": 0.0}},
    }

    result = _wfa_runner(runner_mod,
        market_data=_market_data(),
        strategy_config=_canonical_strategy_config(strategy_config),
        wfa_config={"windowing": {"train_size": 35, "test_size": 10, "step_size": 20}},
    ).run()

    assert result.metadata["candidate_budget_policy"] == "full_grid"
    assert result.metadata["candidate_budget_method"] == "full_grid"
    assert result.metadata["candidate_budget_seed"] is None
    assert result.selected_optimum["candidate_budget_policy"].unique().tolist() == ["full_grid"]
    assert result.selected_optimum["candidate_budget_method"].unique().tolist() == ["full_grid"]


def test_unified_portfolio_wfa_manual_ratio_windowing_is_reported():
    runner_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFARunner_validation_workflow")
    strategy_config = {
        "metadata": {"strategy_id": "ratio_window_probe"},
        "universe": {"symbols": ["AAA", "BBB"]},
        "parameter_domains": {"lookback": [2, 4]},
        "computed_fields": [
            {
                "name": "momentum",
                "op": "indicator.momentum",
                "source": "close",
                "period": {"param_ref": "lookback"},
            }
        ],
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "selection": {
            "eligible": {"field": "close", "op": "gt", "value": 0},
            "rank_by": "momentum",
            "rank_order": "desc",
            "top_n": 1,
        },
        "allocation": {"method": "equal_weight", "position_limit": 1.0},
        "fill_model": {"cost": {"transaction_cost": 0.0, "slippage": 0.0}},
    }

    result = _wfa_runner(runner_mod,
        market_data=_market_data(),
        strategy_config=_canonical_strategy_config(strategy_config),
        wfa_config={
            "windowing": {
                "size_mode": "ratio",
                "train_ratio": 0.5,
                "test_ratio": 0.2,
                "step_size": 7,
            },
            "optimizer": {"objectives": ["sharpe"]},
        },
    ).run()

    assert result.metadata["windowing"]["size_mode"] == "manual_ratio"
    assert result.metadata["windowing"]["sizing_source"] == "input_ratios"
    assert result.metadata["windowing"]["effective_train_size"] == 45
    assert result.metadata["windowing"]["effective_test_size"] == 18
    assert result.metadata["windowing"]["effective_step_size"] == 7


def test_unified_portfolio_wfa_auto_windowing_uses_parameter_domain_lookback():
    runner_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFARunner_validation_workflow")
    dates = pd.date_range("2022-01-03", periods=120, freq="B")
    close = pd.DataFrame(
        {
            "AAA": [100.0 + idx * 0.3 for idx in range(len(dates))],
            "BBB": [100.0 + idx * 0.1 for idx in range(len(dates))],
        },
        index=dates,
    )
    strategy_config = {
        "metadata": {"strategy_id": "auto_window_probe"},
        "universe": {"symbols": ["AAA", "BBB"]},
        "parameter_domains": {"lookback": {"type": "range", "start": 10, "end": 30, "step": 10}},
        "computed_fields": [
            {
                "name": "momentum",
                "op": "indicator.momentum",
                "source": "close",
                "period": {"param_ref": "lookback"},
            }
        ],
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "selection": {
            "eligible": {"field": "close", "op": "gt", "value": 0},
            "rank_by": "momentum",
            "rank_order": "desc",
            "top_n": 1,
        },
        "allocation": {"method": "equal_weight", "position_limit": 1.0},
        "fill_model": {"cost": {"transaction_cost": 0.0, "slippage": 0.0}},
    }

    result = _wfa_runner(runner_mod,
        market_data={"close": close},
        strategy_config=_canonical_strategy_config(strategy_config),
        wfa_config={"windowing": {"size_mode": "auto", "target_window_count": 3}},
    ).run()

    assert result.metadata["windowing"]["size_mode"] == "auto"
    assert result.metadata["windowing"]["strategy_max_lookback"] == 30
    assert result.metadata["windowing"]["effective_train_size"] >= 60
    assert result.metadata["windowing"]["auto_indicators"]["min_train_size"] >= 60


def test_unified_portfolio_wfa_filters_low_viability_is_candidates_before_ranking():
    runner_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFARunner_validation_workflow")
    strategy_config = {
        "metadata": {"strategy_id": "viability_filter_probe"},
        "universe": {"symbols": ["AAA", "BBB"]},
        "parameter_domains": {"lookback": [2, 80]},
        "computed_fields": [
            {
                "name": "momentum",
                "op": "indicator.momentum",
                "source": "close",
                "period": {"param_ref": "lookback"},
            }
        ],
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "selection": {
            "eligible": {"field": "close", "op": "gt", "value": 0},
            "rank_by": "momentum",
            "rank_order": "desc",
            "top_n": 1,
        },
        "allocation": {"method": "equal_weight", "position_limit": 1.0},
        "fill_model": {"cost": {"transaction_cost": 0.0, "slippage": 0.0}},
    }

    result = _wfa_runner(runner_mod,
        market_data=_market_data(),
        strategy_config=_canonical_strategy_config(strategy_config),
        wfa_config={
            "windowing": {"train_size": 35, "test_size": 10, "step_size": 20},
            "optimizer": {
                "objectives": ["sharpe"],
                "selection_constraints": {
                    "enabled": True,
                    "max_lookback_fraction_of_train": 0.5,
                },
            },
        },
    ).run()

    assert result.metadata["selection_constraints"]["enabled"] is True
    assert set(result.selected_optimum["semantic_combo"].unique().tolist()) == {
        '{"lookback": 2}'
    }
    assert set(result.selected_optimum["selection_pool_count"].unique().tolist()) == {1}
    rejected = result.candidate_diagnostics[
        result.candidate_diagnostics["semantic_combo"].eq('{"lookback": 80}')
    ]
    assert not rejected.empty
    assert rejected["candidate_viability_pass"].eq(False).all()
    assert rejected["candidate_viability_reasons"].str.contains("lookback_fraction").all()


def test_unified_portfolio_wfa_fails_when_no_candidate_passes_constraints():
    runner_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFARunner_validation_workflow")
    strategy_config = {
        "metadata": {"strategy_id": "fallback_honesty_probe"},
        "universe": {"symbols": ["AAA", "BBB"]},
        "parameter_domains": {"lookback": [80]},
        "computed_fields": [
            {
                "name": "momentum",
                "op": "indicator.momentum",
                "source": "close",
                "period": {"param_ref": "lookback"},
            }
        ],
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "selection": {
            "eligible": {"field": "close", "op": "gt", "value": 0},
            "rank_by": "momentum",
            "rank_order": "desc",
            "top_n": 1,
        },
        "allocation": {"method": "equal_weight", "position_limit": 1.0},
        "fill_model": {"cost": {"transaction_cost": 0.0, "slippage": 0.0}},
    }

    with pytest.raises(ValueError, match="No WFA candidate passed"):
        _wfa_runner(runner_mod,
            market_data=_market_data(),
            strategy_config=_canonical_strategy_config(strategy_config),
            wfa_config={
                "windowing": {"train_size": 35, "test_size": 10, "step_size": 20},
                "optimizer": {
                    "objectives": ["sharpe"],
                    "selection_constraints": {
                        "enabled": True,
                        "max_lookback_fraction_of_train": 0.5,
                    },
                },
                "acceptance": {"min_oos_sharpe": -1.0, "min_oos_is_ratio": 0.0},
            },
        ).run()


def test_unified_portfolio_wfa_rejects_missing_viability_evidence() -> None:
    runner_mod = importlib.import_module(
        "validation_workflow.UnifiedPortfolioWFARunner_validation_workflow"
    )
    runner = object.__new__(runner_mod.UnifiedPortfolioWFARunner)
    runner.selection_constraints = {"enabled": True}

    with pytest.raises(ValueError, match="viability evidence"):
        runner._candidate_selection_pool(  # noqa: SLF001
            [{"candidate": {"params": {"lookback": 20}}, "metrics": {"sharpe": 1.0}}]
        )


def test_unified_portfolio_wfa_bool_strings_do_not_enable_constraints():
    runner_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFARunner_validation_workflow")
    strategy_config = {
        "metadata": {"strategy_id": "bool_string_probe"},
        "universe": {"symbols": ["AAA", "BBB"]},
        "parameter_domains": {"lookback": [2]},
        "computed_fields": [
            {
                "name": "momentum",
                "op": "indicator.momentum",
                "source": "close",
                "period": {"param_ref": "lookback"},
            }
        ],
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "selection": {
            "eligible": {"field": "close", "op": "gt", "value": 0},
            "rank_by": "momentum",
            "rank_order": "desc",
            "top_n": 1,
        },
        "allocation": {"method": "equal_weight", "position_limit": 1.0},
        "fill_model": {"cost": {"transaction_cost": 0.0, "slippage": 0.0}},
    }

    result = _wfa_runner(runner_mod,
        market_data=_market_data(),
        strategy_config=_canonical_strategy_config(strategy_config),
        wfa_config={
            "windowing": {"train_size": 35, "test_size": 10, "step_size": 20},
            "optimizer": {
                "objectives": ["sharpe"],
                "selection_constraints": {
                    "enabled": "false",
                    "max_lookback_fraction_of_train": 0.5,
                },
            },
        },
    ).run()

    assert result.metadata["selection_constraints"]["enabled"] is False


def test_unified_portfolio_wfa_has_no_constraint_fallback_opt_in():
    runner_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFARunner_validation_workflow")
    strategy_config = {
        "metadata": {"strategy_id": "fallback_opt_in_probe"},
        "universe": {"symbols": ["AAA", "BBB"]},
        "parameter_domains": {"lookback": [80]},
        "computed_fields": [
            {
                "name": "momentum",
                "op": "indicator.momentum",
                "source": "close",
                "period": {"param_ref": "lookback"},
            }
        ],
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "selection": {
            "eligible": {"field": "close", "op": "gt", "value": 0},
            "rank_by": "momentum",
            "rank_order": "desc",
            "top_n": 1,
        },
        "allocation": {"method": "equal_weight", "position_limit": 1.0},
        "fill_model": {"cost": {"transaction_cost": 0.0, "slippage": 0.0}},
    }

    with pytest.raises(ValueError, match="No WFA candidate passed"):
        _wfa_runner(runner_mod,
            market_data=_market_data(),
            strategy_config=_canonical_strategy_config(strategy_config),
            wfa_config={
                "windowing": {"train_size": 35, "test_size": 10, "step_size": 20},
                "optimizer": {
                    "objectives": ["sharpe"],
                    "selection_constraints": {
                        "enabled": True,
                        "max_lookback_fraction_of_train": 0.5,
                    },
                },
                "acceptance": {
                    "min_oos_sharpe": -1.0,
                    "min_oos_is_ratio": 0.0,
                    "allow_selection_constraints_fallback_acceptance": True,
                },
            },
        ).run()


def test_unified_portfolio_wfa_carries_risk_gate_counts():
    runner_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFARunner_validation_workflow")
    strategy_config = {
        "metadata": {"strategy_id": "risk_gate_wfa_probe"},
        "universe": {"symbols": ["AAA", "BBB"]},
        "parameter_domains": {"lookback": [2, 4]},
        "computed_fields": [
            {
                "name": "momentum",
                "op": "indicator.momentum",
                "source": "close",
                "period": {"param_ref": "lookback"},
            }
        ],
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "selection": {
            "eligible": {"field": "close", "op": "gt", "value": 0},
            "rank_by": "momentum",
            "rank_order": "desc",
            "top_n": 2,
        },
        "allocation": {"method": "equal_weight", "position_limit": 1.0},
        "fill_model": {"cost": {"transaction_cost": 0.0, "slippage": 0.0}},
        "risk": {"max_positions": 1, "gate_action": "block_new_orders"},
    }

    result = _wfa_runner(runner_mod,
        market_data=_market_data(),
        strategy_config=_canonical_strategy_config(strategy_config),
        wfa_config={"windowing": {"train_size": 35, "test_size": 10, "step_size": 20}},
    ).run()

    assert (result.selected_optimum["is_risk_gate_event_count"] > 0).any()
    assert (result.selected_optimum["oos_risk_gate_event_count"] > 0).any()
    snapshot = json.loads(result.selected_optimum["oos_portfolio_json"].iloc[0])
    assert snapshot["risk_gate_event_count"] > 0
    assert snapshot["risk_gate_summary"]["event_count"] == snapshot["risk_gate_event_count"]


def test_unified_portfolio_wfa_treats_fixed_policy_as_rolling_validation():
    runner_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFARunner_validation_workflow")
    strategy_config = {
        "metadata": {"strategy_id": "fixed_policy_probe"},
        "universe": {"symbols": ["AAA", "BBB"]},
        "computed_fields": [],
        "rebalance": {"trigger": {"op": "calendar.month_start"}},
        "allocation": {"method": "fixed_weights", "weights": {"AAA": 0.5, "BBB": 0.5}},
        "fill_model": {"cost": {"transaction_cost": 0.0, "slippage": 0.0}},
    }
    wfa_config = {
        "windowing": {"train_size": 30, "test_size": 10, "step_size": 25},
        "optimizer": {"objectives": ["sharpe"]},
    }

    result = _wfa_runner(runner_mod,
        market_data=_market_data(),
        strategy_config=_canonical_strategy_config(strategy_config),
        wfa_config=wfa_config,
    ).run()

    assert result.metadata["workflow"] == "rolling_validation"
    assert result.metadata["candidate_count"] == 1
    assert result.selected_optimum["workflow"].unique().tolist() == ["rolling_validation"]
    assert result.selected_optimum["semantic_combo"].unique().tolist() == [
        '{"policy": "fixed"}'
    ]
    assert result.window_backtests
    assert {
        item["oos_result"].config["metadata"]["source_workflow"]
        for item in result.window_backtests
    } == {"rolling_validation"}


def test_unified_portfolio_wfa_reuses_same_oos_backtest_across_objectives(monkeypatch):
    runner_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFARunner_validation_workflow")
    call_sizes = []

    def fake_rust_candidates(
        self, *, candidates, market_data, run_id_base, run_scope
    ):
        del self, run_id_base, run_scope
        size = len(market_data["close"].index)
        call_sizes.append(size)
        dates = market_data["close"].index
        results = []
        for candidate in candidates:
            equity = pd.DataFrame(
                {
                    "Time": dates,
                    "Equity_value": [
                        100.0 + idx - (2.0 if idx > 0 and idx % 5 == 0 else 0.0)
                        for idx in range(len(dates))
                    ],
                    "Portfolio_return": [0.0] + [0.01 for _ in range(max(len(dates) - 1, 0))],
                    "Turnover": [0.0 for _ in dates],
                    "Trade_cost": [0.0 for _ in dates],
                    "Selected_count": [1 for _ in dates],
                    "Gross_exposure": [1.0 for _ in dates],
                    "Cash_weight": [0.0 for _ in dates],
                    "Weight_AAA": [1.0 for _ in dates],
                    "Contribution_AAA": [0.0] + [0.01 for _ in range(max(len(dates) - 1, 0))],
                }
            )
            results.append(
                SimpleNamespace(
                    strategy_id=candidate.get("candidate_id", "cached_oos_probe"),
                    config=dict(candidate.get("config") or {}),
                    equity_curve=equity,
                    rebalance_audit=pd.DataFrame(),
                    risk_gate_events=pd.DataFrame(),
                    validation_report={"accounting_fast_path": "rust_test_double"},
                )
            )
        return results

    monkeypatch.setattr(
        runner_mod.UnifiedPortfolioWFARunner,
        "_run_candidates_with_rust",
        fake_rust_candidates,
    )

    strategy_config = {
        "metadata": {"strategy_id": "cached_oos_probe"},
        "universe": {"symbols": ["AAA"]},
        "computed_fields": [],
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "allocation": {"method": "fixed_weights", "weights": {"AAA": 1.0}},
        "fill_model": {"cost": {"transaction_cost": 0.0, "slippage": 0.0}},
    }
    result = _wfa_runner(runner_mod,
        market_data={"close": _market_data()["close"][["AAA"]]},
        strategy_config=_canonical_strategy_config(strategy_config),
        wfa_config={
            "windowing": {"train_size": 35, "test_size": 10, "step_size": 20},
            "optimizer": {"objectives": ["sharpe", "calmar"]},
        },
    ).run()

    assert result.metadata["window_count"] == 3
    assert call_sizes.count(35) == result.metadata["window_count"]
    assert call_sizes.count(10) == result.metadata["window_count"]
    assert len(call_sizes) == result.metadata["window_count"] * 2
    assert set(result.selected_optimum["objective"].unique().tolist()) == {"sharpe", "calmar"}
    assert len(result.window_backtests) == result.metadata["window_count"] * 2
    first_window_ids = [
        item["backtest_id"]
        for item in result.window_backtests
        if int(item["window_id"]) == 1
    ]
    assert len(first_window_ids) == 2
    assert len(set(first_window_ids)) == 2


def test_unified_portfolio_wfa_can_optimize_single_asset_position_state_strategy():
    runner_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFARunner_validation_workflow")
    dates = pd.date_range("2023-01-02", periods=80, freq="B")
    close = pd.DataFrame(
        {"QQQ": [100.0 + idx * 0.2 + (idx % 11) * 0.4 for idx in range(len(dates))]},
        index=dates,
    )
    strategy_config = {
        "metadata": {"strategy_id": "single_signal_wfa_probe"},
        "universe": {"symbols": ["QQQ"]},
        "parameter_domains": {"ma_period": [2, 4]},
        "computed_fields": [
            {
                "name": "fast_ma",
                "op": "indicator.sma",
                "source": "close",
                "period": {"param_ref": "ma_period"},
            },
            {
                "name": "slow_ma",
                "op": "indicator.sma",
                "source": "close",
                "period": 8,
            }
        ],
        "signals": {
            "entry": {"field": "fast_ma", "op": "crosses_above", "right_field": "slow_ma"},
            "exit": {"field": "fast_ma", "op": "crosses_below", "right_field": "slow_ma"},
            "target_weight": 1.0,
        },
        "allocation": {"method": "position_state"},
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "fill_model": {
            "actions": [
                {"signal": "entry", "offset_bars": 1, "price": "open", "action": "enter"},
                {"signal": "exit", "offset_bars": 1, "price": "open", "action": "exit"},
            ],
            "cost": {"transaction_cost": 0.0, "slippage": 0.0},
        },
    }
    wfa_config = {
        "windowing": {"train_size": 30, "test_size": 10, "step_size": 20},
        "optimizer": {"objectives": ["sharpe"]},
    }

    result = _wfa_runner(runner_mod,
        market_data={"close": close, "open": close.copy()},
        strategy_config=_canonical_strategy_config(strategy_config),
        wfa_config=wfa_config,
    ).run()

    assert result.metadata["workflow"] == "walk_forward_analysis"
    assert result.metadata["candidate_count"] == 2
    assert set(result.selected_optimum["semantic_combo"].tolist()).issubset(
        {'{"ma_period": 2}', '{"ma_period": 4}'}
    )
    assert result.selected_optimum["wfa_row_type"].unique().tolist() == ["selected_optimum"]


def test_unified_portfolio_wfa_exporter_separates_selected_and_diagnostics(tmp_path):
    runner_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFARunner_validation_workflow")
    exporter_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFAExporter_validation_workflow")
    strategy_config = {
        "metadata": {"strategy_id": "export_probe"},
        "universe": {"symbols": ["AAA", "BBB"]},
        "parameter_domains": {"lookback": [2, 4]},
        "computed_fields": [
            {
                "name": "momentum",
                "op": "indicator.momentum",
                "source": "close",
                "period": {"param_ref": "lookback"},
            }
        ],
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "selection": {
            "eligible": {"field": "close", "op": "gt", "value": 0},
            "rank_by": "momentum",
            "rank_order": "desc",
            "top_n": 1,
        },
        "allocation": {"method": "equal_weight", "position_limit": 1.0},
        "fill_model": {"cost": {"transaction_cost": 0.0, "slippage": 0.0}},
    }
    result = _wfa_runner(runner_mod,
        market_data=_market_data(),
        strategy_config=_canonical_strategy_config(strategy_config),
        wfa_config={"windowing": {"train_size": 35, "test_size": 10, "step_size": 20}},
    ).run()

    paths = exporter_mod.UnifiedPortfolioWFAExporter(
        result=result,
        output_dir=tmp_path,
        run_id="wfa_unified_probe",
    ).export()

    assert any(path.endswith("_selected_optimum.parquet") for path in paths)
    assert any(path.endswith("_candidate_diagnostics.parquet") for path in paths)
    metadata_path = next(path for path in paths if path.endswith("_metadata.json"))
    metadata = exporter_mod.load_unified_wfa_metadata(metadata_path)
    assert metadata["row_contract"] == "selected_optimum_per_window"
    assert metadata["legacy_grid_detected"] is False


def test_unified_portfolio_wfa_exporter_bounds_long_run_id(tmp_path):
    runner_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFARunner_validation_workflow")
    exporter_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFAExporter_validation_workflow")
    result = runner_mod.UnifiedPortfolioWFAResult(
        selected_optimum=pd.DataFrame({"window_id": [1], "objective": ["sharpe"]}),
        candidate_diagnostics=pd.DataFrame({"window_id": [1], "rank": [1]}),
        window_backtests=[],
        metadata={"row_contract": "selected_optimum_per_window"},
    )

    paths = exporter_mod.UnifiedPortfolioWFAExporter(
        result=result,
        output_dir=tmp_path,
        run_id="wfa_" + ("very_long_strategy_identifier_" * 10),
    ).export()

    assert paths
    assert all(len(Path(path).name) <= 120 for path in paths)


def test_unified_portfolio_wfa_requires_positive_oos_for_acceptance():
    runner_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFARunner_validation_workflow")
    dates = pd.date_range("2024-01-02", periods=45, freq="B")
    close = pd.DataFrame(
        {"AAA": [100.0 + idx for idx in range(25)] + [125.0 - idx * 2 for idx in range(20)]},
        index=dates,
    )
    strategy_config = {
        "metadata": {"strategy_id": "negative_oos_probe"},
        "universe": {"symbols": ["AAA"]},
        "computed_fields": [],
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "allocation": {"method": "fixed_weights", "weights": {"AAA": 1.0}},
        "fill_model": {"cost": {"transaction_cost": 0.0, "slippage": 0.0}},
    }
    result = _wfa_runner(runner_mod,
        market_data={"close": close},
        strategy_config=_canonical_strategy_config(strategy_config),
        wfa_config={
            "windowing": {"train_size": 20, "test_size": 10, "step_size": 20},
            "optimizer": {"objectives": ["sharpe"]},
            "acceptance": {"min_oos_sharpe": 0.0, "require_positive_oos": True},
        },
    ).run()

    assert bool(result.selected_optimum["accepted"].iloc[0]) is False
    assert result.selected_optimum["review_status"].iloc[0] == "Review"


def test_walk_forward_engine_routes_multi_asset_config_to_unified_runner(tmp_path):
    config_mod = importlib.import_module("validation_workflow.ConfigLoader_validation_workflow")
    engine_mod = importlib.import_module("validation_workflow.WalkForwardEngine_validation_workflow")
    dates = pd.date_range("2023-01-02", periods=70, freq="B")
    close = pd.DataFrame(
        {
            "Time": dates,
            "AAA": [100.0 + idx * 0.3 for idx in range(len(dates))],
            "BBB": [120.0 - idx * 0.1 + max(0, idx - 35) * 0.7 for idx in range(len(dates))],
        }
    )
    close_path = tmp_path / "close.csv"
    close.to_csv(close_path, index=False)
    output_dir = tmp_path / "wfa_outputs"
    config_payload = {
        "wfa_config": {
            "workflow_id": "walk_forward_analysis",
            "engine": "unified_portfolio",
            "run_id": "managed_unified_probe",
            "windowing": {"train_size": 30, "test_size": 10, "step_size": 20},
            "optimizer": {"objectives": ["sharpe"]},
            "outputs": {"output_dir": str(output_dir), "candidate_diagnostics": True},
        },
        "dataloader": {"source": "multi_asset"},
        "backtester": {
            "strategy_mode": "multi_asset_portfolio",
            "market_data": {"close": {"path": str(close_path), "time_column": "Time"}},
            "strategy_run_config": {
                "schema_version": "strategy_run",
                "platform": {
                    "strategy_mode_id": "multi_asset_portfolio",
                    "strategy_profile_id": "selection_timing_portfolio",
                    "workflow_id": "walk_forward_analysis",
                },
                "data": {
                    "provider": "file",
                    "frequency": "1D",
                    "calendar": "XNYS",
                    "close": {"path": str(close_path), "time_column": "Time"},
                },
                "universe": {"symbols": ["AAA", "BBB"]},
                "parameter_domains": {"lookback": [2, 4]},
                "computed_fields": [
                    {
                        "name": "momentum",
                        "op": "indicator.momentum",
                        "source": "close",
                        "period": {"param_ref": "lookback"},
                    }
                ],
                "rebalance": {"trigger": {"op": "calendar.every_session"}},
                "selection": {
                    "eligible": {"field": "close", "op": "gt", "value": 0},
                    "rank_by": "momentum",
                    "rank_order": "desc",
                    "top_n": 1,
                },
                "allocation": {"method": "equal_weight", "position_limit": 1.0},
                "fill_model": {
                    "timing": "signal_close_for_next_bar",
                    "price": "close_to_close",
                    "cost": {"transaction_cost": 0.0, "slippage": 0.0},
                },
                "risk": {"max_positions": 1, "max_gross_exposure": 1.0, "allow_short": False},
                "outputs": {"equity_curve": True},
                "metadata": {"strategy_id": "managed_unified_wfa_probe"},
            },
        },
        "metricstracker": {},
    }
    config_path = tmp_path / "wfa_config.json"
    config_path.write_text("{}", encoding="utf-8")
    config_data = config_mod.WFAConfigData(config_payload, str(config_path))

    result = engine_mod.WalkForwardEngine(config_data).run()

    assert result["contract_audit"]["runtime"] == "unified_portfolio_wfa"
    assert result["metadata"]["row_contract"] == "selected_optimum_per_window"
    assert result["selected_optimum"]["wfa_row_type"].unique().tolist() == ["selected_optimum"]
    assert any(path.endswith("_selected_optimum.parquet") for path in result["exported_files"])


def test_wfa_shell_preserves_unified_engine_flag():
    config_mod = importlib.import_module("backtester.StrategyRunConfig_backtester")
    loader_mod = importlib.import_module("validation_workflow.ConfigLoader_validation_workflow")

    normalized = config_mod.normalize_wfa_run_config(
        {
            "schema_version": "wfa_run",
            "engine": "unified_portfolio",
            "strategy_run_path": "workspace/runs/backtest_fixture.json",
            "platform": {"workflow_id": "walk_forward_analysis"},
            "windowing": {"mode": "rolling", "target_window_count": 3},
            "optimizer": {"objectives": ["sharpe"]},
            "acceptance": {},
            "outputs": {"selected_optimum": True},
        }
    )

    runtime_config = loader_mod.ConfigLoader._wfa_config_from_strategy_run(normalized)

    assert normalized["engine"] == "unified_portfolio"
    assert runtime_config["engine"] == "unified_portfolio"


def test_strategy_run_wfa_train_candidates_use_rust_batch_after_portfolio_normalization():
    runner_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFARunner_validation_workflow")

    dates = pd.date_range("2023-01-02", periods=70, freq="B")
    close = pd.DataFrame({"QQQ": [100.0 + idx for idx in range(len(dates))]}, index=dates)
    open_ = close * 1.001
    strategy_config = {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "selection_timing_portfolio",
            "strategy_preset_id": "single_asset_signal",
            "workflow_id": "parameter_matrix",
        },
        "data": {"provider": "synthetic", "frequency": "1D", "calendar": "XNYS"},
        "universe": {"symbols": ["QQQ"]},
        "computed_fields": [
            {"name": "sma_fast", "op": "indicator.sma", "source": "close", "period": {"param_ref": "fast"}},
            {"name": "sma_slow", "op": "indicator.sma", "source": "close", "period": 20},
        ],
        "signals": {
            "entry": {"field": "sma_fast", "op": "gt", "right_field": "sma_slow"},
            "exit": {"field": "sma_fast", "op": "le", "right_field": "sma_slow"},
        },
        "allocation": {"method": "position_state", "target_weight": 1.0},
        "fill_model": {
            "timing": "timeline",
            "actions": [
                {"signal": "entry", "offset_bars": 1, "price": "open", "action": "enter"},
                {"signal": "exit", "offset_bars": 1, "price": "open", "action": "exit"},
            ],
            "cost": {"transaction_cost": 0.0, "slippage": 0.0},
        },
        "parameter_domains": {"fast": [5, 10]},
        "risk": {"max_positions": 1, "max_gross_exposure": 1.0, "allow_short": False},
        "metadata": {"strategy_id": "strategy_run_wfa_rust_batch_probe"},
    }

    result = _wfa_runner(runner_mod,
        market_data={"close": close, "open": open_},
        strategy_config=_canonical_strategy_config(strategy_config),
        wfa_config={
            "windowing": {"train_size": 30, "test_size": 10, "step_size": 20},
            "optimizer": {"objectives": ["sharpe"]},
        },
    ).run()

    assert result.metadata["train_backend_counts"] == {
        "signal_rust_engine_request_batch": 2
    }
    assert set(result.candidate_diagnostics["train_backend"].unique()) == {
        "signal_rust_engine_request_batch"
    }


def test_multi_candidate_wfa_fails_when_rust_batch_producer_is_missing(monkeypatch):
    runner_mod = importlib.import_module("validation_workflow.UnifiedPortfolioWFARunner_validation_workflow")
    bridge_mod = importlib.import_module("backtester.UnifiedBacktestRunner_backtester")

    def empty_batch(*args, **kwargs):
        return None

    monkeypatch.setattr(
        bridge_mod.UnifiedBacktestRunnerBacktester,
        "_try_run_grouped_engine_request_batch",
        empty_batch,
    )

    dates = pd.date_range("2023-01-02", periods=70, freq="B")
    close = pd.DataFrame({"AAA": [100.0 + idx for idx in range(len(dates))]}, index=dates)
    strategy_config = {
        "metadata": {"strategy_id": "missing_rust_wfa_probe"},
        "universe": {"symbols": ["AAA"]},
        "parameter_domains": {"lookback": [2, 4]},
        "computed_fields": [
            {
                "name": "momentum",
                "op": "indicator.momentum",
                "source": "close",
                "period": {"param_ref": "lookback"},
            }
        ],
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "selection": {
            "eligible": {"field": "close", "op": "gt", "value": 0},
            "rank_by": "momentum",
            "rank_order": "desc",
            "top_n": 1,
        },
        "allocation": {"method": "equal_weight", "position_limit": 1.0},
        "fill_model": {"cost": {"transaction_cost": 0.0, "slippage": 0.0}},
    }

    runner = _wfa_runner(runner_mod,
        market_data={"close": close},
        strategy_config=_canonical_strategy_config(strategy_config),
        wfa_config={
            "windowing": {"train_size": 30, "test_size": 10, "step_size": 20},
            "optimizer": {"objectives": ["sharpe"]},
        },
    )

    with pytest.raises(RuntimeError, match="unsupported_engine_request_shape"):
        runner.run()
