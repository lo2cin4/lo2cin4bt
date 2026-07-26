from __future__ import annotations

import math

import pytest

from validation_workflow.ConfigValidator_validation_workflow import ConfigValidator
from validation_workflow.HeatmapMatrixBuilder_validation_workflow import (
    HeatmapMatrixBuilder,
)
from validation_workflow.OptunaSearchEngine_validation_workflow import (
    OptunaSearchEngine,
)
from validation_workflow.RobustSelector_validation_workflow import RobustSelector
from validation_workflow.WFAAcceptanceEvaluator_validation_workflow import (
    WFAAcceptanceEvaluator,
)


def test_wfa_validator_accepts_optuna_blocks(tmp_path) -> None:
    strategy_path = tmp_path / "strategy_run.json"
    config_path = tmp_path / "wfa_optuna.json"
    strategy_path.write_text(
        """
        {
          "schema_version": "strategy_run",
          "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "workflow_id": "walk_forward_analysis"
          },
          "data": {
            "provider": "yfinance",
            "frequency": "1D",
            "start_date": "2024-01-01"
          },
          "universe": {"symbols": ["QQQ"]},
          "computed_fields": [],
          "selection": {},
          "allocation": {"method": "equal_weight", "position_limit": 1.0},
          "rebalance": {"trigger": {"op": "calendar.every_session"}},
          "fill_model": {"timing": "signal_close_for_next_bar", "price": "close_to_close"},
          "risk": {"max_positions": 1, "max_gross_exposure": 1.0, "long_short": "long_only"},
              "parameter_domains": {"lookback": [10, 20]},
          "outputs": {"equity_curve": true},
          "metadata": {"strategy_id": "wfa-optuna-validator-smoke"}
        }
        """,
        encoding="utf-8",
    )
    config_path.write_text(
        """
            {
              "schema_version": "wfa_run",
              "platform": {"workflow_id": "walk_forward_analysis"},
              "strategy_run_path": "strategy_run.json",
          "windowing": {
            "mode": "rolling",
            "train_ratio": 0.6,
            "test_ratio": 0.2,
            "step_size": 30
          },
          "optimizer": {
            "type": "optuna",
            "mode": "single_objective",
            "sampler": "tpe",
            "multivariate": true,
            "n_trials": 24,
            "n_startup_trials": 8,
            "timeout_seconds": 300,
            "pruner": "hyperband",
            "objectives": ["sharpe", "calmar"]
          },
          "acceptance": {
            "min_oos_is_ratio": 0.7,
            "min_trade_count": 3
          },
          "outputs": {
            "selected_optimum": true,
            "candidate_diagnostics": true,
            "window_backtests": false
          }
        }
        """,
        encoding="utf-8",
    )
    validator = ConfigValidator()
    assert validator.validate_config(str(config_path)) is True


def test_heatmap_matrix_builder_builds_payload() -> None:
    builder = HeatmapMatrixBuilder()
    payload = builder.build_payload(
        run_id="run_1",
        param_axes=["fast_ma", "slow_ma"],
        rows=[
            {
                "backtest_id": "a",
                "label": "A",
                "semantic_combo": {"fast_ma": 10, "slow_ma": 20},
                "sharpe": 1.2,
                "total_return": 0.3,
                "max_drawdown": -0.2,
                "trade_count": 12,
                "exposure_time": 80.0,
            },
            {
                "backtest_id": "b",
                "label": "B",
                "semantic_combo": {"fast_ma": 15, "slow_ma": 20},
                "sharpe": 1.5,
                "total_return": 0.35,
                "max_drawdown": -0.18,
                "trade_count": 14,
                "exposure_time": 85.0,
            },
        ],
    )
    assert payload["contract_id"].endswith("parameter-heatmap-payload-v2")
    assert payload["default_x_axis"] == "fast_ma"
    assert payload["default_y_axis"] == "slow_ma"
    assert payload["rows"]
    assert payload["wfa_pack_previews"]["balanced"]["candidate_count"] >= 1
    assert payload["shortlist_rows"][0]["candidate_key"]

    matrix = builder.build_matrix(
        rows=payload["rows"],
        x_axis="fast_ma",
        y_axis="slow_ma",
        objective="sharpe",
    )
    assert matrix["x_values"] == [10, 15]
    assert matrix["y_values"] == [20]
    assert matrix["z"][0][0] == 1.2


def test_heatmap_matrix_builder_rejects_missing_parameter_coordinates() -> None:
    builder = HeatmapMatrixBuilder()

    with pytest.raises(ValueError, match="parameter coordinate"):
        builder.build_payload(
            run_id="missing_coordinate",
            param_axes=["fast_ma", "slow_ma"],
            rows=[
                {
                    "backtest_id": "missing_slow",
                    "label": "Missing slow MA",
                    "semantic_combo": {"fast_ma": 10},
                    "sharpe": 1.2,
                    "total_return": 0.3,
                    "max_drawdown": -0.2,
                    "trade_count": 12,
                    "exposure_time": 80.0,
                }
            ],
        )


def test_heatmap_shortlist_preserves_portfolio_snapshot_metrics() -> None:
    builder = HeatmapMatrixBuilder()
    payload = builder.build_payload(
        run_id="portfolio_run",
        param_axes=["lookback", "sma_period"],
        rows=[
            {
                "backtest_id": "portfolio_a",
                "label": "Portfolio A",
                "semantic_combo": {"lookback": 20, "sma_period": 60},
                "sharpe": 0.9,
                "total_return": 1.4,
                "cagr": 0.12,
                "calmar": 0.6,
                "max_drawdown": -0.2,
                "trade_count": 18,
                "rebalance_count": 18,
                "exposure_time": 0.85,
                "final_equity": 240.0,
            },
            {
                "backtest_id": "portfolio_b",
                "label": "Portfolio B",
                "semantic_combo": {"lookback": 30, "sma_period": 60},
                "sharpe": 0.8,
                "total_return": 1.1,
                "cagr": 0.1,
                "calmar": 0.5,
                "max_drawdown": -0.22,
                "trade_count": 14,
                "rebalance_count": 14,
                "exposure_time": 0.75,
                "final_equity": 210.0,
            },
        ],
    )

    first = next(
        row for row in payload["shortlist_rows"] if row["backtest_id"] == "portfolio_a"
    )
    assert first["rebalance_count"] == 18
    assert first["cagr"] == 0.12
    assert first["calmar"] == 0.6
    assert first["exposure_time"] == 0.85
    assert first["final_equity"] == 240.0


def test_heatmap_accepts_portfolio_candidates_without_trade_diagnostics() -> None:
    builder = HeatmapMatrixBuilder()
    payload = builder.build_payload(
        run_id="portfolio_without_trade_diagnostics",
        param_axes=["month_week", "weekday"],
        rows=[
            {
                "backtest_id": "calendar_a",
                "semantic_combo": {"month_week": 1, "weekday": "monday"},
                "sharpe": 0.4,
                "total_return": 0.3,
                "max_drawdown": -0.2,
                "trade_count": None,
                "exposure_time": None,
            },
            {
                "backtest_id": "calendar_b",
                "semantic_combo": {"month_week": 2, "weekday": "monday"},
                "sharpe": 0.5,
                "total_return": 0.4,
                "max_drawdown": -0.18,
                "trade_count": None,
                "exposure_time": None,
            },
        ],
    )

    assert len(payload["rows"]) == 2
    assert all(math.isfinite(row["stability_score"]) for row in payload["rows"])
    assert all(row["trade_count"] is None for row in payload["rows"])
    assert all(row["exposure_time"] is None for row in payload["rows"])


def test_acceptance_evaluator_computes_robust_score() -> None:
    evaluator = WFAAcceptanceEvaluator({"min_oos_is_ratio": 0.7})
    result = evaluator.evaluate(
        {
                "mean_is_sharpe": 1.0,
                "mean_oos_sharpe": 0.8,
                "mean_oos_calmar": 0.6,
                "oos_std": 0.1,
            "max_drawdown": -0.2,
        }
    )
    assert result.accepted is True
    assert result.robust_score is not None
    assert result.metrics["oos_is_ratio"] == 0.8


def test_acceptance_evaluator_rejects_negative_oos_even_when_ratio_is_positive() -> (
    None
):
    evaluator = WFAAcceptanceEvaluator({"min_oos_is_ratio": 0.7})
    result = evaluator.evaluate(
        {
            "mean_is_sharpe": -1.0,
            "mean_oos_sharpe": -0.8,
            "oos_std": 0.1,
            "max_drawdown": -0.2,
        }
    )
    assert result.accepted is False
    assert "oos_sharpe_not_positive" in result.reasons


def test_robust_selector_clusters_candidates() -> None:
    selector = RobustSelector(random_seed=7)
    summary = selector.cluster_candidates(
        [
            {
                "label": "A",
                "params": {"fast_ma": 10, "slow_ma": 20},
                "mean_oos_sharpe": 0.8,
                "robust_score": 1.0,
            },
            {
                "label": "B",
                "params": {"fast_ma": 11, "slow_ma": 20},
                "mean_oos_sharpe": 0.82,
                "robust_score": 1.1,
            },
            {
                "label": "C",
                "params": {"fast_ma": 40, "slow_ma": 80},
                "mean_oos_sharpe": 0.5,
                "robust_score": 0.6,
            },
        ]
    )
    assert summary["clusters"]
    assert summary["representatives"]


def test_optuna_search_engine_runs_tpe_study(tmp_path) -> None:
    engine = OptunaSearchEngine(
        {
            "mode": "single_objective",
            "sampler": "tpe",
            "multivariate": True,
            "n_trials": 8,
            "n_startup_trials": 3,
            "random_seed": 42,
            "pruner": "none",
        },
        storage_dir=tmp_path,
    )
    payload = engine.optimize(
        study_name="fixture",
        search_space=[
            {"name": "fast_ma", "type": "int", "low": 5, "high": 20},
            {"name": "risk_pct", "type": "float", "low": 0.5, "high": 2.0},
        ],
        objective_fn=lambda params, trial: (
            -abs(params["fast_ma"] - 11) - abs(params["risk_pct"] - 1.2)
        ),
    )
    assert payload["completed_trials"] >= 1
    assert "best_params" in payload
