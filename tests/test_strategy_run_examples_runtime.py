import copy
import json
import os
import sys
from pathlib import Path

import pandas as pd
import pytest


_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


from backtester.EngineRequest_backtester import build_engine_request  # noqa: E402
from backtester.StrategyRunConfig_backtester import normalize_strategy_run_config  # noqa: E402
from backtester.UnifiedBacktestRunner_backtester import UnifiedBacktestRunnerBacktester  # noqa: E402
from dataloader.market_data_bundle import build_market_data_bundle  # noqa: E402
from dataloader.market_data_loader import market_data_spec_from_requirements  # noqa: E402


def _load_example(name: str) -> dict:
    path = _REPO_ROOT / "backtester" / "contracts" / "strategy" / "examples" / name
    return json.loads(path.read_text(encoding="utf-8"))


def _with_full_matrix_retention(config: dict) -> dict:
    out = copy.deepcopy(config)
    out.setdefault("fill_model", {})["matrix_result_retention"] = 10000
    out["fill_model"]["matrix_workers"] = 1
    return out


def _with_bounded_matrix(config: dict, *, values_per_axis: int = 2) -> dict:
    if _run_full_example_matrix():
        return _with_full_matrix_retention(config)
    out = _with_full_matrix_retention(config)
    bounded_domains = {}
    for name, spec in dict(out.get("parameter_domains") or {}).items():
        values = _domain_values(spec)[:values_per_axis]
        bounded_domains[name] = {"type": "set", "values": values}
    out["parameter_domains"] = bounded_domains
    return out


def _run_full_example_matrix() -> bool:
    return os.getenv("LO2CIN4BT_TEST_FULL_EXAMPLE_MATRIX", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "full",
    }


def _domain_values(spec) -> list:
    if isinstance(spec, list):
        return list(spec)
    if not isinstance(spec, dict):
        return []
    if isinstance(spec.get("values"), list):
        return list(spec["values"])
    if str(spec.get("type", "")).lower() == "range" or {"start", "end"}.issubset(spec.keys()):
        start = int(spec.get("start"))
        end = int(spec.get("end"))
        step = int(spec.get("step") or 1)
        if step == 0:
            return []
        if start <= end and step > 0:
            return list(range(start, end + 1, step))
        if start >= end and step < 0:
            return list(range(start, end - 1, step))
    return []


def _frames_for_symbols(symbols: list[str], *, periods: int = 420) -> dict[str, pd.DataFrame]:
    dates = pd.date_range("2020-01-02", periods=periods, freq="B")
    close_values = {}
    for index, symbol in enumerate(symbols):
        if len(symbols) == 1:
            close_values[symbol] = [
                100.0 + row * 0.03 + (row % 41) * 0.6 - (row % 17) * 0.25
                for row in range(len(dates))
            ]
        else:
            base = 100.0 + 15.0 * index
            close_values[symbol] = [
                base
                + row * (0.04 + 0.01 * index)
                + ((row + index * 7) % 31) * 0.11
                + (row % 13) * 0.03
                for row in range(len(dates))
            ]
    close = pd.DataFrame(close_values, index=dates)
    return {
        "open": close * 0.995,
        "high": close * 1.01,
        "low": close * 0.99,
        "close": close,
        "volume": pd.DataFrame(1_000_000, index=dates, columns=close.columns),
    }


def _run_example(config: dict, tmp_path: Path):
    symbols = [str(item).upper() for item in config["universe"]["symbols"]]
    request = build_engine_request(copy.deepcopy(config))
    spec = market_data_spec_from_requirements(request["data_requirements"])
    bundle = build_market_data_bundle(
        _frames_for_symbols(symbols),
        spec=spec,
        output_root=tmp_path / "market_data_bundle",
    )
    runner = UnifiedBacktestRunnerBacktester()
    return runner.run(
        market_data_bundle=bundle,
        engine_request=request,
    )

def _assert_portfolio_results_equal(left, right) -> None:
    assert len(left["portfolio_results"]) == len(right["portfolio_results"])
    for left_result, right_result in zip(left["portfolio_results"], right["portfolio_results"]):
        assert left_result.config.get("resolved_params") == right_result.config.get("resolved_params")
        for table_name in ("equity_curve", "holdings", "rebalance_audit", "rebalance_trades"):
            pd.testing.assert_frame_equal(
                getattr(left_result, table_name),
                getattr(right_result, table_name),
                check_dtype=False,
                rtol=1e-12,
                atol=1e-12,
            )


def _assert_full_result_bundle(result: dict, expected_candidates: int) -> None:
    equity_paths = [
        Path(path)
        for path in result.get("exported_files", [])
        if str(path).endswith("_equity_curve.parquet")
    ]
    metadata_paths = [
        Path(path)
        for path in result.get("exported_files", [])
        if str(path).endswith("_metadata.json")
    ]
    assert len(equity_paths) == 1
    assert len(metadata_paths) == 1
    equity = pd.read_parquet(equity_paths[0], columns=["Backtest_id", "Equity_value"])
    assert equity["Backtest_id"].astype(str).nunique() == expected_candidates
    assert len(equity) >= expected_candidates
    metadata = json.loads(metadata_paths[0].read_text(encoding="utf-8"))
    assert metadata["schema_version"] == "canonical_result_bundle.v1"
    assert metadata["contract_id"] == "lo2cin4bt.canonical_result_bundle.v1"
    assert metadata["candidate_count"] == expected_candidates
    assert len(metadata["candidates"]) == expected_candidates


def _assert_next_open_timeline_config(config: dict) -> None:
    fill_model = config["fill_model"]
    assert fill_model["timing"] == "timeline"
    assert fill_model["actions"] == [
        {"signal": "entry", "offset_bars": 1, "price": "open", "action": "enter"},
        {"signal": "exit", "offset_bars": 1, "price": "open", "action": "exit"},
    ]


def test_public_single_asset_examples_use_next_open_timeline_actions():
    for name in (
        "strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json",
    ):
        _assert_next_open_timeline_config(_load_example(name))


def test_matrix_result_retention_limits_exported_bundle_candidates(tmp_path):
    public_config = _load_example("strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json")
    config = _with_bounded_matrix(public_config)
    config.setdefault("fill_model", {})["matrix_result_retention"] = 2
    symbols = [str(item).upper() for item in config["universe"]["symbols"]]
    frames = _frames_for_symbols(symbols)
    runner = UnifiedBacktestRunnerBacktester()
    portfolio_config = runner._portfolio_config_from_normalized(config)
    portfolio_config["strategy_id"] = config["metadata"]["strategy_id"]
    variants = runner._portfolio_variants_for_workflow(
        portfolio_config=portfolio_config,
        raw_config=config,
    )
    engine_request = build_engine_request(config)
    market_data_bundle = build_market_data_bundle(
        frames,
        spec=market_data_spec_from_requirements(engine_request["data_requirements"]),
        output_root=tmp_path / "market_data_bundle",
    )

    retained_results, exported_files, matrix_summary = runner._run_portfolio_variant_batch(
        variants=variants,
        market_data=frames,
        market_data_bundle=market_data_bundle,
        engine_request=engine_request,
        export_config={"output_dir": str(tmp_path)},
        run_id_base="retained_bundle_test",
        cache_dir=None,
        portfolio_config=portfolio_config,
    )

    assert len(retained_results) == 2
    assert matrix_summary["variant_count"] == len(variants) == 4
    assert matrix_summary["retained_result_count"] == 2
    assert matrix_summary["compact_result_count"] == 2
    assert [row["result_materialization"] for row in matrix_summary["rows"]].count("full") == 2
    _assert_full_result_bundle({"exported_files": exported_files}, 2)


def test_generic_timer_matrix_uses_rust_timeline_variant_matrix(tmp_path):
    bridge = __import__("backtester.RustCoreBridge_backtester", fromlist=["rust_core_available"])
    if not bridge.rust_core_available():
        import pytest

        pytest.skip("Rust core is unavailable")

    dates = pd.date_range("2024-01-01", periods=8, freq="B")
    close = pd.DataFrame(
        {
            "SPY": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0],
            "IEF": [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 103.5],
        },
        index=dates,
    )
    frames = {
        "open": close * 0.99,
        "high": close * 1.01,
        "low": close * 0.98,
        "close": close,
        "volume": pd.DataFrame(1_000_000, index=dates, columns=close.columns),
        "entry_signal": pd.DataFrame(
            {"SPY": [1.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0], "IEF": 0.0},
            index=dates,
        ),
    }
    config = {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "multi_leg_event_portfolio",
            "workflow_id": "parameter_matrix",
        },
        "data": {"provider": "synthetic", "frequency": "1D", "interval": "1d"},
        "universe": {"symbols": ["SPY", "IEF"]},
        "signals": {
            "entry": {"field": "entry_signal", "op": "eq", "value": 1.0},
            "conflict_policy": "reset_timer_on_reentry_signal",
        },
        "allocation": {"method": "fixed_weights", "weights": {"SPY": 1.0}},
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
                    "weights": {"SPY": 1.0},
                },
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
        },
        "parameter_domains": {"delay_bars": {"type": "set", "values": [2, 3]}},
        "metadata": {"strategy_id": "test_generic_timer_matrix"},
    }

    runner = UnifiedBacktestRunnerBacktester()
    normalized = normalize_strategy_run_config(copy.deepcopy(config))
    portfolio_config = runner._portfolio_config_from_normalized(normalized)
    portfolio_config["strategy_id"] = normalized["metadata"]["strategy_id"]
    variants = runner._portfolio_variants_for_workflow(
        portfolio_config=portfolio_config,
        raw_config=normalized,
    )
    engine_request = build_engine_request(normalized)
    market_data_bundle = build_market_data_bundle(
        frames,
        spec=market_data_spec_from_requirements(engine_request["data_requirements"]),
        output_root=tmp_path / "market_data_bundle",
    )
    portfolio_results, exported_files, matrix_summary = runner._run_portfolio_variant_batch(
        variants=variants,
        market_data=frames,
        market_data_bundle=market_data_bundle,
        engine_request=engine_request,
        export_config={"output_dir": str(tmp_path)},
        run_id_base="reset_timer_matrix_test",
        cache_dir=None,
        portfolio_config=portfolio_config,
    )

    assert len(portfolio_results) == 2
    assert len(matrix_summary["rows"]) == 2
    assert exported_files
    assert {
        item.validation_report.get("signal_producer")
        for item in portfolio_results
    } == {"rust_engine_request_reset_timer_batch_v1"}
    assert all(
        item.validation_report.get("accounting_fast_path")
        == "reset_timer_rust_engine_request_batch"
        for item in portfolio_results
    )
    assert all(
        item.validation_report.get("accounting_kernel") == "rust_timeline_v1"
        for item in portfolio_results
    )


def test_strategy_run_multi_asset_market_spec_preserves_external_features():
    config = {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "multi_leg_event_portfolio",
            "workflow_id": "single_backtest",
        },
        "data": {
            "provider": "yfinance",
            "frequency": "1D",
            "interval": "1d",
            "external_features": [
                {
                    "name": "market_breadth",
                    "path": "workspace/datasets/MARKET_BREADTH_1D.csv",
                    "time_column": "time",
                    "value_column": "close",
                    "scope": "market",
                }
            ],
        },
        "universe": {"symbols": ["SPY", "IEF"]},
        "signals": {"entry": {"field": "market_breadth", "op": "crosses_below", "value": 10}},
        "allocation": {"method": "fixed_weights", "weights": {"SPY": 1.0}},
        "rebalance": {"trigger": {"op": "calendar.first_session"}},
        "fill_model": {
            "timing": "timeline",
            "actions": [
                {
                    "signal": "entry",
                    "offset_bars": 1,
                    "price": "open",
                    "action": "set_target_weights",
                    "weights": {"IEF": 1.0},
                }
            ],
        },
    }

    request = build_engine_request(copy.deepcopy(config))
    spec = market_data_spec_from_requirements(request["data_requirements"])

    assert spec["external_features"][0]["name"] == "market_breadth"


def test_qqq_sma_cross_example_runs_all_variants_from_canonical_strategy_run(tmp_path):
    public_config = _load_example("strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json")
    assert len(_domain_values(public_config["parameter_domains"]["short_ma"])) == 9
    assert len(_domain_values(public_config["parameter_domains"]["long_ma"])) == 19
    config = _with_bounded_matrix(public_config)
    _assert_next_open_timeline_config(config)
    normalized = normalize_strategy_run_config(copy.deepcopy(config))
    assert normalized["computed_fields"] == config["computed_fields"]

    result = _run_example(config, tmp_path)

    assert len(result["portfolio_results"]) == (171 if _run_full_example_matrix() else 4)
    assert result["portfolio_results"][0].feature_cache["computed"] in {0, 2}
    assert result["portfolio_results"][0].validation_report["execution_model"] == "unified_timeline_v1"
    assert result["portfolio_results"][0].validation_report["accounting_backend"] in {"timeline", "rust_timeline"}
    assert (
        result["portfolio_results"][0].validation_report["accounting_fast_path"]
        == "signal_rust_engine_request_batch"
    )
    assert result["portfolio_results"][0].rebalance_trades.empty is False
    _assert_full_result_bundle(result, 171 if _run_full_example_matrix() else 4)


def test_single_asset_matrix_uses_rust_full_batch_for_all_candidates(monkeypatch, tmp_path):
    bridge = __import__("backtester.RustCoreBridge_backtester", fromlist=["run_signal_timeline_batch_via_cli"])
    calls = []

    def fake_batch(payload, *, timeout=30):
        calls.append({"payload": payload, "timeout": timeout})
        daily_events = [
            {
                "date": date,
                "equity_after_trade": 100.0,
                "portfolio_return": 0.0,
                "turnover": 0.0,
                "trade_cost": 0.0,
                "cost_drag": 0.0,
                "cash_weight": 1.0,
                "gross_exposure": 0.0,
                "active_positions": 0,
                "target_weights": {},
                "contribution": {},
            }
            for date in payload["dates"]
        ]
        checkpoint_events = [
            {
                "date": date,
                "phase": phase,
                "equity_before_return": 100.0,
                "equity_before_trade": 100.0,
                "equity_after_trade": 100.0,
                "portfolio_return": 0.0,
                "turnover": 0.0,
                "trade_cost": 0.0,
                "cost_drag": 0.0,
                "cash_weight": 1.0,
                "gross_exposure": 0.0,
                "active_positions": 0,
                "target_weights": {},
                "drift_weights": {},
                "contribution": {},
                "actions": [],
            }
            for date in payload["dates"]
            for phase in ("open", "close")
        ]
        result_tables = {
            "schema_version": "rust_timeline_result_tables.v1",
            "equity_curve": [
                {
                    "Time": date,
                    "Equity_value": 100.0,
                    "Portfolio_return": 0.0,
                    "Turnover": 0.0,
                    "Trade_cost": 0.0,
                    "Selected_count": 0,
                    "Gross_exposure": 0.0,
                    "Cash_weight": 1.0,
                    "Weight_QQQ": 0.0,
                    "Contribution_QQQ": 0.0,
                }
                for date in payload["dates"]
            ],
            "holdings": [],
            "rebalance_audit": [],
            "rebalance_trades": [],
            "risk_gate_events": [],
        }
        return {
            "candidate_count": len(payload["candidates"]),
            "results": [
                {
                    "candidate_id": item["candidate_id"],
                    "resolved_params": item.get("resolved_params", {}),
                    "final_equity": 100.0,
                    "total_return": 0.0,
                    "cagr": 0.0,
                    "sharpe": 0.0,
                    "max_drawdown": 0.0,
                    "days": len(payload["dates"]),
                    "active_rebalances": 0,
                    "average_turnover": 0.0,
                    "average_gross_exposure": 0.0,
                    "timeline": {
                        "start_equity": 100.0,
                        "final_equity": 100.0,
                        "total_return": 0.0,
                        "checkpoints": len(checkpoint_events),
                        "days": len(daily_events),
                        "active_rebalances": 0,
                        "average_turnover": 0.0,
                        "average_gross_exposure": 0.0,
                        "events": checkpoint_events,
                        "daily_events": daily_events,
                        "risk_gate_events": [],
                        "result_tables": result_tables,
                    },
                }
                for item in payload["candidates"]
            ],
        }

    monkeypatch.setattr(bridge, "run_signal_timeline_batch_via_cli", fake_batch)
    config = _load_example("strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json")
    config["parameter_domains"] = {
        "short_ma": {"type": "set", "values": [20, 30]},
        "long_ma": {"type": "set", "values": [120, 130]},
    }
    config.setdefault("fill_model", {})["matrix_result_retention"] = 2
    config["fill_model"]["matrix_workers"] = 1

    result = _run_example(config, tmp_path)

    assert len(result["portfolio_results"]) == 2
    assert result["portfolio_matrix_summary"]["variant_count"] == 4
    assert result["portfolio_matrix_summary"]["row_count"] == 4
    assert result["portfolio_matrix_summary"]["retained_result_count"] == 2
    assert result["portfolio_matrix_summary"]["compact_result_count"] == 2
    assert result["portfolio_matrix_summary"]["coverage"] == "all_candidates"
    assert [row["result_materialization"] for row in result["portfolio_matrix_summary"]["rows"]].count("full") == 2
    assert [row["result_materialization"] for row in result["portfolio_matrix_summary"]["rows"]].count(
        "summary_only"
    ) == 2
    assert calls == []
    assert result["portfolio_results"][0].validation_report["accounting_fast_path"] == (
        "signal_rust_engine_request_batch"
    )
    assert result["portfolio_results"][0].validation_report["accounting_kernel"] == "rust_timeline_v1"
    assert (
        result["portfolio_results"][0]
        .validation_report["rust_timeline_accounting_summary"]["result_table_kernel"]
        == "rust_arrow_parquet_bundle.v1"
    )
    _assert_full_result_bundle(result, 2)


def test_matrix_result_retention_zero_keeps_summary_rows_only(monkeypatch, tmp_path):
    bridge = __import__("backtester.RustCoreBridge_backtester", fromlist=["run_signal_timeline_batch_via_cli"])

    def fake_batch(payload, *, timeout):
        del timeout
        calls.append({"payload": payload})
        checkpoint_events = [
            {
                "Time": "2020-01-02T00:00:00",
                "Event": "rebalance",
                "Weight_X": 1.0,
                "Contribution_X": 0.0,
                "Gross_exposure": 1.0,
                "Trade_cost": 0.0,
                "Cost_rate": 0.0,
            }
        ]
        result_tables = {
            "schema_version": "rust_timeline_result_tables.v1",
            "equity_curve": [
                {
                    "Time": "2020-01-02T00:00:00",
                    "Equity_value": 100.0,
                    "Portfolio_return": 0.0,
                    "Turnover": 0.0,
                    "Trade_cost": 0.0,
                    "Gross_exposure": 0.0,
                    "Weight_X": 0.0,
                    "Contribution_X": 0.0,
                },
                {
                    "Time": "2020-01-03T00:00:00",
                    "Equity_value": 100.0,
                    "Portfolio_return": 0.0,
                    "Turnover": 0.0,
                    "Trade_cost": 0.0,
                    "Gross_exposure": 1.0,
                    "Weight_X": 1.0,
                    "Contribution_X": 0.0,
                },
            ],
            "holdings": [],
            "rebalance_audit": [],
            "rebalance_trades": [],
            "risk_gate_events": [],
        }
        return {
            "results": [
                {
                    "candidate_id": item["candidate_id"],
                    "final_equity": 100.0,
                    "total_return": 0.0,
                    "cagr": 0.0,
                    "sharpe": 0.0,
                    "max_drawdown": 0.0,
                    "days": len(payload["dates"]),
                    "active_rebalances": 0,
                    "average_turnover": 0.0,
                    "average_gross_exposure": 0.0,
                    "timeline": {
                        "start_equity": 100.0,
                        "final_equity": 100.0,
                        "total_return": 0.0,
                        "checkpoints": len(checkpoint_events),
                        "days": len(payload["dates"]),
                        "active_rebalances": 0,
                        "average_turnover": 0.0,
                        "average_gross_exposure": 0.0,
                        "events": checkpoint_events,
                        "daily_events": checkpoint_events,
                        "risk_gate_events": [],
                        "result_tables": result_tables,
                    },
                }
                for item in payload["candidates"]
            ],
        }

    calls = []
    monkeypatch.setattr(bridge, "run_signal_timeline_batch_via_cli", fake_batch)
    config = _load_example("strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json")
    config["parameter_domains"] = {
        "short_ma": {"type": "set", "values": [20, 30]},
        "long_ma": {"type": "set", "values": [120, 130]},
    }
    config.setdefault("fill_model", {})["matrix_result_retention"] = 0
    config["fill_model"]["matrix_workers"] = 1

    result = _run_example(config, tmp_path)

    assert result["portfolio_result"] is None
    assert result["portfolio_results"] == []
    assert result["portfolio_matrix_summary"]["variant_count"] == 4
    assert result["portfolio_matrix_summary"]["row_count"] == 4
    assert result["portfolio_matrix_summary"]["retained_result_count"] == 0
    assert result["portfolio_matrix_summary"]["compact_result_count"] == 4
    assert all(
        row["result_materialization"] == "summary_only"
        for row in result["portfolio_matrix_summary"]["rows"]
    )
    assert result["exported_files"] == []
    assert calls == []


def test_btcusdt_monthly_nth_weekday_same_session_example_runs_full_matrix(tmp_path):
    config = _with_full_matrix_retention(
        _load_example("strategy-run-btcusdt-binance-monthly-nth-weekday-same-session-matrix-example.json")
    )

    result = _run_example(config, tmp_path)

    assert len(result["portfolio_results"]) == 28
    assert result["portfolio_results"][0].feature_cache["computed"] == 0
    assert result["portfolio_results"][0].validation_report["status"] == "valid"
    assert result["portfolio_results"][0].validation_report["execution_model"] == "unified_timeline_v1"
    assert result["portfolio_results"][0].validation_report["accounting_backend"] == "rust_timeline"
    assert (
        result["portfolio_results"][0].validation_report["accounting_fast_path"]
            == "calendar_rust_engine_request_batch"
    )
    assert result["portfolio_results"][0].validation_report["accounting_kernel"] == "rust_timeline_v1"
    assert result["portfolio_matrix_summary"]["coverage"] == "all_candidates"
    assert result["portfolio_matrix_summary"]["compact_result_count"] == 0
    assert any(not item.rebalance_trades.empty for item in result["portfolio_results"])
    _assert_full_result_bundle(result, 28)


def test_vti_avuv_vxus_sgol_dbmf_yearly_rebalance_example_runs(tmp_path):
    config = _with_full_matrix_retention(
        _load_example("strategy-run-vti-avuv-vxus-sgol-dbmf-yfinance-yearly-rebalance-example.json")
    )

    result = _run_example(config, tmp_path)
    portfolio_result = result["portfolio_results"][0]

    assert len(result["portfolio_results"]) == 1
    assert portfolio_result.feature_cache["computed"] == 0
    assert portfolio_result.validation_report["status"] == "valid"
    assert portfolio_result.rebalance_trades.empty is False
    assert float(portfolio_result.equity_curve["Equity_value"].iloc[-1]) > 0.0


def test_voo_gld_momentum_rotation_example_runs(tmp_path):
    config = _with_full_matrix_retention(
        _load_example("strategy-run-voo-gld-yfinance-daily-momentum90-sma250-rotation-example.json")
    )

    result = _run_example(config, tmp_path)
    portfolio_result = result["portfolio_results"][0]

    assert len(result["portfolio_results"]) == 1
    assert portfolio_result.feature_cache["computed"] == 2
    assert portfolio_result.validation_report["status"] == "valid"
    assert portfolio_result.validation_report["execution_model"] == "unified_timeline_v1"
    assert portfolio_result.validation_report["accounting_backend"] == "rust_daily_rank"
    assert portfolio_result.validation_report["feature_producer"] == "rust_engine_request_daily_rank_v1"
    assert portfolio_result.validation_report["accounting_kernel"] == "rust_daily_rank_v1"
    assert portfolio_result.rebalance_trades.empty is False
    assert float(portfolio_result.equity_curve["Equity_value"].iloc[-1]) > 0.0


def test_selection_timing_profile_example_runs_with_authoring_defaults(tmp_path):
    config = _load_example("strategy-run-us-etf-yfinance-daily-selection-timing-momentum-sma-example.json")

    normalized = normalize_strategy_run_config(copy.deepcopy(config))
    result = _run_example(config, tmp_path)
    portfolio_result = result["portfolio_results"][0]

    assert normalized["platform"]["strategy_profile_id"] == "selection_timing_portfolio"
    assert normalized["allocation"]["method"] == "equal_weight"
    assert normalized["risk"]["max_positions"] == 2
    assert normalized["rebalance"]["trigger"]["op"] == "calendar.every_session"
    assert portfolio_result.validation_report["accounting_backend"] == "rust_daily_rank"
    assert portfolio_result.rebalance_trades.empty is False
    assert float(portfolio_result.equity_curve["Equity_value"].iloc[-1]) > 0.0


def test_pair_spread_profile_example_runs_as_shared_timeline_portfolio(tmp_path):
    config = _load_example("strategy-run-spy-qqq-yfinance-monthly-pair-spread-example.json")

    normalized = normalize_strategy_run_config(copy.deepcopy(config))
    result = _run_example(config, tmp_path)
    portfolio_result = result["portfolio_results"][0]

    assert normalized["platform"]["strategy_profile_id"] == "pair_spread_portfolio"
    assert normalized["risk"]["allow_short"] is True
    assert portfolio_result.validation_report["status"] == "valid"
    assert portfolio_result.validation_report["accounting_fast_path"] == "timeline_rust_engine_request_bundle"
    assert portfolio_result.validation_report["profile_contract_kind"] == "pair_spread"
    assert portfolio_result.validation_report["timeline_compile_kind"] == "explicit_actions"
    assert portfolio_result.rebalance_trades.empty is False
    assert float(portfolio_result.equity_curve["Equity_value"].iloc[-1]) > 0.0


def test_multi_leg_event_profile_example_runs_as_shared_timeline_portfolio(tmp_path):
    config = _load_example("strategy-run-qqq-tlt-gld-yfinance-monthly-hedge-overlay-example.json")

    normalized = normalize_strategy_run_config(copy.deepcopy(config))
    result = _run_example(config, tmp_path)
    portfolio_result = result["portfolio_results"][0]

    assert normalized["platform"]["strategy_profile_id"] == "multi_leg_event_portfolio"
    assert portfolio_result.validation_report["status"] == "valid"
    assert portfolio_result.validation_report["accounting_fast_path"] == "timeline_rust_engine_request_bundle"
    assert portfolio_result.validation_report["profile_contract_kind"] == "multi_leg_event"
    assert portfolio_result.validation_report["timeline_compile_kind"] == "explicit_actions"
    assert portfolio_result.rebalance_trades.empty is False
    assert float(portfolio_result.equity_curve["Equity_value"].iloc[-1]) > 0.0


def test_daily_rank_parameter_matrix_uses_grouped_engine_request_batch(tmp_path):
    config = _with_full_matrix_retention(
        _load_example("strategy-run-voo-gld-yfinance-daily-momentum90-sma250-rotation-example.json")
    )
    config["platform"]["workflow_id"] = "parameter_matrix"
    config["computed_fields"][0]["period"] = {"param_ref": "momentum_period"}
    config["computed_fields"][1]["period"] = 2
    config["parameter_domains"] = {
        "momentum_period": {"type": "set", "values": [1, 2]},
    }
    symbols = [str(item).upper() for item in config["universe"]["symbols"]]
    frames = _frames_for_symbols(symbols, periods=20)
    runner = UnifiedBacktestRunnerBacktester()
    portfolio_config = runner._portfolio_config_from_normalized(config)
    portfolio_config["strategy_id"] = config["metadata"]["strategy_id"]
    variants = runner._portfolio_variants_for_workflow(
        portfolio_config=portfolio_config,
        raw_config=config,
    )
    engine_request = build_engine_request(config)
    market_data_bundle = build_market_data_bundle(
        frames,
        spec=market_data_spec_from_requirements(engine_request["data_requirements"]),
        output_root=tmp_path / "market_data_bundle",
    )

    results, rows, exported = runner._try_run_grouped_engine_request_batch(
        variants=variants,
        market_data_bundle=market_data_bundle,
        engine_request=engine_request,
        cache_dir=None,
        export_config={"output_dir": str(tmp_path / "result")},
        run_id_base="daily_rank_engine_request_batch_test",
    )

    assert len(results) == len(rows) == len(variants) == 2
    assert all(
        result.validation_report["accounting_fast_path"]
        == "daily_rank_rust_engine_request_batch"
        for result in results
    )
    assert all(
        result.validation_report["feature_producer"]
        == "rust_engine_request_daily_rank_batch_v1"
        for result in results
    )
    _assert_full_result_bundle({"exported_files": exported}, len(variants))


def test_execution_weight_parser_rejects_invalid_values() -> None:
    runner = UnifiedBacktestRunnerBacktester()

    with pytest.raises(ValueError, match="must be numeric"):
        runner._normalized_weight_map({"QQQ": "invalid"})
    with pytest.raises(ValueError, match="must be finite"):
        runner._normalized_weight_map({"QQQ": float("nan")})
