from __future__ import annotations

import importlib
import json
import subprocess
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from backtester.RustCoreBridge_backtester import (
    run_metrics_batch_via_cli,
    run_plot_bundle_via_cli,
)
from dataloader.market_data_loader import MultiAssetMarketDataLoader
from metricstracker.RustMetrics_metricstracker import compute_metrics_for_frame
from tests.test_strategy_run_examples_runtime import (
    _domain_values,
    _load_example,
    _run_example,
    _with_full_matrix_retention,
)
from tests.test_unified_portfolio_wfa_runner import (
    _canonical_strategy_config,
    _market_data,
    _wfa_runner,
)


_FIXTURE_PATH = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "golden"
    / "canonical_pipeline_golden_v1.json"
)
_GOLDEN = json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))
_PUBLIC_STRATEGY_KEYS = tuple(_GOLDEN["public_strategies"])


def _assert_optional_approx(actual: Any, expected: Any) -> None:
    if expected is None:
        assert actual is None
    elif isinstance(expected, float):
        assert actual == pytest.approx(expected, rel=1e-12, abs=1e-12)
    else:
        assert actual == expected


def _bounded_matrix(config: dict[str, Any]) -> dict[str, Any]:
    out = _with_full_matrix_retention(config)
    out["parameter_domains"] = {
        name: {"type": "set", "values": _domain_values(spec)[:2]}
        for name, spec in dict(out.get("parameter_domains") or {}).items()
    }
    return out


def _run_public_strategy(case: dict[str, Any], tmp_path: Path) -> dict[str, Any]:
    config = _load_example(case["example"])
    if case["bounded_matrix"]:
        config = _bounded_matrix(config)
    return _run_example(config, tmp_path)


@pytest.mark.golden
def test_xnys_multilevel_bar_time_vertical_slice_golden() -> None:
    expected = _GOLDEN["phase2_multitimeframe"]
    assert expected["partial_final_policy_1h"] == "emit"
    assert [item["source_count"] for item in expected["decisions"]] == [390, 210]
    rust_crate = Path(__file__).resolve().parents[1] / "rust" / "lo2cin4bt_core"
    completed = subprocess.run(
        [
            "cargo",
            "test",
            "engine_runtime::timestamp_tests::one_minute_to_five_minute_to_hour_to_session_executes_at_next_external_open",
            "--",
            "--exact",
        ],
        cwd=rust_crate,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


@pytest.mark.golden
def test_dataloader_matches_content_addressed_golden_bundle(tmp_path: Path) -> None:
    expected = _GOLDEN["dataloader"]
    source_frame = pd.DataFrame(
        {
            "Time": ["2024-01-04", "2024-01-02", "2024-01-03"],
            "AAA": [102, 100, 101],
            "BBB": [198, 200, 199],
        }
    )
    fields = {}
    for name in ("open", "high", "low", "close", "volume"):
        source = tmp_path / f"{name}.csv"
        source_frame.to_csv(source, index=False)
        fields[name] = {"path": str(source), "time_column": "Time"}
    timeline_path = tmp_path / "execution_timeline.csv"
    pd.DataFrame(
        {
            "Time": ["2024-01-02", "2024-01-03", "2024-01-04"],
            "external_execution_sequence": [0, 1, 2],
            "bar_open_timestamp": [
                "2024-01-02T14:30:00Z",
                "2024-01-03T14:30:00Z",
                "2024-01-04T14:30:00Z",
            ],
            "bar_close_timestamp": [
                "2024-01-02T21:00:00Z",
                "2024-01-03T21:00:00Z",
                "2024-01-04T21:00:00Z",
            ],
            "available_timestamp": [
                "2024-01-02T21:00:00Z",
                "2024-01-03T21:00:00Z",
                "2024-01-04T21:00:00Z",
            ],
            "session_label": ["2024-01-02", "2024-01-03", "2024-01-04"],
        }
    ).to_csv(timeline_path, index=False)
    execution_stream = {
        "stream_id": "execution_daily",
        "role": "execution",
        "source": {"kind": "external", "provider_id": "fixture"},
        "session_scope": "regular",
        "row_key_kind": "session_label",
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
            "external_execution_sequence_column": "external_execution_sequence",
            "bar_open_time_column": "bar_open_timestamp",
            "bar_close_time_column": "bar_close_timestamp",
            "available_time_column": "available_timestamp",
            "session_label_column": "session_label",
            "availability_policy": "bar_close",
        },
        "timeline_table": "execution_timeline",
        "ohlcv_tables": {
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        },
    }

    bundle = MultiAssetMarketDataLoader(repo_root=tmp_path).load_bundle(
        {
            "provider": "fixture",
            "symbols": ["AAA", "BBB"],
            "calendar_id": "XNYS",
            "timezone": "America/New_York",
            "adjustment_policy": "raw",
            "execution_stream": execution_stream,
            "execution_timeline": {
                "path": str(timeline_path),
                "time_column": "Time",
            },
            "session_windows": [
                {
                    "session_label": label,
                    "open_timestamp": f"{label}T14:30:00Z",
                    "close_timestamp": f"{label}T21:00:00Z",
                }
                for label in ("2024-01-02", "2024-01-03", "2024-01-04")
            ],
            **fields,
        },
        output_root=tmp_path / "bundles",
    )
    manifest = bundle.read_manifest()
    close = bundle.load_frames()["close"].reset_index()
    close["Time"] = close["Time"].dt.strftime("%Y-%m-%d")

    assert manifest["schema_version"] == expected["schema_version"]
    assert manifest["contract_id"] == expected["contract_id"]
    assert manifest["bundle_id"] == expected["bundle_id"]
    assert manifest["content_hash"] == expected["content_hash"]
    assert manifest["tables"]["close"]["content_hash"] == expected["table_hash"]
    for field in ("symbols", "calendar", "timezone", "execution_stream", "session_windows"):
        assert manifest[field] == expected[field]
    assert close.to_dict("records") == expected["rows"]


@pytest.mark.golden
@pytest.mark.parametrize("strategy_key", _PUBLIC_STRATEGY_KEYS)
def test_public_strategy_matches_canonical_rust_golden(
    strategy_key: str,
    tmp_path: Path,
) -> None:
    case = _GOLDEN["public_strategies"][strategy_key]
    result = _run_public_strategy(case, tmp_path / strategy_key)
    actual_rows = result["portfolio_results"]
    assert len(actual_rows) == len(case["rows"])

    for actual, expected in zip(actual_rows, case["rows"]):
        validation = actual.validation_report
        result_validation = validation["result_validation"]
        summary = validation["rust_timeline_accounting_summary"]
        assert actual.strategy_id == expected["strategy_id"]
        assert actual.config.get("resolved_params", {}) == expected["resolved_params"]
        assert len(actual.equity_curve) == expected["equity_rows"]
        assert len(actual.rebalance_trades) == expected["trade_rows"]
        assert summary["active_rebalances"] == expected["active_rebalances"]
        assert summary["final_equity"] == pytest.approx(
            expected["final_equity"], rel=1e-12, abs=1e-12
        )
        assert validation["accounting_kernel"] == expected["accounting_kernel"]
        assert validation["accounting_fast_path"] == expected["accounting_fast_path"]
        assert result_validation["schema_version"] == "result_validation_report.v1"
        assert result_validation["status"] == "valid"
        assert all(check["status"] == "passed" for check in result_validation["checks"])
        assert result_validation["result_hash"] == expected["result_hash"]


@pytest.mark.golden
def test_metricstracker_matches_rust_metrics_golden() -> None:
    payload = {
        "time_unit": 252,
        "risk_free_rate": 0.02,
        "backtest_ids": [
            "winner:parameter_matrix:fixed",
            "loser:parameter_matrix:fixed",
        ],
        "equity": [100, 110, 121, 115, 126.5, 100, 95, 90, 99, 89.1],
        "bah_equity": [100, 104, 108, 112, 116, 100, 101, 102, 103, 104],
        "session_labels": [
            "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",
            "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",
        ],
        "trade_actions": [0, 1, 0, 4, 4, 0, 1, 0, 4, 4],
        "trade_returns": [None, None, None, 0.15, 0.10, None, None, None, -0.10, -0.05],
        "position_size": [0, 1, 1, 0, 0, 0, 1, 1, 0, 0],
        "group_start": [0, 5],
        "group_end": [5, 10],
    }
    result = run_metrics_batch_via_cli(payload, timeout=60)
    actual_by_id = {row["Backtest_id"]: row for row in result["metrics"]}

    assert result["row_count"] == 2
    for backtest_id, expected in _GOLDEN["metrics"].items():
        actual = actual_by_id[backtest_id]
        for field, value in expected.items():
            _assert_optional_approx(actual[field], value)


@pytest.mark.golden
def test_subdaily_metrics_project_to_session_closes_golden() -> None:
    expected = _GOLDEN["subdaily_session_projected_metrics"]
    result = run_metrics_batch_via_cli(
        {
            "time_unit": 252,
            "risk_free_rate": 0.0,
            "backtest_ids": ["intraday:single_backtest:fixed"],
            "equity": [100.0, 105.0, 101.0, 109.0, 102.0, 110.0],
            "bah_equity": [100.0, 104.0, 101.0, 108.0, 102.0, 109.0],
            "session_labels": [
                "2024-01-02",
                "2024-01-02",
                "2024-01-03",
                "2024-01-03",
                "2024-01-04",
                "2024-01-04",
            ],
            "trade_actions": [],
            "trade_returns": [],
            "position_size": [],
            "group_start": [0],
            "group_end": [6],
        },
        timeout=60,
    )

    assert result["annualization"] == expected["annualization"]
    actual = result["metrics"][0]
    for field, value in expected["metric"].items():
        _assert_optional_approx(actual[field], value)


@pytest.mark.golden
def test_plotter_matches_plot_bundle_golden() -> None:
    expected = _GOLDEN["plot_bundle"]
    payload = {
        "run_id": expected["run_id"],
        "chart_type": expected["chart_type"],
        "title": expected["title"],
        "series": [
            {
                "series_id": item["series_id"],
                "label": item["label"],
                "x": item["x"],
                "y": item["y"],
            }
            for item in expected["series"]
        ],
        "x_axis": expected["axes"]["x"],
        "y_axis": expected["axes"]["y"],
        "source_hashes": expected["source_hashes"],
        "artifact_source_refs": expected["artifact_source_refs"],
        "generated_at": expected["generated_at"],
    }

    assert run_plot_bundle_via_cli(payload, timeout=60) == expected


@pytest.mark.golden
def test_wfa_matches_selected_optimum_golden() -> None:
    expected = _GOLDEN["wfa"]
    runner_mod = importlib.import_module(
        "validation_workflow.UnifiedPortfolioWFARunner_validation_workflow"
    )
    strategy_config = _canonical_strategy_config(
        {
            "metadata": {"strategy_id": "golden_wfa_probe"},
            "universe": {"symbols": ["AAA", "BBB"]},
            "parameter_domains": {
                "lookback": {"type": "range", "start": 2, "end": 4, "step": 2}
            },
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
    )
    result = _wfa_runner(
        runner_mod,
        market_data=_market_data(),
        strategy_config=strategy_config,
        wfa_config={
            "windowing": {"train_size": 35, "test_size": 10, "step_size": 20},
            "optimizer": {"objectives": ["sharpe"]},
        },
    ).run()

    for field, value in expected["metadata"].items():
        assert result.metadata[field] == value
    assert len(result.candidate_diagnostics) == expected["candidate_rows"]
    assert len(result.window_backtests) == expected["window_backtests"]
    actual_rows = result.selected_optimum
    assert len(actual_rows) == len(expected["selected"])
    for actual, golden_row in zip(actual_rows.to_dict("records"), expected["selected"]):
        for field, value in golden_row.items():
            actual_value = actual[field]
            if field.endswith(("_start", "_end")):
                actual_value = str(actual_value)
            _assert_optional_approx(actual_value, value)


@pytest.mark.golden
def test_canonical_pipeline_end_to_end_reaches_metrics_and_plot_bundle(
    tmp_path: Path,
) -> None:
    expected = _GOLDEN["end_to_end"]
    strategy_case = _GOLDEN["public_strategies"][expected["strategy_key"]]
    run = _run_public_strategy(strategy_case, tmp_path / "end_to_end")
    result = run["portfolio_results"][0]
    validation = result.validation_report["result_validation"]
    metrics = compute_metrics_for_frame(
        result.equity_curve,
        time_unit=252,
        risk_free_rate=0.02,
        backtest_id=result.strategy_id,
    )
    x_values = result.equity_curve["Time"].astype(str).tolist()
    y_values = result.equity_curve["Equity_value"].astype(float).tolist()
    plot_bundle = run_plot_bundle_via_cli(
        {
            "run_id": result.strategy_id,
            "chart_type": "equity_curve",
            "title": "Canonical end-to-end equity",
            "series": [
                {
                    "series_id": "strategy",
                    "label": "Strategy",
                    "x": x_values,
                    "y": y_values,
                }
            ],
            "x_axis": "time",
            "y_axis": "equity",
            "source_hashes": [validation["result_hash"]],
            "artifact_source_refs": ["canonical_result_bundle"],
            "generated_at": "2026-07-25T00:00:00Z",
        },
        timeout=60,
    )

    assert validation["schema_version"] == expected["result_schema_version"]
    assert validation["status"] == expected["result_status"]
    assert validation["result_hash"] == expected["result_hash"]
    assert validation["table_row_counts"] == expected["table_row_counts"]
    assert y_values[0] == pytest.approx(expected["first_equity"], abs=1e-12)
    assert y_values[-1] == pytest.approx(expected["last_equity"], abs=1e-12)
    for field, value in expected["metrics"].items():
        assert metrics[field] == pytest.approx(value, rel=1e-12, abs=1e-12)
    assert plot_bundle["schema_version"] == "plot_bundle.v1"
    assert plot_bundle["source_hashes"] == [expected["result_hash"]]
    assert plot_bundle["series"][0]["y"][0] == pytest.approx(expected["first_equity"])
    assert plot_bundle["series"][0]["y"][-1] == pytest.approx(expected["last_equity"])
