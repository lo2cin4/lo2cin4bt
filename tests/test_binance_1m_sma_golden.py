from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from backtester.EngineRequest_backtester import build_engine_request
from backtester.UnifiedBacktestRunner_backtester import UnifiedBacktestRunnerBacktester
from dataloader.market_data_bundle import (
    ExecutionStreamSpec,
    ExternalMarketData,
    SessionWindow,
    build_market_data_bundle,
)
from dataloader.market_data_loader import market_data_spec_from_requirements
from metricstracker.RustMetrics_metricstracker import compute_metrics_for_frame


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "golden"
GOLDEN = json.loads(
    (FIXTURE_ROOT / "binance_btcusdt_1m_sma_10_20_golden_v1.json").read_text(
        encoding="utf-8"
    )
)


def _external_data(
    source: pd.DataFrame,
    engine_request: dict,
) -> ExternalMarketData:
    bar_time = engine_request["data_requirements"]["bar_time"]
    session_model = bar_time["session_model"]
    execution = next(
        stream for stream in bar_time["streams"] if stream["role"] == "execution"
    )
    stream = ExecutionStreamSpec.from_mapping(
        {
            **execution,
            "session_scope": session_model["session_scope"],
            "row_key_kind": "event_timestamp",
            "timestamp_semantics": {
                **execution["timestamp_semantics"],
                "external_execution_sequence_column": (
                    "external_execution_sequence"
                ),
            },
            "timeline_table": "execution_timeline",
            "ohlcv_tables": {
                name: name for name in ("open", "high", "low", "close", "volume")
            },
        }
    )
    bar_opens = pd.DatetimeIndex(source.index)
    bar_closes = bar_opens + pd.Timedelta(minutes=1)
    session_labels = bar_opens.strftime("%Y-%m-%d")
    row_keys = pd.DatetimeIndex(bar_closes, name="Time")
    frames = {
        name: pd.DataFrame(
            {"BTCUSDT": source[name].to_numpy()},
            index=row_keys,
        )
        for name in ("open", "high", "low", "close", "volume")
    }
    timeline = pd.DataFrame(
        {
            "external_execution_sequence": range(len(source)),
            "bar_open_timestamp": bar_opens,
            "bar_close_timestamp": bar_closes,
            "available_timestamp": bar_closes,
            "session_label": session_labels,
        },
        index=row_keys,
    )
    sessions = []
    for label in pd.Index(session_labels).unique():
        session_open = pd.Timestamp(label, tz="UTC")
        sessions.append(
            SessionWindow.from_mapping(
                {
                    "session_label": label,
                    "open_timestamp": session_open,
                    "close_timestamp": session_open + pd.Timedelta(days=1),
                }
            )
        )
    return ExternalMarketData(
        frames=frames,
        execution_stream=stream,
        execution_timeline=timeline,
        session_windows=sessions,
    )


@pytest.mark.golden
def test_binance_1m_sma_10_20_complete_month_golden(tmp_path: Path) -> None:
    source_spec = GOLDEN["source"]
    source_path = FIXTURE_ROOT / source_spec["fixture"]
    metadata = json.loads(
        source_path.with_suffix(".metadata.json").read_text(encoding="utf-8")
    )
    assert hashlib.sha256(source_path.read_bytes()).hexdigest() == source_spec["sha256"]
    assert metadata["sha256"] == source_spec["sha256"]
    assert metadata["provider"] == "binance"
    assert metadata["symbol"] == "BTCUSDT"
    assert metadata["interval"] == "1m"

    source = pd.read_csv(source_path, parse_dates=["Time"]).set_index("Time")
    expected_index = pd.date_range(
        source_spec["start_inclusive"],
        source_spec["end_exclusive"],
        freq="min",
        inclusive="left",
        name="Time",
    )
    assert source.index.equals(expected_index)
    assert len(source) == source_spec["row_count"]
    assert source.index.normalize().nunique() == source_spec["session_count"]
    assert not source.index.has_duplicates
    assert not source.isna().any().any()

    config = json.loads(
        (FIXTURE_ROOT / GOLDEN["config_fixture"]).read_text(encoding="utf-8")
    )
    strategy_spec = GOLDEN["strategy"]
    fields = {item["name"]: item for item in config["computed_fields"]}
    assert config["data"]["provider"] == "binance"
    assert config["data"]["start_date"] == source_spec["start_inclusive"]
    assert config["data"]["end_date"] == source_spec["end_exclusive"]
    assert config["universe"]["symbols"] == [strategy_spec["symbol"]]
    assert fields["short_ma"]["period"] == strategy_spec["short_sma"]
    assert fields["long_ma"]["period"] == strategy_spec["long_sma"]
    assert {
        (action["signal"], action["offset_bars"], action["price"])
        for action in config["fill_model"]["actions"]
    } == {
        ("entry", strategy_spec["fill_offset_bars"], strategy_spec["fill_price"]),
        ("exit", strategy_spec["fill_offset_bars"], strategy_spec["fill_price"]),
    }

    engine_request = build_engine_request(config)
    assert engine_request["schema_version"] == "engine_request.v2"
    assert engine_request["strategy"]["stream_binding"] == {
        "execution_stream_id": strategy_spec["execution_stream_id"],
        "decision_stream_id": strategy_spec["decision_stream_id"],
    }
    bundle = build_market_data_bundle(
        _external_data(source, engine_request),
        spec=market_data_spec_from_requirements(
            engine_request["data_requirements"],
            engine_request["strategy"]["stream_binding"],
        ),
        output_root=tmp_path / "market_data_bundle",
    )
    run = UnifiedBacktestRunnerBacktester().run(
        market_data_bundle=bundle,
        engine_request=engine_request,
    )
    result = run["portfolio_results"][0]
    expected = GOLDEN["result"]
    validation = result.validation_report
    result_validation = validation["result_validation"]
    summary = validation["rust_timeline_accounting_summary"]

    assert result.strategy_id == expected["strategy_id"]
    assert result_validation["status"] == expected["validation_status"]
    assert result_validation["result_hash"] == expected["result_hash"]
    assert all(
        check["status"] == "passed" for check in result_validation["checks"]
    )
    assert validation["accounting_kernel"] == expected["accounting_kernel"]
    assert validation["accounting_fast_path"] == expected["accounting_fast_path"]
    assert len(result.equity_curve) == expected["equity_rows"]
    assert len(result.execution_equity_curve) == expected["execution_equity_rows"]
    assert len(result.rebalance_trades) == expected["trade_rows"]
    assert len(result.rebalance_audit) == expected["rebalance_audit_rows"]
    assert summary["active_rebalances"] == expected["active_rebalances"]
    assert summary["final_equity"] == pytest.approx(expected["final_equity"], abs=1e-12)
    assert summary["intraday_max_drawdown"] == pytest.approx(
        expected["intraday_max_drawdown"], abs=1e-12
    )

    first_equity = result.equity_curve.iloc[0]
    last_equity = result.equity_curve.iloc[-1]
    assert first_equity["Session_label"] == expected["first_equity_session"]
    assert first_equity["Equity_value"] == pytest.approx(expected["first_equity"], abs=1e-12)
    assert last_equity["Session_label"] == expected["last_equity_session"]
    assert last_equity["Equity_value"] == pytest.approx(expected["last_equity"], abs=1e-12)
    first_trade = result.rebalance_trades.iloc[0]
    last_trade = result.rebalance_trades.iloc[-1]
    assert first_trade["Time"] == expected["first_trade_time"]
    assert first_trade["Action"] == expected["first_trade_action"]
    assert last_trade["Time"] == expected["last_trade_time"]
    assert last_trade["Action"] == expected["last_trade_action"]

    metrics = compute_metrics_for_frame(
        result.execution_equity_curve,
        time_unit=config["metricstracker"]["time_unit"],
        risk_free_rate=config["metricstracker"]["risk_free_rate"],
        backtest_id=result.strategy_id,
    )
    expected_metrics = GOLDEN["metrics"]
    assert metrics["Annualization"] == {
        "schema_version": "metrics_annualization.v1",
        "basis": expected_metrics["basis"],
        "projection_policy": expected_metrics["projection_policy"],
        "periods_per_year": expected_metrics["periods_per_year"],
        "risk_free_rate_annual": config["metricstracker"]["risk_free_rate"],
    }
    assert metrics["Projected_session_count"] == expected_metrics[
        "projected_session_count"
    ]
    assert metrics["Projected_return_interval_count"] == expected_metrics[
        "projected_return_interval_count"
    ]
    assert metrics["Total_return"] == pytest.approx(
        expected_metrics["total_return"], abs=1e-12
    )
    assert metrics["Annualized_return (CAGR)"] == pytest.approx(
        expected_metrics["annualized_return_cagr"], abs=1e-12
    )
    assert metrics["Sharpe"] == pytest.approx(expected_metrics["sharpe"], abs=1e-12)
    assert metrics["Intraday_max_drawdown"] == pytest.approx(
        expected["intraday_max_drawdown"], abs=1e-12
    )
