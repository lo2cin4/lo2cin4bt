from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXED_ALLOCATION_CONFIG = (
    REPO_ROOT
    / "backtester"
    / "contracts"
    / "strategy"
    / "examples"
    / "strategy-run-vti-avuv-vxus-sgol-dbmf-yfinance-yearly-rebalance-example.json"
)
ROTATION_CONFIG = (
    REPO_ROOT
    / "backtester"
    / "contracts"
    / "strategy"
    / "examples"
    / "strategy-run-voo-gld-yfinance-daily-momentum90-sma250-rotation-example.json"
)
LONG_SHORT_ROTATION_CONFIG = (
    REPO_ROOT
    / "backtester"
    / "contracts"
    / "strategy"
    / "examples"
    / "strategy-run-us-sector-etf-yfinance-monthly-12-1-long-short-rotation-example.json"
)
ADJUSTED_LONG_SHORT_CONFIG = (
    REPO_ROOT
    / "tests"
    / "fixtures"
    / "strategy-run-us-sector-etf-yfinance-monthly-adjusted-12-1-long-short-test.json"
)
CALENDAR_CONFIG = (
    REPO_ROOT
    / "backtester"
    / "contracts"
    / "strategy"
    / "examples"
    / "strategy-run-btcusdt-binance-monthly-nth-weekday-same-session-matrix-example.json"
)
PAIR_CONFIG = (
    REPO_ROOT
    / "backtester"
    / "contracts"
    / "strategy"
    / "examples"
    / "strategy-run-spy-qqq-yfinance-monthly-pair-spread-example.json"
)
MULTI_LEG_CONFIG = (
    REPO_ROOT
    / "backtester"
    / "contracts"
    / "strategy"
    / "examples"
    / "strategy-run-qqq-tlt-gld-yfinance-monthly-hedge-overlay-example.json"
)
SIGNAL_CONFIG = (
    REPO_ROOT
    / "backtester"
    / "contracts"
    / "strategy"
    / "examples"
    / "strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json"
)


def test_fixed_allocation_engine_request_executes_directly_in_rust(tmp_path: Path) -> None:
    bundle_mod = __import__("dataloader.market_data_bundle", fromlist=["dummy"])
    request_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    bridge = __import__("backtester.RustCoreBridge_backtester", fromlist=["dummy"])
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    config = json.loads(FIXED_ALLOCATION_CONFIG.read_text(encoding="utf-8"))
    config["data"].update(
        {
            "provider": "fixture",
            "start_date": "2023-12-29",
            "end_date": "2025-01-02",
        }
    )
    config["universe"]["symbols"] = ["AAA", "BBB"]
    config["allocation"]["weights"] = {"AAA": 0.6, "BBB": 0.4}
    config["risk"]["max_positions"] = 2
    config["metadata"]["strategy_id"] = "fixed_allocation_rust_engine_request_test"
    request = request_mod.build_engine_request(config)
    dates = pd.to_datetime(["2023-12-29", "2024-01-02", "2024-12-31", "2025-01-02"])
    close = pd.DataFrame(
        {"AAA": [100.0, 110.0, 121.0, 133.1], "BBB": [100.0, 100.0, 100.0, 100.0]},
        index=dates,
    )
    bundle = bundle_mod.build_market_data_bundle(
        {"close": close},
        spec={
            "provider": "fixture",
            "symbols": ["AAA", "BBB"],
            "frequency": "1D",
            "calendar": "XNYS",
            "timezone": "America/New_York",
            "point_in_time": True,
        },
        output_root=tmp_path / "bundles",
    )
    artifact_root = tmp_path / "rust-result"

    direct = bridge._ENGINE_SERVICE_CLIENT.execute_engine_request(
        request,
        bundle.read_manifest(),
        timeout=60,
        artifact_output_dir=str(artifact_root),
        artifact_run_id="direct-fixed-allocation",
    )

    returns = close.pct_change(fill_method=None)
    assert not returns.iloc[1:].isna().any().any()
    returns.iloc[0] = 0.0
    rebalance_dates = {dates[0], dates[1], dates[3]}
    checkpoints = []
    for date in dates:
        is_rebalance = date in rebalance_dates
        checkpoints.append(
            {
                "time": date.date().isoformat(),
                "rebalance": is_rebalance,
                "returns": {symbol: float(returns.at[date, symbol]) for symbol in close.columns},
                "target_weights": {"AAA": 0.6, "BBB": 0.4} if is_rebalance else {},
                "selected_assets": ["AAA", "BBB"] if is_rebalance else [],
                "ranked_assets": ["AAA", "BBB"] if is_rebalance else [],
                "score": {"AAA": 0.6, "BBB": 0.4} if is_rebalance else {},
                "eligible": {"AAA": True, "BBB": True} if is_rebalance else {},
                "rank_by": "fixed_weight" if is_rebalance else None,
            }
        )
    oracle = bridge.run_accounting_via_cli(
        {
            "config": {
                "starting_equity": 100.0,
                "cost_rate": 0.0015,
                "max_gross_exposure": 1.0,
                "allow_short": False,
                "risk_gates": {"max_positions": 2},
            },
            "checkpoints": checkpoints,
        },
        timeout=60,
    )

    assert direct["final_equity"] == pytest.approx(oracle["final_equity"])
    assert direct["average_turnover"] == pytest.approx(oracle["average_turnover"])
    assert direct["active_rebalances"] == oracle["active_rebalances"] == 3
    assert len(direct["events"]) == len(oracle["events"])
    for direct_event, oracle_event in zip(direct["events"], oracle["events"]):
        assert direct_event["time"] == oracle_event["time"]
        assert direct_event["active_positions"] == oracle_event["active_positions"]
        for field in (
            "equity_before_trade",
            "equity_after_trade",
            "portfolio_return",
            "turnover",
            "cost_drag",
            "cash_weight",
            "gross_exposure",
        ):
            assert direct_event[field] == pytest.approx(oracle_event[field], abs=1e-12)
        for field in ("target_weights", "drift_weights", "contribution"):
            assert direct_event[field] == pytest.approx(oracle_event[field], abs=1e-12)
    assert direct["artifact_bundle"]["schema_version"] == "rust_portfolio_result_bundle.v1"

    partial_config = json.loads(json.dumps(config))
    partial_config["fill_model"]["liquidity"] = {"max_fill_fraction": 0.5}
    partial = bridge._ENGINE_SERVICE_CLIENT.execute_engine_request(
        request_mod.build_engine_request(partial_config),
        bundle.read_manifest(),
        timeout=60,
    )
    first_event = partial["events"][0]
    assert first_event["target_weights"] == pytest.approx({"AAA": 0.3, "BBB": 0.2})
    assert first_event["turnover"] == pytest.approx(0.5)
    assert {order["status"] for order in first_event["orders"]} == {"partially_filled"}


def test_rotation_engine_request_executes_selection_directly_in_rust(tmp_path: Path) -> None:
    bundle_mod = __import__("dataloader.market_data_bundle", fromlist=["dummy"])
    request_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    bridge = __import__("backtester.RustCoreBridge_backtester", fromlist=["dummy"])
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    config = json.loads(ROTATION_CONFIG.read_text(encoding="utf-8"))
    config["data"].update(
        {
            "provider": "fixture",
            "start_date": "2024-01-01",
            "end_date": "2024-01-04",
        }
    )
    config["universe"]["symbols"] = ["AAA", "BBB"]
    config["computed_fields"][0]["period"] = 1
    config["computed_fields"][1]["period"] = 2
    config["metadata"]["strategy_id"] = "rotation_rust_engine_request_test"
    request = request_mod.build_engine_request(config)
    dates = pd.date_range("2024-01-01", periods=4, freq="D")
    close = pd.DataFrame(
        {"AAA": [10.0, 11.0, 12.0, 13.0], "BBB": [10.0, 9.0, 8.0, 7.0]},
        index=dates,
    )
    bundle = bundle_mod.build_market_data_bundle(
        {"close": close},
        spec={
            "provider": "fixture",
            "symbols": ["AAA", "BBB"],
            "frequency": "1D",
            "calendar": "XNYS",
            "timezone": "America/New_York",
            "point_in_time": True,
        },
        output_root=tmp_path / "bundles",
    )

    direct = bridge._ENGINE_SERVICE_CLIENT.execute_engine_request(
        request,
        bundle.read_manifest(),
        timeout=60,
        artifact_output_dir=str(tmp_path / "rust-result"),
        artifact_run_id="direct-rotation",
    )

    assert direct["candidate_count"] == 1
    assert len(direct["results"]) == 1
    result = direct["results"][0]
    assert result["candidate_id"] == "rotation_rust_engine_request_test"
    assert result["days"] == 4
    assert result["active_rebalances"] == 1
    assert result["final_equity"] > 100.0
    assert direct["artifact_bundle"]["schema_version"] == "rust_portfolio_result_bundle.v1"

    partial_config = json.loads(json.dumps(config))
    partial_config["fill_model"]["liquidity"] = {"max_fill_fraction": 0.5}
    partial = bridge._ENGINE_SERVICE_CLIENT.execute_engine_request(
        request_mod.build_engine_request(partial_config),
        bundle.read_manifest(),
        timeout=60,
    )
    partial_events = partial["results"][0]["summary"]["events"]
    partial_orders = [order for event in partial_events for order in event.get("orders", [])]
    assert partial_orders
    assert {order["status"] for order in partial_orders} == {"partially_filled"}


def test_long_short_rotation_executes_next_open_and_borrow_cost_in_rust(
    tmp_path: Path,
) -> None:
    bundle_mod = __import__("dataloader.market_data_bundle", fromlist=["dummy"])
    request_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    bridge = __import__("backtester.RustCoreBridge_backtester", fromlist=["dummy"])
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    config = json.loads(LONG_SHORT_ROTATION_CONFIG.read_text(encoding="utf-8"))
    symbols = ["AAA", "BBB", "CCC", "DDD"]
    config["data"].update(
        {"provider": "fixture", "start_date": "2024-01-31", "end_date": "2024-06-28"}
    )
    config["universe"]["symbols"] = symbols
    config["computed_fields"][0]["start_lag"] = 3
    config["computed_fields"][0]["end_lag"] = 1
    config["selection"]["long_top_n"] = 1
    config["selection"]["short_bottom_n"] = 1
    config["risk"]["max_positions"] = 2
    config["metadata"]["strategy_id"] = "long_short_next_open_rust_test"
    dates = pd.to_datetime(
        ["2024-01-31", "2024-02-29", "2024-03-28", "2024-04-30", "2024-05-31", "2024-06-28"]
    )
    close = pd.DataFrame(
        {
            "AAA": [100, 110, 120, 130, 140, 150],
            "BBB": [100, 105, 110, 115, 120, 125],
            "CCC": [100, 95, 90, 85, 80, 75],
            "DDD": [100, 90, 80, 70, 60, 50],
        },
        index=dates,
        dtype=float,
    )
    open_prices = close.shift(1).fillna(close.iloc[0])
    bundle = bundle_mod.build_market_data_bundle(
        {"open": open_prices, "close": close},
        spec={
            "provider": "fixture",
            "symbols": symbols,
            "frequency": "1D",
            "calendar": "XNYS",
            "timezone": "America/New_York",
            "point_in_time": True,
        },
        output_root=tmp_path / "long-short-bundle",
    )

    direct = bridge._ENGINE_SERVICE_CLIENT.execute_engine_request(
        request_mod.build_engine_request(config),
        bundle.read_manifest(),
        timeout=60,
    )

    events = direct["results"][0]["summary"]["events"]
    executed = [
        event
        for event in events
        if event["rebalance"] and any(abs(weight) > 1e-12 for weight in event["executed_weights"])
    ]
    assert executed
    assert executed[0]["decision_row"] < events.index(executed[0])
    assert sum(weight > 0 for weight in executed[0]["executed_weights"]) == 1
    assert sum(weight < 0 for weight in executed[0]["executed_weights"]) == 1
    assert any(event["borrow_cost"] > 0 for event in events)


def test_generic_computed_field_chain_executes_adjusted_momentum_in_rust(
    tmp_path: Path,
) -> None:
    bundle_mod = __import__("dataloader.market_data_bundle", fromlist=["dummy"])
    request_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    bridge = __import__("backtester.RustCoreBridge_backtester", fromlist=["dummy"])
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    config = json.loads(ADJUSTED_LONG_SHORT_CONFIG.read_text(encoding="utf-8"))
    symbols = ["AAA", "BBB", "CCC", "DDD"]
    config["data"].update(
        {"provider": "fixture", "start_date": "2024-01-31", "end_date": "2024-06-28"}
    )
    config["universe"]["symbols"] = symbols
    config["computed_fields"][1]["start_lag"] = 3
    config["selection"]["long_top_n"] = 1
    config["selection"]["short_bottom_n"] = 1
    config["risk"]["max_positions"] = 2
    config["metadata"]["strategy_id"] = "generic_adjusted_momentum_rust_test"
    dates = pd.to_datetime(
        ["2024-01-31", "2024-02-29", "2024-03-28", "2024-04-30", "2024-05-31", "2024-06-28"]
    )
    close = pd.DataFrame(
        {
            "AAA": [100, 100, 120, 60, 61, 62],
            "BBB": [100, 100, 110, 220, 221, 222],
            "CCC": [100, 100, 90, 90, 89, 88],
            "DDD": [100, 100, 80, 80, 79, 78],
        },
        index=dates,
        dtype=float,
    )
    open_prices = close.shift(1).fillna(close.iloc[0])
    bundle = bundle_mod.build_market_data_bundle(
        {"open": open_prices, "close": close},
        spec={
            "provider": "fixture",
            "symbols": symbols,
            "frequency": "1D",
            "calendar": "XNYS",
            "timezone": "America/New_York",
            "point_in_time": True,
        },
        output_root=tmp_path / "adjusted-momentum-bundle",
    )

    direct = bridge._ENGINE_SERVICE_CLIENT.execute_engine_request(
        request_mod.build_engine_request(config),
        bundle.read_manifest(),
        timeout=60,
    )

    events = direct["results"][0]["summary"]["events"]
    executed = [
        event
        for event in events
        if event["rebalance"] and any(abs(weight) > 1e-12 for weight in event["executed_weights"])
    ]
    assert executed
    first_weights = executed[0]["executed_weights"]
    assert first_weights[1] == pytest.approx(0.5)
    assert first_weights[3] == pytest.approx(-0.5)
    assert executed[0]["decision_row"] < events.index(executed[0])


def test_daily_rank_reads_factor_score_from_market_data_bundle(tmp_path: Path) -> None:
    bundle_mod = __import__("dataloader.market_data_bundle", fromlist=["dummy"])
    request_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    bridge = __import__("backtester.RustCoreBridge_backtester", fromlist=["dummy"])
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    config = json.loads(ROTATION_CONFIG.read_text(encoding="utf-8"))
    config["data"].update(
        {"provider": "fixture", "start_date": "2024-01-01", "end_date": "2024-01-03"}
    )
    config["universe"]["symbols"] = ["AAA", "BBB"]
    config["computed_fields"] = []
    config["selection"] = {
        "eligible": {"field": "close", "op": "gt", "value": 0},
        "rank_by": "composite_factor_score",
        "rank_order": "desc",
        "top_n": 1,
    }
    config["rebalance"]["trigger"]["op"] = "calendar.every_session"
    request = request_mod.build_engine_request(config)
    dates = pd.date_range("2024-01-01", periods=3, freq="D")
    close = pd.DataFrame({"AAA": [10.0, 11.0, 12.0], "BBB": [10.0, 10.0, 10.0]}, index=dates)
    factor_score = pd.DataFrame(
        {"AAA": [2.0, 1.0, 3.0], "BBB": [1.0, 2.0, 1.0]},
        index=dates,
    )
    bundle = bundle_mod.build_market_data_bundle(
        {"close": close, "composite_factor_score": factor_score},
        spec={
            "provider": "fixture",
            "symbols": ["AAA", "BBB"],
            "frequency": "1D",
            "calendar": "XNYS",
            "timezone": "America/New_York",
            "point_in_time": True,
        },
        output_root=tmp_path / "factor-bundle",
    )

    direct = bridge._ENGINE_SERVICE_CLIENT.execute_engine_request(
        request,
        bundle.read_manifest(),
        timeout=60,
    )

    events = direct["results"][0]["summary"]["events"]
    assert [event["selected_indices"] for event in events] == [[0], [1], [0]]


def test_daily_rank_engine_request_supports_month_start_rebalance(tmp_path: Path) -> None:
    bundle_mod = __import__("dataloader.market_data_bundle", fromlist=["dummy"])
    request_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    bridge = __import__("backtester.RustCoreBridge_backtester", fromlist=["dummy"])
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    config = json.loads(ROTATION_CONFIG.read_text(encoding="utf-8"))
    config["data"].update(
        {"provider": "fixture", "start_date": "2024-01-30", "end_date": "2024-02-02"}
    )
    config["universe"]["symbols"] = ["AAA", "BBB"]
    config["computed_fields"][0]["period"] = 1
    config["computed_fields"][1]["period"] = 2
    config["rebalance"]["trigger"]["op"] = "calendar.month_start"
    request = request_mod.build_engine_request(config)
    dates = pd.date_range("2024-01-30", periods=4, freq="D")
    close = pd.DataFrame({"AAA": [10, 11, 12, 13], "BBB": [10, 9, 8, 7]}, index=dates)
    bundle = bundle_mod.build_market_data_bundle(
        {"close": close},
        spec={
            "provider": "fixture",
            "symbols": ["AAA", "BBB"],
            "frequency": "1D",
            "calendar": "XNYS",
            "timezone": "America/New_York",
            "point_in_time": True,
        },
        output_root=tmp_path / "bundles",
    )

    direct = bridge._ENGINE_SERVICE_CLIENT.execute_engine_request(
        request,
        bundle.read_manifest(),
        timeout=60,
    )

    summary = direct["results"][0]["summary"]
    assert [event["rebalance"] for event in summary["events"]] == [True, False, True, False]
    assert len(summary["result_tables"]["rebalance_audit"]) == 2


def test_engine_request_batch_accepts_complete_mixed_decision_requests(tmp_path: Path) -> None:
    bundle_mod = __import__("dataloader.market_data_bundle", fromlist=["dummy"])
    request_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    bridge = __import__("backtester.RustCoreBridge_backtester", fromlist=["dummy"])
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    fixed = json.loads(FIXED_ALLOCATION_CONFIG.read_text(encoding="utf-8"))
    fixed["data"].update(
        {"provider": "fixture", "start_date": "2024-01-01", "end_date": "2024-01-04"}
    )
    fixed["universe"]["symbols"] = ["AAA", "BBB"]
    fixed["allocation"]["weights"] = {"AAA": 0.5, "BBB": 0.5}
    fixed["risk"]["max_positions"] = 2
    fixed["metadata"]["strategy_id"] = "batch_fixed"
    rotation = json.loads(ROTATION_CONFIG.read_text(encoding="utf-8"))
    rotation["data"].update(
        {"provider": "fixture", "start_date": "2024-01-01", "end_date": "2024-01-04"}
    )
    rotation["universe"]["symbols"] = ["AAA", "BBB"]
    rotation["computed_fields"][0]["period"] = 1
    rotation["computed_fields"][1]["period"] = 2
    rotation["metadata"]["strategy_id"] = "batch_rotation"
    requests = [
        request_mod.build_engine_request(fixed),
        request_mod.build_engine_request(rotation),
    ]
    dates = pd.date_range("2024-01-01", periods=4, freq="D")
    close = pd.DataFrame({"AAA": [10, 11, 12, 13], "BBB": [10, 9, 8, 7]}, index=dates)
    bundle = bundle_mod.build_market_data_bundle(
        {"close": close},
        spec={
            "provider": "fixture",
            "symbols": ["AAA", "BBB"],
            "frequency": "1D",
            "calendar": "XNYS",
            "timezone": "America/New_York",
            "point_in_time": True,
        },
        output_root=tmp_path / "bundles",
    )

    batch = bridge._ENGINE_SERVICE_CLIENT.execute_engine_request_batch(
        requests,
        bundle.read_manifest(),
        timeout=60,
    )

    assert batch["request_count"] == 2
    assert [item["strategy_id"] for item in batch["results"]] == [
        "batch_fixed",
        "batch_rotation",
    ]
    assert all(item["result"] for item in batch["results"])

    rotation_b = json.loads(ROTATION_CONFIG.read_text(encoding="utf-8"))
    rotation_b["data"].update(
        {"provider": "fixture", "start_date": "2024-01-01", "end_date": "2024-01-04"}
    )
    rotation_b["universe"]["symbols"] = ["AAA", "BBB"]
    rotation_b["computed_fields"][0]["period"] = 2
    rotation_b["computed_fields"][1]["period"] = 2
    rotation_b["metadata"]["strategy_id"] = "batch_rotation_b"
    grouped = bridge._ENGINE_SERVICE_CLIENT.execute_engine_request_batch(
        [requests[1], request_mod.build_engine_request(rotation_b)],
        bundle.read_manifest(),
        timeout=60,
    )

    assert grouped["execution_mode"] == "grouped"
    assert grouped["shape"] == "daily_rank"
    assert grouped["request_count"] == 2
    assert grouped["result"]["candidate_count"] == 2


@pytest.mark.parametrize(
    ("config_path", "symbols", "strategy_id"),
    [
        (CALENDAR_CONFIG, ["AAA"], "calendar_same_session_engine_request_test"),
        (PAIR_CONFIG, ["AAA", "BBB"], "pair_spread_engine_request_test"),
        (MULTI_LEG_CONFIG, ["AAA", "BBB", "CCC"], "multi_leg_engine_request_test"),
    ],
)
def test_calendar_timeline_shapes_execute_directly_from_engine_request(
    tmp_path: Path,
    config_path: Path,
    symbols: list[str],
    strategy_id: str,
) -> None:
    bundle_mod = __import__("dataloader.market_data_bundle", fromlist=["dummy"])
    request_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    bridge = __import__("backtester.RustCoreBridge_backtester", fromlist=["dummy"])
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["platform"]["workflow_id"] = "single_backtest"
    config["parameter_domains"] = {}
    config["data"].update(
        {
            "provider": "fixture",
            "start_date": "2024-01-01",
            "end_date": "2024-01-12",
        }
    )
    config["universe"]["symbols"] = symbols
    config["metadata"]["strategy_id"] = strategy_id
    if config_path == CALENDAR_CONFIG:
        config["signals"]["entry"]["ordinal"] = 1
        config["signals"]["entry"]["weekday"] = "monday"
    elif config_path == PAIR_CONFIG:
        config["fill_model"]["actions"][0]["weights"] = {"AAA": 1.0, "BBB": -1.0}
    else:
        config["signals"]["entry"]["ordinal"] = 1
        config["signals"]["entry"]["weekday"] = "monday"
        config["fill_model"]["actions"][0]["weights"] = {"AAA": 1.0}
        config["fill_model"]["actions"][1]["weights"] = {"BBB": 0.5, "CCC": 0.5}
        config["fill_model"]["actions"][2]["weights"] = {"AAA": 1.0}
    request = request_mod.build_engine_request(config)
    dates = pd.date_range("2024-01-01", periods=10, freq="B")
    close = pd.DataFrame(
        {
            symbol: [100.0 + row * (index + 1) for row in range(len(dates))]
            for index, symbol in enumerate(symbols)
        },
        index=dates,
    )
    open_ = close * 0.995
    bundle = bundle_mod.build_market_data_bundle(
        {"open": open_, "close": close},
        spec={
            "provider": "fixture",
            "symbols": symbols,
            "frequency": "1D",
            "calendar": "XNYS",
            "timezone": "America/New_York",
            "point_in_time": True,
        },
        output_root=tmp_path / strategy_id / "bundles",
    )

    direct = bridge._ENGINE_SERVICE_CLIENT.execute_engine_request(
        request,
        bundle.read_manifest(),
        timeout=60,
        artifact_output_dir=str(tmp_path / strategy_id / "rust-result"),
        artifact_run_id=strategy_id,
    )

    assert direct["candidate_count"] == 1
    assert direct["results"][0]["candidate_id"] == strategy_id
    assert direct["results"][0]["days"] == 10
    assert direct["artifact_bundle"]["schema_version"] == "rust_portfolio_result_bundle.v1"

    partial_config = json.loads(json.dumps(config))
    partial_config["fill_model"]["liquidity"] = {"max_fill_fraction": 0.5}
    partial = bridge._ENGINE_SERVICE_CLIENT.execute_engine_request(
        request_mod.build_engine_request(partial_config),
        bundle.read_manifest(),
        timeout=60,
    )
    partial_orders = [
        order
        for event in partial["results"][0]["timeline"]["events"]
        for action in event["actions"]
        for order in action["orders"]
    ]
    assert partial_orders
    assert {order["status"] for order in partial_orders} == {"partially_filled"}


def test_single_asset_cross_signal_executes_directly_from_engine_request(tmp_path: Path) -> None:
    bundle_mod = __import__("dataloader.market_data_bundle", fromlist=["dummy"])
    request_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    bridge = __import__("backtester.RustCoreBridge_backtester", fromlist=["dummy"])
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    config = json.loads(SIGNAL_CONFIG.read_text(encoding="utf-8"))
    config["platform"]["workflow_id"] = "single_backtest"
    config["parameter_domains"] = {}
    config["computed_fields"][0]["period"] = 2
    config["computed_fields"][1]["period"] = 3
    config["data"].update(
        {"provider": "fixture", "start_date": "2024-01-01", "end_date": "2024-01-12"}
    )
    config["universe"]["symbols"] = ["AAA"]
    config["metadata"]["strategy_id"] = "single_asset_cross_engine_request_test"
    request = request_mod.build_engine_request(config)
    dates = pd.date_range("2024-01-01", periods=9, freq="B")
    close = pd.DataFrame({"AAA": [10, 9, 8, 9, 10, 9, 8, 9, 10]}, index=dates)
    open_ = close * 0.995
    bundle = bundle_mod.build_market_data_bundle(
        {"open": open_, "close": close},
        spec={
            "provider": "fixture",
            "symbols": ["AAA"],
            "frequency": "1D",
            "calendar": "XNYS",
            "timezone": "America/New_York",
            "point_in_time": True,
        },
        output_root=tmp_path / "bundles",
    )

    direct = bridge._ENGINE_SERVICE_CLIENT.execute_engine_request(
        request,
        bundle.read_manifest(),
        timeout=60,
        artifact_output_dir=str(tmp_path / "rust-result"),
        artifact_run_id="single-asset-cross",
    )

    assert direct["candidate_count"] == 1
    result = direct["results"][0]
    assert result["candidate_id"] == "single_asset_cross_engine_request_test"
    assert result["days"] == 9
    assert result["active_rebalances"] >= 2
    assert direct["artifact_bundle"]["schema_version"] == "rust_portfolio_result_bundle.v1"

    partial_config = json.loads(json.dumps(config))
    partial_config["fill_model"]["liquidity"] = {"max_fill_fraction": 0.5}
    partial_request = request_mod.build_engine_request(partial_config)
    partial = bridge._ENGINE_SERVICE_CLIENT.execute_engine_request(
        partial_request,
        bundle.read_manifest(),
        timeout=60,
    )
    partial_orders = [
        order
        for event in partial["results"][0]["timeline"]["events"]
        for action in event["actions"]
        for order in action["orders"]
    ]
    assert partial_orders
    assert all(order["status"] == "partially_filled" for order in partial_orders)
    assert all(abs(order["remaining_delta"]) > 0.0 for order in partial_orders)

    runner_mod = __import__("backtester.UnifiedBacktestRunner_backtester", fromlist=["dummy"])
    production = runner_mod.UnifiedBacktestRunnerBacktester().run(
        market_data_bundle=bundle,
        engine_request=request,
    )
    validation = production["portfolio_result"].validation_report
    assert validation["accounting_fast_path"] == "signal_rust_engine_request_bundle"
    assert validation["signal_producer"] == "rust_engine_request_signal_timeline_v1"
    assert validation["feature_producer"] == "rust_engine_request_decision_fields_v1"


def test_shared_rust_matrix_bundle_is_filtered_per_candidate(tmp_path: Path) -> None:
    from backtester.UnifiedBacktestRunner_backtester import (
        UnifiedBacktestRunnerBacktester,
    )

    path = tmp_path / "equity.parquet"
    pd.DataFrame(
        {
            "Backtest_id": ["candidate-a", "candidate-a", "candidate-b"],
            "Time": ["2026-01-01", "2026-01-02", "2026-01-01"],
            "Equity_value": [100.0, 101.0, 100.0],
        }
    ).to_parquet(path, index=False)

    frame = UnifiedBacktestRunnerBacktester._artifact_bundle_frame(  # noqa: SLF001
        {"bundle_paths": {"equity_curve": str(path)}},
        "equity_curve",
        candidate_id="candidate-a",
    )

    assert len(frame) == 2
    assert frame["Backtest_id"].unique().tolist() == ["candidate-a"]


def test_rust_artifact_bundle_reader_fails_when_table_is_missing() -> None:
    from backtester.UnifiedBacktestRunner_backtester import (
        UnifiedBacktestRunnerBacktester,
    )

    with pytest.raises(RuntimeError, match="missing table path"):
        UnifiedBacktestRunnerBacktester._artifact_bundle_frame(  # noqa: SLF001
            {"bundle_paths": {}},
            "equity_curve",
            candidate_id="candidate-a",
        )


def test_rust_compact_result_rejects_missing_required_accounting_values() -> None:
    from backtester.UnifiedBacktestRunner_backtester import (
        UnifiedBacktestRunnerBacktester,
    )

    item = {
        "candidate_id": "candidate-a",
        "final_equity": 101.0,
        "total_return": 0.01,
        "days": 2,
        "average_turnover": 0.5,
        "average_gross_exposure": 1.0,
        "result_validation": {
            "schema_version": "result_validation_report.v1",
            "status": "valid",
            "result_hash": "a" * 64,
        },
    }

    with pytest.raises(ValueError, match="active_rebalances"):
        UnifiedBacktestRunnerBacktester()._portfolio_matrix_row_from_rust_compact(  # noqa: SLF001
            item=item,
            config={"strategy_id": "candidate-a"},
        )


def test_rust_compact_result_preserves_valid_zero_counts() -> None:
    from backtester.UnifiedBacktestRunner_backtester import (
        UnifiedBacktestRunnerBacktester,
    )

    item = {
        "candidate_id": "candidate-a",
        "final_equity": 100.0,
        "total_return": 0.0,
        "days": 2,
        "active_rebalances": 0,
        "average_turnover": 0.0,
        "average_gross_exposure": 0.0,
        "result_validation": {
            "schema_version": "result_validation_report.v1",
            "status": "valid",
            "result_hash": "a" * 64,
        },
    }

    row = UnifiedBacktestRunnerBacktester()._portfolio_matrix_row_from_rust_compact(  # noqa: SLF001
        item=item,
        config={"strategy_id": "candidate-a"},
    )

    assert row["rebalance_count"] == 0
    assert row["avg_turnover"] == 0.0
