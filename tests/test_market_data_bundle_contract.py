from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator


REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = (
    REPO_ROOT
    / "backtester"
    / "contracts"
    / "runtime"
    / "market-data-bundle-v1.schema.json"
)
EXAMPLE_PATH = (
    REPO_ROOT
    / "backtester"
    / "contracts"
    / "runtime"
    / "examples"
    / "market-data-bundle-v1.example.json"
)


def _frames() -> dict[str, pd.DataFrame]:
    index = pd.to_datetime(["2024-01-02", "2024-01-03"])
    return {
        "open": pd.DataFrame(
            {"AAA": [99.0, 100.0], "BBB": [199.0, 200.0]},
            index=index,
        ),
        "close": pd.DataFrame(
            {"AAA": [100.0, 101.0], "BBB": [200.0, 202.0]},
            index=index,
        ),
        "factor": pd.DataFrame(
            {"AAA": [1.0, 1.5], "BBB": [2.0, 2.5]},
            index=index,
        ),
    }


def _spec() -> dict[str, object]:
    return {
        "provider": "fixture",
        "symbols": ["AAA", "BBB"],
        "frequency": "1D",
        "calendar": "XNYS",
        "timezone": "America/New_York",
        "point_in_time": True,
        "adjustment_policy": "split_dividend_adjusted",
        "availability_policy": "bar_close",
    }


def test_builds_schema_valid_content_addressed_bundle(tmp_path: Path) -> None:
    mod = __import__("dataloader.market_data_bundle", fromlist=["dummy"])

    bundle = mod.build_market_data_bundle(
        _frames(),
        spec=_spec(),
        output_root=tmp_path,
    )
    manifest = bundle.read_manifest()

    Draft202012Validator(json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))).validate(
        manifest
    )
    assert manifest["bundle_id"].startswith("mdb-")
    assert bundle.content_hash == manifest["content_hash"]
    assert manifest["symbols"] == ["AAA", "BBB"]
    assert manifest["row_count"] == 2
    assert manifest["time_semantics"] == {
        "index_kind": "session_label",
        "event_time_column": "Time",
        "available_time_column": None,
        "availability_policy": "bar_close",
        "ordering": "event_time_then_table_name",
    }
    assert manifest["quality"]["duplicate_time_policy"] == "fail"
    assert manifest["tables"]["close"]["role"] == "bars"
    assert manifest["tables"]["factor"]["role"] == "features"
    assert manifest["tables"]["close"]["row_count"] == 2

    loaded = bundle.load_frames()
    expected = _frames()
    expected["close"].index.name = "Time"
    expected["factor"].index.name = "Time"
    pd.testing.assert_frame_equal(loaded["close"], expected["close"], check_freq=False)
    pd.testing.assert_frame_equal(loaded["factor"], expected["factor"], check_freq=False)


def test_loader_attaches_benchmark_without_expanding_trading_universe(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    loader_mod = __import__("dataloader.market_data_loader", fromlist=["dummy"])
    index = pd.to_datetime(["2024-01-02", "2024-01-03"])

    def fake_load(self, spec, config_file_path=None):
        symbols = list(spec["symbols"])
        values = {symbol: [100.0, 101.0] for symbol in symbols}
        return {"close": pd.DataFrame(values, index=index)}

    monkeypatch.setattr(loader_mod.MultiAssetMarketDataLoader, "load", fake_load)
    loader = loader_mod.MultiAssetMarketDataLoader(repo_root=REPO_ROOT)
    spec = {
        **_spec(),
        "benchmark": {"provider": "fixture", "symbol": "SPY", "label": "SPY"},
    }

    bundle = loader.load_bundle(spec, output_root=tmp_path)
    manifest = bundle.read_manifest()
    frames = bundle.load_frames()

    assert manifest["symbols"] == ["AAA", "BBB"]
    assert manifest["tables"]["benchmark_close"]["role"] == "benchmarks"
    assert manifest["tables"]["benchmark_close"]["columns"] == ["SPY"]
    assert list(frames["benchmark_close"].columns) == ["SPY"]


def test_same_content_has_same_bundle_hash_across_output_roots(tmp_path: Path) -> None:
    mod = __import__("dataloader.market_data_bundle", fromlist=["dummy"])

    first = mod.build_market_data_bundle(
        _frames(), spec=_spec(), output_root=tmp_path / "first"
    )
    second = mod.build_market_data_bundle(
        _frames(), spec=_spec(), output_root=tmp_path / "second"
    )

    assert first.content_hash == second.content_hash
    assert first.bundle_id == second.bundle_id


def test_shared_manifest_example_matches_python_canonical_hash() -> None:
    mod = __import__("dataloader.market_data_bundle", fromlist=["dummy"])
    manifest = json.loads(EXAMPLE_PATH.read_text(encoding="utf-8"))

    mod.validate_market_data_bundle_manifest(manifest)

    assert mod.market_data_bundle_content_hash(manifest) == manifest["content_hash"]


def test_bundle_rejects_duplicate_timestamps(tmp_path: Path) -> None:
    mod = __import__("dataloader.market_data_bundle", fromlist=["dummy"])
    frames = _frames()
    frames["close"] = pd.concat([frames["close"], frames["close"].iloc[[0]]])

    with pytest.raises(ValueError, match="duplicate timestamps"):
        mod.build_market_data_bundle(frames, spec=_spec(), output_root=tmp_path)


def test_bundle_removes_provider_rows_without_any_tradable_prices(
    tmp_path: Path,
) -> None:
    mod = __import__("dataloader.market_data_bundle", fromlist=["dummy"])
    frames = _frames()
    empty_time = pd.Timestamp("2024-01-04")
    for name in ("open", "close"):
        frames[name].loc[empty_time] = [float("nan"), float("nan")]

    bundle = mod.build_market_data_bundle(frames, spec=_spec(), output_root=tmp_path)
    manifest = bundle.read_manifest()

    assert manifest["row_count"] == 2
    assert manifest["quality"]["missing_value_policy"] == "drop_rows"
    assert manifest["quality"]["warnings"] == [
        "removed 1 provider rows without any tradable prices"
    ]
    assert empty_time not in bundle.load_frames()["close"].index


def test_bundle_rejects_partial_invalid_runtime_prices(tmp_path: Path) -> None:
    mod = __import__("dataloader.market_data_bundle", fromlist=["dummy"])
    frames = _frames()
    frames["close"].iloc[1, 0] = float("nan")

    with pytest.raises(ValueError, match="partial invalid prices for: AAA"):
        mod.build_market_data_bundle(frames, spec=_spec(), output_root=tmp_path)


def test_bundle_detects_table_tampering(tmp_path: Path) -> None:
    mod = __import__("dataloader.market_data_bundle", fromlist=["dummy"])
    bundle = mod.build_market_data_bundle(
        _frames(), spec=_spec(), output_root=tmp_path
    )
    close_path = Path(bundle.read_manifest()["tables"]["close"]["path"])
    tampered = pd.read_parquet(close_path)
    tampered.iloc[0, 0] = 999.0
    tampered.to_parquet(close_path)

    with pytest.raises(ValueError, match="content hash mismatch"):
        bundle.load_frames()


def test_bundle_rejects_engine_request_symbol_mismatch(tmp_path: Path) -> None:
    bundle_mod = __import__("dataloader.market_data_bundle", fromlist=["dummy"])
    request_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    config = json.loads(
        (
            REPO_ROOT
            / "backtester"
            / "contracts"
            / "strategy"
            / "examples"
            / "strategy-run-vti-avuv-vxus-sgol-dbmf-yfinance-yearly-rebalance-example.json"
        ).read_text(encoding="utf-8")
    )
    request = request_mod.build_engine_request(config)
    bundle = bundle_mod.build_market_data_bundle(
        _frames(), spec=_spec(), output_root=tmp_path
    )

    with pytest.raises(ValueError, match="symbols do not match"):
        bundle.validate_against_engine_request(request)


def test_autorunner_dataloader_returns_bundle_not_dataframe(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    loader_mod = __import__("dataloader.market_data_loader", fromlist=["dummy"])
    autorunner_mod = __import__("autorunner.DataLoader_autorunner", fromlist=["dummy"])
    request_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    config = json.loads(
        (
            REPO_ROOT
            / "backtester"
            / "contracts"
            / "strategy"
            / "examples"
            / "strategy-run-spy-qqq-yfinance-monthly-pair-spread-example.json"
        ).read_text(encoding="utf-8")
    )
    request = request_mod.build_engine_request(config)
    frames = _frames()
    frames = {key: frame.rename(columns={"AAA": "SPY", "BBB": "QQQ"}) for key, frame in frames.items()}
    monkeypatch.setattr(
        loader_mod.MultiAssetMarketDataLoader,
        "load",
        lambda self, spec, config_file_path=None: frames,
    )

    loader = autorunner_mod.DataLoaderAutorunner()
    bundle = loader.load_market_data_bundle(
        request,
        output_root=tmp_path,
    )

    assert isinstance(bundle, loader_mod.MarketDataBundle)
    assert bundle.read_manifest()["symbols"] == ["SPY", "QQQ"]
    assert loader.get_loading_summary()["bundle_id"] == bundle.bundle_id


def test_bundle_is_the_only_backtest_data_boundary(tmp_path: Path) -> None:
    bundle_mod = __import__("dataloader.market_data_bundle", fromlist=["dummy"])
    request_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    runner_mod = __import__("autorunner.BacktestRunner_autorunner", fromlist=["dummy"])
    config = {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "allocation_portfolio",
            "workflow_id": "single_backtest",
        },
        "data": {
            "provider": "fixture",
            "frequency": "1D",
            "calendar": "XNYS",
            "timezone": "America/New_York",
        },
        "universe": {"symbols": ["AAA", "BBB"]},
        "allocation": {
            "method": "fixed_weights",
            "weights": {"AAA": 0.6, "BBB": 0.4},
        },
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "fill_model": {"cost": {"transaction_cost": 0.0, "slippage": 0.0}},
        "risk": {"max_positions": 2, "max_gross_exposure": 1.0},
        "parameter_domains": {},
        "metadata": {"strategy_id": "bundle_boundary_probe"},
    }
    request = request_mod.build_engine_request(config)
    bundle = bundle_mod.build_market_data_bundle(
        _frames(),
        spec=_spec(),
        output_root=tmp_path,
    )
    runner = runner_mod.BacktestRunnerAutorunner()

    result = runner.run_backtest(bundle, request)

    assert result["success"] is True
    assert result["market_data_bundle_id"] == bundle.bundle_id
    assert result["market_data_bundle_hash"] == bundle.content_hash
    with pytest.raises(TypeError, match="MarketDataBundle"):
        runner.run_backtest(_frames()["close"], request)

    custom_simulation = json.loads(json.dumps(config))
    custom_simulation["simulation"] = {
        "account": {"account_type": "margin", "leverage_limit": 2.0},
    }
    custom_request = request_mod.build_engine_request(custom_simulation)
    margin_result = runner.run_backtest(bundle, custom_request)
    assert margin_result["success"] is True
    assert margin_result["market_data_bundle_id"] == bundle.bundle_id
