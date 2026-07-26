import importlib
from pathlib import Path

import pandas as pd
import pytest


def test_multi_asset_market_data_loader_reads_wide_csv(tmp_path):
    mod = importlib.import_module("dataloader.market_data_loader")
    path = tmp_path / "close.csv"
    pd.DataFrame(
        {
            "Time": ["2024-01-03", "2024-01-02"],
            "AAA": ["101.5", "100.0"],
            "BBB": ["201.5", "200.0"],
        }
    ).to_csv(path, index=False)

    frames = mod.MultiAssetMarketDataLoader(repo_root=Path.cwd()).load(
        {"close": {"path": str(path), "time_column": "Time"}}
    )

    assert list(frames.keys()) == ["close"]
    assert frames["close"].index.strftime("%Y-%m-%d").tolist() == ["2024-01-02", "2024-01-03"]
    assert frames["close"].loc[pd.Timestamp("2024-01-03"), "AAA"] == pytest.approx(101.5)


def test_multi_asset_loader_seals_frames_as_market_data_bundle(tmp_path):
    mod = importlib.import_module("dataloader.market_data_loader")
    path = tmp_path / "close.csv"
    pd.DataFrame(
        {
            "Time": ["2024-01-02", "2024-01-03"],
            "AAA": [100.0, 101.0],
            "BBB": [200.0, 202.0],
        }
    ).to_csv(path, index=False)

    bundle = mod.MultiAssetMarketDataLoader(repo_root=Path.cwd()).load_bundle(
        {
            "symbols": ["AAA", "BBB"],
            "frequency": "1D",
            "close": {"path": str(path), "time_column": "Time"},
        },
        output_root=tmp_path / "bundles",
    )

    assert bundle.read_manifest()["symbols"] == ["AAA", "BBB"]
    assert bundle.load_frames()["close"].shape == (2, 2)


def test_market_data_spec_uses_engine_request_authority() -> None:
    mod = importlib.import_module("dataloader.market_data_loader")

    spec = mod.market_data_spec_from_requirements(
        {
            "provider": "yfinance",
            "symbols": ["SPY", "GLD"],
            "frequency": "1D",
            "calendar": "XNYS",
            "timezone": "America/New_York",
            "start_date": "2020-01-01",
            "end_date": "2024-01-01",
            "provider_config": {
                "provider": "wrong-provider",
                "symbols": ["WRONG"],
                "timeout": 20,
            },
        }
    )

    assert spec["provider"] == "yfinance"
    assert spec["symbols"] == ["SPY", "GLD"]
    assert spec["timeout"] == 20


def test_multi_asset_market_data_loader_dispatches_coinbase(monkeypatch):
    mod = importlib.import_module("dataloader.market_data_loader")
    calls = []

    def fake_download(self, spec):
        calls.append(spec)
        return {"close": pd.DataFrame({"BTC-USD": [100.0]}, index=[pd.Timestamp("2024-01-01")])}

    monkeypatch.setattr(mod.MultiAssetMarketDataLoader, "_download_coinbase", fake_download)

    frames = mod.MultiAssetMarketDataLoader(repo_root=Path.cwd()).load(
        {"provider": "coinbase", "symbols": ["BTC-USD"]}
    )

    assert calls and calls[0]["provider"] == "coinbase"
    assert frames["close"].loc[pd.Timestamp("2024-01-01"), "BTC-USD"] == pytest.approx(100.0)


@pytest.mark.parametrize(("field_name", "value"), [("frequency", "5m"), ("interval", "1h")])
def test_multi_asset_market_data_loader_rejects_subdaily_specs(field_name, value):
    mod = importlib.import_module("dataloader.market_data_loader")

    with pytest.raises(ValueError, match="session-level bars only"):
        mod.MultiAssetMarketDataLoader(repo_root=Path.cwd()).load(
            {"provider": "yfinance", "symbols": ["SPY", "GLD"], field_name: value}
        )


def test_multi_asset_market_data_loader_joins_market_level_external_feature(monkeypatch, tmp_path):
    mod = importlib.import_module("dataloader.market_data_loader")
    feature_path = tmp_path / "MARKET_BREADTH_1D.csv"
    pd.DataFrame(
        {
            "Date": ["2024-01-02", "2024-01-03"],
            "Breadth": [20.0, 14.0],
        }
    ).to_csv(feature_path, index=False)

    def fake_download(self, spec):
        index = pd.to_datetime(["2024-01-02", "2024-01-03"])
        return {
            "close": pd.DataFrame({"SPY": [100.0, 101.0], "GLD": [50.0, 52.0]}, index=index),
            "open": pd.DataFrame({"SPY": [99.0, 100.0], "GLD": [49.0, 51.0]}, index=index),
        }

    monkeypatch.setattr(mod.MultiAssetMarketDataLoader, "_download_yfinance", fake_download)

    frames = mod.MultiAssetMarketDataLoader(repo_root=Path.cwd()).load(
        {
            "provider": "yfinance",
            "symbols": ["SPY", "GLD"],
            "external_features": [
                {
                    "name": "market_breadth",
                    "path": str(feature_path),
                    "time_column": "Date",
                    "value_column": "Breadth",
                    "scope": "market",
                }
            ],
        }
    )

    assert set(frames) >= {"open", "close", "market_breadth"}
    assert frames["market_breadth"].columns.tolist() == ["SPY", "GLD"]
    assert frames["market_breadth"].loc[pd.Timestamp("2024-01-03"), "SPY"] == pytest.approx(14.0)
    assert frames["market_breadth"].loc[pd.Timestamp("2024-01-03"), "GLD"] == pytest.approx(14.0)


def test_multi_asset_market_data_loader_pivots_symbol_external_feature(tmp_path):
    mod = importlib.import_module("dataloader.market_data_loader")
    close_path = tmp_path / "close.csv"
    feature_path = tmp_path / "custom_factor.csv"
    pd.DataFrame(
        {
            "Time": ["2024-01-02", "2024-01-03"],
            "AAA": [100.0, 101.0],
            "BBB": [200.0, 202.0],
        }
    ).to_csv(close_path, index=False)
    pd.DataFrame(
        {
            "Time": ["2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
            "Symbol": ["AAA", "BBB", "AAA", "BBB"],
            "Factor": [1.0, 2.0, 1.5, 2.5],
        }
    ).to_csv(feature_path, index=False)

    frames = mod.MultiAssetMarketDataLoader(repo_root=Path.cwd()).load(
        {
            "symbols": ["AAA", "BBB"],
            "close": {"path": str(close_path), "time_column": "Time"},
            "external_features": [
                {
                    "name": "factor",
                    "path": str(feature_path),
                    "time_column": "Time",
                    "symbol_column": "Symbol",
                    "value_column": "Factor",
                }
            ],
        }
    )

    assert frames["factor"].columns.tolist() == ["AAA", "BBB"]
    assert frames["factor"].loc[pd.Timestamp("2024-01-03"), "AAA"] == pytest.approx(1.5)
    assert frames["factor"].loc[pd.Timestamp("2024-01-03"), "BBB"] == pytest.approx(2.5)


def test_coinbase_symbol_download_normalizes_ohlcv():
    mod = importlib.import_module("dataloader.market_data_loader")

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return [
                [1704153600, "90", "110", "100", "105", "12.5"],
                [1704240000, "95", "115", "105", "111", "9.0"],
            ]

    class FakeRequests:
        @staticmethod
        def get(url, params, timeout):
            assert url.endswith("/products/BTC-USD/candles")
            assert params["granularity"] == 86400
            assert timeout == 7
            return FakeResponse()

    frame = mod.MultiAssetMarketDataLoader._download_coinbase_symbol(
        requests_module=FakeRequests,
        api_base="https://example.test",
        symbol="BTC-USD",
        granularity=86400,
        start="2024-01-02",
        end="2024-01-04",
        timeout=7,
    )

    assert frame.index.strftime("%Y-%m-%d").tolist() == ["2024-01-02", "2024-01-03"]
    assert frame.loc[pd.Timestamp("2024-01-02"), "close"] == pytest.approx(105.0)
    assert frame.loc[pd.Timestamp("2024-01-03"), "volume"] == pytest.approx(9.0)


@pytest.mark.parametrize("provider", ["futu", "ibkr"])
def test_multi_asset_market_data_loader_validates_broker_provider_symbols(provider):
    mod = importlib.import_module("dataloader.market_data_loader")

    with pytest.raises(ValueError, match=f"provider={provider}"):
        mod.MultiAssetMarketDataLoader(repo_root=Path.cwd()).load({"provider": provider})


def test_futu_loader_symbol_mapping_without_gateway_import():
    mod = importlib.import_module("dataloader.futu_loader")

    assert mod.FutuMarketDataLoader._to_futu_code("QQQ", "US") == "US.QQQ"
    assert mod.FutuMarketDataLoader._to_futu_code("HK.00700", "US") == "HK.00700"


def test_ibkr_loader_bar_size_mapping():
    mod = importlib.import_module("dataloader.ibkr_loader")

    assert mod.IBKRMarketDataLoader._bar_size("1d") == "1 day"
    assert mod.IBKRMarketDataLoader._bar_size("5m") == "5 mins"
