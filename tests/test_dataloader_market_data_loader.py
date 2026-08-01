import importlib
import json
from pathlib import Path

import pandas as pd
import pytest
import exchange_calendars as xcals


REPO_ROOT = Path(__file__).resolve().parents[1]
XNYS_1M_FIXTURE = (
    REPO_ROOT
    / "verification"
    / "fixtures"
    / "dataloader"
    / "xnys_1m_timestamp_session.csv"
)
QQQ_DAILY_CONFIG = (
    REPO_ROOT
    / "backtester"
    / "contracts"
    / "strategy"
    / "examples"
    / "strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json"
)
BTC_CONFIG = (
    REPO_ROOT
    / "backtester"
    / "contracts"
    / "strategy"
    / "examples"
    / "strategy-run-btcusdt-binance-monthly-nth-weekday-same-session-matrix-example.json"
)


def _execution_stream(*, unit: str, row_key_kind: str) -> dict:
    return {
        "stream_id": f"execution_{unit}",
        "role": "execution",
        "source": {"kind": "external", "provider_id": "file"},
        "session_scope": "regular",
        "row_key_kind": row_key_kind,
        "bar_spec": {
            "aggregation": "time",
            "step": 1,
            "unit": unit,
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


def _ohlcv(index: pd.DatetimeIndex, symbol: str) -> dict[str, pd.DataFrame]:
    close = pd.DataFrame(
        {symbol: [100.0 + offset for offset in range(len(index))]},
        index=index,
    )
    return {
        "open": close,
        "high": close + 1.0,
        "low": close - 1.0,
        "close": close,
        "volume": pd.DataFrame(100.0, index=index, columns=[symbol]),
    }


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
        {
            "execution_stream": _execution_stream(
                unit="day",
                row_key_kind="session_label",
            ),
            "close": {"path": str(path), "time_column": "Time"},
        }
    )

    assert list(frames.keys()) == ["close"]
    assert frames["close"].index.strftime("%Y-%m-%d").tolist() == ["2024-01-02", "2024-01-03"]
    assert frames["close"].loc[pd.Timestamp("2024-01-03"), "AAA"] == pytest.approx(101.5)


def test_multi_asset_loader_seals_frames_as_market_data_bundle(tmp_path):
    mod = importlib.import_module("dataloader.market_data_loader")
    price = pd.DataFrame(
        {
            "Time": ["2024-01-02", "2024-01-03"],
            "AAA": [100.0, 101.0],
            "BBB": [200.0, 202.0],
        }
    )
    fields = {}
    for name in ("open", "high", "low", "close", "volume"):
        path = tmp_path / f"{name}.csv"
        price.to_csv(path, index=False)
        fields[name] = {"path": str(path), "time_column": "Time"}
    timeline_path = tmp_path / "execution_timeline.csv"
    pd.DataFrame(
        {
            "Time": ["2024-01-02", "2024-01-03"],
            "external_execution_sequence": [0, 1],
            "bar_open_timestamp": [
                "2024-01-02T14:30:00Z",
                "2024-01-03T14:30:00Z",
            ],
            "bar_close_timestamp": [
                "2024-01-02T21:00:00Z",
                "2024-01-03T21:00:00Z",
            ],
            "available_timestamp": [
                "2024-01-02T21:00:00Z",
                "2024-01-03T21:00:00Z",
            ],
            "session_label": ["2024-01-02", "2024-01-03"],
        }
    ).to_csv(timeline_path, index=False)

    bundle = mod.MultiAssetMarketDataLoader(repo_root=Path.cwd()).load_bundle(
        {
            "provider": "file",
            "symbols": ["AAA", "BBB"],
            "calendar_id": "XNYS",
            "timezone": "America/New_York",
            "adjustment_policy": "raw",
            "execution_stream": _execution_stream(
                unit="day", row_key_kind="session_label"
            ),
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
                for label in ("2024-01-02", "2024-01-03")
            ],
            **fields,
        },
        output_root=tmp_path / "bundles",
    )

    assert bundle.read_manifest()["symbols"] == ["AAA", "BBB"]
    assert bundle.load_frames()["close"].shape == (2, 2)


def test_file_backed_1m_loader_preserves_event_timestamps_and_session_labels(
    tmp_path,
):
    mod = importlib.import_module("dataloader.market_data_loader")
    raw = pd.read_csv(XNYS_1M_FIXTURE)
    fields = {}
    for name in ("open", "high", "low", "close", "volume"):
        path = tmp_path / f"{name}.csv"
        raw.to_csv(path, index=False)
        fields[name] = {"path": str(path), "time_column": "Time"}
    times = pd.to_datetime(raw["Time"], utc=True)
    timeline_path = tmp_path / "execution_timeline.csv"
    pd.DataFrame(
        {
            "Time": raw["Time"],
            "external_execution_sequence": range(len(raw)),
            "bar_open_timestamp": times - pd.Timedelta(minutes=1),
            "bar_close_timestamp": times,
            "available_timestamp": times,
            "session_label": [
                "2024-03-11",
                "2024-07-03",
                "2024-07-03",
                "2024-07-03",
                "2024-11-04",
            ],
        }
    ).to_csv(timeline_path, index=False)

    bundle = mod.MultiAssetMarketDataLoader(repo_root=REPO_ROOT).load_bundle(
        {
            "provider": "file",
            "symbols": ["QQQ"],
            "calendar_id": "XNYS",
            "timezone": "America/New_York",
            "adjustment_policy": "raw",
            "execution_stream": _execution_stream(
                unit="minute", row_key_kind="event_timestamp"
            ),
            "execution_timeline": {
                "path": str(timeline_path),
                "time_column": "Time",
            },
            "session_windows": [
                {
                    "session_label": "2024-03-11",
                    "open_timestamp": "2024-03-11T13:30:00Z",
                    "close_timestamp": "2024-03-11T20:00:00Z",
                },
                {
                    "session_label": "2024-07-03",
                    "open_timestamp": "2024-07-03T13:30:00Z",
                    "close_timestamp": "2024-07-03T17:00:00Z",
                },
                {
                    "session_label": "2024-11-04",
                    "open_timestamp": "2024-11-04T14:30:00Z",
                    "close_timestamp": "2024-11-04T21:00:00Z",
                },
            ],
            **fields,
        },
        output_root=tmp_path / "bundles",
    )

    manifest = bundle.read_manifest()
    loaded = bundle.load_frames()["close"]
    timeline = bundle.load_execution_timeline()

    expected_index = pd.DatetimeIndex(
        [
            "2024-03-11T13:31:00Z",
            "2024-07-03T13:31:00Z",
            "2024-07-03T13:32:00Z",
            "2024-07-03T13:33:00Z",
            "2024-11-04T14:31:00Z",
        ],
        name="Time",
    )
    pd.testing.assert_index_equal(loaded.index, expected_index)
    assert loaded.columns.tolist() == ["QQQ"]
    assert timeline["session_label"].tolist() == [
        "2024-03-11",
        "2024-07-03",
        "2024-07-03",
        "2024-07-03",
        "2024-11-04",
    ]
    assert manifest["time_range"] == {
        "start": "2024-03-11T13:31:00+00:00",
        "end": "2024-11-04T14:31:00+00:00",
    }
    assert manifest["execution_stream"]["row_key_kind"] == "event_timestamp"
    assert manifest["execution_stream"]["bar_spec"]["unit"] == "minute"


def test_file_backed_loader_rejects_legacy_frequency_even_with_index_kind(tmp_path):
    mod = importlib.import_module("dataloader.market_data_loader")

    with pytest.raises(
        ValueError,
        match="rejects legacy time fields",
    ):
        mod.MultiAssetMarketDataLoader(repo_root=REPO_ROOT).load(
            {
                "provider": "file",
                "symbols": ["QQQ"],
                "frequency": "1m",
                "index_kind": "session_label",
                "close": {"path": str(XNYS_1M_FIXTURE), "time_column": "Time"},
            }
        )


def test_market_data_spec_uses_engine_request_authority() -> None:
    mod = importlib.import_module("dataloader.market_data_loader")
    stream = _execution_stream(unit="day", row_key_kind="session_label")
    stream["source"]["provider_id"] = "yfinance"

    spec = mod.market_data_spec_from_requirements(
        {
            "provider": "yfinance",
            "symbols": ["SPY", "GLD"],
            "bar_time": {
                "session_model": {
                    "calendar_id": "XNYS",
                    "timezone": "America/New_York",
                    "session_scope": "regular",
                },
                "price_model": {
                    "price_basis": "split_dividend_adjusted",
                },
                "streams": [stream],
            },
            "start_date": "2020-01-01",
            "end_date": "2024-01-01",
            "provider_config": {
                "provider": "wrong-provider",
                "symbols": ["WRONG"],
                "timeout": 20,
            },
        },
        {"execution_stream_id": stream["stream_id"]},
    )

    assert spec["provider"] == "yfinance"
    assert spec["symbols"] == ["SPY", "GLD"]
    assert spec["timeout"] == 20
    assert spec["calendar_id"] == "XNYS"
    assert spec["timezone"] == "America/New_York"
    assert spec["adjustment_policy"] == "split_dividend_adjusted"
    assert "frequency" not in spec


def test_multi_asset_market_data_loader_dispatches_coinbase(monkeypatch):
    mod = importlib.import_module("dataloader.market_data_loader")
    calls = []

    def fake_download(self, spec):
        calls.append(spec)
        return _ohlcv(pd.DatetimeIndex(["2024-01-01"]), "BTC-USD")

    monkeypatch.setattr(mod.MultiAssetMarketDataLoader, "_download_coinbase", fake_download)

    frames = mod.MultiAssetMarketDataLoader(repo_root=Path.cwd()).load(
        {
            "provider": "coinbase",
            "symbols": ["BTC-USD"],
            "adjustment_policy": "raw",
        }
    )

    assert calls and calls[0]["provider"] == "coinbase"
    assert frames["close"].loc[pd.Timestamp("2024-01-01"), "BTC-USD"] == pytest.approx(100.0)


@pytest.mark.parametrize(("field_name", "value"), [("frequency", "5m"), ("interval", "1h")])
def test_multi_asset_market_data_loader_rejects_legacy_time_specs(field_name, value):
    mod = importlib.import_module("dataloader.market_data_loader")

    with pytest.raises(ValueError, match="rejects legacy time fields"):
        mod.MultiAssetMarketDataLoader(repo_root=Path.cwd()).load(
            {"provider": "yfinance", "symbols": ["SPY", "GLD"], field_name: value}
        )


def test_row_key_kind_cannot_fallback_to_legacy_index_kind() -> None:
    mod = importlib.import_module("dataloader.market_data_loader")

    with pytest.raises(ValueError, match="typed execution_stream is required"):
        mod.MultiAssetMarketDataLoader._configured_row_key_kind(
            {"index_kind": "event_timestamp"}
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
        close = pd.DataFrame(
            {"SPY": [100.0, 101.0], "GLD": [50.0, 52.0]},
            index=index,
        )
        return {
            "open": close - 1.0,
            "high": close + 1.0,
            "low": close - 2.0,
            "close": close,
            "volume": pd.DataFrame(100.0, index=index, columns=["SPY", "GLD"]),
        }

    monkeypatch.setattr(mod.MultiAssetMarketDataLoader, "_download_yfinance", fake_download)

    stream = _execution_stream(unit="day", row_key_kind="session_label")
    stream["source"]["provider_id"] = "yfinance"
    frames = mod.MultiAssetMarketDataLoader(repo_root=Path.cwd()).load(
        {
            "provider": "yfinance",
            "symbols": ["SPY", "GLD"],
            "adjustment_policy": "split_dividend_adjusted",
            "execution_stream": stream,
            "session_model": {
                "calendar_id": "XNYS",
                "timezone": "America/New_York",
                "session_scope": "regular",
            },
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
            "execution_stream": _execution_stream(
                unit="day",
                row_key_kind="session_label",
            ),
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
        row_key_kind="session_label",
        timestamp_convention="bar_close",
        start="2024-01-02",
        end="2024-01-04",
        timeout=7,
    )

    assert frame.index.strftime("%Y-%m-%d").tolist() == ["2024-01-02", "2024-01-03"]
    assert frame.loc[pd.Timestamp("2024-01-02"), "close"] == pytest.approx(105.0)
    assert frame.loc[pd.Timestamp("2024-01-03"), "volume"] == pytest.approx(9.0)


def test_binance_bar_close_uses_exact_typed_boundary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from autorunner.DataLoader_autorunner import DataLoaderAutorunner
    from backtester.EngineRequest_backtester import build_engine_request
    import requests

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> list[list[object]]:
            return [
                [
                    1704067200000,
                    "100",
                    "110",
                    "90",
                    "105",
                    "12.5",
                    1704067259999,
                    "0",
                    1,
                    "0",
                    "0",
                    "0",
                ]
            ]

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: FakeResponse())
    config = json.loads(BTC_CONFIG.read_text(encoding="utf-8"))
    config["data"]["provider"] = "binance"
    config["data"]["benchmark"] = {
        "provider": "binance",
        "symbol": "BTCUSDT",
        "label": "BTCUSDT buy and hold",
    }
    config["universe"]["symbols"] = ["BTCUSDT"]
    stream = config["data"]["bar_time"]["streams"][0]
    stream["stream_id"] = "execution_minute"
    stream["source"]["provider_id"] = "binance"
    stream["bar_spec"] = {
        "aggregation": "time",
        "step": 1,
        "unit": "minute",
        "price_type": "last",
        "alignment": "session_open",
    }
    config["data"]["stream_binding"] = {
        "execution_stream_id": "execution_minute",
        "decision_stream_id": "execution_minute",
    }
    request = build_engine_request(config)

    bundle = DataLoaderAutorunner().load_market_data_bundle(
        request,
        output_root=tmp_path,
    )

    expected_close = pd.DatetimeIndex(["2024-01-01T00:01:00Z"], name="Time")
    assert bundle.load_frames()["close"].index.equals(expected_close)
    timeline = bundle.load_execution_timeline()
    assert timeline["bar_open_timestamp"].tolist() == ["2024-01-01T00:00:00Z"]
    assert timeline["bar_close_timestamp"].tolist() == ["2024-01-01T00:01:00Z"]
    assert timeline["available_timestamp"].tolist() == ["2024-01-01T00:01:00Z"]


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
    daily = {"execution_stream": _execution_stream(unit="day", row_key_kind="session_label")}
    five_minute = {
        "execution_stream": _execution_stream(
            unit="minute",
            row_key_kind="event_timestamp",
        )
    }
    five_minute["execution_stream"]["bar_spec"]["step"] = 5

    assert mod.IBKRMarketDataLoader._bar_size(daily) == "1 day"
    assert mod.IBKRMarketDataLoader._bar_size(five_minute) == "5 mins"


def test_market_data_spec_selects_bound_external_execution_stream() -> None:
    from backtester.EngineRequest_backtester import build_engine_request
    from dataloader.market_data_loader import market_data_spec_from_requirements

    config = json.loads(QQQ_DAILY_CONFIG.read_text(encoding="utf-8"))
    request = build_engine_request(config)
    spec = market_data_spec_from_requirements(
        request["data_requirements"],
        request["strategy"]["stream_binding"],
    )

    assert spec["execution_stream"]["stream_id"] == "execution_daily"
    assert spec["execution_stream"]["bar_spec"]["unit"] == "day"
    assert spec["session_model"] == config["data"]["bar_time"]["session_model"]
    assert "execution_stream" not in request["data_requirements"]["provider_config"]


def test_market_data_spec_rejects_unsupported_monthly_provider_stream() -> None:
    from backtester.EngineRequest_backtester import build_engine_request
    from dataloader.market_data_loader import market_data_spec_from_requirements

    config = json.loads(BTC_CONFIG.read_text(encoding="utf-8"))
    request = build_engine_request(config)
    request["data_requirements"]["bar_time"]["streams"][0]["bar_spec"][
        "unit"
    ] = "month"

    with pytest.raises(
        ValueError,
        match="only one-day session_label bars or minute/hour event_timestamp bars",
    ):
        market_data_spec_from_requirements(
            request["data_requirements"],
            request["strategy"]["stream_binding"],
        )


def test_public_yfinance_daily_builds_typed_timeline_with_dst_and_half_day(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from autorunner.DataLoader_autorunner import DataLoaderAutorunner
    from backtester.EngineRequest_backtester import build_engine_request
    from dataloader.market_data_loader import MultiAssetMarketDataLoader

    config = json.loads(QQQ_DAILY_CONFIG.read_text(encoding="utf-8"))
    request = build_engine_request(config)
    index = xcals.get_calendar("XNYS").sessions_in_range(
        "2024-03-08",
        "2024-11-29",
    )
    index = pd.DatetimeIndex(index, name="Time")
    monkeypatch.setattr(
        MultiAssetMarketDataLoader,
        "_download_yfinance",
        lambda _self, _spec: _ohlcv(index, "QQQ"),
    )

    bundle = DataLoaderAutorunner().load_market_data_bundle(
        request,
        output_root=tmp_path,
    )

    timeline = bundle.load_execution_timeline()
    selected = timeline[
        timeline["session_label"].isin(
            ["2024-03-08", "2024-03-11", "2024-11-29"]
        )
    ]
    assert selected["bar_open_timestamp"].tolist() == [
        "2024-03-08T14:30:00Z",
        "2024-03-11T13:30:00Z",
        "2024-11-29T14:30:00Z",
    ]
    assert selected["bar_close_timestamp"].tolist() == [
        "2024-03-08T21:00:00Z",
        "2024-03-11T20:00:00Z",
        "2024-11-29T18:00:00Z",
    ]


def test_public_crypto_daily_builds_utc_session_windows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from autorunner.DataLoader_autorunner import DataLoaderAutorunner
    from backtester.EngineRequest_backtester import build_engine_request
    from dataloader.market_data_loader import MultiAssetMarketDataLoader

    config = json.loads(BTC_CONFIG.read_text(encoding="utf-8"))
    stream = config["data"]["bar_time"]["streams"][0]
    stream["bar_spec"] = {
        "aggregation": "time",
        "step": 1,
        "unit": "day",
        "price_type": "last",
        "alignment": "session_open",
    }
    request = build_engine_request(config)
    index = pd.DatetimeIndex(["2024-01-01", "2024-01-02"], name="Time")
    monkeypatch.setattr(
        MultiAssetMarketDataLoader,
        "_download_coinbase",
        lambda _self, _spec: _ohlcv(index, "BTC-USD"),
    )

    bundle = DataLoaderAutorunner().load_market_data_bundle(
        request,
        output_root=tmp_path,
    )

    assert bundle.read_manifest()["session_windows"] == [
        {
            "session_label": "2024-01-01",
            "open_timestamp": "2024-01-01T00:00:00Z",
            "close_timestamp": "2024-01-02T00:00:00Z",
        },
        {
            "session_label": "2024-01-02",
            "open_timestamp": "2024-01-02T00:00:00Z",
            "close_timestamp": "2024-01-03T00:00:00Z",
        },
    ]


def test_event_timestamp_rows_keep_provider_times_and_explicit_xnys_membership(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    mod = importlib.import_module("dataloader.market_data_loader")
    index = pd.DatetimeIndex(
        ["2024-11-29T14:31:00Z", "2024-11-29T17:59:00Z"],
        name="Time",
    )
    monkeypatch.setattr(
        mod.MultiAssetMarketDataLoader,
        "load",
        lambda _self, _spec, config_file_path=None: _ohlcv(index, "QQQ"),
    )
    spec = {
        "provider": "yfinance",
        "symbols": ["QQQ"],
        "calendar_id": "XNYS",
        "timezone": "America/New_York",
        "adjustment_policy": "raw",
        "session_model": {
            "calendar_id": "XNYS",
            "timezone": "America/New_York",
            "session_scope": "regular",
        },
        "execution_stream": _execution_stream(
            unit="minute",
            row_key_kind="event_timestamp",
        ),
    }
    spec["execution_stream"]["source"]["provider_id"] = "yfinance"

    bundle = mod.MultiAssetMarketDataLoader(repo_root=REPO_ROOT).load_bundle(
        spec,
        output_root=tmp_path,
    )

    assert bundle.load_frames()["close"].index.equals(index)
    assert bundle.load_execution_timeline()["session_label"].tolist() == [
        "2024-11-29",
        "2024-11-29",
    ]
    assert bundle.read_manifest()["session_windows"][0]["close_timestamp"] == (
        "2024-11-29T18:00:00Z"
    )


@pytest.mark.parametrize(
    ("unit", "expected_close"),
    [
        ("minute", "2024-11-29T14:31:00Z"),
        ("hour", "2024-11-29T15:30:00Z"),
    ],
)
def test_yfinance_native_bar_open_is_normalized_to_typed_bar_close(
    unit: str,
    expected_close: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    mod = importlib.import_module("dataloader.market_data_loader")
    stream = _execution_stream(unit=unit, row_key_kind="event_timestamp")
    stream["source"]["provider_id"] = "yfinance"
    spec = {
        "provider": "yfinance",
        "symbols": ["QQQ"],
        "calendar_id": "XNYS",
        "timezone": "America/New_York",
        "adjustment_policy": "split_dividend_adjusted",
        "session_model": {
            "calendar_id": "XNYS",
            "timezone": "America/New_York",
            "session_scope": "regular",
        },
        "execution_stream": stream,
    }
    native_open = pd.DatetimeIndex(
        ["2024-11-29 09:30:00"],
        tz="America/New_York",
        name="Time",
    )
    row_keys = mod.MultiAssetMarketDataLoader._normalize_yfinance_row_keys(
        native_open,
        spec=spec,
    )
    assert row_keys.strftime("%Y-%m-%dT%H:%M:%SZ").tolist() == [expected_close]
    monkeypatch.setattr(
        mod.MultiAssetMarketDataLoader,
        "load",
        lambda _self, _spec, config_file_path=None: _ohlcv(row_keys, "QQQ"),
    )

    bundle = mod.MultiAssetMarketDataLoader(repo_root=REPO_ROOT).load_bundle(
        spec,
        output_root=tmp_path,
    )

    timeline = bundle.load_execution_timeline()
    assert timeline["bar_open_timestamp"].tolist() == [
        "2024-11-29T14:30:00Z"
    ]
    assert timeline["bar_close_timestamp"].tolist() == [expected_close]
    assert timeline["session_label"].tolist() == ["2024-11-29"]


@pytest.mark.parametrize(
    ("unit", "step", "native_open", "expected_close"),
    [
        ("hour", 1, "2024-03-11 15:30:00", "2024-03-11T20:00:00Z"),
        ("hour", 1, "2024-11-29 12:30:00", "2024-11-29T18:00:00Z"),
        ("minute", 90, "2024-11-29 12:30:00", "2024-11-29T18:00:00Z"),
    ],
)
def test_yfinance_final_partial_bar_clamps_to_xnys_session_close(
    unit: str,
    step: int,
    native_open: str,
    expected_close: str,
) -> None:
    mod = importlib.import_module("dataloader.market_data_loader")
    stream = _execution_stream(unit=unit, row_key_kind="event_timestamp")
    stream["bar_spec"]["step"] = step
    stream["source"]["provider_id"] = "yfinance"
    spec = {
        "provider": "yfinance",
        "symbols": ["QQQ"],
        "session_model": {
            "calendar_id": "XNYS",
            "timezone": "America/New_York",
            "session_scope": "regular",
        },
        "execution_stream": stream,
    }
    row_keys = mod.MultiAssetMarketDataLoader._normalize_yfinance_row_keys(
        pd.DatetimeIndex(
            [native_open],
            tz="America/New_York",
            name="Time",
        ),
        spec=spec,
    )

    assert row_keys.strftime("%Y-%m-%dT%H:%M:%SZ").tolist() == [expected_close]


@pytest.mark.parametrize(
    ("provider", "price_basis"),
    [
        ("yfinance", "split_adjusted"),
        ("coinbase", "split_dividend_adjusted"),
        ("binance", "split_dividend_adjusted"),
    ],
)
def test_provider_price_basis_mismatch_is_rejected(
    provider: str,
    price_basis: str,
) -> None:
    mod = importlib.import_module("dataloader.market_data_loader")

    with pytest.raises(ValueError, match="cannot exactly supply adjustment_policy"):
        mod.MultiAssetMarketDataLoader(repo_root=REPO_ROOT).load(
            {
                "provider": provider,
                "symbols": ["QQQ"],
                "adjustment_policy": price_basis,
            }
        )


def test_provider_time_materializer_rejects_unknown_calendar(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    mod = importlib.import_module("dataloader.market_data_loader")
    index = pd.DatetimeIndex(["2024-01-02"], name="Time")
    monkeypatch.setattr(
        mod.MultiAssetMarketDataLoader,
        "load",
        lambda _self, _spec, config_file_path=None: _ohlcv(index, "QQQ"),
    )
    spec = {
        "provider": "yfinance",
        "symbols": ["QQQ"],
        "calendar_id": "UNKNOWN",
        "timezone": "UTC",
        "adjustment_policy": "raw",
        "session_model": {
            "calendar_id": "UNKNOWN",
            "timezone": "UTC",
            "session_scope": "regular",
        },
        "execution_stream": _execution_stream(
            unit="day",
            row_key_kind="session_label",
        ),
    }
    spec["execution_stream"]["source"]["provider_id"] = "yfinance"

    with pytest.raises(ValueError, match="Unsupported calendar_id"):
        mod.MultiAssetMarketDataLoader(repo_root=REPO_ROOT).load_bundle(
            spec,
            output_root=tmp_path,
        )


def test_file_provider_requires_explicit_timeline_and_windows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    mod = importlib.import_module("dataloader.market_data_loader")
    index = pd.DatetimeIndex(["2024-01-02"], name="Time")
    monkeypatch.setattr(
        mod.MultiAssetMarketDataLoader,
        "load",
        lambda _self, _spec, config_file_path=None: _ohlcv(index, "QQQ"),
    )

    with pytest.raises(
        ValueError,
        match="other providers must supply execution_timeline and session_windows",
    ):
        mod.MultiAssetMarketDataLoader(repo_root=REPO_ROOT).load_bundle(
            {
                "provider": "file",
                "symbols": ["QQQ"],
                "execution_stream": _execution_stream(
                    unit="day",
                    row_key_kind="session_label",
                ),
            },
            output_root=tmp_path,
        )
