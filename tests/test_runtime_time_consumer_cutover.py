from __future__ import annotations

import json
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
STRATEGY_PATH = (
    REPO_ROOT
    / "backtester"
    / "contracts"
    / "strategy"
    / "examples"
    / "strategy-run-voo-gld-yfinance-daily-momentum90-sma250-rotation-example.json"
)
WFA_PATH = (
    REPO_ROOT
    / "backtester"
    / "contracts"
    / "strategy"
    / "examples"
    / "wfa-run-voo-gld-yfinance-daily-momentum90-sma250-rotation-example.json"
)
LEGACY_TIME_FIELDS = {"frequency", "interval", "calendar", "timezone"}


def _strategy() -> dict:
    return json.loads(STRATEGY_PATH.read_text(encoding="utf-8"))


def _execution_stream(data: dict) -> dict:
    stream_id = data["stream_binding"]["execution_stream_id"]
    return next(
        stream
        for stream in data["bar_time"]["streams"]
        if stream["stream_id"] == stream_id
    )


def test_autorunner_runtime_packet_preserves_typed_time_only() -> None:
    mod = __import__("autorunner.ConfigLoader_autorunner", fromlist=["dummy"])

    loaded = mod.ConfigLoader().load_config(str(STRATEGY_PATH))

    assert loaded is not None
    assert LEGACY_TIME_FIELDS.isdisjoint(loaded.dataloader_config)
    assert loaded.dataloader_config["bar_time"] == _strategy()["data"]["bar_time"]
    assert loaded.dataloader_config["stream_binding"] == _strategy()["data"][
        "stream_binding"
    ]


def test_wfa_runtime_packet_preserves_typed_time_only() -> None:
    mod = __import__(
        "validation_workflow.ConfigLoader_validation_workflow",
        fromlist=["dummy"],
    )

    loaded = mod.ConfigLoader().load_config(str(WFA_PATH))

    assert loaded is not None
    assert LEGACY_TIME_FIELDS.isdisjoint(loaded.dataloader_config)
    strategy_data = loaded.backtester_config["strategy_run_config"]["data"]
    assert loaded.dataloader_config["bar_time"] == strategy_data["bar_time"]
    assert loaded.dataloader_config["stream_binding"] == strategy_data[
        "stream_binding"
    ]


def test_unified_portfolio_context_carries_typed_time_only() -> None:
    runner_mod = __import__(
        "backtester.UnifiedBacktestRunner_backtester",
        fromlist=["dummy"],
    )
    config_mod = __import__(
        "backtester.StrategyRunConfig_backtester",
        fromlist=["dummy"],
    )
    normalized = config_mod.normalize_strategy_run_config(_strategy())

    portfolio = runner_mod.UnifiedBacktestRunnerBacktester._portfolio_config_from_normalized(
        normalized
    )

    assert LEGACY_TIME_FIELDS.isdisjoint(portfolio["data_context"])
    assert portfolio["data_context"] == {
        "bar_time": normalized["data"]["bar_time"],
        "stream_binding": normalized["data"]["stream_binding"],
    }


def test_autorunner_data_loader_rejects_legacy_time_and_records_bar_spec() -> None:
    mod = __import__("autorunner.DataLoader_autorunner", fromlist=["dummy"])
    data = _strategy()["data"]
    config = {
        "source": "multi_asset",
        "bar_time": data["bar_time"],
        "stream_binding": data["stream_binding"],
    }
    loader = mod.DataLoaderAutorunner()

    loaded = loader.load_data(config)

    assert loaded is not None
    summary = loader.get_loading_summary()
    assert "frequency" not in summary
    assert summary["execution_stream_id"] == "execution_daily"
    assert summary["bar_spec"] == _execution_stream(data)["bar_spec"]

    with pytest.raises(ValueError, match="legacy time fields"):
        loader.load_data({**config, "frequency": "1D"})


def test_provider_adapters_derive_native_interval_only_from_typed_bar_spec() -> None:
    ibkr_mod = __import__("dataloader.ibkr_loader", fromlist=["dummy"])
    futu_mod = __import__("dataloader.futu_loader", fromlist=["dummy"])
    multi_asset_mod = __import__("dataloader.market_data_loader", fromlist=["dummy"])
    data = _strategy()["data"]
    spec = {"execution_stream": _execution_stream(data)}

    class KLType:
        K_DAY = "K_DAY"

    assert ibkr_mod.IBKRMarketDataLoader._bar_size(spec) == "1 day"
    assert futu_mod.FutuMarketDataLoader._futu_ktype(spec, KLType) == "K_DAY"
    assert (
        multi_asset_mod.MultiAssetMarketDataLoader._provider_interval(
            spec,
            provider="binance",
        )
        == "1d"
    )
    assert (
        multi_asset_mod.MultiAssetMarketDataLoader._coinbase_granularity(spec)
        == 86400
    )

    with pytest.raises(ValueError, match="typed execution_stream"):
        ibkr_mod.IBKRMarketDataLoader._bar_size({"interval": "1d"})
    with pytest.raises(ValueError, match="typed execution_stream"):
        futu_mod.FutuMarketDataLoader._futu_ktype({"frequency": "1D"}, KLType)


def test_app_lineage_identity_uses_typed_execution_stream() -> None:
    runtime_mod = __import__("app.runtime.runtime", fromlist=["dummy"])
    data = _strategy()["data"]
    dataloader = {
        "source": "multi_asset",
        "asset_symbols": ["VOO", "GLD"],
        "bar_time": data["bar_time"],
        "stream_binding": data["stream_binding"],
    }

    identity = runtime_mod.AppRuntimeService._lineage_provider_identity(
        provider="yfinance",
        dataloader_config=dataloader,
        raw_config=_strategy(),
    )

    assert {"frequency", "interval", "calendar"}.isdisjoint(identity)
    assert identity["execution_stream_id"] == "execution_daily"
    assert identity["bar_spec"] == _execution_stream(data)["bar_spec"]
    assert identity["calendar_id"] == "XNYS"
    assert identity["timezone"] == "America/New_York"
