# dataloader

`dataloader` is now a non-interactive market data boundary. New runtime code
must not call the retired prompt-driven loader wrappers.

## Active Entry Points

- `market_data_loader.py`
  - Main multi-asset loader used by app runtime, backtester payloads, and
    portfolio/WFA paths.
  - Supports provider specs such as `yfinance`, `binance`, `coinbase`, `futu`,
    `ibkr`, file-backed wide frames, and provider data joined with local
    external features.
- `futu_loader.py` / `ibkr_loader.py`
  - Broker data adapters used only through `MultiAssetMarketDataLoader`.

Return calculations are owned by the shared Rust core. Python loaders do not
calculate or fall back to a second return implementation.

## Retired Legacy Files

These prompt-driven / wrapper modules have been removed:

- `base_loader.py`
- `validator_loader.py`
- `file_loader.py`
- `yfinance_loader.py`
- `binance_loader.py`
- `coinbase_loader.py`

Use `MultiAssetMarketDataLoader` for current strategy configs. Single-asset
strategies are represented as one-symbol `strategy_run` configs and are loaded
by the unified runner through the same market-data boundary.

## Multi-Asset Boundary

Provider requests are compiled from an EngineRequest with
`market_data_spec_from_requirements`. Do not author a loader interval directly:
the provider adapter derives its exact provider value from the bound execution
stream `BarSpec`, and rejects unsupported combinations without aliases or
fallbacks.

## Exact Provider Window Contract

The capability contract describes what the adapter can request exactly. It is
not a promise that a local broker gateway, account permission, entitlement, or
instrument history is available.

| Provider | Certified native bars | Adapter history/window rule |
| --- | --- | --- |
| yfinance | 1d only | Intraday and higher direct periods are rejected. Missing rows, duplicate timestamps, and out-of-order timestamps fail the run; no provider or frequency fallback exists. |
| Binance Spot | 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d | Pages at 1,000 candles. Explicit end timestamps are treated as exclusive and provider responses are filtered to `[start, end)`. Available history still begins at the symbol listing. |
| Coinbase Exchange | 1m, 5m, 15m, 1h, 6h, 1d | Pages at 300 candles. Each response is filtered to its requested half-open page window before duplicate validation because the provider may return boundary candles outside that window. |
| Futu OpenAPI | 1m, 5m, 15m, 30m, 1h, 1d | Pages at 1,000 candles. Requires the optional package, a running OpenD gateway, API permission, and market-data entitlement. |
| IBKR | 1m, 5m, 15m, 30m, 1h, 1d | Current adapter makes one bounded request and rejects wider spans up front. Requires the optional package, a running TWS/IB Gateway, API permission, and market-data entitlement. |

`unbounded` in a provider capability means “no additional adapter history
window cap after pagination.” It never means pre-listing data, guaranteed
broker entitlement, or guaranteed connectivity. Unsupported intervals and
unavailable windows fail; the loader does not change provider or frequency.

All certified providers apply one fail-closed quality gate after adapter
normalization. Missing OHLCV fields or values, duplicate timestamps,
out-of-order timestamps, misaligned fields, and missing bars stop the run.
Failures use `run_failure.v1`; Run Center and AI-readable output receive the
same `error_code`, provider, message, details, and corrective action.

## Provider Data Plus Local Features

Strategy configs should describe what data is needed. They should not require
users to prebuild one intermediate CSV per internal field. For example, a
strategy can download SPY/GLD prices from yfinance and join one local market
breadth file:

```python
frames = MultiAssetMarketDataLoader(repo_root=Path.cwd()).load(
    {
        "provider": "yfinance",
        "symbols": ["SPY", "GLD"],
        "start": "2010-01-01",
        "external_features": [
            {
                "name": "market_breadth",
                "path": "workspace/datasets/MARKET_BREADTH_1D.csv",
                "time_column": "time",
                "value_column": "close",
                "scope": "market",
            }
        ],
    }
)
```

The loader returns internal frames such as `open`, `high`, `low`, `close`, and
`market_breadth`. The source file stores the breadth value in its `close` column;
configs must name the real source column, not an idealized column name. If the external
feature file has no symbol column, it is treated as a market-level feature and
copied across the requested symbols. Signal thresholds belong in strategy
configs, not in generated input CSV files.
