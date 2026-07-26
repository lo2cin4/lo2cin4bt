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

## Multi-Asset Example

```python
from pathlib import Path

from dataloader.market_data_loader import MultiAssetMarketDataLoader

frames = MultiAssetMarketDataLoader(repo_root=Path.cwd()).load(
    {
        "provider": "yfinance",
        "symbols": ["QQQ", "GLD"],
        "start": "2020-01-01",
        "interval": "1d",
    }
)
```

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
