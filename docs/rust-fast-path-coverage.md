# Rust Fast Path Coverage

Updated: 2026-07-02

This inventory tracks the current shipped `strategy_run` examples and workspace runs.

## Covered Now

| Config / family | Current Rust path | Status |
| --- | --- | --- |
| `strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json` | `single_asset_next_open_timeline_rust_full_batch` | Covered |
| `strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json` | `single_asset_next_open_timeline_rust_full_batch` | Covered |
| `strategy-run-btcusdt-binance-monthly-nth-weekday-same-session-matrix-example.json` | `single_asset_calendar_same_session_rust_full_batch` | Covered |
| `strategy-run-voo-gld-yfinance-daily-momentum90-sma250-rotation-example.json` | `daily_rank_rust_direct_bundle` or `daily_rank_rust_producer` | Covered |
| `strategy-run-vti-avuv-vxus-sgol-dbmf-yfinance-yearly-rebalance-example.json` | `fixed_allocation_rust_direct_bundle` / `scheduled_target_weight_rust` | Covered |

## Repo-Tested Extra Family

| Family | Current Rust path | Status |
| --- | --- | --- |
| Calendar baseline/event overlay | `calendar_overlay_timeline_rust_full_batch` | Covered |
| Generic multi-leg event timeline | shared Rust timeline accounting | Covered |

## Still Fail-Fast By Design

These are not silent Python slow paths anymore:

- Daily-rank parameter matrices with more than one candidate and no dedicated Rust full-batch producer
- Generic portfolio parameter matrices that do not match a supported Rust producer shape
- Any future strategy family not explicitly mapped to a Rust producer

Those shapes now fail fast with a clear runtime error until a native Rust producer is added.
