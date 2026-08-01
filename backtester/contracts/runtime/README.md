# Runtime Contracts

This folder holds contract-first runtime artifacts that sit between authoring
config and exported result bundles.

- `normalized-strategy-plan-v1.schema.json`
  - planner output contract shared by single backtest, matrix, and WFA flows
- `canonical-result-bundle-v1.schema.json`
  - compact bundle contract consumed by metrics/payload readers
- `engine-request-v2.schema.json`
  - the only public Python runtime request accepted by the autorunner and each
    WFA/rolling-validation window
  - includes a stable request hash, explicit workflow/window, resolved
    parameters, simulation policy, output policy, and lineage
- `market-data-bundle-v2.schema.json`
  - versioned Rust/Python contract for normalized market data transport
  - the DataLoader seals provider/file frames into this content-addressed
    Parquet bundle before the shared Rust runtime
  - contains one typed external execution stream, an explicit execution
    timeline, and concrete session windows
  - direct daily provider bars remain direct with `session_label` row keys;
    intraday bars use UTC `event_timestamp` row keys
  - requires exact OHLCV/timeline row alignment and has no frequency,
    compatibility-mapper, aggregation, or fallback path
- `bar-time-contract-v1.schema.json`
  - Phase 0 typed time-bar contract shared by the planned in-place
    `strategy_run`, EngineRequest, and MarketDataBundle cutover
  - separates stream role (`execution`/`decision`) from source lineage
    (`external`/`derived`) while requiring exactly one external execution stream
  - records UTC nanosecond availability ordering, session semantics, price
    policy, and shared-Rust-only derived lineage
  - requires an explicit final-partial policy: `omit` drops a final incomplete
    fixed bucket, while `emit` is legal only when the bucket reaches the
    declared session-window close and therefore becomes available at that
    close; it never exposes an in-progress bar
  - for example, an XNYS `1h` stream feeding a session stream uses `emit` so
    the final 30 minutes of regular and half-day sessions remain in the
    downstream session OHLCV and external-source lineage
- `provider-timeframe-capability-v1.schema.json`
  - fail-closed provider declaration for exact external timeframe, history,
    pagination, timestamp, calendar/session, price, corporate-action, and
    quality capabilities
  - explicitly forbids provider and frequency fallback

The timeframe contracts and MarketDataBundle v2 form one in-place runtime
contract. They do not activate a second runtime or accept two timeframe
syntaxes. See
`docs/architecture/multi-timeframe-contract-migration.zh-Hant.md`.

Supported strategy profiles compile into `EngineRequest.v2` and are validated
against the same generated fixtures in Python and Rust. Derived decision bars
may only be produced by the shared Rust aggregation route; the DataLoader never
aggregates a direct daily or intraday execution stream.
