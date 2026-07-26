# Runtime Contracts

This folder holds contract-first runtime artifacts that sit between authoring
config and exported result bundles.

- `normalized-strategy-plan-v1.schema.json`
  - planner output contract shared by single backtest, matrix, and WFA flows
- `canonical-result-bundle-v1.schema.json`
  - compact bundle contract consumed by metrics/payload readers
- `engine-request-v1.schema.json`
  - the only public Python runtime request accepted by the autorunner and each
    WFA/rolling-validation window
  - includes a stable request hash, explicit workflow/window, resolved
    parameters, simulation policy, output policy, and lineage
- `market-data-bundle-v1.schema.json`
  - versioned Rust/Python contract for normalized market data transport
  - currently a validated schema/type boundary; DataLoader does not yet emit
    this bundle in the production runtime

The six supported strategy profiles are compiled into `EngineRequest.v1` and
validated against the same generated fixtures in Python and Rust. The current
Python runner still selects shape-specific Rust producer methods internally,
so this contract cutover does not yet mean that the one-service Rust engine or
the `MarketDataBundle.v1` runtime wiring is complete.
