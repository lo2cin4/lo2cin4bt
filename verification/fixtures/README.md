# Verification Fixtures

This directory contains small fixtures used by the lo2cin4bt verification
scaffold. They are public engineering fixtures, not trading evidence.

## Fixture Groups

- `dataloader/`: compact source slices for loader behavior checks.
- `statanalyser/`: compact statistical-analysis input slices.
- `backtester/`: deterministic price/strategy fixtures for backtester truth
  and contract checks.
- `wfa/`: compact inputs for walk-forward and rolling-validation mechanics.
- `manifests/`: case manifests that define which checks run in each round.
- `source_map.json`: provenance notes for every public fixture.

## Evidence Boundary

- `*_slice.*` files are source slices for import and schema behavior checks.
- `mini_*.*` files are tiny deterministic truth fixtures.
- `*_expected.json` files are expected-output or spot-check definitions.
- Synthetic and minified fixtures prove mechanics, parity, regression behavior,
  and schema compatibility only.
- These fixtures do not prove market edge, strategy profitability, external data
  correctness, broker-grade execution, or survivorship-free universe coverage.

Use larger real-data research artifacts outside this public fixture tree when
the question is strategy validity rather than software correctness.
