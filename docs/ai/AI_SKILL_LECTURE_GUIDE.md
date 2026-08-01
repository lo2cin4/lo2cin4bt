# lo2cin4bt AI Skill + Lecture Guide

> Canonical repo-local Codex skill: `skills/lo2cin4bt/SKILL.md`.
> Use this guide to keep teaching content aligned with the current runtime.

## Teaching Position

Lecture content should teach lo2cin4bt as a local research application with a
browser-first workflow:

```text
strategy_run / wfa_run config
-> data requirements: provider + symbols + external_features
-> Python control plane validates config and loads market data
-> EngineRequest + MarketDataBundle
-> persistent Rust strategy ops, fills, accounting and risk
-> CanonicalResultBundle
-> mandatory Rust result validator
-> Rust metrics + PlotBundle
-> outputs/app -> FastAPI 2424 -> React pages
```

Teach only the current config expression and current compute model.

## Module Guidance

- Module 00/01: install, repo map, no-live-trading boundary, and host tools
  model. The repo does not bundle Python, Node.js, Rust, `.venv/`,
  `node_modules/`, Cargo caches, or Rust `target/`.
- Module 02: data providers and symbol conventions. Do not mix Binance and
  Coinbase symbol formats without an adapter. Teach that configs describe data
  requirements: provider, symbols, date range, typed `data.bar_time`, stream
  bindings, and optional `data.external_features[]`. `yfinance` is daily-only;
  intraday requests must match the exact provider capability. Missing,
  duplicated, or out-of-order rows stop the run with structured failure
  evidence. Do not teach users to prebuild separate
  open/high/low/close/feature CSV files for one strategy.
- Module 03: current configs are `strategy_run` and `wfa_run`. New timing uses
  `fill_model.timing = "timeline"` and explicit `actions[]`. Teach typed
  `data.bar_time`, execution versus decision streams, direct daily input, and
  shared-Rust higher-timeframe derivation without look-ahead.
- Module 04: Run Center creates app-managed results. Result pages do not show
  configs until a run has completed. Intraday charts retain intraday equity and
  trade timestamps; headline annualized metrics use validated session closes,
  with `intraday_max_drawdown` shown separately.
- Module 05: strategy semantics converge into signals, selections, target
  weights, and timeline actions. Baseline portfolio positions can use
  `calendar.first_session`; re-entry timer extension uses
  `fill_model.position_policy.on_entry_signal_while_holding = "reset_timer"`.
- Module 06: Parameter Matrix expands only declared `parameter_domains` backed
  by schema/op support and `param_ref`; arbitrary config fields are not
  automatically sweepable. Large jobs use bounded batches. Every candidate
  keeps summary/ranking evidence, while only retained top candidates require
  full equity/trade/plot artifacts.
- Module 07: metricstracker uses Rust/Polars parquet reads plus Rust metric
  math. Explain `metricstracker.time_unit` and `metricstracker.risk_free_rate`.
- Module 08: WFA runs selected strategy routes per window; it is not investment
  proof and should be read with regime and overfitting caution. The validation
  method is unchanged; current behavior adds bounded large-job execution and
  exact objective-to-artifact selection.
- Module 09: safety checks include look-ahead guards, cost/slippage, benchmark
  alignment, invariant checks, and the shared flatten-first risk contract.
  Supported post-trigger routes are `permanent_stop` and
  `shadow_until_recovery`; do not teach generic pause exceptions.
- Module 10: provider extension must preserve the standard market-data bundle.
- Module 11: factor/stats diagnostics are optional overlays. Any factor logic
  which changes positions still executes through the shared Rust engine.
- Labs and app UI teaching should cite the actual page payload or artifact path
  rather than screenshots alone.
- Canonical candidate identity is
  `base_strategy_id:workflow_id:parameter_suffix` (`fixed` without parameter
  values). Candidate, objective, retained artifact, WFA window, and dashboard
  payload must match exactly; missing evidence is a contract error, not a
  fallback selection.
- Python setup and execution use only `uv sync --locked` and
  `uv run --locked --exact`.

## Data Requirements Boundary

When teaching or generating configs, keep this boundary:

- Public config says what data is needed.
- `dataloader` downloads provider OHLCV and joins local external features.
- Internal frames such as `open`, `high`, `low`, `close`, and custom feature
  matrices are runtime implementation details.
- Result bundles and app payloads are output artifacts, not files the user must
  prepare as input.

For local market-breadth examples, use a file under `workspace/datasets/` and
declare its real time and value columns. Put thresholds in the signal rule
rather than generating an intermediate strategy-specific input file.

## Current Terms To Use

- `strategy_run`
- `wfa_run`
- `fill_model.timing = "timeline"`
- `fill_model.actions[]`
- timeline action
- target weights
- persistent Rust compute service
- CanonicalResultBundle
- mandatory Rust result validator
- PlotBundle
- Rust/Polars metricstracker
- artifact manifest
- chart payload
- app registry


## Example Policy

Examples should be taken from the actual bundled config files or generated
workspace configs at the time of teaching. Do not hard-code old Binance,
Coinbase, QQQ, or BTC example names without inspecting the current file.

Any JSON shown as runnable must validate against the current schema. Prefer
copying a minimal section from `backtester/contracts/strategy/examples/` and
run `tests/test_lecture_contracts.py` after updating Lecture content.

## Visual Contract

- Keep one CSS token system; do not append a second visual theme or page-only
  overflow exception. The Lecture is zero-build static HTML, so do not add a
  second Tailwind/Shadcn frontend toolchain for documentation-only components.
- Shared navigation must contain all 19 pages and exactly one active page.
- Desktop and 390px mobile renders must have no document-level horizontal
  overflow. Wide tables may scroll only inside `.table-scroll`.
- Long reference dictionaries use progressive disclosure; core conclusions
  remain visible before the details.
- Mermaid diagrams use the shared accessible viewer: click or press Enter/Space
  to open a modal, then zoom, reset, or open the SVG in a standalone window.
- Every page shows lesson type, reading time, progress ring, nearby chapter
  stepper, and current-contract status through the shared Lecture runtime.
- The course home starts with a learning map, prerequisites, learning outcomes,
  and a beginner glossary covering `strategy_run config`, Run Center,
  Backtests versus Metrics Overview, and WFA.
- Hands-on Labs visually separate manual operation and Agent/Skill operation.
  Every major step states the expected result and common failure mode.
- Paths, commands, URLs, and `run_id` references use the shared one-click copy
  control. Lab completion criteria use the persistent shared checklist
  component.

When the workspace is empty, explain that this is normal for a clean public
clone. Ask the agent to initialize supported examples into ignored workspace
folders if the user wants runnable default configs.

## Teaching Safety

Every lecture or AI explanation should include the research boundary when the
topic touches strategy performance, parameter selection, WFA, live data, broker
accounts, or future trading decisions. lo2cin4bt does not provide investment
advice or live-trading instructions.
