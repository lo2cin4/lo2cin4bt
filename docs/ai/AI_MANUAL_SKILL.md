# lo2cin4bt AI Manual Skill

> Canonical repo-local Codex skill: `skills/lo2cin4bt/SKILL.md`.
> Use that skill first, then load only the required files under
> `skills/lo2cin4bt/references/`.

## Current Public Contracts

User-facing runnable configs use:

- `strategy_run` for backtests, parameter matrices, rolling validation, and
  strategy examples.
- `wfa_run` for walk-forward analysis configs that reference an explicit
  `strategy_run_path`.

Teach only these current public schema names. Do not introduce version-suffixed
schema names in beginner or public-facing material.

## Strategy Building Blocks

For strategy creation, load
`skills/lo2cin4bt/references/strategy-authoring-template.md` before
writing a runnable config. The AI must first decide whether current Strategy
Building Blocks cover the idea. If they do not, report
`unsupported_needs_new_building_block` and stop before producing a runnable
config.

When the strategy is supported, the AI must apply
`skills/lo2cin4bt/references/strategy-identity-and-summary.md`. It writes the
short human strategy concept into `platform.display_label`, encodes all actual
behavior in structured executable fields, and previews both the result selector
and Strategy Logic panel from the config before calling it runnable. Completed
run pages deterministically read the saved strategy snapshot; they do not ask
AI to reinterpret the user's original sentence.

## Current Execution Model

New strategy timing should be explained as:

```text
Strategy Config DSL
-> schema + Strategy Building Block support validation
-> normalized Machine IR and EngineRequest
-> persistent shared Rust engine
-> CanonicalResultBundle
-> mandatory Rust result validator
-> Rust metricstracker and PlotBundle
-> app-managed payloads
-> frontend result pages on port 2424
```

The current mental model is simple:

1. The strategy decides what it wants to hold.
2. Timeline actions decide when and at what price the trade happens.
3. Every supported profile compiles into the same EngineRequest and enters the
   persistent shared Rust engine; profiles describe intent, not separate runtime
   families.
4. The Rust engine writes one CanonicalResultBundle containing the applicable
   equity, trades, holdings, rebalance, risk, and audit evidence.
5. The mandatory Rust validator checks the bundle before Rust metricstracker and
   PlotBundle generation.
6. Python remains the orchestration, config/schema validation, data-loading, and
   app-service layer. It is not a fallback strategy or metrics calculator.

Teach the boundary explicitly:

- Supported production integration is subprocess/CLI based through
  `backtester/RustCoreBridge_backtester.py`.
- Do not describe the current runtime as a PyO3 or maturin extension wheel.

## Shared Rust Compute Boundary

Teach the compute boundary this way:

- Single-asset, selection/timing, allocation, rotation, calendar-event, pair,
  and multi-leg profiles are authoring shapes for one shared Rust runtime.
- WFA explicitly reruns the referenced strategy config over isolated train/test
  windows; it does not introduce another strategy engine.
- Metricstracker core math and result validation are Rust-backed. Python must
  not be described as a canonical strategy or performance fallback.
- Public teaching describes one validated route from EngineRequest to
  CanonicalResultBundle, metrics, PlotBundle, and app payloads.

Use `validation_report`, artifact manifests, and generated payloads as evidence
for the route a run actually used. Do not infer the route from a screenshot.


## Metric Assumptions

Strategy configs may set:

- `metricstracker.time_unit`
- `metricstracker.risk_free_rate`

Traditional daily assets default to 252 annualization days. Crypto defaults to
365. The default annual risk-free rate is 0.04 unless the config overrides it.
Sharpe, Sortino, CAGR, Calmar, and annualized volatility should be explained
under those assumptions.

## Result Page Semantics

`Metrics Overview`, `Backtests`, `Parameter Matrix`, and `WFA` are result
pages. They show completed app-managed runs from `outputs/app/`, not every config
that exists in `workspace/runs/` or `workspace/wfa/`.

If Run Center can see configs but result pages are empty, the correct first
explanation is: no current completed run result has been generated yet.

## Fresh Clone Workspace

The public repository does not track local runnable workspace configs or
outputs. A fresh clone may have empty `workspace/runs/` and `workspace/wfa/`.
That is normal. If the user wants the built-in examples, the AI agent should
create or copy supported example configs from bundled contracts into the ignored
workspace folders, then run them locally.

## Safety Boundary

lo2cin4bt is for local research, education, and software operation only.
Agents must not place trades, move funds, enable live trading, change broker or
exchange accounts, or present results as investment advice.

## Evidence Rules

- Inspect actual config JSON, normalized snapshots, artifacts, tests, and
  payload JSON before making claims.
- Treat older artifacts that lack current validation fields as stale.
- Open fills require explicit open market data. Never say the engine silently
  uses close prices as open prices.
- Unsupported strategy logic must be reported as
  `unsupported_needs_new_building_block` until code, tests, and documentation
  exist.
