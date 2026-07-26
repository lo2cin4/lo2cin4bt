# 🚀 lo2cin4bt

[繁體中文](README.md)

![coverage gate](https://img.shields.io/badge/coverage_gate-50%25_min-blue)

![lo2cin4bt neon platform preview](assets/readme/lo2cin4btneon.jpg)

> You are the PM for lo2cin4bt. Check the local environment, initialize the built-in strategy examples, and run a QQQ daily SMA Cross backtest. Keep everything local; do not run live trading or place orders.

## 🧭 What Is lo2cin4bt

lo2cin4bt is a quantitative strategy backtesting framework built by lo2cin4 using AI. Describe the strategy you want to AI, and you can create a local backtest and inspect its results in the browser app without writing any code.

Note: lo2cin4bt does not provide investment advice.

## ✨ Why Choose lo2cin4bt

- **Open source**: users can inspect how the framework handles data, signals, and backtest results.
- **Runs locally**: data and strategy research stay on your own machine.
- **Beginner-friendly workflow**: describe the idea to AI, let it create workspace files, then review the result in the browser.
- **Backtesting and visualization together**: single backtests, parameter matrices, WFA, and result pages are part of one local workflow.
- **One shared Rust execution route**: one Rust engine vectorizes indicator, signal, and target-weight precomputation, then performs fills, holdings, costs, risk, and equity accounting in time order. These are internal stages of one engine, not separate backtest routes. Metricstracker computes through Rust/Polars.
- **Flexible data and assets**: local files and market-data sources can be used when their format and availability are clearly defined.
- **AI with clear boundaries**: AI creates docs and configs that match user requirements and code constraints; it does not invent engine behavior.
- **Traceable results**: each result should trace back to its config, data source, cost, slippage, benchmark, and generated artifacts.
- **Configurable performance assumptions**: strategy configs can set annualization days and risk-free rate, so Sharpe, CAGR, and related metrics do not rely only on fixed defaults.
- **Practical safety checks**: workspace checks, config validation, fixed-example regression tests, frontend/backend display checks, and quant review help catch common mistakes.

## ⚡ Three-Step Quick Start

1. On GitHub, click `<> Code`, choose `Download ZIP`, then unzip the folder.
2. If you use a CLI assistant, type `cd <your downloaded lo2cin4bt directory>` in PowerShell, then start your tool, for example `opencode`, `claude`, `aider`, or `codex`.
3. Ask your AI agent to read the whole folder, then copy this prompt to AI:

```text
You are the PM for lo2cin4bt. Read AGENTS.md, README.en.md, agents/lo2cin4bt_PM.agent.md, and the required skills/docs first.
Use only the AGENTS.md, README.en.md, agents/lo2cin4bt_PM.agent.md, skills/, and docs/ inside the current lo2cin4bt project folder as authority. Do not rely on parent folders or other agent settings from my local machine.
Check my Python, Node.js, Rust, frontend build, and workspace status. If required components are missing, list what is missing, the recommended install method, and which local paths would change, then wait for my confirmation before installing. If Run Center has no strategies yet, initialize the currently supported built-in examples into workspace/runs/.
Tell me which agents and skills are available, what each one does, and what I can ask you to do as a beginner. Keep everything local for research, backtesting, and learning only. Do not run live trading, place orders, or ask for broker passwords.
```

## ✅ What Beginners Should Be Able To Do

- Start lo2cin4bt successfully.
- Open the browser-based backtest platform.
- Find and run the built-in strategy examples.
- Try all 8 public built-in backtest examples.
- Review results, charts, metrics, holdings, and trade records.
- Open the HTML lecture or related tutorial docs.
- Let `lo2cin4btWorkAgent` use the `lo2cin4bt-teaching` skill to teach how the platform works.
- Let `lo2cin4btWorkAgent` use the `lo2cin4bt-strategy-builder` skill to try building a strategy.
- Let `lo2cin4bt_PM` assign the same `lo2cin4btWorkAgent` the required skills, and request the independent risk reviewer when bias or result validity needs review.

## 🛡️ What Beginners Should Not Need Or Encounter

- You should not need to edit core code outside `workspace/` for normal use.
- AI agents should not create a strategy with obvious look-ahead bias without warning you.
- Unsupported strategy logic should not be disguised as a runnable config.
- The software should not guide you into real orders, live trading, fund movement, or broker account setting changes.
- You should not need to submit API keys, broker passwords, private data, or other sensitive information.

## 📁 Beginner-Safe Workspace

When researching strategies, treat `workspace/` as the safe working area. Local input data, runnable strategy configs, WFA configs, custom indicators, and AI notes should start there.

- Data files: `workspace/datasets/`
- Runnable backtest configs: `workspace/runs/`
- WFA configs: `workspace/wfa/`
- External data contracts: `workspace/features/`
- Custom indicators: `workspace/indicators/extensions/`
- AI notes or review records: `workspace/reports/agents/`

For normal strategy research, AI should create or edit files inside `workspace/` only. It should not need to change `app/`, `backtester/`, `dataloader/`, `autorunner/`, `validation_workflow/`, `metricstracker/`, or `plotter/`.

If a strategy uses external data, such as IPO dates, earnings releases, index membership, sentiment data, or your own CSV files, AI must state when that data would have been known in real life. This prevents the backtest from accidentally looking into the future. For example, data published after the market close cannot be used for a same-day market-open trade. These data contracts usually live in `workspace/features/` and should pass the workspace checks. Data marked as revised history is research/demo data or requires further review; it is not proof of point-in-time, bias-free availability.

## 🔄 Local Backtest Flow

1. Copy this prompt to AI:

```text
You are lo2cin4bt/agents/lo2cin4bt_PM.agent.md. Read agents/lo2cin4bt_PM.agent.md first, then load the required skills and docs.
Check the environment first. If workspace/runs has no built-in strategies yet, initialize the currently supported examples from backtester/contracts/strategy/examples/.
Create or select a QQQ daily dual-moving-average cross strategy config with beginner-safe defaults. Run local backtests only; do not trade live.
Launch the local app and open or reuse one http://127.0.0.1:2424/ browser tab. In Run Center, select the QQQ daily SMA Cross config, run the backtest, then open Metrics Overview and briefly report whether it succeeded.
```

2. Wait for AI to finish the run and open the visualization.

## 🧰 Install

Prepare Python, Node.js, and the repo-pinned Rust 1.96.0 toolchain first; see [`docs/RUST_TOOLCHAIN.md`](docs/RUST_TOOLCHAIN.md) for Rust install and compatibility notes.

Windows:

```powershell
git clone <repository-url> lo2cin4bt
cd lo2cin4bt
.\scripts\setup.ps1
.\.venv\Scripts\python.exe main.py
```

macOS / Linux:

```bash
git clone <repository-url> lo2cin4bt
cd lo2cin4bt
bash scripts/setup.sh
.venv/bin/python main.py
```

Open:

```text
http://127.0.0.1:2424/
```

Update an existing folder:

```powershell
git pull
.\scripts\setup.ps1
```

You can also create a lo2cin4bt desktop shortcut:

```powershell
.\scripts\create_windows_shortcut.ps1
```

After that, double-click the `lo2cin4bt` desktop shortcut to start the local backtesting app. If you move the project folder later, run the shortcut command again.

Reserve at least 1.5 GB of local disk space when installing the Python and frontend dependencies. Actual usage varies by operating system and package version.

See [`docs/INSTALL.md`](docs/INSTALL.md) for detailed setup and [`Troubleshooting.md`](Troubleshooting.md) for common issues.

## ⚙️ Python And Rust Boundary

The production path does not run two separate backtests in Python and Rust.
Each language owns a different part of the workflow:

- **Python, the platform control plane**: receives AI-generated strategy
  configs, checks their format and platform support, loads market data from a
  provider or local file, schedules Run Center jobs, and maintains persistent
  service transport with the Rust engine. Python also handles artifact,
  manifest, registry, and payload-index I/O and frontend orchestration, but it
  does not own the canonical fills or equity curve.
- **Rust, the backtest compute core**: calculates supported indicators and
  computed fields, produces signals, calendar triggers, rankings, and target
  weights, then processes fills, positions, cash, trading costs, risk actions,
  and equity accounting in time order. Rust also owns canonical result
  validation, performance metrics, and `PlotBundle` projection.
- **Parameter Matrix and WFA**: Python expands parameter candidates, divides
  WFA windows, and schedules work. Every candidate still enters the same Rust
  core. Unsupported strategy shapes fail with a clear error instead of falling
  back to a Python backtest.
- **Integration**: `backtester/RustCoreBridge_backtester.py` manages the
  repo-pinned persistent Rust engine service, `engine_service_cli`, and
  exchanges JSON- and Parquet-backed data contracts with it.

For the user, the complete flow is: describe a strategy to AI → Python checks
the config, loads data, and schedules the job → Rust runs and validates the
backtest and calculates its metrics → Python stores the result and presents it
in the browser.

The supported production path does not require a PyO3 or maturin extension
wheel.

## 📈 QQQ Daily SMA Cross Demo

After downloading the project, if Run Center has no strategies yet, ask your AI agent
to inspect `backtester/contracts/strategy/examples/` and initialize the current supported examples into `workspace/runs/`.

This example uses daily QQQ data. It enters when the short moving average crosses above the long moving average and exits when it crosses below. Beginner-safe assumptions include:

- Short MA from `20` to `100`
- Long MA from `120` to `300`
- Workflow: Parameter Matrix
- Cost and slippage explicitly declared in `fill_model`
- No live trading

## ⚖️ Fixed Allocation Demo

```text
backtester/contracts/strategy/examples/strategy-run-vti-avuv-vxus-sgol-dbmf-yfinance-yearly-rebalance-example.json
```

Windows:

```powershell
New-Item -ItemType Directory -Force workspace\runs
Copy-Item backtester\contracts\strategy\examples\strategy-run-vti-avuv-vxus-sgol-dbmf-yfinance-yearly-rebalance-example.json workspace\runs\strategy-run-vti-avuv-vxus-sgol-dbmf-yfinance-yearly-rebalance-example.json
```

macOS / Linux:

```bash
mkdir -p workspace/runs
cp backtester/contracts/strategy/examples/strategy-run-vti-avuv-vxus-sgol-dbmf-yfinance-yearly-rebalance-example.json workspace/runs/strategy-run-vti-avuv-vxus-sgol-dbmf-yfinance-yearly-rebalance-example.json
```

## 🖥️ Platform Screenshots And Walkthrough

### 🏠 Overview

![lo2cin4bt overview](assets/readme/en/01-overview.png)

### ▶️ Run Center

![lo2cin4bt run center](assets/readme/en/02-run-center-first-run.png)

Full English walkthrough: <https://youtu.be/03CduKFc4sg?si=GE7Y2EFKnsiF3HFV>

## 🧩 Supported Strategies and Research Workflows

The public version includes eight backtest examples that can be initialized locally. They are not eight separate engine routes. Each example combines reusable strategy building blocks and runs through the same Rust core.

| Public example | Strategy capability demonstrated |
| --- | --- |
| QQQ Daily SMA Cross | Single-asset signals and timing |
| BTC Monthly Nth Weekday Event | Calendar and trading-session events |
| QQQ, TLT, GLD Monthly Hedge Overlay | Multi-leg events and hedge allocation |
| SPY, QQQ Monthly Pair Spread | Pair and relative-value trading |
| VOO, QQQ, IWM, GLD Selection Timing | Multi-asset filtering, ranking, and top-N selection |
| US Sector ETF Monthly 12-1 Rotation | Cross-sectional long-short ranking and momentum rotation |
| VOO, GLD Momentum and SMA Filter | Multi-asset rotation and regime filtering |
| VTI, AVUV, VXUS, SGOL, DBMF Annual Allocation | Fixed weights and scheduled rebalancing |

Parameter Matrix, Walk-Forward Analysis (WFA), and rolling validation are research workflows that can be applied to strategies; they are not separate strategy families. Custom computed fields and indicator extensions add new strategy capabilities.

### 🧱 Reusable Strategy Building Blocks

AI translates a strategy idea into data sources, computed fields, signals, asset filters and rankings, allocation, rebalancing, fill modeling, trading costs, risk controls, and parameter domains within one strategy config. The Rust core currently provides 30 reusable computed-field operations:

| Building-block group | Example capabilities |
| --- | --- |
| Indicators | SMA, EMA, momentum, calendar return, volatility, RSI, MACD, ATR, Bollinger Bands, rolling z-score, rolling percentile |
| Math | Add, subtract, multiply, divide, negate, absolute value, clip |
| Data transforms | Lag, fill missing values, conditional selection |
| Rolling windows | Minimum, maximum, sum, median, correlation |
| Cross-sectional comparison | Rank, percentile, z-score, winsorize |

See the [computed-field building-block reference](skills/lo2cin4bt/references/computed-field-building-blocks.md) for canonical operation names, parameters, and composition examples.

If a strategy needs behavior the engine does not support yet, AI should stop and explain the missing capability instead of using synthetic price series or filename inference to pretend support exists.

## 🗄️ Connected Data Sources

| Logo | Source | Data | Status | Entry / Notes |
| --- | --- | --- | --- | --- |
| <img src="assets/readme/logos/yfinance.svg" alt="Yahoo Finance" height="26"> | `yfinance` | ETF, stocks, beginner examples | Available | No account required for market data. |
| <img src="assets/readme/logos/binance.svg" alt="Binance" height="26"> | `binance` | Crypto spot klines / OHLCV, such as BTCUSDT | Available | No account is required for market data. |
| <img src="assets/readme/logos/coinbase.svg" alt="Coinbase" height="26"> | `coinbase` | Coinbase product format, such as `BTC-USD` | Available | No account required for market data. |
| <img src="assets/readme/logos/files.svg" alt="Local files" height="26"> | Local files | CSV, Parquet, research datasets | Available | Put private datasets under `workspace/datasets/`. |
| <img src="assets/readme/logos/futu-display.svg" alt="FUTU" height="26"> | `futu` | Advanced HK / US market data | Advanced | Market-data use only; follow the official documentation for read-only data setup. |
| <img src="assets/readme/logos/ibkr-icon.png" alt="IBKR" height="30"> | `ibkr` | Advanced stocks, ETFs, futures market data | Advanced | Official link: <https://www.interactivebrokers.com/> |

lo2cin4bt currently does not support order placement.

Broker or exchange accounts, when used, are for read-only market data only.

## 🛠️ Development Status

lo2cin4bt aims to keep strategy ideas inside a documented, checkable research workflow instead of letting AI create one-off scripts that cannot be reviewed. The current development focus is user-facing research capability:

- Combined multi-strategy performance views.
- Clearer teaching and display around per-strategy annualization days and risk-free-rate assumptions.
- Stronger parameter-matrix, WFA, and stress-test workflows.
- Clearer ways to share strategy configs and completed result bundles.
- Easier workspace flows for custom data, custom indicators, and custom strategies.

## 🎯 Future Goals

- Maintain Golden regression coverage across all eight public strategies, WFA, Rust metrics, and plot payloads.
- Raise core module coverage.
- Improve first-time installation and startup checks.
- Simplify custom indicator onboarding.
- Keep frontend display aligned with backend payload truth.
- Add more QuantReview-approved strategy building blocks.

## 📚 Docs

- [Tutorial](docs/TUTORIAL.md)
- [Install](docs/INSTALL.md)
- [Runtime Flow](docs/runtime-flow.md)
- [Backtest Testing](docs/BACKTEST_TESTING.md)
- [Quality Gates](docs/QUALITY_GATES.md)
- [Repository Structure](docs/REPOSITORY_STRUCTURE.md)
- [Strategy Building Blocks](backtester/contracts/ops/README.md)
- [Security Policy](SECURITY.md)
- [Contributing](docs/CONTRIBUTING.md)
- [Troubleshooting](Troubleshooting.md)

## 🤖 AI Docs

- [`skills/lo2cin4bt/SKILL.md`](skills/lo2cin4bt/SKILL.md)
- [`docs/ai/AI_MANUAL_SKILL.md`](docs/ai/AI_MANUAL_SKILL.md)
- [`docs/ai/AI_SKILL_LECTURE_GUIDE.md`](docs/ai/AI_SKILL_LECTURE_GUIDE.md)
- [`skills/lo2cin4bt/agents/openai.yaml`](skills/lo2cin4bt/agents/openai.yaml)

## 📄 License

This project uses the Creative Commons Attribution-NonCommercial 4.0 International (CC BY-NC 4.0) license and does not permit commercial use. See [`LICENSE`](LICENSE) for the full terms. Backtest results are research evidence only, not investment advice or performance promises.

## 💬 Contact / Business

For collaboration, teaching, research workflow design, or business inquiries, contact lo2cin4 through [Telegram](https://t.me/lo2cin4group) or [Discord](https://discord.gg/sSnZuq3DNu).
