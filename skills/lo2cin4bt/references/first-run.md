# First Run For Beginners

Use this when the user is new, has only Codex or an AI coding assistant, and wants to get lo2cin4bt running.

## What The User Needs

- Windows 10/11, macOS, or Linux.
- Python 3.12 or newer.
- Node.js 24 LTS recommended. Minimum supported frontend runtime is Node.js
  20.19.0 or 22.12.0.
- Rust 1.96.0 through rustup for the Rust backtester/metricstracker core.
- Git, or the ability to download a GitHub ZIP.
- No broker account is required for beginner local backtests. FUTU, IBKR, or exchange accounts may be used later for read-only market data only.

## Host Tools Model

The GitHub repo does not bundle Python, Node.js, Rust, `.venv/`,
`node_modules/`, Cargo registry caches, or Rust `target/` output. These are
machine-level tools or rebuildable local artifacts.

For Windows users, prefer host tool locations outside the repo:

```text
C:\dev-tools\python
C:\dev-tools\nodejs
C:\dev-tools\rust
```

AI assistants should run `python scripts/doctor.py` to detect what is already
available. If a tool is missing, install or point the user to a host location,
then rerun doctor. Do not copy runtimes or build caches into the repo to make a
local run work.

## Download

Preferred:

```bash
git clone <repository-url> lo2cin4bt
cd lo2cin4bt
```

ZIP fallback:

1. Download the repository ZIP from GitHub.
2. Extract it to a simple path such as `D:\lo2cin4bt` or `~/lo2cin4bt`.
3. Open Codex or the terminal with that folder as the working directory.

The public repo does not track local `workspace/runs/*.json` or `workspace/wfa/*.json` files. A clean clone may therefore show an empty Run Center until the user asks an AI agent to initialize supported example configs from bundled contracts into those ignored workspace folders. WFA configs in `workspace/wfa/` must point to strategy configs with explicit repo-relative paths like `workspace/runs/my-strategy.json`; do not write only the filename.

When initializing examples, inspect `backtester/contracts/strategy/examples/` first and copy the current supported examples. Do not hard-code old Binance or Coinbase filenames without checking the actual bundled files.

For other examples, use the current bundled examples, add configs from the owner/community channel, or ask Codex to create a supported `strategy_run` config using `indicator-recipes.md`.

## Windows Setup

```powershell
cd lo2cin4bt
.\scripts\setup.ps1
.\.venv\Scripts\python.exe main.py
```

Open:

```text
http://127.0.0.1:2424/
```

## macOS / Linux Setup

```bash
cd lo2cin4bt
bash scripts/setup.sh
.venv/bin/python main.py
```

Open:

```text
http://127.0.0.1:2424/
```

## Manual Setup

Use this when setup scripts fail or the user wants to see every step.

```bash
python -m venv .venv
# Windows: .\.venv\Scripts\Activate.ps1
# macOS/Linux: source .venv/bin/activate
python -m pip install --upgrade pip wheel setuptools
python -m pip install -r requirements.lock
cd plotter/web
npm ci
npm run build
cd ../..
python scripts/doctor.py
python main.py
```

## First Successful Run

1. Open Run Center.
2. Pick one Backtest config from `workspace/runs/`.
3. Click the run button for Backtests.
4. Wait until the batch completes.
5. Open Metrics Overview from the result.
6. Confirm at least one row appears in Strategy Table.
7. Open Backtests and inspect equity, drawdown, rebalance/trade rows, costs, and data health.

Good evidence:

- `GET http://127.0.0.1:2424/api/app/health` returns `{"status":"ok"}`.
- Run Center shows the completed run.
- Metrics Overview loads without page error.
- `outputs/app/run_snapshots/{run_id}/` exists locally.
- `outputs/app/ai_review/{run_id}/ai_review_pack.json` exists after payload export.

## First Prompt For Codex

```text
Use $lo2cin4bt. Read AGENTS.md, README.md, README.en.md,
skills/lo2cin4bt/SKILL.md, and skills/lo2cin4bt/references/first-run.md.
Help me run the simplest local backtest available in workspace/runs, then explain
the result using only repo files and generated artifacts. If workspace configs are
missing, create one supported beginner config first and tell me where you saved it.
```

## Common Beginner Choices

- Backtest first, WFA later.
- Use yfinance ETF examples before broker/gateway providers.
- Use one small config before a large Parameter Matrix.
- Keep external broker packages and FUTU/IBKR out of the first run unless the user is explicitly setting up read-only market data.
