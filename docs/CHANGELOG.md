# Changelog

All notable project changes should be recorded here.

Version policy follows `MAJOR.MINOR.PATCH`:

- PATCH: bug fixes, typo fixes, docs corrections, test-only fixes.
- MINOR: meaningful user-facing feature changes.
- MAJOR: large compatibility-breaking changes.

## 2.1.0 - Unified Rust Research Runtime

Release date: 2026-07-22.

### 繁體中文

本版本完成 2.0 beta 開始的 Rust 架構升級，並把重點放在回測可信度、執行速度及使用體驗。以下內容以使用者可感受到的改變為主。

#### 主要升級

- **所有受支援策略改用同一個 Rust 回測核心。** 單次回測、參數矩陣（Parameter Matrix）、前向分析（WFA）及滾動驗證不再各走不同程式路線，結果更一致，新策略亦更容易加入平台。
- **增加可重用的策略積木及技術指標。** 使用者可以組合選股、擇時、資產配置、日曆事件、配對／價差、多腿事件、多空輪動，以及 EMA、RSI、MACD、布林通道、波動率和標準分數等條件。
- **增加月度多空輪動能力。** 平台可以按排名同時買入強勢資產及賣空弱勢資產，控制總曝險與淨曝險，並把借貨成本計入回測。
- **增加八個公開內建策略範例。** 新使用者可以直接試用 QQQ 均線穿越、BTC 日曆效應、月度避險、配對價差、選股擇時、板塊多空輪動、動能輪動及固定配置，不必由空白設定開始。
- **增加一鍵截圖匯出。** 使用者可以一次保存資金曲線、策略表、參數矩陣、回測摘要、風險診斷、再平衡、資產貢獻及配置變化，方便研究記錄與分享。
- **增加中文先行的互動課程及專案 AI 技能（Skill）。** 新使用者可以跟隨學習路線理解回測、指標、前向分析和執行中心，也可以讓 AI 協助建立設定及解讀結果。

#### 體驗改善

- **前端統一由 `http://127.0.0.1:2424/` 提供。** 使用者不再需要判斷應該開啟哪一個開發伺服器或連接埠，所有正式頁面都使用同一條啟動路線。
- **大型參數矩陣的工作排程已改良。** 大型工作不再長時間佔用全部容量，其他較小回測可以獲得執行機會，而完整候選結果仍會按設定保留。
- **策略名稱及摘要改為從標準設定產生。** 回測清單會顯示日期、資產、策略概念、工作流程及簡短執行識別碼，使用者更容易分辨不同結果。
- **回測輸出及刪除流程已統一。** 刪除一個回測時，平台會一併清除相關狀態、圖表、截圖及審閱產物，避免舊資料繼續出現在前端。
- **交易時間及成本假設已標準化。** 收市後確認的訊號會在下一個合資格時段執行，公開範例亦統一使用 `0.1%` 交易費率，減少前視偏差及過度樂觀的結果。

#### 問題修正

- **修正多項績效數據顯示 `n/a` 或錯誤數值的問題。** 年化回報、夏普比率、最大回撤、卡瑪比率、換手率、交易、再平衡、資產貢獻、配置及風險診斷現在都讀取同一份已驗證結果。
- **修正基準比較及交易標記。** 基準按鈕和基準回報恢復運作，入場／出場標記會直接顯示在資金曲線，而且預設關閉，不再產生一張重複圖表。
- **修正策略比較的預設顯示。** 比較頁預設顯示首三名策略，資金曲線預設只顯示一個策略，避免大量線條令圖表難以閱讀。
- **修正參數矩陣、熱圖及前向分析頁面的空白與排版問題。** 候選資料、熱圖及 WFA 結果可以正常顯示，長名稱、卡片和表格亦不再超出畫面。
- **修正前向分析重複計算相同樣本外（OOS）區間。** 相同測試只會執行一次並由相關候選共用，保留原有結果內容之餘縮短等候時間。
- **修正長頁面截圖出現半透明白帶。** 匯出的指標及配置截圖現在會保持一致背景，不再在分段位置出現白霧。

#### 架構整理與驗收

- **移除已由 Rust 取代的 Python 回測器、舊設定映射及策略專屬執行路線。** 平台不再同時維護兩套會產生不同結果的回測邏輯，舊設定及舊結果需要按 2.1 格式重新執行。
- **所有結果增加強制驗證關卡。** 未通過資料、帳務及結果完整性檢查的回測不會交給績效頁或前端，降低錯誤結果被當成有效研究證據的風險。
- **更新黃金測試（golden test）及完整驗收套件。** 測試現已覆蓋前視偏差、固定排序、帳務、借貨成本、介面合約、前端頁面及乾淨環境回測，讓後續改動更容易發現結果退化。
- **公開版本移除私人策略、回測結果、生成輸出及內部維護資料。** GitHub 使用者只會取得產品所需的程式、範例和文件，不會包含擁有者的私人研究內容。
- **本版本仍定位為本機研究軟體。** 平台不會向券商提交訂單，也不會把任何回測結果視為實盤交易授權。

### English

This release completes the Rust architecture upgrade started in the 2.0 beta, with a focus on backtest reliability, execution speed, and usability. The notes below describe changes users can see and use.

#### Major Upgrades

- **All supported strategies now use one shared Rust backtest engine.** Single backtests, Parameter Matrix, WFA, and rolling validation no longer follow separate execution paths, producing more consistent results and making new strategies easier to add.
- **Reusable strategy building blocks and technical indicators were added.** Users can combine selection, timing, allocation, calendar events, pair/spread, multi-leg, and long/short rotation logic with EMA, RSI, MACD, Bollinger Bands, volatility, z-score, and other fields.
- **Monthly long/short rotation is now supported.** The platform can buy stronger assets and short weaker assets from the same ranking, control gross and net exposure, and include borrow costs in the backtest.
- **Eight public built-in strategy examples were added.** New users can start with QQQ moving-average cross, BTC calendar effects, monthly hedging, pair spread, selection/timing, sector long/short rotation, momentum rotation, and fixed allocation instead of writing a config from scratch.
- **One-click screenshot export was added.** Users can save equity curves, strategy lists, parameter matrices, summaries, risk diagnostics, rebalances, asset contributions, and allocation changes in one action.
- **A Chinese-first interactive Lecture and project AI Skills were added.** New users can follow a guided path through backtests, metrics, WFA, and Run Center, or ask AI to help create configs and interpret results.

#### Experience Improvements

- **The frontend now has one supported address at `http://127.0.0.1:2424/`.** Users no longer need to choose between development servers or ports because every production page follows the same startup route.
- **Scheduling for large Parameter Matrix jobs was improved.** Large jobs no longer occupy all capacity for long periods, allowing smaller backtests to progress while full candidate results remain available when requested.
- **Strategy names and summaries now come from canonical config metadata.** Result lists show the date, assets, strategy concept, workflow, and short run identity, making different runs easier to recognize.
- **Backtest output storage and deletion were unified.** Deleting a run now removes its status, charts, screenshots, and review artifacts together, preventing stale data from reappearing in the frontend.
- **Trade timing and cost assumptions were standardized.** Signals confirmed at the close execute on the next eligible session, and public examples use a `0.1%` transaction fee to reduce look-ahead bias and overly optimistic results.

#### Bug Fixes

- **Missing or incorrect performance values were fixed.** Annualized return, Sharpe, drawdown, Calmar, turnover, trades, rebalances, contributions, allocations, and risk diagnostics now read the same validated result.
- **Benchmark comparison and trade markers were fixed.** Benchmark controls and returns work again, while optional entry and exit markers appear on the equity chart and remain off by default instead of creating a duplicate chart.
- **Default strategy comparison views were fixed.** The comparison page shows the top three strategies by default and the equity chart starts with one strategy, reducing visual clutter.
- **Blank and overflowing Parameter Matrix, heatmap, and WFA panels were fixed.** Candidate data, heatmaps, and WFA results now render correctly, while long names, cards, and tables stay inside the layout.
- **Repeated WFA work for identical out-of-sample windows was fixed.** Matching candidates now share one OOS calculation, preserving the same outputs while reducing waiting time.
- **Translucent white bands in long-page screenshots were fixed.** Exported metric and allocation images now keep a consistent background across capture boundaries.

#### Architecture Cleanup And Validation

- **Python backtest engines, legacy config mappings, and strategy-specific execution paths superseded by Rust were removed.** The platform no longer maintains two result-producing implementations, and old configs or outputs must be rerun using the 2.1 format.
- **A mandatory result-validation gate was added.** Backtests that fail data, accounting, or completeness checks cannot reach metrics or the frontend, reducing the risk of treating invalid output as research evidence.
- **Golden tests and the full acceptance suite were expanded.** Coverage now includes look-ahead prevention, deterministic ordering, accounting, borrow costs, interface contracts, frontend routes, and clean-environment backtests, helping future changes catch result regressions.
- **Private strategies, backtest outputs, generated artifacts, and internal maintenance data were removed from the public release.** GitHub users receive only the product code, examples, and documentation, without the owner's private research material.
- **This release remains local research software.** It does not submit broker orders or treat any backtest result as authorization for live trading.

## 2.0.0 - Public Baseline

Release type: initial public GitHub baseline for the 2.x line.

### Added

- Browser-first FastAPI + React app at `http://127.0.0.1:2424/`.
- One persistent Rust engine service for every supported strategy profile and
  workflow.
- Mandatory Rust result validation before metrics, PlotBundle, API, or frontend
  consumption.
- Canonical EngineRequest, MarketDataBundle, CanonicalResultBundle, Rust
  metrics, and PlotBundle contracts.
- Run Center for local backtest and WFA batch execution.
- Metrics Overview page for ranking and filtering strategy rows.
- Backtests page with equity, benchmark, drawdown, trade/event rows, holdings,
  allocation changes, asset contribution, turnover, costs, and risk diagnostics.
- Parameter Matrix for structured `parameter_domains`.
- WFA dashboard with selected optimum rows and OOS evidence separation.
- Rolling validation path for fixed strategies.
- AI-readable review packs under `outputs/app/ai_review/{run_id}/`.
- Repo-local Codex skill under `skills/lo2cin4bt/`.
- Beginner-first Lecture learning map, terminology guide, manual and Agent
  routes, expected-result troubleshooting, persistent checklists, copy actions,
  progress navigation, and zoomable Mermaid diagrams.
- Release guard tests for skill docs, frontend field coverage, stale public docs,
  workspace boundaries, and broker safety wording.
- Release CI gates for Python regression, Rust tests and release binaries,
  Ruff, mypy, consistency audit, doctor, deterministic contract generation,
  production frontend build, and port 2424 Playwright routes.
- Bundled Shippori Mincho frontend font files for local runtime consistency.

### Changed

- Replaced older UI/runtime flow with the app-managed `outputs/app/` contract.
- Moved result-changing calculations to the shared Rust engine; Python remains
  the control plane for orchestration, transport, provider access, and artifact
  management.
- Moved active public config style to `strategy_run`.
- Standardized strategy labels for strategy tables.
- Clarified benchmark labels such as same-symbol buy-and-hold vs explicit SPY.
- Improved frontend load performance through payload compression/cache and
  route-level UI code splitting.
- Updated workspace policy: user configs and datasets are local or distributed
  outside GitHub by default.

### Removed From Public Release

- Public legacy vector/sequential plotter runtime surfaces.
- Tracked local workspace strategy/feature JSON files.
- Runtime output snapshots and generated chart payloads.
- Old root `assets/` CSS files that were no longer part of the active frontend.

### Safety Notes

- lo2cin4bt remains local research tooling.
- No result authorizes live trading.
- FUTU / IBKR support is optional market-data gateway work only; no order
  placement workflow is part of this release.
- Old artifacts that lack current fields should be rerun, not mixed into final
  validation evidence.

See also:

- `docs/QUALITY_GATES.md`
