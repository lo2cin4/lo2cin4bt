# Changelog

All notable project changes should be recorded here.

Version policy follows `MAJOR.MINOR.PATCH`:

- PATCH: bug fixes, typo fixes, docs corrections, test-only fixes.
- MINOR: meaningful user-facing feature changes.
- MAJOR: large compatibility-breaking changes.

## 2.2.1 - Adjusted Price Data Validation Fix

Release date: 2026-08-01.

### 繁體中文

- **經調整價格可以正常載入。** 使用 yfinance 的股息及拆股調整價格時，極微小的數值誤差不再被誤判為不合法，相關回測可以正常開始。
- **資料品質檢查仍然嚴格。** 真正不符合開高低收關係、缺失或無效的價格資料仍會令回測停止並顯示錯誤。

### English

- **Adjusted prices now load correctly.** Tiny numerical differences in yfinance dividend- and split-adjusted prices are no longer mistaken for invalid data, so affected backtests can start normally.
- **Data-quality checks remain strict.** Prices with genuinely invalid open-high-low-close relationships, missing values, or invalid values still stop the backtest with an error.

## 2.2.0 - Intraday And Multi-Timeframe Research

Release date: 2026-07-31.

### 繁體中文

本版本把 lo2cin4bt 由以日線研究為主，擴展成可以可靠處理分鐘、小時及其他周期的研究平台。資料時間、交易時段、成交先後、績效計算及畫面展示現採用同一套規則。

#### 主要升級

- **正式支援日內及多周期回測。** 策略可以使用 1 分鐘、5 分鐘、15 分鐘、1 小時、4 小時、6 小時、12 小時、日線及其他已聲明周期。日線來源毋須轉換；需要配合不同決策與成交周期時，系統會按設定處理，毋須建立另一套策略或回測流程。
- **高周期訊號可以在較低周期執行。** 例如策略可以等一小時資料完成後才確認訊號，再於下一個合資格的分鐘價格成交。訊號不會預先看到尚未完成的資料，減少回測結果因偷看未來而過度樂觀。
- **加入 Binance BTCUSDT 一分鐘雙均線範例。** 內建範例使用 10／20 期簡單移動平均線，並以完整一個月、43,200 個分鐘資金點作固定測試，供用戶檢查分鐘資料、成交、資金曲線及績效是否一致。
- **資料來源會先確認是否支援所選周期。** yfinance 只接受日線；Binance、Coinbase、FUTU 及 IBKR 會按各自已聲明的能力檢查請求。供應商不支援、資料缺漏、時間重複或先後錯亂時，回測會停止並列出原因，不會暗中改用另一個周期或資料來源。
- **分鐘策略會顯示完整日內資金曲線。** 畫面會保留每個分鐘或原始資料點的資金變化、買賣位置及時間，不會把整個交易日壓成一個點。日線策略則繼續顯示日線資金曲線。
- **新增日內最大回撤。** 用戶除了可以查看跨日最大回撤，亦可以看到同一交易時段內曾經出現的最大資金跌幅，更容易發現日終數字未能反映的日內風險。

#### 績效與交易時間

- **分鐘策略不會把每一分鐘當成一年化計算的一日。** 資金曲線仍保留完整日內變化；年化回報、夏普比率及其他需要按日比較的指標，會使用每個交易時段結束時的資金值計算，避免分鐘數量令年化結果被誇大。
- **交易時段長短會按實際市場日曆處理。** 美股正常交易日的 390 分鐘、半日市的 210 分鐘及夏令時間轉換都會按當日實際安排判斷。半日市不會被誤報為缺資料，資料亦不會跨過收市時間錯誤合併。
- **畫面會交代本次回測使用的時間資料。** 回測、績效、參數矩陣及前向分析頁會顯示資料周期、交易時段、時區、決策周期及成交周期，方便用戶確認策略實際如何運作。

#### 參數矩陣與前向分析改善

- **大型參數研究更穩定。** 使用大量分鐘資料及參數組合時，研究可以完成，並保留完整排名及排名最高候選的回測結果。
- **每組參數都會開啟自己的結果。** 排名、資金曲線及績效資料會保持一致，不會把一組參數連到另一組結果。
- **前向分析會按每個窗口分開選參數和測試。** 系統只用訓練期選參數，再把同一組參數放到下一段未見資料測試；畫面會開啟該窗口選中的結果。
- **指定結果缺失時會報錯。** 指標、回測、參數矩陣及前向分析頁不會因為指定結果不存在或載入失敗，便靜默改為顯示第一份結果，避免用戶在不知情下閱讀錯誤回測。

#### 介面與教學

- **操作畫面與教學已同步更新。** 執行中心、參數矩陣、前向分析、README、互動課程及疑難排解內容已配合分鐘回測、資料來源限制及新版功能更新。

#### 使用提醒

- 2.1.0 的舊結果如果缺少現行時間或年化資料，需要用 2.2.0 重新執行；系統不會猜測舊結果的缺失內容。
- lo2cin4bt 仍然是本機研究軟體，不會向券商提交訂單，回測結果亦不代表實盤交易授權。

### English

This release expands lo2cin4bt from primarily daily research into a platform that can reliably handle minute, hourly, and other timeframes. Data timestamps, trading sessions, execution order, performance calculations, and frontend charts now follow the same rules.

#### Major Upgrades

- **Intraday and multi-timeframe backtests are now supported.** Strategies can use 1-minute, 5-minute, 15-minute, 1-hour, 4-hour, 6-hour, 12-hour, daily, and other declared timeframes. Native daily data remains usable as-is, while different decision and execution timeframes can be combined without creating a separate strategy or backtest path.
- **Higher-timeframe signals can execute on a lower timeframe.** A strategy can wait for an hourly bar to finish, confirm the signal, and trade at the next eligible minute price. Incomplete future data is never exposed to the signal.
- **A Binance BTCUSDT 1-minute moving-average example was added.** The built-in SMA 10/20 example is locked to one complete month and 43,200 minute-level equity points, providing a stable reference for data, fills, equity, and performance.
- **Provider capability is checked before data is accepted.** yfinance is limited to daily data, while Binance, Coinbase, FUTU, and IBKR requests are checked against their declared capabilities. Unsupported periods, missing rows, duplicate timestamps, or out-of-order data stop the run with a clear error instead of silently switching provider or timeframe.
- **Intraday strategies now keep an intraday equity curve.** The frontend preserves each minute or source-data point together with trade markers and timestamps. Daily strategies continue to display daily equity.
- **Intraday maximum drawdown was added.** Users can review the largest loss within a trading session as well as the existing drawdown across the full backtest.

#### Performance And Market Time

- **A minute is not treated as a day for annualized statistics.** The detailed equity curve remains intraday, while annualized return, Sharpe ratio, and other day-comparable statistics use equity at the end of each trading session. This prevents minute counts from inflating annualized results.
- **Session length follows the actual exchange calendar.** A normal 390-minute US session, a 210-minute half day, and daylight-saving transitions are evaluated using the schedule for that date. Half days are not misclassified as missing data, and bars do not cross the market close.
- **Time settings are visible in the frontend.** Backtest, metrics, Parameter Matrix, and WFA pages show the data timeframe, session, timezone, decision timeframe, and execution timeframe used by the run.

#### Parameter Matrix And WFA Improvements

- **Large parameter studies are more dependable.** Studies using extensive minute data and parameter combinations can complete while retaining the full ranking and full backtest results for the highest-ranked candidates.
- **Each parameter set opens its own result.** Rankings, equity curves, and performance data remain aligned, so one parameter row cannot open another candidate's result.
- **WFA selects and tests parameters separately for each window.** Each window selects parameters from training data only, applies that selection to the next unseen period, and opens the result selected for that window.
- **Missing requested results now fail visibly.** Metrics, Backtests, Parameter Matrix, and WFA no longer switch to the first available result when a requested artifact is missing or cannot be loaded.

#### Interface And Guidance

- **The interface and guidance now match the new workflow.** Run Center, Parameter Matrix, WFA, the README, the interactive Lecture, and troubleshooting guidance were updated for intraday research, provider limits, and the current interface.

#### Usage Notes

- Results created by 2.1.0 that lack current time or annualization metadata must be rerun with 2.2.0. The system does not guess missing fields in old artifacts.
- lo2cin4bt remains local research software. It does not submit broker orders, and no backtest result authorizes live trading.

## 2.1.0 - Unified Rust Research Runtime

Release date: 2026-07-27.

### 繁體中文

本版本完成 2.0 beta 開始的 Rust 架構升級，並把重點放在回測可信度、執行速度及使用體驗。以下內容以用戶可感受到的改變為主。

#### 主要升級

- **所有受支援策略改用同一個 Rust 回測核心。** 單次回測、參數矩陣（Parameter Matrix）、前向分析（WFA）及滾動驗證不再各走不同程式路線，結果更一致，新策略亦更容易加入平台。
- **增加可重用的策略積木及技術指標。** 用戶可以組合選股、擇時、資產配置、日曆事件、配對／價差、多腿事件、多空輪動，以及 EMA、RSI、MACD、布林通道、波動率和標準分數等條件。
- **增加月度多空輪動能力。** 回測時可以按排名買入強勢資產、賣空弱勢資產，控制總曝險與淨曝險，並把借貨成本計入回測。
- **增加八個公開內建策略範例。** 用戶可以試用 QQQ 均線穿越、BTC 日曆效應、月度避險、配對價差、選股擇時、板塊多空輪動、動能輪動與固定配置策略。
- **增加一鍵截圖匯出。** 用戶可以一次保存資金曲線、策略表、參數矩陣、回測摘要、風險診斷、再平衡、資產貢獻及配置變化，方便研究記錄與分享。
- **增加中文先行的互動課程及專案 AI 技能（Skill）。** 用戶可以跟隨學習路線理解回測、指標、前向分析和執行中心，也可以讓 AI 協助建立設定及解讀結果。

#### 體驗改善

- **前端統一由 `http://127.0.0.1:2424/` 提供。** 用戶不再需要判斷應該開啟哪一個開發伺服器或連接埠，所有正式頁面都使用同一條啟動路線。
- **大型參數矩陣的工作排程已改良。** 大型工作不再長時間佔用全部容量，其他較小回測可以獲得執行機會，而完整候選結果仍會按設定保留。
- **策略名稱及摘要改為從標準設定產生。** 回測清單會顯示日期、資產、策略概念、工作流程及簡短執行識別碼，用戶更容易分辨不同結果。
- **回測輸出及刪除流程已統一。** 刪除一個回測時，平台會一併清除相關狀態、圖表、截圖及審閱產物，避免舊資料繼續出現在前端。
- **交易時間及成本假設已標準化。** 收市後確認的訊號會在下一個合資格時段執行，公開範例亦統一使用 `0.1%` 交易費率，減少前視偏差及過度樂觀的結果。

#### 問題修正

- **修正多項績效數據顯示 `n/a` 或錯誤數值的問題。** 年化回報、夏普比率、最大回撤、卡瑪比率、換手率、交易、再平衡、資產貢獻、配置及風險診斷現在都讀取同一份已驗證結果。
- **修正基準比較及交易標記。** 基準按鈕和基準回報恢復運作，入場／出場標記會直接顯示在資金曲線，而且預設關閉，不再產生一張重複圖表。
- **修正策略比較的預設顯示。** 比較頁預設顯示首三名策略，資金曲線預設只顯示一個策略，避免大量線條令圖表難以閱讀。
- **修正參數矩陣、熱圖及前向分析頁面的空白與排版問題。** 候選資料、熱圖及 WFA 結果可以正常顯示，長名稱、卡片和表格亦不再超出畫面。
- **修正前向分析重複計算相同樣本外（OOS）區間。** 相同測試只會執行一次並由相關候選共用，保留原有結果內容之餘縮短等候時間。
- **修正長頁面截圖問題。** 匯出的指標及配置截圖現在會保持一致背景，不再在分段位置出現顯示問題。

#### 架構整理與驗收

- **移除已由 Rust 取代的 Python 回測器、舊設定映射及策略專屬執行路線。** 平台不再同時維護兩套會產生不同結果的回測邏輯，舊設定及舊結果需要按 2.1 格式重新執行。
- **所有結果增加強制驗證關卡。** 未通過資料、帳務及結果完整性檢查的回測不會交給績效頁或前端，降低錯誤結果被當成有效研究證據的風險。
- **更新黃金測試（golden test）及完整驗收套件。** 測試現已覆蓋前視偏差、固定排序、帳務、借貨成本、介面合約、前端頁面及乾淨環境回測，讓後續改動更容易發現結果退化。
- **公開版本移除私人策略、回測結果、生成輸出及內部維護資料。** GitHub 用戶只會取得產品所需的程式、範例和文件，不會包含擁有者的私人研究內容。
- **本版本仍定位為本機研究軟體。** 平台不會向券商提交訂單，也不會把任何回測結果視為實盤交易授權。

### English

This release completes the Rust architecture upgrade started in the 2.0 beta, with a focus on backtest reliability, execution speed, and usability. The notes below describe changes users can see and use.

#### Major Upgrades

- **All supported strategies now use one shared Rust backtest engine.** Single backtests, Parameter Matrix, WFA, and rolling validation no longer follow separate execution paths, producing more consistent results and making new strategies easier to add.
- **Reusable strategy building blocks and technical indicators were added.** Users can combine selection, timing, allocation, calendar events, pair/spread, multi-leg, and long/short rotation logic with EMA, RSI, MACD, Bollinger Bands, volatility, z-score, and other fields.
- **Monthly long/short rotation is now supported.** Backtests can rank assets, buy the strongest, short the weakest, control gross and net exposure, and include borrow costs.
- **Eight public built-in strategy examples were added.** Users can try QQQ moving-average cross, BTC calendar effects, monthly hedging, pair spread, selection/timing, sector long/short rotation, momentum rotation, and fixed-allocation strategies.
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
- **Long-page screenshot issues were fixed.** Exported metric and allocation images now keep a consistent background across capture boundaries.

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
