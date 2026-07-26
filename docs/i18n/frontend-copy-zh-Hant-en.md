# lo2cin4bt Frontend Copy Glossary: English / 繁體中文

Version: 0.1
Date: 2026-05-10
Scope: `app.api`, `plotter/web`, and current frontend copy for Run Center, Metrics, Backtests, Parameter Matrix, WFA, and Factor Analysis.

This file is the canonical frontend copy map for future English / Traditional Chinese parity. The current app opens in Traditional Chinese by default and offers English through the language toggle. Traditional Chinese uses natural written Traditional Chinese for Hong Kong / Taiwan users, while technical terms such as config, parquet, WFA, OOS, IS, and Parameter Matrix may stay in English with a stable Chinese label.

| key | English | 繁體中文 | 使用位置 | 備註 |
| --- | --- | --- | --- | --- |
| `nav.commandCenter` | Overview | 總覽 | AppShell nav | Landing status page; job launch stays in Run Center. |
| `nav.runCenter` | Run Center | 執行中心 | AppShell nav, Run Center page | Canonical page name. |
| `nav.metrics` | Metrics | 策略表現 | AppShell nav, Metrics layout | Metrics section root. |
| `nav.walkForward` | Walk-Forward | 前向分析 (WFA) | AppShell nav, WFA labels | Keep WFA visible. |
| `language.en` | EN | EN | Language toggle | Short toggle label. |
| `language.zhHant` | 繁中 | 繁中 | Language toggle | Short toggle label. |
| `common.loading.runCenter` | Loading run center... | 正在載入執行中心... | Run Center | Loading state. |
| `common.loading.metricsOverview` | Loading metrics overview... | 正在載入策略表現總覽... | Metrics Overview | Loading state. |
| `common.loading.backtestDetail` | Loading backtest detail... | 正在載入回測詳情... | Backtests | Loading state. |
| `common.loading.parameterMatrix` | Loading parameter research workspace... | 正在載入參數矩陣... | Parameter Matrix | Loading state. |
| `common.loading.wfa` | Loading WFA dashboard... | 正在載入前向分析 (WFA)... | WFA | Loading state. |
| `common.selectAll` | Select All | 全選 | Run Center, Metrics, Parameter Matrix | Shared action. |
| `common.clear` | Clear | 清除 | Run Center | Shared action. |
| `common.clearAll` | Clear All | 全部清除 | Metrics | Shared action. |
| `common.previous` | Previous | 上一頁 | Paginated tables | Shared pagination. |
| `common.next` | Next | 下一頁 | Paginated tables | Shared pagination. |
| `common.page` | Page | 頁 | Paginated tables | Keep numeric format as `頁 X / Y`. |
| `common.showBenchmark` | Show Benchmark | 顯示 Benchmark 基準 | Metrics, Backtests | Keep Benchmark visible. |
| `common.actions` | Actions | 操作 | Tables | Shared table heading. |
| `common.rank` | Rank | 排名 | Tables | Shared table heading. |
| `common.label` | Label | 標籤 | Tables | Shared table heading. |
| `common.show` | Show | 顯示 | Tables | Shared table heading. |
| `common.select` | Select | 選取 | Tables | Shared table heading. |
| `common.dateRange` | Date Range | 日期範圍 | Metrics, Backtests | Shared table heading. |
| `common.asset` | Asset | 資產 | Backtests, WFA | Shared table heading. |
| `common.status` | Status | 狀態 | Backtests | Shared table heading. |
| `workflow.backtest` | Backtest | 回測 | API labels, Metrics Backtests | Noun. |
| `workflow.backtests` | Backtests | 回測 | Run Center, Metrics nav | Plural page/section label. |
| `workflow.singleBacktest` | Single Backtest | 單次回測 | API mode display | Mode label. |
| `workflow.parameterMatrix` | Parameter Matrix | 參數矩陣 | API mode display, Metrics subnav, page | Live frontend label. |
| `workflow.walkForward` | Walk-Forward | 前向分析 (WFA) | API labels, WFA | Long form. |
| `workflow.rollingWindows` | Rolling Windows | 滾動視窗 | API mode display, WFA | Mode label. |
| `workflow.rollingValidation` | Rolling Validation | 滾動驗證 | WFA alternate title | Workflow label. |
| `workflow.factorAnalysis` | Factor Analysis | Factor Analysis 因子分析 | Run Center, legacy statanalyser | Preferred replacement for Predictor Analysis. |
| `workflow.predictorAnalysisLegacy` | Predictor Analysis | Predictor Analysis 預測因子分析（舊稱） | API legacy label | Compatibility only. |
| `workflow.summary` | Summary | 摘要 | API mode display | Generic. |
| `runCenter.title` | Run Center | 執行中心 | Run Center page title | Canonical page heading. |
| `runCenter.subtitle` | Batch-select configs, watch running jobs live on the same page, and open fresh results without leaving this workflow. | 批次選取 config，在同一頁即時查看執行中的工作，並直接開啟新產生的結果。 | Run Center hero card | Keep `config`. |
| `runCenter.searchConfig` | Search config... | 搜尋 config... | Config selector | Placeholder. |
| `runCenter.runFactorBatch` | Run Factor Analysis Batch | 執行 Factor Analysis 因子分析批次 | Run Center buttons | Action. |
| `runCenter.runBacktestBatch` | Run Backtest Batch | 執行回測批次 | Run Center buttons | Action. |
| `runCenter.runWfaBatch` | Run Walk-Forward Batch | 執行前向分析 (WFA) 批次 | Run Center buttons | Action. |
| `runCenter.runningJobs` | Running Jobs | 執行中工作 | Run Center section | Section title. |
| `runCenter.noTrackedBatches` | No tracked or active batches right now. | 目前沒有追蹤中或執行中的批次。 | Run Center empty state | Empty state. |
| `runCenter.noLogsYet` | No logs yet. | 尚未有 log。 | Run Center logs | Keep log. |
| `runCenter.batchResults` | Batch Results | 批次結果 | Run Center section | Section title. |
| `runCenter.openWorkspace` | Open Folder | 開啟設定資料夾 | Config selector | Action. |
| `runCenter.openOutput` | Open Output | 開啟輸出資料夾 | Config selector | Action. |
| `runCenter.openMetrics` | Open Metrics | 開啟策略表現 | Batch result link | Action. |
| `runCenter.openWalkForward` | Open Walk-Forward | 開啟前向分析 (WFA) | Batch result link | Action. |
| `runCenter.reviewFactorSummaryHere` | Review Factor Summary Here | 在此查看因子摘要 | Batch result action | Action. |
| `runCenter.factorSummary` | Factor Analysis Summary | Factor Analysis 因子分析摘要 | Run Center section | Section title. |
| `runCenter.factorSummarySubtitle` | Factor analysis stays inside Run Center in v1. Use this panel to inspect the latest managed analysis artifacts without leaving the execution flow. | v1 版的因子分析保留在執行中心內。可在此面板查看最新受管理分析 artifact，而不用離開執行流程。 | Run Center section | Keep artifact. |
| `runCenter.selectFactorRun` | Select a factor analysis run | 選取一個因子分析 run | Run Center select | Placeholder. |
| `runCenter.noFactorRunSelected` | No factor analysis run selected yet. | 尚未選取因子分析 run。 | Run Center empty state | Empty state. |
| `runCenter.loadingFactorSummary` | Loading factor analysis summary... | 正在載入因子分析摘要... | Run Center loading | Loading state. |
| `runCenter.unableFactorSummary` | Unable to load factor analysis summary. | 無法載入因子分析摘要。 | Run Center error | Error state. |
| `metrics.title` | Metrics | 策略表現 | Metrics layout title | Section root. |
| `metrics.subtitle` | Shared metrics file selection for Overview, Parameter Matrix, and Backtests. | 總覽、參數矩陣與回測會共用同一個策略表現檔案。 | Metrics layout subtitle | Natural Traditional Chinese copy. |
| `metrics.overview` | Overview | 總覽 | Metrics subnav | Subnav. |
| `metrics.backtests` | Backtests | 回測 | Metrics subnav | Subnav. |
| `metrics.metricsFileSelection` | Metrics File Selection | 策略表現檔案選取 | Metrics layout control | Use user-facing Chinese label for Metrics. |
| `metrics.backtestSelection` | Backtest Selection | 回測選取 | Metrics layout control | Control label. |
| `metricsOverview.title` | Metrics Overview | 策略表現總覽 | Metrics Overview title | Page title. |
| `metricsOverview.subtitle` | Compare strategy equity curves against normalized buy-and-hold equity, then drill into a selected strategy. | 比較策略 equity curve 與標準化 buy-and-hold equity，再深入查看選取策略。 | Metrics Overview card | Keep equity curve and buy-and-hold. |
| `metricsOverview.strategyTable` | Strategy Table | 策略表 | Metrics Overview table section | Section title. |
| `metricsOverview.strategyTableSubtitle` | Choose the active row here. Filters live with the ranking table, while the KPI cards below follow the selected strategy. | 在此選取目前使用的列。篩選條件套用於排名表，下方 KPI 卡會跟隨已選策略。 | Metrics Overview table | Subtitle. |
| `metricsOverview.selectedStrategySummary` | Selected Strategy Summary | 已選策略摘要 | Metrics Overview detail section | Section title. |
| `metricsOverview.currentSelection` | Current selection | 目前選取 | Metrics Overview / Backtests | Prefix. |
| `metricsOverview.portfolioEquity` | Portfolio Equity | 投資組合 Equity | Portfolio overview | Keep Equity. |
| `metricsOverview.portfolioEquitySubtitle` | Equity value through time after rebalance turnover and configured costs. | 納入再平衡 turnover 與設定成本後的 equity value 時間序列。 | Portfolio overview | Keep technical terms. |
| `metricsOverview.portfolioStrategyTableSubtitle` | Choose the active row here. Filters live with the ranking table, while the selected summary below follows the active portfolio. | 在此選取目前使用的列。篩選條件套用於排名表，下方摘要會跟隨目前投資組合。 | Portfolio overview table | Subtitle. |
| `metricsTable.trades` | Trades | 交易 | Metrics table | Column. |
| `metricsTable.exposure` | Exposure | 曝險 | Metrics table | Column. |
| `metricsTable.profitFactor` | Profit Factor | Profit Factor 獲利因子 | Metrics table | Column. |
| `metricsTable.lastTrade` | Last Trade | 最後交易 | Metrics table | Column. |
| `metricsTable.checkpoints` | Checkpoints | 檢查點 | Portfolio table | Column. |
| `metricsTable.totalReturn` | Total Return | 總報酬 | Table/KPI | Metric label. |
| `metricsTable.maxDrawdown` | Max Drawdown | 最大回撤 | Table/KPI | Metric label. |
| `metricsTable.avgHoldings` | Avg Holdings | 平均持倉數 | Portfolio filter | Placeholder label. |
| `backtests.summary` | Backtest Summary | 回測摘要 | Backtests page | Section title. |
| `backtests.summarySubtitle` | Read the identity and headline metrics first, then use the grouped panels for context. | 先查看識別資訊與核心指標，再用分組面板補充背景。 | Backtests page | Subtitle. |
| `backtests.portfolioSummary` | Portfolio Backtest Summary | 投資組合回測摘要 | Backtests portfolio page | Section title. |
| `backtests.portfolioSummarySubtitle` | Portfolio-level performance for the selected strategy table row. | 已選策略表列的投資組合層級表現。 | Backtests portfolio page | Subtitle. |
| `backtests.contextPanels` | Context Panels | 背景面板 | Backtests page | Section title. |
| `backtests.equityVsBenchmark` | Equity vs Benchmark | Equity 與 Benchmark 基準比較 | Backtests page | Section title. |
| `backtests.entryExits` | Entry & Exits | 進出場 | Backtests page | Section title. |
| `backtests.tradeReturnDistribution` | Trade Return Distribution | 交易報酬分佈 | Backtests page | Section title. |
| `backtests.tradeSummary` | Trade Summary | 交易摘要 | Backtests page | Section title. |
| `backtests.tradeSummarySubtitle` | Paginated closed-trade summary for the selected strategy. | 已選策略的已平倉交易分頁摘要。 | Backtests page | Subtitle. |
| `backtests.selectMetricsFirst` | Select a metrics run first before opening Backtests. | 請先選取策略表現執行結果，再開啟回測頁面。 | Backtests missing state | Use user-facing Chinese label for Metrics. |
| `backtests.noBacktestSelected` | No backtest is selected yet. | 尚未選取回測。 | Backtests missing state | Empty state. |
| `parameterMatrix.title` | Parameter Research | 參數矩陣 | Parameter Matrix page title | Existing title, aligned to page name. |
| `parameterMatrix.subtitle` | Review completed parameter-test candidates and stable regions. WFA is run separately from Run Center using the strategy parameter range. | 檢視已完成參數測試中的候選組合與穩定區域；前向分析 (WFA) 會由執行中心另外使用策略參數範圍進行。 | Parameter Matrix hero | Keep WFA/parameter-test. |
| `parameterMatrix.selectMetricsFirst` | Select a metrics run first before opening Parameter Matrix. | 請先選取策略表現執行結果，再開啟參數矩陣。 | Parameter Matrix missing state | Use user-facing Chinese label for Metrics. |
| `parameterMatrix.noMatrix` | No parameter matrix is available for this run. Single backtests and fixed-weight portfolios do not expose parameter research. | 此執行結果沒有可用的參數矩陣。單次回測與固定權重投資組合不會提供參數研究。 | Parameter Matrix missing state | Missing state. |
| `parameterMatrix.notEnoughParams` | This run does not expose enough semantic parameters for a heatmap. | 此執行結果沒有足夠的語義參數可繪製熱圖。 | Parameter Matrix missing state | Keep heatmap. |
| `parameterMatrix.rankedReview` | Ranked Parameter Review | 參數排名檢視 | Parameter Matrix section | Section title. |
| `parameterMatrix.heatmapDiagnostics` | Heatmap Diagnostics | 熱圖診斷 | Parameter Matrix section | Section title. |
| `parameterMatrix.candidateReview` | Parameter Candidate Review | 參數候選檢視 | Parameter Matrix section | Section title. |
| `parameterMatrix.candidateReviewSubtitle` | Review why each candidate was selected, then compare the stable clusters against WFA after Run Center completes its independent window test. | 檢視每個候選參數被選出的原因，並在執行中心完成獨立視窗測試後，與前向分析 (WFA) 交叉比對。 | Parameter Matrix section | Keep clusters/WFA. |
| `parameterMatrix.candidateEvidence` | Candidate Evidence | 候選依據 | Parameter Matrix section | Section title. |
| `wfa.title` | Walk-Forward Analysis | 前向分析 (WFA) | WFA title | Use WFA-visible technical label. |
| `wfa.rollingValidationTitle` | Rolling Validation | 滾動驗證 | WFA alternate title | Workflow variant. |
| `wfa.noOutputs` | No WFA outputs are available yet. Run a Walk-Forward Analysis job from Run Center first. | 目前沒有可用的前向分析 (WFA) 輸出。請先在執行中心執行前向分析工作。 | WFA missing state | Missing state. |
| `wfa.missingRun` | The selected WFA run no longer exists. Choose an available WFA run or start a new WFA job. | 目前選取的前向分析 (WFA) 執行結果已不存在。請選取其他結果或重新執行。 | WFA missing state | Missing state. |
| `wfa.noManagedRun` | No managed WFA run is available yet. | 目前尚未有可管理的前向分析 (WFA) 結果。 | WFA missing state | Missing state. |
| `wfa.windowPortfolioValidation` | Window Portfolio Validation | 視窗投資組合驗證 | WFA portfolio section | Section title. |
| `wfa.windowPortfolioEvidence` | Window Portfolio Evidence | 視窗投資組合依據 | WFA portfolio section | Section title. |
| `wfa.windowPortfolioSubtitle` | Portfolio WFA evidence is computed from the selected OOS run for each window, not from candidate diagnostics. | 投資組合前向分析 (WFA) 的證據來自每個視窗選出的 OOS 回測，而不是候選診斷資料。 | WFA portfolio section | Keep WFA/OOS/candidate diagnostics. |
| `wfa.parameterFamilySummary` | Parameter Family / Cluster Summary | 參數族群 / 分群摘要 | WFA cluster section | Keep Cluster. |
| `wfa.parameterFamilySubtitle` | Families group similar IS-selected parameter sets. Unique sets and selected windows are shown separately. | 族群會把相似的 IS 選中參數歸在一起；獨立參數組與被選中視窗會分開顯示。 | WFA cluster section | Keep IS/unique sets. |
| `wfa.isParameterSets` | IS Parameter Sets | IS 參數組 | WFA table section | Section title. |
| `wfa.isParameterSetsSubtitle` | Exact parameter sets selected by rolling IS optimization. Use Show Windows to inspect when each set was selected. | 由每個 IS 視窗最佳化選出的實際參數組。可用「顯示視窗」查看該參數在哪些視窗被選中。 | WFA table section | Keep IS optimization. |
| `wfa.window` | Window | 視窗 | WFA tables | Column. |
| `wfa.windows` | Windows | 視窗 | WFA tables | Column/action. |
| `wfa.train` | Train | 訓練區間 | WFA table | Column. |
| `wfa.test` | Test | 測試區間 | WFA table | Column. |
| `wfa.family` | Family | 族群 | WFA table | Column. |
| `wfa.selectionEvidence` | Selection Evidence | 選取依據 | WFA table | Column. |
| `wfa.selectedWindows` | Selected Windows | 被選中視窗 | WFA table | Column. |
| `wfa.worstOos` | Worst OOS | 最差 OOS | WFA table | Column. |
| `wfa.riskGates` | Risk Gates | 風控門檻 | WFA table | Column. |
| `wfa.robustScore` | Robust Score | 穩健分數 | WFA table | Column. |
| `wfa.reviewResult` | Review Result | 檢視結果 | WFA table | Column. |
| `wfa.needsReview` | Needs Review | 需檢視 | WFA status | Status. |
| `wfa.review` | Needs Review | 需檢視 | WFA table | Column/status. |
| `wfa.pass` | Pass | 通過 | WFA status | Status. |
| `wfa.showWindows` | Show Windows | 顯示視窗 | WFA action | Action. |
| `wfa.hideWindows` | Hide Windows | 隱藏視窗 | WFA action | Action. |
| `api.module.backtest` | Backtest | 回測 | `app.api.labels.MODULE_DISPLAY` | API-facing display. |
| `api.module.walkForward` | Walk-Forward | 前向分析 (WFA) | `app.api.labels.MODULE_DISPLAY` | API-facing display. |
| `api.module.predictorAnalysis` | Predictor Analysis | Factor Analysis 因子分析（舊稱 Predictor Analysis） | `app.api.labels.MODULE_DISPLAY` | Frontend should display Factor Analysis where possible. |
| `api.mode.matrix` | Parameter Matrix | Parameter Matrix 參數矩陣 | `app.api.labels.MODE_DISPLAY` | API-facing display. |
| `api.mode.single` | Single Backtest | 單次回測 | `app.api.labels.MODE_DISPLAY` | API-facing display. |
| `api.mode.windows` | Rolling Windows | 滾動視窗 | `app.api.labels.MODE_DISPLAY` | API-facing display. |
| `api.mode.summary` | Summary | 摘要 | `app.api.labels.MODE_DISPLAY` | API-facing display. |
| `api.category.top20Sharpe` | Top 20 Sharpe | Sharpe 前 20 | `app.api.payloads.CATEGORY_MAP` | Category label from API. |
| `api.category.top20Return` | Top 20 Return | Return 前 20 | `app.api.payloads.CATEGORY_MAP` | Category label from API. |
| `api.category.top20Cagr` | Top 20 CAGR | CAGR 前 20 | `app.api.payloads.CATEGORY_MAP` | Category label from API. |
| `api.category.top20Calmar` | Top 20 Calmar | Calmar 前 20 | `app.api.payloads.CATEGORY_MAP` | Category label from API. |
| `api.category.top20Sortino` | Top 20 Sortino | Sortino 前 20 | `app.api.payloads.CATEGORY_MAP` | Category label from API. |
| `api.category.top20RecoveryFactor` | Top 20 Recovery Factor | Recovery Factor 前 20 | `app.api.payloads.CATEGORY_MAP` | Category label from API. |
| `api.category.top20InformationRatio` | Top 20 Information Ratio | Information Ratio 前 20 | `app.api.payloads.CATEGORY_MAP` | Category label from API. |
| `api.category.top20ProfitFactor` | Top 20 Profit Factor | Profit Factor 前 20 | `app.api.payloads.CATEGORY_MAP` | Category label from API. |
| `api.category.top20LowestMdd` | Top 20 Lowest MDD | MDD 最低前 20 | `app.api.payloads.CATEGORY_MAP` | Category label from API. |
| `api.category.top20ExcessReturn` | Top 20 Excess Return | Excess Return 前 20 | `app.api.payloads.CATEGORY_MAP` | Category label from API. |

## Translation Rules

- Preserve IDs, filenames, paths, symbols, config keys, run IDs, parquet/CSV/JSON, IS/OOS/WFA, and metric abbreviations in English.
- Prefer `Factor Analysis 因子分析` in the frontend. Keep `Predictor Analysis 預測因子分析（舊稱）` only when documenting legacy compatibility.
- Use `總覽` / `Overview` for the landing status page, `執行中心` / `Run Center` for job launch and live execution, `回測` for Backtest/Backtests, `策略表現` for Metrics, `參數矩陣` for Matrix, `前向分析 (WFA)` for WFA optimization, and `滾動驗證` for rolling validation.
- For first visible title use bilingual technical names where helpful, e.g. `Parameter Matrix 參數矩陣`, `前向分析 (WFA)`, `Benchmark 基準`.
- Do not translate user data values, strategy labels, artifact names, or API-provided run labels unless they are known legacy UI labels such as `Predictor Analysis`.
