# Validation Workflow

此資料夾負責回測後的驗證工作流程，不是另一套回測引擎。

## 正式流程

1. `ConfigLoader_validation_workflow.py` 載入 canonical `wfa_run` 設定。
2. `ConfigValidator_validation_workflow.py` 驗證設定及時間窗口。
3. `UnifiedPortfolioWFARunner_validation_workflow.py` 建立 train/OOS
   EngineRequest，並交由同一個 persistent Rust service 執行。
4. `OptunaSearchEngine_validation_workflow.py` 負責可選的參數搜尋。
5. `RobustSelector_validation_workflow.py` 只使用訓練期結果挑選候選者。
6. `WFAAcceptanceEvaluator_validation_workflow.py` 評估 OOS 接受條件。
7. `ResultsExporter_validation_workflow.py` 輸出驗證 artifacts。

## 邊界

- WFA 與 rolling validation 都是 workflow，不是獨立 backtester path。
- 所有 train/OOS 回測經 canonical EngineRequest、MarketDataBundle 及 Rust engine。
- Parameter Matrix 由 strategy-run parameter domains 及 Rust grouped batch 執行。
- 已退役的 `condition_pairs`、`indicator_params` 及 Python indicator optimizer
  不可重新加入 compatibility mapping。
- Metrics 由 Rust metrics service 計算；Python 只負責 transport 及 artifact 寫入。
