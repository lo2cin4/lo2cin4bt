"""
WalkForwardEngine_wfanalyser.py

【功能說明】
------------------------------------------------------------
本模組為 WFA 的核心引擎，負責執行 Walk-Forward Analysis 流程，
包括窗口劃分、參數優化、回測執行等。

【流程與數據流】
------------------------------------------------------------
- 主流程：載入數據 → 劃分窗口 → 參數優化 → 測試窗口回測 → 收集結果
- 數據流：配置數據 → 窗口劃分 → 優化結果 → 回測結果 → WFA 結果

【維護與擴充重點】
------------------------------------------------------------
- 窗口劃分邏輯需要確保數據完整性
- 參數優化需要與 ParameterOptimizer 協調
- 回測執行需要重用 VectorBacktestEngine

【常見易錯點】
------------------------------------------------------------
- 窗口劃分錯誤導致數據不完整
- 參數優化結果未正確傳遞到測試窗口
- 結果收集格式不一致

【範例】
------------------------------------------------------------
- 執行 WFA：engine = WalkForwardEngine(config_data, logger); results = engine.run()

【與其他模組的關聯】
------------------------------------------------------------
- 調用 DataLoaderWFAAnalyser 載入數據
- 調用 ParameterOptimizer 進行參數優化
- 調用 VectorBacktestEngine 執行回測
- 調用 metricstracker 計算績效指標

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本 WFA 功能

【參考】
------------------------------------------------------------
- Base_wfanalyser.py: WFA 框架核心控制器
- ParameterOptimizer_wfanalyser.py: 參數優化器
- VectorBacktestEngine_backtester.py: 向量化回測引擎
- wfanalyser/README.md: WFA 模組詳細說明
"""

import logging
import math
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from rich.text import Text

from wfanalyser.DataLoader_wfanalyser import DataLoaderWFAAnalyser
from utils import show_error, show_info, show_step_panel, show_success, show_warning
from wfanalyser.ParameterOptimizer_wfanalyser import ParameterOptimizer
from wfanalyser.utils import get_console

console = get_console()


class WalkForwardEngine:
    """
    WFA 核心引擎

    負責執行 Walk-Forward Analysis 流程，包括窗口劃分、
    參數優化、回測執行等。
    """

    def __init__(self, config_data: Any, logger: Optional[logging.Logger] = None):
        """
        初始化 WalkForwardEngine

        Args:
            config_data: WFA 配置數據對象
            logger: 日誌記錄器
        """
        self.config_data = config_data
        self.logger = logger or logging.getLogger("lo2cin4bt.wfanalyser.engine")
        self.data: Optional[pd.DataFrame] = None
        self.frequency: Optional[str] = None
        self.wfa_config = config_data.wfa_config
        self.results: List[Dict[str, Any]] = []
        self.windows: List[Dict[str, Any]] = []  # 保存窗口劃分信息，用於日期顯示

    def run(self) -> Optional[Dict[str, Any]]:
        """
        執行 WFA 流程

        Returns:
            Optional[Dict[str, Any]]: WFA 結果，如果執行失敗則返回 None
        """
        try:
            show_info("WFANALYSER", "🚀 開始執行 Walk-Forward Analysis")

            # 步驟 1: 載入數據
            self._load_data()

            if self.data is None:
                show_error("WFANALYSER", "數據載入失敗，無法繼續執行 WFA")
                return None

            # 步驟 2: 劃分窗口
            windows = self._divide_windows()
            self.windows = windows  # 保存窗口劃分信息

            if not windows:
                show_error("WFANALYSER", "窗口劃分失敗，無法繼續執行 WFA")
                return None

            # 顯示窗口劃分結果
            mode = self.wfa_config.get("mode", "standard")
            show_success("WFANALYSER", f"成功劃分 {len(windows)} 個窗口 (模式: {mode})")

            # 步驟 3: 處理所有窗口（方案 A：所有 condition_pairs 一起處理）
            # VectorBacktestEngine 已經內建參數組合的並行化，會自動使用
            
            # 所有窗口的結果
            all_window_results = []
            all_window_status = []
            
            # 外層循環：窗口（方案 A）
            for window_idx, window in enumerate(windows, 1):
                console.print(
                    f"  [dim]處理窗口 {window_idx}/{len(windows)}[/dim]"
                )
                
                # 處理單一窗口（所有 condition_pairs 一起處理）
                window_result, status = self._process_window(
                    window, window_idx, len(windows)
                )
                
                all_window_status.append(status)
                if window_result:
                    all_window_results.append(window_result)
            
            # 步驟 4: 顯示結果摘要表格
            # 步驟 5: 收集結果（方案 A）
            final_results = self._collect_results(all_window_results)
            
            # 檢查是否有成功的窗口
            if len(all_window_results) == 0:
                # 所有窗口都失敗，顯示詳細的失敗原因
                failure_summary = []
                failure_summary.append(f"⚠️ 所有 {len(windows)} 個窗口處理都失敗")
                
                # 統計失敗原因
                failure_reasons = {}
                for status in all_window_status:
                    # 檢查 sharpe 和 calmar 的失敗原因
                    for objective in ["sharpe", "calmar"]:
                        reason = status.get(f"{objective}_failure_reason")
                        if reason:
                            if reason not in failure_reasons:
                                failure_reasons[reason] = 0
                            failure_reasons[reason] += 1
                    
                    # 檢查是否有異常錯誤
                    if status.get("error"):
                        error_msg = status.get("error", "未知錯誤")
                        if error_msg not in failure_reasons:
                            failure_reasons[error_msg] = 0
                        failure_reasons[error_msg] += 1
                
                if failure_reasons:
                    failure_summary.append("\n失敗原因統計：")
                    for reason, count in failure_reasons.items():
                        failure_summary.append(f"  • {reason}: {count} 次")
                else:
                    failure_summary.append("\n未找到具體失敗原因，請檢查日誌文件")
                
                # 顯示前幾個窗口的詳細狀態
                failure_summary.append(f"\n前 3 個窗口狀態：")
                for idx, status in enumerate(all_window_status[:3], 1):
                    window_id = status.get("window_id", f"窗口 {idx}")
                    train_size = status.get("train_size", "N/A")
                    test_size = status.get("test_size", "N/A")
                    sharpe_status = status.get("sharpe_status", "未執行")
                    calmar_status = status.get("calmar_status", "未執行")
                    failure_summary.append(
                        f"  {window_id}: 訓練集={train_size}, 測試集={test_size}, "
                        f"Sharpe={sharpe_status}, Calmar={calmar_status}"
                    )
                
                show_warning("WFANALYSER", "\n".join(failure_summary))
                self.logger.warning(f"WFA 執行完成但所有窗口都失敗: {failure_reasons}")
            else:
                show_success("WFANALYSER",
                    f"WFA 執行完成\n"
                    f"   窗口數: {len(windows)}\n"
                    f"   成功處理: {len(all_window_results)} 個窗口結果"
                )

            return final_results

        except Exception as e:
            show_error("WFANALYSER", f"WFA 執行失敗: {e}")
            self.logger.error(f"WFA 執行失敗: {e}")
            return None

    def _load_data(self) -> None:
        """載入數據"""
        try:
            data_loader = DataLoaderWFAAnalyser(logger=self.logger)

            # 合併 dataloader_config 和 predictor_config
            full_dataloader_config = {
                **self.config_data.dataloader_config,
                "predictor_config": self.config_data.predictor_config,
            }

            self.data = data_loader.load_data(full_dataloader_config)
            self.frequency = data_loader.frequency

            if self.data is not None:
                data_loader.display_loading_summary()

        except Exception as e:
            self.logger.error(f"數據載入失敗: {e}")
            raise

    def _divide_windows(self) -> List[Dict[str, Any]]:
        """
        劃分窗口

        Returns:
            List[Dict[str, Any]]: 窗口列表，每個窗口包含 train_start, train_end, test_start, test_end
        """
        if self.data is None:
            return []

        total_points = len(self.data)
        mode = self.wfa_config.get("mode", "standard")
        train_pct = self.wfa_config.get("train_set_percentage", 0.6)
        test_pct = self.wfa_config.get("test_set_percentage", 0.2)
        step_size = self.wfa_config.get("step_size", 30)

        # 計算窗口大小（向下取整）
        train_size = math.floor(total_points * train_pct)
        test_size = math.floor(total_points * test_pct)

        windows = []

        if mode == "standard":
            # 標準 Walk-Forward：固定訓練集和測試集大小，滾動前進
            current_start = 0

            while current_start + train_size + test_size <= total_points:
                train_start = current_start
                train_end = train_start + train_size
                test_start = train_end
                test_end = test_start + test_size

                windows.append(
                    {
                        "window_id": len(windows) + 1,
                        "train_start": train_start,
                        "train_end": train_end,
                        "test_start": test_start,
                        "test_end": test_end,
                        "train_data": self.data.iloc[train_start:train_end],
                        "test_data": self.data.iloc[test_start:test_end],
                    }
                )

                # 向前移動步長
                current_start += step_size

        elif mode == "anchored":
            # Anchored Walk-Forward：固定起點，訓練集逐步增長
            # 初始訓練集大小應該等於測試集大小（這樣第一個窗口是 1個測試集長度 IS → 1個測試集長度 OOS）
            train_start = 0
            initial_train_size = test_size  # 初始訓練集 = 測試集大小
            current_train_size = initial_train_size
            # 注意：train_set_percentage 僅作為參考，實際窗口生成以數據可用性為準
            # 只要數據足夠，訓練集可以繼續增長，不受 train_set_percentage 硬性限制

            while train_start + current_train_size + test_size <= total_points:
                train_end = train_start + current_train_size
                test_start = train_end
                test_end = test_start + test_size

                windows.append(
                    {
                        "window_id": len(windows) + 1,
                        "train_start": train_start,
                        "train_end": train_end,
                        "test_start": test_start,
                        "test_end": test_end,
                        "train_data": self.data.iloc[train_start:train_end],
                        "test_data": self.data.iloc[test_start:test_end],
                    }
                )

                # 訓練集增長（每次增加步長）
                current_train_size += step_size

        return windows

    def _process_window(
        self, window: Dict[str, Any], current: int, total: int
    ) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
        """
        處理單個窗口

        Args:
            window: 窗口數據
            current: 當前窗口編號
            total: 總窗口數

        Returns:
            Tuple[Optional[Dict[str, Any]], Dict[str, Any]]: (窗口處理結果, 狀態信息)
        """
        status = {
            "window_id": window["window_id"],
            "train_size": len(window["train_data"]),
            "test_size": len(window["test_data"]),
            "sharpe_status": "未執行",
            "calmar_status": "未執行",
            "sharpe_metric": None,
            "calmar_metric": None,
            "sharpe_is": None,  # In-Sample (訓練集) Sharpe
            "sharpe_oos": None,  # Out-of-Sample (測試集) Sharpe
            "sharpe_is_return": None,  # In-Sample (訓練集) Return
            "sharpe_oos_return": None,  # Out-of-Sample (測試集) Return
            "calmar_is": None,  # In-Sample (訓練集) Calmar
            "calmar_oos": None,  # Out-of-Sample (測試集) Calmar
            "calmar_is_return": None,  # In-Sample (訓練集) Return
            "calmar_oos_return": None,  # Out-of-Sample (測試集) Return
            "sharpe_failure_reason": None,  # 失敗原因
            "calmar_failure_reason": None,  # 失敗原因
        }

        try:
            # 步驟 1: 參數優化（在訓練集上）
            optimizer = ParameterOptimizer(
                window["train_data"],
                self.frequency,
                self.config_data,
                logger=self.logger,
            )

            optimization_objectives = self.wfa_config.get(
                "optimization_objectives", ["sharpe", "calmar"]
            )

            window_results = {}

            for objective in optimization_objectives:
                # 執行參數優化（靜默模式），同時獲取訓練集績效
                optimal_params, train_metrics = optimizer.optimize_with_is_metrics(
                    objective, silent=True
                )

                if optimal_params is None:
                    status[f"{objective}_status"] = "失敗"
                    # 獲取失敗原因
                    failure_reason = optimizer.get_last_failure_reason()
                    status[f"{objective}_failure_reason"] = failure_reason or "未知原因"
                    continue

                # 獲取所有 condition_pair 的 grid_regions
                all_grid_regions = optimizer.get_all_grid_regions()
                condition_pairs = self.config_data.backtester_config.get("condition_pairs", [])
                
                # DEBUG: 記錄所有 grid_regions
                self.logger.info(
                    f"[DEBUG] 窗口 {current} 目標 {objective}: "
                    f"找到 {len(all_grid_regions)} 個 grid_regions, "
                    f"strategy_idx: {list(all_grid_regions.keys())}"
                )
                
                # 為每個 condition_pair 分別處理
                all_condition_pair_results = {}
                all_condition_pair_test_results = {}
                
                for strategy_idx, pair in enumerate(condition_pairs):
                    self.logger.info(
                        f"[DEBUG] 處理 condition_pair {strategy_idx + 1} "
                        f"({pair.get('entry', [])} + {pair.get('exit', [])}) 的 OOS 測試"
                    )
                    
                    # 獲取該 condition_pair 的 grid_region
                    grid_region = optimizer.get_last_grid_region(strategy_idx=strategy_idx)
                    
                    if not grid_region:
                        self.logger.warning(
                            f"[DEBUG] condition_pair {strategy_idx + 1} ({pair.get('entry', [])} + {pair.get('exit', [])}) "
                            f"沒有 grid_region，跳過 OOS 測試"
                        )
                        continue
                    
                    self.logger.info(
                        f"[DEBUG] condition_pair {strategy_idx + 1} 找到 grid_region, "
                        f"參數組合數: {len(grid_region.get('all_params', []))}"
                    )
                    
                    # 提取該 condition_pair 的參數（從 optimal_params 中過濾）
                    # 但需要包含所有 condition_pair 的參數，因為 _run_grid_test_backtest 需要完整的配置
                    # 實際上，我們應該為每個 condition_pair 分別構建回測配置
                    # 但為了簡化，我們先使用完整的 optimal_params，然後在回測時只使用該 condition_pair 的參數
                    
                    # 步驟 2: 使用九宮格參數在測試集上回測（計算平均表現）
                    # 注意：grid_region 只包含該 condition_pair 的參數，所以回測時只會使用該 condition_pair
                    self.logger.info(
                        f"[DEBUG] 開始執行 condition_pair {strategy_idx + 1} 的 OOS 測試"
                    )
                    
                    test_result = self._run_grid_test_backtest(
                        window["test_data"], grid_region, optimal_params, objective, silent=True
                    )
                    
                    if test_result:
                        self.logger.info(
                            f"[DEBUG] condition_pair {strategy_idx + 1} OOS 測試成功, "
                            f"metric: {test_result.get('metrics', {}).get(objective, 'N/A')}"
                        )
                        
                        # 提取該 condition_pair 的參數（從 optimal_params 中過濾）
                        pair_params = {}
                        strategy_idx_1based = strategy_idx + 1
                        for key, value in optimal_params.items():
                            if f"_strategy_{strategy_idx_1based}" in key:
                                pair_params[key] = value
                        
                        self.logger.info(
                            f"[DEBUG] condition_pair {strategy_idx + 1} 提取的參數鍵: {list(pair_params.keys())}"
                        )
                        
                        all_condition_pair_results[strategy_idx] = {
                            "grid_region": grid_region,
                            "optimal_params": pair_params,
                            "test_result": test_result,
                        }
                        all_condition_pair_test_results[strategy_idx] = test_result
                    else:
                        self.logger.warning(
                            f"[DEBUG] condition_pair {strategy_idx + 1} OOS 測試失敗"
                        )
                
                # 合併所有 condition_pair 的結果
                # 計算平均績效（所有 condition_pair 的平均）
                self.logger.info(
                    f"[DEBUG] 合併結果: 找到 {len(all_condition_pair_test_results)} 個 condition_pair 的測試結果, "
                    f"strategy_idx: {list(all_condition_pair_test_results.keys())}"
                )
                
                if all_condition_pair_test_results:
                    all_metrics = []
                    all_returns = []
                    for strategy_idx_result, test_result in all_condition_pair_test_results.items():
                        metrics = test_result.get("metrics", {})
                        if metrics:
                            metric_value = metrics.get(objective)
                            return_value = metrics.get("total_return")
                            self.logger.info(
                                f"[DEBUG] condition_pair {strategy_idx_result + 1} 的結果: "
                                f"{objective}={metric_value}, return={return_value}"
                            )
                            all_metrics.append(metric_value)
                            all_returns.append(return_value)
                    
                    avg_metric = sum(all_metrics) / len(all_metrics) if all_metrics else None
                    avg_return = sum(all_returns) / len(all_returns) if all_returns else None
                    
                    self.logger.info(
                        f"[DEBUG] 平均績效: {objective}={avg_metric}, return={avg_return}"
                    )
                    
                    # 記錄訓練集（IS）績效（使用第一個 condition_pair 的 train_metrics）
                    if train_metrics:
                        status[f"{objective}_is"] = train_metrics.get(objective)
                        status[f"{objective}_is_return"] = train_metrics.get("total_return")
                        # 記錄 IS 的 MDD
                        status["is_mdd"] = train_metrics.get("max_drawdown")
                        # 同時記錄其他指標（如果有的話）
                        if objective == "sharpe" and "calmar" in train_metrics:
                            status["calmar_is"] = train_metrics.get("calmar")
                            status["calmar_is_return"] = train_metrics.get("total_return")
                        elif objective == "calmar" and "sharpe" in train_metrics:
                            status["sharpe_is"] = train_metrics.get("sharpe")
                            status["sharpe_is_return"] = train_metrics.get("total_return")
                    
                    # 使用第一個 condition_pair 的 grid_region 用於顯示
                    first_strategy_idx = min(all_condition_pair_results.keys())
                    first_grid_region = all_condition_pair_results[first_strategy_idx]["grid_region"]
                    if first_grid_region:
                        status[f"{objective}_grid_params"] = first_grid_region.get("all_params")
                        status[f"{objective}_grid_avg_metric"] = first_grid_region.get("avg_metric")
                        all_params = first_grid_region.get("all_params", [])
                        if all_params:
                            status[f"{objective}_display_params"] = all_params[0]
                    
                    status[f"{objective}_status"] = "成功"
                    status[f"{objective}_metric"] = avg_metric
                    status[f"{objective}_oos"] = avg_metric  # OOS 績效（平均）
                    status[f"{objective}_oos_return"] = avg_return  # OOS 回報（平均）
                    
                    # 合併所有 condition_pair 的測試結果
                    combined_test_result = {
                        "metrics": {
                            objective: avg_metric,
                            "total_return": avg_return,
                        },
                        "individual_results": [],  # 包含所有 condition_pair 的結果
                        "all_condition_pair_results": all_condition_pair_results,  # 保存所有 condition_pair 的詳細結果
                    }
                    
                    window_results[objective] = {
                        "optimal_params": optimal_params,  # 包含所有 condition_pair 的參數
                        "grid_region": first_grid_region,  # 第一個 condition_pair 的 grid_region（用於向後兼容）
                        "all_grid_regions": all_grid_regions,  # 所有 condition_pair 的 grid_regions
                        "train_metrics": train_metrics,  # IS 績效
                        "test_result": combined_test_result,  # OOS 績效（合併後的）
                        "window_info": {
                            "window_id": window["window_id"],
                            "train_start": window["train_start"],
                            "train_end": window["train_end"],
                            "test_start": window["test_start"],
                            "test_end": window["test_end"],
                        },
                    }
                else:
                    status[f"{objective}_status"] = "失敗"
                    status[f"{objective}_failure_reason"] = "所有 condition_pairs 的測試回測都失敗"

            return (window_results if window_results else None, status)

        except Exception as e:
            self.logger.error(f"處理窗口 {current} 失敗: {e}")
            status["error"] = str(e)
            return (None, status)

    def _run_grid_test_backtest(
        self,
        test_data: pd.DataFrame,
        grid_region: Optional[Dict[str, Any]],
        fallback_params: Dict[str, Any],
        objective: str,
        silent: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """
        在測試集上使用九宮格參數執行回測，計算平均表現

        Args:
            test_data: 測試集數據
            grid_region: 九宮格區域信息（包含所有9個參數組合）
            fallback_params: 回退參數（如果九宮格失敗時使用）
            objective: 優化目標
            silent: 是否靜默模式

        Returns:
            Optional[Dict[str, Any]]: 回測結果（包含平均績效）
        """
        try:
            from backtester.VectorBacktestEngine_backtester import VectorBacktestEngine
            from metricstracker.MetricsCalculator_metricstracker import (
                MetricsCalculatorMetricTracker,
            )
            import numpy as np
            import io
            import logging
            from contextlib import redirect_stdout, redirect_stderr

            # 如果沒有九宮格區域，使用單一參數
            if not grid_region or "all_params" not in grid_region:
                return self._run_single_test_backtest(test_data, fallback_params, objective, silent)

            all_params = grid_region["all_params"]
            all_metrics = []
            all_equity_curves = []
            all_returns = []
            all_individual_results = []  # 保存每個參數組合的完整結果（包括失敗的）
            valid_count = 0

            for param_idx, params in enumerate(all_params):
                # 構建回測配置
                backtest_config = self._build_backtest_config(params)

                # 執行回測
                engine = VectorBacktestEngine(
                    test_data, self.frequency, self.logger, symbol=getattr(self.config_data, "symbol", "X")
                )

                # 在靜默模式下抑制輸出
                if silent:
                    old_level = logging.getLogger().level
                    logging.getLogger().setLevel(logging.ERROR)
                    try:
                        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                            results = engine.run_backtests(backtest_config)
                    finally:
                        logging.getLogger().setLevel(old_level)
                else:
                    results = engine.run_backtests(backtest_config)

                if not results or len(results) == 0:
                    continue

                # 使用第一個結果
                result = results[0]
                
                # 檢查是否有錯誤
                if result.get("error") is not None:
                    continue
                
                # 檢查是否有交易記錄
                if "records" not in result:
                    continue
                
                records = result["records"]
                if not isinstance(records, pd.DataFrame) or records.empty:
                    continue
                
                # 檢查是否有實際交易
                if "Trade_action" in records.columns:
                    trade_count = (records["Trade_action"] == 1).sum()
                    if trade_count == 0:
                        continue

                # 計算績效指標
                metrics_calc = MetricsCalculatorMetricTracker(
                    records,
                    time_unit=365,
                    risk_free_rate=0.04,
                )

                # 計算所有指標（sharpe、calmar、sortino、total_return、max_drawdown）
                sharpe_value = metrics_calc.sharpe()
                calmar_value = metrics_calc.calmar()
                sortino_value = metrics_calc.sortino()
                total_return = metrics_calc.total_return()
                max_drawdown = metrics_calc.max_drawdown()
                
                # 獲取優化目標的 metric 值
                metric_value = sharpe_value if objective == "sharpe" else calmar_value
                
                # 保存每個參數組合的結果（無論成功或失敗）
                oos_return = None
                oos_metric = None
                oos_sharpe = None
                oos_calmar = None
                oos_sortino = None
                oos_mdd = None
                
                if not pd.isna(metric_value) and metric_value != float("inf") and metric_value != float("-inf"):
                    all_metrics.append(metric_value)
                    valid_count += 1
                    oos_metric = metric_value
                    
                    if not pd.isna(total_return) and total_return != float("inf") and total_return != float("-inf"):
                        all_returns.append(total_return)
                        oos_return = total_return
                    
                    # 保存所有指標值（即使為 None 也要記錄）
                    if not pd.isna(sharpe_value) and sharpe_value != float("inf") and sharpe_value != float("-inf"):
                        oos_sharpe = sharpe_value
                    if not pd.isna(calmar_value) and calmar_value != float("inf") and calmar_value != float("-inf"):
                        oos_calmar = calmar_value
                    if not pd.isna(sortino_value) and sortino_value != float("inf") and sortino_value != float("-inf"):
                        oos_sortino = sortino_value
                    if not pd.isna(max_drawdown) and max_drawdown != float("inf") and max_drawdown != float("-inf"):
                        oos_mdd = max_drawdown
                    
                    # 保存 equity curve
                    if "Equity_value" in records.columns:
                        all_equity_curves.append(records["Equity_value"].values)
                
                # 保存每個參數組合的單獨結果（包括失敗的）
                all_individual_results.append({
                    "param_index": param_idx,
                    "params": params,
                    "metric": oos_metric,
                    "return": oos_return,
                    "sharpe": oos_sharpe,
                    "calmar": oos_calmar,
                    "sortino": oos_sortino,
                    "max_drawdown": oos_mdd,
                    "success": oos_metric is not None,
                })

            if not all_metrics:
                # 如果所有參數組合都失敗，回退到單一參數
                return self._run_single_test_backtest(test_data, fallback_params, objective, silent)

            # 計算平均績效
            avg_metric = sum(all_metrics) / len(all_metrics)
            avg_return = sum(all_returns) / len(all_returns) if all_returns else None

            # 計算所有指標的平均值
            all_sharpes = [r.get("sharpe") for r in all_individual_results if r.get("sharpe") is not None]
            all_calmars = [r.get("calmar") for r in all_individual_results if r.get("calmar") is not None]
            all_sortinos = [r.get("sortino") for r in all_individual_results if r.get("sortino") is not None]
            all_mdds = [r.get("max_drawdown") for r in all_individual_results if r.get("max_drawdown") is not None]
            
            avg_sharpe = sum(all_sharpes) / len(all_sharpes) if all_sharpes else None
            avg_calmar = sum(all_calmars) / len(all_calmars) if all_calmars else None
            avg_sortino = sum(all_sortinos) / len(all_sortinos) if all_sortinos else None
            avg_mdd = sum(all_mdds) / len(all_mdds) if all_mdds else None

            # 計算平均 equity curve
            avg_equity = None
            if all_equity_curves:
                # 找到最短長度
                min_length = min(len(eq) for eq in all_equity_curves)
                # 截斷所有 equity curve 到相同長度
                truncated_curves = [eq[:min_length] for eq in all_equity_curves]
                # 計算平均
                avg_equity = np.mean(truncated_curves, axis=0)

            metrics = {
                objective: avg_metric,
                "sharpe": avg_sharpe,
                "calmar": avg_calmar,
                "sortino": avg_sortino,
                "total_return": avg_return,
                "max_drawdown": avg_mdd,
                "param_count": valid_count,
            }

            return {
                "backtest_result": None,  # 使用平均 equity curve 而不是單一記錄
                "equity_curve": avg_equity,
                "metrics": metrics,  # 平均績效
                "all_metrics": all_metrics,  # 保存所有參數的績效，用於分析
                "individual_results": all_individual_results,  # 每個參數組合的單獨結果
            }

        except Exception as e:
            self.logger.error(f"九宮格測試集回測失敗: {e}")
            # 回退到單一參數
            return self._run_single_test_backtest(test_data, fallback_params, objective, silent)

    def _run_single_test_backtest(
        self, test_data: pd.DataFrame, optimal_params: Dict[str, Any], objective: str, silent: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        在測試集上執行單一參數的回測（回退方法）

        Args:
            test_data: 測試集數據
            optimal_params: 最優參數
            objective: 優化目標
            silent: 是否靜默模式

        Returns:
            Optional[Dict[str, Any]]: 回測結果
        """
        try:
            from backtester.VectorBacktestEngine_backtester import VectorBacktestEngine

            # 構建回測配置（使用最優參數）
            backtest_config = self._build_backtest_config(optimal_params)

            # 執行回測
            engine = VectorBacktestEngine(
                test_data, self.frequency, self.logger, symbol=getattr(self.config_data, "symbol", "X")
            )

            # 在靜默模式下抑制輸出
            if silent:
                import io
                import logging
                from contextlib import redirect_stdout, redirect_stderr

                # 抑制 stdout、stderr 和 logging
                old_level = logging.getLogger().level
                logging.getLogger().setLevel(logging.ERROR)
                try:
                    with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                        results = engine.run_backtests(backtest_config)
                finally:
                    logging.getLogger().setLevel(old_level)
            else:
                results = engine.run_backtests(backtest_config)

            if results and len(results) > 0:
                # 計算績效指標
                from metricstracker.MetricsCalculator_metricstracker import (
                    MetricsCalculatorMetricTracker,
                )

                # 使用第一個結果（應該只有一個，因為參數已固定）
                result = results[0]
                
                # 檢查是否有錯誤
                if result.get("error") is not None:
                    return None
                
                # 檢查是否有交易記錄
                if "records" not in result:
                    return None
                
                records = result["records"]
                if not isinstance(records, pd.DataFrame) or records.empty:
                    return None
                
                # 檢查是否有實際交易
                if "Trade_action" in records.columns:
                    trade_count = (records["Trade_action"] == 1).sum()
                    if trade_count == 0:
                        return None
                
                metrics_calc = MetricsCalculatorMetricTracker(
                    records,
                    time_unit=365,
                    risk_free_rate=0.04,
                )

                metrics = {
                    "sharpe": metrics_calc.sharpe(),
                    "calmar": metrics_calc.calmar(),
                    "sortino": metrics_calc.sortino(),
                    "total_return": metrics_calc.total_return(),
                    "max_drawdown": metrics_calc.max_drawdown(),
                }

                # 獲取 equity curve
                equity_curve = None
                if "Equity_value" in records.columns:
                    equity_curve = records["Equity_value"].values

                return {
                    "backtest_result": result,
                    "equity_curve": equity_curve,
                    "metrics": metrics,
                }

            return None

        except Exception as e:
            self.logger.error(f"單一參數測試集回測失敗: {e}")
            return None

    def _build_backtest_config(self, optimal_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        構建回測配置（使用最優參數）

        Args:
            optimal_params: 最優參數

        Returns:
            Dict[str, Any]: 回測配置
        """
        # 從原始配置構建，但使用最優參數
        backtest_config = {
            "condition_pairs": self.config_data.backtester_config.get(
                "condition_pairs", []
            ),
            "indicator_params": optimal_params,  # 使用最優參數
            "predictors": [
                self.config_data.backtester_config.get("selected_predictor", "X")
            ],
            "trading_params": self.config_data.backtester_config.get(
                "trading_params", {}
            ),
        }

        return backtest_config

    def _collect_results(
        self, wfa_results: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        收集 WFA 結果

        Args:
            wfa_results: 各窗口的結果

        Returns:
            Dict[str, Any]: 最終 WFA 結果
        """
        final_results = {
            "wfa_config": self.wfa_config,
            "total_windows": len(wfa_results),
            "results_by_objective": {},
            "data": self.data,  # 保存數據引用，供 ResultsExporter 使用
        }

        # 按優化目標分組結果
        objectives = self.wfa_config.get("optimization_objectives", ["sharpe", "calmar"])

        for objective in objectives:
            objective_results = []
            for window_result in wfa_results:
                if objective in window_result:
                    objective_results.append(window_result[objective])

            final_results["results_by_objective"][objective] = objective_results

        return final_results

    def _display_results_summary(
        self, window_status: List[Dict[str, Any]], wfa_results: List[Dict[str, Any]]
    ) -> None:
        """
        顯示結果摘要表格

        Args:
            window_status: 各窗口的狀態信息
            wfa_results: WFA 結果列表
        """
        from rich.table import Table

        table = Table(
            title="📊 WFA 執行結果摘要",
            show_lines=True,
            border_style="#dbac30",
        )
        table.add_column("窗口", style="cyan", no_wrap=True)
        table.add_column("日期範圍", style="white", no_wrap=False)
        table.add_column("訓練集大小", style="white")
        table.add_column("測試集大小", style="white")
        table.add_column("Sharpe 狀態", style="white")
        table.add_column("Sharpe IS", style="#1e90ff")
        table.add_column("Sharpe OOS", style="#1e90ff")
        table.add_column("Sharpe 最佳參數", style="yellow")
        table.add_column("IS Return%", style="#1e90ff")
        table.add_column("OOS Return%", style="#1e90ff")
        table.add_column("Calmar 狀態", style="white")
        table.add_column("Calmar IS", style="#1e90ff")
        table.add_column("Calmar OOS", style="#1e90ff")
        table.add_column("Calmar 最佳參數", style="yellow")
        table.add_column("Calmar IS Return%", style="#1e90ff")
        table.add_column("Calmar OOS Return%", style="#1e90ff")

        # 創建窗口結果映射，用於獲取參數信息
        window_result_map = {}
        for window_result in wfa_results:
            for objective in ["sharpe", "calmar"]:
                if objective in window_result:
                    window_id = window_result[objective].get("window_info", {}).get("window_id")
                    if window_id:
                        if window_id not in window_result_map:
                            window_result_map[window_id] = {}
                        window_result_map[window_id][objective] = window_result[objective]

        for status in window_status:
            window_id = status.get("window_id", "N/A")
            train_size = status.get("train_size", 0)
            test_size = status.get("test_size", 0)
            
            # 提取日期範圍
            date_range_str = self._get_date_range_for_window(window_id, window_result_map)
            sharpe_status = status.get("sharpe_status", "未執行")
            sharpe_is = status.get("sharpe_is")
            sharpe_oos = status.get("sharpe_oos") or status.get("sharpe_metric")
            sharpe_is_return = status.get("sharpe_is_return")
            sharpe_oos_return = status.get("sharpe_oos_return")
            sharpe_failure_reason = status.get("sharpe_failure_reason")
            
            calmar_status = status.get("calmar_status", "未執行")
            calmar_is = status.get("calmar_is")
            calmar_oos = status.get("calmar_oos") or status.get("calmar_metric")
            calmar_is_return = status.get("calmar_is_return")
            calmar_oos_return = status.get("calmar_oos_return")
            
            calmar_is_return_value = (
                f"[#1e90ff]{calmar_is_return*100:.2f}%[/#1e90ff]"
                if calmar_is_return is not None
                else "N/A"
            )
            calmar_oos_return_value = (
                f"[#1e90ff]{calmar_oos_return*100:.2f}%[/#1e90ff]"
                if calmar_oos_return is not None
                else "N/A"
            )
            calmar_failure_reason = status.get("calmar_failure_reason")
            
            # 提取最佳參數（優先從 status 中獲取，如果沒有則從 window_result_map 獲取）
            sharpe_display_params = status.get("sharpe_display_params")
            if not sharpe_display_params:
                sharpe_result = window_result_map.get(window_id, {}).get("sharpe")
                sharpe_params_str = self._extract_params_for_display(sharpe_result)
            else:
                sharpe_params_str = self._format_params_dict_simple(sharpe_display_params)
            
            calmar_display_params = status.get("calmar_display_params")
            if not calmar_display_params:
                calmar_result = window_result_map.get(window_id, {}).get("calmar")
                calmar_params_str = self._extract_params_for_display(calmar_result)
            else:
                calmar_params_str = self._format_params_dict_simple(calmar_display_params)

            # 格式化狀態顯示
            if sharpe_status == "成功":
                sharpe_status_display = f"[green]✅ {sharpe_status}[/green]"
            elif sharpe_status == "失敗":
                # 顯示失敗原因
                reason = sharpe_failure_reason or "未知原因"
                # 截斷過長的失敗原因
                if len(reason) > 30:
                    reason = reason[:27] + "..."
                sharpe_status_display = f"[red]❌ 失敗 ({reason})[/red]"
            else:
                sharpe_status_display = f"[yellow]⚠️ {sharpe_status}[/yellow]"

            if calmar_status == "成功":
                calmar_status_display = f"[green]✅ {calmar_status}[/green]"
            elif calmar_status == "失敗":
                # 顯示失敗原因
                reason = calmar_failure_reason or "未知原因"
                # 截斷過長的失敗原因
                if len(reason) > 30:
                    reason = reason[:27] + "..."
                calmar_status_display = f"[red]❌ 失敗 ({reason})[/red]"
            else:
                calmar_status_display = f"[yellow]⚠️ {calmar_status}[/yellow]"

            # 格式化指標值
            sharpe_is_value = (
                f"[#1e90ff]{sharpe_is:.4f}[/#1e90ff]"
                if sharpe_is is not None
                else "N/A"
            )
            sharpe_oos_value = (
                f"[#1e90ff]{sharpe_oos:.4f}[/#1e90ff]"
                if sharpe_oos is not None
                else "N/A"
            )
            sharpe_is_return_value = (
                f"[#1e90ff]{sharpe_is_return*100:.2f}%[/#1e90ff]"
                if sharpe_is_return is not None
                else "N/A"
            )
            sharpe_oos_return_value = (
                f"[#1e90ff]{sharpe_oos_return*100:.2f}%[/#1e90ff]"
                if sharpe_oos_return is not None
                else "N/A"
            )
            calmar_is_value = (
                f"[#1e90ff]{calmar_is:.4f}[/#1e90ff]"
                if calmar_is is not None
                else "N/A"
            )
            calmar_oos_value = (
                f"[#1e90ff]{calmar_oos:.4f}[/#1e90ff]"
                if calmar_oos is not None
                else "N/A"
            )

            table.add_row(
                str(window_id),
                date_range_str,
                str(train_size),
                str(test_size),
                sharpe_status_display,
                sharpe_is_value,
                sharpe_oos_value,
                sharpe_params_str,
                sharpe_is_return_value,
                sharpe_oos_return_value,
                calmar_status_display,
                    calmar_is_value,
                    calmar_oos_value,
                    calmar_params_str,
                    calmar_is_return_value,
                    calmar_oos_return_value,
                )

        console.print(table)

        # 統計信息
        total_windows = len(window_status)
        sharpe_success = sum(
            1 for s in window_status if s.get("sharpe_status") == "成功"
        )
        calmar_success = sum(
            1 for s in window_status if s.get("calmar_status") == "成功"
        )

        show_info("WFANALYSER",
            f"📊 統計信息:\n"
            f"   總窗口數: {total_windows}\n"
            f"   Sharpe 成功: {sharpe_success}/{total_windows}\n"
            f"   Calmar 成功: {calmar_success}/{total_windows}"
        )

    def _extract_params_for_display(self, window_result: Optional[Dict[str, Any]]) -> str:
        """
        從窗口結果中提取參數用於顯示

        Args:
            window_result: 窗口結果字典

        Returns:
            str: 格式化的參數字符串
        """
        if not window_result:
            return "N/A"
        
        try:
            grid_region = window_result.get("grid_region", {})
            all_params = grid_region.get("all_params", [])
            
            if not all_params:
                # 回退到單一參數
                optimal_params = window_result.get("optimal_params", {})
                if optimal_params:
                    return self._format_params_dict_simple(optimal_params)
                return "N/A"
            
            # 使用第一個參數組合作為代表（或可以選擇平均最高的）
            first_params = all_params[0] if all_params else {}
            return self._format_params_dict_simple(first_params)
            
        except Exception as e:
            self.logger.warning(f"提取參數用於顯示失敗: {e}")
            return "N/A"
    
    def _format_params_dict_simple(self, params: Dict[str, Any]) -> str:
        """
        簡單格式化參數字典為字符串

        Args:
            params: 參數字典

        Returns:
            str: 格式化的字符串
        """
        try:
            from wfanalyser.ResultsExporter_wfanalyser import ResultsExporter
            
            # 使用 ResultsExporter 的參數提取邏輯
            from pathlib import Path
            exporter = ResultsExporter({}, Path("records/wfanalyser"), self.logger)
            param_dict = exporter._extract_params_dict(params)
            formatted = exporter._format_params_dict(param_dict)
            
            # 簡化顯示，只顯示參數值（去掉大括號和引號）
            if formatted and formatted != "{}":
                # 提取參數值，格式如: MA1:40, MA4:100
                parts = []
                for key, value in param_dict.items():
                    parts.append(f"{key}:{value}")
                return ", ".join(parts) if parts else "N/A"
            
            return "N/A"
            
        except Exception as e:
            self.logger.warning(f"格式化參數失敗: {e}")
            return "N/A"

    def _get_date_range_for_window(self, window_id: int, window_result_map: Optional[Dict[str, Any]] = None) -> str:
        """
        獲取窗口的日期範圍

        Args:
            window_id: 窗口ID
            window_result_map: 窗口結果映射（未使用，保留以保持接口兼容性）

        Returns:
            str: 格式化的日期範圍字符串
        """
        try:
            # 必須從保存的窗口劃分信息中獲取
            train_start = None
            test_end = None
            
            # 從保存的窗口劃分信息中查找
            if not hasattr(self, 'windows') or not self.windows:
                self.logger.warning(f"窗口 {window_id}: self.windows 不存在或為空")
                return "N/A"
            
            for window in self.windows:
                if window.get("window_id") == window_id:
                    train_start = window.get("train_start")
                    test_end = window.get("test_end")
                    break
            
            if train_start is None or test_end is None:
                self.logger.warning(f"窗口 {window_id}: 無法從 self.windows 中找到窗口信息")
                return "N/A"
            
            if self.data is None:
                self.logger.warning(f"窗口 {window_id}: self.data 為 None")
                return "N/A"
            
            # 嘗試從數據中獲取日期列（優先順序：Time > time > Date > date > datetime > DateTime）
            date_column = None
            for col in ["Time", "time", "Date", "date", "datetime", "DateTime"]:
                if col in self.data.columns:
                    date_column = col
                    break
            
            if date_column is None:
                # 如果沒有日期列，返回索引範圍
                return f"索引 {train_start}-{test_end-1}"
            
            # 獲取日期
            train_start_date = self.data.iloc[train_start][date_column]
            test_end_date = self.data.iloc[test_end - 1][date_column]  # test_end 是 exclusive，所以減1
            
            # 格式化日期
            if isinstance(train_start_date, pd.Timestamp):
                train_date_str = train_start_date.strftime("%Y-%m-%d")
            elif hasattr(train_start_date, 'strftime'):
                train_date_str = train_start_date.strftime("%Y-%m-%d")
            else:
                train_date_str = str(train_start_date)
            
            if isinstance(test_end_date, pd.Timestamp):
                test_date_str = test_end_date.strftime("%Y-%m-%d")
            elif hasattr(test_end_date, 'strftime'):
                test_date_str = test_end_date.strftime("%Y-%m-%d")
            else:
                test_date_str = str(test_end_date)
            
            return f"{train_date_str}\n至 {test_date_str}"
            
        except Exception as e:
            self.logger.warning(f"獲取日期範圍失敗: {e}")
            return "N/A"
    
    def _get_indicator_configs(self) -> List[Dict[str, Any]]:
        """
        獲取所有指標配置列表（支持舊格式字典和新格式列表）
        
        Returns:
            List[Dict[str, Any]]: 指標配置列表
        """
        indicator_configs = self.config_data.backtester_config.get("indicator_params", [])
        
        # 如果 indicator_params 是字典（舊格式），轉換為列表
        if isinstance(indicator_configs, dict):
            indicator_configs = [indicator_configs]
        
        if not indicator_configs:
            raise ValueError("未找到任何指標配置，請檢查 backtester.indicator_params 配置")
        
        return indicator_configs
    
    def _create_temp_config_data(
        self, indicator_params_config: Dict[str, Any], condition_pair_idx: int = 0
    ) -> Any:
        """
        為單一指標配置創建臨時的 config_data
        
        Args:
            indicator_params_config: 單一指標配置字典
            condition_pair_idx: 對應的 condition_pair 索引（從0開始）
            
        Returns:
            Any: 臨時的 config_data 對象（具有相同的接口）
        """
        # 創建一個臨時對象，複製原始 config_data 但替換 indicator_params 和 condition_pairs
        class TempConfigData:
            def __init__(self, original_config, indicator_params, pair_idx):
                self.wfa_config = original_config.wfa_config
                self.dataloader_config = original_config.dataloader_config
                self.predictor_config = original_config.predictor_config
                self.metricstracker_config = original_config.metricstracker_config
                
                # 創建新的 backtester_config
                self.backtester_config = original_config.backtester_config.copy()
                
                # 替換 indicator_params
                self.backtester_config["indicator_params"] = indicator_params
                
                # 只使用對應的 condition_pair
                all_condition_pairs = original_config.backtester_config.get("condition_pairs", [])
                if pair_idx < len(all_condition_pairs):
                    # 只使用對應的 condition_pair
                    self.backtester_config["condition_pairs"] = [all_condition_pairs[pair_idx]]
                else:
                    # 如果索引超出範圍，使用第一個（向後兼容）
                    self.logger.warning(
                        f"condition_pair_idx {pair_idx} 超出範圍，使用第一個 condition_pair"
                    )
                    self.backtester_config["condition_pairs"] = (
                        all_condition_pairs[:1] if all_condition_pairs else []
                    )
                
                # 保留其他屬性
                for attr in ["symbol", "file_name"]:
                    if hasattr(original_config, attr):
                        setattr(self, attr, getattr(original_config, attr))
        
        return TempConfigData(self.config_data, indicator_params_config, condition_pair_idx)
    
    def _process_single_config_window(
        self,
        window: Dict[str, Any],
        current: int,
        total: int,
        temp_config_data: Any,
        config_id: str,
        indicator_params_config: Dict[str, Any],
    ) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
        """
        處理單一配置 + 單一窗口（方案 B）
        
        Args:
            window: 窗口數據
            current: 當前窗口編號
            total: 總窗口數
            temp_config_data: 臨時的配置數據（包含單一指標配置）
            config_id: 配置ID（例如 "config_1"）
            indicator_params_config: 指標參數配置字典
            
        Returns:
            Tuple[Optional[Dict[str, Any]], Dict[str, Any]]: (窗口處理結果, 狀態信息)
        """
        status = {
            "window_id": window["window_id"],
            "config_id": config_id,
            "train_size": len(window["train_data"]),
            "test_size": len(window["test_data"]),
            "sharpe_status": "未執行",
            "calmar_status": "未執行",
            "sharpe_metric": None,
            "calmar_metric": None,
            "sharpe_is": None,
            "sharpe_oos": None,
            "sharpe_is_return": None,
            "sharpe_oos_return": None,
            "calmar_is": None,
            "calmar_oos": None,
            "calmar_is_return": None,
            "calmar_oos_return": None,
            "sharpe_failure_reason": None,
            "calmar_failure_reason": None,
        }
        
        try:
            # 步驟 1: 參數優化（在訓練集上）
            optimizer = ParameterOptimizer(
                window["train_data"],
                self.frequency,
                temp_config_data,  # 使用臨時配置數據
                logger=self.logger,
            )
            
            optimization_objectives = self.wfa_config.get(
                "optimization_objectives", ["sharpe", "calmar"]
            )
            
            window_results = {}
            
            for objective in optimization_objectives:
                # 執行參數優化（靜默模式），同時獲取訓練集績效
                optimal_params, train_metrics = optimizer.optimize_with_is_metrics(
                    objective, silent=True
                )
                
                if optimal_params is None:
                    status[f"{objective}_status"] = "失敗"
                    failure_reason = optimizer.get_last_failure_reason()
                    status[f"{objective}_failure_reason"] = failure_reason or "未知原因"
                    continue
                
                # 記錄訓練集（IS）績效
                if train_metrics:
                    status[f"{objective}_is"] = train_metrics.get(objective)
                    status[f"{objective}_is_return"] = train_metrics.get("total_return")
                    # 記錄 IS 的 MDD
                    status["is_mdd"] = train_metrics.get("max_drawdown")
                    if objective == "sharpe" and "calmar" in train_metrics:
                        status["calmar_is"] = train_metrics.get("calmar")
                        status["calmar_is_return"] = train_metrics.get("total_return")
                    elif objective == "calmar" and "sharpe" in train_metrics:
                        status["sharpe_is"] = train_metrics.get("sharpe")
                        status["sharpe_is_return"] = train_metrics.get("total_return")
                
                # 獲取九宮格區域信息
                grid_region = optimizer.get_last_grid_region()
                if grid_region:
                    status[f"{objective}_grid_params"] = grid_region.get("all_params")
                    status[f"{objective}_grid_avg_metric"] = grid_region.get("avg_metric")
                    all_params = grid_region.get("all_params", [])
                    if all_params:
                        status[f"{objective}_display_params"] = all_params[0]
                
                # 步驟 2: 使用九宮格參數在測試集上回測（計算平均表現）
                test_result = self._run_grid_test_backtest(
                    window["test_data"], grid_region, optimal_params, objective, silent=True
                )
                
                if test_result:
                    metrics = test_result.get("metrics", {})
                    status[f"{objective}_status"] = "成功"
                    status[f"{objective}_metric"] = metrics.get(objective)
                    status[f"{objective}_oos"] = metrics.get(objective)
                    status[f"{objective}_oos_return"] = metrics.get("total_return")
                    
                    if status.get(f"{objective}_is_return") is None and train_metrics:
                        status[f"{objective}_is_return"] = train_metrics.get("total_return")
                    
                    window_results[objective] = {
                        "indicator_config_id": config_id,  # 標記配置ID
                        "indicator_params": indicator_params_config,
                        "optimal_params": optimal_params,
                        "grid_region": grid_region,
                        "train_metrics": train_metrics,
                        "test_result": test_result,
                        "window_info": {
                            "window_id": window["window_id"],
                            "train_start": window["train_start"],
                            "train_end": window["train_end"],
                            "test_start": window["test_start"],
                            "test_end": window["test_end"],
                        },
                    }
                else:
                    status[f"{objective}_status"] = "失敗"
                    status[f"{objective}_failure_reason"] = "測試回測失敗：無有效結果或無交易"
            
            return (window_results if window_results else None, status)
            
        except Exception as e:
            self.logger.error(f"處理配置 {config_id} 窗口 {current} 失敗: {e}")
            status["error"] = str(e)
            return (None, status)
    
    def _collect_results_by_config(
        self,
        all_config_results: Dict[str, Any],
        windows: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        按配置收集 WFA 結果（方案 B）
        
        Args:
            all_config_results: 按配置分組的結果 {config_id: {window_results, ...}}
            windows: 窗口列表
            
        Returns:
            Dict[str, Any]: 最終 WFA 結果
        """
        final_results = {
            "wfa_config": self.wfa_config,
            "total_windows": len(windows),
            "total_configs": len(all_config_results),
            "results_by_objective": {},
            "data": self.data,
        }
        
        # 按優化目標分組結果
        objectives = self.wfa_config.get("optimization_objectives", ["sharpe", "calmar"])
        
        for objective in objectives:
            objective_results = []
            
            # 遍歷所有配置
            for config_id, config_data in all_config_results.items():
                window_results = config_data.get("window_results", [])
                
                # 遍歷該配置的所有窗口結果
                for window_result in window_results:
                    if objective in window_result:
                        # window_result[objective] 已經包含 indicator_config_id
                        objective_results.append(window_result[objective])
            
            final_results["results_by_objective"][objective] = objective_results
        
        return final_results
    
    def _display_results_summary_by_config(
        self,
        all_config_results: Dict[str, Any],
        windows: List[Dict[str, Any]],
    ) -> None:
        """
        顯示結果摘要表格（方案 B：按配置分組）
        
        Args:
            all_config_results: 按配置分組的結果
            windows: 窗口列表
        """
        from rich.table import Table
        
        # 為每個配置創建一個表格
        for config_id, config_data in all_config_results.items():
            window_results = config_data.get("window_results", [])
            window_status = config_data.get("window_status", [])
            
            # 創建窗口結果映射，用於獲取參數信息
            window_result_map = {}
            for window_result in window_results:
                for objective in ["sharpe", "calmar"]:
                    if objective in window_result:
                        window_id = window_result[objective].get("window_info", {}).get("window_id")
                        if window_id:
                            if window_id not in window_result_map:
                                window_result_map[window_id] = {}
                            window_result_map[window_id][objective] = window_result[objective]
            
            table = Table(
                title=f"📊 WFA 執行結果摘要 - {config_id}",
                show_lines=True,
                border_style="#dbac30",
            )
            table.add_column("窗口", style="cyan", no_wrap=True)
            table.add_column("日期範圍", style="white", no_wrap=False)
            table.add_column("訓練集大小", style="white")
            table.add_column("測試集大小", style="white")
            table.add_column("Sharpe 狀態", style="white")
            table.add_column("Sharpe IS", style="#1e90ff")
            table.add_column("Sharpe OOS", style="#1e90ff")
            table.add_column("Sharpe 最佳參數", style="yellow")
            table.add_column("Sharpe IS Return%", style="#1e90ff")
            table.add_column("Sharpe OOS Return%", style="#1e90ff")
            table.add_column("Calmar 狀態", style="white")
            table.add_column("Calmar IS", style="#1e90ff")
            table.add_column("Calmar OOS", style="#1e90ff")
            table.add_column("Calmar 最佳參數", style="yellow")
            table.add_column("Calmar IS Return%", style="#1e90ff")
            table.add_column("Calmar OOS Return%", style="#1e90ff")
            
            for status in window_status:
                window_id = status.get("window_id", "N/A")
                train_size = status.get("train_size", 0)
                test_size = status.get("test_size", 0)
                
                date_range_str = self._get_date_range_for_window(window_id, window_result_map)
                sharpe_status = status.get("sharpe_status", "未執行")
                sharpe_is = status.get("sharpe_is")
                sharpe_oos = status.get("sharpe_oos") or status.get("sharpe_metric")
                sharpe_is_return = status.get("sharpe_is_return")
                sharpe_oos_return = status.get("sharpe_oos_return")
                
                calmar_status = status.get("calmar_status", "未執行")
                calmar_is = status.get("calmar_is")
                calmar_oos = status.get("calmar_oos") or status.get("calmar_metric")
                calmar_is_return = status.get("calmar_is_return")
                calmar_oos_return = status.get("calmar_oos_return")
                
                sharpe_display_params = status.get("sharpe_display_params")
                if not sharpe_display_params:
                    sharpe_result = window_result_map.get(window_id, {}).get("sharpe")
                    sharpe_params_str = self._extract_params_for_display(sharpe_result)
                else:
                    sharpe_params_str = self._format_params_dict_simple(sharpe_display_params)
                
                calmar_display_params = status.get("calmar_display_params")
                if not calmar_display_params:
                    calmar_result = window_result_map.get(window_id, {}).get("calmar")
                    calmar_params_str = self._extract_params_for_display(calmar_result)
                else:
                    calmar_params_str = self._format_params_dict_simple(calmar_display_params)
                
                # 格式化狀態和值
                if sharpe_status == "成功":
                    sharpe_status_display = f"[green]✅ {sharpe_status}[/green]"
                elif sharpe_status == "失敗":
                    reason = status.get("sharpe_failure_reason", "未知原因")
                    if len(reason) > 30:
                        reason = reason[:27] + "..."
                    sharpe_status_display = f"[red]❌ 失敗 ({reason})[/red]"
                else:
                    sharpe_status_display = f"[yellow]⚠️ {sharpe_status}[/yellow]"
                
                if calmar_status == "成功":
                    calmar_status_display = f"[green]✅ {calmar_status}[/green]"
                elif calmar_status == "失敗":
                    reason = status.get("calmar_failure_reason", "未知原因")
                    if len(reason) > 30:
                        reason = reason[:27] + "..."
                    calmar_status_display = f"[red]❌ 失敗 ({reason})[/red]"
                else:
                    calmar_status_display = f"[yellow]⚠️ {calmar_status}[/yellow]"
                
                sharpe_is_value = (
                    f"[#1e90ff]{sharpe_is:.4f}[/#1e90ff]"
                    if sharpe_is is not None
                    else "N/A"
                )
                sharpe_oos_value = (
                    f"[#1e90ff]{sharpe_oos:.4f}[/#1e90ff]"
                    if sharpe_oos is not None
                    else "N/A"
                )
                sharpe_is_return_value = (
                    f"[#1e90ff]{sharpe_is_return*100:.2f}%[/#1e90ff]"
                    if sharpe_is_return is not None
                    else "N/A"
                )
                sharpe_oos_return_value = (
                    f"[#1e90ff]{sharpe_oos_return*100:.2f}%[/#1e90ff]"
                    if sharpe_oos_return is not None
                    else "N/A"
                )
                calmar_is_value = (
                    f"[#1e90ff]{calmar_is:.4f}[/#1e90ff]"
                    if calmar_is is not None
                    else "N/A"
                )
                calmar_oos_value = (
                    f"[#1e90ff]{calmar_oos:.4f}[/#1e90ff]"
                    if calmar_oos is not None
                    else "N/A"
                )
                calmar_is_return_value = (
                    f"[#1e90ff]{calmar_is_return*100:.2f}%[/#1e90ff]"
                    if calmar_is_return is not None
                    else "N/A"
                )
                calmar_oos_return_value = (
                    f"[#1e90ff]{calmar_oos_return*100:.2f}%[/#1e90ff]"
                    if calmar_oos_return is not None
                    else "N/A"
                )
                
                table.add_row(
                    str(window_id),
                    date_range_str,
                    str(train_size),
                    str(test_size),
                    sharpe_status_display,
                    sharpe_is_value,
                    sharpe_oos_value,
                    sharpe_params_str,
                    sharpe_is_return_value,
                    sharpe_oos_return_value,
                    calmar_status_display,
                    calmar_is_value,
                    calmar_oos_value,
                    calmar_params_str,
                    calmar_is_return_value,
                    calmar_oos_return_value,
                )
            
            console.print(table)
            
            # 統計信息
            total_windows = len(window_status)
            sharpe_success = sum(
                1 for s in window_status if s.get("sharpe_status") == "成功"
            )
            calmar_success = sum(
                1 for s in window_status if s.get("calmar_status") == "成功"
            )
            
            show_info("WFANALYSER",
                f"📊 {config_id} 統計信息:\n"
                f"   總窗口數: {total_windows}\n"
                f"   Sharpe 成功: {sharpe_success}/{total_windows}\n"
                f"   Calmar 成功: {calmar_success}/{total_windows}"
            )
    

