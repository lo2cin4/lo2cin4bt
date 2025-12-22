"""
ParameterOptimizer_wfanalyser.py

【功能說明】
------------------------------------------------------------
本模組負責 WFA 的參數優化功能，在訓練集上尋找最優參數，
優化目標可以是 Sharpe 或 Calmar。

【流程與數據流】
------------------------------------------------------------
- 主流程：生成參數組合 → 執行回測 → 計算績效 → 選擇最優參數
- 數據流：訓練數據 → 參數組合 → 回測結果 → 績效指標 → 最優參數

【維護與擴充重點】
------------------------------------------------------------
- 參數組合生成需要與 VectorBacktestEngine 兼容
- 績效計算需要與 metricstracker 兼容
- 優化邏輯需要高效且準確

【常見易錯點】
------------------------------------------------------------
- 參數組合生成錯誤導致優化失敗
- 績效計算錯誤導致選擇錯誤的最優參數
- 記憶體使用過大導致優化失敗

【範例】
------------------------------------------------------------
- 優化參數：optimizer = ParameterOptimizer(train_data, frequency, config); optimal = optimizer.optimize("sharpe")

【與其他模組的關聯】
------------------------------------------------------------
- 調用 VectorBacktestEngine 執行回測
- 調用 MetricsCalculator 計算績效指標
- 依賴配置數據生成參數組合

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本優化功能

【參考】
------------------------------------------------------------
- WalkForwardEngine_wfanalyser.py: WFA 核心引擎
- VectorBacktestEngine_backtester.py: 向量化回測引擎
- MetricsCalculator_metricstracker.py: 績效指標計算器
- wfanalyser/README.md: WFA 模組詳細說明
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


class ParameterOptimizer:
    """
    參數優化器

    在訓練集上尋找最優參數，優化目標可以是 Sharpe 或 Calmar。
    """

    def __init__(
        self,
        train_data: pd.DataFrame,
        frequency: str,
        config_data: Any,
        logger: Optional[logging.Logger] = None,
    ):
        """
        初始化 ParameterOptimizer

        Args:
            train_data: 訓練集數據
            frequency: 數據頻率
            config_data: 配置數據對象
            logger: 日誌記錄器
        """
        self.train_data = train_data
        self.frequency = frequency
        self.config_data = config_data
        self.logger = logger or logging.getLogger("lo2cin4bt.wfanalyser.optimizer")
        self.backtester_config = config_data.backtester_config
        self._last_failure_reason: Optional[str] = None
        self._last_grid_region: Optional[Dict[str, Any]] = None  # 向後兼容
        self._last_grid_regions: Dict[int, Dict[str, Any]] = {}  # 保存所有 condition_pair 的 grid_regions

    def optimize(self, objective: str, silent: bool = True) -> Optional[Dict[str, Any]]:
        """
        執行參數優化（向後兼容方法）

        Args:
            objective: 優化目標（"sharpe" 或 "calmar"）
            silent: 是否靜默模式（不顯示進度條和詳細輸出）

        Returns:
            Optional[Dict[str, Any]]: 最優參數，如果優化失敗則返回 None
        """
        result, _ = self.optimize_with_is_metrics(objective, silent)
        return result

    def optimize_with_is_metrics(
        self, objective: str, silent: bool = True
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        執行參數優化，並返回訓練集績效指標

        Args:
            objective: 優化目標（"sharpe" 或 "calmar"）
            silent: 是否靜默模式（不顯示進度條和詳細輸出）

        Returns:
            Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]: 
            (最優參數, 訓練集績效指標)，如果優化失敗則返回 (None, None)
        """
        self._last_failure_reason = None
        """
        執行參數優化

        Args:
            objective: 優化目標（"sharpe" 或 "calmar"）
            silent: 是否靜默模式（不顯示進度條和詳細輸出）

        Returns:
            Optional[Dict[str, Any]]: 最優參數，如果優化失敗則返回 None
        """
        try:
            # 生成所有參數組合
            from backtester.VectorBacktestEngine_backtester import VectorBacktestEngine

            engine = VectorBacktestEngine(
                self.train_data, self.frequency, self.logger, symbol=getattr(self.config_data, "symbol", "X")
            )

            # 構建回測配置
            backtest_config = self._build_optimization_config()

            # 生成參數組合
            parameter_combinations = engine.generate_parameter_combinations(backtest_config)

            if not parameter_combinations:
                self.logger.warning(f"未生成任何參數組合，配置: {backtest_config}")
                self._last_failure_reason = "未生成任何參數組合"
                return None, None

            # 執行回測（在靜默模式下，需要抑制輸出）
            if silent:
                import io
                from contextlib import redirect_stdout, redirect_stderr
                import logging

                # 同時抑制 stdout 和 stderr
                with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                    # 也嘗試抑制 logging
                    old_level = logging.getLogger().level
                    logging.getLogger().setLevel(logging.ERROR)
                    try:
                        results = engine.run_backtests(backtest_config)
                    finally:
                        logging.getLogger().setLevel(old_level)
            else:
                results = engine.run_backtests(backtest_config)

            if not results:
                self.logger.warning("回測執行返回空結果")
                self._last_failure_reason = "回測執行返回空結果"
                return None, None

            # 診斷：檢查結果狀態
            error_count = sum(1 for r in results if r.get("error") is not None)
            no_trade_count = 0
            valid_count = 0
            
            for r in results:
                if r.get("error") is None:
                    records = r.get("records", pd.DataFrame())
                    if isinstance(records, pd.DataFrame) and not records.empty:
                        if "Trade_action" in records.columns:
                            trade_count = (records["Trade_action"] == 1).sum()
                            if trade_count > 0:
                                valid_count += 1
                            else:
                                no_trade_count += 1


            # 為每個 condition_pair 分別進行優化
            condition_pairs = self.backtester_config.get("condition_pairs", [])
            all_optimal_params = {}
            all_train_metrics = {}
            all_grid_regions = {}
            
            for strategy_idx, pair in enumerate(condition_pairs):
                # DEBUG: 檢查該 condition_pair 的結果數量和 strategy_id 格式
                sample_strategy_ids = [r.get("strategy_id", "N/A") for r in results[:5]]
                self.logger.info(
                    f"[DEBUG] condition_pair {strategy_idx + 1} ({pair.get('entry', [])} + {pair.get('exit', [])}): "
                    f"總結果數: {len(results)}, 樣本 strategy_id: {sample_strategy_ids}"
                )
                
                # DEBUG: 統計該 condition_pair 的結果數量
                pair_results_count = 0
                for r in results:
                    result_strategy_id = r.get("strategy_id", "")
                    if result_strategy_id:
                        try:
                            if "_" in result_strategy_id:
                                parts = result_strategy_id.split("_")
                                result_strategy_idx = None
                                for part in reversed(parts):
                                    if part.isdigit():
                                        result_strategy_idx = int(part) - 1
                                        break
                                if result_strategy_idx == strategy_idx:
                                    pair_results_count += 1
                        except (ValueError, IndexError):
                            pass
                
                self.logger.info(
                    f"[DEBUG] condition_pair {strategy_idx + 1} 找到 {pair_results_count} 個匹配的結果"
                )
                
                # 為該 condition_pair 找到最優結果
                optimal_result = self._find_optimal_result(results, objective, strategy_idx=strategy_idx)
                
                if optimal_result is None:
                    # DEBUG: 檢查為什麼沒有找到結果
                    sample_strategy_ids = [r.get("strategy_id", "N/A") for r in results[:10]]
                    self.logger.warning(
                        f"[DEBUG] condition_pair {strategy_idx + 1} ({pair.get('entry', [])} + {pair.get('exit', [])}) "
                        f"未找到有效的 {objective.upper()} 結果。"
                        f"樣本 strategy_id: {sample_strategy_ids}, 匹配結果數: {pair_results_count}"
                    )
                    continue
                
                self.logger.info(
                    f"[DEBUG] condition_pair {strategy_idx + 1} 找到最優結果，strategy_id: {optimal_result.get('strategy_id', 'N/A')}"
                )
                
                # 提取最優參數（需要知道是哪個 condition_pair）
                single_optimal_params = self._extract_optimal_params(optimal_result, strategy_idx=strategy_idx)
                
                if not single_optimal_params:
                    self.logger.warning(
                        f"condition_pair {strategy_idx + 1} 未能提取參數"
                    )
                    continue
                
                # 生成九宮格參數組合並選擇平均最高的區域
                # 只使用該 condition_pair 的結果
                pair_results = []
                for r in results:
                    result_strategy_id = r.get("strategy_id", "")
                    if not result_strategy_id:
                        # 如果沒有 strategy_id，且 strategy_idx 是 0，則包含（向後兼容）
                        if strategy_idx == 0:
                            pair_results.append(r)
                        continue
                    
                    # 解析 strategy_id：可能是 "strategy_1", "strategy_2" 等
                    try:
                        if "_" in result_strategy_id:
                            parts = result_strategy_id.split("_")
                            result_strategy_idx = None
                            for part in reversed(parts):
                                if part.isdigit():
                                    result_strategy_idx = int(part) - 1
                                    break
                            
                            if result_strategy_idx == strategy_idx:
                                pair_results.append(r)
                        elif result_strategy_id.isdigit():
                            # 如果 strategy_id 是純數字
                            if int(result_strategy_id) - 1 == strategy_idx:
                                pair_results.append(r)
                    except (ValueError, IndexError):
                        # 如果無法解析，且 strategy_idx 是 0，則包含（向後兼容）
                        if strategy_idx == 0:
                            pair_results.append(r)
                
                self.logger.info(
                    f"[DEBUG] condition_pair {strategy_idx + 1} 用於九宮格的結果數: {len(pair_results)}"
                )
                
                grid_region = self._find_best_grid_region(
                    single_optimal_params, pair_results, objective, silent, strategy_idx=strategy_idx
                )
                
                if grid_region:
                    self.logger.info(
                        f"[DEBUG] condition_pair {strategy_idx + 1} 九宮格區域選擇成功，"
                        f"參數組合數: {len(grid_region.get('all_params', []))}"
                    )
                else:
                    self.logger.warning(
                        f"[DEBUG] condition_pair {strategy_idx + 1} 九宮格區域選擇失敗"
                    )
                
                if grid_region is None:
                    # 如果九宮格失敗，回退到單一最優參數
                    self.logger.warning(
                        f"condition_pair {strategy_idx + 1} 九宮格區域選擇失敗，使用單一最優參數"
                    )
                    train_metrics = self._calculate_train_metrics(optimal_result, objective)
                    all_optimal_params.update(single_optimal_params)
                    all_train_metrics.update(train_metrics or {})
                else:
                    # 使用九宮格區域的參數和績效
                    grid_params = grid_region["params"]
                    grid_train_metrics = grid_region["train_metrics"]
                    grid_all_params = grid_region["all_params"]  # 所有9個參數組合
                    
                    # 合併參數
                    all_optimal_params.update(grid_params)
                    all_train_metrics.update(grid_train_metrics or {})
                    
                    # 保存九宮格信息（為每個 condition_pair 分別保存）
                    if strategy_idx not in all_grid_regions:
                        all_grid_regions[strategy_idx] = {}
                    all_grid_regions[strategy_idx] = {
                        "all_params": grid_all_params,
                        "avg_metric": grid_region.get("avg_metric"),
                        "individual_metrics": grid_region.get("individual_metrics", []),
                        "individual_full_metrics": grid_region.get("individual_full_metrics", []),
                        "train_metrics": grid_region.get("train_metrics"),
                    }
            
            # 如果沒有任何有效的 condition_pair，返回失敗
            if not all_optimal_params:
                failure_reason = (
                    f"所有 condition_pairs 都未找到有效的 {objective.upper()} 結果。"
                    f"錯誤: {error_count}, 無交易: {no_trade_count}, 有效: {valid_count}"
                )
                self.logger.warning(failure_reason)
                self._last_failure_reason = failure_reason
                return None, None
            
            # 保存所有 condition_pair 的 grid_regions
            # 使用字典格式：{strategy_idx: grid_region}
            self._last_grid_regions = all_grid_regions  # 保存所有
            self._last_grid_region = None  # 向後兼容，但不再使用
            
            # DEBUG: 記錄保存的 grid_regions
            self.logger.info(
                f"[DEBUG] 保存的 grid_regions: {list(all_grid_regions.keys())}, "
                f"總數: {len(all_grid_regions)}"
            )
            
            # 為了向後兼容，也保存第一個有效的 grid_region
            if all_grid_regions:
                first_strategy_idx = min(all_grid_regions.keys())
                self._last_grid_region = all_grid_regions[first_strategy_idx]
                self.logger.info(
                    f"[DEBUG] 第一個 grid_region 來自 strategy_idx: {first_strategy_idx}"
                )
            else:
                # 如果沒有 grid_region，創建一個空的
                self._last_grid_region = {
                    "all_params": [],
                    "avg_metric": None,
                    "individual_metrics": [],
                    "individual_full_metrics": [],
                    "train_metrics": all_train_metrics,
                }
                self.logger.warning("[DEBUG] 沒有 grid_regions，創建空的 grid_region")

            # DEBUG: 記錄最終的 optimal_params
            self.logger.info(
                f"[DEBUG] 最終 optimal_params 的鍵: {list(all_optimal_params.keys())[:10]}... "
                f"(總數: {len(all_optimal_params)})"
            )

            return all_optimal_params, all_train_metrics

        except Exception as e:
            self.logger.error(f"參數優化失敗: {e}")
            self._last_failure_reason = f"優化過程異常: {str(e)}"
            return None, None

    def _is_variable_param(self, value: Any) -> bool:
        """
        判斷參數是否為可變參數（範圍或逗號分隔的多值）
        
        Args:
            value: 參數值
            
        Returns:
            bool: 如果是可變參數返回 True，否則返回 False
        """
        if not isinstance(value, str):
            return False
        
        # 檢查是否為範圍格式（如 "10:200:10"）
        if ":" in value:
            parts = value.split(":")
            if len(parts) == 3:
                try:
                    # 嘗試解析為數字範圍
                    start = float(parts[0])
                    end = float(parts[1])
                    step = float(parts[2])
                    # 如果 start == end，則只有一個值，不是可變參數
                    if start == end:
                        return False
                    # 如果 step > 0 且 end < start，則沒有有效值
                    if step > 0 and end < start:
                        return False
                    # 如果 step < 0 且 end > start，則沒有有效值
                    if step < 0 and end > start:
                        return False
                    # 計算實際會產生多少個值
                    if step > 0:
                        num_values = int((end - start) / step) + 1
                    elif step < 0:
                        num_values = int((start - end) / abs(step)) + 1
                    else:
                        return False  # step 不能為 0
                    # 只有當會產生多個值時，才是可變參數
                    return num_values > 1
                except (ValueError, ZeroDivisionError):
                    return False
        
        # 檢查是否為逗號分隔格式（如 "1,1.5,2"）
        if "," in value:
            parts = [x.strip() for x in value.split(",")]
            if len(parts) > 1:
                # 檢查是否都是有效數字
                try:
                    unique_values = set()
                    for part in parts:
                        unique_values.add(float(part))
                    # 只有當有超過1個不同值時，才是可變參數
                    return len(unique_values) > 1
                except ValueError:
                    return False
        
        return False
    
    def _count_variable_params(self, params_config: Dict[str, Any]) -> int:
        """
        統計參數配置中的可變參數數量
        
        Args:
            params_config: 參數配置字典
            
        Returns:
            int: 可變參數的數量
        """
        count = 0
        # 排除元數據參數
        excluded_keys = {"strat_idx", "indicator_type", "_help", "_description"}
        
        for key, value in params_config.items():
            if key in excluded_keys:
                continue
            if self._is_variable_param(value):
                count += 1
        
        return count
    
    def _validate_parameter_config(self, params_config: Dict[str, Any], indicator_name: str) -> None:
        """
        驗證參數配置（已棄用，保留以向後兼容）
        現在改用 _count_variable_params 來統計，並在 condition_pair 層級驗證總和
        
        Args:
            params_config: 參數配置字典
            indicator_name: 指標名稱（用於錯誤提示）
        """
        # 不再單獨驗證每個指標，改為在 condition_pair 層級驗證總和
        variable_count = self._count_variable_params(params_config)
        if variable_count > 0:
            variable_params = []
            excluded_keys = {"strat_idx", "indicator_type", "_help", "_description"}
            for key, value in params_config.items():
                if key not in excluded_keys and self._is_variable_param(value):
                    variable_params.append(key)
            self.logger.debug(
                f"指標 {indicator_name}: 可變參數 {variable_params} ({variable_count} 個)"
            )

    def _build_optimization_config(self) -> Dict[str, Any]:
        """
        構建優化用的回測配置

        Returns:
            Dict[str, Any]: 回測配置
        """
        from backtester.Indicators_backtester import IndicatorsBacktester

        condition_pairs = self.backtester_config.get("condition_pairs", [])
        raw_indicator_params = self.backtester_config.get("indicator_params", {})
        
        # 轉換 indicator_params：從字典格式轉換為參數列表格式
        # 配置格式：{"MA1_strategy_1": {"ma_type": "SMA", "ma_range": "10:200:10"}}
        # 需要轉換為：{"MA1_strategy_1": [IndicatorParams, IndicatorParams, ...]}
        indicators_helper = IndicatorsBacktester(logger=self.logger)
        indicator_params = {}

        for i, pair in enumerate(condition_pairs):
            strategy_idx = i + 1

            # 先收集 entry 和 exit 的所有參數配置，然後驗證總和
            entry_param_configs = []
            exit_param_configs = []
            
            # 處理開倉指標
            for entry_indicator in pair.get("entry", []):
                strategy_alias = f"{entry_indicator}_strategy_{strategy_idx}"
                if strategy_alias in raw_indicator_params:
                    params_config = raw_indicator_params[strategy_alias]
                    entry_param_configs.append((strategy_alias, params_config))
                    
                    # 調用 get_indicator_params 生成參數列表
                    try:
                        param_list = indicators_helper.get_indicator_params(
                            entry_indicator, params_config
                        )
                        if not param_list:
                            self.logger.warning(
                                f"指標 {entry_indicator} (strategy_alias={strategy_alias}) 未生成任何參數組合，"
                                f"params_config={params_config}"
                            )
                        indicator_params[strategy_alias] = param_list
                    except Exception as e:
                        self.logger.error(
                            f"生成 {entry_indicator} (strategy_alias={strategy_alias}) 參數列表失敗: {e}, "
                            f"params_config={params_config}"
                        )
                        indicator_params[strategy_alias] = []
                else:
                    self.logger.warning(
                        f"未找到指標 {entry_indicator} 的參數配置 (strategy_alias={strategy_alias})，"
                        f"可用的配置鍵: {list(raw_indicator_params.keys())}"
                    )

            # 處理平倉指標
            for exit_indicator in pair.get("exit", []):
                strategy_alias = f"{exit_indicator}_strategy_{strategy_idx}"
                if strategy_alias in raw_indicator_params:
                    params_config = raw_indicator_params[strategy_alias]
                    exit_param_configs.append((strategy_alias, params_config))
                    
                    # 調用 get_indicator_params 生成參數列表
                    try:
                        param_list = indicators_helper.get_indicator_params(
                            exit_indicator, params_config
                        )
                        if not param_list:
                            self.logger.warning(
                                f"指標 {exit_indicator} (strategy_alias={strategy_alias}) 未生成任何參數組合，"
                                f"params_config={params_config}"
                            )
                        indicator_params[strategy_alias] = param_list
                    except Exception as e:
                        self.logger.error(
                            f"生成 {exit_indicator} (strategy_alias={strategy_alias}) 參數列表失敗: {e}, "
                            f"params_config={params_config}"
                        )
                        indicator_params[strategy_alias] = []
                else:
                    self.logger.warning(
                        f"未找到指標 {exit_indicator} 的參數配置 (strategy_alias={strategy_alias})，"
                        f"可用的配置鍵: {list(raw_indicator_params.keys())}"
                    )
            
            # 驗證 entry + exit 的總可變參數數量（不超過2個）
            total_variable_params = 0
            all_indicator_names = []
            for strategy_alias, params_config in entry_param_configs + exit_param_configs:
                variable_count = self._count_variable_params(params_config)
                total_variable_params += variable_count
                all_indicator_names.append(strategy_alias)
            
            if total_variable_params > 2:
                raise ValueError(
                    f"condition_pair {strategy_idx} (entry + exit) 的可變參數總數超過2個（找到 {total_variable_params} 個）。"
                    f"涉及的指標: {', '.join(all_indicator_names)}"
                    f"WFA 要求 entry + exit 兩個指標的可變參數總和最多只能有2個。"
                    f"請修改配置，將多餘的可變參數改為固定值。"
                )
            
            if total_variable_params > 0:
                self.logger.info(
                    f"condition_pair {strategy_idx}: entry + exit 總可變參數 {total_variable_params} 個，"
                    f"指標: {', '.join(all_indicator_names)}"
                )

        backtest_config = {
            "condition_pairs": condition_pairs,
            "indicator_params": indicator_params,
            "predictors": [
                self.backtester_config.get("selected_predictor", "X")
            ],
            "trading_params": self.backtester_config.get("trading_params", {}),
        }

        return backtest_config

    def _find_optimal_result(
        self, results: List[Dict[str, Any]], objective: str, strategy_idx: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """
        從回測結果中找到最優結果

        Args:
            results: 回測結果列表
            objective: 優化目標（"sharpe" 或 "calmar"）
            strategy_idx: 可選的策略索引，如果提供則只從該策略的結果中選擇

        Returns:
            Optional[Dict[str, Any]]: 最優結果，如果未找到則返回 None
        """
        from metricstracker.MetricsCalculator_metricstracker import (
            MetricsCalculatorMetricTracker,
        )

        valid_results = []

        for result in results:
            # 如果指定了 strategy_idx，只處理該策略的結果
            if strategy_idx is not None:
                result_strategy_id = result.get("strategy_id", "")
                # strategy_id 格式為 "strategy_1", "strategy_2" 等
                if result_strategy_id:
                    try:
                        # 解析 strategy_id：可能是 "strategy_1" 或 "strategy_1_xxx"
                        if "_" in result_strategy_id:
                            # 提取最後一個數字部分
                            parts = result_strategy_id.split("_")
                            # 找到最後一個數字
                            result_strategy_idx = None
                            for part in reversed(parts):
                                if part.isdigit():
                                    result_strategy_idx = int(part) - 1
                                    break
                            
                            if result_strategy_idx is None:
                                # 如果無法解析，且 strategy_idx 是 0，則包含（向後兼容）
                                if strategy_idx != 0:
                                    continue
                            elif result_strategy_idx != strategy_idx:
                                continue
                        else:
                            # 如果無法解析，跳過
                            continue
                    except (ValueError, IndexError):
                        # 如果無法解析 strategy_id，跳過
                        continue
                else:
                    # 如果沒有 strategy_id，且 strategy_idx 不是 0，跳過
                    # （因為舊的結果可能沒有 strategy_id，假設是第一個策略）
                    if strategy_idx != 0:
                        continue

            # 檢查是否有錯誤
            if result.get("error") is not None:
                continue

            # 檢查是否有交易記錄
            if "records" not in result:
                continue

            records = result["records"]
            if not isinstance(records, pd.DataFrame) or records.empty:
                continue

            # 檢查是否有實際交易（開倉交易）
            if "Trade_action" not in records.columns:
                continue

            trade_count = (records["Trade_action"] == 1).sum()
            if trade_count == 0:
                # 無交易，跳過
                continue

            # 計算績效指標
            try:
                metrics_calc = MetricsCalculatorMetricTracker(
                    records,
                    time_unit=365,
                    risk_free_rate=0.04,
                )

                if objective == "sharpe":
                    metric_value = metrics_calc.sharpe()
                elif objective == "calmar":
                    metric_value = metrics_calc.calmar()
                else:
                    continue

                # 跳過無效值
                if pd.isna(metric_value) or metric_value == float("inf") or metric_value == float("-inf"):
                    continue

                result["optimal_metric"] = metric_value
                result["trade_count"] = trade_count
                valid_results.append(result)

            except Exception as e:
                self.logger.warning(f"計算績效指標失敗: {e}")
                continue

        if not valid_results:
            return None

        # 選擇最優結果（最大化目標指標）
        optimal_result = max(valid_results, key=lambda x: x.get("optimal_metric", float("-inf")))

        return optimal_result

    def _extract_optimal_params(self, optimal_result: Dict[str, Any], strategy_idx: Optional[int] = None) -> Dict[str, Any]:
        """
        從最優結果中提取參數

        Args:
            optimal_result: 最優結果
            strategy_idx: 策略索引（從0開始），如果為None則從結果中推斷

        Returns:
            Dict[str, Any]: 最優參數（格式與 indicator_params 相同）
        """
        # 從結果中提取參數信息
        # VectorBacktestEngine 的結果包含 "params" 字段，格式為：
        # {"entry": [...], "exit": [...], "predictor": "..."}

        optimal_params = {}

        if "params" not in optimal_result:
            self.logger.warning("結果中缺少 params 字段")
            return optimal_params

        params = optimal_result["params"]
        entry_params = params.get("entry", [])
        exit_params = params.get("exit", [])

        # 將參數轉換為 indicator_params 格式
        # 需要根據 condition_pairs 來構建正確的格式
        condition_pairs = self.backtester_config.get("condition_pairs", [])

        if not condition_pairs:
            self.logger.warning("配置中缺少 condition_pairs")
            return optimal_params

        # 確定 strategy_idx
        if strategy_idx is None:
            # 從結果中推斷 strategy_idx
            strategy_id = optimal_result.get("strategy_id", "")
            if strategy_id:
                try:
                    strategy_idx = int(strategy_id.split("_")[-1]) - 1
                except (ValueError, IndexError):
                    strategy_idx = 0
            else:
                strategy_idx = 0
        
        if strategy_idx >= len(condition_pairs):
            self.logger.warning(f"strategy_idx {strategy_idx} 超出範圍，使用第一個 condition_pair")
            strategy_idx = 0

        # 使用對應的條件配對
        pair = condition_pairs[strategy_idx]
        strategy_idx_1based = strategy_idx + 1  # 轉換為1-based索引

        # 處理開倉指標參數
        entry_indicators = pair.get("entry", [])
        if not entry_indicators:
            self.logger.warning("條件配對中缺少 entry 指標")
        else:
            for i, entry_indicator in enumerate(entry_indicators):
                strategy_alias = f"{entry_indicator}_strategy_{strategy_idx_1based}"
                if i < len(entry_params):
                    param_dict = entry_params[i]
                    # 轉換為 indicator_params 格式（需要是 IndicatorParams 對象列表）
                    from backtester.IndicatorParams_backtester import IndicatorParams
                    
                    # 從 param_dict 構建 IndicatorParams
                    # param_dict 是通過 to_dict() 生成的，格式為：
                    # {"indicator_type": "MA", "period": 20, "ma_type": "SMA", ...}
                    indicator_type = param_dict.get("indicator_type", "MA")
                    indicator_param = IndicatorParams(indicator_type)
                    
                    # 添加所有參數（跳過 indicator_type，因為已經在構造函數中設置）
                    for key, value in param_dict.items():
                        if key != "indicator_type" and key not in ["trading_params"]:
                            # 參數值直接使用，不需要包裝在 {"value": ..., "type": ...} 中
                            # 因為 add_param 會自動包裝
                            indicator_param.add_param(key, value)
                    
                    # indicator_params 格式應該是 {strategy_alias: [IndicatorParams, ...]}
                    optimal_params[strategy_alias] = [indicator_param]
                else:
                    self.logger.warning(f"entry_params 長度不足: {len(entry_params)} < {i+1}")

        # 處理平倉指標參數
        exit_indicators = pair.get("exit", [])
        if not exit_indicators:
            self.logger.warning("條件配對中缺少 exit 指標")
        else:
            for i, exit_indicator in enumerate(exit_indicators):
                strategy_alias = f"{exit_indicator}_strategy_{strategy_idx_1based}"
                if i < len(exit_params):
                    param_dict = exit_params[i]
                    # 轉換為 indicator_params 格式
                    from backtester.IndicatorParams_backtester import IndicatorParams
                    
                    indicator_type = param_dict.get("indicator_type", "MA")
                    indicator_param = IndicatorParams(indicator_type)
                    
                    # 添加所有參數
                    for key, value in param_dict.items():
                        if key != "indicator_type" and key not in ["trading_params"]:
                            indicator_param.add_param(key, value)
                    
                    optimal_params[strategy_alias] = [indicator_param]
                else:
                    self.logger.warning(f"exit_params 長度不足: {len(exit_params)} < {i+1}")

        if not optimal_params:
            self.logger.warning("未能提取任何參數")
        return optimal_params

    def _calculate_train_metrics(
        self, optimal_result: Dict[str, Any], objective: str
    ) -> Optional[Dict[str, Any]]:
        """
        計算訓練集（IS）績效指標

        Args:
            optimal_result: 最優回測結果
            objective: 優化目標

        Returns:
            Optional[Dict[str, Any]]: 訓練集績效指標
        """
        try:
            from metricstracker.MetricsCalculator_metricstracker import (
                MetricsCalculatorMetricTracker,
            )

            if "records" not in optimal_result:
                return None

            records = optimal_result["records"]
            if not isinstance(records, pd.DataFrame) or records.empty:
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

            return metrics

        except Exception as e:
            self.logger.warning(f"計算訓練集績效指標失敗: {e}")
            return None

    def get_last_failure_reason(self) -> Optional[str]:
        """
        獲取最後一次失敗的原因

        Returns:
            Optional[str]: 失敗原因
        """
        return getattr(self, "_last_failure_reason", None)

    def get_last_grid_region(self, strategy_idx: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        獲取最後一次優化的九宮格區域信息

        Args:
            strategy_idx: 可選的策略索引，如果提供則返回該策略的 grid_region
        
        Returns:
            Optional[Dict[str, Any]]: 九宮格區域信息
        """
        # 如果指定了 strategy_idx，從 _last_grid_regions 獲取
        if strategy_idx is not None:
            last_grid_regions = getattr(self, "_last_grid_regions", {})
            return last_grid_regions.get(strategy_idx)
        
        # 向後兼容：返回第一個 grid_region
        return getattr(self, "_last_grid_region", None)
    
    def get_all_grid_regions(self) -> Dict[int, Dict[str, Any]]:
        """
        獲取所有 condition_pair 的九宮格區域信息
        
        Returns:
            Dict[int, Dict[str, Any]]: 所有 condition_pair 的 grid_regions
        """
        return getattr(self, "_last_grid_regions", {})

    def _find_best_grid_region(
        self,
        single_optimal_params: Dict[str, Any],
        all_results: List[Dict[str, Any]],
        objective: str,
        silent: bool = True,
        strategy_idx: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        找到總和最大的九宮格區域（使用二維積分圖像加速）
        
        使用二維積分圖像（2D Prefix Sum）來快速計算所有可能的3x3區域的總和，
        時間複雜度：O(N²)，空間複雜度：O(N²)

        Args:
            single_optimal_params: 單一最優參數（未使用，保留以保持接口兼容性）
            all_results: 所有回測結果
            objective: 優化目標
            silent: 是否靜默模式

        Returns:
            Optional[Dict[str, Any]]: 最佳九宮格區域信息
        """
        try:
            # 步驟1: 從所有結果中提取參數值和metric值，構建二維矩陣
            self.logger.info("構建參數-績效二維矩陣...")
            matrix, param_mapping, result_mapping = self._build_parameter_matrix(
                all_results, objective, strategy_idx=strategy_idx
            )
            
            if matrix is None or matrix.size == 0:
                self.logger.warning("無法構建參數矩陣")
                return None
            
            rows, cols = matrix.shape
            self.logger.info(f"參數矩陣大小: {rows} x {cols}")
            
            # 步驟2: 構建積分圖像（2D Prefix Sum）
            # prefix[i+1][j+1] = sum of all values in rectangle [0,0] to [i,j]
            self.logger.info("構建積分圖像...")
            prefix = np.zeros((rows + 1, cols + 1), dtype=np.float64)
            
            # 計算前綴和
            for i in range(rows):
                for j in range(cols):
                    prefix[i + 1][j + 1] = (
                        matrix[i][j]
                        + prefix[i][j + 1]
                        + prefix[i + 1][j]
                        - prefix[i][j]
                    )
            
            # 步驟3: 遍歷所有可能的3x3區域，找到總和最大的
            self.logger.info("搜索最佳3x3區域...")
            best_sum = float("-inf")
            best_i, best_j = -1, -1
            grid_size = 3  # 九宮格大小
            
            # 遍歷所有可能的左上角位置
            for i in range(rows - grid_size + 1):
                for j in range(cols - grid_size + 1):
                    # 使用O(1)公式計算3x3區域的總和
                    # sum = prefix[i+k][j+k] - prefix[i+k][j] - prefix[i][j+k] + prefix[i][j]
                    region_sum = (
                        prefix[i + grid_size][j + grid_size]
                        - prefix[i + grid_size][j]
                        - prefix[i][j + grid_size]
                        + prefix[i][j]
                    )
                    
                    if region_sum > best_sum:
                        best_sum = region_sum
                        best_i, best_j = i, j
            
            # 清理積分圖像以節省空間
            del prefix
            
            if best_i == -1 or best_j == -1:
                self.logger.warning("未找到有效的3x3區域")
                return None
            
            self.logger.info(
                f"找到最佳3x3區域: 位置=({best_i}, {best_j}), 總和={best_sum:.4f}, 平均={best_sum/9:.4f}"
            )
            
            # 步驟4: 從最佳區域構建參數組合列表
            best_params_list = []
            best_results_list = []
            best_metrics_list = []
            
            for di in range(grid_size):
                for dj in range(grid_size):
                    i_idx = best_i + di
                    j_idx = best_j + dj
                    
                    # 從映射中獲取參數值和結果
                    if (i_idx, j_idx) in param_mapping:
                        params = param_mapping[(i_idx, j_idx)]
                        result = result_mapping.get((i_idx, j_idx))
                        
                        if params and result:
                            best_params_list.append(params)
                            best_results_list.append(result)
                            
                            # 計算完整指標
                            full_metrics = self._calculate_individual_full_metrics(result)
                            metric = self._calculate_metric_from_result(result, objective)
                            
                            best_metrics_list.append({
                                "params": params,
                                "result": result,
                                "metric": metric,
                                "full_metrics": full_metrics,
                            })
            
            if not best_params_list:
                self.logger.warning("無法從最佳區域構建參數組合")
                return None
            
            # 步驟5: 構建返回結果
            avg_metric = best_sum / len(best_metrics_list) if best_metrics_list else 0.0
            individual_full_metrics_list = [m["full_metrics"] for m in best_metrics_list]
            
            best_region = {
                "params": best_params_list[0],  # 使用第一個參數作為代表
                "all_params": best_params_list,  # 所有9個參數組合
                "avg_metric": avg_metric,
                "individual_metrics": [m["metric"] for m in best_metrics_list],
                "individual_full_metrics": individual_full_metrics_list,
                "train_metrics": self._calculate_grid_train_metrics(best_metrics_list, objective),
            }
            
            self.logger.info(
                f"最終選擇的最佳九宮格區域: 總和={best_sum:.4f}, 平均={avg_metric:.4f}"
            )
            
            return best_region

        except Exception as e:
            self.logger.error(f"查找最佳九宮格區域失敗: {e}", exc_info=True)
            return None

    def _get_all_param_keys(self, param_dict: Dict[str, Any]) -> List[str]:
        """
        從參數字典中動態獲取所有參數鍵（排除元數據）
        
        Args:
            param_dict: 參數字典（從 to_dict() 獲取）
            
        Returns:
            List[str]: 參數鍵列表
        """
        # 排除元數據和固定參數（這些不會用於構建矩陣）
        excluded_keys = {"indicator_type", "strat_idx", "ma_type", "mode"}
        param_keys = [k for k in param_dict.keys() if k not in excluded_keys]
        return param_keys
    
    def _get_param_key(self, indicator_type: str, param_dict: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """
        獲取參數鍵名（動態方式）
        
        優先從 param_dict 中動態獲取，如果沒有提供則使用默認映射
        
        Args:
            indicator_type: 指標類型
            param_dict: 可選的參數字典（從 to_dict() 獲取）
            
        Returns:
            str: 參數鍵名，如果無法確定則返回 None
        """
        # 如果提供了 param_dict，優先使用動態方式
        if param_dict:
            param_keys = self._get_all_param_keys(param_dict)
            if param_keys:
                # 返回第一個參數鍵（通常是最重要的）
                return param_keys[0]
        
        # 回退到默認映射（向後兼容）
        indicator_type = indicator_type.upper()
        if indicator_type.startswith("MA"):
            return "period"
        elif indicator_type.startswith("BOLL"):
            return "ma_length"
        elif indicator_type.startswith("HL"):
            return "n_length"
        elif indicator_type.startswith("PERC"):
            return "window"
        elif indicator_type.startswith("VALUE"):
            return "n_length"
        return "period"
    
    def _map_config_key_to_param_key(self, config_key: str, param_dict: Dict[str, Any]) -> Optional[str]:
        """
        將配置中的參數鍵名映射到 param_dict 中的鍵名
        
        例如：
        - config_key: "m_range" -> param_key: "m_length" (對於 HL)
        - config_key: "n_range" -> param_key: "n_length" (對於 HL)
        - config_key: "window_range" -> param_key: "window" (對於 PERC)
        - config_key: "ma_range" -> param_key: "ma_length" (對於 BOLL) 或 "period" (對於 MA)
        
        Args:
            config_key: 配置中的鍵名（如 "m_range", "n_range", "window_range", "ma_range"）
            param_dict: 參數字典（從 to_dict() 獲取）
            
        Returns:
            Optional[str]: 映射後的鍵名，如果無法映射則返回 None
        """
        # 定義映射規則
        key_mapping = {
            "m_range": "m_length",
            "n_range": "n_length",
            "window_range": "window",
            "percentile_range": "percentile",
            "ma_range": None,  # 需要根據 indicator_type 判斷
        }
        
        # 直接映射
        if config_key in key_mapping:
            mapped_key = key_mapping[config_key]
            if mapped_key and mapped_key in param_dict:
                return mapped_key
        
        # 對於 ma_range，需要根據 indicator_type 判斷
        if config_key == "ma_range":
            indicator_type = param_dict.get("indicator_type", "")
            if indicator_type == "MA":
                if "period" in param_dict:
                    return "period"
            elif indicator_type == "BOLL":
                if "ma_length" in param_dict:
                    return "ma_length"
        
        # 如果無法映射，嘗試直接使用 config_key（去掉 _range 後綴）
        if config_key.endswith("_range"):
            base_key = config_key.replace("_range", "")
            if base_key in param_dict:
                return base_key
        
        return None

    def _build_parameter_matrix(
        self, all_results: List[Dict[str, Any]], objective: str, strategy_idx: Optional[int] = None
    ) -> Tuple[Optional[np.ndarray], Dict[Tuple[int, int], Dict[str, Any]], Dict[Tuple[int, int], Dict[str, Any]]]:
        """
        構建參數-績效二維矩陣
        
        從所有回測結果中提取MA1和MA4的參數值，構建一個二維矩陣，
        其中matrix[i][j]對應MA1=param1_list[i], MA4=param2_list[j]的metric值
        
        Args:
            all_results: 所有回測結果
            objective: 優化目標
            
        Returns:
            Tuple[Optional[np.ndarray], Dict, Dict]: 
            - 二維矩陣（如果無法構建則返回None）
            - 參數映射：{(i, j): params_dict}
            - 結果映射：{(i, j): result_dict}
        """
        import numpy as np
        from metricstracker.MetricsCalculator_metricstracker import (
            MetricsCalculatorMetricTracker,
        )
        
        condition_pairs = self.backtester_config.get("condition_pairs", [])
        if not condition_pairs:
            return None, {}, {}
        
        # 確定使用哪個 condition_pair
        if strategy_idx is None:
            strategy_idx = 0
        if strategy_idx >= len(condition_pairs):
            strategy_idx = 0
        
        pair = condition_pairs[strategy_idx]
        entry_indicators = pair.get("entry", [])
        exit_indicators = pair.get("exit", [])
        
        if not entry_indicators or not exit_indicators:
            return None, {}, {}
        
        # 收集所有有效的參數值和metric值
        param1_values = set()  # MA1參數值
        param2_values = set()  # MA4參數值
        param_data = {}  # {(param1, param2): (metric, result, params)}
        
        for result in all_results:
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
            if "Trade_action" not in records.columns:
                continue
            
            trade_count = (records["Trade_action"] == 1).sum()
            if trade_count == 0:
                continue
            
            # 提取參數值
            if "params" not in result:
                continue
            
            params = result["params"]
            entry_params = params.get("entry", [])
            exit_params = params.get("exit", [])
            
            if not entry_params or not exit_params:
                continue
            
            # 提取參數值（根據指標類型動態獲取參數鍵）
            entry_param_dict = entry_params[0] if isinstance(entry_params[0], dict) else entry_params[0].to_dict() if hasattr(entry_params[0], "to_dict") else {}
            exit_param_dict = exit_params[0] if isinstance(exit_params[0], dict) else exit_params[0].to_dict() if hasattr(exit_params[0], "to_dict") else {}
            
            # 從配置中獲取可變參數鍵（只使用可變參數構建矩陣）
            raw_indicator_params = self.backtester_config.get("indicator_params", {})
            strategy_idx_1based = strategy_idx + 1
            
            entry_indicator_name = entry_indicators[0] if entry_indicators else ""
            exit_indicator_name = exit_indicators[0] if exit_indicators else ""
            entry_strategy_alias = f"{entry_indicator_name}_strategy_{strategy_idx_1based}"
            exit_strategy_alias = f"{exit_indicator_name}_strategy_{strategy_idx_1based}"
            
            # 從配置中獲取可變參數鍵
            entry_config = raw_indicator_params.get(entry_strategy_alias, {})
            exit_config = raw_indicator_params.get(exit_strategy_alias, {})
            
            entry_variable_keys = [k for k in entry_config.keys() if self._is_variable_param(entry_config[k])]
            exit_variable_keys = [k for k in exit_config.keys() if self._is_variable_param(exit_config[k])]
            
            # 如果配置中沒有可變參數信息，回退到從 param_dict 中獲取所有鍵
            if not entry_variable_keys:
                entry_variable_keys = self._get_all_param_keys(entry_param_dict)
            if not exit_variable_keys:
                exit_variable_keys = self._get_all_param_keys(exit_param_dict)
            
            if len(entry_variable_keys) == 0 or len(exit_variable_keys) == 0:
                self.logger.warning(
                    f"無法獲取可變參數鍵: entry_variable_keys={entry_variable_keys}, "
                    f"exit_variable_keys={exit_variable_keys}, "
                    f"entry_param_dict={entry_param_dict}, exit_param_dict={exit_param_dict}"
                )
                continue
            
            # 使用第一個可變參數鍵（根據驗證，最多只有2個可變參數，這裡取第一個）
            param1_key = entry_variable_keys[0]
            param2_key = exit_variable_keys[0]
            
            # 將配置鍵名映射到 param_dict 中的鍵名（例如 "m_range" -> "m_length"）
            # 對於 HL，配置中是 "m_range"，但 param_dict 中是 "m_length"
            param1_key_mapped = self._map_config_key_to_param_key(param1_key, entry_param_dict)
            param2_key_mapped = self._map_config_key_to_param_key(param2_key, exit_param_dict)
            
            if param1_key_mapped:
                param1_key = param1_key_mapped
            if param2_key_mapped:
                param2_key = param2_key_mapped
            
            param1_value = entry_param_dict.get(param1_key)
            param2_value = exit_param_dict.get(param2_key)
            
            if param1_value is None or param2_value is None:
                # DEBUG: 記錄為什麼參數值為None
                self.logger.debug(
                    f"參數值為None: entry_indicator={entry_param_dict.get('indicator_type', 'N/A')}, "
                    f"param1_key={param1_key}, param1_value={param1_value}, "
                    f"exit_indicator={exit_param_dict.get('indicator_type', 'N/A')}, param2_key={param2_key}, "
                    f"param2_value={param2_value}, entry_param_dict={entry_param_dict}, "
                    f"exit_param_dict={exit_param_dict}"
                )
                continue
            
            # 計算metric值
            try:
                metrics_calc = MetricsCalculatorMetricTracker(
                    records,
                    time_unit=365,
                    risk_free_rate=0.04,
                )
                
                if objective == "sharpe":
                    metric_value = metrics_calc.sharpe()
                elif objective == "calmar":
                    metric_value = metrics_calc.calmar()
                else:
                    continue
                
                # 跳過無效值
                if pd.isna(metric_value) or metric_value == float("inf") or metric_value == float("-inf"):
                    continue
                
                param1_values.add(param1_value)
                param2_values.add(param2_value)
                
                # 構建參數字典（用於後續生成參數組合）
                params_dict = self._extract_optimal_params(result)
                
                param_data[(param1_value, param2_value)] = (metric_value, result, params_dict)
                
            except Exception as e:
                self.logger.warning(f"計算績效指標失敗: {e}")
                continue
        
        if not param1_values or not param2_values:
            self.logger.warning("未找到有效的參數值")
            return None, {}, {}
        
        # 排序參數值並構建索引映射
        param1_list = sorted(param1_values)
        param2_list = sorted(param2_values)
        
        param1_to_idx = {val: idx for idx, val in enumerate(param1_list)}
        param2_to_idx = {val: idx for idx, val in enumerate(param2_list)}
        
        # 構建二維矩陣
        rows = len(param1_list)
        cols = len(param2_list)
        matrix = np.full((rows, cols), np.nan, dtype=np.float64)
        
        param_mapping = {}  # {(i, j): params_dict}
        result_mapping = {}  # {(i, j): result_dict}
        
        for (param1, param2), (metric_value, result, params_dict) in param_data.items():
            i = param1_to_idx[param1]
            j = param2_to_idx[param2]
            matrix[i][j] = metric_value
            param_mapping[(i, j)] = params_dict
            result_mapping[(i, j)] = result
        
        # 將NaN值替換為-inf（這樣不會影響總和計算，但會避免選擇無效區域）
        matrix = np.where(np.isnan(matrix), float("-inf"), matrix)
        
        self.logger.info(
            f"構建參數矩陣完成: {rows}x{cols}, "
            f"MA1範圍={min(param1_list)}-{max(param1_list)}, "
            f"MA4範圍={min(param2_list)}-{max(param2_list)}, "
            f"有效數據點={np.sum(~np.isinf(matrix))}"
        )
        
        return matrix, param_mapping, result_mapping

    def _get_step_size(self, range_config: Dict[str, Any], indicator_type: str) -> Optional[int]:
        """從配置中獲取步長"""
        indicator_type = indicator_type.upper()
        
        if indicator_type.startswith("MA"):
            ma_range = range_config.get("ma_range", "")
            if ma_range:
                parts = ma_range.split(":")
                if len(parts) >= 3:
                    return int(parts[2])
        elif indicator_type.startswith("BOLL"):
            ma_range = range_config.get("ma_range", "")
            if ma_range:
                parts = ma_range.split(":")
                if len(parts) >= 3:
                    return int(parts[2])
        elif indicator_type.startswith("HL"):
            n_range = range_config.get("n_range", "")
            if n_range:
                parts = n_range.split(":")
                if len(parts) >= 3:
                    return int(parts[2])
        elif indicator_type.startswith("PERC"):
            window_range = range_config.get("window_range", "")
            if window_range:
                parts = window_range.split(":")
                if len(parts) >= 3:
                    return int(parts[2])
        elif indicator_type.startswith("VALUE"):
            n_range = range_config.get("n_range", "")
            if n_range:
                parts = n_range.split(":")
                if len(parts) >= 3:
                    return int(parts[2])
        
        return None

    def _build_grid_param_config(
        self,
        entry_indicators: List[str],
        exit_indicators: List[str],
        strategy_idx: int,
        entry_values: Tuple,
        exit_values: Tuple,
        entry_neighbors: List,
        exit_neighbors: List,
    ) -> Optional[Dict[str, Any]]:
        """構建九宮格參數配置"""
        from backtester.IndicatorParams_backtester import IndicatorParams

        grid_params = {}

        # 處理開倉指標
        for i, entry_indicator in enumerate(entry_indicators):
            strategy_alias = f"{entry_indicator}_strategy_{strategy_idx}"
            
            if i < len(entry_neighbors):
                _, _, optimal_param = entry_neighbors[i]
                
                # 創建新的 IndicatorParams
                indicator_type = optimal_param.indicator_type
                indicator_param = IndicatorParams(indicator_type)
                
                # 複製所有參數
                for key in optimal_param.params:
                    value = optimal_param.get_param(key)
                    if value is not None:
                        indicator_param.add_param(key, value)
                
                # 更新目標參數值
                if i < len(entry_values):
                    param_key = self._get_param_key(entry_indicator)
                    indicator_param.add_param(param_key, entry_values[i])
                
                grid_params[strategy_alias] = [indicator_param]

        # 處理平倉指標
        for i, exit_indicator in enumerate(exit_indicators):
            strategy_alias = f"{exit_indicator}_strategy_{strategy_idx}"
            
            if i < len(exit_neighbors):
                _, _, optimal_param = exit_neighbors[i]
                
                indicator_type = optimal_param.indicator_type
                indicator_param = IndicatorParams(indicator_type)
                
                for key in optimal_param.params:
                    value = optimal_param.get_param(key)
                    if value is not None:
                        indicator_param.add_param(key, value)
                
                if i < len(exit_values):
                    param_key = self._get_param_key(exit_indicator)
                    indicator_param.add_param(param_key, exit_values[i])
                
                grid_params[strategy_alias] = [indicator_param]

        return grid_params if grid_params else None

    def _calculate_metric_from_result(
        self, result: Dict[str, Any], objective: str
    ) -> Optional[float]:
        """從結果中計算績效指標"""
        try:
            from metricstracker.MetricsCalculator_metricstracker import (
                MetricsCalculatorMetricTracker,
            )

            if "records" not in result:
                return None

            records = result["records"]
            if not isinstance(records, pd.DataFrame) or records.empty:
                return None

            metrics_calc = MetricsCalculatorMetricTracker(
                records,
                time_unit=365,
                risk_free_rate=0.04,
            )

            if objective == "sharpe":
                metric_value = metrics_calc.sharpe()
            elif objective == "calmar":
                metric_value = metrics_calc.calmar()
            else:
                return None

            if pd.isna(metric_value) or metric_value == float("inf") or metric_value == float("-inf"):
                return None

            return metric_value

        except Exception as e:
            self.logger.warning(f"計算績效指標失敗: {e}")
            return None

    def _calculate_individual_full_metrics(self, result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        計算單個結果的完整績效指標
        
        Args:
            result: 回測結果
            
        Returns:
            Optional[Dict[str, Any]]: 包含 sharpe、calmar、sortino、total_return 的字典
        """
        try:
            from metricstracker.MetricsCalculator_metricstracker import (
                MetricsCalculatorMetricTracker,
            )

            if "records" not in result:
                return None

            records = result["records"]
            if not isinstance(records, pd.DataFrame) or records.empty:
                return None

            metrics_calc = MetricsCalculatorMetricTracker(
                records,
                time_unit=365,
                risk_free_rate=0.04,
            )

            sharpe = metrics_calc.sharpe()
            calmar = metrics_calc.calmar()
            sortino = metrics_calc.sortino()
            total_return = metrics_calc.total_return()
            max_drawdown = metrics_calc.max_drawdown()

            # 處理 NaN 和 Inf
            if pd.isna(sharpe) or sharpe == float("inf") or sharpe == float("-inf"):
                sharpe = None
            if pd.isna(calmar) or calmar == float("inf") or calmar == float("-inf"):
                calmar = None
            if pd.isna(sortino) or sortino == float("inf") or sortino == float("-inf"):
                sortino = None
            if pd.isna(total_return) or total_return == float("inf") or total_return == float("-inf"):
                total_return = None
            if pd.isna(max_drawdown) or max_drawdown == float("inf") or max_drawdown == float("-inf"):
                max_drawdown = None

            return {
                "sharpe": sharpe,
                "calmar": calmar,
                "sortino": sortino,
                "total_return": total_return,
                "max_drawdown": max_drawdown,
            }
        except Exception as e:
            self.logger.warning(f"計算完整績效指標失敗: {e}")
            return None

    def _calculate_grid_train_metrics(
        self, grid_metrics: List[Dict[str, Any]], objective: str
    ) -> Optional[Dict[str, Any]]:
        """計算九宮格區域的平均訓練集績效指標"""
        try:
            from metricstracker.MetricsCalculator_metricstracker import (
                MetricsCalculatorMetricTracker,
            )

            all_sharpe = []
            all_calmar = []
            all_sortino = []
            all_returns = []
            all_equity_curves = []

            for grid_metric in grid_metrics:
                result = grid_metric["result"]
                if "records" not in result:
                    continue

                records = result["records"]
                if not isinstance(records, pd.DataFrame) or records.empty:
                    continue

                metrics_calc = MetricsCalculatorMetricTracker(
                    records,
                    time_unit=365,
                    risk_free_rate=0.04,
                )

                sharpe = metrics_calc.sharpe()
                calmar = metrics_calc.calmar()
                sortino = metrics_calc.sortino()
                total_return = metrics_calc.total_return()

                if not pd.isna(sharpe) and sharpe != float("inf") and sharpe != float("-inf"):
                    all_sharpe.append(sharpe)
                if not pd.isna(calmar) and calmar != float("inf") and calmar != float("-inf"):
                    all_calmar.append(calmar)
                if not pd.isna(sortino) and sortino != float("inf") and sortino != float("-inf"):
                    all_sortino.append(sortino)
                if not pd.isna(total_return) and total_return != float("inf") and total_return != float("-inf"):
                    all_returns.append(total_return)

                # 保存 equity curve
                if "Equity_value" in records.columns:
                    all_equity_curves.append(records["Equity_value"].values)

            if not all_sharpe and not all_calmar:
                return None

            # 計算平均值
            avg_sharpe = sum(all_sharpe) / len(all_sharpe) if all_sharpe else None
            avg_calmar = sum(all_calmar) / len(all_calmar) if all_calmar else None
            avg_sortino = sum(all_sortino) / len(all_sortino) if all_sortino else None
            avg_return = sum(all_returns) / len(all_returns) if all_returns else None

            # 計算平均 equity curve
            avg_equity = None
            if all_equity_curves:
                import numpy as np
                min_length = min(len(eq) for eq in all_equity_curves)
                truncated_curves = [eq[:min_length] for eq in all_equity_curves]
                avg_equity = np.mean(truncated_curves, axis=0)

            metrics = {
                "sharpe": avg_sharpe,
                "calmar": avg_calmar,
                "sortino": avg_sortino,
                "total_return": avg_return,
                "equity_curve": avg_equity,
                "param_count": len(grid_metrics),
            }

            return metrics

        except Exception as e:
            self.logger.warning(f"計算九宮格訓練集績效指標失敗: {e}")
            return None


