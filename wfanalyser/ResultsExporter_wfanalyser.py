"""
ResultsExporter_wfanalyser.py

【功能說明】
------------------------------------------------------------
本模組負責 WFA 結果的導出功能，將 WFA 結果導出為 parquet 和 CSV 格式。

【流程與數據流】
------------------------------------------------------------
- 主流程：接收結果 → 格式化數據 → 導出文件
- 數據流：WFA 結果 → 格式化 DataFrame → 文件輸出

【維護與擴充重點】
------------------------------------------------------------
- 結果格式需要與可視化平台兼容
- 導出格式需要包含所有重要信息

【常見易錯點】
------------------------------------------------------------
- 結果格式不一致導致導出失敗
- 文件路徑錯誤導致導出失敗

【範例】
------------------------------------------------------------
- 導出結果：exporter = ResultsExporter(results, output_dir); exporter.export()

【與其他模組的關聯】
------------------------------------------------------------
- 被 Base_wfanalyser 調用，導出 WFA 結果
- 為可視化平台提供數據

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本導出功能

【參考】
------------------------------------------------------------
- Base_wfanalyser.py: WFA 框架核心控制器
- plotter/WFADataImporter_plotter.py: WFA 可視化平台數據導入
- wfanalyser/README.md: WFA 模組詳細說明
"""

import logging
import random
import re
import string
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from rich.text import Text

from .utils import get_console
from utils import show_error, show_info, show_success

console = get_console()


class ResultsExporter:
    """
    WFA 結果導出器

    負責將 WFA 結果導出為 parquet 和 CSV 格式。
    """

    def __init__(
        self,
        results: Dict[str, Any],
        output_dir: Path,
        config_data: Optional[Any] = None,
        logger: Optional[logging.Logger] = None,
        data: Optional[pd.DataFrame] = None,
    ):
        """
        初始化 ResultsExporter

        Args:
            results: WFA 結果
            output_dir: 輸出目錄
            config_data: 配置數據對象（用於生成文件名）
            logger: 日誌記錄器
            data: 原始數據DataFrame（用於獲取實際時間）
        """
        self.results = results
        self.output_dir = Path(output_dir)
        self.config_data = config_data
        self.logger = logger or logging.getLogger("lo2cin4bt.wfanalyser.exporter")
        self.data = data  # 保存數據引用，用於根據索引獲取實際時間

        # 確保輸出目錄存在
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 讀取 output_csv 配置（默認為 True 保持向後兼容）
        if config_data and hasattr(config_data, 'wfa_config'):
            self.output_csv = config_data.wfa_config.get("output_csv", True)
        else:
            self.output_csv = True  # 默認值
        
        # 生成文件名基礎前綴（格式：日期_品種_預測檔案名字_預測因子）
        self.filename_base_prefix = self._generate_filename_base_prefix()
        
        # 生成同一策略共享的8位英數亂碼（sharpe 和 calmar 使用同一個）
        self.shared_random_code = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))

    def export(self) -> None:
        """
        導出 WFA 結果

        將結果導出為 parquet 和 CSV 格式。
        """
        try:
            show_info("WFANALYSER", "💾 開始導出 WFA 結果")

            # 按優化目標導出結果
            results_by_objective = self.results.get("results_by_objective", {})

            for objective, objective_results in results_by_objective.items():
                if not objective_results:
                    continue

                # 導出該目標的所有窗口結果
                self._export_objective_results(objective, objective_results)

            show_success("WFANALYSER", "WFA 結果導出完成")

        except Exception as e:
            self.logger.error(f"結果導出失敗: {e}")
            show_error("WFANALYSER", f"結果導出失敗: {e}")

    def _export_objective_results(
        self, objective: str, objective_results: list
    ) -> None:
        """
        導出單個優化目標的結果

        Args:
            objective: 優化目標名稱
            objective_results: 該目標的所有窗口結果
        """
        try:
            # 構建結果 DataFrame
            rows = []

            for window_result in objective_results:
                window_info = window_result.get("window_info", {})
                test_result = window_result.get("test_result", {})
                train_metrics = window_result.get("train_metrics", {})
                grid_region = window_result.get("grid_region", {})
                
                # DEBUG: 檢查是否有 all_condition_pair_results
                all_condition_pair_results = test_result.get("all_condition_pair_results", {})
                self.logger.info(
                    f"[DEBUG] 導出窗口 {window_info.get('window_id')} {objective}: "
                    f"all_condition_pair_results 的鍵: {list(all_condition_pair_results.keys())}, "
                    f"總數: {len(all_condition_pair_results)}"
                )
                
                # 獲取 OOS 績效
                test_metrics = test_result.get("metrics", {})
                
                # 如果存在九宮格區域，為每個參數組合創建一條記錄
                all_params = grid_region.get("all_params", [])
                individual_is_metrics = grid_region.get("individual_metrics", [])  # 優化目標指標列表
                individual_full_metrics = grid_region.get("individual_full_metrics", [])  # 每個參數組合的完整指標列表
                individual_oos_results = test_result.get("individual_results", [])
                
                # DEBUG: 檢查 all_grid_regions
                all_grid_regions = window_result.get("all_grid_regions", {})
                self.logger.info(
                    f"[DEBUG] 導出窗口 {window_info.get('window_id')} {objective}: "
                    f"all_grid_regions 的鍵: {list(all_grid_regions.keys())}, "
                    f"總數: {len(all_grid_regions)}"
                )
                
                # 統一使用 all_condition_pair_results 處理（即使只有一個 condition_pair）
                # 因為即使只有一個，all_condition_pair_results 也會是 {0: {...}} 的結構
                # 即使 OOS 測試失敗（all_condition_pair_results 為空），只要有 grid_regions，也應該導出 IS 結果
                if all_grid_regions:
                    self.logger.info(
                        f"[DEBUG] 發現 {len(all_condition_pair_results)} 個 condition_pair 的結果，將分別處理"
                    )
                    # 為每個 condition_pair 分別處理
                    # 如果 all_condition_pair_results 為空，則遍歷 all_grid_regions
                    condition_pairs_to_process = all_condition_pair_results if all_condition_pair_results else all_grid_regions
                    
                    for strategy_idx in condition_pairs_to_process.keys():
                        self.logger.info(
                            f"[DEBUG] 處理 condition_pair {strategy_idx + 1} 的結果"
                        )
                        
                        # 獲取該 condition_pair 的 grid_region（優先從 condition_pair_result 中獲取，否則從 all_grid_regions 中獲取）
                        condition_pair_result = all_condition_pair_results.get(strategy_idx, {}) if all_condition_pair_results else {}
                        pair_grid_region = condition_pair_result.get("grid_region")
                        if not pair_grid_region:
                            pair_grid_region = all_grid_regions.get(strategy_idx, {})
                        if not pair_grid_region:
                            self.logger.warning(
                                f"窗口 {window_info.get('window_id')} condition_pair {strategy_idx + 1}: "
                                f"未找到對應的 grid_region"
                            )
                            continue
                        
                        # 獲取該 condition_pair 的 OOS 結果（從 test_result 中獲取）
                        # 如果 OOS 測試失敗，test_result 可能為空
                        test_result = condition_pair_result.get("test_result", {}) if condition_pair_result else {}
                        pair_oos_results = test_result.get("individual_results", []) if test_result else []
                        pair_test_metrics = test_result.get("metrics", {}) if test_result else {}
                        
                        # 從 grid_region 中獲取參數和指標
                        pair_all_params = pair_grid_region.get("all_params", [])
                        pair_individual_is_metrics = pair_grid_region.get("individual_metrics", [])
                        pair_individual_full_metrics = pair_grid_region.get("individual_full_metrics", [])
                        
                        if not pair_all_params:
                            self.logger.warning(
                                f"窗口 {window_info.get('window_id')} condition_pair {strategy_idx + 1}: "
                                f"grid_region 中沒有參數組合"
                            )
                            continue
                        
                        # 創建 OOS 結果索引映射
                        pair_oos_result_map = {}
                        for oos_result in pair_oos_results:
                            param_idx = oos_result.get("param_index")
                            if param_idx is not None:
                                pair_oos_result_map[param_idx] = oos_result
                        
                        # 為該 condition_pair 的每個參數組合創建記錄
                        for param_idx, params in enumerate(pair_all_params):
                            # 提取參數值
                            param_dict = self._extract_params_dict(params)
                            
                            # 獲取該參數組合的 IS 績效
                            is_metric = pair_individual_is_metrics[param_idx] if param_idx < len(pair_individual_is_metrics) else None
                            full_metrics = pair_individual_full_metrics[param_idx] if param_idx < len(pair_individual_full_metrics) else None
                            
                            # 獲取 IS 指標
                            if full_metrics:
                                is_sharpe = full_metrics.get("sharpe")
                                is_calmar = full_metrics.get("calmar")
                                is_sortino = full_metrics.get("sortino")
                                is_total_return = full_metrics.get("total_return")
                                is_mdd = full_metrics.get("max_drawdown")
                            else:
                                is_sharpe = is_metric if objective == "sharpe" and is_metric is not None else None
                                is_calmar = is_metric if objective == "calmar" and is_metric is not None else None
                                is_sortino = None
                                is_total_return = None
                                is_mdd = None
                            
                            # 獲取 OOS 績效
                            oos_result = pair_oos_result_map.get(param_idx)
                            oos_sharpe = oos_result.get("sharpe") if oos_result and "sharpe" in oos_result else pair_test_metrics.get("sharpe")
                            oos_calmar = oos_result.get("calmar") if oos_result and "calmar" in oos_result else pair_test_metrics.get("calmar")
                            oos_sortino = oos_result.get("sortino") if oos_result and "sortino" in oos_result else pair_test_metrics.get("sortino")
                            oos_mdd = oos_result.get("max_drawdown") if oos_result and "max_drawdown" in oos_result else pair_test_metrics.get("max_drawdown")
                            
                            # 獲取實際時間
                            train_start_date = self._get_date_from_index(window_info.get("train_start"))
                            train_end_date = self._get_date_from_index(window_info.get("train_end"))
                            test_start_date = self._get_date_from_index(window_info.get("test_start"))
                            test_end_date = self._get_date_from_index(window_info.get("test_end"))
                            
                            row = {
                                "window_id": window_info.get("window_id"),
                                "condition_pair_id": strategy_idx + 1,  # condition_pair 編號 (1, 2, ...)
                                "param_combination_id": param_idx + 1,  # 參數組合編號 (1-9)
                                "train_start": window_info.get("train_start"),
                                "train_end": window_info.get("train_end"),
                                "test_start": window_info.get("test_start"),
                                "test_end": window_info.get("test_end"),
                                "train_start_date": train_start_date,
                                "train_end_date": train_end_date,
                                "test_start_date": test_start_date,
                                "test_end_date": test_end_date,
                                "is_sharpe": is_sharpe,
                                "is_calmar": is_calmar,
                                "is_sortino": is_sortino,
                                "is_total_return": is_total_return,
                                "is_mdd": is_mdd,
                                "is_metric": is_metric,
                                "oos_sharpe": oos_sharpe,
                                "oos_calmar": oos_calmar,
                                "oos_sortino": oos_sortino,
                                "oos_total_return": oos_result.get("return") if oos_result else pair_test_metrics.get("total_return"),
                                "oos_mdd": oos_mdd,
                            }
                            
                            # 添加參數列（保持 ENTRY, EXIT 順序）
                            param_dict_str = self._format_params_dict(param_dict)
                            row["optimal_params"] = param_dict_str
                            
                            # 不再添加單獨的參數列，因為 optimal_params 已經包含了所有參數信息
                            
                            rows.append(row)
                    
                    # 處理完所有 condition_pair 後，跳過後續的單一處理邏輯
                    continue
                else:
                    self.logger.info(
                        "[DEBUG] 沒有 all_condition_pair_results，使用單一 grid_region"
                    )
                
                # 調試：檢查 individual_full_metrics 的內容
                self.logger.info(
                    f"窗口 {window_info.get('window_id')} {objective}: "
                    f"all_params長度={len(all_params)}, "
                    f"individual_full_metrics長度={len(individual_full_metrics)}, "
                    f"individual_full_metrics內容={[type(m).__name__ if m is not None else 'None' for m in individual_full_metrics]}"
                )
                
                if all_params and len(all_params) > 1:
                    # 九宮格模式：為每個參數組合創建記錄
                    # 創建 OOS 結果索引映射
                    oos_result_map = {}
                    for oos_result in individual_oos_results:
                        param_idx = oos_result.get("param_index")
                        if param_idx is not None:
                            oos_result_map[param_idx] = oos_result
                    
                    for param_idx, params in enumerate(all_params):
                        # 提取參數值
                        param_dict = self._extract_params_dict(params)
                        
                        # 獲取該參數組合的 IS 績效（優化目標指標）
                        is_metric = individual_is_metrics[param_idx] if param_idx < len(individual_is_metrics) else None
                        
                        # 獲取該參數組合的完整 IS 指標（sharpe、calmar、total_return）
                        full_metrics = individual_full_metrics[param_idx] if param_idx < len(individual_full_metrics) else None
                        
                        # 調試：記錄 full_metrics 的內容
                        if full_metrics is None:
                            self.logger.warning(
                                f"窗口 {window_info.get('window_id')} 參數組合 {param_idx}: "
                                f"full_metrics 為 None，將使用回退邏輯"
                            )
                        else:
                            self.logger.info(
                                f"窗口 {window_info.get('window_id')} 參數組合 {param_idx}: "
                                f"full_metrics={full_metrics}"
                            )
                        
                        # 獲取該參數組合的 OOS 績效
                        oos_result = oos_result_map.get(param_idx)
                        
                        # 計算 IS Sharpe、Calmar 和 Sortino（優先從完整指標中獲取，否則使用該參數組合的指標）
                        if full_metrics:
                            # 優先使用完整指標中的值，即使為 None 也要記錄（這樣可以區分是否計算失敗）
                            is_sharpe = full_metrics.get("sharpe")
                            is_calmar = full_metrics.get("calmar")
                            is_sortino = full_metrics.get("sortino")
                            is_total_return = full_metrics.get("total_return")
                            is_mdd = full_metrics.get("max_drawdown")
                            
                            # 調試：如果值為 None，記錄警告
                            if is_sharpe is None or is_calmar is None or is_total_return is None:
                                self.logger.warning(
                                    f"窗口 {window_info.get('window_id')} 參數組合 {param_idx}: "
                                    f"full_metrics 中有 None 值: sharpe={is_sharpe}, calmar={is_calmar}, return={is_total_return}"
                                )
                        else:
                            # 如果沒有完整指標，使用優化目標指標（如果是對應的指標）或平均值
                            self.logger.warning(
                                f"窗口 {window_info.get('window_id')} 參數組合 {param_idx}: "
                                f"使用回退邏輯，train_metrics={train_metrics}"
                            )
                            is_sharpe = is_metric if objective == "sharpe" and is_metric is not None else train_metrics.get("sharpe")
                            is_calmar = is_metric if objective == "calmar" and is_metric is not None else train_metrics.get("calmar")
                            is_sortino = train_metrics.get("sortino")
                            is_total_return = train_metrics.get("total_return")
                            is_mdd = train_metrics.get("max_drawdown")
                        
                        # 獲取 OOS 績效（從 individual_results 中獲取完整指標）
                        # 優先從 individual_results 中獲取，如果沒有則從 test_metrics 中獲取
                        oos_sharpe = oos_result.get("sharpe") if oos_result and "sharpe" in oos_result else test_metrics.get("sharpe")
                        oos_calmar = oos_result.get("calmar") if oos_result and "calmar" in oos_result else test_metrics.get("calmar")
                        oos_sortino = oos_result.get("sortino") if oos_result and "sortino" in oos_result else test_metrics.get("sortino")
                        oos_mdd = oos_result.get("max_drawdown") if oos_result and "max_drawdown" in oos_result else test_metrics.get("max_drawdown")
                        
                        # 獲取實際時間
                        train_start_date = self._get_date_from_index(window_info.get("train_start"))
                        train_end_date = self._get_date_from_index(window_info.get("train_end"))
                        test_start_date = self._get_date_from_index(window_info.get("test_start"))
                        test_end_date = self._get_date_from_index(window_info.get("test_end"))
                        
                        row = {
                            "window_id": window_info.get("window_id"),
                            "param_combination_id": param_idx + 1,  # 參數組合編號 (1-9)
                            "train_start": window_info.get("train_start"),
                            "train_end": window_info.get("train_end"),
                            "test_start": window_info.get("test_start"),
                            "test_end": window_info.get("test_end"),
                            "train_start_date": train_start_date,
                            "train_end_date": train_end_date,
                            "test_start_date": test_start_date,
                            "test_end_date": test_end_date,
                            "is_sharpe": is_sharpe,
                            "is_calmar": is_calmar,
                            "is_sortino": is_sortino,
                            "is_total_return": is_total_return,
                            "is_mdd": is_mdd,
                            "is_metric": is_metric,  # 該參數組合的 IS 績效（Sharpe 或 Calmar）
                            "oos_sharpe": oos_sharpe,
                            "oos_calmar": oos_calmar,
                            "oos_sortino": oos_sortino,
                            "oos_total_return": oos_result.get("return") if oos_result else test_metrics.get("total_return"),
                            "oos_mdd": oos_mdd,
                        }
                        
                        # 添加參數列（動態添加，根據實際參數）
                        param_dict_str = self._format_params_dict(param_dict)
                        row["optimal_params"] = param_dict_str
                        
                        # 不再添加單獨的參數列，因為 optimal_params 已經包含了所有參數信息
                        
                        rows.append(row)
                else:
                    # 單一參數模式（回退）
                    optimal_params = window_result.get("optimal_params", {})
                    param_dict = self._extract_params_dict_from_optimal(optimal_params)
                    param_dict_str = self._format_params_dict(param_dict)
                    
                    # 獲取實際時間
                    train_start_date = self._get_date_from_index(window_info.get("train_start"))
                    train_end_date = self._get_date_from_index(window_info.get("train_end"))
                    test_start_date = self._get_date_from_index(window_info.get("test_start"))
                    test_end_date = self._get_date_from_index(window_info.get("test_end"))
                    
                    row = {
                        "window_id": window_info.get("window_id"),
                        "condition_pair_id": 1,  # 單一 condition_pair 模式，默認為 1
                        "param_combination_id": 1,
                        "train_start": window_info.get("train_start"),
                        "train_end": window_info.get("train_end"),
                        "test_start": window_info.get("test_start"),
                        "test_end": window_info.get("test_end"),
                        "train_start_date": train_start_date,
                        "train_end_date": train_end_date,
                        "test_start_date": test_start_date,
                        "test_end_date": test_end_date,
                        "is_sharpe": train_metrics.get("sharpe"),
                        "is_calmar": train_metrics.get("calmar"),
                        "is_total_return": train_metrics.get("total_return"),
                        "is_mdd": train_metrics.get("max_drawdown"),
                        "is_metric": train_metrics.get(objective),
                        "oos_sharpe": test_metrics.get("sharpe"),
                        "oos_calmar": test_metrics.get("calmar"),
                        "oos_total_return": test_metrics.get("total_return"),
                        "oos_mdd": test_metrics.get("max_drawdown"),
                        "optimal_params": param_dict_str,
                    }
                    
                    # 不再添加單獨的參數列，因為 optimal_params 已經包含了所有參數信息
                    
                    rows.append(row)

            if not rows:
                return

            df = pd.DataFrame(rows)

            # 生成文件名（格式：日期_品種_預測檔案名字_預測因子_wfa_目標_"8位英數亂碼"）
            # 同一策略用同一組8位英數亂碼（sharpe 和 calmar 使用同一個）
            filename_base = f"{self.filename_base_prefix}_wfa_{objective}_{self.shared_random_code}"
            
            # 導出為 parquet
            parquet_path = self.output_dir / f"{filename_base}.parquet"
            df.to_parquet(parquet_path, index=False)

            # 根據 output_csv 決定是否導出 CSV
            export_msg_lines = [f"✅ {objective.upper()} 結果已導出:", f"   Parquet: {parquet_path}"]
            
            if self.output_csv:
                csv_path = self.output_dir / f"{filename_base}.csv"
                df.to_csv(csv_path, index=False, encoding="utf-8-sig")
                export_msg_lines.append(f"   CSV: {csv_path}")
            else:
                export_msg_lines.append("   CSV: 已跳過（output_csv=false）")

            show_success("WFANALYSER", "\n".join(export_msg_lines))

        except Exception as e:
            self.logger.error(f"導出 {objective} 結果失敗: {e}")
            show_error("WFANALYSER", f"導出 {objective} 結果失敗: {e}")

    def _extract_params_dict(self, params: Dict[str, Any]) -> Dict[str, str]:
        """
        從參數配置中提取可讀的參數字典

        Args:
            params: 參數配置字典（格式：{strategy_alias: [IndicatorParams, ...]})

        Returns:
            Dict[str, str]: 可讀的參數字典（例如 {'MA1': '10', 'MA4': '20'}）
        """
        param_dict = {}
        
        try:
            for strategy_alias, param_list in params.items():
                if not param_list:
                    continue
                
                # 取第一個參數（應該只有一個）
                indicator_param = param_list[0]
                
                # 提取指標名稱（例如 "MA1_strategy_1" -> "MA1"）
                # 使用正則表達式移除所有 _strategy_X 後綴（X 為任意數字）
                indicator_name = re.sub(r"_strategy_\d+$", "", strategy_alias)
                
                # 轉換為字典並提取關鍵參數
                if hasattr(indicator_param, "to_dict"):
                    param_values = indicator_param.to_dict()
                    
                    # 根據指標類型提取關鍵參數
                    indicator_type = param_values.get("indicator_type", "")
                    
                    if indicator_type == "MA":
                        # 統一使用 plateau 格式：所有參數作為獨立鍵值對
                        period = param_values.get("period")
                        ma_type = param_values.get("ma_type")
                        short_period = param_values.get("shortMA_period")
                        long_period = param_values.get("longMA_period")
                        mode = param_values.get("mode")
                        m = param_values.get("m")
                        
                        if period is not None:
                            param_dict[f"{indicator_name}_period"] = str(period)
                        if ma_type is not None:
                            param_dict[f"{indicator_name}_ma_type"] = str(ma_type)
                        if short_period is not None:
                            param_dict[f"{indicator_name}_shortMA_period"] = str(short_period)
                        if long_period is not None:
                            param_dict[f"{indicator_name}_longMA_period"] = str(long_period)
                        if mode is not None:
                            param_dict[f"{indicator_name}_mode"] = str(mode)
                        if m is not None:
                            param_dict[f"{indicator_name}_m"] = str(m)
                    elif indicator_type == "BOLL":
                        # 統一使用 plateau 格式：所有參數作為獨立鍵值對
                        ma_length = param_values.get("ma_length")
                        std_multiplier = param_values.get("std_multiplier")
                        if ma_length is not None:
                            param_dict[f"{indicator_name}_ma_length"] = str(ma_length)
                        if std_multiplier is not None:
                            param_dict[f"{indicator_name}_std_multiplier"] = str(std_multiplier)
                    elif indicator_type == "HL":
                        # 統一使用 plateau 格式：所有參數作為獨立鍵值對
                        n_length = param_values.get("n_length")
                        m_length = param_values.get("m_length")
                        if n_length is not None:
                            param_dict[f"{indicator_name}_n_length"] = str(n_length)
                        if m_length is not None:
                            param_dict[f"{indicator_name}_m_length"] = str(m_length)
                    elif indicator_type == "PERC":
                        # 統一使用 plateau 格式：所有參數作為獨立鍵值對
                        window = param_values.get("window")
                        percentile = param_values.get("percentile")
                        m1 = param_values.get("m1")
                        m2 = param_values.get("m2")
                        if window is not None:
                            param_dict[f"{indicator_name}_window"] = str(window)
                        if percentile is not None:
                            param_dict[f"{indicator_name}_percentile"] = str(percentile)
                        if m1 is not None:
                            param_dict[f"{indicator_name}_m1"] = str(m1)
                        if m2 is not None:
                            param_dict[f"{indicator_name}_m2"] = str(m2)
                    elif indicator_type == "VALUE":
                        # 統一使用 plateau 格式：所有參數作為獨立鍵值對
                        n_length = param_values.get("n_length")
                        m_value = param_values.get("m_value")
                        m1_value = param_values.get("m1_value")
                        m2_value = param_values.get("m2_value")
                        if n_length is not None:
                            param_dict[f"{indicator_name}_n_length"] = str(n_length)
                        if m_value is not None:
                            param_dict[f"{indicator_name}_m_value"] = str(m_value)
                        if m1_value is not None:
                            param_dict[f"{indicator_name}_m1_value"] = str(m1_value)
                        if m2_value is not None:
                            param_dict[f"{indicator_name}_m2_value"] = str(m2_value)
                    else:
                        # 其他指標類型，統一使用 plateau 格式：提取所有參數作為獨立鍵值對
                        # 排除 indicator_type 和 strat_idx（這些不是參數值）
                        for param_key, param_value in param_values.items():
                            if param_key not in ["indicator_type", "strat_idx"] and param_value is not None:
                                param_dict[f"{indicator_name}_{param_key}"] = str(param_value)
                
        
        except Exception as e:
            self.logger.warning(f"提取參數字典失敗: {e}")
        
        # 返回有序字典（保持插入順序，即 ENTRY, EXIT 順序）
        # Python 3.7+ 的字典是有序的，保持原有的插入順序即可
        return param_dict

    def _extract_params_dict_from_optimal(self, optimal_params: Dict[str, Any]) -> Dict[str, str]:
        """
        從最優參數中提取可讀的參數字典

        Args:
            optimal_params: 最優參數配置

        Returns:
            Dict[str, str]: 可讀的參數字典
        """
        return self._extract_params_dict(optimal_params)

    def _format_params_dict(self, param_dict: Dict[str, str]) -> str:
        """
        格式化參數字典為字符串

        Args:
            param_dict: 參數字典（值為字符串）

        Returns:
            str: 格式化的字符串（例如 "{'MA1': '10', 'HL1_n_length': '1', 'HL1_m_length': '70'}"）
        """
        if not param_dict:
            return "{}"
        
        # 保持插入順序（ENTRY, EXIT 順序），不進行排序
        # 值已經是字符串類型，需要加引號
        formatted = "{" + ", ".join(f"'{k}': '{v}'" for k, v in param_dict.items()) + "}"
        return formatted

    def _generate_filename_base_prefix(self) -> str:
        """
        生成文件名基礎前綴（不包含最後的8位數亂碼）
        
        格式：日期_品種_預測檔案名字_預測因子
        
        Returns:
            str: 文件名基礎前綴
        """
        try:
            # 1. 日期（當前日期，格式：YYYYMMDD）
            date_str = datetime.now().strftime("%Y%m%d")
            
            # 2. 品種（從 dataloader 配置中獲取）
            symbol = "UNKNOWN"
            if self.config_data:
                dataloader_config = getattr(self.config_data, "dataloader_config", None)
                if dataloader_config:
                    source = dataloader_config.get("source", "")
                    
                    if source == "binance":
                        binance_config = dataloader_config.get("binance_config", {})
                        symbol = binance_config.get("symbol", "UNKNOWN")
                    elif source == "yfinance":
                        yfinance_config = dataloader_config.get("yfinance_config", {})
                        symbol = yfinance_config.get("symbol", "UNKNOWN")
                    elif source == "coinbase":
                        coinbase_config = dataloader_config.get("coinbase_config", {})
                        symbol = coinbase_config.get("symbol", "UNKNOWN")
                    elif source == "file":
                        file_config = dataloader_config.get("file_config", {})
                        file_path = file_config.get("file_path", "")
                        if file_path:
                            # 從文件路徑中提取品種名稱（如果可能）
                            symbol = Path(file_path).stem.replace(" ", "_")
            
            # 3. 預測檔案名字（從 predictor_config 中獲取）
            predictor_filename = "price"
            predictor_column = "X"
            
            if self.config_data:
                predictor_config = getattr(self.config_data, "predictor_config", None)
                if predictor_config:
                    predictor_path = predictor_config.get("predictor_path", "")
                    predictor_column = predictor_config.get("predictor_column", "X")
                    
                    if predictor_config.get("skip_predictor", False):
                        predictor_filename = "price"
                    elif predictor_path:
                        # 提取檔案名（不含路徑和擴展名）
                        predictor_filename = Path(predictor_path).stem
                
                # 4. 預測因子（從 backtester 配置或 predictor_config 中獲取）
                backtester_config = getattr(self.config_data, "backtester_config", None)
                if backtester_config:
                    selected_predictor = backtester_config.get("selected_predictor", predictor_column)
                    if selected_predictor:
                        predictor_column = selected_predictor
            
            # 組合文件名基礎前綴（不包含8位數亂碼，因為每個objective會有不同的亂碼）
            filename_parts = [
                date_str,
                symbol,
                predictor_filename,
                predictor_column,
            ]
            
            # 清理文件名中的無效字符
            filename_base_prefix = "_".join(str(part) for part in filename_parts if part)
            # 替換可能存在的無效字符
            invalid_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
            for char in invalid_chars:
                filename_base_prefix = filename_base_prefix.replace(char, '_')
            
            return filename_base_prefix
            
        except Exception as e:
            self.logger.warning(f"生成文件名基礎前綴失敗: {e}，使用默認名稱")
            # 回退到默認格式
            date_str = datetime.now().strftime("%Y%m%d")
            return f"{date_str}_UNKNOWN_price_X"
    
    def _get_date_from_index(self, index: Optional[int]) -> Optional[str]:
        """
        根據索引從數據中獲取實際時間
        
        Args:
            index: 數據索引
            
        Returns:
            Optional[str]: 格式化的時間字符串，如果無法獲取則返回 None
        """
        if index is None or self.data is None:
            return None
            
        try:
            if index < 0 or index >= len(self.data):
                return None
            
            # 嘗試從數據中獲取日期列（優先順序：Time > time > Date > date > datetime > DateTime）
            date_column = None
            for col in ["Time", "time", "Date", "date", "datetime", "DateTime"]:
                if col in self.data.columns:
                    date_column = col
                    break
            
            if date_column is None:
                return None
            
            # 獲取時間值
            date_value = self.data.iloc[index][date_column]
            
            # 格式化時間（根據類型自動判斷格式）
            if isinstance(date_value, pd.Timestamp):
                # 如果有時間部分，保留；否則只顯示日期
                if date_value.hour == 0 and date_value.minute == 0 and date_value.second == 0:
                    return date_value.strftime("%Y-%m-%d")
                else:
                    return date_value.strftime("%Y-%m-%d %H:%M:%S")
            elif hasattr(date_value, 'strftime'):
                return date_value.strftime("%Y-%m-%d %H:%M:%S")
            else:
                return str(date_value)
                
        except Exception as e:
            self.logger.warning(f"根據索引 {index} 獲取時間失敗: {e}")
            return None


