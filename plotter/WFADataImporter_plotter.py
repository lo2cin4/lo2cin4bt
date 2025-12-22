"""
WFADataImporter_plotter.py

【功能說明】
------------------------------------------------------------
本模組為 WFA 可視化平台的數據導入核心模組，負責讀取和解析 wfanalyser 產生的 parquet 檔案，
支援掃描指定資料夾、解析窗口數據、構建九宮格參數矩陣、提取 IS 和 OOS 指標。

【流程與數據流】
------------------------------------------------------------
- 主流程：掃描目錄 → 讀取檔案 → 解析窗口 → 構建九宮格矩陣 → 組織數據 → 返回結果

【維護與擴充重點】
------------------------------------------------------------
- 新增數據格式、參數結構時，請同步更新頂部註解與對應模組
- 若 parquet 檔案格式有變動，需同步更新解析邏輯

【常見易錯點】
------------------------------------------------------------
- 檔案路徑錯誤或檔案不存在
- parquet 檔案格式不符合預期
- 九宮格矩陣構建邏輯錯誤

【錯誤處理】
------------------------------------------------------------
- 檔案不存在時提供詳細錯誤訊息
- 解析失敗時提供診斷建議

【範例】
------------------------------------------------------------
- 基本使用：importer = WFADataImporter("path/to/wfanalyser")
- 載入數據：data = importer.load_all_wfa_files()

【與其他模組的關聯】
------------------------------------------------------------
- 被 WFAVisualizationPlotter 調用，提供 WFA 數據導入功能
- 依賴 wfanalyser 產生的 parquet 檔案格式
- 輸出數據供 WFADashboardGenerator 和 WFACallbackHandler 使用
- 使用 plotter/utils/FileUtils_utils_plotter.py 進行文件掃描

【參考】
------------------------------------------------------------
- WFAVisualization_plotter.py: WFA 可視化平台主類
- WFADashboardGenerator_plotter.py: WFA 界面生成器
- plotter/README.md: WFA 可視化平台詳細說明
"""

import ast
import glob
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from rich.text import Text

from utils import show_step_panel, show_warning, show_info


class WFADataImporter:
    """
    WFA 數據導入器

    負責讀取和解析 wfanalyser 產生的 parquet 檔案，
    提取窗口數據、構建九宮格參數矩陣、組織 IS 和 OOS 指標。
    """

    def __init__(
        self,
        wfa_data_path: Optional[str] = None,
        metrics_data_path: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """
        初始化 WFA 數據導入器

        Args:
            wfa_data_path: wfanalyser 產生的 parquet 檔案目錄路徑
            metrics_data_path: metricstracker 產生的 parquet 檔案目錄路徑（可選，用於獲取 Sortino）
            logger: 日誌記錄器，預設為 None
        """
        # 設置默認路徑
        base_dir = os.path.dirname(os.path.dirname(__file__))
        self.wfa_data_path = (
            wfa_data_path
            or os.path.join(base_dir, "records", "wfanalyser")
        )
        self.metrics_data_path = (
            metrics_data_path
            or os.path.join(base_dir, "records", "metricstracker")
        )

        self.logger = logger or logging.getLogger(__name__)
        self.logger.setLevel(logging.WARNING)
        from utils import get_console
        self.console = get_console()

        # 確保目錄存在
        if not os.path.exists(self.wfa_data_path):
            raise FileNotFoundError(f"WFA 數據目錄不存在: {self.wfa_data_path}")

    def scan_wfa_parquet_files(self) -> List[str]:
        """
        掃描 wfanalyser 目錄中的 parquet 檔案

        Returns:
            List[str]: parquet 檔案路徑列表
        """
        from .utils.FileUtils_utils_plotter import scan_parquet_files as scan_files
        
        return scan_files(self.wfa_data_path, self.logger)

    def parse_optimal_params(self, params_str: str) -> Dict[str, Any]:
        """
        解析 optimal_params 字符串

        Args:
            params_str: 參數字符串，格式如 "{'MA1': 50, 'MA4': 110}"

        Returns:
            Dict[str, Any]: 解析後的參數字典
        """
        try:
            # 嘗試使用 ast.literal_eval 安全解析
            if isinstance(params_str, str):
                params_dict = ast.literal_eval(params_str)
                return params_dict if isinstance(params_dict, dict) else {}
            elif isinstance(params_str, dict):
                return params_str
            else:
                return {}
        except (ValueError, SyntaxError) as e:
            self.logger.warning(f"解析 optimal_params 失敗: {params_str}, 錯誤: {e}")
            return {}

    def _extract_indicator_combination(self, row: pd.Series) -> str:
        """
        從數據行中提取指標組合名稱（格式與參數高原一致：Entry: PERC2 | Exit: PERC3）

        Args:
            row: 數據行（包含 optimal_params）

        Returns:
            str: 指標組合名稱，格式如 "Entry: MA1 | Exit: MA4" 或 "Entry: MA1,MA9 | Exit: MA4"
        """
        try:
            params_str = row.get("optimal_params", "{}")
            param_dict = self.parse_optimal_params(params_str)
            
            if not param_dict:
                # 如果無法解析，返回默認格式
                condition_pair_id = row.get("condition_pair_id", 1)
                return f"策略 {condition_pair_id}"
            
            # 從參數鍵中提取指標名稱（新格式：{indicator_name}_{param_name}）
            # 例如：MA1_period, MA1_ma_type, HL1_n_length, HL1_m_length
            # 需要提取唯一的指標名稱前綴
            indicator_names = set()
            
            for key in param_dict.keys():
                # 移除參數後綴（如 _period, _ma_type, _n_length, _m_length 等）
                # 匹配模式：指標名稱（字母+數字）後跟下劃線和參數名
                match = re.match(r'^([A-Z]+\d+)_', key)
                if match:
                    indicator_name = match.group(1)  # 例如 MA1, HL1, PERC2
                    indicator_names.add(indicator_name)
                else:
                    # 如果沒有匹配標準格式，嘗試提取基礎名稱
                    base_name = key.split('_')[0]
                    if re.match(r'^[A-Z]+\d+$', base_name):
                        indicator_names.add(base_name)
            
            if not indicator_names:
                condition_pair_id = row.get("condition_pair_id", 1)
                return f"策略 {condition_pair_id}"
            
            # 根據 optimal_params 中參數的出現順序判斷 entry/exit
            # 通常 Entry 指標的參數會先出現，Exit 指標的參數後出現
            entry_indicators = []
            exit_indicators = []
            
            # 獲取所有指標及其在參數鍵中首次出現的順序
            indicator_order = {}
            for key in sorted(param_dict.keys()):
                match = re.match(r'^([A-Z]+\d+)_', key)
                if match:
                    indicator_name = match.group(1)
                    if indicator_name not in indicator_order:
                        indicator_order[indicator_name] = len(indicator_order)
            
            # 按照在參數中出現的順序排序指標
            sorted_indicators = sorted(indicator_names, key=lambda x: indicator_order.get(x, 999))
            
            # 如果只有兩個指標，第一個通常是 Entry，第二個通常是 Exit
            # 如果有多個指標，根據在參數中出現的順序：前面的作為 Entry，後面的作為 Exit
            if len(sorted_indicators) == 2:
                entry_indicators.append(sorted_indicators[0])
                exit_indicators.append(sorted_indicators[1])
            elif len(sorted_indicators) == 1:
                # 只有一個指標時，無法判斷，默認作為 Entry
                entry_indicators.append(sorted_indicators[0])
            else:
                # 多個指標時，按照出現順序：前面的作為 Entry，後面的作為 Exit
                # 通常 Entry 指標會先出現
                mid_point = len(sorted_indicators) // 2
                entry_indicators = sorted_indicators[:mid_point]
                exit_indicators = sorted_indicators[mid_point:]
            
            # 去重並排序
            entry_indicators = sorted(set(entry_indicators))
            exit_indicators = sorted(set(exit_indicators))
            
            # 構建顯示名稱（與參數高原格式一致，使用 | 分隔多個指標）
            if entry_indicators and exit_indicators:
                entry_str = " | ".join(entry_indicators)
                exit_str = " | ".join(exit_indicators)
                return f"Entry: {entry_str} | Exit: {exit_str}"
            elif entry_indicators:
                entry_str = " | ".join(entry_indicators)
                return f"Entry: {entry_str}"
            elif exit_indicators:
                exit_str = " | ".join(exit_indicators)
                return f"Exit: {exit_str}"
            else:
                condition_pair_id = row.get("condition_pair_id", 1)
                return f"策略 {condition_pair_id}"
                
        except Exception as e:
            self.logger.warning(f"提取指標組合失敗: {e}")
            condition_pair_id = row.get("condition_pair_id", 1)
            return f"策略 {condition_pair_id}"
    
    def _extract_indicator_combination_from_param_info(self, param_info: Optional[Dict[str, Any]], row: pd.Series) -> str:
        """
        從 param_info 中提取指標組合名稱（格式與參數高原一致）
        這是更準確的方法，因為 param_info 中包含正確的參數鍵信息

        Args:
            param_info: build_3x3_matrix 返回的參數信息字典
            row: 數據行（用於獲取 condition_pair_id 作為後備）

        Returns:
            str: 指標組合名稱，格式如 "Entry: BOLL2 | Exit: BOLL3"
        """
        try:
            if not param_info:
                condition_pair_id = row.get("condition_pair_id", 1)
                return f"策略 {condition_pair_id}"
            
            param1_key = param_info.get("param1_key", "")
            param2_key = param_info.get("param2_key", "")
            
            # 從參數鍵中提取指標名稱（格式：{indicator_name}_{param_name}）
            # 例如：BOLL2_ma_length -> BOLL2
            entry_indicator = None
            exit_indicator = None
            
            # 提取指標名稱
            if param1_key:
                match = re.match(r'^([A-Z]+\d+)_', param1_key)
                if match:
                    entry_indicator = match.group(1)  # param1 對應 Entry（Y軸，行）
            
            if param2_key:
                match = re.match(r'^([A-Z]+\d+)_', param2_key)
                if match:
                    exit_indicator = match.group(1)  # param2 對應 Exit（X軸，列）
            
            # 如果沒有找到，使用舊方法作為後備
            if not entry_indicator and not exit_indicator:
                return self._extract_indicator_combination(row)
            
            # 構建顯示名稱（使用 | 分隔多個指標，與參數高原格式一致）
            if entry_indicator and exit_indicator:
                return f"Entry: {entry_indicator} | Exit: {exit_indicator}"
            elif entry_indicator:
                return f"Entry: {entry_indicator}"
            elif exit_indicator:
                return f"Exit: {exit_indicator}"
            else:
                condition_pair_id = row.get("condition_pair_id", 1)
                return f"策略 {condition_pair_id}"
                
        except Exception as e:
            self.logger.warning(f"從 param_info 提取指標組合失敗: {e}")
            # 使用舊方法作為後備
            return self._extract_indicator_combination(row)
    
    def build_3x3_matrix(
        self, window_data: pd.DataFrame
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        構建 3x3 參數矩陣

        Args:
            window_data: 單個窗口的數據（應包含 9 行，對應 param_combination_id 1-9）

        Returns:
            Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
                - 矩陣數據字典 {metric_name: 3x3 numpy array}
                - 參數信息字典（包含參數名稱和值列表）
        """
        try:
            # 檢查數據是否包含 9 行
            if len(window_data) != 9:
                self.logger.warning(
                    f"窗口數據不包含 9 行，實際有 {len(window_data)} 行"
                )
                return None, None

            # param_combination_id 到矩陣位置的映射（固定位置）：
            # 1: (0,0)  2: (0,1)  3: (0,2)
            # 4: (1,0)  5: (1,1)  6: (1,2)
            # 7: (2,0)  8: (2,1)  9: (2,2)
            position_map = {
                1: (0, 0), 2: (0, 1), 3: (0, 2),
                4: (1, 0), 5: (1, 1), 6: (1, 2),
                7: (2, 0), 8: (2, 1), 9: (2, 2),
            }

            # 初始化 3x3 矩陣（用於存儲各種指標）
            matrices = {
                "is_sharpe": np.full((3, 3), np.nan),
                "is_calmar": np.full((3, 3), np.nan),
                "is_sortino": np.full((3, 3), np.nan),
                "is_total_return": np.full((3, 3), np.nan),
                "is_mdd": np.full((3, 3), np.nan),
                "is_metric": np.full((3, 3), np.nan),
                "oos_sharpe": np.full((3, 3), np.nan),
                "oos_calmar": np.full((3, 3), np.nan),
                "oos_sortino": np.full((3, 3), np.nan),
                "oos_total_return": np.full((3, 3), np.nan),
                "oos_mdd": np.full((3, 3), np.nan),
            }

            # 解析所有參數組合的 optimal_params，用於提取參數信息
            param_dicts = []
            param_values_map = {}  # 存儲每個位置的參數值

            for idx, row in window_data.iterrows():
                param_comb_id = int(row.get("param_combination_id"))
                if param_comb_id not in position_map:
                    continue

                row_idx, col_idx = position_map[param_comb_id]

                # 解析參數
                params_str = row.get("optimal_params", "{}")
                param_dict = self.parse_optimal_params(params_str)
                param_dicts.append(param_dict)
                param_values_map[(row_idx, col_idx)] = param_dict

                # 填充指標值到矩陣
                matrices["is_sharpe"][row_idx, col_idx] = row.get("is_sharpe")
                matrices["is_calmar"][row_idx, col_idx] = row.get("is_calmar")
                matrices["is_sortino"][row_idx, col_idx] = row.get("is_sortino")
                matrices["is_total_return"][row_idx, col_idx] = row.get("is_total_return")
                matrices["is_mdd"][row_idx, col_idx] = row.get("is_mdd")
                matrices["is_metric"][row_idx, col_idx] = row.get("is_metric")
                matrices["oos_sharpe"][row_idx, col_idx] = row.get("oos_sharpe")
                matrices["oos_calmar"][row_idx, col_idx] = row.get("oos_calmar")
                matrices["oos_sortino"][row_idx, col_idx] = row.get("oos_sortino")
                matrices["oos_total_return"][row_idx, col_idx] = row.get("oos_total_return")
                matrices["oos_mdd"][row_idx, col_idx] = row.get("oos_mdd")

            # 提取參數信息（用於顯示軸標籤）
            if not param_dicts or not param_dicts[0]:
                self.logger.warning("無法提取參數信息")
                return None, None

            all_keys = list(param_dicts[0].keys())

            # 找出可變參數（在不同組合中有不同值的參數）
            variable_params = []
            for key in all_keys:
                values = [d.get(key) for d in param_dicts if key in d]
                unique_values = sorted(set(values))
                if len(unique_values) > 1:
                    variable_params.append((key, unique_values))

            # 如果沒有找到可變參數或可變參數超過 2 個，使用前兩個參數鍵
            if len(variable_params) == 0:
                if len(all_keys) >= 2:
                    variable_params = [
                        (all_keys[0], sorted(set([d.get(all_keys[0]) for d in param_dicts]))),
                        (all_keys[1], sorted(set([d.get(all_keys[1]) for d in param_dicts]))),
                    ]
                else:
                    self.logger.warning("無法確定可變參數")
                    return None, None
            elif len(variable_params) > 2:
                variable_params = variable_params[:2]

            param1_key, param1_values = variable_params[0]
            param2_key, param2_values = variable_params[1] if len(variable_params) > 1 else (None, [])

            # 根據矩陣位置提取參數值順序
            # 行（第一維）：從位置 (0,0), (1,0), (2,0) 提取 param1 的值
            row_param_values = []
            for r in range(3):
                if (r, 0) in param_values_map:
                    val = param_values_map[(r, 0)].get(param1_key)
                    if val is not None:
                        row_param_values.append(val)

            # 列（第二維）：從位置 (0,0), (0,1), (0,2) 提取 param2 的值
            col_param_values = []
            if param2_key:
                for c in range(3):
                    if (0, c) in param_values_map:
                        val = param_values_map[(0, c)].get(param2_key)
                        if val is not None:
                            col_param_values.append(val)

            # 參數信息
            param_info = {
                "param1_key": param1_key,
                "param1_values": row_param_values if row_param_values else sorted(param1_values),
                "param2_key": param2_key,
                "param2_values": col_param_values if col_param_values else (sorted(param2_values) if param2_key else []),
            }

            return matrices, param_info

        except Exception as e:
            self.logger.error(f"構建 3x3 矩陣失敗: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None, None

    def load_wfa_file(self, file_path: str) -> Dict[str, Any]:
        """
        載入單個 WFA parquet 檔案

        Args:
            file_path: parquet 檔案路徑

        Returns:
            Dict[str, Any]: 解析後的數據字典，包含：
                - "filename": 檔案名稱
                - "windows": 窗口數據字典 {window_id: {...}}
                - "strategies": 策略列表
        """
        try:
            # 讀取 parquet 檔案
            df = pd.read_parquet(file_path)
            
            # 嘗試從 parquet metadata 中讀取 Entry/Exit 配置
            try:
                import pyarrow.parquet as pq
                parquet_file = pq.ParquetFile(file_path)
                metadata = parquet_file.metadata.metadata
                if metadata:
                    # 嘗試解析 metadata 中的 Entry/Exit 信息
                    # 這裡可以根據實際的 metadata 格式來解析
                    pass
            except Exception:
                # 如果無法讀取 metadata，繼續使用推斷方法
                pass

            # 提取檔案名稱
            filename = os.path.basename(file_path)

            # 按 window_id 和 condition_pair_id 分組
            windows_data = {}
            strategies = set()
            strategy_names = {}  # 存儲策略名稱映射 {strategy_key: indicator_combination}

            for (window_id, condition_pair_id), group in df.groupby(["window_id", "condition_pair_id"]):
                strategy_key = f"strategy_{condition_pair_id}"
                strategies.add(strategy_key)
                
                # 構建 3x3 矩陣（先構建，以便從 param_info 中獲取正確的 Entry/Exit 信息）
                matrices, param_info = self.build_3x3_matrix(group)
                
                # 從 param_info 中提取指標組合信息（這樣能獲取正確的 Entry/Exit）
                if strategy_key not in strategy_names:
                    indicator_combination = self._extract_indicator_combination_from_param_info(param_info, group.iloc[0])
                    strategy_names[strategy_key] = indicator_combination

                if matrices is None:
                    self.logger.warning(
                        f"窗口 {window_id} 策略 {condition_pair_id} 無法構建矩陣"
                    )
                    continue

                # 獲取窗口的時間信息（從第一行獲取）
                first_row = group.iloc[0]
                window_info = {
                    "window_id": window_id,
                    "condition_pair_id": condition_pair_id,
                    "train_start_date": first_row.get("train_start_date"),
                    "train_end_date": first_row.get("train_end_date"),
                    "test_start_date": first_row.get("test_start_date"),
                    "test_end_date": first_row.get("test_end_date"),
                    "matrices": matrices,
                    "param_info": param_info,
                }

                window_key = f"window_{window_id}_strategy_{condition_pair_id}"
                windows_data[window_key] = window_info

            return {
                "filename": filename,
                "file_path": file_path,
                "windows": windows_data,
                "strategies": sorted(list(strategies)),
                "strategy_names": strategy_names,  # 策略名稱映射
            }

        except Exception as e:
            self.logger.error(f"載入 WFA 檔案失敗 {file_path}: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            raise

    def load_wfa_files_interactive(self) -> List[Dict[str, Any]]:
        """
        互動式載入 WFA parquet 檔案（支援選擇檔案）

        Returns:
            List[Dict[str, Any]]: 選中檔案的解析結果列表
        """
        try:
            parquet_files = self.scan_wfa_parquet_files()
            if not parquet_files:
                self.logger.warning("未找到任何 WFA parquet 檔案")
                return []

            # 互動式選單
            step_content = (
                "🟢 選擇要載入的 WFA 檔案\n"
                "🟢 生成可視化介面[自動]\n"
                "\n"
                "[bold #dbac30]說明[/bold #dbac30]\n"
                "此步驟用於選擇要載入的 WFA parquet 檔案，支援多檔案同時載入。\n"
                "檔案包含 WFA 分析的窗口數據和參數組合結果。\n\n"
                "[bold #dbac30]檔案選擇格式：[/bold #dbac30]\n"
                "• 不導入：輸入 0\n"
                "• 單一檔案：輸入數字（如 1）\n"
                "• 多檔案：用逗號分隔（如 1,2,3）\n"
                "• 全部檔案：直接按 Enter\n\n"
                "[bold #dbac30]可選擇的 WFA parquet 檔案：[/bold #dbac30]"
            )

            # 準備檔案列表（添加 0：不導入 選項）
            file_list = "  [bold #dbac30]0.[/bold #dbac30] 跳過\n"
            for i, f in enumerate(parquet_files, 1):
                file_list += (
                    f"  [bold #dbac30]{i}.[/bold #dbac30] {os.path.basename(f)}\n"
                )

            # 組合完整內容並顯示
            complete_content = step_content + "\n" + file_list
            show_step_panel("PLOTTER", 1, ["數據選擇"], complete_content)

            # 用戶輸入提示
            self.console.print("[bold #dbac30]輸入 WFA 檔案號碼：[/bold #dbac30]")
            file_input = input().strip() or "all"

            # 處理「0：跳過」選項
            if file_input == "0":
                return []

            if not file_input or file_input.lower() == "all":
                selected_files = parquet_files
            else:
                try:
                    # 解析用戶輸入的檔案編號
                    file_indices = [int(x.strip()) for x in file_input.split(",")]
                    selected_files = [
                        parquet_files[i - 1]
                        for i in file_indices
                        if 1 <= i <= len(parquet_files)
                    ]
                    if not selected_files:
                        show_warning("PLOTTER", "沒有選擇有效的檔案，預設載入全部檔案。")
                        selected_files = parquet_files
                except (ValueError, IndexError):
                    show_info("PLOTTER", "🔔 已自動載入全部檔案。")
                    selected_files = parquet_files

            # 載入選定的檔案
            all_data = []
            for file_path in selected_files:
                try:
                    file_data = self.load_wfa_file(file_path)
                    all_data.append(file_data)
                except Exception as e:
                    self.logger.error(f"載入檔案失敗 {file_path}: {e}")
                    continue

            return all_data

        except Exception as e:
            self.logger.error(f"載入 WFA 檔案失敗: {e}")
            raise

    def load_all_wfa_files(self) -> List[Dict[str, Any]]:
        """
        載入所有 WFA parquet 檔案（無互動選擇）

        Returns:
            List[Dict[str, Any]]: 所有檔案的解析結果列表
        """
        try:
            parquet_files = self.scan_wfa_parquet_files()
            if not parquet_files:
                return []

            all_data = []
            for file_path in parquet_files:
                try:
                    file_data = self.load_wfa_file(file_path)
                    all_data.append(file_data)
                except Exception as e:
                    self.logger.error(f"載入檔案失敗 {file_path}: {e}")
                    continue

            return all_data

        except Exception as e:
            self.logger.error(f"載入所有 WFA 檔案失敗: {e}")
            raise

