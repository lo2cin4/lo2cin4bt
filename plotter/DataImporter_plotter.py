"""
DataImporter_plotter.py

【功能說明】
------------------------------------------------------------
本檔案為 plotter 模組的數據導入器，負責讀取和解析 metricstracker 產生的 parquet 檔案。
支援掃描指定資料夾、解析參數組合、提取績效指標和權益曲線數據。

【關聯流程與數據流】
------------------------------------------------------------
- 主流程：掃描目錄 → 讀取檔案 → 解析參數 → 提取數據 → 返回結果
- 各模組間數據流明確，流程如下：

```mermaid
flowchart TD
    A[DataImporterPlotter] -->|掃描| B[目錄掃描]
    B -->|找到| C[Parquet檔案]
    C -->|讀取| D[DataFrame]
    D -->|解析| E[參數組合]
    D -->|提取| F[績效指標]
    D -->|提取| G[權益曲線]
    E -->|返回| H[數據字典]
    F -->|返回| H
    G -->|返回| H
```

【主流程步驟與參數傳遞細節】
------------------------------------------------------------
- 由 BasePlotter 調用，負責數據載入和解析
- DataImporterPlotter 負責檔案掃描、數據讀取、參數解析
- **每次新增/修改數據格式、參數結構時，必須同步檢查本檔案與所有依賴模組**

【維護與擴充提醒】
------------------------------------------------------------
- 新增數據格式、參數結構時，請同步更新頂部註解與對應模組
- 若 parquet 檔案格式有變動，需同步更新解析邏輯

【常見易錯點】
------------------------------------------------------------
- 檔案路徑錯誤或檔案不存在
- parquet 檔案格式不符合預期
- 參數解析邏輯錯誤
- 記憶體使用過大

【範例】
------------------------------------------------------------
- 基本使用：importer = DataImporterPlotter("path/to/data")
- 載入數據：data = importer.load_and_parse_data()

【與其他模組的關聯】
------------------------------------------------------------
- 被 BasePlotter 調用
- 依賴 metricstracker 產生的 parquet 檔案格式
- 輸出數據供 DashboardGenerator 和 CallbackHandler 使用

【維護重點】
------------------------------------------------------------
- 新增/修改數據格式、參數結構時，務必同步更新本檔案與所有依賴模組
- 檔案讀取和解析邏輯需要特別注意錯誤處理

【參考】
------------------------------------------------------------
- 詳細流程規範如有變動，請同步更新本註解與 README
- 其他模組如有依賴本檔案的行為，請於對應模組頂部註解標明
- parquet 檔案格式請參考 metricstracker 模組
"""

import os
import glob
import logging
import json
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime
import pyarrow.parquet as pq

class DataImporterPlotter:
    """
    數據導入器
    
    負責讀取和解析 metricstracker 產生的 parquet 檔案，
    提取參數組合、績效指標和權益曲線數據。
    """
    
    def __init__(self, data_path: str, logger: Optional[logging.Logger] = None):
        """
        初始化數據導入器
        
        Args:
            data_path: metricstracker 產生的 parquet 檔案目錄路徑
            logger: 日誌記錄器，預設為 None
        """
        self.data_path = data_path
        self.logger = logger or logging.getLogger(__name__)
        self.logger.setLevel(logging.WARNING)
        # 不再自動加 handler，避免預設 log 輸出
        # if not self.logger.hasHandlers():
        #     handler = logging.StreamHandler()
        #     formatter = logging.Formatter('[%(levelname)s] %(message)s')
        #     handler.setFormatter(formatter)
        #     self.logger.addHandler(handler)
        
        # 確保目錄存在
        if not os.path.exists(self.data_path):
            raise FileNotFoundError(f"數據目錄不存在: {self.data_path}")
    
    def scan_parquet_files(self) -> List[str]:
        """
        掃描目錄中的 parquet 檔案
        
        Returns:
            List[str]: parquet 檔案路徑列表
        """
        try:
            pattern = os.path.join(self.data_path, "*.parquet")
            parquet_files = glob.glob(pattern)
            
            if not parquet_files:
                self.logger.warning(f"在目錄 {self.data_path} 中未找到 parquet 檔案")
                return []
            
            return sorted(parquet_files)
            
        except Exception as e:
            self.logger.error(f"掃描 parquet 檔案失敗: {e}")
            raise
    
    def parse_parameters_from_filename(self, filename: str) -> Dict[str, Any]:
        """
        從檔案名稱解析參數組合
        
        Args:
            filename: 檔案名稱
            
        Returns:
            Dict[str, Any]: 解析出的參數字典
        """
        try:
            # 移除路徑和副檔名
            basename = os.path.basename(filename)
            name_without_ext = os.path.splitext(basename)[0]
            
            # 預設參數結構
            parameters = {
                'filename': basename,
                'reference_code': '',
                'parameters': {}
            }
            
            # 嘗試解析檔案名稱中的參數
            # 格式範例: 20250718_5ey6hl0q_metrics.parquet
            if '_metrics' in name_without_ext:
                parts = name_without_ext.split('_')
                if len(parts) >= 2:
                    parameters['reference_code'] = parts[1]
            
            return parameters
            
        except Exception as e:
            self.logger.warning(f"解析檔案名稱參數失敗 {filename}: {e}")
            return {'filename': os.path.basename(filename), 'reference_code': '', 'parameters': {}}
    
    def extract_metrics_from_metadata(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        從 DataFrame 的 metadata 中提取績效指標
        
        Args:
            df: 包含 metadata 的 DataFrame
            
        Returns:
            Dict[str, Any]: 績效指標字典
        """
        try:
            metrics = {}
            
            # 檢查是否有 metadata
            if hasattr(df, 'metadata') and df.metadata:
                # 嘗試解析 strategy_metrics1
                if 'strategy_metrics1' in df.metadata:
                    try:
                        strategy_metrics = json.loads(df.metadata['strategy_metrics1'])
                        metrics.update(strategy_metrics)
                    except (json.JSONDecodeError, TypeError) as e:
                        self.logger.warning(f"解析 strategy_metrics1 失敗: {e}")
                
                # 嘗試解析 bah_metrics1
                if 'bah_metrics1' in df.metadata:
                    try:
                        bah_metrics = json.loads(df.metadata['bah_metrics1'])
                        metrics.update({f"bah_{k}": v for k, v in bah_metrics.items()})
                    except (json.JSONDecodeError, TypeError) as e:
                        self.logger.warning(f"解析 bah_metrics1 失敗: {e}")
            
            return metrics
            
        except Exception as e:
            self.logger.warning(f"提取績效指標失敗: {e}")
            return {}
    
    def extract_equity_curve_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        提取權益曲線數據
        
        Args:
            df: 原始 DataFrame
            
        Returns:
            pd.DataFrame: 權益曲線數據
        """
        try:
            # 確保必要的欄位存在
            required_columns = ['Time', 'Equity_value']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                self.logger.warning(f"缺少必要欄位: {missing_columns}")
                return pd.DataFrame()
            
            # 提取權益曲線相關欄位
            equity_columns = ['Time', 'Equity_value', 'Change']
            available_columns = [col for col in equity_columns if col in df.columns]
            
            equity_data = df[available_columns].copy()
            
            # 確保 Time 欄位為 datetime 格式
            if 'Time' in equity_data.columns:
                equity_data['Time'] = pd.to_datetime(equity_data['Time'])
                equity_data = equity_data.sort_values('Time')
            
            return equity_data
            
        except Exception as e:
            self.logger.warning(f"提取權益曲線數據失敗: {e}")
            return pd.DataFrame()
    
    def load_parquet_file(self, file_path: str) -> List[Dict[str, Any]]:
        """
        載入單個 parquet 檔案
        
        Args:
            file_path: parquet 檔案路徑
            
        Returns:
            List[Dict[str, Any]]: 包含數據和元信息的字典列表
        """
        try:
            self.logger.info(f"載入檔案: {file_path}")
            
            # 讀取 parquet 檔案
            df = pd.read_parquet(file_path)
            table = pq.read_table(file_path)
            meta = table.schema.metadata or {}
            batch_metadata = []
            if b'batch_metadata' in meta:
                batch_metadata = json.loads(meta[b'batch_metadata'].decode())
            else:
                self.logger.warning(f"找不到 batch_metadata: {file_path}")
            
            results = []
            for meta_item in batch_metadata:
                backtest_id = meta_item.get('Backtest_id')
                if backtest_id is not None and 'Backtest_id' in df.columns:
                    df_bt = df[df['Backtest_id'] == backtest_id]
                else:
                    df_bt = df
                equity_curve = df_bt[['Time', 'Equity_value']] if 'Equity_value' in df_bt.columns else None
                bah_curve = df_bt[['Time', 'BAH_Equity']] if 'BAH_Equity' in df_bt.columns else None
                results.append({
                    'file_path': file_path,
                    'Backtest_id': backtest_id,
                    'metrics': meta_item,
                    'equity_curve': equity_curve,
                    'bah_curve': bah_curve,
                    'df': df_bt
                })
                self.logger.debug(f"Backtest_id={backtest_id} metrics={meta_item}")
                self.logger.debug(f"equity_curve len={len(equity_curve) if equity_curve is not None else 'None'}")
                self.logger.debug(f"bah_curve len={len(bah_curve) if bah_curve is not None else 'None'}")
            return results
            
        except Exception as e:
            self.logger.error(f"載入檔案失敗 {file_path}: {e}")
            raise
    
    def load_and_parse_data(self) -> Dict[str, Any]:
        """
        載入並解析所有選定的 parquet 檔案，並合併所有 Backtest_id 資料
        """
        try:
            # 掃描檔案
            parquet_files = self.scan_parquet_files()
            if not parquet_files:
                raise FileNotFoundError("未找到任何 parquet 檔案")

            # 互動式選單
            print("[Plotter] 可選擇的 parquet 檔案：")
            for i, f in enumerate(parquet_files, 1):
                print(f"  {i}. {os.path.basename(f)}")
            file_input = input("請輸入要載入的檔案編號（可用逗號分隔多選，或輸入all全選，預設all）：").strip() or 'all'
            if file_input.lower() in ['all', 'al', 'a']:
                selected_files = parquet_files
            else:
                try:
                    idxs = [int(x.strip())-1 for x in file_input.split(',') if x.strip().isdigit()]
                    selected_files = [parquet_files[i] for i in idxs if 0 <= i < len(parquet_files)]
                    if not selected_files:
                        print("[Plotter][WARNING] 輸入無效，預設載入全部檔案。")
                        selected_files = parquet_files
                except Exception:
                    print("[Plotter][WARNING] 輸入無效，預設載入全部檔案。")
                    selected_files = parquet_files

            # 載入所有選定檔案
            all_backtest_ids = []
            all_metrics = {}
            all_equity_curves = {}
            all_bah_curves = {}
            all_file_paths = {}
            all_parameters = []
            for file_path in selected_files:
                try:
                    file_data = self.load_parquet_file(file_path)
                    for item in file_data:
                        backtest_id = item['Backtest_id']
                        if backtest_id is not None:
                            all_backtest_ids.append(backtest_id)
                            all_parameters.append(item['metrics'])
                            all_metrics[backtest_id] = item['metrics']
                            all_equity_curves[backtest_id] = item['equity_curve']
                            all_bah_curves[backtest_id] = item['bah_curve']
                            all_file_paths[backtest_id] = item['file_path']
                except Exception as e:
                    self.logger.error(f"處理檔案失敗 {file_path}: {e}")
                    continue
            if not all_parameters:
                raise ValueError("沒有成功載入任何檔案或找到 Backtest_id")
            result = {
                'dataframes': all_metrics,
                'parameters': all_parameters,
                'metrics': all_metrics,
                'equity_curves': all_equity_curves,
                'bah_curves': all_bah_curves,
                'file_paths': all_file_paths,
                'backtest_ids': all_backtest_ids,  # 新增Backtest_id列表
                'total_files': len(selected_files),
                'loaded_at': datetime.now().isoformat()
            }
            return result
        except Exception as e:
            self.logger.error(f"載入和解析數據失敗: {e}")
            raise
    
    def get_parameter_summary(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        獲取參數摘要信息
        
        Args:
            data: 載入的數據字典
            
        Returns:
            Dict[str, Any]: 參數摘要
        """
        try:
            parameters = data.get('parameters', {})
            
            # 統計參數分布
            param_summary = {}
            for param_key, param_data in parameters.items():
                param_dict = param_data.get('parameters', {})
                for key, value in param_dict.items():
                    if key not in param_summary:
                        param_summary[key] = set()
                    param_summary[key].add(str(value))
            
            # 轉換為列表
            for key in param_summary:
                param_summary[key] = sorted(list(param_summary[key]))
            
            return {
                'total_combinations': len(parameters),
                'parameter_distribution': param_summary,
                'parameter_keys': list(param_summary.keys())
            }
            
        except Exception as e:
            self.logger.warning(f"獲取參數摘要失敗: {e}")
            return {}
    
    def filter_data_by_parameters(self, data: Dict[str, Any], filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        根據參數篩選數據
        
        Args:
            data: 原始數據字典
            filters: 篩選條件字典
            
        Returns:
            Dict[str, Any]: 篩選後的數據字典
        """
        try:
            if not filters:
                return data
            
            parameters = data.get('parameters', {})
            filtered_keys = set(parameters.keys())
            
            # 應用篩選條件
            for param_name, param_values in filters.items():
                if not param_values:  # 空值表示不過濾
                    continue
                
                matching_keys = set()
                for key, param_data in parameters.items():
                    param_dict = param_data.get('parameters', {})
                    if param_name in param_dict:
                        param_value = str(param_dict[param_name])
                        if param_value in param_values:
                            matching_keys.add(key)
                
                filtered_keys = filtered_keys.intersection(matching_keys)
            
            # 構建篩選後的數據
            filtered_data = {}
            for key in filtered_keys:
                filtered_data[key] = data.get(key, {})
            
            return filtered_data
            
        except Exception as e:
            self.logger.warning(f"參數篩選失敗: {e}")
            return data 

    @staticmethod
    def parse_all_parameters(parameters: list) -> dict:
        """
        動態展開所有 Entry_params/Exit_params，回傳 {參數名: [所有值]}，同一參數只列一次。
        """
        param_values = {}
        for param in parameters:
            for key in ['Entry_params', 'Exit_params']:
                if key in param and isinstance(param[key], list):
                    for d in param[key]:
                        if isinstance(d, dict):
                            for k, v in d.items():
                                if k not in param_values:
                                    param_values[k] = set()
                                param_values[k].add(str(v))
        # 轉成 list 並排序
        for k in param_values:
            param_values[k] = sorted(list(param_values[k]))
        return param_values 

    @staticmethod
    def parse_entry_exit_parameters(parameters: list):
        """
        分別展開 Entry_params/Exit_params，回傳 (entry_param_values, exit_param_values)
        """
        entry_param_values = {}
        exit_param_values = {}
        for param in parameters:
            if 'Entry_params' in param and isinstance(param['Entry_params'], list):
                for d in param['Entry_params']:
                    if isinstance(d, dict):
                        for k, v in d.items():
                            if k not in entry_param_values:
                                entry_param_values[k] = set()
                            entry_param_values[k].add(str(v))
            if 'Exit_params' in param and isinstance(param['Exit_params'], list):
                for d in param['Exit_params']:
                    if isinstance(d, dict):
                        for k, v in d.items():
                            if k not in exit_param_values:
                                exit_param_values[k] = set()
                            exit_param_values[k].add(str(v))
        for k in entry_param_values:
            entry_param_values[k] = sorted(list(entry_param_values[k]))
        for k in exit_param_values:
            exit_param_values[k] = sorted(list(exit_param_values[k]))
        return entry_param_values, exit_param_values 

    @staticmethod
    def parse_indicator_param_structure(parameters: list):
        """
        統計所有 entry/exit 下 indicator_type 及其所有參數名與值：
        回傳 {
            'entry': {indicator_type: {param: [值]}},
            'exit': {indicator_type: {param: [值]}}
        }
        """
        result = {'entry': {}, 'exit': {}}
        for param in parameters:
            for key, target in [('Entry_params', 'entry'), ('Exit_params', 'exit')]:
                if key in param and isinstance(param[key], list):
                    for d in param[key]:
                        if isinstance(d, dict):
                            indicator_type = str(d.get('indicator_type', 'Unknown'))
                            if indicator_type not in result[target]:
                                result[target][indicator_type] = {}
                            for k, v in d.items():
                                if k == 'indicator_type':
                                    continue
                                if k not in result[target][indicator_type]:
                                    result[target][indicator_type][k] = set()
                                result[target][indicator_type][k].add(str(v))
        # 轉成 list 並排序
        for target in result:
            for ind in result[target]:
                for k in result[target][ind]:
                    result[target][ind][k] = sorted(list(result[target][ind][k]))
        return result 