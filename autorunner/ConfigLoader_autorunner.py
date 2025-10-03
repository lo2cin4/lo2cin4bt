"""
ConfigLoader_autorunner.py

【功能說明】
------------------------------------------------------------
本模組負責配置文件載入功能，從 JSON 文件中讀取配置數據，
解析和轉換配置參數，為後續模組提供標準化的配置數據結構。

【流程與數據流】
------------------------------------------------------------
- 主流程：讀取文件 → 解析 JSON → 驗證數據 → 轉換格式 → 返回配置
- 數據流：文件路徑 → JSON 數據 → 配置字典 → 標準化配置

【維護與擴充重點】
------------------------------------------------------------
- 新增配置欄位時，請同步更新載入邏輯
- 若配置格式有變動，需同步更新解析邏輯
- 新增/修改配置轉換、數據驗證、錯誤處理時，務必同步更新本檔案

【常見易錯點】
------------------------------------------------------------
- JSON 解析錯誤導致配置載入失敗
- 配置數據轉換錯誤導致參數不正確
- 缺少必要配置時沒有提供預設值

【範例】
------------------------------------------------------------
- 載入單個配置：loader.load_config("config.json") -> ConfigData
- 載入多個配置：loader.load_configs(["config1.json", "config2.json"]) -> [ConfigData1, ConfigData2]
- 獲取配置摘要：loader.get_config_summary(config_data) -> dict

【與其他模組的關聯】
------------------------------------------------------------
- 被 Base_autorunner 調用，提供配置載入功能
- 依賴 json 進行配置文件解析
- 為 DataLoader、BacktestRunner 等提供配置數據

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本載入功能
- v1.1: 新增配置轉換和預設值處理
- v1.2: 新增 Rich Panel 顯示和調試輸出

【參考】
------------------------------------------------------------
- autorunner/DEVELOPMENT_PLAN.md
- Development_Guideline.md
- Base_autorunner.py
- config_template.json
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.panel import Panel
from rich.text import Text

console = Console()


class ConfigData:
    """
    配置數據容器

    封裝配置文件的數據結構，提供標準化的配置訪問介面。
    """

    def __init__(self, config_dict: Dict[str, Any], file_path: str):
        """
        初始化 ConfigData

        Args:
            config_dict: 配置字典
            file_path: 配置文件路徑
        """

        self.file_path = file_path
        self.file_name = Path(file_path).name
        self.raw_config = config_dict.copy()

        # 提取各模組配置
        self.dataloader_config = config_dict.get("dataloader", {})
        self.backtester_config = config_dict.get("backtester", {})
        self.metricstracker_config = config_dict.get("metricstracker", {})

        # 提取預測因子配置（從 dataloader 部分提取）
        dataloader_config = config_dict.get("dataloader", {})
        self.predictor_config = dataloader_config.get("predictor_config", {})

    def get_summary(self) -> Dict[str, Any]:
        """
        獲取配置摘要

        Returns:
            Dict[str, Any]: 配置摘要信息
        """

        config_summary = {
            "file_name": self.file_name,
            "file_path": self.file_path,
            "dataloader_source": self.dataloader_config.get("source", "unknown"),
            "backtester_pairs": len(self.backtester_config.get("condition_pairs", [])),
            "metricstracker_enabled": self.metricstracker_config.get(
                "enable_metrics_analysis", False
            ),
        }

        return config_summary


class ConfigLoader:
    """
    配置文件載入器

    負責從 JSON 文件中載入配置數據，解析和轉換配置參數，
    提供標準化的配置數據結構。
    """

    def __init__(self) -> None:
        """
        初始化 ConfigLoader
        """

        # 預設配置值
        self.default_config = {
            "dataloader": {
                "source": "yfinance",
                "start_date": "2020-01-01",
            },
            "backtester": {
                "condition_pairs": [],
            },
            "metricstracker": {
                "enable_metrics_analysis": False,
            },
        }

    def load_config(self, config_file: str) -> Optional[ConfigData]:
        """
        載入單個配置文件

        Args:
            config_file: 配置文件路徑

        Returns:
            Optional[ConfigData]: 配置數據對象，如果載入失敗則返回 None
        """

        try:
            # 讀取配置文件
            config_dict = self._read_config_file(config_file)
            if config_dict is None:
                return None

            # 合併預設配置
            merged_config = self._merge_with_defaults(config_dict)

            # 轉換配置格式
            processed_config = self._process_config(merged_config)

            # 創建配置數據對象
            config_data_obj = ConfigData(processed_config, config_file)

            return config_data_obj

        except Exception as e:
            print(f"❌ [ERROR] 載入配置文件失敗: {e}")
            self._display_load_error(f"載入失敗: {e}", Path(config_file).name)
            return None

    def load_configs(self, config_files: List[str]) -> List[ConfigData]:
        """
        載入多個配置文件

        Args:
            config_files: 配置文件路徑列表

        Returns:
            List[ConfigData]: 配置數據對象列表
        """

        config_data_list = []
        for config_file in config_files:
            config_data_obj = self.load_config(config_file)
            if config_data_obj is not None:
                config_data_list.append(config_data_obj)

        return config_data_list

    def _read_config_file(self, config_file: str) -> Optional[Dict[str, Any]]:
        """
        讀取配置文件

        Args:
            config_file: 配置文件路徑

        Returns:
            Optional[Dict[str, Any]]: 配置字典，如果讀取失敗則返回 None
        """

        try:
            with open(config_file, "r", encoding="utf-8") as f:
                config_dict = json.load(f)

            return config_dict

        except FileNotFoundError:
            print(f"❌ [ERROR] 配置文件不存在: {config_file}")
            self._display_load_error("配置文件不存在", Path(config_file).name)
            return None
        except json.JSONDecodeError as e:
            print(f"❌ [ERROR] JSON 格式錯誤: {e}")
            self._display_load_error(f"JSON 格式錯誤: {e}", Path(config_file).name)
            return None
        except Exception as e:
            print(f"❌ [ERROR] 讀取配置文件失敗: {e}")
            self._display_load_error(f"讀取失敗: {e}", Path(config_file).name)
            return None

    def _merge_with_defaults(self, config_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        合併預設配置

        Args:
            config_dict: 原始配置字典

        Returns:
            Dict[str, Any]: 合併後的配置字典
        """

        merged_config = self.default_config.copy()

        # 遞歸合併配置
        for key, value in config_dict.items():
            if (
                key in merged_config
                and isinstance(merged_config[key], dict)
                and isinstance(value, dict)
            ):
                merged_dict_key = merged_config[key]
                value_dict = value
                if isinstance(merged_dict_key, dict) and isinstance(value_dict, dict):
                    merged_config[key] = {**merged_dict_key, **value_dict}
            else:
                merged_config[key] = value

        return merged_config

    def _process_config(self, config_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        處理配置數據

        Args:
            config_dict: 配置字典

        Returns:
            Dict[str, Any]: 處理後的配置字典
        """

        processed_config = config_dict.copy()

        # 處理數據載入器配置
        if "dataloader" in processed_config:
            processed_config["dataloader"] = self._process_dataloader_config(
                processed_config["dataloader"]
            )

        # 處理回測器配置
        if "backtester" in processed_config:
            processed_config["backtester"] = self._process_backtester_config(
                processed_config["backtester"]
            )

        # 處理績效追蹤器配置
        if "metricstracker" in processed_config:
            processed_config["metricstracker"] = self._process_metricstracker_config(
                processed_config["metricstracker"]
            )

        return processed_config

    def _process_dataloader_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """處理數據載入器配置"""

        processed = config.copy()

        # 處理日期格式
        if "start_date" in processed:
            processed["start_date"] = str(processed["start_date"])

        # 處理數據源配置
        source = processed.get("source", "yfinance")
        if source == "yfinance" and "yfinance_config" not in processed:
            processed["yfinance_config"] = {
                "symbol": "AAPL",
                "period": "1y",
                "interval": "1d",
            }

        return processed

    def _process_backtester_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """處理回測器配置"""

        processed = config.copy()

        # 處理條件配對
        if "condition_pairs" not in processed:
            processed["condition_pairs"] = []

        # 處理交易參數
        if "trading_params" not in processed:
            processed["trading_params"] = {
                "transaction_cost": 0.001,
                "slippage": 0.0005,
                "trade_delay": 0,
                "trade_price": "close",
            }

        return processed

    def _process_metricstracker_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """處理績效追蹤器配置"""

        processed = config.copy()

        if "enable_metrics_analysis" not in processed:
            processed["enable_metrics_analysis"] = False

        processed.setdefault("file_selection_mode", "auto")
        processed.setdefault("parquet_directory", "records/backtester/")
        processed.setdefault("time_unit", 365)
        processed.setdefault("risk_free_rate", 0.04)

        return processed

    def _display_load_error(self, message: str, context: str = "") -> None:
        """
        顯示載入錯誤信息

        Args:
            message: 錯誤信息
            context: 錯誤上下文
        """

        title = "⚠️ 配置載入錯誤"
        if context:
            title += f" - {context}"

        console.print(
            Panel(
                f"❌ {message}",
                title=Text(title, style="bold #8f1511"),
                border_style="#8f1511",
            )
        )


if __name__ == "__main__":
    # 測試模式

    # 創建載入器實例
    loader = ConfigLoader()

    # 測試載入功能
    test_config = "records/autorunner/config_template.json"
    if Path(test_config).exists():
        config_data = loader.load_config(test_config)
        if config_data:
            summary = config_data.get_summary()
