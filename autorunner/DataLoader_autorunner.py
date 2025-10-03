# pylint: disable=too-many-lines
"""
DataLoader_autorunner.py

【功能說明】
------------------------------------------------------------
本模組負責數據載入功能，封裝現有的 dataloader 模組，
根據配置文件自動載入數據，無需用戶互動輸入。

【流程與數據流】
------------------------------------------------------------
- 主流程：讀取配置 → 選擇載入器 → 載入數據 → 處理預測因子 → 返回數據
- 數據流：配置數據 → 載入器選擇 → 原始數據 → 處理後數據 → 標準化數據

【維護與擴充重點】
------------------------------------------------------------
- 新增數據源時，請同步更新載入器選擇邏輯
- 若 dataloader 介面有變動，需同步更新封裝邏輯
- 新增/修改數據處理、預測因子處理時，務必同步更新本檔案

【常見易錯點】
------------------------------------------------------------
- 數據源配置錯誤導致載入失敗
- 預測因子處理錯誤導致數據不完整
- 數據格式不統一導致後續處理失敗

【範例】
------------------------------------------------------------
- 載入數據：loader.load_data(config) -> DataFrame
- 獲取載入摘要：loader.get_loading_summary() -> dict

【與其他模組的關聯】
------------------------------------------------------------
- 被 Base_autorunner 調用，提供數據載入功能
- 依賴 dataloader 模組進行實際數據載入
- 為 BacktestRunner 提供標準化數據

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本載入功能
- v1.1: 新增預測因子處理
- v1.2: 新增 Rich Panel 顯示和調試輸出

【參考】
------------------------------------------------------------
- autorunner/DEVELOPMENT_PLAN.md
- Development_Guideline.md
- Base_autorunner.py
- dataloader/base_loader.py
"""

import logging
import os
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import requests

# 導入 dataloader 模組
from binance.client import Client
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from dataloader.binance_loader import BinanceLoader
from dataloader.calculator_loader import ReturnCalculator
from dataloader.coinbase_loader import CoinbaseLoader
from dataloader.file_loader import FileLoader
from dataloader.predictor_loader import PredictorLoader
from dataloader.yfinance_loader import YahooFinanceLoader

console = Console()


class DataLoaderAutorunner:
    """
    數據載入封裝器

    封裝現有的 dataloader 模組，根據配置文件自動載入數據，
    無需用戶互動輸入，提供標準化的數據載入介面。
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        初始化 DataLoaderAutorunner

        Args:
            logger: 日誌記錄器
        """

        self.logger = logger or logging.getLogger("DataLoaderAutorunner")
        self.data: Optional[pd.DataFrame] = None
        self.frequency: Optional[str] = None
        self.source: Optional[str] = None
        self.loading_summary: Dict[str, Any] = {}
        self.current_predictor_column: Optional[str] = None
        self.using_price_predictor_only: bool = False

    @staticmethod
    def _require_config_field(config: Dict[str, Any], key: str, context: str) -> Any:
        if key not in config:
            raise ValueError(f"{context} 缺少 {key} 設定")
        value = config[key]
        if isinstance(value, str):
            value = value.strip()
            if value == "":
                raise ValueError(f"{context} 的 {key} 不可為空字串")
            return value
        if value is None:
            raise ValueError(f"{context} 的 {key} 不可為 None")
        return value

    def load_data(self, config: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """
        根據配置載入數據 - 反轉順序：先載入預測因子，再載入對應日期的價格數據

        Args:
            config: 數據載入配置

        Returns:
            Optional[pd.DataFrame]: 載入的數據，如果載入失敗則返回 None
        """

        try:
            # 檢查是否跳過預測因子
            if config.get("predictor_config", {}).get("skip_predictor", False):
                return self._handle_skip_predictor_mode(config)

            # 載入預測因子數據
            predictor_data = self._load_predictor_data(config)
            if predictor_data is None:
                return self._handle_predictor_load_failure(config)

            # 載入價格數據並合併
            result_data = self._load_and_merge_data(config, predictor_data)
            if result_data is None:
                return None

            # 後處理數據
            self.data = self._post_process_data(result_data, config)

            # 更新狀態和摘要
            self._update_final_state(config)

            return self.data

        except Exception as e:
            print(f"❌ [ERROR] 數據載入失敗: {e}")
            self._display_error(f"數據載入失敗: {e}")
            return None

    def _handle_skip_predictor_mode(
        self, config: Dict[str, Any]
    ) -> Optional[pd.DataFrame]:
        """處理跳過預測因子的模式"""
        price_data = self._load_price_data_only(config)
        if price_data is None:
            return None
        self.using_price_predictor_only = True
        return self._apply_price_predictor_only(config, price_data)

    def _handle_predictor_load_failure(
        self, config: Dict[str, Any]
    ) -> Optional[pd.DataFrame]:
        """處理預測因子載入失敗的情況"""
        print("⚠️ [WARNING] 預測因子載入失敗,回退到使用價格數據作為預測因子")
        fallback_price_data = self._load_price_data_only(config)
        if fallback_price_data is None:
            return None
        self.using_price_predictor_only = True
        return self._apply_price_predictor_only(config, fallback_price_data)

    def _load_and_merge_data(
        self, config: Dict[str, Any], predictor_data: pd.DataFrame
    ) -> Optional[pd.DataFrame]:
        """載入價格數據並與預測因子合併"""
        price_data = self._load_price_data_by_date_range(config, predictor_data)
        if price_data is None:
            return None

        merged_data = self._merge_predictor_and_price_data(predictor_data, price_data)
        if merged_data is None:
            return None

        # 將預測因子信息存儲到 data.attrs 中
        self._store_predictor_info(config, merged_data)

        return merged_data

    def _store_predictor_info(
        self,
        config: Dict[str, Any],
        predictor_data: pd.DataFrame,  # pylint: disable=unused-argument
    ) -> None:
        """存儲預測因子信息到數據屬性中"""
        predictor_config = config.get("predictor_config", {})
        predictor_path = predictor_config.get("predictor_path", "")
        predictor_column = predictor_config.get("predictor_column", "")

        if predictor_path:
            predictor_file_name = Path(predictor_path).stem
            if hasattr(predictor_data, "attrs"):
                predictor_data.attrs["predictor_file_name"] = predictor_file_name
                predictor_data.attrs["predictor_column"] = predictor_column

    def _post_process_data(
        self, processed_data: pd.DataFrame, config: Dict[str, Any]
    ) -> pd.DataFrame:
        """後處理數據：計算報酬率、處理差分、驗證數據"""
        # 處理報酬率計算
        if config.get("returns_config", {}).get("calculate_returns", False):
            processed_data = self._calculate_returns(config)

        # 處理差分
        if config.get("difference_config", {}).get("enable_difference", False):
            processed_data = self._process_difference(config)

        # 數據驗證
        processed_data = self._validate_data(processed_data, config)

        return processed_data

    def _update_final_state(self, config: Dict[str, Any]) -> None:
        """更新最終狀態和摘要"""
        # 更新載入摘要
        self._update_loading_summary(config)

        self.using_price_predictor_only = False
        predictor_column = config.get("predictor_config", {}).get("predictor_column")
        self.current_predictor_column = predictor_column

    def _load_yfinance_data(self, config: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """載入 Yahoo Finance 數據"""

        try:
            yfinance_config = config.get("yfinance_config")
            if yfinance_config is None:
                raise ValueError("dataloader.yfinance_config 未設定")

            symbol = self._require_config_field(
                yfinance_config, "symbol", "dataloader.yfinance_config"
            )
            _ = self._require_config_field(
                yfinance_config, "period", "dataloader.yfinance_config"
            )
            interval = self._require_config_field(
                yfinance_config, "interval", "dataloader.yfinance_config"
            )

            # 創建自定義的 YahooFinanceLoader 來處理配置
            class ConfigurableYahooFinanceLoader(YahooFinanceLoader):
                """可配置的 YahooFinanceLoader"""

                def __init__(
                    self, symbol: str, frequency: str, start_date: str, end_date: str
                ) -> None:
                    super().__init__()
                    self._symbol = symbol
                    self._frequency = frequency
                    self._start_date = start_date
                    self._end_date = end_date

                def _get_ticker(self) -> str:
                    return self._symbol

                def _get_frequency(self) -> str:
                    return self._frequency

                def _get_date_range(self) -> Tuple[str, str]:
                    return self._start_date, self._end_date

            start_date = self._require_config_field(config, "start_date", "dataloader")

            end_date = config.get("end_date") or datetime.now().strftime("%Y-%m-%d")

            yfinance_loader = ConfigurableYahooFinanceLoader(
                symbol=symbol,
                frequency=interval,
                start_date=start_date,
                end_date=end_date,
            )

            yfinance_data, frequency = yfinance_loader.load()

            self.frequency = frequency
            self.source = "yfinance"
            if hasattr(yfinance_data, "attrs"):
                yfinance_data.attrs["frequency"] = frequency

            return yfinance_data

        except Exception as e:
            print(f"❌ [ERROR] Yahoo Finance 數據載入失敗: {e}")
            return None

    def _load_binance_data(self, config: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """載入 Binance 數據"""

        try:
            binance_config = config.get("binance_config")
            if binance_config is None:
                raise ValueError("dataloader.binance_config 未設定")

            symbol = self._require_config_field(
                binance_config, "symbol", "dataloader.binance_config"
            )
            interval = self._require_config_field(
                binance_config, "interval", "dataloader.binance_config"
            )

            start_date = self._require_config_field(config, "start_date", "dataloader")
            end_date = config.get("end_date") or datetime.now().strftime("%Y-%m-%d")

            binance_loader = BinanceLoader()
            binance_loader.symbol = symbol
            binance_loader.interval = interval
            binance_loader.start_date = start_date
            binance_loader.end_date = end_date
            binance_data, frequency = binance_loader.load()

            self.frequency = frequency
            self.source = "binance"
            if hasattr(binance_data, "attrs"):
                binance_data.attrs["frequency"] = frequency

            return binance_data

        except Exception as e:
            print(f"❌ [ERROR] Binance 數據載入失敗: {e}")
            return None

    def _load_coinbase_data(self, config: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """載入 Coinbase 數據"""

        try:
            coinbase_config = config.get("coinbase_config")
            if coinbase_config is None:
                raise ValueError("dataloader.coinbase_config 未設定")

            symbol = self._require_config_field(
                coinbase_config, "symbol", "dataloader.coinbase_config"
            )

            start_date = self._require_config_field(config, "start_date", "dataloader")
            end_date = config.get("end_date") or datetime.now().strftime("%Y-%m-%d")

            coinbase_loader = CoinbaseLoader()
            coinbase_loader.symbol = symbol
            coinbase_loader.start_date = start_date
            coinbase_loader.end_date = end_date
            coinbase_data, frequency = coinbase_loader.load()

            self.frequency = frequency
            self.source = "coinbase"
            if hasattr(coinbase_data, "attrs"):
                coinbase_data.attrs["frequency"] = frequency

            return coinbase_data

        except Exception as e:
            print(f"❌ [ERROR] Coinbase 數據載入失敗: {e}")
            return None

    def _load_file_data(self, config: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """載入文件數據"""

        try:
            file_config = config.get("file_config")
            if file_config is None:
                raise ValueError("dataloader.file_config 未設定")

            file_path = self._require_config_field(
                file_config, "file_path", "dataloader.file_config"
            )

            if not Path(file_path).exists():
                raise FileNotFoundError(f"找不到文件: {file_path}")

            file_loader = FileLoader()
            file_data, frequency = file_loader.load()

            self.frequency = frequency
            self.source = "file"
            if hasattr(file_data, "attrs"):
                file_data.attrs["frequency"] = frequency

            return file_data

        except Exception as e:
            print(f"❌ [ERROR] 文件數據載入失敗: {e}")
            return None

    def _process_predictor(self, config: Dict[str, Any]) -> pd.DataFrame:
        """處理預測因子"""

        try:
            predictor_config = config.get("predictor_config")
            if predictor_config is None:
                raise ValueError("dataloader.predictor_config 未設定")

            predictor_path = self._require_config_field(
                predictor_config, "predictor_path", "dataloader.predictor_config"
            )
            predictor_column = self._require_config_field(
                predictor_config, "predictor_column", "dataloader.predictor_config"
            )

            # 驗證預測因子文件路徑
            predictor_path_obj = self._validate_predictor_path(predictor_path)
            if predictor_path_obj is None:
                return self.data

            # 創建並使用可配置的 PredictorLoader
            predictor_loader = self._create_configurable_predictor_loader(
                predictor_path, predictor_column
            )
            data_with_predictor = predictor_loader.load()

            if data_with_predictor is not None:
                return data_with_predictor

            print("❌ [ERROR] 預測因子載入失敗")
            return self.data

        except Exception as e:
            print(f"❌ [ERROR] 預測因子處理失敗: {e}")
            return self.data

    def _validate_predictor_path(self, predictor_path: str) -> Optional[Path]:
        """驗證預測因子文件路徑"""
        if not Path(predictor_path).is_absolute():
            # 如果是相對路徑，從項目根目錄開始
            project_root = Path(__file__).parent.parent
            predictor_path_obj = project_root / predictor_path
        else:
            predictor_path_obj = Path(predictor_path)

        if not predictor_path_obj.exists():
            print(f"❌ [ERROR] 找不到預測因子文件: {predictor_path_obj.absolute()}")
            return None

        return predictor_path_obj

    def _create_configurable_predictor_loader(
        self, predictor_path: str, predictor_column: str
    ) -> PredictorLoader:
        """創建可配置的 PredictorLoader"""

        class ConfigurablePredictorLoader(PredictorLoader):
            """可配置的 PredictorLoader"""

            def __init__(
                self, price_data: Any, predictor_path: str, predictor_column: str
            ) -> None:
                super().__init__(price_data)
                self._predictor_path = predictor_path
                self._predictor_column = predictor_column

            def _get_file_path(self) -> Optional[str]:
                return self._predictor_path

            def _get_time_format(self) -> Optional[str]:
                # 自動推斷時間格式
                return None

        return ConfigurablePredictorLoader(self.data, predictor_path, predictor_column)

    def _process_difference(self, config: Dict[str, Any]) -> pd.DataFrame:
        """處理差分"""

        try:
            difference_config = config.get("difference_config", {})
            difference_config.get("difference_column")

            # 檢查是否有預測因子可以進行差分

            if self.data is None:
                return self.data

            available_factors = [
                col
                for col in self.data.columns
                if col
                not in [
                    "Time",
                    "Open",
                    "High",
                    "Low",
                    "Close",
                    "Volume",
                    "open_return",
                    "close_return",
                    "open_logreturn",
                    "close_logreturn",
                    "index",  # 排除時間索引欄位
                ]
            ]

            if not available_factors:
                return self.data

            # 使用配置中選擇的預測因子進行差分
            predictor_config = config.get("predictor_config", {})
            selected_predictor = predictor_config.get("predictor_column", "aggregated")

            if selected_predictor not in available_factors:
                print(
                    f"❌ [ERROR] 配置的預測因子 '{selected_predictor}' 不存在於數據中"
                )
                return self.data

            predictor_loader = PredictorLoader(self.data)
            data_with_difference, _, _ = predictor_loader.process_difference(
                self.data, selected_predictor
            )

            return data_with_difference

        except Exception as e:
            print(f"❌ [ERROR] 差分處理失敗: {e}")
            return self.data

    def _calculate_returns(
        self, config: Dict[str, Any]  # pylint: disable=unused-argument
    ) -> pd.DataFrame:
        """計算收益率"""

        try:
            calculator = ReturnCalculator(self.data)
            data_with_returns = calculator.calculate_returns()

            return data_with_returns

        except Exception as e:
            print(f"❌ [ERROR] 收益率計算失敗: {e}")
            return self.data

    def _validate_data(
        self, validated_data: pd.DataFrame, config: Dict[str, Any]
    ) -> pd.DataFrame:
        """驗證數據"""

        try:
            # 在 autorunner 模式下，我們跳過需要用戶互動的驗證
            # 只進行基本的數據檢查

            # 檢查必要欄位
            required_columns = ["Time", "Open", "High", "Low", "Close", "Volume"]
            missing_columns = [
                col for col in required_columns if col not in validated_data.columns
            ]

            if missing_columns:
                print(f"⚠️ [WARNING] 缺少必要欄位: {missing_columns}")

            # 檢查數據形狀
            if validated_data.empty:
                print("❌ [ERROR] 數據為空")
                return validated_data

            # 自動化處理缺失值
            validated_data = self._handle_missing_values_automated(
                validated_data, config
            )

            return validated_data

        except Exception as e:
            print(f"❌ [ERROR] 數據驗證失敗: {e}")
            return validated_data

    def _handle_missing_values_automated(
        self, missing_data: pd.DataFrame, config: Dict[str, Any]
    ) -> pd.DataFrame:
        """
        自動化處理缺失值

        Args:
            missing_data: 數據框
            config: 配置信息

        Returns:
            處理後的數據框
        """

        try:
            handle_missing = config.get("handle_missing_values")
            missing_strategy = config.get("missing_value_strategy")

            if handle_missing != "fill" or not missing_strategy:
                print("⚠️ [WARNING] 缺失值處理設定不完整或非 fill，直接返回原始資料")
                return missing_data

            # 定義需要處理的欄位 (價格數據欄位)
            missing_columns = ["Open", "High", "Low", "Close", "Volume"]

            # 檢查預測因子欄位
            predictor_config = config.get("predictor_config", {})
            selected_predictor = predictor_config.get("predictor_column", "aggregated")

            # 如果預測因子已載入,也加入處理列表
            if not predictor_config.get("skip_predictor", False):
                if selected_predictor in missing_data.columns:
                    missing_columns.append(selected_predictor)
                    # 如果預測因子沒有缺失值,檢查其他欄位
                    if missing_data[selected_predictor].isnull().sum() == 0:
                        # 檢查其他價格欄位是否也沒有缺失值
                        other_missing = sum(
                            missing_data[col].isnull().sum()
                            for col in ["Open", "High", "Low", "Close", "Volume"]
                            if col in missing_data.columns
                        )
                        if other_missing == 0:
                            return missing_data

            missing_data = self._fill_missing_values(missing_data, missing_columns, missing_strategy)
            return missing_data

        except Exception as e:
            print(f"❌ [ERROR] 缺失值處理失敗: {e}")
            print(f"❌ [ERROR] 詳細錯誤: {traceback.format_exc()}")
            return missing_data

    def _fill_missing_values(
        self, fill_data: pd.DataFrame, missing_columns: list, strategy: str
    ) -> pd.DataFrame:
        """
        使用指定的策略填充缺失值

        Args:
            fill_data: 數據框
            missing_columns: 有缺失值的欄位列表
            strategy: 填充策略字串

        Returns:
            填充後的數據框
        """

        try:
            for col in missing_columns:
                fill_data = self._apply_fill_strategy(fill_data, col, strategy)

            self._check_remaining_missing_values(fill_data)
            return fill_data

        except Exception as e:
            print(f"❌ [ERROR] 填充缺失值失敗: {e}")
            print(f"❌ [ERROR] 詳細錯誤: {traceback.format_exc()}")
            return fill_data

    def _apply_fill_strategy(
        self, strategy_data: pd.DataFrame, col: str, strategy: str
    ) -> pd.DataFrame:
        """應用特定的填充策略到指定欄位"""
        try:
            if strategy == "A":
                strategy_data[col] = strategy_data[col].ffill()
            elif strategy.startswith("B,"):
                n = int(strategy.split(",")[1])
                strategy_data[col] = strategy_data[col].fillna(
                    strategy_data[col].rolling(window=n, min_periods=1).mean()
                )
            elif strategy.startswith("C,"):
                value = float(strategy.split(",")[1])
                strategy_data[col] = strategy_data[col].fillna(value)
            else:
                print(f"⚠️ [WARNING] 未知的缺失值策略 {strategy}，保持原始資料")
        except Exception as e:
            print(f"⚠️ [WARNING] 缺失值策略 {strategy} 失敗: {e}")

        return strategy_data

    def _check_remaining_missing_values(self, check_data: pd.DataFrame) -> None:
        """檢查填充後是否還有缺失值"""
        remaining_missing = check_data.isnull().sum().sum()
        if remaining_missing > 0:
            print(f"⚠️ [WARNING] 填充後仍有 {remaining_missing} 個缺失值")

    def _update_loading_summary(self, config: Dict[str, Any]) -> None:
        """更新載入摘要"""

        self.loading_summary = {
            "source": self.source,
            "frequency": self.frequency,
            "data_shape": self.data.shape if self.data is not None else (0, 0),
            "columns": list(self.data.columns) if self.data is not None else [],
            "date_range": self._get_date_range(),
            "config_used": {
                "source": config.get("source"),
                "start_date": config.get("start_date"),
                "end_date": config.get("end_date"),
            },
        }

    def _get_date_range(self) -> Tuple[str, str]:
        """獲取數據日期範圍"""
        if self.data is None or "Time" not in self.data.columns:
            return "N/A", "N/A"

        try:
            start_date = self.data["Time"].min().strftime("%Y-%m-%d")
            end_date = self.data["Time"].max().strftime("%Y-%m-%d")
            return start_date, end_date
        except Exception:
            return "N/A", "N/A"

    def get_loading_summary(self) -> Dict[str, Any]:
        """
        獲取載入摘要

        Returns:
            Dict[str, Any]: 載入摘要信息
        """
        return self.loading_summary.copy()

    def display_loading_summary(self) -> None:
        """顯示載入摘要"""

        if not self.loading_summary:
            console.print(
                Panel(
                    "❌ 沒有載入摘要信息",
                    title=Text("⚠️ 載入摘要", style="bold #8f1511"),
                    border_style="#8f1511",
                )
            )
            return

        # 創建摘要表格
        table = Table(title="📊 數據載入摘要")
        table.add_column("項目", style="cyan")
        table.add_column("值", style="magenta")

        table.add_row("數據源", self.loading_summary.get("source", "N/A"))
        table.add_row("頻率", self.loading_summary.get("frequency", "N/A"))
        table.add_row(
            "數據形狀",
            f"{self.loading_summary.get('data_shape', (0, 0))[0]} 行 x {self.loading_summary.get('data_shape', (0, 0))[1]} 列",
        )

        date_range = self.loading_summary.get("date_range", ("N/A", "N/A"))
        table.add_row("日期範圍", f"{date_range[0]} 至 {date_range[1]}")

        columns = self.loading_summary.get("columns", [])
        table.add_row("欄位數量", str(len(columns)))
        table.add_row(
            "主要欄位", ", ".join(columns[:5]) + ("..." if len(columns) > 5 else "")
        )

        console.print(table)

        # 顯示載入成功信息
        console.print(
            Panel(
                f"✅ 數據載入成功！載入了 {self.loading_summary.get('data_shape', (0, 0))[0]} 行數據",
                title=Text("🎉 載入成功", style="bold green"),
                border_style="green",
            )
        )

    def _display_error(self, message: str) -> None:
        """
        顯示錯誤信息

        Args:
            message: 錯誤信息
        """

        console.print(
            Panel(
                f"❌ {message}",
                title=Text("⚠️ 數據載入錯誤", style="bold #8f1511"),
                border_style="#8f1511",
            )
        )

    def _load_predictor_data(self, config: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """載入預測因子數據"""

        try:
            predictor_config = config.get("predictor_config", {})
            predictor_path = predictor_config.get("predictor_path", "")
            predictor_column = predictor_config.get("predictor_column")

            # 驗證基本配置
            if not self._validate_predictor_config(predictor_path, predictor_column):
                return None

            # 處理文件路徑
            predictor_path_obj = self._resolve_predictor_path(predictor_path)
            if predictor_path_obj is None:
                return None

            # 讀取預測因子數據
            predictor_data = self._read_predictor_file(predictor_path_obj)
            if predictor_data is None:
                return None

            # 處理時間和預測因子欄位
            predictor_data = self._process_predictor_columns(
                predictor_data, predictor_config, predictor_column
            )
            if predictor_data is None:
                return None

            # 顯示載入成功信息
            self._display_predictor_load_success(
                predictor_path_obj, predictor_column, predictor_data
            )

            return predictor_data

        except Exception as e:
            print(f"❌ [ERROR] 預測因子數據載入失敗: {e}")
            print(f"詳細錯誤:\n{traceback.format_exc()}")
            return None

    def _validate_predictor_config(
        self, predictor_path: str, predictor_column: str
    ) -> bool:
        """驗證預測因子配置"""
        if not predictor_path:
            return False

        if not predictor_column:
            print("❌ [ERROR] 未指定 predictor_column")
            return False

        return True

    def _resolve_predictor_path(self, predictor_path: str) -> Optional[Path]:
        """解析預測因子文件路徑"""
        if not Path(predictor_path).is_absolute():
            project_root = Path(__file__).parent.parent
            predictor_path_obj = project_root / predictor_path
        else:
            predictor_path_obj = Path(predictor_path)

        if not predictor_path_obj.exists():
            print(f"❌ [ERROR] 預測因子文件不存在: {predictor_path_obj}")
            return None

        return predictor_path_obj

    def _read_predictor_file(self, predictor_path_obj: Path) -> Optional[pd.DataFrame]:
        """讀取預測因子文件"""
        suffix = predictor_path_obj.suffix.lower()
        try:
            if suffix in [".xlsx", ".xls"]:
                return pd.read_excel(predictor_path_obj)
            if suffix == ".csv":
                return pd.read_csv(predictor_path_obj)

            print(f"❌ [ERROR] 不支援的文件格式: {suffix}")
            return None
        except Exception as e:
            print(f"❌ [ERROR] 讀取預測因子文件失敗: {e}")
            return None

    def _process_predictor_columns(
        self,
        predictor_data: pd.DataFrame,
        predictor_config: Dict[str, Any],
        predictor_column: str,
    ) -> Optional[pd.DataFrame]:
        """處理預測因子的時間和預測因子欄位"""
        time_col = predictor_config.get("time_column")
        if not time_col:
            print("❌ [ERROR] 未指定 time_column")
            return None

        if time_col not in predictor_data.columns:
            print(f"❌ [ERROR] 時間欄位 {time_col} 不存在於預測因子文件中")
            print(f"可用欄位: {list(predictor_data.columns)}")
            return None

        if predictor_column not in predictor_data.columns:
            print(f"❌ [ERROR] 預測因子欄位 {predictor_column} 不存在於文件中")
            print(f"可用欄位: {list(predictor_data.columns)}")
            return None

        # 只保留時間欄位和預測因子欄位
        predictor_data = predictor_data[[time_col, predictor_column]].copy()

        # 處理時間格式
        time_format = predictor_config.get("time_format")
        if time_format:
            predictor_data.loc[:, time_col] = pd.to_datetime(
                predictor_data[time_col], format=time_format
            )
        else:
            predictor_data.loc[:, time_col] = pd.to_datetime(predictor_data[time_col])

        predictor_data = predictor_data.set_index(time_col)
        return predictor_data

    def _display_predictor_load_success(
        self,
        predictor_path_obj: Path,
        predictor_column: str,
        predictor_data: pd.DataFrame,
    ) -> None:
        """顯示預測因子載入成功信息"""
        console.print(
            Panel(
                f"✅ 預測因子載入成功\n"
                f"📁 文件: {predictor_path_obj.name}\n"
                f"📊 欄位: {predictor_column}\n"
                f"📏 數據量: {len(predictor_data)} 行",
                title="[bold #dbac30]📊 數據載入 Dataloader[/bold #dbac30]",
                border_style="#dbac30",
            )
        )

    def _load_price_data_by_date_range(
        self, config: Dict[str, Any], predictor_data: pd.DataFrame
    ) -> Optional[pd.DataFrame]:
        """根據預測因子日期範圍載入價格數據"""

        try:
            # 獲取預測因子的日期範圍
            start_date = predictor_data.index.min().strftime("%Y-%m-%d")
            end_date = predictor_data.index.max().strftime("%Y-%m-%d")

            # 選擇數據源
            source = config.get("source", "yfinance")

            # 根據數據源載入價格數據
            if source == "yfinance":
                return self._load_yfinance_data_by_date(config, start_date, end_date)
            if source == "binance":
                return self._load_binance_data_by_date(config, start_date, end_date)
            if source == "coinbase":
                return self._load_coinbase_data_by_date(config, start_date, end_date)
            if source == "file":
                return self._load_file_data_by_date(config, start_date, end_date)

            print(f"❌ [ERROR] 不支援的價格數據源: {source}")
            return None

        except Exception as e:
            print(f"❌ [ERROR] 根據日期範圍載入價格數據失敗: {e}")
            return None

    def _load_price_data_only(self, config: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """僅載入價格數據（跳過預測因子時使用）"""

        try:
            source = config.get("source", "yfinance")

            # 根據數據源載入價格數據
            if source == "yfinance":
                self.data = self._load_yfinance_data(config)
            elif source == "binance":
                self.data = self._load_binance_data(config)
            elif source == "coinbase":
                self.data = self._load_coinbase_data(config)
            elif source == "file":
                self.data = self._load_file_data(config)
            else:
                print(f"❌ [ERROR] 不支援的價格數據源: {source}")
                return None

            if self.data is None:
                return None

            # 計算收益率
            if config.get("returns_config", {}).get("calculate_returns", False):
                self.data = self._calculate_returns(config)

            # 數據驗證
            self.data = self._validate_data(self.data, config)

            # 更新載入摘要
            self._update_loading_summary(config)

            return self.data

        except Exception as e:
            print(f"❌ [ERROR] 價格數據載入失敗: {e}")
            return None

    def _apply_price_predictor_only(
        self, config: Dict[str, Any], price_data: pd.DataFrame
    ) -> Optional[pd.DataFrame]:
        """使用價格資料作為預測因子完成後續流程"""

        price_data = price_data.copy()
        price_data["Price_predictor"] = price_data["Close"].copy()
        predictor_config = config.setdefault("predictor_config", {})
        predictor_config["predictor_column"] = "Price_predictor"
        predictor_config["skip_predictor"] = True

        self.data = price_data
        self.current_predictor_column = "Price_predictor"
        self.using_price_predictor_only = True

        self.data = self._process_difference(config)

        self.data = self._validate_data(self.data, config)
        self._update_loading_summary(config)
        return self.data

    def _merge_predictor_and_price_data(
        self, predictor_data: pd.DataFrame, price_data: pd.DataFrame
    ) -> Optional[pd.DataFrame]:
        """合併預測因子和價格數據"""

        try:
            # 確保價格數據的 Time 為索引
            if "Time" not in price_data.index.names:
                if "Time" in price_data.columns:
                    price_data = price_data.set_index("Time")
                else:
                    print("❌ [ERROR] 價格數據缺少 'Time' 欄位或索引")
                    return None

            # 時間對齊（inner join）
            merged = price_data.merge(
                predictor_data, left_index=True, right_index=True, how="inner"
            )

            if merged.empty:
                print("❌ [ERROR] 價格數據與預測因子數據無時間交集，無法合併")
                return None

            # 重置索引並重命名為 Time 欄位（保持與 dataloader 一致）
            merged = merged.reset_index()
            if "index" in merged.columns:
                merged = merged.rename(columns={"index": "Time"})

            # 確保 Time 欄位格式正確
            merged["Time"] = pd.to_datetime(merged["Time"])

            # 顯示合併成功信息
            predictor_cols = [
                col
                for col in merged.columns
                if col not in ["Time", "Open", "High", "Low", "Close", "Volume"]
            ]
            console.print(
                Panel(
                    f"✅ 預測因子與價格數據合併成功\n"
                    f"📊 預測因子欄位: {', '.join(predictor_cols)}\n"
                    f"📏 合併後數據量: {len(merged)} 行",
                    title="[bold #dbac30]📊 數據載入 Dataloader[/bold #dbac30]",
                    border_style="#dbac30",
                )
            )

            return merged

        except Exception as e:
            print(f"❌ [ERROR] 數據合併失敗: {e}")
            return None

    def _load_yfinance_data_by_date(
        self, config: Dict[str, Any], start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """根據指定日期範圍載入 Yahoo Finance 數據"""

        try:
            yfinance_config = config.get("yfinance_config", {})
            symbol = yfinance_config.get("symbol", "AAPL")
            interval = yfinance_config.get("interval", "1d")

            # 創建自定義的 YahooFinanceLoader
            class ConfigurableYahooFinanceLoader(YahooFinanceLoader):
                """可配置的 YahooFinanceLoader"""

                def __init__(
                    self, symbol: str, frequency: str, start_date: str, end_date: str
                ) -> None:
                    super().__init__()
                    self._symbol = symbol
                    self._frequency = frequency
                    self._start_date = start_date
                    self._end_date = end_date

                def _get_ticker(self) -> str:
                    return self._symbol

                def _get_frequency(self) -> str:
                    return self._frequency

                def _get_date_range(self) -> Tuple[str, str]:
                    return self._start_date, self._end_date

            yfinance_loader_by_date = ConfigurableYahooFinanceLoader(
                symbol=symbol,
                frequency=interval,
                start_date=start_date,
                end_date=end_date,
            )

            yfinance_data_by_date, frequency = yfinance_loader_by_date.load()

            self.frequency = frequency
            self.source = "yfinance"
            if hasattr(yfinance_data_by_date, "attrs"):
                yfinance_data_by_date.attrs["frequency"] = frequency

            return yfinance_data_by_date

        except Exception as e:
            print(f"❌ [ERROR] Yahoo Finance 數據載入失敗: {e}")
            return None

    def _load_binance_data_by_date(
        self, config: Dict[str, Any], start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """根據指定日期範圍載入 Binance 數據"""

        try:

            binance_config = config.get("binance_config", {})
            symbol = binance_config.get("symbol", "BTCUSDT")
            interval = binance_config.get("interval", "1d")

            # 使用無憑證的 Client
            client = Client()
            klines = client.get_historical_klines(
                symbol, interval, start_date, end_date
            )

            if not klines:
                print(f"❌ [ERROR] 無法獲取 '{symbol}' 的數據")
                return None

            # 轉換為 DataFrame
            binance_data_by_date = pd.DataFrame(
                klines,
                columns=[
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "close_time",
                    "quote_asset_volume",
                    "number_of_trades",
                    "taker_buy_base_asset_volume",
                    "taker_buy_quote_asset_volume",
                    "ignore",
                ],
            )

            # 重命名欄位為標準格式
            binance_data_by_date = binance_data_by_date.rename(
                columns={
                    "timestamp": "Time",
                    "open": "Open",
                    "high": "High",
                    "low": "Low",
                    "close": "Close",
                    "volume": "Volume",
                }
            )

            # 轉換時間格式
            binance_data_by_date["Time"] = pd.to_datetime(binance_data_by_date["Time"], unit="ms")

            # 選擇需要的欄位
            binance_data_by_date = binance_data_by_date[["Time", "Open", "High", "Low", "Close", "Volume"]]

            # 轉換為數值類型
            binance_data_by_date[["Open", "High", "Low", "Close", "Volume"]] = binance_data_by_date[
                ["Open", "High", "Low", "Close", "Volume"]
            ].astype(float)

            # 設置索引為 Time
            binance_data_by_date = binance_data_by_date.set_index("Time")

            self.frequency = interval
            self.source = "binance"
            if hasattr(binance_data_by_date, "attrs"):
                binance_data_by_date.attrs["frequency"] = interval

            return binance_data_by_date

        except Exception as e:
            print(f"❌ [ERROR] Binance 數據載入失敗: {e}")
            return None

    def _load_coinbase_data_by_date(  # pylint: disable=too-many-locals
        self, config: Dict[str, Any], start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """根據指定日期範圍載入 Coinbase 數據"""

        try:

            coinbase_config = config.get("coinbase_config", {})
            symbol = coinbase_config.get("symbol", "BTC-USD")
            granularity = 86400  # 1d = 86400 seconds

            # 轉換為 datetime 對象（確保格式正確）
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")

            # Coinbase API 有限制，每次請求最多 300 個數據點
            # 需要分批請求數據
            all_data = []

            # 計算每批的時間範圍
            max_candles = 300
            seconds_per_candle = granularity
            batch_seconds = max_candles * seconds_per_candle

            current_start = start_dt

            while current_start < end_dt:
                current_end = min(
                    current_start + timedelta(seconds=batch_seconds), end_dt
                )

                # Coinbase Exchange API endpoint (public, no auth required)
                url = f"https://api.exchange.coinbase.com/products/{symbol}/candles"
                params = {
                    "start": current_start.isoformat(),
                    "end": current_end.isoformat(),
                    "granularity": granularity,
                }

                response = requests.get(url, params=params)

                if response.status_code != 200:
                    print(
                        f"❌ [ERROR] API 請求失敗：{response.status_code} - {response.text}"
                    )
                    return None

                candles = response.json()

                if candles:
                    all_data.extend(candles)

                # 移動到下一批
                current_start = current_end

            if not all_data:
                print(f"❌ [ERROR] 無法獲取 '{symbol}' 的數據")
                return None

            # 轉換為 DataFrame
            # Coinbase API 返回格式: [timestamp, low, high, open, close, volume]
            coinbase_data_by_date = pd.DataFrame(
                all_data,
                columns=[
                    "timestamp",
                    "low",
                    "high",
                    "open",
                    "close",
                    "volume",
                ],
            )

            # 重命名欄位為標準格式
            coinbase_data_by_date = coinbase_data_by_date.rename(
                columns={
                    "timestamp": "Time",
                    "open": "Open",
                    "high": "High",
                    "low": "Low",
                    "close": "Close",
                    "volume": "Volume",
                }
            )

            # 轉換時間格式
            coinbase_data_by_date["Time"] = pd.to_datetime(coinbase_data_by_date["Time"], unit="s")

            # 選擇需要的欄位
            coinbase_data_by_date = coinbase_data_by_date[["Time", "Open", "High", "Low", "Close", "Volume"]]

            # 轉換為數值類型
            coinbase_data_by_date[["Open", "High", "Low", "Close", "Volume"]] = coinbase_data_by_date[
                ["Open", "High", "Low", "Close", "Volume"]
            ].astype(float)

            # 設置索引為 Time
            coinbase_data_by_date = coinbase_data_by_date.set_index("Time")

            self.frequency = "1d"
            self.source = "coinbase"
            if hasattr(coinbase_data_by_date, "attrs"):
                coinbase_data_by_date.attrs["frequency"] = "1d"

            return coinbase_data_by_date

        except Exception as e:
            print(f"❌ [ERROR] Coinbase 數據載入失敗: {e}")
            return None

    def _load_file_data_by_date(
        self, config: Dict[str, Any], start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """根據指定日期範圍載入文件數據"""

        try:
            # 驗證和解析配置
            file_config = self._validate_file_config(config)
            if file_config is None:
                return None

            # 讀取文件數據
            file_data_by_date = self._read_file_data(file_config["file_path"])
            if file_data_by_date is None:
                return None

            # 處理欄位映射和重命名
            file_data_by_date = self._process_file_columns(file_data_by_date, file_config)
            if file_data_by_date is None:
                return None

            # 處理時間格式和日期過濾
            file_data_by_date = self._process_file_time_and_filter(file_data_by_date, start_date, end_date)

            # 設置最終屬性
            self._set_file_data_attributes(file_data_by_date)

            return file_data_by_date

        except Exception as e:
            print(f"❌ [ERROR] 文件數據載入失敗: {e}")
            return None

    def _validate_file_config(self, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """驗證文件配置"""
        if "file_config" not in config:
            raise ValueError("dataloader.file_config 未設定")

        file_config = config["file_config"]

        try:
            required_fields = [
                "file_path",
                "time_column",
                "open_column",
                "high_column",
                "low_column",
                "close_column",
            ]
            for field in required_fields:
                if field not in file_config:
                    raise ValueError(f"dataloader.file_config 缺少參數: {field}")

            if not file_config["file_path"]:
                raise ValueError("dataloader.file_config.file_path 不可為空")

            # 檢查文件是否存在
            if not os.path.exists(file_config["file_path"]):
                print(f"❌ [ERROR] 文件不存在: {file_config['file_path']}")
                return None

            return file_config

        except KeyError as missing:
            raise ValueError(f"dataloader.file_config 缺少參數: {missing.args[0]}") from missing

    def _read_file_data(self, file_path: str) -> Optional[pd.DataFrame]:
        """讀取文件數據"""
        try:
            if file_path.lower().endswith(".csv"):
                return pd.read_csv(file_path)
            if file_path.lower().endswith((".xlsx", ".xls")):
                return pd.read_excel(file_path)

            print(f"❌ [ERROR] 不支援的文件格式: {file_path}")
            return None
        except Exception as e:
            print(f"❌ [ERROR] 讀取文件失敗: {e}")
            return None

    def _process_file_columns(
        self, process_data: pd.DataFrame, file_config: Dict[str, Any]
    ) -> Optional[pd.DataFrame]:
        """處理文件欄位映射和重命名"""
        # 創建欄位映射
        column_mapping = {
            file_config["time_column"]: "Time",
            file_config["open_column"]: "Open",
            file_config["high_column"]: "High",
            file_config["low_column"]: "Low",
            file_config["close_column"]: "Close",
        }

        # 如果有 Volume 欄位，則添加映射
        volume_column = file_config.get("volume_column")
        if volume_column:
            column_mapping[volume_column] = "Volume"

        process_data = process_data.rename(columns=column_mapping)

        # 確保 Time 欄位存在
        if "Time" not in process_data.columns:
            print(f"❌ [ERROR] 找不到時間欄位: {file_config['time_column']}")
            return None

        return process_data

    def _process_file_time_and_filter(
        self, filter_data: pd.DataFrame, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """處理時間格式和日期過濾"""
        # 轉換時間格式（支持多種格式）
        try:
            # 嘗試 DD/MM/YYYY 格式
            filter_data["Time"] = pd.to_datetime(filter_data["Time"], format="%d/%m/%Y")
        except (ValueError, TypeError):
            try:
                # 嘗試 YYYY-MM-DD 格式
                filter_data["Time"] = pd.to_datetime(filter_data["Time"], format="%Y-%m-%d")
            except (ValueError, TypeError):
                # 讓 pandas 自動推斷格式
                filter_data["Time"] = pd.to_datetime(filter_data["Time"], dayfirst=True)

        # 根據日期範圍過濾數據
        filter_data = filter_data[(filter_data["Time"] >= start_date) & (filter_data["Time"] <= end_date)]

        # 選擇需要的欄位（Volume 為可選）
        required_columns = ["Time", "Open", "High", "Low", "Close"]
        optional_columns = ["Volume"]
        available_columns = [col for col in required_columns if col in filter_data.columns]
        available_columns.extend(
            [col for col in optional_columns if col in filter_data.columns]
        )
        filter_data = filter_data[available_columns]

        # 轉換為數值類型
        numeric_columns = [
            col
            for col in ["Open", "High", "Low", "Close", "Volume"]
            if col in filter_data.columns
        ]
        filter_data[numeric_columns] = filter_data[numeric_columns].astype(float)

        # 設置索引為 Time
        filter_data = filter_data.set_index("Time")

        return filter_data

    def _set_file_data_attributes(self, attr_data: pd.DataFrame) -> None:
        """設置文件數據屬性"""
        self.frequency = "1d"
        self.source = "file"
        if hasattr(attr_data, "attrs"):
            attr_data.attrs["frequency"] = "1d"


if __name__ == "__main__":
    # 測試模式

    # 創建載入器實例
    loader = DataLoaderAutorunner()

    # 測試配置
    test_config = {
        "source": "yfinance",
        "start_date": "2020-01-01",
        "end_date": "2024-01-01",
        "yfinance_config": {"symbol": "AAPL", "period": "1y", "interval": "1d"},
        "predictor_config": {"skip_predictor": True},
        "difference_config": {"enable_difference": False},
        "returns_config": {"calculate_returns": True},
    }

    # 測試載入功能
    data = loader.load_data(test_config)
    if data is not None:
        loader.display_loading_summary()
