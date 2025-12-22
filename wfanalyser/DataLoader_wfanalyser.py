"""
DataLoader_wfanalyser.py

【功能說明】
------------------------------------------------------------
本模組負責 WFA 數據載入功能，直接使用原版 dataloader 模組，
根據配置文件自動載入數據，無需用戶互動輸入。

【流程與數據流】
------------------------------------------------------------
- 主流程：讀取配置 → 調用原版 dataloader → 返回數據
- 數據流：配置數據 → 原版載入器 → 標準化數據

【維護與擴充重點】
------------------------------------------------------------
- 直接使用原版 dataloader 模組，避免重複實現
- 若 dataloader 介面有變動，需同步更新調用邏輯
- 新增/修改數據處理時，優先考慮在原版 dataloader 中實現

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
- 被 WalkForwardEngine 調用，提供數據載入功能
- 直接調用原版 dataloader 模組進行實際數據載入
- 為 WFA 引擎提供標準化數據

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本載入功能

【參考】
------------------------------------------------------------
- dataloader/base_loader.py: 數據載入基底類
- WalkForwardEngine_wfanalyser.py: WFA 核心引擎
- wfanalyser/README.md: WFA 模組詳細說明
"""

import logging
import traceback
from typing import Any, Dict, Optional

import pandas as pd
from rich.text import Text

from .utils import get_console
from utils import show_error, show_info, show_success, show_step_panel, show_warning

console = get_console()


class DataLoaderWFAAnalyser:
    """
    WFA 數據載入封裝器

    直接使用原版 dataloader 模組，根據配置文件自動載入數據，
    無需用戶互動輸入，提供標準化的數據載入介面。
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        初始化 DataLoaderWFAAnalyser

        Args:
            logger: 日誌記錄器
        """
        self.logger = logger or logging.getLogger("lo2cin4bt.wfanalyser.dataloader")
        self.data: Optional[pd.DataFrame] = None
        self.frequency: Optional[str] = None
        self.source: Optional[str] = None
        self.loading_summary: Dict[str, Any] = {}
        self.current_predictor_column: Optional[str] = None
        self.using_price_predictor_only: bool = False

    def load_data(self, config: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """
        根據配置載入數據 - 直接使用原版 dataloader 模組

        Args:
            config: 數據載入配置

        Returns:
            Optional[pd.DataFrame]: 載入的數據，如果載入失敗則返回 None
        """
        try:
            show_step_panel("DATALOADER", 1, ["開始載入數據"], "")

            # 根據配置選擇載入器
            source = config.get("source", "yfinance")

            # 獲取結束日期：優先使用配置中的 end_date，如果沒有則使用當前日期
            from datetime import datetime
            end_date = config.get("end_date")
            if end_date is None:
                end_date = datetime.now().strftime("%Y-%m-%d")

            # 直接使用原版 dataloader 模組，並設置配置參數
            if source == "yfinance":
                from dataloader.yfinance_loader import YahooFinanceLoader

                loader = YahooFinanceLoader()
                # 設置配置參數
                yfinance_config = config.get("yfinance_config", {})
                loader.symbol = yfinance_config.get("symbol", "AAPL")
                loader.interval = yfinance_config.get("interval", "1d")
                loader.start_date = config.get("start_date", "2020-01-01")
                loader.end_date = end_date

            elif source == "binance":
                from dataloader.binance_loader import BinanceLoader

                loader = BinanceLoader()
                # 設置配置參數
                binance_config = config.get("binance_config", {})
                loader.symbol = binance_config.get("symbol", "BTCUSDT")
                loader.interval = binance_config.get("interval", "1d")
                loader.start_date = config.get("start_date", "2020-01-01")
                loader.end_date = end_date

            elif source == "coinbase":
                from dataloader.coinbase_loader import CoinbaseLoader

                loader = CoinbaseLoader()
                # 設置配置參數
                coinbase_config = config.get("coinbase_config", {})
                loader.symbol = coinbase_config.get("symbol", "BTC-USD")
                loader.start_date = config.get("start_date", "2020-01-01")
                loader.end_date = end_date

            elif source == "file":
                from dataloader.file_loader import FileLoader

                loader = FileLoader()
                # 設置配置參數
                file_config = config.get("file_config", {})
                loader.file_path = file_config.get("file_path", "")
                loader.time_column = file_config.get("time_column", "Time")
                loader.open_column = file_config.get("open_column", "Open")
                loader.high_column = file_config.get("high_column", "High")
                loader.low_column = file_config.get("low_column", "Low")
                loader.close_column = file_config.get("close_column", "Close")
                loader.volume_column = file_config.get("volume_column", "Volume")

            else:
                show_error("WFANALYSER", f"不支援的數據源: {source}")
                return None

            # 使用原版載入邏輯
            show_info("DATALOADER", f"正在從 {source} 載入數據")

            data, frequency = loader.load()

            if data is None:
                show_error("WFANALYSER", "數據載入失敗")
                return None

            # 設置屬性
            self.data = data
            self.frequency = frequency
            self.source = source

            # 處理預測因子（如果配置需要）
            predictor_config = config.get("predictor_config", {})
            skip_predictor = predictor_config.get("skip_predictor", False)

            if skip_predictor:
                # 使用價格數據（close）作為預測因子，直接返回，不嘗試載入預測因子
                if "Close" in self.data.columns:
                    self.data["X"] = self.data["Close"].copy()
                    self.current_predictor_column = "X"
                    self.using_price_predictor_only = True
                    # 直接返回，不繼續處理預測因子
                else:
                    show_error("WFANALYSER", "數據中找不到 Close 欄位")
                    return None
            else:
                # 載入預測因子
                self.data = self._load_predictor_data(config)

            # 更新載入摘要
            self._update_loading_summary(config)

            show_success("DATALOADER",
                f"數據載入成功\n"
                f"   數據形狀: {self.data.shape}\n"
                f"   頻率: {self.frequency}\n"
                f"   數據源: {self.source}"
            )

            return self.data

        except Exception as e:
            show_error("WFANALYSER", f"數據載入失敗: {e}\n\n詳細錯誤:\n{traceback.format_exc()}")
            self.logger.error(f"數據載入失敗: {e}")
            return None

    def _load_predictor_data(self, config: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """載入預測因子數據 - 使用 config 中的設置（非交互模式）"""
        try:
            predictor_config = config.get("predictor_config", {})
            predictor_path = predictor_config.get("predictor_path", "")

            if not predictor_path:
                show_warning("WFANALYSER", "未指定預測因子路徑，將使用價格數據作為預測因子")
                if "Close" in self.data.columns:
                    self.data["X"] = self.data["Close"].copy()
                    self.current_predictor_column = "X"
                    self.using_price_predictor_only = True
                return self.data

            # 直接讀取文件，不經過交互式選擇
            predictor_data = self._read_predictor_file_direct(
                predictor_path,
                predictor_config.get("time_column", "time"),
                predictor_config.get("time_format", ""),
            )

            if predictor_data is None:
                show_warning("WFANALYSER", "預測因子載入失敗，將使用價格數據作為預測因子")
                if "Close" in self.data.columns:
                    self.data["X"] = self.data["Close"].copy()
                    self.current_predictor_column = "X"
                    self.using_price_predictor_only = True
                return self.data

            # 合併預測因子到主數據
            predictor_column = predictor_config.get("predictor_column", "X")
            if predictor_column in predictor_data.columns:
                # 合併邏輯（根據時間對齊）
                self.data = self._merge_predictor_data(predictor_data, predictor_column)
                self.current_predictor_column = predictor_column
            else:
                show_warning("WFANALYSER", f"預測因子欄位 {predictor_column} 不存在，將使用價格數據")
                if "Close" in self.data.columns:
                    self.data["X"] = self.data["Close"].copy()
                    self.current_predictor_column = "X"
                    self.using_price_predictor_only = True

            return self.data

        except Exception as e:
            show_warning("WFANALYSER", f"預測因子載入失敗: {e}，將使用價格數據")
            if "Close" in self.data.columns:
                self.data["X"] = self.data["Close"].copy()
                self.current_predictor_column = "X"
                self.using_price_predictor_only = True
            return self.data

    def _read_predictor_file_direct(
        self, file_path: str, time_column: str, time_format: str
    ) -> Optional[pd.DataFrame]:
        """
        直接讀取預測因子文件（非交互模式）

        Args:
            file_path: 文件路徑
            time_column: 時間欄位名稱
            time_format: 時間格式

        Returns:
            Optional[pd.DataFrame]: 預測因子數據
        """
        import os

        # 檢查文件是否存在
        if not os.path.exists(file_path):
            self.logger.error(f"預測因子文件不存在: {file_path}")
            return None

        # 讀取文件
        try:
            if file_path.endswith(".xlsx"):
                data = pd.read_excel(file_path, engine="openpyxl")
            elif file_path.endswith(".csv"):
                data = pd.read_csv(file_path)
            else:
                self.logger.error(f"不支援的文件格式: {file_path}")
                return None
        except Exception as e:
            self.logger.error(f"讀取預測因子文件失敗: {e}")
            return None

        # 處理時間欄位
        data = self._process_predictor_time_column(data, time_column, time_format)
        if data is None:
            return None

        # 清洗和合併數據（使用 PredictorLoader 的內部方法）
        from dataloader.predictor_loader import PredictorLoader

        predictor_loader = PredictorLoader(self.data)
        merged_data = predictor_loader._clean_and_merge_data(data)

        return merged_data

    def _process_predictor_time_column(
        self, data: pd.DataFrame, time_column: str, time_format: str
    ) -> Optional[pd.DataFrame]:
        """
        處理預測因子的時間欄位

        Args:
            data: 預測因子數據
            time_column: 時間欄位名稱
            time_format: 時間格式

        Returns:
            Optional[pd.DataFrame]: 處理後的數據
        """
        # 查找時間欄位
        time_col = None
        for col in [time_column, "Time", "time", "timestamp", "Date", "date"]:
            if col in data.columns:
                time_col = col
                break

        if time_col is None:
            self.logger.error("預測因子數據中找不到時間欄位")
            return None

        # 轉換時間格式
        try:
            if time_format:
                data[time_col] = pd.to_datetime(data[time_col], format=time_format)
            else:
                # 嘗試自動檢測 Unix timestamp
                from dataloader.predictor_loader import PredictorLoader

                predictor_loader = PredictorLoader(self.data)
                data = predictor_loader._detect_and_convert_timestamp_predictor(
                    data, time_col
                )
                # 如果還是字符串，嘗試自動解析
                if data[time_col].dtype == "object":
                    data[time_col] = pd.to_datetime(data[time_col], errors="coerce")
        except Exception as e:
            self.logger.warning(f"時間欄位轉換失敗: {e}，嘗試自動解析")
            data[time_col] = pd.to_datetime(data[time_col], errors="coerce")

        # 重命名時間欄位為 Time（統一格式）
        if time_col != "Time":
            data["Time"] = data[time_col]
            if time_col not in ["Time", "time"]:
                data = data.drop(columns=[time_col])

        return data

    def _merge_predictor_data(
        self, predictor_data: pd.DataFrame, predictor_column: str
    ) -> pd.DataFrame:
        """合併預測因子數據到主數據"""
        try:
            # 確保主數據有 Time 欄位
            if "Time" not in self.data.columns:
                if "time" in self.data.columns:
                    self.data["Time"] = self.data["time"]
                else:
                    show_error("WFANALYSER", "主數據缺少 Time 欄位，無法合併預測因子")
                    return self.data

            # 確保預測因子數據有 Time 欄位
            predictor_time_col = None
            for col in ["Time", "time", "Time", "timestamp"]:
                if col in predictor_data.columns:
                    predictor_time_col = col
                    break

            if predictor_time_col is None:
                show_error("WFANALYSER", "預測因子數據缺少時間欄位，無法合併")
                return self.data

            # 轉換時間格式
            import pandas as pd

            self.data["Time"] = pd.to_datetime(self.data["Time"])
            predictor_data[predictor_time_col] = pd.to_datetime(
                predictor_data[predictor_time_col]
            )

            # 合併數據
            merged_data = self.data.merge(
                predictor_data[[predictor_time_col, predictor_column]],
                left_on="Time",
                right_on=predictor_time_col,
                how="left",
            )

            # 移除臨時時間欄位
            if predictor_time_col != "Time":
                merged_data = merged_data.drop(columns=[predictor_time_col])

            return merged_data

        except Exception as e:
            show_warning("WFANALYSER", f"預測因子合併失敗: {e}，將使用原始數據")
            return self.data

    def _update_loading_summary(self, config: Dict[str, Any]) -> None:
        """更新載入摘要"""
        self.loading_summary = {
            "source": self.source,
            "frequency": self.frequency,
            "data_shape": self.data.shape if self.data is not None else None,
            "predictor_column": self.current_predictor_column,
            "using_price_only": self.using_price_predictor_only,
        }

    def get_loading_summary(self) -> Dict[str, Any]:
        """
        獲取載入摘要

        Returns:
            Dict[str, Any]: 載入摘要信息
        """
        return self.loading_summary

    def display_loading_summary(self) -> None:
        """顯示載入摘要"""
        summary = self.get_loading_summary()
        show_info("DATALOADER",
            f"📊 數據載入摘要:\n"
            f"   數據源: {summary.get('source', 'unknown')}\n"
            f"   頻率: {summary.get('frequency', 'unknown')}\n"
            f"   數據形狀: {summary.get('data_shape', 'unknown')}\n"
            f"   預測因子欄位: {summary.get('predictor_column', 'unknown')}\n"
            f"   僅使用價格: {summary.get('using_price_only', False)}"
        )



