"""
Base_autorunner.py

【功能說明】
------------------------------------------------------------
本模組為 lo2cin4bt Autorunner 的核心控制器，負責協調整個自動化回測流程。
提供配置文件驅動的回測執行，支援多配置文件的批次處理。
直接使用原版 backtester 的結果處理邏輯，避免重複實現。

【流程與數據流】
------------------------------------------------------------
- 主流程：配置文件選擇 → 配置驗證 → 數據載入 → 回測執行 → 績效分析
- 各模組間數據流明確，流程如下：

```mermaid
flowchart TD
    A[BaseAutorunner] -->|選擇配置| B[ConfigSelector]
    A -->|驗證配置| C[ConfigValidator]
    A -->|載入配置| D[ConfigLoader]
    A -->|載入數據| E[DataLoader]
    A -->|執行回測| F[BacktestRunner]
    A -->|績效分析| G[MetricsRunner]
```

【維護與擴充重點】
------------------------------------------------------------
- 直接使用原版 backtester 的結果處理邏輯，避免重複實現
- 若原版 backtester 介面有變動，需同步更新調用邏輯
- 新增/修改結果處理時，優先考慮在原版 backtester 中實現

【常見易錯點】
------------------------------------------------------------
- 配置文件格式錯誤導致載入失敗
- 模組調用順序錯誤導致執行失敗
- 錯誤處理不完善導致程序崩潰

【範例】
------------------------------------------------------------
- 執行單個配置：BaseAutorunner().run()
- 執行多個配置：BaseAutorunner().run_batch()

【與其他模組的關聯】
------------------------------------------------------------
- 調用 ConfigSelector、ConfigValidator、ConfigLoader、DataLoader、BacktestRunner、MetricsRunner
- 直接調用原版 backtester 模組進行結果處理
- 參數結構依賴 config_template.json
- 日誌系統依賴 main.py 的 logging 設定

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本功能實現
- v1.1: 新增多配置文件支援
- v1.2: 新增 Rich Panel 顯示和調試輸出
- v2.0: 重構為直接使用原版 backtester 結果處理邏輯，避免重複實現

【參考】
------------------------------------------------------------
- autorunner/DEVELOPMENT_PLAN.md
- Development_Guideline.md
- backtester/TradeRecordExporter_backtester.py
- main.py
"""

import logging
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from rich.text import Text

from autorunner.BacktestRunner_autorunner import BacktestRunnerAutorunner
from autorunner.utils import get_console
from utils import show_error, show_info, show_success, show_warning, show_welcome

# 導入 autorunner 模組
from autorunner.ConfigLoader_autorunner import ConfigLoader
from autorunner.ConfigSelector_autorunner import ConfigSelector
from autorunner.ConfigValidator_autorunner import ConfigValidator
from autorunner.DataLoader_autorunner import DataLoaderAutorunner
from autorunner.MetricsRunner_autorunner import MetricsRunnerAutorunner

# from rich.progress import Progress, SpinnerColumn, TextColumn  # 暫時註釋，後續使用

console = get_console()


class BaseAutorunner:
    """
    Autorunner 核心控制器

    負責協調整個自動化回測流程，包括配置文件選擇、驗證、載入、
    數據載入、回測執行、績效分析等步驟。
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        初始化 BaseAutorunner

        Args:
            logger: 日誌記錄器，如果為 None 則創建新的
        """

        self.logger = logger or logging.getLogger("lo2cin4bt.autorunner")
        self.logger.info("BaseAutorunner 初始化開始")

        # 建立 Rich Console 供全域輸出使用
        self.console = get_console()

        # 設定基本路徑
        self.project_root = Path(__file__).parent.parent
        self.configs_dir = self.project_root / "records" / "autorunner" / "backtester_autorunner"
        self.templates_dir = self.project_root / "autorunner" / "templates"

        # 確保目錄存在
        self._ensure_directories()

        # 初始化子模組
        self.config_selector = ConfigSelector(self.configs_dir, self.templates_dir)

        self.config_validator = ConfigValidator()

        self.config_loader = ConfigLoader()

        # 初始化執行模組

        self.data_loader = DataLoaderAutorunner(logger=self.logger)
        self.data_loader_frequency = None

        # 其他模組暫時為 None，後續實現
        self.backtest_runner: Optional[Any] = None
        self.metrics_runner: Optional[Any] = None

        self.logger.info("BaseAutorunner 初始化完成")

    def _ensure_directories(self) -> None:
        """確保必要的目錄存在"""

        directories = [
            self.configs_dir,
            self.templates_dir,
            self.project_root / "logs",
            self.project_root / "records" / "backtester",
            self.project_root / "records" / "metricstracker",
        ]

        for directory in directories:
            if not directory.exists():
                directory.mkdir(parents=True, exist_ok=True)

    def run(self) -> None:
        """
        執行 autorunner 主流程

        這是 autorunner 的主要入口點，協調整個自動化回測流程。
        """
        self.logger.info("開始執行 autorunner 主流程")

        try:
            # 顯示歡迎信息
            self._display_welcome()

            # 步驟1: 選擇配置文件
            selected_configs = self._select_configs()
            if not selected_configs:
                return

            # 步驟2: 驗證配置文件
            valid_configs = self._validate_configs(selected_configs)
            if not valid_configs:
                return

            # 步驟3: 載入配置文件
            config_data_list = self._load_configs(valid_configs)
            if not config_data_list:
                return

            # 步驟4: 執行配置文件
            self._execute_configs(config_data_list)

            self.logger.info("autorunner 主流程執行完成")

        except Exception as e:
            print(f"❌ [ERROR] autorunner 執行失敗: {e}")
            self.logger.error("autorunner 執行失敗: %s", e)
            self._display_error(f"autorunner 執行失敗: {e}")
            raise

    def _display_welcome(self) -> None:
        """顯示歡迎信息"""

        welcome_content = (
            "[bold #dbac30]🚀 lo2cin4bt Autorunner[/bold #dbac30]\n"
            "[white]自動化回測執行器 - 配置文件驅動，支援多配置批次執行[/white]\n\n"
            "✨ 功能特色:\n"
            "• 配置文件驅動，無需手動輸入\n"
            "• 支援多配置文件批次執行\n"
            "• 自動化數據載入、回測、績效分析\n"
            "• 豐富的調試輸出和進度顯示\n\n"
            "[bold yellow]準備開始自動化回測流程...[/bold yellow]"
        )

        show_welcome("🚀 Autorunner", welcome_content)

    def _select_configs(self) -> List[str]:
        """
        選擇要執行的配置文件

        Returns:
            List[str]: 選中的配置文件路徑列表
        """

        # 使用 ConfigSelector 選擇配置文件
        selected = self.config_selector.select_configs()

        if not selected:
            self._display_error("沒有選擇任何配置文件")
            return []

        return selected

    # 配置文件選擇相關方法已移至 ConfigSelector 模組

    def _validate_configs(self, config_files: List[str]) -> List[str]:
        """
        驗證配置文件

        Args:
            config_files: 配置文件路徑列表

        Returns:
            List[str]: 有效的配置文件路徑列表
        """

        # 使用 ConfigValidator 驗證配置文件
        validation_results = self.config_validator.validate_configs(config_files)

        # 顯示驗證結果摘要
        self.config_validator.display_validation_summary(
            config_files, validation_results
        )

        # 收集驗證通過的配置文件
        valid_configs = []
        for config_file, is_valid in zip(config_files, validation_results):
            if is_valid:
                valid_configs.append(config_file)

        return valid_configs

    def _load_configs(self, config_files: List[str]) -> List[Any]:
        """
        載入配置文件

        Args:
            config_files: 配置文件路徑列表

        Returns:
            List[Any]: 配置數據對象列表
        """

        # 使用 ConfigLoader 載入配置文件
        config_data_list = self.config_loader.load_configs(config_files)

        return config_data_list

    # 配置文件驗證相關方法已移至 ConfigValidator 模組

    def _execute_configs(self, config_data_list: List[Any]) -> None:
        """
        執行配置文件

        Args:
            config_data_list: 配置數據對象列表
        """

        for i, config_data in enumerate(config_data_list, 1):
            try:
                self._execute_single_config(config_data, i, len(config_data_list))

            except Exception as e:
                print(f"❌ [ERROR] 配置文件 {i} 執行失敗: {e}")
                self._display_error(f"配置文件 {config_data.file_name} 執行失敗: {e}")
                # 繼續執行下一個配置文件
                continue

    def _execute_single_config(
        self, config_data: Any, current: int, total: int
    ) -> None:
        """
        執行單個配置文件

        Args:
            config_data: 配置數據對象
            current: 當前配置文件編號
            total: 總配置文件數量
        """

        self._display_execution_progress(current, total, config_data.file_name)

        # 執行數據載入
        # 合併 dataloader_config 和 predictor_config
        full_dataloader_config = {
            **config_data.dataloader_config,
            "predictor_config": config_data.predictor_config,
        }
        data = self.data_loader.load_data(full_dataloader_config)
        self.data_loader_frequency = self.data_loader.frequency

        if data is not None:
            self.data_loader.display_loading_summary()

            if getattr(self.data_loader, "using_price_predictor_only", False):
                predictor_col = getattr(
                    self.data_loader, "current_predictor_column", None
                )
                if predictor_col:
                    config_data.backtester_config["selected_predictor"] = predictor_col
        else:
            self._display_error("數據載入失敗")
            return

        # 執行回測（傳遞完整的 config_data 以便獲取 dataloader 和 predictor 配置）
        backtest_results = self._execute_backtest(data, config_data.backtester_config, config_data)

        if backtest_results is not None:
            self._display_backtest_summary(backtest_results)
        else:
            self._display_error("回測執行失敗")
            return

        # 執行績效分析
        self._execute_metrics(backtest_results, config_data.metricstracker_config)
        
        # 清理記憶體：刪除大型對象以避免記憶體累積
        del data, backtest_results
        import gc
        gc.collect()

    def _execute_backtest(
        self, data: Any, backtest_config: Dict[str, Any], config_data: Any = None
    ) -> Optional[Dict[str, Any]]:
        """
        執行回測

        Args:
            data: 已載入的數據
            backtest_config: 回測配置
            config_data: 完整的配置數據對象（包含 dataloader 和 predictor 配置）

        Returns:
            回測結果或 None
        """

        try:

            backtest_runner = BacktestRunnerAutorunner()

            # 構建完整的 config，包含 dataloader 信息
            # 如果有 config_data，使用完整的配置信息
            if config_data:
                config = {
                    "backtester": backtest_config,
                    "dataloader": {
                        **config_data.dataloader_config,
                        "frequency": self.data_loader_frequency or config_data.dataloader_config.get("frequency", "1D"),
                        "predictor_config": config_data.predictor_config  # 包含 predictor 配置
                    }
                }
            else:
                # 向後兼容：如果沒有 config_data，只使用基本配置
                config = {
                    "backtester": backtest_config,
                    "dataloader": {"frequency": self.data_loader_frequency or "1D"}
                }
            results = backtest_runner.run_backtest(data, config)

            if results:
                return results

            return None

        except Exception as e:
            print(f"❌ [ERROR] 回測執行異常: {e}")
            print(f"❌ [ERROR] 詳細錯誤: {traceback.format_exc()}")
            return None

    def _display_backtest_summary(self, backtest_results: Dict[str, Any]) -> None:
        """
        顯示回測摘要 - 直接使用原版 backtester 的摘要顯示邏輯

        Args:
            backtest_results: 回測結果
        """

        try:
            if not backtest_results:
                return

            # 檢查結果數據
            results = backtest_results.get("results", [])
            if not results:
                print("⚠️ [WARNING] 沒有回測結果可顯示")
                return
            
            # 直接使用原版的 TradeRecordExporter 顯示摘要
            from backtester.TradeRecordExporter_backtester import TradeRecordExporter_backtester
            
            # 創建導出器並顯示摘要
            exporter = TradeRecordExporter_backtester(
                trade_records=pd.DataFrame(),
                frequency=backtest_results.get(
                    "frequency", self.data_loader_frequency or "1D"
                ),
                results=results,  # 直接使用 results 列表
                data=pd.DataFrame(),  # 空的 DataFrame，因為我們只需要摘要
                Backtest_id=backtest_results.get(
                    "Backtest_id",
                    backtest_results.get("config", {}).get("Backtest_id", ""),
                ),
                predictor_file_name=backtest_results.get("predictor_file_name", ""),
                predictor_column=backtest_results.get("predictor_column", ""),
                symbol=backtest_results.get("symbol"),
                **backtest_results.get(
                    "trading_params",
                    backtest_results.get("config", {}).get("trading_params", {}),
                )
            )
            
            # 直接調用原版方法，分支邏輯會自動判斷是否顯示用戶界面
            exporter.display_backtest_summary()
            
        except Exception as e:
            print(f"❌ [ERROR] 回測摘要顯示失敗: {e}")
            print(f"❌ [ERROR] 詳細錯誤: {traceback.format_exc()}")
            # 簡單的後備顯示
            results = backtest_results.get("results", [])
            print(f"✅ [SUCCESS] 回測完成，共 {len(results)} 個結果")
    

    def _display_execution_progress(
        self, current: int, total: int, config_name: str
    ) -> None:
        """顯示執行進度"""

        progress_content = (
            f"[bold white]正在執行配置文件 {current}/{total}[/bold white]\n"
            f"[yellow]配置文件: {config_name}[/yellow]\n"
            f"[green]進度: {'█' * current}{'░' * (total - current)} {current}/{total}[/green]"
        )

        show_info("AUTORUNNER", progress_content)

    def _display_error(self, message: str) -> None:
        """顯示錯誤信息"""

        show_error("AUTORUNNER", message)

    def _execute_metrics(
        self, backtest_results: Dict[str, Any], metrics_config: Dict[str, Any]
    ) -> None:
        """執行 MetricsRunner 分析"""

        try:

            self.metrics_runner = self.metrics_runner or MetricsRunnerAutorunner(
                logger=self.logger
            )
            summary: Optional[Dict[str, Any]] = self.metrics_runner.run(
                backtest_results, metrics_config
            )

            if summary:
                self.logger.info("Metrics summary: %s", summary)

        except Exception as e:
            print(f"❌ [ERROR] 績效分析執行異常: {e}")
            print(f"❌ [ERROR] 詳細錯誤: {traceback.format_exc()}")


if __name__ == "__main__":
    # 測試模式

    # 創建測試用的 logger
    test_logger = logging.getLogger("test")
    test_logger.setLevel(logging.DEBUG)

    # 創建 autorunner 實例
    autorunner = BaseAutorunner(logger=test_logger)

    # 執行 autorunner
    autorunner.run()
