"""
Base_wfanalyser.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT Walk-Forward Analysis (WFA) 框架的核心控制器，
負責協調整個 WFA 流程，包括配置管理、數據載入、參數優化、回測執行、結果導出等。

【流程與數據流】
------------------------------------------------------------
- 主流程：配置選擇/輸入 → 數據載入 → WFA 執行 → 結果導出 → 可視化
- 各模組間數據流明確，流程如下：

```mermaid
flowchart TD
    A[BaseWFAAnalyser] -->|選擇模式| B[輸入模式/JSON模式]
    B -->|載入配置| C[ConfigLoader]
    C -->|驗證配置| D[ConfigValidator]
    D -->|載入數據| E[DataLoader]
    E -->|執行WFA| F[WalkForwardEngine]
    F -->|參數優化| G[ParameterOptimizer]
    G -->|回測| H[VectorBacktestEngine]
    H -->|績效計算| I[MetricsTracker]
    I -->|導出結果| J[ResultsExporter]
    J -->|可視化| K[BaseWFAPlotter]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增流程步驟、結果欄位、參數顯示時，請同步更新頂部註解與對應模組
- 若參數結構有變動，需同步更新所有依賴模組
- CLI 互動邏輯與 Rich Panel 顯示需保持一致

【常見易錯點】
------------------------------------------------------------
- 窗口劃分邏輯錯誤導致數據不完整
- 參數優化結果未正確傳遞到測試窗口
- 結果導出格式不一致

【範例】
------------------------------------------------------------
- 執行 WFA：BaseWFAAnalyser().run()
- JSON 模式：BaseWFAAnalyser().run_json_mode()
- 輸入模式：BaseWFAAnalyser().run_input_mode()

【與其他模組的關聯】
------------------------------------------------------------
- 調用 ConfigLoader、ConfigValidator、DataLoader、WalkForwardEngine 等
- 重用 dataloader、backtester、metricstracker 模組
- 參數結構依賴 WFA JSON 配置格式

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本功能實現

【參考】
------------------------------------------------------------
- WFA.md: 開發計劃與疑難排解
- Development_Guideline.md: 開發規範
- autorunner/: 參考配置管理方式
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from rich.text import Text

from .utils import get_console
from utils import show_error, show_info, show_success, show_warning, show_welcome

console = get_console()


class BaseWFAAnalyser:
    """
    WFA 核心控制器

    負責協調整個 Walk-Forward Analysis 流程，包括配置管理、
    數據載入、參數優化、回測執行、結果導出等。
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        初始化 BaseWFAAnalyser

        Args:
            logger: 日誌記錄器，如果為 None 則創建新的
        """
        self.logger = logger or logging.getLogger("lo2cin4bt.wfanalyser")
        self.logger.info("BaseWFAAnalyser 初始化開始")

        # 建立 Rich Console 供全域輸出使用
        self.console = get_console()

        # 設定基本路徑
        self.project_root = Path(__file__).parent.parent
        self.configs_dir = self.project_root / "records" / "autorunner" / "wfanalyser_autorunner"
        self.output_dir = self.project_root / "records" / "wfanalyser"

        # 確保目錄存在
        self._ensure_directories()

        # 初始化子模組（延遲導入）
        self.config_loader = None
        self.config_validator = None
        self.config_selector = None
        self.data_loader = None
        self.walk_forward_engine = None
        self.results_exporter = None

        self.logger.info("BaseWFAAnalyser 初始化完成")

    def _ensure_directories(self) -> None:
        """確保必要的目錄存在"""
        directories = [
            self.configs_dir,
            self.output_dir,
            self.project_root / "logs",
        ]

        for directory in directories:
            if not directory.exists():
                directory.mkdir(parents=True, exist_ok=True)

    def run(self) -> None:
        """
        執行 WFA 主流程

        這是 WFA 的主要入口點，協調整個 Walk-Forward Analysis 流程。
        """
        self.logger.info("開始執行 WFA 主流程")

        try:
            # 顯示歡迎信息
            self._display_welcome()

            # 選擇執行模式
            mode = self._select_mode()
            if not mode:
                return

            if mode == "json":
                self.run_json_mode()
            elif mode == "input":
                self.run_input_mode()
            else:
                self._display_error("無效的模式選擇")

        except Exception as e:
            self.logger.error(f"WFA 執行失敗: {e}")
            self._display_error(f"WFA 執行失敗: {e}")
            raise

    def _select_mode(self) -> Optional[str]:
        """
        選擇執行模式

        Returns:
            Optional[str]: 選中的模式（"json" 或 "input"），如果取消則返回 None
        """
        show_info("WFANALYSER",
            "[bold #dbac30]請選擇執行模式：[/bold #dbac30]\n\n"
            "• 輸入 'json' 使用 JSON 配置文件模式\n"
            "• 輸入 'input' 使用互動輸入模式\n"
            "• 輸入 'q' 退出"
        )

        while True:
            user_input = input().strip().lower()

            if user_input == "q":
                return None

            if user_input in ["json", "input"]:
                return user_input

            show_error("WFANALYSER", "無效選擇，請輸入 'json'、'input' 或 'q'")

    def run_json_mode(self) -> None:
        """
        執行 JSON 配置模式

        從 JSON 配置文件讀取配置並執行 WFA。
        """
        self.logger.info("進入 JSON 配置模式")

        try:
            # 初始化配置管理模組
            from wfanalyser.ConfigLoader_wfanalyser import ConfigLoader
            from wfanalyser.ConfigSelector_wfanalyser import ConfigSelector
            from wfanalyser.ConfigValidator_wfanalyser import ConfigValidator

            self.config_loader = ConfigLoader()
            self.config_validator = ConfigValidator()
            self.config_selector = ConfigSelector(self.configs_dir)

            # 選擇配置文件
            selected_configs = self.config_selector.select_configs()
            if not selected_configs:
                return

            # 驗證配置文件
            valid_configs = self._validate_configs(selected_configs)
            if not valid_configs:
                return

            # 載入配置文件
            config_data_list = self._load_configs(valid_configs)
            if not config_data_list:
                return

            # 執行 WFA
            self._execute_wfa_configs(config_data_list)

        except Exception as e:
            self.logger.error(f"JSON 模式執行失敗: {e}")
            self._display_error(f"JSON 模式執行失敗: {e}")
            raise

    def run_input_mode(self) -> None:
        """
        執行互動輸入模式

        通過用戶互動收集配置並執行 WFA。
        """
        self.logger.info("進入互動輸入模式")

        try:
            # 初始化用戶界面模組
            from wfanalyser.UserInterface_wfanalyser import UserInterface

            user_interface = UserInterface(logger=self.logger)
            config = user_interface.collect_config()

            if config:
                # 執行 WFA
                self._execute_wfa_single(config)

        except ImportError as e:
            self._display_error(
                f"缺少用戶界面模組: {e}\n\n"
                "目前輸入模式尚未完成，請使用 JSON 配置模式。"
            )
            self.logger.error(f"缺少用戶界面模組: {e}")
        except Exception as e:
            self.logger.error(f"輸入模式執行失敗: {e}")
            self._display_error(f"輸入模式執行失敗: {e}")
            raise

    def _validate_configs(self, config_files: List[str]) -> List[str]:
        """
        驗證配置文件

        Args:
            config_files: 配置文件路徑列表

        Returns:
            List[str]: 有效的配置文件路徑列表
        """
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
        config_data_list = self.config_loader.load_configs(config_files)
        return config_data_list

    def _execute_wfa_configs(self, config_data_list: List[Any]) -> None:
        """
        執行多個 WFA 配置

        Args:
            config_data_list: 配置數據對象列表
        """
        for i, config_data in enumerate(config_data_list, 1):
            try:
                self._execute_wfa_single(config_data, i, len(config_data_list))
            except Exception as e:
                self.logger.error(f"配置文件 {i} 執行失敗: {e}")
                self._display_error(f"配置文件 {config_data.file_name} 執行失敗: {e}")
                continue

    def _execute_wfa_single(
        self, config_data: Any, current: int = 1, total: int = 1
    ) -> None:
        """
        執行單個 WFA 配置

        Args:
            config_data: 配置數據對象
            current: 當前配置編號
            total: 總配置數量
        """
        self._display_execution_progress(current, total, getattr(config_data, "file_name", "配置"))

        try:
            # 初始化 WFA 引擎
            from wfanalyser.WalkForwardEngine_wfanalyser import WalkForwardEngine

            self.walk_forward_engine = WalkForwardEngine(
                config_data, logger=self.logger
            )

            # 執行 WFA
            results = self.walk_forward_engine.run()

            if results:
                # 導出結果
                from wfanalyser.ResultsExporter_wfanalyser import ResultsExporter

                # 從 results 中獲取數據引用
                data = results.get("data") if isinstance(results, dict) else None
                
                self.results_exporter = ResultsExporter(
                    results, output_dir=self.output_dir, config_data=config_data, logger=self.logger, data=data
                )
                self.results_exporter.export()

                self._display_success("WFA 執行完成")
            else:
                self._display_error("WFA 執行失敗")

        except ImportError as e:
            self._display_error(
                f"缺少核心模組: {e}\n\n"
                "目前 WFA 核心引擎尚未完成，請檢查配置和模組。"
            )
            self.logger.error(f"缺少核心模組: {e}")
        except Exception as e:
            self._display_error(f"WFA 執行失敗: {e}")
            self.logger.error(f"WFA 執行失敗: {e}")
            raise

    def _display_welcome(self) -> None:
        """顯示歡迎信息"""
        welcome_content = (
            "[bold #dbac30]🚀 lo2cin4bt Walk-Forward Analysis[/bold #dbac30]\n"
            "[white]滾動前向分析 - 參數優化與策略驗證[/white]\n\n"
            "✨ 功能特色:\n"
            "• 支援標準 Walk-Forward 和 Anchored Walk-Forward\n"
            "• 自動參數優化（Sharpe 和 Calmar）\n"
            "• 訓練集 vs 測試集績效對比\n"
            "• 詳細結果導出與可視化\n\n"
            "[bold yellow]準備開始 Walk-Forward Analysis...[/bold yellow]"
        )

        from utils import show_welcome
        show_welcome("🚀 lo2cin4bt Walk-Forward Analysis", welcome_content)

    def _display_execution_progress(
        self, current: int, total: int, config_name: str
    ) -> None:
        """顯示執行進度"""
        progress_content = (
            f"[bold white]正在執行 WFA 配置 {current}/{total}[/bold white]\n"
            f"[yellow]配置: {config_name}[/yellow]\n"
            f"[green]進度: {'█' * current}{'░' * (total - current)} {current}/{total}[/green]"
        )

        show_info("WFANALYSER", progress_content)

    def _display_error(self, message: str) -> None:
        """顯示錯誤信息"""
        show_error("WFANALYSER", message)

    def _display_success(self, message: str) -> None:
        """顯示成功信息"""
        show_success("WFANALYSER", message)


