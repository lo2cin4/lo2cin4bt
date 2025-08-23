"""
Base_plotter.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 可視化平台框架的核心協調器，定義可視化平台的標準介面與主要流程，負責協調數據載入、界面生成、回調處理等各個子模組。

【流程與數據流】
------------------------------------------------------------
- 主流程：初始化 → 數據載入 → 界面生成 → 回調設置 → 啟動服務
- 各模組間數據流明確，流程如下：

```mermaid
flowchart TD
    A[BasePlotter] -->|調用| B[DataImporterPlotter]
    A -->|調用| C[DashboardGenerator]
    A -->|調用| D[CallbackHandler]
    B -->|返回| E[解析後的數據]
    C -->|返回| F[Dash應用實例]
    D -->|設置| G[回調函數]
    E -->|輸入| C
    E -->|輸入| D
    F -->|輸出| H[Web界面]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增主流程步驟、參數、界面元素時，請同步更新頂部註解與對應模組
- 若參數結構有變動，需同步更新 DashboardGenerator、CallbackHandler 等依賴模組
- 新增/修改主流程、參數結構、界面格式時，務必同步更新本檔案與所有依賴模組
- DashboardGenerator 的界面生成邏輯與 CallbackHandler 的回調處理機制需要特別注意

【常見易錯點】
------------------------------------------------------------
- 主流程與各模組流程不同步，導致參數遺漏或界面顯示錯誤
- 初始化環境未正確設置，導致下游模組報錯
- Dash 回調函數命名衝突或依賴關係錯誤

【錯誤處理】
------------------------------------------------------------
- 數據載入失敗時提供詳細錯誤訊息
- 界面生成錯誤時提供診斷建議
- 回調設置失敗時提供備用方案

【範例】
------------------------------------------------------------
- 執行可視化平台：python main.py (選擇選項5)
- 自訂參數啟動：python main.py --plotter --config config.json

【與其他模組的關聯】
------------------------------------------------------------
- 調用 DataImporterPlotter、DashboardGenerator、CallbackHandler
- 參數結構依賴 metricstracker 產生的 parquet 檔案格式
- BasePlotter 負責界面生成與回調處理

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，定義基本介面
- v1.1: 新增 Rich Panel 顯示和步驟跟蹤
- v1.2: 支援多模組協調架構

【參考】
------------------------------------------------------------
- 詳細流程規範如有變動，請同步更新本註解與 README
- 其他模組如有依賴本檔案的行為，請於對應模組頂部註解標明
- DashboardGenerator 的界面生成與 CallbackHandler 的回調處理邏輯請參考對應模組
"""

import logging
import os
from abc import ABC
from typing import Any, Dict, Optional

import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.text import Text


class BasePlotter(ABC):
    """
    可視化平台基底類

    負責協調數據載入、界面生成、回調處理等各個子模組，
    提供標準化的可視化平台介面。
    """

    def __init__(
        self, data_path: Optional[str] = None, logger: Optional[logging.Logger] = None
    ):
        """
        初始化可視化平台

        Args:
            data_path: metricstracker 產生的 parquet 檔案路徑，預設為 records/metricstracker
            logger: 日誌記錄器，預設為 None
        """
        self.data_path = data_path or os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "records", "metricstracker"
        )
        self.logger = logger or logging.getLogger(__name__)
        self.data = None
        self.app = None
        self.callback_handler = None

        # 初始化子模組
        self._init_components()

    def _init_components(self):
        """初始化各個子模組"""
        try:
            from .CallbackHandler_plotter import CallbackHandler
            from .DashboardGenerator_plotter import DashboardGenerator
            from .DataImporter_plotter import DataImporterPlotter

            self.data_importer = DataImporterPlotter(self.data_path, self.logger)
            self.dashboard_generator = DashboardGenerator(self.logger)
            self.callback_handler = CallbackHandler(self.logger)

            self.logger.info("plotter 子模組初始化完成")
        except ImportError as e:
            self.logger.error(f"plotter 子模組導入失敗: {e}")
            raise

    def load_data(self) -> Dict[str, Any]:
        """
        載入和解析 metricstracker 產生的 parquet 檔案

        Returns:
            Dict[str, Any]: 解析後的數據字典，包含：
                - 'dataframes': 各個參數組合的 DataFrame
                - 'parameters': 參數組合列表
                - 'metrics': 績效指標數據
                - 'equity_curves': 權益曲線數據
        """
        try:
            self.logger.info("開始載入 metricstracker 數據")
            self.data = self.data_importer.load_and_parse_data()
            self.logger.info(
                f"數據載入完成，共 {len(self.data.get('dataframes', {}))} 個參數組合"
            )
            return self.data
        except Exception as e:
            self.logger.error(f"數據載入失敗: {e}")
            raise

    def generate_dashboard(self) -> Any:
        """
        生成 Dash 應用界面

        Returns:
            Any: Dash 應用實例
        """
        try:
            if self.data is None:
                self.load_data()

            self.logger.info("開始生成 Dash 界面")
            self.app = self.dashboard_generator.create_app(self.data)
            self.logger.info("Dash 界面生成完成")
            return self.app
        except Exception as e:
            self.logger.error(f"Dash 界面生成失敗: {e}")
            raise

    def setup_callbacks(self):
        """設置 Dash 回調函數"""
        try:
            if self.app is None:
                self.generate_dashboard()

            self.logger.info("開始設置回調函數")

            # 設置主要的回調函數（資金曲線組合圖相關）
            self.callback_handler.setup_callbacks(self.app, self.data)

            # 設置參數高原的回調函數
            from .ParameterPlateau_plotter import ParameterPlateauPlotter

            plateau_plotter = ParameterPlateauPlotter()

            # 傳遞DataImporterPlotter實例以使用緩存
            plateau_plotter.data_importer = self.data_importer

            plateau_plotter.register_callbacks(self.app, self.data)

            self.logger.info("回調函數設置完成")
        except Exception as e:
            self.logger.error(f"回調函數設置失敗: {e}")
            raise

    def run(self, host: str = "127.0.0.1", port: int = 8050, debug: bool = False):
        """
        運行可視化平台

        Args:
            host: 主機地址，預設為 127.0.0.1
            port: 端口號，預設為 8050
            debug: 是否開啟調試模式，預設為 False
        """
        try:
            # 確保界面和回調都已設置
            if self.app is None:
                self.generate_dashboard()
            # 強制每次都setup_callbacks，確保callback註冊
            self.setup_callbacks()

            self.logger.info(f"啟動可視化平台於 http://{host}:{port}")
            console = Console()

            # 第二步：生成可視化介面[自動] - 步驟說明
            step_content = (
                "🟢 選擇要載入的檔案\n"
                "🟢 生成可視化介面[自動]\n"
                "\n"
                "[bold #dbac30]說明[/bold #dbac30]\n"
                "可視化平台已成功啟動！請按照以下方式開啟界面：\n\n"
                "[bold #dbac30]方式一：[/bold #dbac30] 直接點擊下方連結\n"
                f"[bold #dbac30]方式二：[/bold #dbac30] 在瀏覽器中輸入：[underline]http://{host}:{port}[/underline]\n\n"
                "[bold #dbac30]操作提示：[/bold #dbac30]\n"
                "• 界面開啟後可進行參數篩選、圖表互動\n"
                "• 支援多策略比較、績效分析\n"
                "• 按 Ctrl+C 可停止服務"
            )
            console.print(
                Panel(
                    step_content,
                    title=Text(
                        "👁️ 可視化 Plotter 步驟：生成可視化介面", style="bold #dbac30"
                    ),
                    border_style="#dbac30",
                )
            )

            # 啟動 Dash 應用
            self.app.run(host=host, port=port, debug=debug)

        except Exception as e:
            self.logger.error(f"可視化平台啟動失敗: {e}")
            raise

    def get_data_summary(self) -> Dict[str, Any]:
        """
        獲取數據摘要信息

        Returns:
            Dict[str, Any]: 數據摘要，包含：
                - 'total_combinations': 總參數組合數
                - 'parameters': 參數列表
                - 'date_range': 日期範圍
                - 'file_info': 檔案信息
        """
        if self.data is None:
            self.load_data()

        summary = {
            "total_combinations": len(self.data.get("dataframes", {})),
            "parameters": (
                list(self.data.get("parameters", {}).keys())
                if self.data.get("parameters")
                else []
            ),
            "date_range": self._get_date_range(),
            "file_info": self._get_file_info(),
        }

        return summary

    def _get_date_range(self) -> Dict[str, str]:
        """獲取數據的日期範圍"""
        try:
            if not self.data or not self.data.get("dataframes"):
                return {"start": "", "end": ""}

            # 從第一個 DataFrame 獲取日期範圍
            first_df = list(self.data["dataframes"].values())[0]
            if "Time" in first_df.columns:
                start_date = first_df["Time"].min()
                end_date = first_df["Time"].max()
                return {
                    "start": (
                        start_date.strftime("%Y-%m-%d")
                        if hasattr(start_date, "strftime")
                        else str(start_date)
                    ),
                    "end": (
                        end_date.strftime("%Y-%m-%d")
                        if hasattr(end_date, "strftime")
                        else str(end_date)
                    ),
                }
            return {"start": "", "end": ""}
        except Exception as e:
            self.logger.warning(f"獲取日期範圍失敗: {e}")
            return {"start": "", "end": ""}

    def _get_file_info(self) -> Dict[str, Any]:
        """獲取檔案信息"""
        try:
            if not self.data or not self.data.get("file_paths"):
                return {}

            file_info = {}
            for param_key, file_path in self.data["file_paths"].items():
                if os.path.exists(file_path):
                    stat = os.stat(file_path)
                    file_info[param_key] = {
                        "path": file_path,
                        "size": stat.st_size,
                        "modified": pd.Timestamp(stat.st_mtime, unit="s").strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }

            return file_info
        except Exception as e:
            self.logger.warning(f"獲取檔案信息失敗: {e}")
            return {}

    def validate_data(self) -> bool:
        """
        驗證數據格式和完整性

        Returns:
            bool: 數據是否有效
        """
        try:
            if self.data is None:
                return False

            # 檢查必要的數據結構
            required_keys = ["dataframes", "parameters", "metrics", "equity_curves"]
            for key in required_keys:
                if key not in self.data:
                    self.logger.warning(f"缺少必要數據鍵: {key}")
                    return False

            # 檢查數據是否為空
            if not self.data.get("dataframes"):
                self.logger.warning("數據框架為空")
                return False

            return True

        except Exception as e:
            self.logger.error(f"數據驗證失敗: {e}")
            return False

    def export_data(self, output_path: str, format: str = "csv"):
        """
        導出處理後的數據

        Args:
            output_path: 輸出路徑
            format: 輸出格式，支援 'csv', 'parquet', 'json'
        """
        try:
            if self.data is None:
                self.load_data()

            if format == "csv":
                for param_key, df in self.data.get("dataframes", {}).items():
                    file_path = os.path.join(output_path, f"{param_key}.csv")
                    df.to_csv(file_path, index=False)
            elif format == "parquet":
                for param_key, df in self.data.get("dataframes", {}).items():
                    file_path = os.path.join(output_path, f"{param_key}.parquet")
                    df.to_parquet(file_path, index=False)
            elif format == "json":
                import json

                summary = self.get_data_summary()
                file_path = os.path.join(output_path, "data_summary.json")
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(summary, f, ensure_ascii=False, indent=2)
            else:
                raise ValueError(f"不支援的輸出格式: {format}")

            self.logger.info(f"數據導出完成: {output_path}")
        except Exception as e:
            self.logger.error(f"數據導出失敗: {e}")
            raise
