"""
WFAVisualization_plotter.py

【功能說明】
------------------------------------------------------------
本模組為 WFA 可視化平台的主類，負責協調數據載入、界面生成、回調處理等各個子模組。
提供標準化的 WFA 可視化平台介面，參考 BasePlotter 的設計。
- 負責 WFA 數據載入與解析
- 生成 WFA 專用的 Dash 應用界面
- 協調 WFA 相關子模組（數據導入、界面生成、回調處理、圖表組件）

【流程與數據流】
------------------------------------------------------------
- 主流程：初始化 → 數據載入 → 界面生成 → 回調設置 → 啟動服務
- 各模組間數據流明確，流程如下：

```mermaid
flowchart TD
    A[WFAVisualizationPlotter] -->|調用| B[WFADataImporter]
    A -->|調用| C[WFADashboardGenerator]
    A -->|調用| D[WFACallbackHandler]
    A -->|調用| E[WFAChartComponents]
    B -->|返回| F[WFA 數據列表]
    C -->|返回| G[Dash應用實例]
    D -->|設置| H[回調函數]
    F -->|輸入| C
    F -->|輸入| D
    G -->|輸出| I[WFA Web界面]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增主流程步驟、參數、界面元素時，請同步更新頂部註解與對應模組
- 若 WFA 數據結構有變動，需同步更新 WFADataImporter、WFADashboardGenerator、WFACallbackHandler 等依賴模組
- 新增/修改主流程、數據結構、界面格式時，務必同步更新本檔案與所有依賴模組
- WFADashboardGenerator 的界面生成邏輯與 WFACallbackHandler 的回調處理機制需要特別注意

【常見易錯點】
------------------------------------------------------------
- 主流程與各模組流程不同步，導致參數遺漏或界面顯示錯誤
- WFA 數據載入失敗導致界面無法生成
- Dash 回調函數命名衝突或依賴關係錯誤
- 子模組初始化順序錯誤導致依賴缺失

【範例】
------------------------------------------------------------
- 執行 WFA 可視化平台：plotter = WFAVisualizationPlotter(); plotter.run()
- 自訂數據路徑：plotter = WFAVisualizationPlotter(wfa_data_path="path/to/wfa/data")
- 自訂端口：plotter.run(host="127.0.0.1", port=8051, debug=False)

【與其他模組的關聯】
------------------------------------------------------------
- 調用 WFADataImporter、WFADashboardGenerator、WFACallbackHandler、WFAChartComponents
- 數據結構依賴 wfanalyser 產生的 parquet 檔案格式
- 與 BasePlotter 設計模式保持一致，便於維護和擴充

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本 WFA 可視化功能
- v1.1: 新增批量下載功能支援

【參考】
------------------------------------------------------------
- Base_plotter.py: 參考核心協調器的設計模式
- plotter/README.md: WFA 可視化平台詳細說明
- 其他模組如有依賴本檔案的行為，請於對應模組頂部註解標明
"""

import logging
import os
from typing import Any, Dict, List, Optional

from utils import show_step_panel


class WFAVisualizationPlotter:
    """
    WFA 可視化平台主類

    負責協調數據載入、界面生成、回調處理等各個子模組，
    提供標準化的 WFA 可視化平台介面。
    """

    def __init__(
        self,
        wfa_data_path: Optional[str] = None,
        metrics_data_path: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """
        初始化 WFA 可視化平台

        Args:
            wfa_data_path: wfanalyser 產生的 parquet 檔案目錄路徑
            metrics_data_path: metricstracker 產生的 parquet 檔案目錄路徑（可選）
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
        self.wfa_data = None
        self.app = None

        # 初始化子模組
        self._init_components()

    def _init_components(self):
        """初始化各個子模組"""
        try:
            from .WFACallbackHandler_plotter import WFACallbackHandler
            from .WFADashboardGenerator_plotter import WFADashboardGenerator
            from .WFADataImporter_plotter import WFADataImporter
            from .WFAChartComponents_plotter import WFAChartComponents

            self.data_importer = WFADataImporter(
                self.wfa_data_path, self.metrics_data_path, self.logger
            )
            self.dashboard_generator = WFADashboardGenerator(self.logger)
            self.chart_components = WFAChartComponents(self.logger)
            self.callback_handler = WFACallbackHandler(
                self.logger, self.chart_components
            )

            self.logger.info("WFA plotter 子模組初始化完成")
        except ImportError as e:
            self.logger.error(f"WFA plotter 子模組導入失敗: {e}")
            raise

    def load_data(self) -> List[Dict[str, Any]]:
        """
        載入和解析 WFA parquet 檔案

        Returns:
            List[Dict[str, Any]]: WFA 數據列表
        """
        try:
            self.logger.info("開始載入 WFA 數據")
            self.wfa_data = self.data_importer.load_all_wfa_files()
            self.logger.info(f"WFA 數據載入完成，共 {len(self.wfa_data)} 個檔案")
            return self.wfa_data
        except Exception as e:
            self.logger.error(f"WFA 數據載入失敗: {e}")
            raise

    def generate_dashboard(self) -> Any:
        """
        生成 Dash 應用界面

        Returns:
            Any: Dash 應用實例
        """
        try:
            if self.wfa_data is None:
                self.load_data()

            self.logger.info("開始生成 WFA Dash 界面")
            self.app = self.dashboard_generator.create_app(self.wfa_data)
            self.logger.info("WFA Dash 界面生成完成")
            return self.app
        except Exception as e:
            self.logger.error(f"WFA Dash 界面生成失敗: {e}")
            raise

    def setup_callbacks(self):
        """設置 Dash 回調函數"""
        try:
            if self.app is None:
                self.generate_dashboard()

            self.logger.info("開始設置 WFA 回調函數")
            self.callback_handler.setup_callbacks(
                self.app, self.wfa_data, self.chart_components
            )
            self.logger.info("WFA 回調函數設置完成")
        except Exception as e:
            self.logger.error(f"WFA 回調函數設置失敗: {e}")
            raise

    def run(self, host: str = "127.0.0.1", port: int = 8051, debug: bool = False):
        """
        運行 WFA 可視化平台

        Args:
            host: 主機地址，預設為 127.0.0.1
            port: 端口號，預設為 8051
            debug: 是否開啟調試模式，預設為 False
        """
        try:
            # 確保界面和回調都已設置
            if self.app is None:
                self.generate_dashboard()
            # 強制每次都 setup_callbacks，確保 callback 註冊
            self.setup_callbacks()

            self.logger.info(f"啟動 WFA 可視化平台於 http://{host}:{port}")

            # 顯示啟動信息
            step_content = (
                "🟢 載入 WFA 數據\n"
                "🟢 生成可視化介面[自動]\n"
                "\n"
                "[bold #dbac30]說明[/bold #dbac30]\n"
                "WFA 可視化平台已成功啟動！請按照以下方式開啟界面：\n\n"
                "[bold #dbac30]方式一：[/bold #dbac30] 直接點擊下方連結\n"
                f"[bold #dbac30]方式二：[/bold #dbac30] 在瀏覽器中輸入：[underline]http://{host}:{port}[/underline]\n\n"
                "[bold #dbac30]操作提示：[/bold #dbac30]\n"
                "• 界面開啟後可選擇檔案和策略\n"
                "• 點擊窗口框框內的按鈕切換指標顯示\n"
                "• 按 Ctrl+C 可停止服務"
            )
            show_step_panel("PLOTTER", 1, ["生成可視化介面"], step_content)

            # 運行應用
            self.app.run_server(host=host, port=port, debug=debug)

        except Exception as e:
            self.logger.error(f"WFA 可視化平台啟動失敗: {e}")
            raise


