"""
WFACallbackHandler_plotter.py

【功能說明】
------------------------------------------------------------
本模組為 WFA 可視化平台的回調處理器，負責處理 Dash 應用的所有交互事件。
- 處理檔案選擇變更，更新策略選項和主顯示區
- 處理策略選擇變更，更新窗口顯示和圖表
- 處理指標切換按鈕（Sharpe、Sortino、Calmar、MDD），更新九宮格熱力圖
- 處理下載按鈕點擊，觸發批量圖表下載

【流程與數據流】
------------------------------------------------------------
- 主流程：用戶交互 → 回調觸發 → 數據處理 → 界面更新
- 各模組間數據流明確，流程如下：

```mermaid
flowchart TD
    A[用戶交互] -->|觸發| B[WFACallbackHandler]
    B -->|處理| C[檔案/策略/指標變更]
    C -->|更新| D[WFAChartComponents]
    D -->|生成圖表| E[界面顯示]
    B -->|下載請求| F[WFADownloadHandler]
    F -->|保存圖片| G[文件系統]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增交互功能時，請同步更新頂部註解與對應的回調函數
- 若 WFA 數據結構有變動，需同步更新回調處理邏輯
- 新增/修改交互功能、數據結構時，務必同步更新本檔案與所有依賴模組
- 回調函數的 Input/Output/State 依賴關係需要特別注意，避免循環依賴

【常見易錯點】
------------------------------------------------------------
- 回調函數的 Input/Output ID 錯誤導致回調未觸發
- 數據格式不一致導致界面顯示錯誤
- 回調函數依賴關係錯誤導致循環更新
- 下載功能調用錯誤導致下載失敗

【範例】
------------------------------------------------------------
- 設置回調：handler = WFACallbackHandler(logger, chart_components); handler.setup_callbacks(app, wfa_data)

【與其他模組的關聯】
------------------------------------------------------------
- 被 WFAVisualizationPlotter 調用，處理 Dash 應用的回調
- 依賴 WFAChartComponents 生成圖表組件
- 依賴 WFADownloadHandler 處理下載功能
- 使用 WFA 數據結構（從 WFADataImporter 載入）

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本回調處理功能
- v1.1: 新增批量下載功能支援

【參考】
------------------------------------------------------------
- WFAVisualization_plotter.py: WFA 可視化平台主類
- WFAChartComponents_plotter.py: WFA 圖表組件生成器
- WFADownloadHandler_plotter.py: WFA 下載處理器
- plotter/README.md: WFA 可視化平台詳細說明
"""

import logging
from typing import Any, Dict, List, Optional

from dash import ALL, Input, Output, State, dcc, html
from dash import callback_context as ctx
from dash.exceptions import PreventUpdate


class WFACallbackHandler:
    """WFA 回調處理器"""

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        chart_components: Optional[Any] = None,
    ):
        """
        初始化回調處理器

        Args:
            logger: 日誌記錄器
            chart_components: WFAChartComponents 實例
        """
        self.logger = logger or logging.getLogger(__name__)
        self.wfa_data = None
        self.chart_components = chart_components

    def setup_callbacks(
        self,
        app: Any,
        wfa_data: List[Dict[str, Any]],
        chart_components: Optional[Any] = None,
    ):
        """
        設置所有回調函數

        Args:
            app: Dash 應用實例
            wfa_data: WFA 數據列表
            chart_components: WFAChartComponents 實例
        """
        self.wfa_data = wfa_data
        if chart_components:
            self.chart_components = chart_components

        # 檔案選擇變更時更新策略選項和主顯示區
        @app.callback(
            [
                Output("wfa-strategy-selector", "options"),
                Output("wfa-strategy-selector", "value"),
                Output("wfa-main-display", "children"),
            ],
            [Input("wfa-file-selector", "value")],
        )
        def update_strategy_and_display(selected_file):
            """當檔案選擇變更時，更新策略選項和主顯示區"""
            if not selected_file or not self.wfa_data:
                return [], None, []

            # 找到選中的檔案數據
            file_data = None
            for data in self.wfa_data:
                if data.get("filename") == selected_file:
                    file_data = data
                    break

            if not file_data:
                return [], None, []

            # 生成策略選項
            strategies = file_data.get("strategies", [])
            strategy_names = file_data.get("strategy_names", {})
            strategy_options = []
            for s in strategies:
                # 使用指標組合名稱，如果沒有則使用默認格式
                label = strategy_names.get(s, f"策略 {s.split('_')[1]}")
                strategy_options.append({"label": label, "value": s})
            default_strategy = strategies[0] if strategies else None

            # 生成主顯示區（所有窗口）
            main_display_children = self._generate_main_display(file_data, default_strategy)

            # 初始顯示時，確保所有窗口都使用 Sharpe 指標
            return strategy_options, default_strategy, main_display_children

        # 策略選擇變更時更新主顯示區
        @app.callback(
            Output("wfa-main-display", "children", allow_duplicate=True),
            [Input("wfa-strategy-selector", "value")],
            [State("wfa-file-selector", "value")],
            prevent_initial_call=True,
        )
        def update_display_on_strategy_change(selected_strategy, selected_file):
            """當策略選擇變更時，更新主顯示區"""
            if not selected_strategy or not selected_file or not self.wfa_data:
                raise PreventUpdate

            # 找到選中的檔案數據
            file_data = None
            for data in self.wfa_data:
                if data.get("filename") == selected_file:
                    file_data = data
                    break

            if not file_data:
                raise PreventUpdate

            # 生成主顯示區
            main_display_children = self._generate_main_display(file_data, selected_strategy)

            return main_display_children

        # 指標按鈕點擊時更新對應窗口的存儲和圖表
        @app.callback(
            [
                Output({"type": "wfa-metric-store", "window": ALL}, "data", allow_duplicate=True),
                Output({"type": "wfa-is-chart", "window": ALL}, "children", allow_duplicate=True),
                Output({"type": "wfa-oos-chart", "window": ALL}, "children", allow_duplicate=True),
                Output({"type": "wfa-metric-btn", "window": ALL, "metric": ALL}, "style", allow_duplicate=True),
            ],
            [Input({"type": "wfa-metric-btn", "window": ALL, "metric": ALL}, "n_clicks")],
            [
                State({"type": "wfa-metric-store", "window": ALL}, "data"),
                State("wfa-file-selector", "value"),
                State("wfa-strategy-selector", "value"),
                State({"type": "wfa-metric-btn", "window": ALL, "metric": ALL}, "id"),
            ],
            prevent_initial_call=True,
        )
        def update_charts_on_metric_click(n_clicks_list, current_metrics, selected_file, selected_strategy, button_ids):
            """當指標按鈕點擊時，更新對應窗口的指標存儲和圖表"""
            if not ctx.triggered or not selected_file or not selected_strategy:
                raise PreventUpdate

            # 找到觸發的按鈕
            triggered_id = ctx.triggered[0]["prop_id"].split(".")[0]
            import json
            triggered_id_dict = json.loads(triggered_id)
            clicked_window_id = triggered_id_dict.get("window")
            clicked_metric = triggered_id_dict.get("metric")

            if clicked_window_id is None or clicked_metric is None:
                raise PreventUpdate

            # 找到選中的檔案數據
            file_data = None
            for data in self.wfa_data:
                if data.get("filename") == selected_file:
                    file_data = data
                    break

            if not file_data:
                raise PreventUpdate

            # 獲取所有窗口數據
            windows_data = file_data.get("windows", {})
            
            # 找到對應策略的所有窗口
            windows_for_strategy = {}
            strategy_id = int(selected_strategy.split("_")[1])
            for key, window_data in windows_data.items():
                if window_data.get("condition_pair_id") == strategy_id:
                    window_id_key = window_data.get("window_id")
                    windows_for_strategy[window_id_key] = window_data

            # 更新指標存儲（使用列表，索引對應窗口順序）
            sorted_window_ids = sorted(windows_for_strategy.keys())
            
            # 將 current_metrics 轉換為列表（如果需要的話）
            if current_metrics is None:
                current_metrics = ["Sharpe"] * len(sorted_window_ids)
            elif isinstance(current_metrics, list):
                # 確保列表長度足夠
                while len(current_metrics) < len(sorted_window_ids):
                    current_metrics.append("Sharpe")
            else:
                current_metrics = ["Sharpe"] * len(sorted_window_ids)
            
            # 更新被點擊窗口的指標
            updated_metrics = current_metrics.copy()
            if clicked_window_id in sorted_window_ids:
                idx = sorted_window_ids.index(clicked_window_id)
                updated_metrics[idx] = clicked_metric

            # 生成更新後的圖表
            from .WFAChartComponents_plotter import WFAChartComponents

            chart_components = WFAChartComponents(self.logger)

            is_charts = []
            oos_charts = []
            button_styles = []

            # 為每個窗口生成圖表
            for idx, wid in enumerate(sorted_window_ids):
                window_data = windows_for_strategy[wid]
                param_info = window_data.get("param_info", {})
                matrices = window_data.get("matrices", {})

                # 確定當前窗口應該顯示的指標
                current_metric = updated_metrics[idx]

                # 獲取指標矩陣
                metric_key_map = {
                    "Sharpe": "is_sharpe",
                    "Sortino": "is_sortino",
                    "Calmar": "is_calmar",
                    "MDD": "is_mdd",
                }
                oos_metric_key_map = {
                    "Sharpe": "oos_sharpe",
                    "Sortino": "oos_sortino",
                    "Calmar": "oos_calmar",
                    "MDD": "oos_mdd",
                }

                is_metric_key = metric_key_map.get(current_metric, "is_sharpe")
                oos_metric_key = oos_metric_key_map.get(current_metric, "oos_sharpe")

                is_matrix = matrices.get(is_metric_key)
                oos_matrix = matrices.get(oos_metric_key)

                # 生成 IS 和 OOS 圖表
                is_chart = (
                    chart_components.create_3x3_heatmap(is_matrix, current_metric, param_info, "IS")
                    if is_matrix is not None
                    else html.Div(html.P("數據不可用", className="text-center"))
                )
                oos_chart = (
                    chart_components.create_3x3_heatmap(oos_matrix, current_metric, param_info, "OOS")
                    if oos_matrix is not None
                    else html.Div(html.P("數據不可用", className="text-center"))
                )

                is_charts.append(is_chart)
                oos_charts.append(oos_chart)

                # 更新按鈕樣式（為這個窗口的4個按鈕）
                for btn_metric in ["Sharpe", "Sortino", "Calmar", "MDD"]:
                    is_active = (btn_metric == current_metric)
                    button_styles.append(
                        {
                            "backgroundColor": "#dbac30" if is_active else "transparent",
                            "color": "#000000" if is_active else "#dbac30",
                            "border": "2px solid #dbac30",
                            "padding": "8px 15px",
                            "margin": "5px",
                            "cursor": "pointer",
                            "fontWeight": "bold",
                        }
                    )

            return updated_metrics, is_charts, oos_charts, button_styles

        # 下載按鈕回調（參考 ParameterPlateau 的實現方式）
        @app.callback(
            Output("wfa-download-status", "children"),
            [
                Input("wfa-btn-download-current-file", "n_clicks"),
                Input("wfa-btn-download-all-files", "n_clicks"),
            ],
            [State("wfa-file-selector", "value")],
        )
        def handle_download(n_clicks_current, n_clicks_all, selected_file):
            """處理批量下載（保存圖片到資料夾）"""
            if not ctx.triggered:
                raise PreventUpdate

            trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]

            try:
                from plotter.WFADownloadHandler_plotter import WFADownloadHandler

                download_handler = WFADownloadHandler(logger=self.logger)

                if trigger_id == "wfa-btn-download-current-file":
                    # 下載當前檔案的所有圖表
                    if not selected_file:
                        return html.P(
                            "請先選擇檔案",
                            style={"color": "#dbac30"},
                        )

                    # 找到對應的檔案數據
                    file_data = None
                    for data in self.wfa_data:
                        if data.get("filename") == selected_file:
                            file_data = data
                            break

                    if not file_data:
                        return html.P(
                            "找不到檔案數據",
                            style={"color": "#dbac30"},
                        )

                    # 生成所有圖表並保存到資料夾
                    result = download_handler.download_all_charts_for_file(
                        file_data, self.chart_components
                    )

                    status_messages = [
                        html.P(
                            f"✅ 下載完成！已下載 {result['downloaded_count']} 個圖表",
                            style={"color": "#dbac30", "fontWeight": "bold", "margin": "5px 0"},
                        ),
                    ]

                    if result["error_count"] > 0:
                        status_messages.append(
                            html.P(
                                f"⚠️ 有 {result['error_count']} 個圖表下載失敗",
                                style={"color": "#8f1511", "margin": "5px 0"},
                            )
                        )

                    if result.get("download_dir"):
                        status_messages.append(
                            html.P(
                                f"📁 保存位置: {result['download_dir']}",
                                style={"color": "#dbac30", "margin": "5px 0", "fontSize": "12px"},
                            ),
                        )
                    elif result.get("message"):
                        # 如果有錯誤訊息（如 PIL 未安裝），顯示錯誤訊息
                        status_messages.append(
                            html.P(
                                result["message"].replace("\n", " "),
                                style={"color": "#8f1511", "margin": "5px 0", "fontSize": "12px"},
                            ),
                        )

                    return html.Div(status_messages)

                elif trigger_id == "wfa-btn-download-all-files":
                    # 下載所有檔案的所有圖表
                    if not self.wfa_data:
                        return html.P(
                            "沒有載入任何檔案",
                            style={"color": "#dbac30"},
                        )

                    # 為所有檔案生成圖表並保存到資料夾
                    result = download_handler.download_all_charts_for_all_files(
                        self.wfa_data, self.chart_components
                    )

                    status_messages = [
                        html.P(
                            f"✅ 下載完成！已下載 {result['downloaded_count']} 個圖表（來自 {result['total_files']} 個檔案）",
                            style={"color": "#dbac30", "fontWeight": "bold", "margin": "5px 0"},
                        ),
                    ]

                    if result["error_count"] > 0:
                        status_messages.append(
                            html.P(
                                f"⚠️ 有 {result['error_count']} 個圖表下載失敗",
                                style={"color": "#8f1511", "margin": "5px 0"},
                            )
                        )

                    status_messages.append(
                        html.P(
                            f"📁 保存位置: records/plotter/ (每個檔案有自己的資料夾)",
                            style={"color": "#dbac30", "margin": "5px 0", "fontSize": "12px"},
                        ),
                    )

                    return html.Div(status_messages)

            except Exception as e:
                import traceback
                error_msg = str(e)
                error_traceback = traceback.format_exc()
                self.logger.error(f"下載處理失敗: {error_msg}")
                self.logger.error(error_traceback)
                # 在界面上也顯示詳細錯誤
                return html.Div(
                    [
                        html.P(
                            f"❌ 下載失敗: {error_msg}",
                            style={"color": "#8f1511", "fontWeight": "bold"},
                        ),
                        html.P(
                            "請查看終端/日誌以獲取詳細錯誤信息",
                            style={"color": "#dbac30", "fontSize": "12px"},
                        ),
                    ]
                )

            raise PreventUpdate

    def _generate_main_display(
        self, file_data: Dict[str, Any], selected_strategy: str
    ) -> List:
        """
        生成主顯示區的所有窗口框框

        Args:
            file_data: 檔案數據
            selected_strategy: 選中的策略（例如 "strategy_1"）

        Returns:
            List: 窗口框框組件列表
        """
        try:
            from .WFADashboardGenerator_plotter import WFADashboardGenerator

            dashboard_generator = WFADashboardGenerator(self.logger)
            windows_data = file_data.get("windows", {})

            # 找到對應策略的所有窗口
            windows_for_strategy = {}
            strategy_id = int(selected_strategy.split("_")[1])
            for key, window_data in windows_data.items():
                if window_data.get("condition_pair_id") == strategy_id:
                    window_id = window_data.get("window_id")
                    windows_for_strategy[window_id] = window_data

            # 按 window_id 排序
            sorted_window_ids = sorted(windows_for_strategy.keys())

            # 生成窗口框框，每行兩個
            window_boxes = []
            for i, window_id in enumerate(sorted_window_ids):
                window_data = windows_for_strategy[window_id]
                window_box = dashboard_generator.create_window_box(
                    window_id, window_data, window_data.get("param_info", {}), "Sharpe"
                )
                window_boxes.append(window_box)

                # 每兩個窗口換行
                if (i + 1) % 2 == 0:
                    window_boxes.append(html.Div(style={"width": "100%", "clear": "both"}))

            return window_boxes

        except Exception as e:
            self.logger.error(f"生成主顯示區失敗: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return [html.Div(f"生成主顯示區失敗: {str(e)}")]

