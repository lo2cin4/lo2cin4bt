"""
WFADashboardGenerator_plotter.py

【功能說明】
------------------------------------------------------------
本模組為 WFA 可視化平台的界面生成器，負責生成 Dash 界面布局。
- 創建 WFA 專用的 Dash 應用實例
- 生成文件選擇、策略選擇、窗口顯示等控制組件
- 構建完整的 WFA 可視化界面布局

【流程與數據流】
------------------------------------------------------------
- 主流程：接收 WFA 數據 → 生成界面組件 → 創建 Dash 應用 → 返回應用實例
- 各模組間數據流明確，流程如下：

```mermaid
flowchart TD
    A[WFAVisualizationPlotter] -->|調用| B[WFADashboardGenerator]
    B -->|接收| C[WFA 數據列表]
    C -->|生成| D[界面組件]
    D -->|創建| E[Dash 應用]
    E -->|返回| F[WFAVisualizationPlotter]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增界面元素、控制組件時，請同步更新頂部註解與對應模組
- 若 WFA 數據結構有變動，需同步更新界面生成邏輯
- 新增/修改界面元素、數據結構時，務必同步更新本檔案與所有依賴模組
- 界面布局與回調處理需要特別注意協調

【常見易錯點】
------------------------------------------------------------
- 界面組件 ID 衝突導致回調錯誤
- 數據結構與界面顯示不一致
- Dash 組件佈局錯誤導致顯示異常

【範例】
------------------------------------------------------------
- 創建界面生成器：generator = WFADashboardGenerator(logger)
- 生成 Dash 應用：app = generator.create_app(wfa_data)

【與其他模組的關聯】
------------------------------------------------------------
- 被 WFAVisualizationPlotter 調用，生成 WFA Dash 界面
- 依賴 WFA 數據結構（從 WFADataImporter 載入）
- 使用 plotter/utils/DashAppUtils_utils_plotter.py 創建 Dash 應用

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本界面生成功能

【參考】
------------------------------------------------------------
- WFAVisualization_plotter.py: WFA 可視化平台主類
- WFADataImporter_plotter.py: WFA 數據導入器
- plotter/utils/DashAppUtils_utils_plotter.py: Dash 應用工具
- plotter/README.md: WFA 可視化平台詳細說明
"""

import logging
import os
from typing import Any, Dict, List, Optional

import dash
import dash_bootstrap_components as dbc
from dash import dcc, html


class WFADashboardGenerator:
    """WFA Dash 界面生成器"""

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        初始化界面生成器

        Args:
            logger: 日誌記錄器
        """
        self.logger = logger or logging.getLogger(__name__)
        self.app = None

    def create_app(self, wfa_data: List[Dict[str, Any]]) -> dash.Dash:
        """
        創建 Dash 應用

        Args:
            wfa_data: WFA 數據列表（從 WFADataImporter 載入的數據）

        Returns:
            dash.Dash: Dash 應用實例
        """
        try:
            layout = self._create_layout(wfa_data)
            from .utils.DashAppUtils_utils_plotter import create_dash_app
            
            self.app = create_dash_app(
                layout=layout,
                app_title="WFA 可視化平台",
                logger=self.logger,
            )
            return self.app
        except Exception as e:
            self.logger.error(f"創建 WFA Dash 應用失敗: {e}")
            raise

    def _create_layout(self, wfa_data: List[Dict[str, Any]]) -> html.Div:
        """
        創建應用布局

        Args:
            wfa_data: WFA 數據列表

        Returns:
            html.Div: 布局組件
        """
        try:
            # 獲取檔案列表
            file_options = []
            if wfa_data:
                for data in wfa_data:
                    filename = data.get("filename", "未知檔案")
                    file_options.append({"label": filename, "value": filename})

            # 頂部控制區
            top_controls = self._create_top_controls(file_options)

            # 主顯示區（窗口框框會通過回調動態生成）
            main_display = html.Div(
                id="wfa-main-display",
                children=[],
                style={"padding": "20px"},
            )

            layout = html.Div(
                [
                    top_controls,
                    main_display,
                ],
                style={
                    "backgroundColor": "#000000",
                    "color": "#ffffff",
                    "minHeight": "100vh",
                    "padding": "20px",
                },
            )

            return layout
        except Exception as e:
            self.logger.error(f"創建布局失敗: {e}")
            raise

    def _create_top_controls(self, file_options: List[Dict[str, str]]) -> html.Div:
        """
        創建頂部控制區

        Args:
            file_options: 檔案選項列表

        Returns:
            html.Div: 頂部控制區組件
        """
        # 標題
        title = html.H5(
            "📊 前向分析 (WFA)",
            className="mb-3",
            style={"textAlign": "center"},
        )

        # 檔案選擇下拉選單（第一行，延長至右側）
        file_selector = html.Div(
            [
                html.Label(
                    "選擇檔案:",
                    style={"color": "#dbac30", "fontWeight": "bold", "marginRight": "10px", "display": "inline-block"},
                ),
                dcc.Dropdown(
                    id="wfa-file-selector",
                    options=file_options,
                    value=file_options[0]["value"] if file_options else None,
                    style={
                        "width": "100%",
                        "backgroundColor": "#181818",
                        "color": "#ffffff",
                    },
                    clearable=False,
                    placeholder="請選擇檔案...",
                ),
            ],
            style={"width": "100%", "marginBottom": "15px"},
        )

        # 策略選擇下拉選單（第二行，延長至右側）
        strategy_selector = html.Div(
            [
                html.Label(
                    "選擇策略:",
                    style={"color": "#dbac30", "fontWeight": "bold", "marginRight": "10px", "display": "inline-block"},
                ),
                dcc.Dropdown(
                    id="wfa-strategy-selector",
                    options=[],
                    value=None,
                    style={
                        "width": "100%",
                        "backgroundColor": "#181818",
                        "color": "#ffffff",
                    },
                    clearable=False,
                    placeholder="請選擇策略...",
                ),
            ],
            style={"width": "100%", "marginBottom": "15px"},
        )

        # 批量下載按鈕（放在策略選擇下方，並排展示）
        download_buttons = html.Div(
            [
                dbc.Button(
                    "下載當前檔案所有圖表",
                    id="wfa-btn-download-current-file",
                    n_clicks=0,
                    color="success",
                    outline=True,
                    className="me-2",
                    style={"flex": "1"},
                ),
                dbc.Button(
                    "下載所有檔案所有圖表",
                    id="wfa-btn-download-all-files",
                    n_clicks=0,
                    color="warning",
                    outline=True,
                    style={"flex": "1"},
                ),
            ],
            style={"display": "flex", "width": "100%", "gap": "10px"},
        )

        # 下載狀態顯示區域
        download_status = html.Div(
            id="wfa-download-status",
            style={"marginTop": "10px", "color": "#dbac30", "textAlign": "center"},
        )

        # 組合頂部控制區
        top_controls = html.Div(
            [
                title,
                html.Div(
                    [
                        file_selector,
                        strategy_selector,
                        download_buttons,
                        download_status,
                    ],
                    style={
                        "width": "100%",
                        "maxWidth": "1200px",
                        "margin": "0 auto",
                        "marginBottom": "30px",
                    },
                ),
            ],
            style={"marginBottom": "30px"},
        )

        return top_controls

    def create_window_box(
        self,
        window_id: int,
        window_data: Dict[str, Any],
        param_info: Dict[str, Any],
        metric: str = "Sharpe",
    ) -> html.Div:
        """
        創建單個窗口的金色框框

        Args:
            window_id: 窗口 ID
            window_data: 窗口數據字典
            param_info: 參數信息
            metric: 當前選中的指標（Sharpe, Sortino, Calmar, MDD）

        Returns:
            html.Div: 窗口框框組件
        """
        try:
            from .WFAChartComponents_plotter import WFAChartComponents

            chart_components = WFAChartComponents(self.logger)
            matrices = window_data.get("matrices", {})

            # 確保 window_id 是 Python 原生 int 類型（不是 np.int64）
            window_id = int(window_id)

            # 隱藏的存儲組件，用於追蹤當前窗口的指標選擇
            metric_store = dcc.Store(
                id={"type": "wfa-metric-store", "window": int(window_id)},
                data=metric,
            )

            # 四個按鈕
            button_style_active = {
                "backgroundColor": "#dbac30",
                "color": "#000000",
                "border": "2px solid #dbac30",
                "padding": "8px 15px",
                "margin": "5px",
                "cursor": "pointer",
                "fontWeight": "bold",
            }
            button_style_inactive = {
                "backgroundColor": "transparent",
                "color": "#dbac30",
                "border": "2px solid #dbac30",
                "padding": "8px 15px",
                "margin": "5px",
                "cursor": "pointer",
                "fontWeight": "bold",
            }

            buttons = html.Div(
                [
                    metric_store,  # 隱藏存儲組件
                    html.Button(
                        "Sharpe",
                        id={"type": "wfa-metric-btn", "window": int(window_id), "metric": "Sharpe"},
                        n_clicks=0,
                        style=button_style_active if metric == "Sharpe" else button_style_inactive,
                    ),
                    html.Button(
                        "Sortino",
                        id={"type": "wfa-metric-btn", "window": int(window_id), "metric": "Sortino"},
                        n_clicks=0,
                        style=button_style_active if metric == "Sortino" else button_style_inactive,
                    ),
                    html.Button(
                        "Calmar",
                        id={"type": "wfa-metric-btn", "window": int(window_id), "metric": "Calmar"},
                        n_clicks=0,
                        style=button_style_active if metric == "Calmar" else button_style_inactive,
                    ),
                    html.Button(
                        "MDD",
                        id={"type": "wfa-metric-btn", "window": int(window_id), "metric": "MDD"},
                        n_clicks=0,
                        style=button_style_active if metric == "MDD" else button_style_inactive,
                    ),
                ],
                style={"textAlign": "center", "marginBottom": "20px"},
            )

            # 獲取指標矩陣
            metric_key_map = {
                "Sharpe": "is_sharpe",
                "Sortino": "is_sortino",
                "Calmar": "is_calmar",
                "MDD": "max_drawdown",
            }
            oos_metric_key_map = {
                "Sharpe": "oos_sharpe",
                "Sortino": "oos_sortino",
                "Calmar": "oos_calmar",
                "MDD": "max_drawdown",
            }

            is_metric_key = metric_key_map.get(metric, "is_sharpe")
            oos_metric_key = oos_metric_key_map.get(metric, "oos_sharpe")

            is_matrix = matrices.get(is_metric_key)
            oos_matrix = matrices.get(oos_metric_key)

            # IS 和 OOS 九宮格
            charts_row = html.Div(
                [
                    html.Div(
                        [
                            html.Div(
                                id={"type": "wfa-is-chart", "window": int(window_id)},
                                children=chart_components.create_3x3_heatmap(
                                    is_matrix, metric, param_info, "IS"
                                )
                                if is_matrix is not None
                                else html.P("數據不可用", className="text-center"),
                            ),
                        ],
                        style={"display": "inline-block", "marginRight": "10px"},  # 減少留空：從 20px 減少到 10px
                    ),
                    html.Div(
                        html.Span("→", style={"fontSize": "40px", "color": "#dbac30"}),
                        style={
                            "display": "inline-flex",  # 使用 flex 以更好地控制垂直對齊
                            "alignItems": "center",     # 垂直置中
                            "justifyContent": "center",
                            "height": "350px",          # 與圖表高度一致
                        },
                    ),
                    html.Div(
                        [
                            html.Div(
                                id={"type": "wfa-oos-chart", "window": int(window_id)},
                                children=chart_components.create_3x3_heatmap(
                                    oos_matrix, metric, param_info, "OOS"
                                )
                                if oos_matrix is not None
                                else html.P("數據不可用", className="text-center"),
                            ),
                        ],
                        style={"display": "inline-block", "marginLeft": "10px"},  # 減少留空：從 20px 減少到 10px
                    ),
                ],
                style={
                    "textAlign": "center",
                    "marginBottom": "20px",
                    "display": "flex",        # 使用 flexbox 布局
                    "alignItems": "center",   # 垂直置中所有元素
                    "justifyContent": "center",
                },
            )

            # 窗口信息（底部布局：最佳參數置左，時間信息置右）
            train_start = window_data.get("train_start_date", "N/A")
            train_end = window_data.get("train_end_date", "N/A")
            test_start = window_data.get("test_start_date", "N/A")
            test_end = window_data.get("test_end_date", "N/A")
            
            # 提取最佳表現的參數（從 param_info 中獲取，或從第一個參數組合獲取）
            best_params_text = "最佳參數：計算中..."
            param_info = window_data.get("param_info", {})
            if param_info:
                param1_key = param_info.get("param1_key", "")
                param2_key = param_info.get("param2_key", "")
                param1_values = param_info.get("param1_values", [])
                param2_values = param_info.get("param2_values", [])
                if param1_key and param1_values:
                    best_param1 = param1_values[1] if len(param1_values) > 1 else param1_values[0] if param1_values else "N/A"
                    best_param2 = param2_values[1] if param2_key and len(param2_values) > 1 else param2_values[0] if param2_values else None
                    if best_param2:
                        best_params_text = f"{param1_key}: {best_param1}, {param2_key}: {best_param2}"
                    else:
                        best_params_text = f"{param1_key}: {best_param1}"

            window_info = html.Div(
                [
                    # 左側：最佳參數
                    html.Div(
                        [
                            html.P(
                                best_params_text,
                                style={
                                    "color": "#dbac30",
                                    "fontWeight": "bold",
                                    "margin": "5px 0",
                                    "textAlign": "left",
                                },
                            ),
                        ],
                        style={"flex": "1", "textAlign": "left"},
                    ),
                    # 右側：時間信息
                    html.Div(
                        [
                            html.P(
                                f"window {window_id}",
                                style={
                                    "color": "#dbac30",
                                    "fontWeight": "bold",
                                    "margin": "5px 0",
                                    "textAlign": "right",
                                },
                            ),
                            html.P(
                                f"IS: {train_start} - {train_end}",
                                style={"margin": "5px 0", "textAlign": "right"},
                            ),
                            html.P(
                                f"OOS: {test_start} - {test_end}",
                                style={"margin": "5px 0", "textAlign": "right"},
                            ),
                        ],
                        style={"flex": "1", "textAlign": "right"},
                    ),
                ],
                style={
                    "display": "flex",
                    "justifyContent": "space-between",
                    "alignItems": "flex-start",
                    "marginTop": "10px",
                },
            )

            # 組合金色框框
            window_box = html.Div(
                [buttons, charts_row, window_info],
                style={
                    "border": "3px solid #dbac30",
                    "backgroundColor": "#181818",
                    "padding": "20px",
                    "margin": "10px",
                    "borderRadius": "10px",
                    "display": "inline-block",
                    "width": "calc(50% - 40px)",  # 兩個框框並排，每個佔50%寬度
                    "verticalAlign": "top",
                    "minWidth": "600px",
                },
            )

            return window_box

        except Exception as e:
            self.logger.error(f"創建窗口框框失敗: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return html.Div(f"創建窗口框框失敗: {str(e)}")

