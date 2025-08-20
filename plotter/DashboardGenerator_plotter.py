"""
DashboardGenerator_plotter.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 可視化平台的界面生成核心模組，負責生成基於 Dash 的可視化平台界面，包含參數篩選控制面板、權益曲線圖表、績效指標表格等組件。

【流程與數據流】
------------------------------------------------------------
- 主流程：接收數據 → 生成布局 → 創建組件 → 設置樣式 → 返回應用
- 各模組間數據流明確，流程如下：

```mermaid
flowchart TD
    A[DashboardGenerator] -->|接收| B[解析後的數據]
    B -->|生成| C[布局結構]
    C -->|創建| D[控制組件]
    C -->|創建| E[圖表組件]
    C -->|創建| F[指標組件]
    D -->|組合| G[Dash應用]
    E -->|組合| G
    F -->|組合| G
    G -->|返回| H[Web界面]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增界面元素、布局結構時，請同步更新頂部註解與對應模組
- 若組件結構有變動，需同步更新 CallbackHandler 的回調函數
- 新增/修改界面元素、布局結構時，務必同步更新本檔案與所有依賴模組
- 組件 ID 命名和布局結構需要特別注意一致性

【常見易錯點】
------------------------------------------------------------
- 組件 ID 命名衝突
- 布局結構不正確
- 樣式設置錯誤
- 數據綁定失敗

【錯誤處理】
------------------------------------------------------------
- 組件創建失敗時提供詳細錯誤訊息
- 布局生成錯誤時提供診斷建議
- 樣式設置失敗時提供備用方案

【範例】
------------------------------------------------------------
- 基本使用：generator = DashboardGenerator()
- 創建應用：app = generator.create_app(data)

【與其他模組的關聯】
------------------------------------------------------------
- 被 BasePlotter 調用
- 依賴 ChartComponents 和 MetricsDisplay 生成組件
- 輸出 Dash 應用供 CallbackHandler 設置回調

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，支援基本界面生成
- v1.1: 新增動態組件生成支援
- v1.2: 新增多指標自動擴充功能

【參考】
------------------------------------------------------------
- 詳細流程規範如有變動，請同步更新本註解與 README
- 其他模組如有依賴本檔案的行為，請於對應模組頂部註解標明
- Dash 組件創建和布局設計請參考 Dash 官方文檔
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional

import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import dcc, html


class DashboardGenerator:
    """
    Dash 界面生成器

    負責生成基於 Dash 的可視化平台界面，
    包含參數篩選、圖表顯示、績效指標等組件。
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        初始化界面生成器

        Args:
            logger: 日誌記錄器，預設為 None
        """
        self.logger = logger or logging.getLogger(__name__)
        self.app = None

    def create_app(self, data: Dict[str, Any]) -> dash.Dash:
        """
        創建 Dash 應用

        Args:
            data: 解析後的數據字典

        Returns:
            dash.Dash: Dash 應用實例
        """
        try:
            self.logger.info("開始創建 Dash 應用")
            layout = self._create_layout(data)
            # 自定義主題色
            external_stylesheets = [
                dbc.themes.BOOTSTRAP,  # 用基本主題，全部自定義
                "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.5/font/bootstrap-icons.css",
            ]
            assets_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), "assets"
            )
            self.app = dash.Dash(
                __name__,
                external_stylesheets=external_stylesheets,
                suppress_callback_exceptions=True,
                assets_folder=assets_path,
            )
            self.app.title = "Lo2cin4BT 可視化平台"
            self.app.layout = layout
            self.logger.info("Dash 應用創建完成")
            return self.app
        except Exception as e:
            self.logger.error(f"創建 Dash 應用失敗: {e}")
            raise

    def _create_layout(self, data: Dict[str, Any]) -> html.Div:
        """
        創建應用布局

        Args:
            data: 解析後的數據字典

        Returns:
            html.Div: 布局組件
        """
        try:
            from .DataImporter_plotter import DataImporterPlotter

            indicator_param_structure = (
                DataImporterPlotter.parse_indicator_param_structure(
                    data.get("parameters", [])
                )
            )
            layout = html.Div(
                [
                    # 標題欄
                    self._create_header(),
                    # 主要內容區域
                    dbc.Container(
                        [
                            # 資產曲線頁面布局（帶左側控制面板）
                            html.Div(
                                [
                                    dbc.Row(
                                        [
                                            # 左側控制面板
                                            dbc.Col(
                                                [
                                                    self._create_control_panel(
                                                        indicator_param_structure
                                                    )
                                                ],
                                                width=3,
                                            ),
                                            # 右側主要內容
                                            dbc.Col(
                                                [
                                                    self._create_asset_curve_content(
                                                        data
                                                    )
                                                ],
                                                width=9,
                                            ),
                                        ]
                                    )
                                ],
                                id="layout-asset-curve-with-panel",
                                style={"display": "block"},
                            ),
                            # 參數高原頁面布局（全寬，無左側控制面板）
                            html.Div(
                                [
                                    dbc.Row(
                                        [
                                            dbc.Col(
                                                [
                                                    self._create_parameter_landscape_content(
                                                        data
                                                    )
                                                ],
                                                width=12,
                                            )
                                        ]
                                    )
                                ],
                                id="layout-parameter-landscape-full",
                                style={"display": "none"},
                            ),
                        ],
                        fluid=True,
                    ),
                ]
            )

            return layout

        except Exception as e:
            self.logger.error(f"創建布局失敗: {e}")
            raise

    def _create_header(self) -> html.Div:
        """創建標題欄"""
        return html.Div(
            [
                dbc.Navbar(
                    dbc.Container(
                        [
                            dbc.NavbarBrand("Lo2cin4BT 可視化平台", className="ms-2"),
                            dbc.Nav(
                                [
                                    # 頁面切換按鈕
                                    dbc.NavItem(
                                        dbc.Button(
                                            "📊 資產曲線組合圖",
                                            id="btn-asset-curve",
                                            color="success",
                                            className="me-2",
                                            n_clicks=0,
                                        )
                                    ),
                                    dbc.NavItem(
                                        dbc.Button(
                                            "🏔️ 參數高原",
                                            id="btn-parameter-landscape",
                                            color="info",
                                            className="me-2",
                                            n_clicks=0,
                                        )
                                    ),
                                    dbc.NavItem(
                                        dbc.NavLink(
                                            "lo2cin4官網",
                                            href="https://lo2cin4.com",
                                            target="_blank",
                                            style={
                                                "color": "#ecbc4f",
                                                "fontWeight": "bold",
                                            },
                                        )
                                    ),
                                    dbc.NavItem(
                                        dbc.NavLink(
                                            "lo2cin4終身會籍詳情",
                                            href="https://lo2cin4.com/membership/",
                                            target="_blank",
                                            style={
                                                "color": "#ecbc4f",
                                                "fontWeight": "bold",
                                            },
                                        )
                                    ),
                                    dbc.NavItem(
                                        dbc.NavLink(
                                            "其他社群連結",
                                            href="https://linktr.ee/lo2cin4",
                                            target="_blank",
                                            style={
                                                "color": "#ecbc4f",
                                                "fontWeight": "bold",
                                            },
                                        )
                                    ),
                                ],
                                className="ms-auto",
                                style={"gap": "1.2rem"},
                            ),
                        ]
                    ),
                    color="primary",
                    dark=True,
                    className="mb-4",
                )
            ]
        )

    def _create_control_panel(self, indicator_param_structure: dict) -> html.Div:
        """
        創建控制面板（新版：indicator_type toggle group + 動態 collapsible checklist）
        Args:
            indicator_param_structure: {'entry': {type: {param: [值]}}, 'exit': ...}
        Returns:
            html.Div: 控制面板組件
        """
        try:
            entry_types = list(indicator_param_structure["entry"].keys())
            exit_types = list(indicator_param_structure["exit"].keys())
            for t in entry_types:
                pass
            for t in exit_types:
                pass
            # 預設全選
            entry_type_default = entry_types
            exit_type_default = exit_types
            entry_type_toggle = html.Div(
                [
                    html.Label("入場指標類型", className="form-label"),
                    dcc.Checklist(
                        id="entry_indicator_type_toggle",
                        options=[{"label": t, "value": t} for t in entry_types],
                        value=entry_type_default,
                        inputStyle={"margin-right": "8px"},
                        className="mb-2",
                        inline=True,
                    ),
                ],
                className="mb-2",
            )
            exit_type_toggle = html.Div(
                [
                    html.Label("出場指標類型", className="form-label"),
                    dcc.Checklist(
                        id="exit_indicator_type_toggle",
                        options=[{"label": t, "value": t} for t in exit_types],
                        value=exit_type_default,
                        inputStyle={"margin-right": "8px"},
                        className="mb-2",
                        inline=True,
                    ),
                ],
                className="mb-2",
            )

            def make_param_block(type_name, param_dict, prefix):
                # ⚠️ 此區塊為動態產生 collapsible checklist，支援多指標自動擴充，勿隨意更動！
                # 只要 param_dict 結構正確，未來新增指標會自動支援
                param_blocks = []
                for param, values in param_dict.items():
                    param_blocks.append(
                        html.Div(
                            [
                                html.Label(param, className="form-label"),
                                dcc.Checklist(
                                    id={
                                        "type": f"{prefix}_param_checklist",
                                        "indicator": type_name,
                                        "param": param,
                                    },
                                    options=[{"label": v, "value": v} for v in values],
                                    value=values,  # 預設全選
                                    className="mb-2",
                                    inline=True,
                                ),
                            ]
                        )
                    )
                # 標題列右側小型切換按鈕
                header = html.Div(
                    [
                        html.Span(
                            type_name, style={"fontWeight": "bold", "fontSize": "16px"}
                        ),
                        dbc.Button(
                            "全選",  # 動態文字，由callback控制
                            id={
                                "type": f"{prefix}_param_toggle_all",
                                "indicator": type_name,
                            },
                            color="secondary",
                            size="sm",
                            outline=True,
                            style={"float": "right", "marginLeft": "8px"},
                        ),
                    ],
                    style={
                        "display": "flex",
                        "alignItems": "center",
                        "justifyContent": "space-between",
                    },
                )
                return html.Div(
                    [
                        header,
                        html.Div(param_blocks, className="ms-3"),
                    ],
                    className="mb-2",
                )

            # 動態產生 collapsible，只對 toggle group 勾選的 type 展開
            entry_param_blocks = [
                dbc.Collapse(
                    make_param_block(t, indicator_param_structure["entry"][t], "entry"),
                    id={"type": "entry_param_collapse", "indicator": t},
                    is_open=True,  # callback 控制展開/收合
                )
                for t in entry_types
            ]
            exit_param_blocks = [
                dbc.Collapse(
                    make_param_block(t, indicator_param_structure["exit"][t], "exit"),
                    id={"type": "exit_param_collapse", "indicator": t},
                    is_open=True,  # callback 控制展開/收合
                )
                for t in exit_types
            ]
            control_panel = html.Div(
                [
                    html.H5("控制面板", className="mb-3"),
                    # Sorting 區塊（移到標題下方）
                    html.Div(
                        [
                            html.Label(
                                "Sorting",
                                style={"color": "#ecbc4f", "fontWeight": "bold"},
                            ),
                            dcc.Dropdown(
                                id="sorting_select",
                                options=[
                                    {
                                        "label": "Top 20 Return",
                                        "value": "Top20_Total_return",
                                    },
                                    {
                                        "label": "Top 20 least Max_drawdown",
                                        "value": "Top20_least_Max_drawdown",
                                    },
                                    {
                                        "label": "Top 20 Recovery Factor",
                                        "value": "Top20_Recovery_factor",
                                    },
                                    {"label": "Top 20 Sharpe", "value": "Top20_Sharpe"},
                                    {
                                        "label": "Top 20 Sortino",
                                        "value": "Top20_Sortino",
                                    },
                                    {"label": "Top 20 Calmar", "value": "Top20_Calmar"},
                                    {
                                        "label": "Top 20 Information Ratio",
                                        "value": "Top20_Information_ratio",
                                    },
                                ],
                                placeholder="選擇排序方式",
                                style={
                                    "width": "100%",
                                    "background": "#181818",
                                    "color": "#ecbc4f",
                                    "border": "1.5px solid #8f1511",
                                },
                            ),
                        ],
                        style={"marginBottom": "16px"},
                    ),
                    entry_type_toggle,
                    html.Div(entry_param_blocks, id="entry_param_blocks"),
                    html.Hr(),
                    exit_type_toggle,
                    html.Div(exit_param_blocks, id="exit_param_blocks"),
                ],
                className="p-3 border rounded",
            )
            return control_panel
        except Exception as e:
            import traceback

            tb = traceback.format_exc()
            self.logger.error(f"創建控制面板失敗: {e}\n{tb}")
            return html.Div("控制面板創建失敗")

    def _create_asset_curve_content(self, data: Dict[str, Any]) -> html.Div:
        """
        創建資產曲線頁面內容

        Args:
            data: 解析後的數據字典

        Returns:
            html.Div: 資產曲線頁面組件
        """
        try:
            return html.Div(
                [
                    html.H5("📊 資產曲線組合圖", className="mb-3"),
                    dcc.Graph(id="equity_chart", style={"height": "1000px"}),
                    html.H5("績效指標", className="mb-3"),
                    html.Div(id="selected_details"),
                ]
            )

        except Exception as e:
            self.logger.error(f"創建資產曲線內容失敗: {e}")
            return html.Div("資產曲線內容創建失敗")

    def _create_parameter_landscape_content(self, data: Dict[str, Any]) -> html.Div:
        """
        創建參數高原頁面內容

        Args:
            data: 解析後的數據字典

        Returns:
            html.Div: 參數高原頁面組件
        """
        try:
            from .ParameterPlateau_plotter import ParameterPlateauPlotter

            plateau_plotter = ParameterPlateauPlotter()
            return plateau_plotter.create_parameter_landscape_layout(data)

        except Exception as e:
            self.logger.error(f"創建參數高原內容失敗: {e}")
            return html.Div("參數高原內容創建失敗")

    def create_equity_chart(
        self,
        equity_data: Dict[str, Any],
        bah_data: Dict[str, Any],
        selected_params: List[str],
    ) -> dict:
        """
        創建權益曲線圖表（使用ChartComponents）

        Args:
            equity_data: 權益曲線數據
            bah_data: BAH權益曲線數據
            selected_params: 選中的參數組合

        Returns:
            dict: Plotly 圖表配置
        """
        try:
            from .ChartComponents_plotter import ChartComponents

            # 使用ChartComponents創建圖表
            chart_components = ChartComponents(self.logger)
            return chart_components.create_equity_chart(
                equity_data=equity_data,
                selected_params=selected_params,
                bah_data=bah_data,
            )

        except Exception as e:
            self.logger.error(f"創建權益曲線圖表失敗: {e}")
            return {}

    def create_metrics_table(
        self, metrics_data: Dict[str, Any], selected_params: List[str]
    ) -> html.Div:
        """
        創建績效指標表格

        Args:
            metrics_data: 績效指標數據
            selected_params: 選中的參數組合

        Returns:
            html.Div: 表格組件
        """
        try:
            if not selected_params:
                return html.Div("請選擇參數組合")

            # 創建表格數據
            table_data = []
            for param_key in selected_params:
                if param_key in metrics_data:
                    metrics = metrics_data[param_key]
                    row = {"參數組合": param_key}
                    row.update(metrics)
                    table_data.append(row)

            if not table_data:
                return html.Div("無績效指標數據")

            # 創建表格
            table = dbc.Table.from_dataframe(
                pd.DataFrame(table_data),
                striped=True,
                bordered=True,
                hover=True,
                responsive=True,
                className="table-sm",
            )

            return table

        except Exception as e:
            self.logger.error(f"創建績效指標表格失敗: {e}")
            return html.Div("表格創建失敗")

    def create_selected_details(
        self, data: Dict[str, Any], selected_param: str
    ) -> html.Div:
        """
        創建選中策略詳情

        Args:
            data: 數據字典
            selected_param: 選中的參數組合

        Returns:
            html.Div: 詳情組件
        """
        try:
            if not selected_param or selected_param not in data.get("parameters", {}):
                return html.Div("請選擇策略")

            param_data = data["parameters"][selected_param]
            metrics = data.get("metrics", {}).get(selected_param, {})

            # 創建詳情卡片
            details = html.Div(
                [
                    dbc.Card(
                        [
                            dbc.CardHeader("策略詳情"),
                            dbc.CardBody(
                                [
                                    html.H6("參數信息"),
                                    html.Pre(
                                        json.dumps(
                                            param_data, indent=2, ensure_ascii=False
                                        )
                                    ),
                                    html.Hr(),
                                    html.H6("績效指標"),
                                    html.Pre(
                                        json.dumps(
                                            metrics, indent=2, ensure_ascii=False
                                        )
                                    ),
                                ]
                            ),
                        ]
                    )
                ]
            )

            return details

        except Exception as e:
            self.logger.error(f"創建選中詳情失敗: {e}")
            return html.Div("詳情創建失敗")
