"""
CallbackHandler_plotter.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 可視化平台的回調處理核心模組，負責處理 Dash 應用的回調函數，包括參數篩選、圖表更新、數據過濾等互動功能。

【流程與數據流】
------------------------------------------------------------
- 主流程：接收用戶輸入 → 處理數據 → 更新組件 → 返回結果
- 各模組間數據流明確，流程如下：

```mermaid
flowchart TD
    A[用戶交互] -->|觸發| B[回調函數]
    B -->|處理| C[數據過濾]
    C -->|生成| D[更新組件]
    D -->|返回| E[界面更新]
    E -->|顯示| F[用戶界面]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增回調函數、組件ID時，請同步更新頂部註解與對應模組
- 若組件結構有變動，需同步更新 DashboardGenerator 的組件創建
- 新增/修改回調函數、組件ID時，務必同步更新本檔案與所有依賴模組
- 回調函數的輸入輸出組件ID需要特別注意一致性

【常見易錯點】
------------------------------------------------------------
- 回調函數命名衝突
- 組件ID不匹配
- 數據類型錯誤
- 回調依賴關係錯誤

【錯誤處理】
------------------------------------------------------------
- 回調函數錯誤時提供詳細錯誤訊息
- 組件ID不匹配時提供診斷建議
- 數據處理失敗時提供備用方案

【範例】
------------------------------------------------------------
- 基本使用：handler = CallbackHandler()
- 設置回調：handler.setup_callbacks(app, data)

【與其他模組的關聯】
------------------------------------------------------------
- 被 BasePlotter 調用
- 依賴 DashboardGenerator 的組件ID
- 處理用戶交互並更新界面

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，支援基本回調處理
- v1.1: 新增動態回調支援
- v1.2: 新增多指標自動擴充回調

【參考】
------------------------------------------------------------
- 詳細流程規範如有變動，請同步更新本註解與 README
- 其他模組如有依賴本檔案的行為，請於對應模組頂部註解標明
- Dash 回調函數設計請參考 Dash 官方文檔
"""

import logging
from typing import Any, Dict, Optional

from dash import ALL, Input, Output, State
from dash.exceptions import PreventUpdate


class CallbackHandler:
    """
    Dash 回調處理器

    負責處理 Dash 應用的回調函數，
    包括參數篩選、圖表更新、數據過濾等互動功能。
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        初始化回調處理器

        Args:
            logger: 日誌記錄器，預設為 None
        """
        self.logger = logger or logging.getLogger(__name__)
        self.data = None

    def setup_callbacks(self, app, data: Dict[str, Any]):
        # 頁面切換回調函數
        @app.callback(
            [
                Output("layout-asset-curve-with-panel", "style"),
                Output("layout-parameter-landscape-full", "style"),
            ],
            [
                Input("btn-asset-curve", "n_clicks"),
                Input("btn-parameter-landscape", "n_clicks"),
            ],
            prevent_initial_call=False,
        )
        def switch_page(asset_curve_clicks, parameter_landscape_clicks):
            """切換頁面顯示"""
            if not ctx.triggered_id:
                # 初始狀態：顯示資產曲線頁面
                return {"display": "block"}, {"display": "none"}

            if ctx.triggered_id == "btn-asset-curve":
                return {"display": "block"}, {"display": "none"}
            elif ctx.triggered_id == "btn-parameter-landscape":
                return {"display": "none"}, {"display": "block"}

            # 預設顯示資產曲線頁面
            return {"display": "block"}, {"display": "none"}

        # ⚠️ 以下 callback 為動態展開/收合 collapsible，支援多指標自動擴充，勿隨意更動！
        @app.callback(
            [
                Output({"type": "entry_param_collapse", "indicator": ALL}, "is_open"),
                Output({"type": "exit_param_collapse", "indicator": ALL}, "is_open"),
            ],
            [
                Input("entry_indicator_type_toggle", "value"),
                Input("exit_indicator_type_toggle", "value"),
            ],
            [
                State({"type": "entry_param_collapse", "indicator": ALL}, "id"),
                State({"type": "exit_param_collapse", "indicator": ALL}, "id"),
            ],
        )
        def toggle_collapse(entry_types, exit_types, entry_ids, exit_ids):
            entry_open = [id["indicator"] in entry_types for id in entry_ids]
            exit_open = [id["indicator"] in exit_types for id in exit_ids]
            return entry_open, exit_open

        # ⚠️ 切換按鈕 callback 在全選和清空之間切換，只影響對應 indicator checklist，其他保持原狀。
        @app.callback(
            [
                Output(
                    {"type": "entry_param_checklist", "indicator": ALL, "param": ALL},
                    "value",
                ),
                Output(
                    {"type": "entry_param_toggle_all", "indicator": ALL}, "children"
                ),
            ],
            Input({"type": "entry_param_toggle_all", "indicator": ALL}, "n_clicks"),
            State(
                {"type": "entry_param_checklist", "indicator": ALL, "param": ALL}, "id"
            ),
            State(
                {"type": "entry_param_checklist", "indicator": ALL, "param": ALL},
                "options",
            ),
            State(
                {"type": "entry_param_checklist", "indicator": ALL, "param": ALL},
                "value",
            ),
            prevent_initial_call=True,
        )
        def entry_toggle_all(n_clicks, ids, options, current_values):
            triggered = ctx.triggered_id
            if not triggered:
                raise PreventUpdate
            indicator = triggered["indicator"]

            # 動態處理：為每個參數設置值
            values = []
            for id_, opts, cur in zip(ids, options, current_values):
                if id_["indicator"] == indicator:
                    all_vals = [o["value"] for o in opts]
                    # 檢查是否已經全選
                    is_all_selected = (
                        cur
                        and len(cur) == len(all_vals)
                        and all(v in cur for v in all_vals)
                    )
                    if is_all_selected:
                        # 如果全選，則清空
                        values.append([])
                    else:
                        # 如果不是全選，則全選
                        values.append(all_vals)
                else:
                    values.append(cur)

            # 動態處理：為每個indicator按鈕設置文字
            # 獲取所有唯一的indicator類型
            unique_indicators = set(id_["indicator"] for id_ in ids)
            button_texts = []

            for unique_indicator in unique_indicators:
                if unique_indicator == indicator:
                    # 檢查觸發的indicator是否全選
                    indicator_values = []
                    for id_, opts, cur in zip(ids, options, current_values):
                        if id_["indicator"] == unique_indicator:
                            indicator_values.extend(cur if cur else [])

                    # 獲取該indicator的所有選項
                    all_options = []
                    for id_, opts, cur in zip(ids, options, current_values):
                        if id_["indicator"] == unique_indicator:
                            all_options.extend([o["value"] for o in opts])

                    # 檢查是否全選
                    is_all_selected = len(indicator_values) == len(all_options) and all(
                        v in indicator_values for v in all_options
                    )
                    button_texts.append("反選" if is_all_selected else "全選")
                else:
                    button_texts.append("全選")

            return values, button_texts

        @app.callback(
            [
                Output(
                    {"type": "exit_param_checklist", "indicator": ALL, "param": ALL},
                    "value",
                ),
                Output({"type": "exit_param_toggle_all", "indicator": ALL}, "children"),
            ],
            Input({"type": "exit_param_toggle_all", "indicator": ALL}, "n_clicks"),
            State(
                {"type": "exit_param_checklist", "indicator": ALL, "param": ALL}, "id"
            ),
            State(
                {"type": "exit_param_checklist", "indicator": ALL, "param": ALL},
                "options",
            ),
            State(
                {"type": "exit_param_checklist", "indicator": ALL, "param": ALL},
                "value",
            ),
            prevent_initial_call=True,
        )
        def exit_toggle_all(n_clicks, ids, options, current_values):
            triggered = ctx.triggered_id
            if not triggered:
                raise PreventUpdate
            indicator = triggered["indicator"]

            # 動態處理：為每個參數設置值
            values = []
            for id_, opts, cur in zip(ids, options, current_values):
                if id_["indicator"] == indicator:
                    all_vals = [o["value"] for o in opts]
                    # 檢查是否已經全選
                    is_all_selected = (
                        cur
                        and len(cur) == len(all_vals)
                        and all(v in cur for v in all_vals)
                    )
                    if is_all_selected:
                        # 如果全選，則清空
                        values.append([])
                    else:
                        # 如果不是全選，則全選
                        values.append(all_vals)
                else:
                    values.append(cur)

            # 動態處理：為每個indicator按鈕設置文字
            # 獲取所有唯一的indicator類型
            unique_indicators = set(id_["indicator"] for id_ in ids)
            button_texts = []

            for unique_indicator in unique_indicators:
                if unique_indicator == indicator:
                    # 檢查觸發的indicator是否全選
                    indicator_values = []
                    for id_, opts, cur in zip(ids, options, current_values):
                        if id_["indicator"] == unique_indicator:
                            indicator_values.extend(cur if cur else [])

                    # 獲取該indicator的所有選項
                    all_options = []
                    for id_, opts, cur in zip(ids, options, current_values):
                        if id_["indicator"] == unique_indicator:
                            all_options.extend([o["value"] for o in opts])

                    # 檢查是否全選
                    is_all_selected = len(indicator_values) == len(all_options) and all(
                        v in indicator_values for v in all_options
                    )
                    button_texts.append("反選" if is_all_selected else "全選")
                else:
                    button_texts.append("全選")

            return values, button_texts

        # === 功能性主 callback ===
        @app.callback(
            Output("equity_chart", "figure"),
            Input("entry_indicator_type_toggle", "value"),
            Input("exit_indicator_type_toggle", "value"),
            Input(
                {"type": "entry_param_checklist", "indicator": ALL, "param": ALL},
                "value",
            ),
            Input(
                {"type": "exit_param_checklist", "indicator": ALL, "param": ALL},
                "value",
            ),
            Input(
                {"type": "entry_param_checklist", "indicator": ALL, "param": ALL}, "id"
            ),
            Input(
                {"type": "exit_param_checklist", "indicator": ALL, "param": ALL}, "id"
            ),
            Input(
                {"type": "entry_param_checklist", "indicator": ALL, "param": ALL},
                "options",
            ),
            Input(
                {"type": "exit_param_checklist", "indicator": ALL, "param": ALL},
                "options",
            ),
            Input("sorting_select", "value"),
        )
        def update_equity_chart(
            entry_types,
            exit_types,
            entry_vals,
            exit_vals,
            entry_ids,
            exit_ids,
            entry_opts,
            exit_opts,
            sorting_value,
        ):
            parameters = data.get("parameters", [])
            equity_curves = data.get("equity_curves", {})
            bah_curves = data.get(
                "bah_curves", {}
            )  # <--- 新增這行，確保 BAH 曲線來源正確
            # checklist value 為空時自動補全為 options 全部 value
            entry_param_map = {}
            for val, id_, opts in zip(entry_vals, entry_ids, entry_opts):
                if id_["indicator"] in entry_types:
                    v = val if val else [o["value"] for o in opts]
                    entry_param_map.setdefault(id_["indicator"], {})[id_["param"]] = v
            exit_param_map = {}
            for val, id_, opts in zip(exit_vals, exit_ids, exit_opts):
                if id_["indicator"] in exit_types:
                    v = val if val else [o["value"] for o in opts]
                    exit_param_map.setdefault(id_["indicator"], {})[id_["param"]] = v
            backtest_ids = data.get("backtest_ids", [])
            metrics = data.get("metrics", {})
            filtered_ids = []
            for i, param in enumerate(parameters):
                bid = param.get(
                    "Backtest_id", backtest_ids[i] if i < len(backtest_ids) else str(i)
                )
                entry_ok = False
                exit_ok = False

                # 檢查入場指標 - 所有入場指標都必須匹配
                entry_indicators_match = True
                for d in param.get("Entry_params", []):
                    indicator = str(d.get("indicator_type"))
                    
                    # 首先檢查指標類型是否在用戶勾選範圍內
                    if indicator not in entry_types:
                        entry_indicators_match = False
                        break
                    
                    # 然後檢查該指標的所有參數是否匹配
                    param_match = True
                    for k, v in d.items():
                        if k == "indicator_type":
                            continue
                        # 如果該參數有用戶勾選的值，則必須匹配
                        if k in entry_param_map.get(indicator, {}):
                            checklist_vals = entry_param_map[indicator][k]
                            if str(v) not in [str(x) for x in checklist_vals]:
                                param_match = False
                                break
                    # 如果該指標不匹配，則整個入場不匹配
                    if not param_match:
                        entry_indicators_match = False
                        break
                entry_ok = entry_indicators_match

                # 檢查出場指標 - 所有出場指標都必須匹配
                exit_indicators_match = True
                for d in param.get("Exit_params", []):
                    indicator = str(d.get("indicator_type"))
                    
                    # 首先檢查指標類型是否在用戶勾選範圍內
                    if indicator not in exit_types:
                        exit_indicators_match = False
                        break
                    
                    # 然後檢查該指標的所有參數是否匹配
                    param_match = True
                    for k, v in d.items():
                        if k == "indicator_type":
                            continue
                        # 如果該參數有用戶勾選的值，則必須匹配
                        if k in exit_param_map.get(indicator, {}):
                            checklist_vals = exit_param_map[indicator][k]
                            if str(v) not in [str(x) for x in checklist_vals]:
                                param_match = False
                                break
                    # 如果該指標不匹配，則整個出場不匹配
                    if not param_match:
                        exit_indicators_match = False
                        break
                exit_ok = exit_indicators_match

                if entry_ok and exit_ok:
                    filtered_ids.append(bid)
            # 只根據 sorting_value 排序，取前 20
            sort_map = {
                "Top20_Total_return": ("Total_return", True),
                "Top20_least_Max_drawdown": ("Max_drawdown", True),
                "Top20_Recovery_factor": ("Recovery_factor", True),
                "Top20_Sharpe": ("Sharpe", True),
                "Top20_Sortino": ("Sortino", True),
                "Top20_Calmar": ("Calmar", True),
                "Top20_Information_ratio": ("Information_ratio", True),
            }
            if sorting_value in sort_map:
                sort_field, descending = sort_map[sorting_value]
                filtered_ids = sorted(
                    filtered_ids,
                    key=lambda bid: float(
                        metrics.get(bid, {}).get(
                            sort_field, float("-inf" if descending else "inf")
                        )
                    ),
                    reverse=descending,
                )[:20]
            # 🚀 使用ChartComponents的優化函數
            from .ChartComponents_plotter import ChartComponents

            chart_components = ChartComponents()
            return chart_components.create_equity_chart_for_callback(
                equity_curves=equity_curves,
                bah_curves=bah_curves,
                filtered_ids=filtered_ids,
                parameters=parameters,
            )

        # === 選中策略詳情展示 callback ===
        @app.callback(
            Output("selected_details", "children"),
            Input("equity_chart", "clickData"),
            State("entry_indicator_type_toggle", "value"),
            State("exit_indicator_type_toggle", "value"),
            State(
                {"type": "entry_param_checklist", "indicator": ALL, "param": ALL},
                "value",
            ),
            State(
                {"type": "exit_param_checklist", "indicator": ALL, "param": ALL},
                "value",
            ),
            State(
                {"type": "entry_param_checklist", "indicator": ALL, "param": ALL}, "id"
            ),
            State(
                {"type": "exit_param_checklist", "indicator": ALL, "param": ALL}, "id"
            ),
            State(
                {"type": "entry_param_checklist", "indicator": ALL, "param": ALL},
                "options",
            ),
            State(
                {"type": "exit_param_checklist", "indicator": ALL, "param": ALL},
                "options",
            ),
        )
        def show_selected_details(
            clickData,
            entry_types,
            exit_types,
            entry_vals,
            exit_vals,
            entry_ids,
            exit_ids,
            entry_opts,
            exit_opts,
        ):
            # 找出目前 filtered_ids
            parameters = data.get("parameters", [])
            backtest_ids = data.get("backtest_ids", [])
            metrics = data.get("metrics", {})
            # checklist value 為空時自動補全為 options 全部 value
            entry_param_map = {}
            for val, id_, opts in zip(entry_vals, entry_ids, entry_opts):
                if id_["indicator"] in entry_types:
                    v = val if val else [o["value"] for o in opts]
                    entry_param_map.setdefault(id_["indicator"], {})[id_["param"]] = v
            exit_param_map = {}
            for val, id_, opts in zip(exit_vals, exit_ids, exit_opts):
                if id_["indicator"] in exit_types:
                    v = val if val else [o["value"] for o in opts]
                    exit_param_map.setdefault(id_["indicator"], {})[id_["param"]] = v
            filtered_ids = []
            for i, param in enumerate(parameters):
                bid = param.get(
                    "Backtest_id", backtest_ids[i] if i < len(backtest_ids) else str(i)
                )
                entry_ok = False
                exit_ok = False
                for d in param.get("Entry_params", []):
                    if str(d.get("indicator_type")) in entry_types:
                        indicator = str(d.get("indicator_type"))
                        entry_ok = True
                        for k, v in d.items():
                            if k == "indicator_type":
                                continue
                            if k in entry_param_map[indicator]:
                                checklist_vals = entry_param_map[indicator][k]
                                if str(v) not in [str(x) for x in checklist_vals]:
                                    entry_ok = False
                                    break
                        break
                for d in param.get("Exit_params", []):
                    if str(d.get("indicator_type")) in exit_types:
                        indicator = str(d.get("indicator_type"))
                        exit_ok = True
                        for k, v in d.items():
                            if k == "indicator_type":
                                continue
                            if k in exit_param_map[indicator]:
                                checklist_vals = exit_param_map[indicator][k]
                                if str(v) not in [str(x) for x in checklist_vals]:
                                    exit_ok = False
                                    break
                        break
                if entry_ok and exit_ok:
                    filtered_ids.append(bid)
            # 根據 clickData 找出選中的 Backtest_id
            sel_bid = None
            if clickData and "points" in clickData and len(clickData["points"]) > 0:
                sel_bid = clickData["points"][0].get("customdata")
                if sel_bid not in metrics:
                    sel_bid = None
            if not sel_bid:
                return "請點選資金曲線以顯示詳情"
            meta = metrics.get(sel_bid, {})
            # Details 欄位
            details_fields = [
                "Backtest_id",
                "Frequency",
                "Asset",
                "Strategy",
                "Predictor",
                "Entry_params",
                "Exit_params",
                "Transaction_cost",
                "Slippage_cost",
                "Trade_delay",
                "Trade_price",
                "Data_start_time",
                "Data_end_time",
            ]
            details_table = [
                html.Tr([html.Th(f), html.Td(str(meta.get(f, "")))])
                for f in details_fields
            ]

            # Performance 欄位
            def fmt3(x, field=None):
                try:
                    if field in ["Trade_count", "Max_consecutive_losses"]:
                        return str(int(float(x)))
                    f = float(x)
                    return f"{f:.3f}"
                except Exception:
                    return str(x)

            perf_fields = [
                "Total_return",
                "Annualized_return (CAGR)",
                "Std",
                "Annualized_std",
                "Downside_risk",
                "Annualized_downside_risk",
                "Max_drawdown",
                "Average_drawdown",
                "Recovery_factor",
                "Sharpe",
                "Sortino",
                "Calmar",
                "Information_ratio",
                "Alpha",
                "Beta",
                "Trade_count",
                "Win_rate",
                "Profit_factor",
                "Avg_trade_return",
                "Max_consecutive_losses",
                "Exposure_time",
                "Max_holding_period_ratio",
            ]
            bah_fields = [
                "BAH_Total_return",
                "BAH_Annualized_return (CAGR)",
                "BAH_Std",
                "BAH_Annualized_std",
                "BAH_Downside_risk",
                "BAH_Annualized_downside_risk",
                "BAH_Max_drawdown",
                "BAH_Average_drawdown",
                "BAH_Recovery_factor",
                "BAH_Sharpe",
                "BAH_Sortino",
                "BAH_Calmar",
            ]
            perf_table = [
                html.Tr([html.Th(f), html.Td(fmt3(meta.get(f, ""), f))])
                for f in perf_fields
            ]
            bah_table = [
                html.Tr([html.Th(f), html.Td(fmt3(meta.get(f, ""), f))])
                for f in bah_fields
            ]
            return html.Div(
                [
                    html.H5("Details"),
                    html.Table(
                        details_table,
                        className="table table-sm table-bordered details-table",
                    ),
                    html.H5("Performance"),
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Table(
                                        perf_table,
                                        className="table table-sm table-bordered performance-table",
                                    )
                                ],
                                style={
                                    "width": "48%",
                                    "display": "inline-block",
                                    "verticalAlign": "top",
                                },
                            ),
                            html.Div(
                                [
                                    html.Table(
                                        bah_table,
                                        className="table table-sm table-bordered performance-table",
                                    )
                                ],
                                style={
                                    "width": "48%",
                                    "display": "inline-block",
                                    "verticalAlign": "top",
                                    "marginLeft": "4%",
                                },
                            ),
                        ]
                    ),
                ]
            )

        from dash import ctx, html

        @app.callback(
            [Output("filter_list_store", "data"), Output("active_filters", "children")],
            [
                Input("filter_apply_btn", "n_clicks"),
                Input({"type": "remove_filter_btn", "index": ALL}, "n_clicks"),
            ],
            [
                State("filter_metric", "value"),
                State("filter_op", "value"),
                State("filter_value", "value"),
                State("filter_list_store", "data"),
            ],
            prevent_initial_call=True,
        )
        def update_filter_list(
            apply_click, remove_clicks, metric, op, value, filter_list
        ):
            triggered = ctx.triggered_id
            # 新增 filter
            if triggered == "filter_apply_btn" and metric and op and value is not None:
                filter_list = filter_list or []
                filter_list.append({"metric": metric, "op": op, "value": value})
            # 移除 filter
            elif (
                isinstance(triggered, dict)
                and triggered.get("type") == "remove_filter_btn"
            ):
                idx = triggered.get("index")
                if filter_list and 0 <= idx < len(filter_list):
                    filter_list.pop(idx)
            # 生成顯示
            children = []
            for i, f in enumerate(filter_list):
                label = f"{f['metric']} {f['op']} {f['value']}"
                children.append(
                    html.Div(
                        [
                            html.Span(
                                label, style={"color": "#ecbc4f", "marginRight": "6px"}
                            ),
                            html.Button(
                                "x",
                                id={"type": "remove_filter_btn", "index": i},
                                n_clicks=0,
                                style={
                                    "background": "#8f1511",
                                    "color": "#fff",
                                    "border": "none",
                                    "borderRadius": "4px",
                                    "padding": "0 6px",
                                    "cursor": "pointer",
                                },
                            ),
                        ],
                        style={
                            "background": "#232323",
                            "border": "1.5px solid #8f1511",
                            "borderRadius": "4px",
                            "padding": "2px 8px",
                            "display": "flex",
                            "alignItems": "center",
                        },
                    )
                )
            return filter_list, children
