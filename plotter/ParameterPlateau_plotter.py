"""
參數高原分析模組
獨立於資金曲線組合圖功能，專門處理參數高原分析
"""

import logging
from typing import Any, Dict, List

import dash
import dash_bootstrap_components as dbc
import numpy as np
import plotly.graph_objs as go
from dash import ALL, Input, Output, State
from dash import callback_context as ctx
from dash import dcc, html
from dash.exceptions import PreventUpdate


class ParameterIndexManager:
    """參數索引管理器 - 管理分層數據索引"""

    def __init__(self, parameters_data):
        self.parameters_data = parameters_data
        self.strategy_indexes = {}
        self.parameter_combinations = {}
        self.logger = logging.getLogger(__name__)

    def build_indexes(self):
        """建立分層數據索引"""
        try:

            self.logger.info("開始建立參數索引...")

            # 按策略分組參數
            for i, param in enumerate(self.parameters_data):
                strategy_key = self._extract_strategy_key(param)
                if strategy_key not in self.strategy_indexes:
                    self.strategy_indexes[strategy_key] = []
                self.strategy_indexes[strategy_key].append(i)

            # 為每個策略建立參數組合索引
            for strategy_key, indices in self.strategy_indexes.items():
                self._build_strategy_parameter_indexes(strategy_key, indices)
            self.logger.info(
                f"參數索引建立完成，共 {len(self.strategy_indexes)} 個策略"
            )
            return True

        except Exception as e:
            self.logger.error(f"建立參數索引失敗: {e}")
            return False

    def _extract_strategy_key(self, param):
        """提取策略鍵值"""
        try:
            entry_names = []
            exit_names = []

            if "Entry_params" in param:
                for entry_param in param["Entry_params"]:
                    indicator_type = entry_param.get("indicator_type", "")
                    strat_idx = entry_param.get("strat_idx", "")
                    entry_names.append(f"{indicator_type}{strat_idx}")

            if "Exit_params" in param:
                for exit_param in param["Exit_params"]:
                    indicator_type = exit_param.get("indicator_type", "")
                    strat_idx = exit_param.get("strat_idx", "")
                    exit_names.append(f"{indicator_type}{strat_idx}")

            strategy_key = f"Entry:{','.join(sorted(entry_names))}|Exit:{','.join(sorted(exit_names))}"
            return strategy_key

        except Exception as e:
            self.logger.error(f"提取策略鍵值失敗: {e}")
            return "unknown"

    def _build_strategy_parameter_indexes(self, strategy_key, indices):
        """為特定策略建立參數組合索引"""
        try:
            if strategy_key not in self.parameter_combinations:
                self.parameter_combinations[strategy_key] = {}

            # 分析該策略的所有參數組合
            for idx in indices:
                param = self.parameters_data[idx]
                param_hash = self._create_parameter_hash(param)
                self.parameter_combinations[strategy_key][param_hash] = idx

            self.logger.info(
                f"策略 {strategy_key} 的參數索引建立完成，共 {len(self.parameter_combinations[strategy_key])} 個組合"
            )

        except Exception as e:
            self.logger.error(f"建立策略參數索引失敗 {strategy_key}: {e}")

    def _create_parameter_hash(self, param):
        """創建參數組合的哈希值"""
        try:
            hash_parts = []

            # Entry參數
            if "Entry_params" in param:
                for entry_param in param["Entry_params"]:
                    for key, value in entry_param.items():
                        if key not in ["indicator_type", "strat_idx"]:
                            hash_parts.append(f"Entry_{key}_{value}")

            # Exit參數
            if "Exit_params" in param:
                for exit_param in param["Exit_params"]:
                    for key, value in exit_param.items():
                        if key not in ["indicator_type", "strat_idx"]:
                            hash_parts.append(f"Exit_{key}_{value}")

            return "|".join(sorted(hash_parts))

        except Exception as e:
            self.logger.error(f"創建參數哈希失敗: {e}")
            return "unknown"

    def find_data_subset(self, strategy_key, fixed_params):
        """根據固定參數找到數據子集"""
        try:
            if strategy_key not in self.parameter_combinations:
                return []

            # 構建固定參數的查詢條件
            query_hash = self._create_fixed_params_hash(fixed_params)

            # 查找匹配的參數組合
            matching_indices = []
            for param_hash, idx in self.parameter_combinations[strategy_key].items():
                if self._matches_fixed_params(param_hash, query_hash):
                    matching_indices.append(idx)

            return matching_indices

        except Exception as e:
            self.logger.error(f"查找數據子集失敗: {e}")
            return []

    def _create_fixed_params_hash(self, fixed_params):
        """創建固定參數的查詢哈希"""
        try:
            hash_parts = []
            for param_name, value in fixed_params.items():
                hash_parts.append(f"{param_name}_{value}")
            return "|".join(sorted(hash_parts))

        except Exception as e:
            self.logger.error(f"創建固定參數哈希失敗: {e}")
            return ""

    def _matches_fixed_params(self, param_hash, query_hash):
        """檢查參數哈希是否匹配固定參數查詢"""
        try:
            if not query_hash:
                return True

            query_parts = set(query_hash.split("|"))
            param_parts = set(param_hash.split("|"))

            # 檢查所有查詢條件是否都滿足
            return query_parts.issubset(param_parts)

        except Exception as e:
            self.logger.error(f"參數匹配檢查失敗: {e}")
            return False

    def get_variable_params(self, strategy_key, fixed_params):
        """獲取可變參數列表"""
        try:
            # 找到符合固定參數條件的數據子集
            data_indices = self.find_data_subset(strategy_key, fixed_params)
            if not data_indices:
                return {}

            # 分析可變參數
            variable_params = {}
            sample_param = self.parameters_data[data_indices[0]]

            # 分析Entry參數
            if "Entry_params" in sample_param:
                for entry_param in sample_param["Entry_params"]:
                    for key, value in entry_param.items():
                        if key not in ["indicator_type", "strat_idx"]:
                            param_name = f"Entry_{key}"
                            if param_name not in fixed_params:
                                # 收集所有可能的值
                                values = set()
                                for idx in data_indices:
                                    param = self.parameters_data[idx]
                                    if "Entry_params" in param:
                                        for ep in param["Entry_params"]:
                                            if (
                                                ep.get("indicator_type")
                                                == entry_param["indicator_type"]
                                                and ep.get("strat_idx")
                                                == entry_param["strat_idx"]
                                            ):
                                                values.add(ep.get(key))
                                if len(values) > 1:
                                    variable_params[param_name] = sorted(list(values))

            # 分析Exit參數
            if "Exit_params" in sample_param:
                for exit_param in sample_param["Exit_params"]:
                    for key, value in exit_param.items():
                        if key not in ["indicator_type", "strat_idx"]:
                            param_name = f"Exit_{key}"
                            if param_name not in fixed_params:
                                # 收集所有可能的值
                                values = set()
                                for idx in data_indices:
                                    param = self.parameters_data[idx]
                                    if "Exit_params" in param:
                                        for ep in param["Exit_params"]:
                                            if (
                                                ep.get("indicator_type")
                                                == exit_param["indicator_type"]
                                                and ep.get("strat_idx")
                                                == exit_param["strat_idx"]
                                            ):
                                                values.add(ep.get(key))
                                if len(values) > 1:
                                    variable_params[param_name] = sorted(list(values))

            return variable_params

        except Exception as e:
            self.logger.error(f"獲取可變參數失敗: {e}")
            return {}


class ParameterPlateauPlotter:
    """參數高原分析器"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.index_manager = None

    def create_parameter_landscape_layout(self, data: Dict[str, Any]) -> html.Div:
        """
        創建參數高原頁面布局

        Args:
            data: 解析後的數據字典

        Returns:
            html.Div: 參數高原頁面組件
        """
        try:
            return html.Div(
                [
                    html.H5("🏔️ 參數高原分析", className="mb-3"),
                    # 策略選擇區域
                    html.Div(
                        [
                            html.Label("選擇策略:", className="form-label fw-bold"),
                            dcc.Dropdown(
                                id="strategy-selector",
                                placeholder="請選擇策略...",
                                className="mb-3",
                            ),
                        ],
                        className="mb-4",
                    ),
                    # 績效指標選擇區域
                    html.Div(
                        [
                            html.Label(
                                "選擇績效指標:", className="form-label fw-bold mb-2"
                            ),
                            html.Div(
                                [
                                    dbc.Button(
                                        "Sharpe Ratio",
                                        id="btn-sharpe",
                                        color="primary",
                                        outline=False,
                                        className="me-2",
                                    ),
                                    dbc.Button(
                                        "Sortino Ratio",
                                        id="btn-sortino",
                                        color="primary",
                                        outline=True,
                                        className="me-2",
                                    ),
                                    dbc.Button(
                                        "Calmar Ratio",
                                        id="btn-calmar",
                                        color="primary",
                                        outline=True,
                                        className="me-2",
                                    ),
                                    dbc.Button(
                                        "MDD",
                                        id="btn-mdd",
                                        color="primary",
                                        outline=True,
                                        className="me-2",
                                    ),
                                ],
                                className="mb-3",
                            ),
                        ],
                        className="mb-4",
                    ),
                    # 參數控制面板
                    html.Div(
                        [
                            html.Label("參數控制面板", className="form-label fw-bold"),
                            html.Div(
                                id="parameter-control-panel",
                                className="p-3 border rounded bg-light",
                            ),
                        ],
                        className="mb-4",
                    ),
                    # 參數高原圖表
                    html.Div(
                        [
                            html.Label("參數高原圖表", className="form-label fw-bold"),
                            html.Div(
                                id="parameter-landscape-chart",
                                className="p-3 border rounded",
                            ),
                        ],
                        className="mb-4",
                    ),
                ]
            )

        except Exception as e:
            self.logger.error(f"創建參數高原布局失敗: {e}")
            return html.Div("參數高原布局創建失敗")

    def create_parameter_control_panel(
        self, analysis: Dict[str, Any], strategy_key: str = None
    ) -> html.Div:
        """
        創建參數控制面板 - 支持動態軸選擇

        Args:
            analysis: 策略參數分析結果
            strategy_key: 策略鍵值，用於索引管理

        Returns:
            html.Div: 參數控制面板組件
        """
        try:
            variable_params = analysis.get("variable_params", {})
            param_count = len(variable_params)

            if not variable_params:
                return html.P("沒有找到可變參數", className="text-muted")

            # 為每個參數創建勾選框 + 滑動條的組合
            param_controls = []
            for i, (param_name, param_values) in enumerate(variable_params.items()):
                if len(param_values) > 1:
                    # 新的邏輯：支持動態軸選擇
                    # 初始狀態：所有參數都可選，用戶自由選擇固定哪些
                    is_checked = False  # 初始都不勾選
                    is_disabled = False  # 初始都啟用
                    checkbox_disabled = False  # 都可以勾選/反勾
                    css_class = "slider-enabled"  # 初始都啟用

                    control = html.Div(
                        [
                            # 勾選框和參數名稱
                            html.Div(
                                [
                                    dbc.Checkbox(
                                        id={"type": "checkbox", "index": i},
                                        value=is_checked,  # 初始都不勾選
                                        disabled=checkbox_disabled,  # 都可以勾選/反勾
                                        className="me-2",
                                    ),
                                    html.Label(
                                        f"{param_name}",
                                        className="form-label fw-bold ms-2",
                                    ),
                                ],
                                className="d-flex align-items-center mb-2",
                            ),
                            # 滑動條（根據勾選狀態啟用/禁用）
                            dcc.Slider(
                                id={"type": "slider", "index": i},
                                min=min(param_values),
                                max=max(param_values),
                                step=None,  # 使用 marks 中定義的步長
                                value=min(param_values),  # 初始值設為最小值
                                disabled=is_disabled,  # 初始都啟用
                                marks={
                                    val: str(val) for val in param_values
                                },  # 使用實際參數值作為索引和標籤
                                tooltip={"placement": "bottom", "always_visible": True},
                                className=css_class,
                            ),
                        ],
                        className="mb-3",
                    )
                    param_controls.append(control)

            # 創建確認按鈕（初始狀態為禁用）
            confirm_button = dbc.Button(
                "更新圖表",
                id="btn-update-chart",
                color="secondary",
                disabled=True,
                className="mt-3",
            )

            return html.Div(
                [
                    # 使用說明
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Span(
                                        "使用說明:",
                                        className="fw-bold",
                                        style={"color": "#20c997"},
                                    )
                                ],
                                className="mb-2",
                            ),
                            html.Ol(
                                [
                                    html.Li(
                                        "勾選參數來固定其值",
                                        className="mb-1",
                                        style={"color": "#20c997"},
                                    ),
                                    html.Li(
                                        "使用滑動條選擇固定的參數值",
                                        className="mb-1",
                                        style={"color": "#20c997"},
                                    ),
                                    html.Li(
                                        "未勾選的參數將作為圖表的XY軸",
                                        className="mb-1",
                                        style={"color": "#20c997"},
                                    ),
                                    html.Li(
                                        "需要2個未被勾選的參數才能生成圖表",
                                        className="mb-1",
                                        style={"color": "#20c997"},
                                    ),
                                ],
                                className="mb-3",
                                style={"paddingLeft": "20px"},
                            ),
                        ],
                        className="mb-3",
                    ),
                    html.H6(
                        f"當前有 {param_count} 個參數，最多可固定 {max(0, param_count-2)} 個參數，必須留空至少 2 個參數",
                        className="text-info mb-3",
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Label(
                                        "固定參數🔓",
                                        className="form-label fw-bold me-2",
                                    ),
                                    dbc.Button(
                                        "全選",
                                        id="btn-toggle-all-params",
                                        color="secondary",
                                        size="sm",
                                        outline=True,
                                        className="ms-2",
                                    ),
                                ],
                                className="d-flex align-items-center",
                            ),
                            html.P(
                                "勾選後滑動條變暗無法滑動，未勾選的參數可滑動選擇值",
                                className="text-muted mb-0",
                            ),
                        ],
                        className="d-flex align-items-center mb-2",
                    ),
                    *param_controls,
                    confirm_button,
                ]
            )

        except Exception as e:
            self.logger.error(f"創建參數控制面板失敗: {e}")
            return html.P("參數控制面板創建失敗")

    def create_2d_parameter_heatmap(
        self,
        analysis: Dict[str, Any],
        metric: str,
        data: Dict[str, Any],
        strategy_key: str = None,
        fixed_params: Dict[str, Any] = None,
    ) -> html.Div:
        """
        創建2D參數高原熱力圖 - 支持動態軸選擇

        Args:
            analysis: 策略參數分析結果
            metric: 績效指標名稱
            data: 完整數據字典
            strategy_key: 策略鍵值，用於索引管理
            fixed_params: 固定參數字典，格式：{param_name: value}

        Returns:
            html.Div: 包含圖表的組件
        """

        try:
            # 檢查是否有足夠的可變參數
            variable_params = analysis.get("variable_params", {})

            if len(variable_params) < 2:
                # 詳細顯示可變參數信息
                param_info = []
                for param_name, param_values in variable_params.items():
                    param_info.append(f"{param_name}: {len(param_values)} 個值")
                return html.Div(
                    [
                        html.P(
                            "需要至少2個可變參數才能生成圖表", className="text-warning"
                        ),
                        html.P(
                            f"📊 當前找到的可變參數: {len(variable_params)}",
                            className="text-muted",
                        ),
                        html.P(
                            f"🔍 參數詳情: {', '.join(param_info)}",
                            className="text-muted",
                        ),
                        html.P(
                            "💡 建議: 檢查 Entry_params 和 Exit_params 中的參數變化",
                            className="text-info",
                        ),
                    ]
                )

            # 動態選擇可變參數作為X和Y軸
            param_names = list(variable_params.keys())

            # 根據固定參數選擇可變參數作為軸
            if fixed_params:
                # 找出未被固定的參數（可變參數）
                variable_axis_params = []
                for param_name in param_names:
                    if param_name not in fixed_params:
                        variable_axis_params.append(param_name)

                if len(variable_axis_params) >= 2:
                    x_axis = variable_axis_params[0]
                    y_axis = variable_axis_params[1]
                else:
                    return html.Div(
                        [
                            html.P(
                                "可變軸參數不足，無法生成2D圖表",
                                className="text-warning",
                            ),
                            html.P(
                                f"📊 當前可變軸參數: {len(variable_axis_params)}",
                                className="text-muted",
                            ),
                            html.P(
                                f"🔍 固定參數: {fixed_params}", className="text-info"
                            ),
                        ]
                    )
            else:
                # 沒有固定參數時，使用前兩個參數作為軸
                if len(param_names) >= 2:
                    x_axis = param_names[0]
                    y_axis = param_names[1]
                else:
                    return html.Div(
                        [
                            html.P(
                                "可變參數數量不足，無法生成2D圖表",
                                className="text-warning",
                            ),
                            html.P(
                                f"📊 當前可變參數: {len(param_names)}",
                                className="text-muted",
                            ),
                        ]
                    )

            # 獲取參數值列表
            x_values = variable_params[x_axis]
            y_values = variable_params[y_axis]

            # 創建績效指標矩陣
            performance_matrix = []
            valid_data_points = 0
            total_data_points = 0
            nan_data_points = 0

            for y_val in y_values:
                row = []
                for x_val in x_values:
                    # 查找對應的績效指標值（支持固定參數篩選）
                    performance_value = self._find_performance_value_enhanced(
                        analysis,
                        x_axis,
                        x_val,
                        y_axis,
                        y_val,
                        metric,
                        data,
                        fixed_params,
                    )
                    row.append(performance_value)

                    total_data_points += 1
                    if performance_value is not None and not np.isnan(
                        performance_value
                    ):
                        valid_data_points += 1
                    else:
                        nan_data_points += 1

                performance_matrix.append(row)

            # 檢查是否有有效數據
            if valid_data_points == 0:
                return html.Div(
                    [
                        html.P(
                            f"❌ 沒有找到有效的 {metric} 數據", className="text-warning"
                        ),
                        html.P(
                            f"📊 數據統計: 總點數 {total_data_points}, 有效點數 {valid_data_points}, NaN 點數 {nan_data_points}",
                            className="text-muted",
                        ),
                        html.P(
                            "💡 可能原因: 這些參數組合沒有產生交易，無法計算績效指標",
                            className="text-info",
                        ),
                    ]
                )

            # 保持完整的網格結構，不過濾數據
            # 將 NaN 值轉換為 0 以便顯示，但保持原始數據結構
            display_matrix = []
            for row in performance_matrix:
                display_row = []
                for val in row:
                    if val is not None and not np.isnan(val):
                        display_row.append(val)
                    else:
                        display_row.append(0)  # 用 0 填充 NaN，但保持網格結構
                display_matrix.append(display_row)

            # 使用原始數據進行顯示，保持網格完整性
            filtered_x_values = x_values
            filtered_y_values = y_values
            filtered_matrix = display_matrix

            # 轉換為numpy數組
            filtered_array = np.array(filtered_matrix)

            # 獲取threshold標準和顏色映射
            colorscale = self._get_threshold_based_colorscale(
                metric, performance_matrix
            )

            # 根據指標類型設定zmin和zmax來強制使用threshold標準
            if metric == "Sharpe":
                zmin, zmax = 0.5, 2.0  # 強制使用 0.5 到 2.0 的範圍
            elif metric == "Sortino":
                zmin, zmax = 0.5, 2.0  # 強制使用 0.5 到 2.0 的範圍
            elif metric == "Calmar":
                zmin, zmax = 0.5, 2.0  # 強制使用 0.5 到 2.0 的範圍
            elif metric == "Max_drawdown":
                zmin, zmax = -0.7, -0.0  # 強制使用 -0.30 到 -0.05 的範圍
            else:
                zmin, zmax = None, None  # 使用自動範圍

            # 創建熱力圖
            fig = go.Figure(
                data=go.Heatmap(
                    z=filtered_array,
                    x=filtered_x_values,
                    y=filtered_y_values,
                    colorscale=colorscale,
                    zmin=zmin,
                    zmax=zmax,
                    text=[
                        [
                            f"{val:.2f}" if val is not None and val != 0 else ""
                            for val in row
                        ]
                        for row in filtered_matrix
                    ],
                    texttemplate="%{text}",
                    textfont={"size": 14, "color": "#000000", "family": "Arial Black"},
                    hoverongaps=False,
                    hovertemplate=f"<b>{x_axis}</b>: %{{x}}<br>"
                    + f"<b>{y_axis}</b>: %{{y}}<br>"
                    + f"<b>{metric}</b>: %{{z:.2f}}<extra></extra>",
                    # 設置間隙來創建邊框效果
                    xgap=2,
                    ygap=2,
                )
            )

            # 創建圖表標題，顯示固定參數和可變軸
            chart_title = f"{metric} 參數高原圖表"
            if fixed_params:
                fixed_params_text = ", ".join(
                    [f"{k}={v}" for k, v in fixed_params.items()]
                )
                chart_title += f" (固定: {fixed_params_text})"
            chart_title += f" | X軸: {x_axis}, Y軸: {y_axis} - {valid_data_points}/{total_data_points} 有效數據點"

            # 更新布局
            fig.update_layout(
                title=chart_title,
                xaxis_title=x_axis,
                yaxis_title=y_axis,
                template=None,
                height=600,
                plot_bgcolor="#000000",  # 黑色背景來突出邊框效果
                paper_bgcolor="#181818",
                font=dict(color="#f5f5f5", size=14),
                xaxis=dict(
                    color="#ecbc4f",
                    gridcolor="rgba(0,0,0,0)",  # 移除格線
                    showgrid=False,  # 隱藏格線
                    tickfont=dict(color="#ecbc4f"),
                    zeroline=False,  # 隱藏零線
                    showticklabels=False,  # 隱藏 X 軸數字格位
                ),
                yaxis=dict(
                    color="#ecbc4f",
                    gridcolor="rgba(0,0,0,0)",  # 移除格線
                    showgrid=False,  # 隱藏格線
                    tickfont=dict(color="#ecbc4f"),
                    zeroline=False,  # 隱藏零線
                    showticklabels=False,  # 隱藏 Y 軸數字格位
                ),
                title_font=dict(color="#ecbc4f", size=18),
            )

            # 創建圖表組件
            chart_component = dcc.Graph(
                id="parameter-heatmap",
                figure=fig,
                config={"displayModeBar": True, "displaylogo": False},
            )

            # 添加數據質量信息
            info_panel = html.Div(
                [
                    html.H6("📊 數據質量信息", className="mt-3 mb-2"),
                    html.P(f"總參數組合: {total_data_points}", className="mb-1"),
                    html.P(
                        f"有效數據點: {valid_data_points} ({valid_data_points/total_data_points*100:.1f}%)",
                        className="mb-1 text-success",
                    ),
                    html.P(
                        f"無效數據點: {nan_data_points} ({nan_data_points/total_data_points*100:.1f}%)",
                        className="mb-1 text-warning",
                    ),
                    html.P(
                        f"顯示的參數範圍: X軸({len(filtered_x_values)}個值), Y軸({len(filtered_y_values)}個值)",
                        className="mb-1",
                    ),
                ],
                className="p-3 border rounded bg-light mt-3",
            )

            return html.Div([chart_component, info_panel])

        except Exception as e:
            return html.P(f"創建圖表失敗: {str(e)}", className="text-danger")

    def _find_performance_value_enhanced(
        self,
        analysis: Dict[str, Any],
        x_axis: str,
        x_val: str,
        y_axis: str,
        y_val: str,
        metric: str,
        data: Dict[str, Any],
        fixed_params: Dict[str, Any] = None,
    ) -> float:
        """
        增強的績效指標查找（支持固定參數篩選）

        Args:
            analysis: 策略參數分析結果
            x_axis: X軸參數名（可能包含 Entry/Exit 前綴）
            x_val: X軸參數值
            y_axis: Y軸參數名（可能包含 Entry/Exit 前綴）
            y_val: Y軸參數值
            metric: 績效指標名稱
            data: 完整數據字典
            fixed_params: 固定參數字典，格式：{param_name: value}

        Returns:
            float: 績效指標值，如果找不到則返回None
        """
        try:

            parameters = data.get("parameters", [])
            parameter_indices = analysis["parameter_indices"]

            for idx in parameter_indices:
                param = parameters[idx]

                # 首先檢查固定參數是否匹配
                if fixed_params:
                    fixed_params_match = True
                    for fixed_param_name, fixed_param_value in fixed_params.items():
                        if not self._check_param_match_enhanced(
                            param, fixed_param_name, fixed_param_value
                        ):
                            fixed_params_match = False
                            break

                    if not fixed_params_match:
                        continue  # 固定參數不匹配，跳過這個組合

                # 檢查軸參數是否匹配
                x_match = self._check_param_match_enhanced(param, x_axis, x_val)
                y_match = self._check_param_match_enhanced(param, y_axis, y_val)

                if x_match and y_match:
                    # 找到匹配的參數組合，返回績效指標值
                    performance_value = self._extract_metric_value(param, metric)
                    return performance_value

            return None
            return None

        except Exception as e:
            self.logger.error(f"查找績效指標值失敗: {e}")
            return None

    def _get_param_summary(self, param: Dict[str, Any]) -> str:
        """獲取參數的摘要信息，用於調試"""
        try:
            summary_parts = []

            if "Entry_params" in param:
                for entry_param in param["Entry_params"]:
                    indicator_type = entry_param.get("indicator_type", "")
                    strat_idx = entry_param.get("strat_idx", "")
                    # 添加所有非系統參數
                    for key, value in entry_param.items():
                        if key not in ["indicator_type", "strat_idx"]:
                            summary_parts.append(
                                f"Entry_{indicator_type}{strat_idx}_{key}={value}"
                            )

            if "Exit_params" in param:
                for exit_param in param["Exit_params"]:
                    indicator_type = exit_param.get("indicator_type", "")
                    strat_idx = exit_param.get("strat_idx", "")
                    # 添加所有非系統參數
                    for key, value in exit_param.items():
                        if key not in ["indicator_type", "strat_idx"]:
                            summary_parts.append(
                                f"Exit_{indicator_type}{strat_idx}_{key}={value}"
                            )

            return ", ".join(summary_parts)

        except Exception as e:
            return f"參數摘要生成失敗: {e}"

    def _check_param_match_enhanced(
        self, param: Dict[str, Any], param_name: str, param_value: str
    ) -> bool:
        """
        增強的參數匹配檢查（支持多指標策略）

        參數名稱格式：Entry_MA8_shortMA_period 或 Exit_MA5_longMA_period
        """
        try:
            # 檢查參數名稱格式
            if param_name.startswith(("Entry_", "Exit_")):
                # Entry/Exit 參數格式：Entry_MA8_shortMA_period 或 Exit_MA5_longMA_period
                parts = param_name.split(
                    "_", 2
                )  # 分割為 ['Entry/Exit', 'MA8/MA5', 'shortMA_period/longMA_period']
                if len(parts) >= 3:
                    param_type = parts[0]  # Entry 或 Exit
                    indicator_key = parts[1]  # MA8 或 MA5
                    actual_param_name = parts[2]  # shortMA_period 或 longMA_period

                    # 檢查對應的參數組
                    param_key = f"{param_type}_params"
                    if param_key in param:
                        for param_item in param[param_key]:
                            # 檢查指標是否匹配
                            param_indicator = param_item.get(
                                "indicator_type", ""
                            ) + param_item.get("strat_idx", "")
                            param_actual_value = param_item.get(actual_param_name)

                            # 數值類型轉換和比較
                            try:
                                param_float = float(param_actual_value)
                                target_float = float(param_value)
                                value_match = (
                                    abs(param_float - target_float) < 0.001
                                )  # 允許小的浮點誤差
                            except (ValueError, TypeError):
                                value_match = str(param_actual_value) == str(
                                    param_value
                                )

                            if param_indicator == indicator_key and value_match:
                                return True

                    return False
            else:
                # 沒有前綴的參數（向後兼容）
                entry_match = False
                exit_match = False

                # 檢查 Entry_params
                if "Entry_params" in param:
                    for entry_param in param["Entry_params"]:
                        entry_value = entry_param.get(param_name)
                        try:
                            entry_float = float(entry_value)
                            param_float = float(param_value)
                            if abs(entry_float - param_float) < 0.001:
                                entry_match = True
                                break
                        except (ValueError, TypeError):
                            if str(entry_value) == str(param_value):
                                entry_match = True
                                break

                # 檢查 Exit_params
                if "Exit_params" in param:
                    for exit_param in param["Exit_params"]:
                        exit_value = exit_param.get(param_name)
                        try:
                            exit_float = float(exit_value)
                            param_float = float(param_value)
                            if abs(exit_float - param_float) < 0.001:
                                exit_match = True
                                break
                        except (ValueError, TypeError):
                            if str(exit_value) == str(param_value):
                                exit_match = True
                                break

                return entry_match or exit_match

        except Exception as e:
            self.logger.error(f"參數匹配檢查失敗: {e}")
            return False

    def _extract_metric_value(self, param: Dict[str, Any], metric: str) -> float:
        """提取績效指標值"""
        try:
            if metric == "Max_drawdown":
                metric_key = "Max_drawdown"
            else:
                metric_key = metric

            # 從頂層提取績效指標（所有策略都使用這種存儲方式）
            if metric_key in param:
                value = param[metric_key]
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return None

            return None

        except Exception:
            return None

    def register_callbacks(self, app: dash.Dash, data: Dict[str, Any]):
        """
        註冊參數高原相關的 callbacks

        Args:
            app: Dash 應用實例
            data: 數據字典
        """

        # 策略選擇下拉選單的回調函數
        @app.callback(
            Output("strategy-selector", "options"),
            Input("btn-parameter-landscape", "n_clicks"),
            prevent_initial_call=False,
        )
        def populate_strategy_selector(n_clicks):
            """填充策略選擇下拉選單"""
            strategy_groups = data.get("strategy_groups", {})

            options = []
            for strategy_key, strategy_info in strategy_groups.items():
                entry_names = strategy_info["entry_names"]
                exit_names = strategy_info["exit_names"]
                count = strategy_info["count"]

                # 創建更友好的顯示標籤
                label = f"Entry: {', '.join(entry_names)} | Exit: {', '.join(exit_names)} ({count} 組合)"
                options.append({"label": label, "value": strategy_key})

            return options

        # 績效指標按鈕狀態控制回調函數
        @app.callback(
            [
                Output("btn-sharpe", "outline"),
                Output("btn-sortino", "outline"),
                Output("btn-calmar", "outline"),
                Output("btn-mdd", "outline"),
            ],
            [
                Input("btn-sharpe", "n_clicks"),
                Input("btn-sortino", "n_clicks"),
                Input("btn-calmar", "n_clicks"),
                Input("btn-mdd", "n_clicks"),
            ],
            prevent_initial_call=True,
        )
        def update_button_states(
            sharpe_clicks, sortino_clicks, calmar_clicks, mdd_clicks
        ):
            """更新績效指標按鈕的長亮狀態"""
            if not ctx.triggered_id:
                raise PreventUpdate

            # 預設所有按鈕都是 outline=True（未選中）
            states = [True, True, True, True]

            # 根據觸發的按鈕設置對應狀態為 False（選中，長亮）
            if ctx.triggered_id == "btn-sharpe":
                states[0] = False
            elif ctx.triggered_id == "btn-sortino":
                states[1] = False
            elif ctx.triggered_id == "btn-calmar":
                states[2] = False
            elif ctx.triggered_id == "btn-mdd":
                states[3] = False

            return states

        # 處理slider狀態更新的回調函數
        @app.callback(
            [
                Output({"type": "slider", "index": ALL}, "disabled"),
                Output({"type": "slider", "index": ALL}, "className"),
            ],
            Input({"type": "checkbox", "index": ALL}, "value"),
            prevent_initial_call=True,
        )
        def update_slider_states(checkbox_values):
            """根據checkbox狀態更新slider狀態"""
            if not checkbox_values:
                return [], []

            # 返回滑動條狀態：勾選的禁用，未勾選的啟用
            slider_states = [val for val in checkbox_values]
            # 返回CSS類名：勾選的變暗，未勾選的變亮
            css_classes = [
                "slider-disabled" if val else "slider-enabled"
                for val in checkbox_values
            ]

            return slider_states, css_classes

        # 全選/反選參數的回調函數
        @app.callback(
            [
                Output({"type": "checkbox", "index": ALL}, "value"),
                Output("btn-toggle-all-params", "children"),
            ],
            Input("btn-toggle-all-params", "n_clicks"),
            [
                State({"type": "checkbox", "index": ALL}, "value"),
                State({"type": "checkbox", "index": ALL}, "id"),
            ],
            prevent_initial_call=True,
        )
        def toggle_all_parameters(n_clicks, checkbox_values, checkbox_ids):
            """全選/反選所有參數"""
            if not n_clicks or not checkbox_values:
                raise PreventUpdate

            # 檢查是否已經全選
            is_all_selected = all(checkbox_values)

            if is_all_selected:
                # 如果全選，則全部反選
                return [False] * len(checkbox_values), "全選"
            else:
                # 如果不是全選，則全部選中
                return [True] * len(checkbox_values), "反選"

        # 處理更新圖表按鈕狀態的回調函數
        @app.callback(
            [
                Output("btn-update-chart", "color"),
                Output("btn-update-chart", "disabled"),
                Output("btn-update-chart", "children"),
            ],
            [
                Input({"type": "checkbox", "index": ALL}, "value"),
                Input("strategy-selector", "value"),
            ],
            prevent_initial_call=True,
        )
        def update_button_state(checkbox_values, strategy_key):
            """根據checkbox狀態和策略更新按鈕狀態和顯示現況"""

            if not checkbox_values or not strategy_key:
                return "secondary", True, "更新圖表"

            # 計算已勾選和未勾選的數量
            checked_count = sum(1 for val in checkbox_values if val)
            unchecked_count = len(checkbox_values) - checked_count

            # 構建現況顯示文字
            status_text = (
                f"更新圖表 (已勾選: {checked_count}, 未勾選: {unchecked_count})"
            )

            # 只有當未勾選的參數=2個時，按鈕才亮紅色且可點擊
            if unchecked_count == 2:
                # 檢查是否有索引管理器，如果沒有則建立
                if not hasattr(self, "index_manager") or self.index_manager is None:
                    parameters = data.get("parameters", [])
                    self.index_manager = ParameterIndexManager(parameters)
                    self.index_manager.build_indexes()

                return "danger", False, status_text
            else:
                return "secondary", True, status_text

        # 參數控制面板更新回調函數
        @app.callback(
            Output("parameter-control-panel", "children"),
            Input("strategy-selector", "value"),
            prevent_initial_call=True,
        )
        def update_parameter_control_panel(strategy_key):
            """更新參數控制面板"""
            if not strategy_key:
                return html.P("請選擇策略", className="text-muted")

            try:
                from .DataImporter_plotter import DataImporterPlotter

                parameters = data.get("parameters", [])

                # 使用緩存的策略分析方法
                # 檢查是否有DataImporterPlotter實例
                if hasattr(self, "data_importer") and self.data_importer is not None:
                    analysis = self.data_importer.get_strategy_analysis_cached(
                        parameters, strategy_key
                    )
                else:
                    # 如果沒有實例，創建一個臨時實例
                    temp_importer = DataImporterPlotter("", None)
                    analysis = temp_importer.get_strategy_analysis_cached(
                        parameters, strategy_key
                    )

                if not analysis:
                    return html.P("無法分析策略參數", className="text-danger")

                return self.create_parameter_control_panel(analysis)

            except Exception as e:
                return html.P(
                    f"更新參數控制面板失敗: {str(e)}", className="text-danger"
                )

        # 生成2D參數高原圖表的回調函數
        @app.callback(
            Output("parameter-landscape-chart", "children"),
            [
                Input("strategy-selector", "value"),
                Input("btn-sharpe", "n_clicks"),
                Input("btn-sortino", "n_clicks"),
                Input("btn-calmar", "n_clicks"),
                Input("btn-mdd", "n_clicks"),
                Input("btn-update-chart", "n_clicks"),
            ],
            [
                State({"type": "checkbox", "index": ALL}, "value"),
                State({"type": "slider", "index": ALL}, "value"),
            ],
            prevent_initial_call=True,
        )
        def generate_parameter_landscape_chart(
            strategy_key,
            sharpe_clicks,
            sortino_clicks,
            calmar_clicks,
            mdd_clicks,
            update_clicks,
            checkbox_values,
            slider_values,
        ):
            """生成2D參數高原圖表 - 支持動態軸選擇"""

            if not strategy_key:
                return html.P("請選擇策略", className="text-muted")

            # 確定選中的績效指標
            ctx_triggered = ctx.triggered_id if ctx.triggered_id else None
            if ctx_triggered == "btn-sharpe":
                metric = "Sharpe"
            elif ctx_triggered == "btn-sortino":
                metric = "Sortino"
            elif ctx_triggered == "btn-calmar":
                metric = "Calmar"
            elif ctx_triggered == "btn-mdd":
                metric = "Max_drawdown"
            else:
                metric = "Sharpe"  # 預設

            # 生成圖表
            try:
                from .DataImporter_plotter import DataImporterPlotter

                parameters = data.get("parameters", [])

                # 使用緩存的策略分析方法
                if hasattr(self, "data_importer") and self.data_importer is not None:
                    analysis = self.data_importer.get_strategy_analysis_cached(
                        parameters, strategy_key
                    )
                else:
                    # 如果沒有實例，創建一個臨時實例
                    temp_importer = DataImporterPlotter("", None)
                    analysis = temp_importer.get_strategy_analysis_cached(
                        parameters, strategy_key
                    )

                if not analysis:
                    return html.P("無法分析策略參數", className="text-danger")

                # 檢查是否有索引管理器
                if not hasattr(self, "index_manager") or self.index_manager is None:
                    self.index_manager = ParameterIndexManager(parameters)
                    self.index_manager.build_indexes()

                # 構建固定參數字典
                fixed_params = {}
                if checkbox_values and slider_values:
                    variable_params = analysis.get("variable_params", {})
                    param_names = list(variable_params.keys())

                    for i, (is_checked, slider_value) in enumerate(
                        zip(checkbox_values, slider_values)
                    ):
                        if i < len(param_names) and is_checked:
                            param_name = param_names[i]
                            fixed_params[param_name] = slider_value

                # 創建2D熱力圖（支持動態軸選擇）
                chart = self.create_2d_parameter_heatmap(
                    analysis, metric, data, strategy_key, fixed_params
                )
                return chart

            except Exception as e:
                return html.P(f"生成圖表失敗: {str(e)}", className="text-danger")

    def _get_threshold_based_colorscale(
        self, metric: str, performance_matrix: List[List[float]]
    ) -> List[List]:
        """
        根據績效指標的threshold標準生成顏色漸變

        Args:
            metric: 績效指標名稱
            performance_matrix: 績效指標矩陣

        Returns:
            List[List]: 顏色漸變配置
        """
        try:
            # 提取所有有效的績效值
            valid_values = []
            for row in performance_matrix:
                for val in row:
                    if val is not None and not np.isnan(val):
                        valid_values.append(val)

            if not valid_values:
                # 如果沒有有效值，返回預設顏色
                return [[0.0, "#520032"], [0.5, "#F2933A"], [1.0, "#F9F8BB"]]

            # 根據指標類型設定threshold
            if metric == "Sharpe":
                thresholds = {
                    "unacceptable": 0.5,  # 不可接受
                    "qualified": 1.0,  # 合格
                    "good": 1.5,  # 良好
                    "excellent": 2.0,  # 優秀及以上
                }
            elif metric == "Sortino":
                thresholds = {
                    "unacceptable": 0.5,  # 不可接受
                    "qualified": 1.0,  # 合格
                    "good": 1.5,  # 良好
                    "excellent": 2.0,  # 優秀及以上
                }
            elif metric == "Calmar":
                thresholds = {
                    "unacceptable": 0.5,  # 不可接受
                    "qualified": 0.7,  # 合格
                    "good": 1.2,  # 良好
                    "excellent": 2.0,  # 優秀及以上
                }
            elif metric == "Max_drawdown":
                # MDD是負值，數值範圍通常是 -1.0 到 0
                # 例如 -0.57 表示 -57% 的drawdown
                thresholds = {
                    "unacceptable": -0.7,  # 不可接受
                    "qualified": -0.5,  # 合格
                    "good": -0.3,  # 良好
                    "excellent": -0.1,  # 優秀及以上
                }
            else:
                # 預設threshold
                thresholds = {
                    "unacceptable": 0.5,
                    "qualified": 1.0,
                    "good": 1.5,
                    "excellent": 2.0,
                }

            # 計算實際的數值範圍
            min_val = min(valid_values)
            max(valid_values)

            # 根據threshold計算顏色位置
            if metric == "Max_drawdown":
                # MDD是負值，邏輯需要反轉：
                # 數值越小（越接近0）越好，數值越大（越負）越差
                if min_val >= thresholds["excellent"]:
                    # 所有值都在優秀範圍內（>= -0.05，即 <= 5%）
                    colorscale = [[0.0, "#F9F8BB"], [0.5, "#FFF399"], [1.0, "#FFE252"]]
                elif min_val >= thresholds["good"]:
                    # 最低到良好範圍（>= -0.10，即 <= 10%）
                    colorscale = [[0.0, "#FFD525"], [0.5, "#FFE252"], [1.0, "#FFF399"]]
                elif min_val >= thresholds["qualified"]:
                    # 最低到合格範圍（>= -0.20，即 <= 20%）
                    colorscale = [[0.0, "#F2933A"], [0.5, "#F5A23A"], [1.0, "#FFD525"]]
                else:
                    # 包含不可接受範圍（< -0.20，即 > 20%）
                    colorscale = [
                        [0.0, "#520032"],
                        [0.3, "#751614"],
                        [0.6, "#F2933A"],
                        [1.0, "#F0AA38"],
                    ]
            else:
                # 其他指標的正常處理 - 使用真正的threshold映射
                # 創建基於threshold的漸變顏色，而不是基於數據範圍
                colorscale = [
                    [0.0, "#520032"],  # 最差 (<= 0.5)
                    [0.25, "#751614"],  # 差 (0.5 到 1.0)
                    [0.5, "#F2933A"],  # 合格 (1.0 到 1.5)
                    [0.75, "#FFD525"],  # 良好 (1.5 到 2.0)
                    [1.0, "#F9F8BB"],  # 優秀 (>= 2.0)
                ]

            return colorscale

        except Exception:
            # 返回預設顏色
            return [[0.0, "#520032"], [0.5, "#F2933A"], [1.0, "#F9F8BB"]]
