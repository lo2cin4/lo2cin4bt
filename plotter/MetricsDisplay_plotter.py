"""
MetricsDisplay_plotter.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 可視化平台的績效指標顯示工具，負責生成績效指標表格和詳情顯示，包括表格生成、指標格式化、詳情卡片等。

【流程與數據流】
------------------------------------------------------------
- 被 DashboardGenerator 和 CallbackHandler 調用，負責指標顯示
- 主要數據流：

```mermaid
flowchart TD
    A[MetricsDisplay] -->|接收| B[績效指標數據]
    B -->|處理| C[數據格式化]
    C -->|生成| D[表格組件]
    C -->|生成| E[詳情組件]
    D -->|返回| F[HTML表格]
    E -->|返回| G[詳情卡片]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增指標類型、顯示格式時，請同步更新頂部註解與對應模組
- 若指標格式有變動，需同步更新調用模組
- 指標格式化和表格生成需要特別注意一致性

【常見易錯點】
------------------------------------------------------------
- 數據格式不正確
- 指標計算錯誤
- 顯示格式不當
- 表格樣式問題

【範例】
------------------------------------------------------------
- 基本使用：display = MetricsDisplay()
- 生成表格：table = display.create_metrics_table(data)

【與其他模組的關聯】
------------------------------------------------------------
- 被 DashboardGenerator 和 CallbackHandler 調用
- 依賴 Dash Bootstrap Components
- 輸出表格組件供界面顯示

【參考】
------------------------------------------------------------
- 詳細流程規範如有變動，請同步更新本註解與 README
- 其他模組如有依賴本檔案的行為，請於對應模組頂部註解標明
- 表格組件設計請參考 Dash Bootstrap Components 文檔
"""

import logging
from typing import Any, Dict, List, Optional

import dash_bootstrap_components as dbc
import numpy as np
import pandas as pd
from dash import html


class MetricsDisplay:
    """
    績效指標顯示組件

    負責生成績效指標表格和詳情顯示，
    包括表格生成、指標格式化、詳情卡片等。
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        初始化績效指標顯示組件

        Args:
            logger: 日誌記錄器，預設為 None
        """
        self.logger = logger or logging.getLogger(__name__)

        # 定義指標分類
        self.metric_categories = {
            "returns": ["net_profit", "annualized_return", "total_return", "cagr"],
            "risk": ["max_drawdown", "volatility", "var_95", "cvar_95"],
            "risk_adjusted": [
                "sharpe_ratio",
                "sortino_ratio",
                "calmar_ratio",
                "information_ratio",
            ],
            "trading": [
                "total_trades",
                "win_rate",
                "profit_factor",
                "avg_trade_return",
            ],
            "bah_comparison": [
                "bah_return",
                "bah_annualized_return",
                "bah_max_drawdown",
                "excess_return",
            ],
        }

        # 定義指標顯示名稱
        self.metric_names = {
            "net_profit": "淨利潤",
            "annualized_return": "年化回報率",
            "total_return": "總回報率",
            "cagr": "複合年增長率",
            "max_drawdown": "最大回撤",
            "volatility": "波動率",
            "var_95": "VaR (95%)",
            "cvar_95": "CVaR (95%)",
            "sharpe_ratio": "夏普比率",
            "sortino_ratio": "索提諾比率",
            "calmar_ratio": "卡爾瑪比率",
            "information_ratio": "信息比率",
            "total_trades": "總交易次數",
            "win_rate": "勝率",
            "profit_factor": "盈虧比",
            "avg_trade_return": "平均交易回報",
            "bah_return": "買入持有回報",
            "bah_annualized_return": "買入持有年化回報",
            "bah_max_drawdown": "買入持有最大回撤",
            "excess_return": "超額回報",
            "transaction_cost": "手續費",
            "slippage_cost": "滑點成本",
        }

    def create_metrics_table(
        self,
        metrics_data: Dict[str, Any],
        selected_params: List[str],
        sort_by: str = "net_profit",
    ) -> html.Div:
        """
        創建績效指標表格

        Args:
            metrics_data: 績效指標數據字典
            selected_params: 選中的參數組合列表
            sort_by: 排序指標，預設為 'net_profit'

        Returns:
            html.Div: 表格組件
        """
        try:
            if not selected_params:
                return html.Div("請選擇參數組合", className="alert alert-info")

            # 創建表格數據
            table_data = []
            for param_key in selected_params:
                if param_key in metrics_data:
                    metric_data = metrics_data[param_key]
                    row = {"參數組合": param_key}

                    # 添加所有可用指標
                    for metric_key, metric_name in self.metric_names.items():
                        if metric_key in metric_data:
                            value = metric_data[metric_key]
                            row[metric_name] = self._format_metric_value(
                                metric_key, value
                            )
                        else:
                            row[metric_name] = "N/A"

                    table_data.append(row)

            if not table_data:
                return html.Div("無績效指標數據", className="alert alert-warning")

            # 排序數據
            if sort_by in self.metric_names:
                sort_column = self.metric_names[sort_by]
                if sort_column in table_data[0]:
                    table_data.sort(
                        key=lambda x: self._extract_numeric_value(
                            x.get(sort_column, 0)
                        ),
                        reverse=True,
                    )

            # 創建表格
            df = pd.DataFrame(table_data)
            table = dbc.Table.from_dataframe(
                df,
                striped=True,
                bordered=True,
                hover=True,
                responsive=True,
                className="table-sm table-dark",
            )

            return html.Div(
                [
                    html.H6(
                        f"績效指標表格 (按 {self.metric_names.get(sort_by, sort_by)} 排序)",
                        className="mb-3",
                    ),
                    table,
                ]
            )

        except Exception as e:
            self.logger.error(f"創建績效指標表格失敗: {e}")
            return html.Div("表格創建失敗", className="alert alert-danger")

    def create_detailed_metrics_card(
        self, param_key: str, metrics_data: Dict[str, Any], parameters: Dict[str, Any]
    ) -> html.Div:
        """
        創建詳細績效指標卡片

        Args:
            param_key: 參數組合鍵
            metrics_data: 績效指標數據字典
            parameters: 參數數據字典

        Returns:
            html.Div: 詳細指標卡片
        """
        try:
            if param_key not in metrics_data:
                return html.Div("無數據", className="alert alert-warning")

            metrics = metrics_data[param_key]
            param_info = parameters.get(param_key, {})

            # 創建指標卡片
            cards = []

            for category, metric_keys in self.metric_categories.items():
                category_cards = []
                for metric_key in metric_keys:
                    if metric_key in metrics:
                        value = metrics[metric_key]
                        formatted_value = self._format_metric_value(metric_key, value)

                        card = dbc.Card(
                            [
                                dbc.CardBody(
                                    [
                                        html.H6(
                                            self.metric_names.get(
                                                metric_key, metric_key
                                            ),
                                            className="card-title",
                                        ),
                                        html.P(formatted_value, className="card-text"),
                                    ]
                                )
                            ],
                            className="mb-2",
                        )
                        category_cards.append(card)

                if category_cards:
                    category_name = self._get_category_name(category)
                    cards.append(
                        html.Div(
                            [
                                html.H5(category_name, className="mb-3"),
                                dbc.Row(
                                    [dbc.Col(card, width=6) for card in category_cards]
                                ),
                            ]
                        )
                    )

            # 創建參數信息卡片
            param_card = self._create_parameter_card(param_info)

            return html.Div(
                [
                    html.H4(f"策略詳情: {param_key}", className="mb-4"),
                    param_card,
                    html.Hr(),
                    *cards,
                ]
            )

        except Exception as e:
            self.logger.error(f"創建詳細指標卡片失敗: {e}")
            return html.Div("卡片創建失敗", className="alert alert-danger")

    def create_summary_statistics(
        self, metrics_data: Dict[str, Any], selected_params: List[str]
    ) -> html.Div:
        """
        創建摘要統計信息

        Args:
            metrics_data: 績效指標數據字典
            selected_params: 選中的參數組合列表

        Returns:
            html.Div: 摘要統計組件
        """
        try:
            if not selected_params:
                return html.Div("請選擇參數組合", className="alert alert-info")

            # 計算統計信息
            summary_stats = {}
            for metric_key in self.metric_names.keys():
                values = []
                for param_key in selected_params:
                    if (
                        param_key in metrics_data
                        and metric_key in metrics_data[param_key]
                    ):
                        value = metrics_data[param_key][metric_key]
                        if isinstance(value, (int, float)) and not pd.isna(value):
                            values.append(value)

                if values:
                    summary_stats[metric_key] = {
                        "mean": np.mean(values),
                        "median": np.median(values),
                        "std": np.std(values),
                        "min": np.min(values),
                        "max": np.max(values),
                    }

            # 創建統計表格
            stats_data = []
            for metric_key, stats in summary_stats.items():
                metric_name = self.metric_names.get(metric_key, metric_key)
                row = {
                    "指標": metric_name,
                    "平均值": self._format_metric_value(metric_key, stats["mean"]),
                    "中位數": self._format_metric_value(metric_key, stats["median"]),
                    "標準差": self._format_metric_value(metric_key, stats["std"]),
                    "最小值": self._format_metric_value(metric_key, stats["min"]),
                    "最大值": self._format_metric_value(metric_key, stats["max"]),
                }
                stats_data.append(row)

            if not stats_data:
                return html.Div("無統計數據", className="alert alert-warning")

            # 創建表格
            df = pd.DataFrame(stats_data)
            table = dbc.Table.from_dataframe(
                df,
                striped=True,
                bordered=True,
                hover=True,
                responsive=True,
                className="table-sm table-dark",
            )

            return html.Div(
                [
                    html.H6(
                        f"摘要統計 (共 {len(selected_params)} 個組合)", className="mb-3"
                    ),
                    table,
                ]
            )

        except Exception as e:
            self.logger.error(f"創建摘要統計失敗: {e}")
            return html.Div("統計創建失敗", className="alert alert-danger")

    def _format_metric_value(self, metric_key: str, value: Any) -> str:
        """
        格式化指標值

        Args:
            metric_key: 指標鍵
            value: 指標值

        Returns:
            str: 格式化後的值
        """
        try:
            if pd.isna(value) or value is None:
                return "N/A"

            if not isinstance(value, (int, float)):
                return str(value)

            # 根據指標類型格式化
            if "rate" in metric_key or "ratio" in metric_key:
                return f"{value:.2%}"
            elif "profit" in metric_key or "return" in metric_key:
                return f"{value:,.2f}"
            elif "trades" in metric_key or "count" in metric_key:
                return f"{int(value):,}"
            else:
                return f"{value:.4f}"

        except Exception as e:
            self.logger.warning(f"格式化指標值失敗 {metric_key}: {e}")
            return str(value)

    def _extract_numeric_value(self, formatted_value: str) -> float:
        """
        從格式化值中提取數值

        Args:
            formatted_value: 格式化後的值

        Returns:
            float: 數值
        """
        try:
            if formatted_value == "N/A":
                return 0.0

            # 移除百分比符號和逗號
            cleaned = formatted_value.replace("%", "").replace(",", "")
            return float(cleaned)

        except Exception as e:
            self.logger.warning(f"提取數值失敗 {formatted_value}: {e}")
            return 0.0

    def _get_category_name(self, category: str) -> str:
        """
        獲取分類名稱

        Args:
            category: 分類鍵

        Returns:
            str: 分類名稱
        """
        category_names = {
            "returns": "收益指標",
            "risk": "風險指標",
            "risk_adjusted": "風險調整後收益",
            "trading": "交易指標",
            "bah_comparison": "買入持有比較",
        }
        return category_names.get(category, category)

    def _create_parameter_card(self, param_info: Dict[str, Any]) -> html.Div:
        """
        創建參數信息卡片

        Args:
            param_info: 參數信息字典

        Returns:
            html.Div: 參數卡片
        """
        try:
            if not param_info:
                return html.Div("無參數信息", className="alert alert-warning")

            # 創建參數列表
            param_items = []
            for key, value in param_info.items():
                if key != "parameters":  # 跳過嵌套的參數字典
                    # 統一顯示首字大寫，與 parquet metadata 一致
                    display_key = key[0].upper() + key[1:] if key else key
                    param_items.append(html.Li(f"{display_key}: {value}"))

            # 如果有嵌套參數，也顯示
            if "parameters" in param_info:
                nested_params = param_info["parameters"]
                for key, value in nested_params.items():
                    param_items.append(html.Li(f"{key}: {value}"))

            return html.Div(
                [
                    html.H6("參數信息", className="mb-2"),
                    html.Ul(param_items, className="list-unstyled"),
                ],
                className="mb-3 p-3 border rounded",
            )

        except Exception as e:
            self.logger.error(f"創建參數卡片失敗: {e}")
            return html.Div("參數卡片創建失敗", className="alert alert-danger")
