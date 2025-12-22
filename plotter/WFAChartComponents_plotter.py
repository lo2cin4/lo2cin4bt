"""
WFAChartComponents_plotter.py

【功能說明】
------------------------------------------------------------
本模組為 WFA 可視化平台的圖表組件生成器，負責生成 WFA 專用的圖表組件。
- 生成九宮格熱力圖（3x3 矩陣）用於顯示 IS/OOS 績效對比
- 根據績效指標類型（Sharpe、Sortino、Calmar、MDD）生成對應的顏色漸變
- 支援多種績效指標的可視化，包含 IS 和 OOS 兩個子圖

【流程與數據流】
------------------------------------------------------------
- 主流程：接收 WFA 數據 → 構建 3x3 績效矩陣 → 生成 Plotly 圖表 → 返回圖表組件
- 各模組間數據流明確，流程如下：

```mermaid
flowchart TD
    A[WFACallbackHandler] -->|調用| B[WFAChartComponents]
    B -->|接收| C[WFA 窗口數據]
    C -->|構建| D[3x3 績效矩陣]
    D -->|生成| E[Plotly 熱力圖]
    E -->|返回| F[圖表組件]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增績效指標類型時，請同步更新顏色漸變邏輯和頂部註解
- 若績效矩陣結構有變動，需同步更新圖表生成邏輯
- 新增/修改績效指標、圖表類型時，務必同步更新本檔案與所有依賴模組
- 顏色漸變邏輯需要與 ParameterPlateau 保持一致

【常見易錯點】
------------------------------------------------------------
- 績效矩陣維度不正確導致圖表顯示錯誤
- 顏色漸變邏輯錯誤導致顏色映射不準確
- 指標值範圍異常導致圖表無法正常顯示

【範例】
------------------------------------------------------------
- 創建組件生成器：components = WFAChartComponents(logger)
- 構建九宮格矩陣：chart = components.build_3x3_matrix(param_info, metric, wfa_data, file_data)

【與其他模組的關聯】
------------------------------------------------------------
- 被 WFACallbackHandler 調用，生成圖表組件
- 被 WFADownloadHandler 調用，生成下載用的圖表
- 參考 plotter/ParameterPlateau_plotter.py 的顏色漸變邏輯

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本圖表生成功能

【參考】
------------------------------------------------------------
- plotter/ParameterPlateau_plotter.py: 參數高原可視化（顏色漸變邏輯參考）
- WFACallbackHandler_plotter.py: WFA 回調處理器
- WFADownloadHandler_plotter.py: WFA 下載處理器
- plotter/README.md: WFA 可視化平台詳細說明
"""

import logging
from typing import Any, Dict, List, Optional

import numpy as np
import plotly.graph_objs as go
from dash import dcc, html


class WFAChartComponents:
    """WFA 圖表組件生成器"""

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        初始化圖表組件生成器

        Args:
            logger: 日誌記錄器
        """
        self.logger = logger or logging.getLogger(__name__)

    def _get_threshold_based_colorscale(
        self, metric: str, performance_matrix: np.ndarray
    ) -> List[List]:
        """
        根據績效指標的threshold標準生成顏色漸變（參考 ParameterPlateau）

        Args:
            metric: 績效指標名稱 (Sharpe, Sortino, Calmar, Max_drawdown)
            performance_matrix: 3x3 績效指標矩陣

        Returns:
            List[List]: 顏色漸變配置
        """
        try:
            # 提取所有有效的績效值
            valid_values = performance_matrix[~np.isnan(performance_matrix)].tolist()

            if not valid_values:
                return [[0.0, "#520032"], [0.5, "#F2933A"], [1.0, "#F9F8BB"]]

            # 根據指標類型設定threshold
            if metric in ["Sharpe", "Sortino"]:
                colorscale = [
                    [0.0, "#520032"],  # 最差 (<= 0.5)
                    [0.25, "#751614"],  # 差 (0.5 到 1.0)
                    [0.5, "#F2933A"],  # 合格 (1.0 到 1.5)
                    [0.75, "#FFD525"],  # 良好 (1.5 到 2.0)
                    [1.0, "#F9F8BB"],  # 優秀 (>= 2.0)
                ]
            elif metric == "Calmar":
                colorscale = [
                    [0.0, "#520032"],  # 最差 (<= 0.5)
                    [0.25, "#751614"],  # 差 (0.5 到 0.7)
                    [0.5, "#F2933A"],  # 合格 (0.7 到 1.2)
                    [0.75, "#FFD525"],  # 良好 (1.2 到 2.0)
                    [1.0, "#F9F8BB"],  # 優秀 (>= 2.0)
                ]
            elif metric in ["Max_drawdown", "MDD"]:
                # MDD是負值，數值越小（越接近0）越好
                min_val = min(valid_values)
                if min_val >= -0.1:
                    colorscale = [[0.0, "#F9F8BB"], [0.5, "#FFF399"], [1.0, "#FFE252"]]
                elif min_val >= -0.3:
                    colorscale = [[0.0, "#FFD525"], [0.5, "#FFE252"], [1.0, "#FFF399"]]
                elif min_val >= -0.5:
                    colorscale = [[0.0, "#F2933A"], [0.5, "#F5A23A"], [1.0, "#FFD525"]]
                else:
                    colorscale = [
                        [0.0, "#520032"],
                        [0.3, "#751614"],
                        [0.6, "#F2933A"],
                        [1.0, "#F0AA38"],
                    ]
            else:
                colorscale = [[0.0, "#520032"], [0.5, "#F2933A"], [1.0, "#F9F8BB"]]

            return colorscale

        except Exception as e:
            self.logger.error(f"生成顏色漸變失敗: {e}")
            return [[0.0, "#520032"], [0.5, "#F2933A"], [1.0, "#F9F8BB"]]

    def create_3x3_heatmap(
        self,
        matrix: np.ndarray,
        metric: str,
        param_info: Dict[str, Any],
        title_prefix: str = "",
    ) -> html.Div:
        """
        創建 3x3 九宮格熱力圖

        Args:
            matrix: 3x3 績效指標矩陣（numpy array）
            metric: 績效指標名稱
            param_info: 參數信息字典，包含 param1_key, param1_values, param2_key, param2_values
            title_prefix: 圖表標題前綴（例如 "IS" 或 "OOS"）

        Returns:
            html.Div: 包含熱力圖的 Dash 組件
        """
        try:
            # 獲取參數信息
            param1_key = param_info.get("param1_key", "")
            param1_values = param_info.get("param1_values", [])
            param2_key = param_info.get("param2_key", "")
            param2_values = param_info.get("param2_values", [])

            # 準備 x 軸和 y 軸標籤
            x_labels = [str(v) for v in param2_values] if param2_values else ["", "", ""]
            y_labels = [str(v) for v in param1_values] if param1_values else ["", "", ""]

            # 如果沒有足夠的標籤，使用默認標籤
            while len(x_labels) < 3:
                x_labels.append("")
            while len(y_labels) < 3:
                y_labels.append("")

            # 獲取顏色漸變
            colorscale = self._get_threshold_based_colorscale(metric, matrix)

            # 設定 zmin 和 zmax
            if metric in ["Sharpe", "Sortino", "Calmar"]:
                zmin, zmax = 0.5, 2.0
            elif metric in ["Max_drawdown", "MDD"]:
                zmin, zmax = -0.7, -0.0
            else:
                zmin, zmax = None, None

            # 生成文本矩陣（用於顯示數值）
            text_matrix = []
            for row in matrix:
                text_row = []
                for val in row:
                    if val is not None and not np.isnan(val):
                        # 確保 val 是數字類型
                        try:
                            val_float = float(val)
                            # MDD 和 Max_drawdown 都使用百分比格式
                            if metric in ["Max_drawdown", "MDD"]:
                                text_row.append(f"{val_float:.2%}")
                            else:
                                text_row.append(f"{val_float:.2f}")
                        except (ValueError, TypeError):
                            # 如果轉換失敗，使用原始值（但這不應該發生）
                            text_row.append(str(val))
                    else:
                        text_row.append("")
                text_matrix.append(text_row)

            # 創建熱力圖
            fig = go.Figure(
                data=go.Heatmap(
                    z=matrix,
                    x=x_labels,
                    y=y_labels,
                    colorscale=colorscale,
                    zmin=zmin,
                    zmax=zmax,
                    text=text_matrix,
                    texttemplate="%{text}",
                    textfont={"size": 14, "color": "#000000", "family": "Arial Black"},
                    hoverongaps=False,
                    xgap=2,
                    ygap=2,
                )
            )

            # 設定布局
            chart_title = f"{title_prefix} {metric}" if title_prefix else metric
            if param1_key and param2_key:
                chart_title += f" | {param1_key} vs {param2_key}"

            fig.update_layout(
                title=chart_title,
                xaxis_title=param2_key or "",
                yaxis_title=param1_key or "",
                template=None,
                height=350,  # 輕微放大：從 300 增加到 350
                width=350,   # 輕微放大：從 300 增加到 350
                plot_bgcolor="#000000",
                paper_bgcolor="#181818",
                font=dict(color="#f5f5f5", size=12),
                xaxis=dict(
                    color="#ecbc4f",
                    gridcolor="rgba(0,0,0,0)",
                    showgrid=False,
                    tickfont=dict(color="#ecbc4f", size=10),
                    zeroline=False,
                ),
                yaxis=dict(
                    color="#ecbc4f",
                    gridcolor="rgba(0,0,0,0)",
                    showgrid=False,
                    tickfont=dict(color="#ecbc4f", size=10),
                    zeroline=False,
                ),
                title_font=dict(color="#ecbc4f", size=18),
                margin=dict(l=50, r=10, t=50, b=50),  # 減少右邊留空：從 20 減少到 10
            )

            return html.Div(
                dcc.Graph(figure=fig, config={"displayModeBar": False}),
                style={"display": "inline-block"},
            )

        except Exception as e:
            self.logger.error(f"創建 3x3 熱力圖失敗: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return html.Div(
                html.P(f"生成圖表失敗: {str(e)}", className="text-danger"),
                className="text-center",
            )

