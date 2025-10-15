"""
ChartComponents_plotter.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 可視化平台的圖表組件工具，負責生成各種圖表組件，包括權益曲線圖、績效比較圖、參數分布圖等。

【流程與數據流】
------------------------------------------------------------
- 被 DashboardGenerator 和 CallbackHandler 調用，負責圖表生成
- 主要數據流：

```mermaid
flowchart TD
    A[ChartComponents] -->|接收| B[圖表數據]
    B -->|處理| C[數據格式化]
    C -->|生成| D[Plotly圖表]
    D -->|返回| E[圖表組件]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增圖表類型、樣式時，請同步更新頂部註解與對應模組
- 若圖表配置有變動，需同步更新調用模組
- 圖表配置和樣式設置需要特別注意一致性

【常見易錯點】
------------------------------------------------------------
- 數據格式不正確
- 圖表配置錯誤
- 樣式設置不當
- 記憶體使用過大

【範例】
------------------------------------------------------------
- 基本使用：components = ChartComponents()
- 生成圖表：chart = components.create_equity_chart(data)

【與其他模組的關聯】
------------------------------------------------------------
- 被 DashboardGenerator 和 CallbackHandler 調用
- 依賴 Plotly 圖表庫
- 輸出圖表組件供界面顯示

【參考】
------------------------------------------------------------
- 詳細流程規範如有變動，請同步更新本註解與 README
- 其他模組如有依賴本檔案的行為，請於對應模組頂部註解標明
- Plotly 圖表配置請參考 Plotly 官方文檔
"""

import logging
from typing import Any, Dict, List, Optional

import pandas as pd


class ChartComponents:
    """
    圖表組件生成器

    負責生成各種圖表組件，
    包括權益曲線圖、績效比較圖、參數分布圖等。
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        初始化圖表組件生成器

        Args:
            logger: 日誌記錄器，預設為 None
        """
        self.logger = logger or logging.getLogger(__name__)

        # 初始化緩存
        self.drawdown_cache = {}
        self.sampled_data_cache = {}

    def _smart_sample_data(
        self, df: pd.DataFrame, max_points: int = 2000
    ) -> pd.DataFrame:
        """
        智能採樣數據，保留關鍵轉折點

        Args:
            df: 原始數據框
            max_points: 最大保留點數

        Returns:
            pd.DataFrame: 採樣後的數據框
        """
        try:
            if len(df) <= max_points:
                return df

            # 如果數據量過大，使用智能採樣
            if len(df) > max_points * 2:
                # 使用Douglas-Peucker算法的簡化版本
                return self._douglas_peucker_sampling(df, max_points)
            else:
                # 簡單等間隔採樣
                step = len(df) // max_points
                return df.iloc[::step].copy()

        except Exception as e:
            self.logger.error(f"智能採樣失敗: {e}")
            return df

    def _douglas_peucker_sampling(
        self, df: pd.DataFrame, max_points: int
    ) -> pd.DataFrame:
        """
        Douglas-Peucker算法採樣，保留重要轉折點

        Args:
            df: 原始數據框
            max_points: 最大保留點數

        Returns:
            pd.DataFrame: 採樣後的數據框
        """
        try:
            if "Equity_value" not in df.columns or len(df) <= max_points:
                return df

            # 簡化版Douglas-Peucker算法
            equity_values = df["Equity_value"].values
            indices = [0, len(df) - 1]  # 保留首尾點

            # 遞歸尋找重要轉折點
            def find_important_points(start_idx, end_idx, tolerance=0.001):
                if end_idx - start_idx <= 1:
                    return

                start_val = equity_values[start_idx]
                end_val = equity_values[end_idx]

                # 計算直線方程
                if end_idx != start_idx:
                    slope = (end_val - start_val) / (end_idx - start_idx)

                    # 找到距離直線最遠的點
                    max_distance = 0
                    max_idx = start_idx

                    for i in range(start_idx + 1, end_idx):
                        expected_val = start_val + slope * (i - start_idx)
                        distance = abs(equity_values[i] - expected_val) / (
                            abs(start_val) + 1e-8
                        )

                        if distance > max_distance:
                            max_distance = distance
                            max_idx = i

                    # 如果距離超過閾值，保留該點
                    if max_distance > tolerance:
                        indices.append(max_idx)
                        find_important_points(start_idx, max_idx, tolerance)
                        find_important_points(max_idx, end_idx, tolerance)

            # 執行採樣
            find_important_points(0, len(df) - 1)
            indices = sorted(list(set(indices)))

            # 如果點數仍然過多，進一步採樣
            if len(indices) > max_points:
                step = len(indices) // max_points
                indices = indices[::step]

            return df.iloc[indices].copy()

        except Exception as e:
            self.logger.error(f"Douglas-Peucker採樣失敗: {e}")
            # 降級到簡單採樣
            step = len(df) // max_points
            return df.iloc[::step].copy()

    def _get_cached_drawdown(
        self, param_key: str, equity_series: pd.Series
    ) -> pd.Series:
        """
        獲取緩存的回撤計算結果

        Args:
            param_key: 參數鍵
            equity_series: 權益序列

        Returns:
            pd.Series: 回撤序列
        """
        try:
            # 創建緩存鍵
            cache_key = f"{param_key}_{len(equity_series)}_{hash(str(equity_series.iloc[::100]))}"

            if cache_key in self.drawdown_cache:
                return self.drawdown_cache[cache_key]

            # 計算回撤
            drawdown = self._calculate_drawdown(equity_series)

            # 存入緩存
            self.drawdown_cache[cache_key] = drawdown

            # 清理緩存（防止內存過大）
            if len(self.drawdown_cache) > 100:
                # 保留最新的50個
                keys_to_keep = list(self.drawdown_cache.keys())[-50:]
                self.drawdown_cache = {k: self.drawdown_cache[k] for k in keys_to_keep}

            return drawdown

        except Exception as e:
            self.logger.error(f"獲取緩存回撤失敗: {e}")
            return self._calculate_drawdown(equity_series)

    def _optimize_data_for_chart(
        self, equity_data: Dict[str, pd.DataFrame], selected_params: List[str]
    ) -> Dict[str, pd.DataFrame]:
        """
        優化數據用於圖表顯示

        Args:
            equity_data: 原始權益數據
            selected_params: 選中的參數組合

        Returns:
            Dict[str, pd.DataFrame]: 優化後的數據
        """
        try:
            optimized_data = {}

            for param_key in selected_params:
                if param_key in equity_data:
                    df = equity_data[param_key]
                    if (
                        not df.empty
                        and "Time" in df.columns
                        and "Equity_value" in df.columns
                    ):
                        # 根據數據量智能採樣
                        if len(df) > 10000:
                            # 大量數據：採樣到2000點
                            df = self._smart_sample_data(df, 2000)
                        elif len(df) > 5000:
                            # 中等數據：採樣到1500點
                            df = self._smart_sample_data(df, 1500)
                        elif len(df) > 2000:
                            # 較少數據：採樣到1000點
                            df = self._smart_sample_data(df, 1000)

                        optimized_data[param_key] = df

            return optimized_data

        except Exception as e:
            self.logger.error(f"優化數據失敗: {e}")
            return equity_data

    def create_equity_chart(
        self,
        equity_data: Dict[str, pd.DataFrame],
        selected_params: List[str],
        max_lines: int = 20,
        bah_data: Optional[Dict[str, pd.DataFrame]] = None,
        parameters: Optional[List[Dict[str, Any]]] = None,
        is_callback_mode: bool = False,
    ) -> dict:
        """
        創建權益曲線圖表（統一智能版本）

        Args:
            equity_data: 權益曲線數據字典
            selected_params: 選中的參數組合列表
            max_lines: 最大顯示線數，預設為 20
            bah_data: BAH權益曲線數據字典（可選）
            parameters: 參數信息列表，用於獲取資產信息（可選）
            is_callback_mode: 是否為CallbackHandler模式（自動檢測數據格式）

        Returns:
            dict: Plotly 圖表配置
        """
        try:
            import plotly.graph_objs as go

            fig = go.Figure()

            # 限制顯示線數
            display_params = selected_params[:max_lines]

            # 🚀 優化：預先優化數據
            optimized_data = self._optimize_data_for_chart(equity_data, display_params)

            # 記錄已繪製的BAH資產，避免重複
            instrument_bah = {}

            # 智能檢測數據格式並處理
            if is_callback_mode or (
                parameters and any("Asset" in str(p) for p in parameters)
            ):
                # CallbackHandler模式：使用backtest_id作為key
                for i, param_key in enumerate(display_params):
                    if param_key in optimized_data:
                        df = optimized_data[param_key]
                        if (
                            not df.empty
                            and "Time" in df.columns
                            and "Equity_value" in df.columns
                        ):
                            # 生成顏色
                            color = self._get_color(i, len(display_params))

                            fig.add_trace(
                                go.Scatter(
                                    x=pd.to_datetime(df["Time"]),
                                    y=df["Equity_value"],
                                    mode="lines",
                                    name=str(param_key),  # 直接使用backtest_id
                                    line=dict(width=1, color=color),
                                    customdata=[param_key] * len(df),
                                    hovertemplate="<b>%{fullData.name}</b><br>"
                                    + "時間: %{x}<br>"
                                    + "權益值: %{y:,.2f}<br>"
                                    + "<extra></extra>",
                                )
                            )

                            # 添加對應的BAH曲線（如果提供）
                            if bah_data and parameters:
                                param = next(
                                    (
                                        p
                                        for p in parameters
                                        if p.get("Backtest_id") == param_key
                                    ),
                                    None,
                                )
                                if param:
                                    instrument = param.get("Asset", None)
                                    if instrument and instrument not in instrument_bah:
                                        bah_df = bah_data.get(param_key)
                                        if (
                                            bah_df is not None
                                            and not bah_df.empty
                                            and "Time" in bah_df.columns
                                            and "BAH_Equity" in bah_df.columns
                                        ):
                                            fig.add_trace(
                                                go.Scatter(
                                                    x=pd.to_datetime(bah_df["Time"]),
                                                    y=bah_df["BAH_Equity"],
                                                    mode="lines",
                                                    name="Benchmark",
                                                    line=dict(
                                                        dash="dot", color="#ecbc4f"
                                                    ),
                                                    hovertemplate="<b>%{fullData.name}</b><br>"
                                                    + "時間: %{x}<br>"
                                                    + "BAH權益值: %{y:,.2f}<br>"
                                                    + "<extra></extra>",
                                                )
                                            )
                                            instrument_bah[instrument] = True
            else:
                # 通用模式：使用參數組合作為key
                for i, param_key in enumerate(display_params):
                    if param_key in optimized_data:
                        df = optimized_data[param_key]
                        if (
                            not df.empty
                            and "Time" in df.columns
                            and "Equity_value" in df.columns
                        ):
                            # 生成顏色
                            color = self._get_color(i, len(display_params))

                            fig.add_trace(
                                go.Scatter(
                                    x=pd.to_datetime(df["Time"]),
                                    y=df["Equity_value"],
                                    mode="lines",
                                    name=f"{param_key} (策略)",  # 添加策略標識
                                    line=dict(width=1, color=color),
                                    customdata=[param_key] * len(df),
                                    hovertemplate="<b>%{fullData.name}</b><br>"
                                    + "時間: %{x}<br>"
                                    + "權益值: %{y:,.2f}<br>"
                                    + "<extra></extra>",
                                )
                            )

                            # 添加對應的BAH曲線（如果提供）
                            if bah_data and parameters:
                                param = next(
                                    (
                                        p
                                        for p in parameters
                                        if p.get("Backtest_id") == param_key
                                    ),
                                    None,
                                )
                                if param:
                                    instrument = param.get("Asset", None)
                                    if instrument and instrument not in instrument_bah:
                                        bah_df = bah_data.get(param_key)
                                        if (
                                            bah_df is not None
                                            and not bah_df.empty
                                            and "Time" in bah_df.columns
                                            and "BAH_Equity" in bah_df.columns
                                        ):
                                            fig.add_trace(
                                                go.Scatter(
                                                    x=pd.to_datetime(bah_df["Time"]),
                                                    y=bah_df["BAH_Equity"],
                                                    mode="lines",
                                                    name="Benchmark",
                                                    line=dict(
                                                        dash="dot", color="#ecbc4f"
                                                    ),
                                                    hovertemplate="<b>%{fullData.name}</b><br>"
                                                    + "時間: %{x}<br>"
                                                    + "BAH權益值: %{y:,.2f}<br>"
                                                    + "<extra></extra>",
                                                )
                                            )
                                            instrument_bah[instrument] = True

            # 更新布局（統一使用DashboardGenerator的樣式）
            fig.update_layout(
                title=dict(
                    text="權益曲線比較",
                    font=dict(color="#ecbc4f", size=18),  # 標題使用主題色
                ),
                xaxis_title=dict(
                    text="時間",
                    font=dict(color="#ecbc4f", size=15),  # X軸標題使用主題色
                ),
                yaxis_title=dict(
                    text="權益值",
                    font=dict(color="#ecbc4f", size=15),  # Y軸標題使用主題色
                ),
                template=None,  # 不使用預設模板
                height=1000,  # 統一使用1000高度
                showlegend=True,
                plot_bgcolor="#181818",
                paper_bgcolor="#181818",
                font=dict(color="#f5f5f5", size=15),
                legend=dict(
                    font=dict(color="#ecbc4f", size=13),
                    orientation="v",  # 垂直圖例
                    yanchor="top",
                    y=1,
                    xanchor="left",
                    x=1.02,
                ),
                xaxis=dict(
                    color="#ecbc4f",  # X軸刻度使用主題色
                    gridcolor="#444",
                    title_font=dict(color="#ecbc4f", size=15),  # X軸標題字體
                    tickfont=dict(color="#ecbc4f", size=12),  # X軸刻度字體
                ),
                yaxis=dict(
                    color="#ecbc4f",  # Y軸刻度使用主題色
                    gridcolor="#444",
                    title_font=dict(color="#ecbc4f", size=15),  # Y軸標題字體
                    tickfont=dict(color="#ecbc4f", size=12),  # Y軸刻度字體
                ),
                hovermode="x unified",
                margin=dict(l=50, r=50, t=50, b=50),
            )

            return fig.to_dict()

        except Exception as e:
            self.logger.error(f"創建權益曲線圖表失敗: {e}")
            return {}

    def create_equity_chart_for_callback(
        self,
        equity_curves: Dict[str, pd.DataFrame],
        bah_curves: Dict[str, pd.DataFrame],
        filtered_ids: List[str],
        parameters: List[Dict[str, Any]],
    ) -> dict:
        """
        為CallbackHandler創建權益曲線圖表（特殊版本）

        Args:
            equity_curves: 權益曲線數據字典，key為backtest_id
            bah_curves: BAH權益曲線數據字典，key為backtest_id
            filtered_ids: 過濾後的backtest_id列表
            parameters: 參數信息列表

        Returns:
            dict: Plotly 圖表配置
        """
        try:
            import plotly.graph_objs as go

            fig = go.Figure()
            instrument_bah = {}

            # 添加策略權益曲線
            for idx, bid in enumerate(filtered_ids):
                df = equity_curves.get(bid)
                if (
                    df is not None
                    and not df.empty
                    and "Time" in df.columns
                    and "Equity_value" in df.columns
                ):
                    # 🚀 優化：對數據進行採樣
                    if len(df) > 2000:
                        df = self._smart_sample_data(df, 2000)

                    fig.add_trace(
                        go.Scatter(
                            x=pd.to_datetime(df["Time"]),
                            y=df["Equity_value"],
                            mode="lines",
                            name=str(bid),
                            line=dict(width=1),  # 統一線條寬度
                            customdata=[bid] * len(df),
                            hovertemplate="<b>%{fullData.name}</b><br>"
                            + "時間: %{x}<br>"
                            + "權益值: %{y:,.2f}<br>"
                            + "<extra></extra>",
                        )
                    )

                # 畫 BAH 曲線（每種 instrument 只畫一條）
                param = next(
                    (p for p in parameters if p.get("Backtest_id") == bid), None
                )
                if param:
                    instrument = param.get("Asset", None)
                    if instrument and instrument not in instrument_bah:
                        bah_df = bah_curves.get(bid)
                        if (
                            bah_df is not None
                            and not bah_df.empty
                            and "Time" in bah_df.columns
                            and "BAH_Equity" in bah_df.columns
                        ):
                            # 🚀 優化：對BAH數據進行採樣
                            if len(bah_df) > 2000:
                                bah_df = self._smart_sample_data(bah_df, 2000)

                            fig.add_trace(
                                go.Scatter(
                                    x=pd.to_datetime(bah_df["Time"]),
                                    y=bah_df["BAH_Equity"],
                                    mode="lines",
                                    name="Benchmark",
                                    line=dict(dash="dot", color="#ecbc4f"),
                                    hovertemplate="<b>%{fullData.name}</b><br>"
                                    + "時間: %{x}<br>"
                                    + "BAH權益值: %{y:,.2f}<br>"
                                    + "<extra></extra>",
                                )
                            )
                            instrument_bah[instrument] = True

            # 更新布局（統一使用DashboardGenerator的樣式）
            fig.update_layout(
                title=dict(
                    text="權益曲線比較",
                    font=dict(color="#ecbc4f", size=18),  # 標題使用主題色
                ),
                xaxis_title=dict(
                    text="時間",
                    font=dict(color="#ecbc4f", size=15),  # X軸標題使用主題色
                ),
                yaxis_title=dict(
                    text="權益值",
                    font=dict(color="#ecbc4f", size=15),  # Y軸標題使用主題色
                ),
                template=None,
                height=1000,  # 恢復原始高度
                showlegend=True,
                plot_bgcolor="#181818",
                paper_bgcolor="#181818",
                font=dict(color="#f5f5f5", size=15),
                legend=dict(
                    font=dict(color="#ecbc4f", size=13),
                    orientation="v",  # 垂直圖例
                    yanchor="top",
                    y=1,
                    xanchor="left",
                    x=1.02,
                ),
                xaxis=dict(
                    color="#ecbc4f",  # X軸刻度使用主題色
                    gridcolor="#444",
                    title_font=dict(color="#ecbc4f", size=15),  # X軸標題字體
                    tickfont=dict(color="#ecbc4f", size=12),  # X軸刻度字體
                ),
                yaxis=dict(
                    color="#ecbc4f",  # Y軸刻度使用主題色
                    gridcolor="#444",
                    title_font=dict(color="#ecbc4f", size=15),  # Y軸標題字體
                    tickfont=dict(color="#ecbc4f", size=12),  # Y軸刻度字體
                ),
                margin=dict(l=50, r=50, t=50, b=50),  # 添加邊距
            )

            return fig.to_dict()

        except Exception as e:
            self.logger.error(f"為CallbackHandler創建權益曲線圖表失敗: {e}")
            return {}

    def get_optimization_stats(self) -> Dict[str, Any]:
        """
        獲取優化統計信息

        Returns:
            Dict[str, Any]: 優化統計信息
        """
        try:
            return {
                "drawdown_cache_size": len(self.drawdown_cache),
                "sampled_data_cache_size": len(self.sampled_data_cache),
                "total_cache_entries": len(self.drawdown_cache)
                + len(self.sampled_data_cache),
            }
        except Exception as e:
            self.logger.error(f"獲取優化統計失敗: {e}")
            return {}

    def create_performance_comparison_chart(
        self, metrics_data: Dict[str, Any], selected_params: List[str]
    ) -> dict:
        """
        創建績效比較圖表

        Args:
            metrics_data: 績效指標數據字典
            selected_params: 選中的參數組合列表

        Returns:
            dict: Plotly 圖表配置
        """
        try:
            import plotly.graph_objs as go
            from plotly.subplots import make_subplots

            # 創建子圖
            fig = make_subplots(
                rows=2,
                cols=2,
                subplot_titles=("年化回報率", "夏普比率", "最大回撤", "勝率"),
                specs=[
                    [{"secondary_y": False}, {"secondary_y": False}],
                    [{"secondary_y": False}, {"secondary_y": False}],
                ],
            )

            # 準備數據
            param_names = []
            annual_returns = []
            sharpe_ratios = []
            max_drawdowns = []
            win_rates = []

            for param_key in selected_params:
                if param_key in metrics_data:
                    metrics = metrics_data[param_key]
                    param_names.append(param_key)
                    annual_returns.append(metrics.get("annualized_return", 0))
                    sharpe_ratios.append(metrics.get("sharpe_ratio", 0))
                    max_drawdowns.append(abs(metrics.get("max_drawdown", 0)))
                    win_rates.append(metrics.get("win_rate", 0))

            # 添加柱狀圖
            colors = [
                self._get_color(i, len(param_names)) for i in range(len(param_names))
            ]

            fig.add_trace(
                go.Bar(
                    x=param_names,
                    y=annual_returns,
                    name="年化回報率",
                    marker_color=colors,
                    showlegend=False,
                ),
                row=1,
                col=1,
            )

            fig.add_trace(
                go.Bar(
                    x=param_names,
                    y=sharpe_ratios,
                    name="夏普比率",
                    marker_color=colors,
                    showlegend=False,
                ),
                row=1,
                col=2,
            )

            fig.add_trace(
                go.Bar(
                    x=param_names,
                    y=max_drawdowns,
                    name="最大回撤",
                    marker_color=colors,
                    showlegend=False,
                ),
                row=2,
                col=1,
            )

            fig.add_trace(
                go.Bar(
                    x=param_names,
                    y=win_rates,
                    name="勝率",
                    marker_color=colors,
                    showlegend=False,
                ),
                row=2,
                col=2,
            )

            # 更新布局
            fig.update_layout(
                title="績效指標比較",
                template="plotly_dark",
                height=600,
                showlegend=False,
                margin=dict(l=50, r=50, t=50, b=50),
            )

            # 更新軸標籤
            fig.update_xaxes(title_text="參數組合", row=1, col=1)
            fig.update_xaxes(title_text="參數組合", row=1, col=2)
            fig.update_xaxes(title_text="參數組合", row=2, col=1)
            fig.update_xaxes(title_text="參數組合", row=2, col=2)

            fig.update_yaxes(title_text="年化回報率 (%)", row=1, col=1)
            fig.update_yaxes(title_text="夏普比率", row=1, col=2)
            fig.update_yaxes(title_text="最大回撤 (%)", row=2, col=1)
            fig.update_yaxes(title_text="勝率 (%)", row=2, col=2)

            return fig.to_dict()

        except Exception as e:
            self.logger.error(f"創建績效比較圖表失敗: {e}")
            return {}

    def create_parameter_distribution_chart(self, parameters: Dict[str, Any]) -> dict:
        """
        創建參數分布圖表

        Args:
            parameters: 參數數據字典

        Returns:
            dict: Plotly 圖表配置
        """
        try:
            import plotly.graph_objs as go
            from plotly.subplots import make_subplots

            # 統計參數分布
            param_distribution = {}
            for param_key, param_data in parameters.items():
                param_dict = param_data.get("parameters", {})
                for key, value in param_dict.items():
                    if key not in param_distribution:
                        param_distribution[key] = {}
                    value_str = str(value)
                    param_distribution[key][value_str] = (
                        param_distribution[key].get(value_str, 0) + 1
                    )

            # 創建子圖
            num_params = len(param_distribution)
            if num_params == 0:
                return {}

            cols = min(3, num_params)
            rows = (num_params + cols - 1) // cols

            fig = make_subplots(
                rows=rows,
                cols=cols,
                subplot_titles=list(param_distribution.keys()),
                specs=[[{"type": "pie"} for _ in range(cols)] for _ in range(rows)],
            )

            # 添加餅圖
            for i, (param_name, param_values) in enumerate(param_distribution.items()):
                row = i // cols + 1
                col = i % cols + 1

                labels = list(param_values.keys())
                values = list(param_values.values())

                fig.add_trace(
                    go.Pie(
                        labels=labels, values=values, name=param_name, showlegend=False
                    ),
                    row=row,
                    col=col,
                )

            # 更新布局
            fig.update_layout(
                title="參數分布",
                template="plotly_dark",
                height=300 * rows,
                showlegend=False,
                margin=dict(l=50, r=50, t=50, b=50),
            )

            return fig.to_dict()

        except Exception as e:
            self.logger.error(f"創建參數分布圖表失敗: {e}")
            return {}

    def create_drawdown_chart(
        self, equity_data: Dict[str, pd.DataFrame], selected_params: List[str]
    ) -> dict:
        """
        創建回撤圖表

        Args:
            equity_data: 權益曲線數據字典
            selected_params: 選中的參數組合列表

        Returns:
            dict: Plotly 圖表配置
        """
        try:
            import plotly.graph_objs as go

            fig = go.Figure()

            # 🚀 優化：預先優化數據並使用緩存
            optimized_data = self._optimize_data_for_chart(equity_data, selected_params)

            # 計算回撤
            for i, param_key in enumerate(selected_params):
                if param_key in optimized_data:
                    df = optimized_data[param_key]
                    if (
                        not df.empty
                        and "Time" in df.columns
                        and "Equity_value" in df.columns
                    ):
                        # 🚀 優化：使用緩存計算回撤
                        drawdown = self._get_cached_drawdown(
                            param_key, df["Equity_value"]
                        )

                        # 生成顏色
                        color = self._get_color(i, len(selected_params))

                        fig.add_trace(
                            go.Scatter(
                                x=df["Time"],
                                y=drawdown * 100,  # 轉換為百分比
                                mode="lines",
                                name=f"{param_key} 回撤",
                                line=dict(width=1, color=color),
                                fill="tonexty",
                                fillcolor=color,
                                opacity=0.3,
                                hovertemplate="<b>%{fullData.name}</b><br>"
                                + "時間: %{x}<br>"
                                + "回撤: %{y:.2f}%<br>"
                                + "<extra></extra>",
                            )
                        )

            # 更新布局
            fig.update_layout(
                title="回撤分析",
                xaxis_title="時間",
                yaxis_title="回撤 (%)",
                template="plotly_dark",
                height=400,
                showlegend=True,
                legend=dict(
                    orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
                ),
                hovermode="x unified",
            )

            return fig.to_dict()

        except Exception as e:
            self.logger.error(f"創建回撤圖表失敗: {e}")
            return {}

    def _calculate_drawdown(self, equity_series: pd.Series) -> pd.Series:
        """
        計算回撤序列

        Args:
            equity_series: 權益序列

        Returns:
            pd.Series: 回撤序列
        """
        try:
            # 計算累積最大值
            running_max = equity_series.expanding().max()

            # 計算回撤
            drawdown = (equity_series - running_max) / running_max

            return drawdown

        except Exception as e:
            self.logger.error(f"計算回撤失敗: {e}")
            return pd.Series([0] * len(equity_series))

    def _get_color(self, index: int, total: int) -> str:
        """
        根據索引生成顏色

        Args:
            index: 顏色索引
            total: 總數量

        Returns:
            str: 顏色字符串
        """
        try:
            # 預定義顏色列表
            colors = [
                "#1f77b4",
                "#ff7f0e",
                "#2ca02c",
                "#d62728",
                "#9467bd",
                "#8c564b",
                "#e377c2",
                "#7f7f7f",
                "#bcbd22",
                "#17becf",
                "#aec7e8",
                "#ffbb78",
                "#98df8a",
                "#ff9896",
                "#c5b0d5",
                "#c49c94",
                "#f7b6d2",
                "#c7c7c7",
                "#dbdb8d",
                "#9edae5",
            ]

            if index < len(colors):
                return colors[index]
            else:
                # 如果超出預定義顏色，生成隨機顏色
                import random

                return f"rgb({random.randint(0, 255)}, {random.randint(0, 255)}, {random.randint(0, 255)})"

        except Exception as e:
            self.logger.error(f"生成顏色失敗: {e}")
            return "#1f77b4"  # 預設顏色
