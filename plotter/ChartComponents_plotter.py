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
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np

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
        
    def create_equity_chart(self, equity_data: Dict[str, pd.DataFrame], 
                           selected_params: List[str], 
                           max_lines: int = 20) -> dict:
        """
        創建權益曲線圖表
        
        Args:
            equity_data: 權益曲線數據字典
            selected_params: 選中的參數組合列表
            max_lines: 最大顯示線數，預設為 20
            
        Returns:
            dict: Plotly 圖表配置
        """
        try:
            import plotly.graph_objs as go
            
            fig = go.Figure()
            
            # 限制顯示線數
            display_params = selected_params[:max_lines]
            
            # 添加權益曲線
            for i, param_key in enumerate(display_params):
                if param_key in equity_data:
                    df = equity_data[param_key]
                    if not df.empty and 'Time' in df.columns and 'Equity_value' in df.columns:
                        # 生成顏色
                        color = self._get_color(i, len(display_params))
                        
                        fig.add_trace(
                            go.Scatter(
                                x=df['Time'],
                                y=df['Equity_value'],
                                mode='lines',
                                name=param_key,
                                line=dict(width=1, color=color),
                                hovertemplate='<b>%{fullData.name}</b><br>' +
                                            '時間: %{x}<br>' +
                                            '權益值: %{y:,.2f}<br>' +
                                            '<extra></extra>'
                            )
                        )
            
            # 更新布局
            fig.update_layout(
                title="權益曲線比較",
                xaxis_title="時間",
                yaxis_title="權益值",
                template="plotly_dark",
                height=500,
                showlegend=True,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                ),
                hovermode='x unified',
                margin=dict(l=50, r=50, t=50, b=50)
            )
            
            return fig.to_dict()
            
        except Exception as e:
            self.logger.error(f"創建權益曲線圖表失敗: {e}")
            return {}
    
    def create_performance_comparison_chart(self, metrics_data: Dict[str, Any], 
                                          selected_params: List[str]) -> dict:
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
                rows=2, cols=2,
                subplot_titles=('年化回報率', '夏普比率', '最大回撤', '勝率'),
                specs=[[{"secondary_y": False}, {"secondary_y": False}],
                       [{"secondary_y": False}, {"secondary_y": False}]]
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
                    annual_returns.append(metrics.get('annualized_return', 0))
                    sharpe_ratios.append(metrics.get('sharpe_ratio', 0))
                    max_drawdowns.append(abs(metrics.get('max_drawdown', 0)))
                    win_rates.append(metrics.get('win_rate', 0))
            
            # 添加柱狀圖
            colors = [self._get_color(i, len(param_names)) for i in range(len(param_names))]
            
            fig.add_trace(
                go.Bar(x=param_names, y=annual_returns, name='年化回報率', 
                      marker_color=colors, showlegend=False),
                row=1, col=1
            )
            
            fig.add_trace(
                go.Bar(x=param_names, y=sharpe_ratios, name='夏普比率', 
                      marker_color=colors, showlegend=False),
                row=1, col=2
            )
            
            fig.add_trace(
                go.Bar(x=param_names, y=max_drawdowns, name='最大回撤', 
                      marker_color=colors, showlegend=False),
                row=2, col=1
            )
            
            fig.add_trace(
                go.Bar(x=param_names, y=win_rates, name='勝率', 
                      marker_color=colors, showlegend=False),
                row=2, col=2
            )
            
            # 更新布局
            fig.update_layout(
                title="績效指標比較",
                template="plotly_dark",
                height=600,
                showlegend=False,
                margin=dict(l=50, r=50, t=50, b=50)
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
                param_dict = param_data.get('parameters', {})
                for key, value in param_dict.items():
                    if key not in param_distribution:
                        param_distribution[key] = {}
                    value_str = str(value)
                    param_distribution[key][value_str] = param_distribution[key].get(value_str, 0) + 1
            
            # 創建子圖
            num_params = len(param_distribution)
            if num_params == 0:
                return {}
            
            cols = min(3, num_params)
            rows = (num_params + cols - 1) // cols
            
            fig = make_subplots(
                rows=rows, cols=cols,
                subplot_titles=list(param_distribution.keys()),
                specs=[[{"type": "pie"} for _ in range(cols)] for _ in range(rows)]
            )
            
            # 添加餅圖
            for i, (param_name, param_values) in enumerate(param_distribution.items()):
                row = i // cols + 1
                col = i % cols + 1
                
                labels = list(param_values.keys())
                values = list(param_values.values())
                
                fig.add_trace(
                    go.Pie(
                        labels=labels,
                        values=values,
                        name=param_name,
                        showlegend=False
                    ),
                    row=row, col=col
                )
            
            # 更新布局
            fig.update_layout(
                title="參數分布",
                template="plotly_dark",
                height=300 * rows,
                showlegend=False,
                margin=dict(l=50, r=50, t=50, b=50)
            )
            
            return fig.to_dict()
            
        except Exception as e:
            self.logger.error(f"創建參數分布圖表失敗: {e}")
            return {}
    
    def create_drawdown_chart(self, equity_data: Dict[str, pd.DataFrame], 
                             selected_params: List[str]) -> dict:
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
            
            # 計算回撤
            for i, param_key in enumerate(selected_params):
                if param_key in equity_data:
                    df = equity_data[param_key]
                    if not df.empty and 'Time' in df.columns and 'Equity_value' in df.columns:
                        # 計算回撤
                        drawdown = self._calculate_drawdown(df['Equity_value'])
                        
                        # 生成顏色
                        color = self._get_color(i, len(selected_params))
                        
                        fig.add_trace(
                            go.Scatter(
                                x=df['Time'],
                                y=drawdown * 100,  # 轉換為百分比
                                mode='lines',
                                name=f"{param_key} 回撤",
                                line=dict(width=1, color=color),
                                fill='tonexty',
                                fillcolor=color,
                                opacity=0.3,
                                hovertemplate='<b>%{fullData.name}</b><br>' +
                                            '時間: %{x}<br>' +
                                            '回撤: %{y:.2f}%<br>' +
                                            '<extra></extra>'
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
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                ),
                hovermode='x unified'
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
                '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
                '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
                '#aec7e8', '#ffbb78', '#98df8a', '#ff9896', '#c5b0d5',
                '#c49c94', '#f7b6d2', '#c7c7c7', '#dbdb8d', '#9edae5'
            ]
            
            if index < len(colors):
                return colors[index]
            else:
                # 如果超出預定義顏色，生成隨機顏色
                import random
                return f"rgb({random.randint(0, 255)}, {random.randint(0, 255)}, {random.randint(0, 255)})"
                
        except Exception as e:
            self.logger.error(f"生成顏色失敗: {e}")
            return '#1f77b4'  # 預設顏色 