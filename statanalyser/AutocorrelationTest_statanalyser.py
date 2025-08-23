"""
AutocorrelationTest_statanalyser.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 統計分析模組，負責對時序數據進行自相關檢定（如 ACF、PACF、Durbin-Watson、Ljung-Box 等），評估殘差或報酬率的自相關性與週期性，輔助模型選擇與診斷。

【流程與數據流】
------------------------------------------------------------
- 繼承 Base_statanalyser，作為統計分析子類之一
- 檢定結果傳遞給 ReportGenerator 或下游模組

```mermaid
flowchart TD
    A[AutocorrelationTest] -->|檢定結果| B[ReportGenerator/下游模組]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增/修改檢定類型、參數、圖表邏輯時，請同步更新頂部註解與下游流程
- 若介面、欄位、分析流程有變動，需同步更新本檔案與 Base_statanalyser
- 統計結果格式如有調整，請同步通知協作者

【常見易錯點】
------------------------------------------------------------
- 檢定參數設置錯誤或數據點不足會導致結果異常
- 頻率設定不符或欄位型態錯誤會影響分析正確性
- 統計結果格式不符會影響下游報表或流程

【範例】
------------------------------------------------------------
- test = AutocorrelationTest(data, predictor_col, return_col, freq='D')
  result = test.analyze()

【與其他模組的關聯】
------------------------------------------------------------
- 繼承 Base_statanalyser，檢定結果傳遞給 ReportGenerator 或下游模組
- 需與 ReportGenerator、主流程等下游結構保持一致

【參考】
------------------------------------------------------------
- statsmodels、plotly 官方文件
- Base_statanalyser.py、ReportGenerator_statanalyser.py
- 專案 README
"""

from typing import Dict

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from rich.console import Console
from rich.panel import Panel
from statsmodels.tsa.stattools import acf, pacf

from .Base_statanalyser import BaseStatAnalyser


class AutocorrelationTest(BaseStatAnalyser):
    """自相關性檢驗模組，檢測序列的記憶效應和週期性"""

    def __init__(
        self, data: pd.DataFrame, predictor_col: str, return_col: str, freq: str = "D"
    ):
        super().__init__(data, predictor_col, return_col)
        self.freq = freq.upper()
        if self.freq not in ["D", "H", "T"]:
            print(f"警告：未知頻率 {self.freq}，使用預設 'D'")
            self.freq = "D"

    def analyze(self) -> Dict:
        """執行 ACF 和 PACF 分析"""
        console = Console()
        series = self.data[self.predictor_col].dropna()
        if len(series) < 5:
            print(f"3. 檢驗結果：數據點不足（{len(series)}個）")
            return {"success": False, "acf_lags": [], "pacf_lags": []}
        # 設置滯後期數
        lags = {
            "D": min(60, len(series) // 2),
            "H": min(24, len(series) // 2),
            "T": min(120, len(series) // 2),
        }.get(self.freq, min(20, len(series) // 2))
        # 美化步驟說明 Panel
        panel_content = (
            "🟢 選擇用於統計分析的預測因子\n"
            "🟢 收益率相關性檢驗[自動]\n"
            "🟢 ADF/KPSS 平穩性檢驗[自動]\n"
            "🟢 ACF/PACF 自相關性檢驗[自動]\n"
            "🔴 生成 ACF 或 PACF 互動圖片\n"
            "🔴 統計分佈檢驗[自動]\n"
            "🔴 季節性檢驗[自動]\n\n"
            "[bold #dbac30]說明[/bold #dbac30]\n"
            f"3. '{self.predictor_col}' 自相關性檢驗（ACF 和 PACF）\n"
            "檢驗功能：檢測序列的記憶效應和週期性。如有記憶效應，代表可用歷史數據預測未來數值，用家可嘗試發掘背後原因是否具備邏輯。小心過擬合。\n"
            f"檢測最大滯後期數：{lags}（頻率={self.freq}）"
        )
        panel = Panel(
            panel_content,
            title="[bold #dbac30]🔬 統計分析 StatAnalyser 步驟：自相關性檢驗[自動][/bold #dbac30]",
            border_style="#dbac30",
        )
        console.print(panel)

        # 計算 ACF 和 PACF
        acf_result = acf(series, nlags=lags, alpha=0.05, fft=True)
        if isinstance(acf_result, tuple) and len(acf_result) >= 2:
            acf_vals, acf_conf = acf_result[:2]
        else:
            acf_vals = acf_result
            acf_conf = None

        pacf_result = pacf(series, nlags=lags, alpha=0.05)
        if isinstance(pacf_result, tuple) and len(pacf_result) >= 2:
            pacf_vals, pacf_conf = pacf_result[:2]
        else:
            pacf_vals = pacf_result
            pacf_conf = None

        # 顯著滯後期
        threshold = 1.96 / np.sqrt(len(series))
        acf_sig_lags = [i for i in range(1, lags + 1) if abs(acf_vals[i]) > threshold]
        pacf_sig_lags = [i for i in range(1, lags + 1) if abs(pacf_vals[i]) > threshold]

        # 統計結果表格
        from rich.table import Table

        # 主要統計指標表格
        stats_table = Table(
            title="自相關性統計指標", border_style="#dbac30", show_lines=True
        )
        stats_table.add_column("指標", style="bold white")
        stats_table.add_column("數值", style="bold white")
        stats_table.add_column("說明", style="bold white")

        # 計算主要統計指標
        acf_max = max(abs(acf_vals[1:])) if len(acf_vals) > 1 else 0
        pacf_max = max(abs(pacf_vals[1:])) if len(pacf_vals) > 1 else 0
        acf_max_lag = np.argmax(abs(acf_vals[1:])) + 1 if len(acf_vals) > 1 else 0
        pacf_max_lag = np.argmax(abs(pacf_vals[1:])) + 1 if len(pacf_vals) > 1 else 0

        stats_table.add_row(
            "數據點數", f"[bold #1e90ff]{len(series)}[/bold #1e90ff]", "有效數據點數量"
        )
        stats_table.add_row(
            "檢測滯後期",
            f"[bold #1e90ff]{lags}[/bold #1e90ff]",
            f"最大檢測滯後期（頻率={self.freq}）",
        )
        stats_table.add_row(
            "顯著性閾值",
            f"[bold #1e90ff]{threshold:.4f}[/bold #1e90ff]",
            "95% 置信區間閾值",
        )
        stats_table.add_row(
            "ACF 最大值",
            f"[bold #1e90ff]{acf_max:.4f}[/bold #1e90ff]",
            f"滯後期 {acf_max_lag}",
        )
        stats_table.add_row(
            "PACF 最大值",
            f"[bold #1e90ff]{pacf_max:.4f}[/bold #1e90ff]",
            f"滯後期 {pacf_max_lag}",
        )
        stats_table.add_row(
            "ACF 顯著期數",
            f"[bold #1e90ff]{len(acf_sig_lags)}[/bold #1e90ff]",
            f"超過閾值的滯後期數",
        )
        stats_table.add_row(
            "PACF 顯著期數",
            f"[bold #1e90ff]{len(pacf_sig_lags)}[/bold #1e90ff]",
            f"超過閾值的滯後期數",
        )

        console.print(stats_table)

        # 顯著滯後期詳細表格
        sig_table = Table(
            title="ACF/PACF 顯著滯後期詳細結果", border_style="#dbac30", show_lines=True
        )
        sig_table.add_column("類型", style="bold white")
        sig_table.add_column("顯著滯後期", style="bold white")
        sig_table.add_column("對應係數值", style="bold white")

        if acf_sig_lags:
            acf_values = [f"{acf_vals[lag]:.4f}" for lag in acf_sig_lags]
            sig_table.add_row(
                "ACF",
                f"[bold #1e90ff]{acf_sig_lags}[/bold #1e90ff]",
                f"[bold #1e90ff]{acf_values}[/bold #1e90ff]",
            )
        else:
            sig_table.add_row(
                "ACF",
                "[bold #1e90ff]無[/bold #1e90ff]",
                "[bold #1e90ff]無[/bold #1e90ff]",
            )

        if pacf_sig_lags:
            pacf_values = [f"{pacf_vals[lag]:.4f}" for lag in pacf_sig_lags]
            sig_table.add_row(
                "PACF",
                f"[bold #1e90ff]{pacf_sig_lags}[/bold #1e90ff]",
                f"[bold #1e90ff]{pacf_values}[/bold #1e90ff]",
            )
        else:
            sig_table.add_row(
                "PACF",
                "[bold #1e90ff]無[/bold #1e90ff]",
                "[bold #1e90ff]無[/bold #1e90ff]",
            )

        console.print(sig_table)

        # 詢問是否生成ACF和PACF圖片（美化步驟說明）
        panel_content = (
            "🟢 選擇用於統計分析的預測因子\n"
            "🟢 收益率相關性檢驗[自動]\n"
            "🟢 ADF/KPSS平穩性檢驗[自動]\n"
            "🟢 ACF/PACF 自相關性檢驗[自動]\n"
            "🟢 輸出 ACF 或 PACF 互動圖片\n"
            "🔴 統計分佈檢驗[自動]\n"
            "🔴 季節性檢驗[自動]\n\n"
            "[bold #dbac30]說明[/bold #dbac30]\n"
            "輸出 ACF 或 PACF 互動圖片\n"
            "ACF 分析因子在不同時間間隔（lag）下的過去和現在有多相關。如線高於灰色區域，代表在 lag 之前周期內的數據對最新數值有影響。\n"
            "PACF 分析因子在 lag 周期前的特定時間點對現在有多相關，去掉了中間的數值。\n"
            "如高於灰色區域，代表 lag 前的特定數值對現數值有較大影響。\n"
            "例子：\n"
            "都不顯著：網站訪客，隨機無規律。\n"
            "ACF顯著，PACF不顯著：氣溫，連續趨勢無單日主導。\n"
            "ACF不顯著，PACF顯著：股票交易量，突發事件短期影響。\n"
            "ACF顯著，PACF顯著：聖誕飾品銷售，趨勢+直接推動。\n"
        )
        panel = Panel(
            panel_content,
            title="[bold #dbac30]🔬 統計分析 StatAnalyser 步驟：ACF/PACF 圖片生成[互動][/bold #dbac30]",
            border_style="#dbac30",
        )
        console.print(panel)
        console.print(
            "[bold #dbac30]輸出 ACF 或 PACF 互動圖片？(輸入 y 生成，n 跳過，預設 n)[/bold #dbac30]"
        )
        generate_plots = console.input().strip().lower() or "n"
        generate_plots = generate_plots == "y"

        # 根據設定決定是否繪製圖表
        if generate_plots:
            print("正在生成 ACF 和 PACF 圖片...")
            # 繪製圖表
            fig = make_subplots(
                rows=2,
                cols=1,
                subplot_titles=(
                    f"ACF of {self.predictor_col}",
                    f"PACF of {self.predictor_col}",
                ),
            )
            fig.add_trace(
                go.Scatter(
                    x=list(range(lags + 1)),
                    y=acf_vals,
                    mode="lines+markers",
                    name="ACF",
                ),
                row=1,
                col=1,
            )
            if acf_conf is not None:
                fig.add_trace(
                    go.Scatter(
                        x=list(range(lags + 1)),
                        y=acf_conf[:, 0] - acf_vals,
                        line=dict(color="rgba(0,0,0,0)"),
                        showlegend=False,
                    ),
                    row=1,
                    col=1,
                )
                fig.add_trace(
                    go.Scatter(
                        x=list(range(lags + 1)),
                        y=acf_conf[:, 1] - acf_vals,
                        fill="tonexty",
                        line=dict(color="rgba(100,100,100,0.3)"),
                        name="95% CI",
                    ),
                    row=1,
                    col=1,
                )
            fig.add_trace(
                go.Scatter(
                    x=list(range(lags + 1)),
                    y=pacf_vals,
                    mode="lines+markers",
                    name="PACF",
                ),
                row=2,
                col=1,
            )
            if pacf_conf is not None:
                fig.add_trace(
                    go.Scatter(
                        x=list(range(lags + 1)),
                        y=pacf_conf[:, 0] - pacf_vals,
                        line=dict(color="rgba(0,0,0,0)"),
                        showlegend=False,
                    ),
                    row=2,
                    col=1,
                )
                fig.add_trace(
                    go.Scatter(
                        x=list(range(lags + 1)),
                        y=pacf_conf[:, 1] - pacf_vals,
                        fill="tonexty",
                        line=dict(color="rgba(100,100,100,0.3)"),
                        name="95% CI",
                    ),
                    row=2,
                    col=1,
                )
            fig.update_layout(template="plotly_dark", height=600, showlegend=True)
            fig.update_xaxes(title_text="Lag", row=1, col=1)
            fig.update_xaxes(title_text="Lag", row=2, col=1)
            fig.update_yaxes(title_text="Autocorrelation", row=1, col=1)
            fig.update_yaxes(title_text="Partial Autocorrelation", row=2, col=1)
            fig.show(renderer="browser")
        else:
            print("跳過 ACF 和 PACF 圖片生成")
        # 建議 Panel
        if acf_sig_lags or pacf_sig_lags:
            suggestion = (
                "[bold green]存在自相關，代表歷史數據對未來有影響。[/bold green]\n"
                "你可以：\n"
                "- 嘗試繪製時序圖，觀察趨勢與週期性。\n"
                "- 嘗試將過去幾期的數值（lag features）加入預測模型。\n"
                "- 進行移動平均、差分等預處理，觀察對預測效果的影響。\n"
                "如有興趣，可進一步學習 AR、MA、ARMA、ARIMA 等模型。"
            )
        else:
            suggestion = (
                "[bold yellow]無顯著自相關，資料較隨機，歷史對未來影響小。[/bold yellow]\n"
                "你可以：\n"
                "- 嘗試其他特徵工程（如外部因子、非線性轉換）。\n"
                "- 檢查資料品質或資料頻率是否合適。"
            )
        console.print(
            Panel(
                suggestion,
                title="[bold #8f1511]🔬 統計分析 StatAnalyser[/bold #8f1511]",
                border_style="#dbac30",
            )
        )
        console.print("\n")

        self.results = {
            "success": True,
            "acf_lags": acf_sig_lags,
            "pacf_lags": pacf_sig_lags,
            "has_autocorr": bool(acf_sig_lags or pacf_sig_lags),
        }
        return self.results
