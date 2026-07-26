"""Autocorrelation test for statanalyser."""

from __future__ import annotations

from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from statsmodels.tsa.stattools import acf, pacf

from .Base_statanalyser import BaseStatAnalyser


class AutocorrelationTest(BaseStatAnalyser):
    def __init__(
        self,
        data: pd.DataFrame,
        predictor_col: str,
        return_col: str,
        freq: str = "D",
        analysis_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(data, predictor_col, return_col, analysis_config=analysis_config)
        self.freq = str(freq).upper() if freq else "D"
        if self.freq not in {"D", "H", "T"}:
            self.freq = "D"

    def analyze(self) -> Dict[str, Any]:
        from rich.table import Table
        from utils import get_console, show_info, show_step_panel

        console = get_console()
        series = self.data[self.predictor_col].dropna()
        if len(series) < 5:
            self.results = {"success": False, "acf_lags": [], "pacf_lags": []}
            return self.results

        lags_config = self.analysis_config.get("lags")
        if isinstance(lags_config, list) and lags_config:
            try:
                parsed_lags = [int(value) for value in lags_config]
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "Autocorrelation lags must contain positive integers"
                ) from exc
            if any(value <= 0 for value in parsed_lags):
                raise ValueError(
                    "Autocorrelation lags must contain positive integers"
                )
            lags = max(parsed_lags)
        else:
            lags = 0

        if lags <= 0:
            lags = {
                "D": min(60, len(series) // 2),
                "H": min(24, len(series) // 2),
                "T": min(120, len(series) // 2),
            }.get(self.freq, min(20, len(series) // 2))

        panel_content = (
            "自動化自相關檢驗\n"
            f"Predictor: {self.predictor_col}\n"
            f"Frequency: {self.freq}\n"
            f"Lags: {lags}\n"
        )
        show_step_panel("STATANALYSER", 1, ["自相關性檢驗[自動]"], panel_content)

        acf_result = acf(series, nlags=lags, alpha=0.05, fft=True)
        if isinstance(acf_result, tuple) and len(acf_result) >= 2:
            acf_vals, acf_conf = acf_result[:2]
        else:
            acf_vals, acf_conf = acf_result, None

        pacf_result = pacf(series, nlags=lags, alpha=0.05)
        if isinstance(pacf_result, tuple) and len(pacf_result) >= 2:
            pacf_vals, pacf_conf = pacf_result[:2]
        else:
            pacf_vals, pacf_conf = pacf_result, None

        threshold = 1.96 / np.sqrt(len(series))
        acf_sig_lags = [i for i in range(1, lags + 1) if abs(acf_vals[i]) > threshold]
        pacf_sig_lags = [i for i in range(1, lags + 1) if abs(pacf_vals[i]) > threshold]

        stats_table = Table(title="自相關統計摘要", border_style="#dbac30", show_lines=True)
        stats_table.add_column("項目", style="bold white")
        stats_table.add_column("數值", style="bold white")
        stats_table.add_column("說明", style="bold white")
        stats_table.add_row("樣本數", str(len(series)), "用於 ACF/PACF")
        stats_table.add_row("最大滯後", str(lags), "由 config 或資料長度決定")
        stats_table.add_row("臨界值", f"{threshold:.4f}", "95% 信賴區間")
        stats_table.add_row("ACF 顯著滯後", str(len(acf_sig_lags)), "超過臨界值")
        stats_table.add_row("PACF 顯著滯後", str(len(pacf_sig_lags)), "超過臨界值")
        console.print(stats_table)

        sig_table = Table(title="ACF/PACF 顯著滯後", border_style="#dbac30", show_lines=True)
        sig_table.add_column("類型", style="bold white")
        sig_table.add_column("滯後", style="bold white")
        sig_table.add_column("數值", style="bold white")
        sig_table.add_row(
            "ACF",
            str(acf_sig_lags) if acf_sig_lags else "-",
            str([round(float(acf_vals[lag]), 4) for lag in acf_sig_lags]) if acf_sig_lags else "-",
        )
        sig_table.add_row(
            "PACF",
            str(pacf_sig_lags) if pacf_sig_lags else "-",
            str([round(float(pacf_vals[lag]), 4) for lag in pacf_sig_lags]) if pacf_sig_lags else "-",
        )
        console.print(sig_table)

        output_spec = self.analysis_config.get("output", [])
        if isinstance(output_spec, dict):
            generate_plots = bool(output_spec.get("plots", False))
        elif isinstance(output_spec, list):
            generate_plots = "plots" in output_spec
        else:
            generate_plots = False
        generate_plots = generate_plots or bool(self.analysis_config.get("include_plots", False))

        if generate_plots:
            fig = make_subplots(
                rows=2,
                cols=1,
                subplot_titles=(
                    f"ACF of {self.predictor_col}",
                    f"PACF of {self.predictor_col}",
                ),
            )
            fig.add_trace(
                go.Scatter(x=list(range(lags + 1)), y=acf_vals, mode="lines+markers", name="ACF"),
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
                go.Scatter(x=list(range(lags + 1)), y=pacf_vals, mode="lines+markers", name="PACF"),
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
            plot_path = self.get_output_dir() / f"autocorrelation_{self.predictor_col}.html"
            fig.write_html(str(plot_path))

        if acf_sig_lags or pacf_sig_lags:
            suggestion = (
                "自相關顯著，建議加入 lag features 或考慮 AR/ARIMA 類模型。"
            )
        else:
            suggestion = "未見明顯自相關，lag features 的收益可能有限。"
        show_info("STATANALYSER", suggestion)
        self.results = {
            "success": True,
            "acf_lags": acf_sig_lags,
            "pacf_lags": pacf_sig_lags,
            "has_autocorr": bool(acf_sig_lags or pacf_sig_lags),
            "plots_generated": generate_plots,
        }
        if generate_plots:
            self.results["plot_path"] = str(self.get_output_dir() / f"autocorrelation_{self.predictor_col}.html")
        return self.results
