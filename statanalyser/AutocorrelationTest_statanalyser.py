"""
AutocorrelationTest_statanalyser.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 統計分析模組，負責對時序數據進行自相關檢定（如 ACF、PACF、Durbin-Watson、Ljung-Box 等），評估殘差或報酬率的自相關性與週期性，輔助模型選擇與診斷。

【關聯流程與數據流】
------------------------------------------------------------
- 繼承 Base_statanalyser，作為統計分析子類之一
- 檢定結果傳遞給 ReportGenerator 或下游模組

```mermaid
flowchart TD
    A[AutocorrelationTest] -->|檢定結果| B[ReportGenerator/下游模組]
```

【主控流程細節】
------------------------------------------------------------
- 實作 analyze 方法，支援 ACF、PACF、顯著滯後期自動判斷
- 支援互動式選擇是否繪製 ACF/PACF 圖表
- 檢定結果以字典格式返回，包含顯著滯後期、建議模型等
- 支援多種頻率（D/H/T），自動調整滯後期數

【維護與擴充提醒】
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

【維護重點】
------------------------------------------------------------
- 新增/修改檢定類型、參數、圖表邏輯、結果格式時，務必同步更新本檔案與 Base_statanalyser
- 欄位名稱、型態需與下游模組協調一致

【參考】
------------------------------------------------------------
- statsmodels、plotly 官方文件
- Base_statanalyser.py、ReportGenerator_statanalyser.py
- 專案 README
"""
import pandas as pd
import numpy as np
from .Base_statanalyser import BaseStatAnalyser
from statsmodels.tsa.stattools import acf, pacf
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from typing import Dict

class AutocorrelationTest(BaseStatAnalyser):
    """自相關性檢驗模組，檢測序列的記憶效應和週期性"""

    def __init__(self, data: pd.DataFrame, predictor_col: str, return_col: str, freq: str = 'D'):
        super().__init__(data, predictor_col, return_col)
        self.freq = freq.upper()
        if self.freq not in ['D', 'H', 'T']:
            print(f"警告：未知頻率 {self.freq}，使用預設 'D'")
            self.freq = 'D'

    def analyze(self) -> Dict:
        """執行 ACF 和 PACF 分析"""
        print(f"\n=== 檢驗：{self.predictor_col} 自相關性檢驗（ACF 和 PACF） ===")
        print("1. 檢驗名稱：自相關性檢驗")
        print("2. 檢驗功能：檢測序列的記憶效應和週期性")

        series = self.data[self.predictor_col].dropna()
        if len(series) < 5:
            print(f"3. 檢驗結果：數據點不足（{len(series)}個）")
            return {'success': False, 'acf_lags': [], 'pacf_lags': []}

        # 設置滯後期數
        lags = {
            'D': min(60, len(series) // 2),
            'H': min(24, len(series) // 2),
            'T': min(120, len(series) // 2)
        }.get(self.freq, min(20, len(series) // 2))
        print(f"使用滯後期數：{lags}（頻率={self.freq}）")

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

        # 詢問是否生成ACF和PACF圖片
        generate_plots = input("\n是否生成ACF和PACF圖片？(y/n，預設n)：").strip().lower() or 'n'
        generate_plots = generate_plots == 'y'
        
        # 根據設定決定是否繪製圖表
        if generate_plots:
            print("正在生成 ACF 和 PACF 圖片...")
            # 繪製圖表
            fig = make_subplots(rows=2, cols=1, subplot_titles=(f'ACF of {self.predictor_col}', f'PACF of {self.predictor_col}'))
            fig.add_trace(go.Scatter(x=list(range(lags + 1)), y=acf_vals, mode='lines+markers', name='ACF'), row=1, col=1)
            if acf_conf is not None:
                fig.add_trace(go.Scatter(x=list(range(lags + 1)), y=acf_conf[:, 0] - acf_vals, line=dict(color='rgba(0,0,0,0)'), showlegend=False), row=1, col=1)
                fig.add_trace(go.Scatter(x=list(range(lags + 1)), y=acf_conf[:, 1] - acf_vals, fill='tonexty', line=dict(color='rgba(100,100,100,0.3)'), name='95% CI'), row=1, col=1)
            fig.add_trace(go.Scatter(x=list(range(lags + 1)), y=pacf_vals, mode='lines+markers', name='PACF'), row=2, col=1)
            if pacf_conf is not None:
                fig.add_trace(go.Scatter(x=list(range(lags + 1)), y=pacf_conf[:, 0] - pacf_vals, line=dict(color='rgba(0,0,0,0)'), showlegend=False), row=2, col=1)
                fig.add_trace(go.Scatter(x=list(range(lags + 1)), y=pacf_conf[:, 1] - pacf_vals, fill='tonexty', line=dict(color='rgba(100,100,100,0.3)'), name='95% CI'), row=2, col=1)
            fig.update_layout(template='plotly_dark', height=600, showlegend=True)
            fig.update_xaxes(title_text='Lag', row=1, col=1)
            fig.update_xaxes(title_text='Lag', row=2, col=1)
            fig.update_yaxes(title_text='Autocorrelation', row=1, col=1)
            fig.update_yaxes(title_text='Partial Autocorrelation', row=2, col=1)
            fig.show(renderer="browser")
        else:
            print("跳過 ACF 和 PACF 圖片生成")

        # 顯著滯後期
        threshold = 1.96 / np.sqrt(len(series))
        acf_sig_lags = [i for i in range(1, lags + 1) if abs(acf_vals[i]) > threshold]
        pacf_sig_lags = [i for i in range(1, lags + 1) if abs(pacf_vals[i]) > threshold]

        print("3. 檢驗結果數據：")
        print(f"   - ACF 顯著滯後期：{acf_sig_lags if acf_sig_lags else '無'}")
        print(f"   - PACF 顯著滯後期：{pacf_sig_lags if pacf_sig_lags else '無'}")
        print("4. 建議：")
        if acf_sig_lags or pacf_sig_lags:
            p = max(pacf_sig_lags[:5]) if pacf_sig_lags else 0
            q = max(acf_sig_lags[:5]) if acf_sig_lags else 0
            print(f"   - 存在自相關，建議考慮 ARIMA(p={p}, q={q}) 模型")
        else:
            print("   - 無顯著自相關，適合直接建模因子值")
        print("=============================")

        self.results = {
            'success': True,
            'acf_lags': acf_sig_lags,
            'pacf_lags': pacf_sig_lags,
            'has_autocorr': bool(acf_sig_lags or pacf_sig_lags)
        }
        return self.results