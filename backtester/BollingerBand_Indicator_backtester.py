"""
BollingerBand_Indicator_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的「布林通道指標」模組，負責產生 Bollinger Band 指標序列與信號，支援多種突破/回歸策略型態，並可根據預測因子產生交易信號。

【關聯流程與數據流】
------------------------------------------------------------
- 由 IndicatorsBacktester 調用，根據參數產生 Bollinger Band 信號
- 產生信號傳遞給 BacktestEngine 進行回測
- 主要數據流：

```mermaid
flowchart TD
    A[IndicatorsBacktester] -->|調用| B[BollingerBand_Indicator_backtester]
    B -->|產生信號| C[BacktestEngine]
    C -->|模擬交易| D[TradeSimulator]
```

【主控流程細節】
------------------------------------------------------------
- get_params() 產生所有策略型態與參數組合
- generate_signals() 根據參數與預測因子產生交易信號
- calculate() 支援基於價格或預測因子的布林通道計算
- 支援多種策略型態（突破上軌/下軌做多/做空）

【維護與擴充提醒】
------------------------------------------------------------
- 新增策略型態、參數時，請同步更新 get_params/STRATEGY_DESCRIPTIONS/頂部註解
- 若參數結構有變動，需同步更新 IndicatorParams、TradeRecordExporter 等依賴模組
- 信號產生邏輯如有調整，請於 README 詳列

【常見易錯點】
------------------------------------------------------------
- 參數結構未同步更新，導致信號產生錯誤
- 預測因子欄位不存在或數據對齊錯誤
- 信號產生邏輯未考慮 NaN 或邊界情況

【範例】
------------------------------------------------------------
- 產生參數組合：BollingerBandIndicator.get_params(strat_idx, params_config)
- 產生信號：BollingerBandIndicator(data, params).generate_signals(predictor)

【與其他模組的關聯】
------------------------------------------------------------
- 由 IndicatorsBacktester 調用，信號傳遞給 BacktestEngine
- 參數結構依賴 IndicatorParams

【維護重點】
------------------------------------------------------------
- 新增/修改策略型態、參數結構時，務必同步更新本檔案、IndicatorParams、TradeRecordExporter
- 信號產生邏輯需與主流程保持一致

【參考】
------------------------------------------------------------
- 詳細指標規範與參數定義請參閱 README
- 其他模組如有依賴本模組，請於對應檔案頂部註解標明
"""
import pandas as pd
import numpy as np
import logging
import uuid
from .IndicatorParams_backtester import IndicatorParams

class BollingerBandIndicator:
    """
    Bollinger Band 指標與信號產生器
    支援六種指標邏輯，參數可自訂
    """
    STRATEGY_DESCRIPTIONS = [
        "價格突破上軌（ma+n倍sd)做多",
        "價格突破上軌（ma+n倍sd)做空", 
        "價格突破下軌(ma-n倍sd)做多",
        "價格突破下軌(ma-n倍sd)做空"
    ]

    @staticmethod
    def get_strategy_descriptions():
        # 回傳 dict: {'BOLL1': '描述', ...}
        return {f"BOLL{i+1}": desc for i, desc in enumerate(BollingerBandIndicator.STRATEGY_DESCRIPTIONS)}

    def __init__(self, data, params, logger=None):
        self.data = data.copy()
        self.params = params
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.signals = None
    
    @classmethod
    def get_params(cls, strat_idx=None, params_config=None):
        ma_range = params_config.get("ma_range", "10:20:10") if params_config else "10:20:10"
        sd_input = params_config.get("sd_multi", "2,3") if params_config else "2"
        start, end, step = map(int, ma_range.split(":"))
        ma_lengths = list(range(start, end+1, step))
        sd_multi_list = [float(x) for x in sd_input.split(",") if x.strip()]
        param_list = []
        if strat_idx in [1, 2, 3, 4]:
            for n in ma_lengths:
                for sd in sd_multi_list:
                    param = IndicatorParams("BOLL")
                    param.add_param("ma_length", n)
                    param.add_param("std_multiplier", sd)
                    param.add_param("strat_idx", strat_idx)
                    param_list.append(param)
        else:
            for strat_idx in [1, 2, 3, 4]:
                for n in ma_lengths:
                    for sd in sd_multi_list:
                        param = IndicatorParams("BOLL")
                        param.add_param("ma_length", n)
                        param.add_param("std_multiplier", sd)
                        param.add_param("strat_idx", strat_idx)
                        param_list.append(param)
        return param_list

    # get_user_params已廢除，所有參數請由外部傳入

    def calculate(self, ma_window, sd_multi, strat_idx):
        df = self.data.copy()
        df[f'MA{ma_window}'] = df['Close'].rolling(ma_window, min_periods=1).mean()
        df[f'SD{ma_window}'] = df['Close'].rolling(ma_window, min_periods=1).std(ddof=0)
        df[f'Upper{ma_window}'] = df[f'MA{ma_window}'] + sd_multi * df[f'SD{ma_window}']
        df[f'Lower{ma_window}'] = df[f'MA{ma_window}'] - sd_multi * df[f'SD{ma_window}']
        signal = np.zeros(len(df))
        
        for i in range(1, len(df)):
            price = df['Close'].iloc[i]
            prev_price = df['Close'].iloc[i-1]
            ma = df[f'MA{ma_window}'].iloc[i]
            upper = df[f'Upper{ma_window}'].iloc[i]
            lower = df[f'Lower{ma_window}'].iloc[i]
            # 跳過 NaN 值
            if pd.isna(price) or pd.isna(prev_price) or pd.isna(ma) or pd.isna(upper) or pd.isna(lower):
                continue
            
            if strat_idx == 1:  # BOLL1：價格突破上軌做多
                if prev_price <= upper and price > upper:
                    signal[i] = 1
            elif strat_idx == 2:  # BOLL2：價格突破上軌做空
                if prev_price <= upper and price > upper:
                    signal[i] = -1
            elif strat_idx == 3:  # BOLL3：價格突破下軌做多
                if prev_price >= lower and price < lower:
                    signal[i] = 1
            elif strat_idx == 4:  # BOLL4：價格突破下軌做空
                if prev_price >= lower and price < lower:
                    signal[i] = -1
                    
        df[f'BBAND_signal_MA{ma_window}_SD{sd_multi}_S{strat_idx}'] = signal
        self.signals = df[[f'BBAND_signal_MA{ma_window}_SD{sd_multi}_S{strat_idx}']]
        return self.signals

    # run方法已廢除，請直接用generate_signals與外部參數控制

    def generate_signals(self, predictor=None):
        """
        根據 BOLL 參數產生交易信號（1=多頭, -1=空頭, 0=無動作）。
        基於預測因子計算 Bollinger Bands，而非價格。
        
        strat=1: 預測因子突破上軌做多
        strat=2: 預測因子突破上軌做空
        strat=3: 預測因子突破下軌做多
        strat=4: 預測因子突破下軌做空
        """
        ma_length = self.params.get_param("ma_length", 20)
        std_multiplier = self.params.get_param("std_multiplier", 2.0)
        strat_idx = self.params.get_param("strat_idx", 1)
        
        # 使用預測因子而非價格
        if predictor is None:
            predictor_series = self.data["Close"]
            self.logger.warning("未指定預測因子，使用 Close 價格作為預測因子")
        else:
            if predictor in self.data.columns:
                predictor_series = self.data[predictor]
            else:
                raise ValueError(f"預測因子 '{predictor}' 不存在於數據中，可用欄位: {list(self.data.columns)}")
        
        # 基於預測因子計算 Bollinger Bands
        ma = predictor_series.rolling(ma_length, min_periods=1).mean()
        sd = predictor_series.rolling(ma_length, min_periods=1).std(ddof=0)
        upper = ma + std_multiplier * sd
        lower = ma - std_multiplier * sd
        signal = pd.Series(0, index=self.data.index)
        
        for i in range(1, len(predictor_series)):
            # 僅在 i >= ma_length-1 時才允許產生信號，否則信號必為0
            if i < ma_length - 1:
                continue
            p = predictor_series.iloc[i]
            prev_p = predictor_series.iloc[i-1]
            m = ma.iloc[i]
            u = upper.iloc[i]
            l = lower.iloc[i]
            # 跳過 NaN 值
            if pd.isna(p) or pd.isna(prev_p) or pd.isna(m) or pd.isna(u) or pd.isna(l):
                continue
            if strat_idx == 1:  # BOLL1：預測因子突破上軌做多
                if prev_p <= u and p > u:
                    signal.iloc[i] = 1
            elif strat_idx == 2:  # BOLL2：預測因子突破上軌做空
                if prev_p <= u and p > u:
                    signal.iloc[i] = -1
            elif strat_idx == 3:  # BOLL3：預測因子突破下軌做多
                if prev_p >= l and p < l:
                    signal.iloc[i] = 1
            elif strat_idx == 4:  # BOLL4：預測因子突破下軌做空
                if prev_p >= l and p < l:
                    signal.iloc[i] = -1
        return signal 

    def get_min_valid_index(self):
        ma_length = self.params.get_param("ma_length", 20)
        return ma_length - 1 