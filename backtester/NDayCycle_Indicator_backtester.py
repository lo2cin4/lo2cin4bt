"""
NDayCycle_Indicator_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的N日週期指標工具，負責產生基於N日週期的交易信號，支援多種週期長度和信號類型。

【流程與數據流】
------------------------------------------------------------
- 由 IndicatorsBacktester 調用，產生N日週期信號
- 信號傳遞給 BacktestEngine 進行交易模擬

```mermaid
flowchart TD
    A[IndicatorsBacktester] -->|調用| B[NDayCycle_Indicator]
    B -->|產生信號| C[BacktestEngine]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增/修改指標型態、參數時，請同步更新頂部註解與下游流程
- 若指標邏輯有變動，需同步更新本檔案與 IndicatorsBacktester
- 指標參數如有調整，請同步通知協作者

【常見易錯點】
------------------------------------------------------------
- 參數設置錯誤會導致信號產生異常
- 數據對齊問題會影響信號準確性
- 指標邏輯變動會影響下游交易模擬

【範例】
------------------------------------------------------------
- indicator = NDayCycleIndicator()
  signals = indicator.calculate_signals(data, params)

【與其他模組的關聯】
------------------------------------------------------------
- 由 IndicatorsBacktester 調用，信號傳遞給 BacktestEngine
- 需與 IndicatorsBacktester 的指標介面保持一致

【參考】
------------------------------------------------------------
- pandas 官方文件
- Indicators_backtester.py、BacktestEngine_backtester.py
- 專案 README
"""
from .IndicatorParams_backtester import IndicatorParams
import pandas as pd
from typing import Optional

class NDayCycleIndicator:
    def __init__(self, n, **kwargs):
        self.n = n

    @staticmethod
    def get_params(strat_idx=1, params_config=None):
        """獲取 NDayCycle 參數"""
        if params_config and 'n_range' in params_config:
            n_range = params_config['n_range']
        else:
            raise ValueError('n_range 必須由 UserInterface 提供')
        # 解析 n_range 格式: start:end:step
        try:
            if ':' in n_range:
                start, end, step = map(int, n_range.split(':'))
                n_values = list(range(start, end + 1, step))
            else:
                n_values = [int(n_range)]
        except Exception as e:
            print(f"解析 N 範圍失敗: {e}，使用預設值 3")
            n_values = [3]
        # strat_idx: 1=NDAY1（開倉後N日做多，找entry_signal==-1），2=NDAY2（開倉後N日做空，找entry_signal==1）
        params_list = []
        for n in n_values:
            # 若 n 是 dict，強制取 value
            if isinstance(n, dict) and 'value' in n:
                n = n['value']
            n = int(n)
            p = IndicatorParams(
                indicator_type="NDayCycle",
                strat_idx=strat_idx
            )
            p.add_param("n", n)
            p.add_param("strat_idx", strat_idx)
            params_list.append(p)
        return params_list

    @staticmethod
    def get_min_valid_index(params):
        return params.n

    @staticmethod
    def calculate_signals(data, params, predictor=None):
        signal = pd.Series(0.0, index=data.index, dtype=float)
        strat_idx = params.strat_idx if hasattr(params, 'strat_idx') else params.get_param('strat_idx', 1)
        direction = 1 if strat_idx == 1 else -1
        for i in range(len(data)):
            if i >= params.n:
                signal.iloc[i] = direction * 0.1
        return signal

    @staticmethod
    def generate_exit_signal_from_entry(entry_signal, n, strat_idx):
        # direction: 1=NDAY1(多單), -1=NDAY2(空單)
        exit_value = -1 if strat_idx == 1 else 1
        exit_signal = entry_signal.copy() * 0
        for idx, val in entry_signal.items():
            if val == 1 or val == -1:
                exit_idx = idx + n
                if exit_idx < len(entry_signal):
                    exit_signal.iloc[exit_idx] = exit_value
                # 其餘 debug print 已移除
        return exit_signal 