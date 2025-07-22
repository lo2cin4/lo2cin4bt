"""
NDayCycle_Indicator_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的「N日自動平倉指標」模組，僅可作為平倉信號。負責根據開倉信號，自動於第N日產生平倉信號，支援多單/空單型態。

【關聯流程與數據流】
------------------------------------------------------------
- 由 IndicatorsBacktester 調用，僅產生標記信號（0.1/-0.1），實際平倉信號由主流程自動調用 generate_exit_signal_from_entry 產生
- 主要數據流：

```mermaid
flowchart TD
    A[IndicatorsBacktester] -->|調用| B[NDayCycle_Indicator_backtester]
    B -->|標記信號| C[BacktestEngine]
    C -->|產生平倉信號| D[TradeSimulator]
```

【主控流程細節】
------------------------------------------------------------
- get_params() 產生所有 N 值與 strat_idx 組合的參數
- calculate_signals() 產生標記信號（0.1/-0.1），不直接產生實際平倉信號
- generate_exit_signal_from_entry() 根據 entry_signal 自動產生平倉信號，主流程自動調用
- 僅能作為平倉信號，不能作為開倉信號，違規時需報錯

【維護與擴充提醒】
------------------------------------------------------------
- 新增 N 值範圍、strat_idx 時，請同步更新 get_params/頂部註解
- 若參數結構有變動，需同步更新 IndicatorParams、TradeSimulator、BacktestEngine 等依賴模組
- 信號產生與包裝邏輯如有調整，請於 README 詳列

【常見易錯點】
------------------------------------------------------------
- NDayCycle 被誤用為開倉信號，需報錯並重選
- 參數結構未同步更新，導致信號產生錯誤
- entry_signal 與數據長度不符，產生平倉信號異常

【範例】
------------------------------------------------------------
- 產生參數組合：NDayCycleIndicator.get_params(strat_idx, params_config)
- 產生標記信號：NDayCycleIndicator.calculate_signals(data, params)
- 產生平倉信號：NDayCycleIndicator.generate_exit_signal_from_entry(entry_signal, n, strat_idx)

【與其他模組的關聯】
------------------------------------------------------------
- 由 IndicatorsBacktester 調用，標記信號傳遞給 BacktestEngine
- 參數結構依賴 IndicatorParams

【維護重點】
------------------------------------------------------------
- 新增/修改 N 值、strat_idx、參數結構時，務必同步更新本檔案、IndicatorParams、TradeSimulator、BacktestEngine
- 信號產生與包裝邏輯需與主流程保持一致

【參考】
------------------------------------------------------------
- 詳細指標規範與參數定義請參閱 README
- 其他模組如有依賴本模組，請於對應檔案頂部註解標明
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