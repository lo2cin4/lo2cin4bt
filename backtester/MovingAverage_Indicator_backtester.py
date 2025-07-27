"""
MovingAverage_Indicator_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的移動平均指標工具，負責產生移動平均線信號，支援單均線、雙均線、多均線等多種策略型態。

【流程與數據流】
------------------------------------------------------------
- 由 IndicatorsBacktester 調用，產生移動平均線信號
- 信號傳遞給 BacktestEngine 進行交易模擬

```mermaid
flowchart TD
    A[IndicatorsBacktester] -->|調用| B[MovingAverage_Indicator]
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
- indicator = MovingAverageIndicator()
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
import pandas as pd
import numpy as np
import logging
from .IndicatorParams_backtester import IndicatorParams

class MovingAverageIndicator:
    """
    移動平均線指標（僅產生MA序列，不含策略信號）
    支援 SMA、EMA、WMA
    """
    MA_DESCRIPTIONS = [
        "價格升穿n日均線做多",
        "價格升穿n日均線做空",
        "價格跌穿n日均線做多",
        "價格跌穿n日均線做空",
        "n日均線升穿m日均線做多",
        "n日均線升穿m日均線做空",
        "n日均線跌穿m日均線做多",
        "n日均線跌穿m日均線做空",
        "價格連續m日位於n日均線以上做多",
        "價格連續m日位於n日均線以上做空",
        "價格連續m日位於n日均線以下做多",
        "價格連續m日位於n日均線以下做空"
    ]

    def __init__(self, data, params, logger=None):
        self.data = data.copy()
        # self.data.columns = [col.capitalize() for col in self.data.columns]  # 移除這行
        self.params = params
        self.logger = logger or logging.getLogger("MovingAverageIndicator")

    @staticmethod
    def get_strategy_descriptions():
        # 回傳 dict: {'MA1': '描述', ...}
        return {f"MA{i+1}": desc for i, desc in enumerate(MovingAverageIndicator.MA_DESCRIPTIONS)}

    @classmethod
    def get_params(cls, strat_idx=None, params_config=None):
        """
        參數必須完全由 UserInterface 層傳入，否則丟出 ValueError。
        不再於此處設定任何預設值。
        """
        if params_config is None:
            raise ValueError("params_config 必須由 UserInterface 提供，且不得為 None")
        ma_type = params_config["ma_type"]  # 必須由 UI 提供
        param_list = []
        
        if strat_idx in [1, 2, 3, 4]:
            if "ma_range" not in params_config:
                raise ValueError("ma_range 必須由 UserInterface 提供")
            ma_range = params_config["ma_range"]
            start, end, step = map(int, ma_range.split(":"))
            ma_lengths = list(range(start, end+1, step))
            for n in ma_lengths:
                param = IndicatorParams("MA")
                param.add_param("ma_type", ma_type)
                param.add_param("period", n)
                param.add_param("mode", "single")
                param.add_param("strat_idx", strat_idx)
                # 不再加入 shortMA_period/longMA_period
                param_list.append(param)
        elif strat_idx in [5, 6, 7, 8]:
            if "short_range" not in params_config or "long_range" not in params_config:
                raise ValueError("short_range 與 long_range 必須由 UserInterface 提供")
            short_range = params_config["short_range"]
            long_range = params_config["long_range"]
            s_start, s_end, s_step = map(int, short_range.split(":"))
            l_start, l_end, l_step = map(int, long_range.split(":"))
            short_periods = list(range(s_start, s_end+1, s_step))
            long_periods = list(range(l_start, l_end+1, l_step))
            for sp in short_periods:
                for lp in long_periods:
                    if sp < lp:
                        param = IndicatorParams("MA")
                        param.add_param("ma_type", ma_type)
                        param.add_param("shortMA_period", sp)
                        param.add_param("longMA_period", lp)
                        param.add_param("mode", "double")
                        param.add_param("strat_idx", strat_idx)
                        # 不再加入 period=None
                        param_list.append(param)
        elif strat_idx in [9, 10, 11, 12]:
            if "m_range" not in params_config or "n_range" not in params_config:
                raise ValueError("m_range 與 n_range 必須由 UserInterface 提供")
            m_range = params_config["m_range"]
            n_range = params_config["n_range"]
            m_start, m_end, m_step = map(int, m_range.split(":"))
            n_start, n_end, n_step = map(int, n_range.split(":"))
            m_list = list(range(m_start, m_end+1, m_step))
            n_list = list(range(n_start, n_end+1, n_step))
            for m in m_list:
                for n in n_list:
                    param = IndicatorParams("MA")
                    param.add_param("ma_type", ma_type)
                    param.add_param("period", n)
                    param.add_param("m", m)
                    param.add_param("mode", "single")
                    param.add_param("strat_idx", strat_idx)
                    # 不再加入 shortMA_period/longMA_period
                    param_list.append(param)
        else:
            raise ValueError("strat_idx 必須由 UserInterface 明確指定且有效")
        return param_list

    def calculate(self):
        """根據params產生MA序列（單/雙）"""
        ma_type = self.params.get_param("ma_type", "SMA")
        mode = self.params.get_param("mode", "single")
        if mode == "single":
            period = self.params.get_param("period", 20)
            return self._calculate_ma(self.data["Close"], period, ma_type)
        elif mode == "double":
            short_period = self.params.get_param("shortMA_period", 10)
            long_period = self.params.get_param("longMA_period", 20)
            short_ma = self._calculate_ma(self.data["Close"], short_period, ma_type)
            long_ma = self._calculate_ma(self.data["Close"], long_period, ma_type)
            return pd.DataFrame({
                f"short_{ma_type}_{short_period}": short_ma,
                f"long_{ma_type}_{long_period}": long_ma
            })
        else:
            raise ValueError(f"未知MA模式: {mode}")

    def _calculate_ma(self, series, period, ma_type):
        ma_type = ma_type.upper()
        if ma_type == "SMA":
            return series.rolling(window=period).mean()
        elif ma_type == "EMA":
            return series.ewm(span=period, adjust=False).mean()
        elif ma_type == "WMA":
            weights = np.arange(1, period + 1)
            return series.rolling(window=period).apply(lambda x: np.sum(x * weights) / weights.sum(), raw=True)
        else:
            raise ValueError(f"不支持的 MA 類型: {ma_type}")

    def generate_signals(self, predictor=None):
        """
        根據 MA 參數產生交易信號（1=多頭, -1=空頭, 0=無動作）。
        基於預測因子計算 MA，而非價格。
        
        strat_idx=1: 預測因子升穿均線做多
        strat_idx=2: 預測因子升穿均線做空
        strat_idx=3: 預測因子跌穿均線做多
        strat_idx=4: 預測因子跌穿均線做空
        strat_idx=5: 短均線升穿長均線做多
        strat_idx=6: 短均線升穿長均線做空
        strat_idx=7: 短均線跌穿長均線做多
        strat_idx=8: 短均線跌穿長均線做空
        strat_idx=9: 預測因子位於均線以上做多
        strat_idx=10: 預測因子位於均線以上做空
        strat_idx=11: 預測因子位於均線以下做多
        strat_idx=12: 預測因子位於均線以下做空
        """
        strat_idx = self.params.get_param("strat_idx", 1)
        mode = self.params.get_param("mode", "single")
        
        # print(f"[DEBUG] MovingAverageIndicator.generate_signals 開始")
        # print(f"[DEBUG] strat_idx={strat_idx}, mode={mode}")
        # print(f"[DEBUG] predictor={predictor}")
        # print(f"[DEBUG] 數據欄位：{list(self.data.columns)}")
        
        # 使用預測因子而非價格
        if predictor is None:
            predictor_series = self.data["Close"]
            print(f"[DEBUG] 未指定預測因子，使用 Close 價格")
            self.logger.warning("未指定預測因子，使用 Close 價格作為預測因子")
        else:
            if predictor in self.data.columns:
                predictor_series = self.data[predictor]
                # print(f"[DEBUG] 使用預測因子：{predictor}")
                # print(f"[DEBUG] 預測因子統計：均值={predictor_series.mean():.4f}, 標準差={predictor_series.std():.4f}")
                # print(f"[DEBUG] 預測因子範圍：{predictor_series.min():.4f} ~ {predictor_series.max():.4f}")
            else:
                error_msg = f"預測因子 '{predictor}' 不存在於數據中，可用欄位: {list(self.data.columns)}"
                print(f"[DEBUG] {error_msg}")
                raise ValueError(error_msg)
        
        signal = pd.Series(0, index=self.data.index)
        
        if mode == "single":
            period = self.params.get_param("period", 20)
            ma_type = self.params.get_param("ma_type", "SMA")
            # print(f"[DEBUG] 單均線模式：period={period}, ma_type={ma_type}")
            
            ma_series = self._calculate_ma(predictor_series, period, ma_type)
            # print(f"[DEBUG] 均線計算完成，均線統計：均值={ma_series.mean():.4f}, 標準差={ma_series.std():.4f}")
            # print(f"[DEBUG] 均線範圍：{ma_series.min():.4f} ~ {ma_series.max():.4f}")
            
            signal_count = 0
            for i in range(1, len(predictor_series)):
                prev_predictor = predictor_series.iloc[i-1]
                curr_predictor = predictor_series.iloc[i]
                prev_ma = ma_series.iloc[i-1]
                curr_ma = ma_series.iloc[i]
                
                # 跳過 NaN 值
                if pd.isna(prev_predictor) or pd.isna(curr_predictor) or pd.isna(prev_ma) or pd.isna(curr_ma):
                    continue
                
                if strat_idx == 1:  # MA1: 預測因子升穿均線做多
                    if prev_predictor <= prev_ma and curr_predictor > curr_ma:
                        signal.iloc[i] = 1
                        signal_count += 1
                        # if signal_count <= 5:  # 只顯示前5個信號的詳細信息
                        #     print(f"[DEBUG] 信號1生成：位置{i}, 前值={prev_predictor:.4f}<=前均線={prev_ma:.4f}, 當前值={curr_predictor:.4f}>當前均線={curr_ma:.4f}")
                elif strat_idx == 2:  # MA2: 預測因子升穿均線做空
                    if prev_predictor <= prev_ma and curr_predictor > curr_ma:
                        signal.iloc[i] = -1
                        signal_count += 1
                        # if signal_count <= 5:
                        #     print(f"[DEBUG] 信號-1生成：位置{i}, 前值={prev_predictor:.4f}<=前均線={prev_ma:.4f}, 當前值={curr_predictor:.4f}>當前均線={curr_ma:.4f}")
                elif strat_idx == 3:  # MA3: 預測因子跌穿均線做多
                    if prev_predictor >= prev_ma and curr_predictor < curr_ma:
                        signal.iloc[i] = 1
                        signal_count += 1
                        # if signal_count <= 5:
                        #     print(f"[DEBUG] 信號1生成：位置{i}, 前值={prev_predictor:.4f}>=前均線={prev_ma:.4f}, 當前值={curr_predictor:.4f}<當前均線={curr_ma:.4f}")
                elif strat_idx == 4:  # MA4: 預測因子跌穿均線做空
                    if prev_predictor >= prev_ma and curr_predictor < curr_ma:
                        signal.iloc[i] = -1
                        signal_count += 1
                        # if signal_count <= 5:
                        #     print(f"[DEBUG] 信號-1生成：位置{i}, 前值={prev_predictor:.4f}>=前均線={prev_ma:.4f}, 當前值={curr_predictor:.4f}<當前均線={curr_ma:.4f}")
                elif strat_idx == 9:  # MA9: 預測因子位於均線以上做多
                    if curr_predictor > curr_ma:
                        signal.iloc[i] = 1
                        signal_count += 1
                        # if signal_count <= 5:
                        #     print(f"[DEBUG] 信號1生成：位置{i}, 當前值={curr_predictor:.4f}>當前均線={curr_ma:.4f}")
                elif strat_idx == 10:  # MA10: 預測因子位於均線以上做空
                    if curr_predictor > curr_ma:
                        signal.iloc[i] = -1
                        signal_count += 1
                        # if signal_count <= 5:
                        #     print(f"[DEBUG] 信號-1生成：位置{i}, 當前值={curr_predictor:.4f}>當前均線={curr_ma:.4f}")
                elif strat_idx == 11:  # MA11: 預測因子位於均線以下做多
                    if curr_predictor < curr_ma:
                        signal.iloc[i] = 1
                        signal_count += 1
                        # if signal_count <= 5:
                        #     print(f"[DEBUG] 信號1生成：位置{i}, 當前值={curr_predictor:.4f}<當前均線={curr_ma:.4f}")
                elif strat_idx == 12:  # MA12: 預測因子位於均線以下做空
                    if curr_predictor < curr_ma:
                        signal.iloc[i] = -1
                        signal_count += 1
                        # if signal_count <= 5:
                        #     print(f"[DEBUG] 信號-1生成：位置{i}, 當前值={curr_predictor:.4f}<當前均線={curr_ma:.4f}")
            
            # print(f"[DEBUG] 單均線模式完成，總信號數：{signal_count}")
            
        else:  # 雙均線模式
            short_period = self.params.get_param("shortMA_period", 10)
            long_period = self.params.get_param("longMA_period", 20)
            ma_type = self.params.get_param("ma_type", "SMA")
            # print(f"[DEBUG] 雙均線模式：短週期={short_period}, 長週期={long_period}, ma_type={ma_type}")
            
            short_ma = self._calculate_ma(predictor_series, short_period, ma_type)
            long_ma = self._calculate_ma(predictor_series, long_period, ma_type)
            # print(f"[DEBUG] 雙均線計算完成")
            
            signal_count = 0
            for i in range(1, len(predictor_series)):
                prev_short = short_ma.iloc[i-1]
                curr_short = short_ma.iloc[i]
                prev_long = long_ma.iloc[i-1]
                curr_long = long_ma.iloc[i]
                
                # 跳過 NaN 值
                if pd.isna(prev_short) or pd.isna(curr_short) or pd.isna(prev_long) or pd.isna(curr_long):
                    continue
                
                if strat_idx == 5:  # MA5: 短均線升穿長均線做多
                    if prev_short <= prev_long and curr_short > curr_long:
                        signal.iloc[i] = 1
                        signal_count += 1
                        # if signal_count <= 5:
                        #     print(f"[DEBUG] 信號1生成：位置{i}, 短均線升穿長均線")
                elif strat_idx == 6:  # MA6: 短均線升穿長均線做空
                    if prev_short <= prev_long and curr_short > curr_long:
                        signal.iloc[i] = -1
                        signal_count += 1
                        # if signal_count <= 5:
                        #     print(f"[DEBUG] 信號-1生成：位置{i}, 短均線升穿長均線")
                elif strat_idx == 7:  # MA7: 短均線跌穿長均線做多
                    if prev_short >= prev_long and curr_short < curr_long:
                        signal.iloc[i] = 1
                        signal_count += 1
                        # if signal_count <= 5:
                        #     print(f"[DEBUG] 信號1生成：位置{i}, 短均線跌穿長均線")
                elif strat_idx == 8:  # MA8: 短均線跌穿長均線做空
                    if prev_short >= prev_long and curr_short < curr_long:
                        signal.iloc[i] = -1
                        signal_count += 1
                        # if signal_count <= 5:
                        #     print(f"[DEBUG] 信號-1生成：位置{i}, 短均線跌穿長均線")
            
            # print(f"[DEBUG] 雙均線模式完成，總信號數：{signal_count}")
        
        # 最終統計
        signal_counts = signal.value_counts().to_dict()
        # print(f"[DEBUG] 最終信號分佈：{signal_counts}")
        # print(f"[DEBUG] 非零信號數量：{(signal != 0).sum()}")
        
        return signal 

    def get_min_valid_index(self):
        mode = self.params.get_param("mode", "single")
        if mode == "single":
            period = self.params.get_param("period", 20)
            return period - 1
        elif mode == "double":
            long_period = self.params.get_param("longMA_period", 20)
            return long_period - 1
        else:
            return 0 