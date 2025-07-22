"""
【主表格欄位詳細規格】（2024-07-16 最新）
------------------------------------------------------------
| 欄位名稱               | 型態      | 出現時機           | 說明                                         | 產生位置           |
|------------------------|-----------|--------------------|----------------------------------------------|--------------------|
| Time                   | datetime  | 每筆皆有           | 交易紀錄發生時間                             | TradeRecordExporter|
| Open                   | float     | 每筆皆有           | 開盤價                                       | TradeRecordExporter|
| High                   | float     | 每筆皆有           | 最高價                                       | TradeRecordExporter|
| Low                    | float     | 每筆皆有           | 最低價                                       | TradeRecordExporter|
| Close                  | float     | 每筆皆有           | 收盤價                                       | TradeRecordExporter|
| Trading_Instrument     | str       | 每筆皆有           | 標的代碼                                     | TradeRecordExporter|
| Position_Type          | str       | 開倉或平倉時有      | 持倉方向（如 new_long/new_short/close_long/close_short）| TradeRecordExporter|
| Open_Position_Price    | float     | 每筆皆有           | 開倉價格 (平時為0)                           | TradeRecordExporter|
| Close_Position_Price   | float     | 每筆皆有           | 平倉價格 (平時為0)                           | TradeRecordExporter|
| Position_Size          | float/int | 每筆皆有           | 持倉數量 (0=沒有持倉,1=有持倉)               | TradeRecordExporter|
| Return                 | float     | 每筆皆有           | 每日報酬率（小數）(沒有持倉=0)                | TradeRecordExporter|
| Trade_Group_ID         | int/str   | 持倉時有           | 交易分組流水號                               | TradeRecordExporter|
| Trade_Action           | str       | 每筆皆有           | 交易動作(1=開倉,2=平倉,3=減倉,4=平倉,0=沒有動作)| TradeRecordExporter|
| Open_Time              | datetime  | 開倉時有           | 只在開倉時有值                               | TradeRecordExporter|
| Close_Time             | datetime  | 平倉時有           | 只在平倉時有值                               | TradeRecordExporter|
| Parameter_Set_ID       | int/str   | 每筆皆有           | 參數組流水號                                 | TradeRecordExporter|
| Equity_Value           | float     | 每筆皆有           | 每筆後的資產淨值                             | TradeRecordExporter|
| Transaction_Cost       | float     | 每筆皆有           | 交易時固定百份比手續費                       | TradeRecordExporter|
| Slippage_Cost          | float     | 每筆皆有           | 交易時固定百份比滑價                         | TradeRecordExporter|
| Predictor_value        | float     | 每筆皆有           | 預測器分數/值                                | TradeRecordExporter|
| Entry_Signal           | 任意      | 每筆皆有           | 進場訊號內容(觸發時為1,平時為0)              | TradeRecordExporter|
| Exit_Signal            | 任意      | 每筆皆有           | 出場訊號內容(觸發時為-1,平時為0)             | TradeRecordExporter|
| Holding_period_count   | int       | 每筆皆有           | 持倉天數（每日計算），無持倉時為0             | TradeRecordExporter|
| Holding_period         | int/float | 平倉時有           | 持倉天數（最終持倉時間）                     | TradeRecordExporter|
| Trade_return           | float     | 平倉時有           | 一筆交易的百分比報酬率（僅平倉時有值）        | TradeRecordExporter|
| Backtest_id            | str       | 每筆皆有           | 批次唯一識別碼，主表格與 meta 對應           | TradeRecordExporter|
| Drawdown               | float     | 每筆皆有           | 最大回撤（小數）                             | MetricsCalculator  |
| BAH_Equity             | float     | 每筆皆有           | Buy & Hold 模擬資產淨值（小數）              | MetricsCalculator  |
| BAH_Return             | float     | 每筆皆有           | Buy & Hold 當日報酬率（小數）                | MetricsCalculator  |
| BAH_Drawdown           | float     | 每筆皆有           | B&H 最大回撤（小數）                         | 未開發            |
------------------------------------------------------------
"""
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import os
import json

class MetricsCalculatorMetricTracker:
    def __init__(self, df, time_unit, risk_free_rate):
        self.df = df.copy()
        self.time_unit = time_unit
        self.risk_free_rate = risk_free_rate
        self.daily_returns = self.df['Return']  # Return 已是小數
        # 自動偵測年數
        if 'Time' in self.df.columns:
            t0 = pd.to_datetime(self.df['Time'].iloc[0])
            t1 = pd.to_datetime(self.df['Time'].iloc[-1])
            self.years = (t1 - t0).days / self.time_unit
            if self.years <= 0:
                self.years = 1.0
        else:
            self.years = 1.0
        ##print(f"[Metrics] 自動偵測回測年數: {self.years:.3f} 年")

    def total_return(self):
        # 總回報率 = (最終淨值 / 初始淨值 - 1)，小數值
        equity = self.df['Equity_value']
        return equity.iloc[-1] / equity.iloc[0] - 1

    def annualized_return(self):
        # 年化回報率 = [(1 + 總回報率)^(1 / 年數) - 1]，小數值
        tr = self.total_return()
        return pow(1 + tr, 1 / self.years) - 1

    def cagr(self):
        # CAGR = [(最終淨值 / 初始淨值)^(1 / 年數) - 1]，小數值
        equity = self.df['Equity_value']
        return pow(equity.iloc[-1] / equity.iloc[0], 1 / self.years) - 1

    def std(self):
        # 標準差（小數值）
        return np.std(self.daily_returns, ddof=1)

    def annualized_std(self):
        # 年化標準差（小數值）
        return self.std() * np.sqrt(self.time_unit)

    def downside_risk(self, target=0):
        # 下行風險（小數值）
        downside = self.daily_returns[self.daily_returns < target]
        if len(downside) == 0:
            return 0.0
        return np.sqrt(np.mean((downside - target) ** 2))

    def annualized_downside_risk(self, target=0):
        # 年化下行風險（小數值）
        return self.downside_risk(target) * np.sqrt(self.time_unit)

    def max_drawdown(self):
        # 最大回撤（小數值）
        equity = self.df['Equity_value']
        roll_max = equity.cummax()
        drawdown = (equity - roll_max) / roll_max
        return drawdown.min()

    def average_drawdown(self):
        # 平均回撤（小數值）
        equity = self.df['Equity_value']
        roll_max = equity.cummax()
        drawdown = (equity - roll_max) / roll_max
        dd = drawdown[drawdown < 0]
        if len(dd) == 0:
            return 0.0
        # 分段計算每次回撤
        in_drawdown = False
        dds = []
        cur_dd = 0
        for val in drawdown:
            if val < 0:
                if not in_drawdown:
                    in_drawdown = True
                    cur_dd = val
                else:
                    cur_dd = min(cur_dd, val)
            else:
                if in_drawdown:
                    dds.append(cur_dd)
                    in_drawdown = False
        if in_drawdown:
            dds.append(cur_dd)
        if len(dds) == 0:
            return 0.0
        return np.mean(dds)

    def recovery_factor(self):
        # 恢復因子 = 總回報率 / abs(最大回撤)
        tr = self.total_return()
        mdd = abs(self.max_drawdown())
        if mdd == 0:
            return np.nan
        return tr / mdd

    """
    def cov(self):
        # 變異係數（以年度報酬率為單位，小數值）
        df = self.df.copy()
        df['year'] = pd.to_datetime(df['Time']).dt.year
        annual_returns = df.groupby('year')['Return'].sum()
        mu = annual_returns.mean()
        sigma = annual_returns.std(ddof=1)
        if mu == 0:
            return np.nan
        return sigma / mu
    """

    def bah_total_return(self):
        bah_equity = self.df['BAH_Equity']
        return bah_equity.iloc[-1] / bah_equity.iloc[0] - 1

    def bah_annualized_return(self):
        tr = self.bah_total_return()
        return pow(1 + tr, 1 / self.years) - 1

    def bah_cagr(self):
        bah_equity = self.df['BAH_Equity']
        return pow(bah_equity.iloc[-1] / bah_equity.iloc[0], 1 / self.years) - 1

    def bah_std(self):
        return np.std(self.df['BAH_Return'], ddof=1)

    def bah_annualized_std(self):
        return self.bah_std() * np.sqrt(self.time_unit)

    def bah_downside_risk(self, target=0):
        downside = self.df['BAH_Return'][self.df['BAH_Return'] < target]
        if len(downside) == 0:
            return 0.0
        return np.sqrt(np.mean((downside - target) ** 2))

    def bah_annualized_downside_risk(self, target=0):
        return self.bah_downside_risk(target) * np.sqrt(self.time_unit)

    def bah_max_drawdown(self):
        if 'BAH_Drawdown' in self.df.columns:
            return self.df['BAH_Drawdown'].min()
        bah_equity = self.df['BAH_Equity']
        roll_max = bah_equity.cummax()
        drawdown = (bah_equity - roll_max) / roll_max
        return drawdown.min()

    def bah_average_drawdown(self):
        if 'BAH_Drawdown' in self.df.columns:
            dd = self.df['BAH_Drawdown'][self.df['BAH_Drawdown'] < 0]
        else:
            bah_equity = self.df['BAH_Equity']
            roll_max = bah_equity.cummax()
            dd = (bah_equity - roll_max) / roll_max
            dd = dd[dd < 0]
        if len(dd) == 0:
            return 0.0
        return dd.mean()

    def bah_recovery_factor(self):
        tr = self.bah_total_return()
        mdd = abs(self.bah_max_drawdown())
        if mdd == 0:
            return np.nan
        return tr / mdd

    def bah_cov(self):
        mu = self.df['BAH_Return'].mean()
        sigma = self.df['BAH_Return'].std(ddof=1)
        if mu == 0:
            return np.nan
        return sigma / mu

    def bah_sharpe(self):
        mean = self.df['BAH_Return'].mean()
        std = self.df['BAH_Return'].std(ddof=1)
        rf = self.risk_free_rate / self.time_unit
        if std == 0:
            return np.nan
        return ((mean - rf) / std) * np.sqrt(self.time_unit)

    def bah_sortino(self):
        mean = self.df['BAH_Return'].mean()
        downside = self.bah_downside_risk()
        rf = self.risk_free_rate / self.time_unit
        if downside == 0:
            return np.nan
        return (mean - rf) / downside * np.sqrt(self.time_unit)

    def bah_calmar(self):
        ann = self.bah_annualized_return()
        rf_year = self.risk_free_rate 
        mdd = abs(self.bah_max_drawdown())
        if mdd == 0:
            return np.nan
        return ann-rf_year / mdd

    def sharpe(self):
        mean = self.df['Return'].mean()
        std = self.df['Return'].std(ddof=1)
        rf = self.risk_free_rate / self.time_unit
        if std == 0:
            return np.nan
        return (mean - rf) / std * np.sqrt(self.time_unit)

    def sortino(self):
        mean = self.df['Return'].mean()
        downside = self.downside_risk()
        rf = self.risk_free_rate / self.time_unit
        if downside == 0:
            return np.nan
        return (mean - rf) / downside * np.sqrt(self.time_unit)

    def calmar(self):
        ann = self.annualized_return()
        rf_year = self.risk_free_rate 
        mdd = abs(self.max_drawdown())
        if mdd == 0:
            return np.nan
        return ann-rf_year / mdd

    def information_ratio(self):
        """信息比率 (Information Ratio)：衡量策略相對 Buy & Hold 的超額報酬穩定性"""
        if 'Return' not in self.df.columns or 'BAH_Return' not in self.df.columns:
            return None
        diff = self.df['Return'] - self.df['BAH_Return']
        mean_excess = diff.mean()
        tracking_error = diff.std(ddof=1)
        if tracking_error == 0:
            return None
        return mean_excess / tracking_error

    def beta(self):
        """Beta：衡量策略與市場（B&H）的相關性和系統性風險敞口"""
        if 'Return' not in self.df.columns or 'BAH_Return' not in self.df.columns:
            return None
        x = self.df['Return']
        y = self.df['BAH_Return']
        cov = np.cov(x, y, ddof=1)[0, 1]
        var = np.var(y, ddof=1)
        if var == 0:
            return None
        return cov / var

    def alpha(self):
        """Alpha：策略相對市場的超額回報，基於CAPM模型"""
        if 'Return' not in self.df.columns or 'BAH_Return' not in self.df.columns:
            return None
        rf = self.risk_free_rate / self.time_unit if hasattr(self, 'risk_free_rate') and hasattr(self, 'time_unit') else 0
        beta = self.beta()
        mean_return = self.df['Return'].mean()
        mean_bah = self.df['BAH_Return'].mean()
        if beta is None:
            return None
        return mean_return - (rf + beta * (mean_bah - rf))

    def trade_count(self):
        """交易次數 (Trade_count)：只計算開倉次數 (Trade_action == 1)"""
        if 'Trade_action' not in self.df.columns:
            return None
        return int((self.df['Trade_action'] == 1).sum())

    def win_rate(self):
        """勝率 (Win_rate)：盈利交易佔總平倉交易的比例"""
        if 'Trade_action' not in self.df.columns or 'Trade_return' not in self.df.columns:
            return None
        closed = self.df[self.df['Trade_action'] == 4]
        if len(closed) == 0:
            return None
        wins = (closed['Trade_return'] > 0).sum()
        return wins / len(closed)

    def profit_factor(self):
        """盈虧比 (Profit_factor)：總盈利除以總虧損"""
        if 'Trade_return' not in self.df.columns:
            return None
        profits = self.df['Trade_return'][self.df['Trade_return'] > 0].sum()
        losses = self.df['Trade_return'][self.df['Trade_return'] < 0].sum()
        if losses == 0:
            return None
        return profits / abs(losses)

    def avg_trade_return(self):
        """平均交易回報 (Avg_trade_return)：每筆交易的平均收益"""
        if 'Trade_return' not in self.df.columns:
            return None
        return self.df['Trade_return'].mean()

    # expectancy 方法與相關調用已刪除

    def max_consecutive_losses(self):
        """最大連續虧損 (Max_consecutive_losses)：連續虧損交易的最大次數"""
        if 'Trade_return' not in self.df.columns:
            return None
        trade_returns = self.df['Trade_return'].dropna()
        max_count = count = 0
        for r in trade_returns:
            if r < 0:
                count += 1
                max_count = max(max_count, count)
            else:
                count = 0
        return max_count

    def exposure_time(self):
        """持倉時間比例 (Exposure_time)：持倉時間佔總時間的比例"""
        if 'Position_size' not in self.df.columns:
            return None
        return (self.df['Position_size'] != 0).sum() / len(self.df['Position_size']) * 100

    def max_holding_period_ratio(self):
        """最長持倉時間比例 (Max_holding_period_ratio)：單次持倉時間的最長持續時間佔總回測時間的比例"""
        if 'Position_size' not in self.df.columns:
            return None
        max_period = period = 0
        for p in self.df['Position_size']:
            if p != 0:
                period += 1
                max_period = max(max_period, period)
            else:
                period = 0
        if len(self.df['Position_size']) == 0:
            return None
        return max_period / len(self.df['Position_size'])

    def calc_strategy_metrics(self):
        return {
            'Total_return': self.total_return(),
            'Annualized_return (CAGR)': self.annualized_return(),
            'Std': self.std(),
            'Annualized_std': self.annualized_std(),
            'Downside_risk': self.downside_risk(),
            'Annualized_downside_risk': self.annualized_downside_risk(),
            'Max_drawdown': self.max_drawdown(),
            'Average_drawdown': self.average_drawdown(),
            'Recovery_factor': self.recovery_factor(),
            'Sharpe': self.sharpe(),
            'Sortino': self.sortino(),
            'Calmar': self.calmar(),
            'Information_ratio': self.information_ratio(),
            'Alpha': self.alpha(),
            'Beta': self.beta(),
            'Trade_count': self.trade_count(),
            'Win_rate': self.win_rate(),
            'Profit_factor': self.profit_factor(),
            'Avg_trade_return': self.avg_trade_return(),
            'Max_consecutive_losses': self.max_consecutive_losses(),
            'Exposure_time': self.exposure_time(),
            'Max_holding_period_ratio': self.max_holding_period_ratio(),
        }

    def calc_bah_metrics(self):
        return {
            'BAH_Total_return': self.bah_total_return(),
            'BAH_Annualized_return (CAGR)': self.bah_annualized_return(),
            'BAH_Std': self.bah_std(),
            'BAH_Annualized_std': self.bah_annualized_std(),
            'BAH_Downside_risk': self.bah_downside_risk(),
            'BAH_Annualized_downside_risk': self.bah_annualized_downside_risk(),
            'BAH_Max_drawdown': self.bah_max_drawdown(),
            'BAH_Average_drawdown': self.bah_average_drawdown(),
            'BAH_Recovery_factor': self.bah_recovery_factor(),
            # 'BAH_Cov': self.bah_cov(),
            'BAH_Sharpe': self.bah_sharpe(),
            'BAH_Sortino': self.bah_sortino(),
            'BAH_Calmar': self.bah_calmar(),
        }
