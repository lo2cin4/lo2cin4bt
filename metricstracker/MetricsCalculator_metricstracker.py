"""
MetricsCalculator_metricstracker.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 績效分析框架的績效指標計算核心模組，負責計算各種交易績效指標，包括收益率、風險指標、夏普比率、最大回撤等，提供完整的績效評估。

【流程與數據流】
------------------------------------------------------------
- 由 BaseMetricTracker 調用，對交易記錄進行績效指標計算
- 計算結果傳遞給 MetricsExporter 進行導出
- 主要數據流：

```mermaid
flowchart TD
    A[BaseMetricTracker] -->|調用| B[MetricsCalculator]
    B -->|計算績效| C[績效指標]
    B -->|傳遞結果| D[MetricsExporter]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增績效指標、計算邏輯時，請同步更新 calculate_metrics/頂部註解
- 若績效指標結構有變動，需同步更新 MetricsExporter、BaseMetricTracker 等依賴模組
- 績效指標計算邏輯如有調整，請於 README 詳列
- 新增/修改績效指標、計算邏輯時，務必同步更新本檔案與所有依賴模組
- 績效指標定義需與業界標準保持一致

【常見易錯點】
------------------------------------------------------------
- 績效指標計算邏輯錯誤會導致結果不準確
- 數據結構變動會影響計算結果
- 績效指標定義不一致會影響比較分析

【錯誤處理】
------------------------------------------------------------
- 數據缺失時提供詳細診斷
- 計算異常時提供修正建議
- 指標定義錯誤時提供標準參考

【範例】
------------------------------------------------------------
- 計算績效指標：calculator = MetricsCalculator(); metrics = calculator.calculate_metrics(df)
- 計算特定指標：calculate_sharpe_ratio(returns)

【與其他模組的關聯】
------------------------------------------------------------
- 由 BaseMetricTracker 調用，績效指標傳遞給 MetricsExporter
- 績效指標結構依賴 MetricsExporter

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，支援基本績效指標
- v1.1: 新增風險調整指標
- v1.2: 新增多維度績效分析

【參考】
------------------------------------------------------------
- 詳細績效指標定義請參閱 README
- 其他模組如有依賴本模組，請於對應檔案頂部註解標明
"""

import numpy as np
import pandas as pd


class MetricsCalculatorMetricTracker:
    def __init__(self, df, time_unit, risk_free_rate):
        self.df = df.copy()
        self.time_unit = time_unit
        self.risk_free_rate = risk_free_rate
        self.daily_returns = self.df["Return"]  # Return 已是小數
        # 自動偵測年數
        # 使用總交易週期數除以年化單位來計算年數
        total_periods = len(self.df)
        self.years = total_periods / self.time_unit
        if self.years <= 0:
            self.years = 1.0
        ##print(f"[Metrics] 自動偵測回測年數: {self.years:.3f} 年 (總週期: {total_periods}, 年化單位: {self.time_unit})")

    def _get_data_source(self, source_type="strategy"):
        """獲取數據源（策略或BAH）"""
        if source_type == "bah":
            return {
                "equity": self.df.get("BAH_Equity", self.df["Equity_value"]),
                "returns": self.df.get("BAH_Return", self.df["Return"]),
                "drawdown": self.df.get("BAH_Drawdown", None),
            }
        else:
            return {
                "equity": self.df["Equity_value"],
                "returns": self.df["Return"],
                "drawdown": None,
            }

    def _calculate_total_return(self, source_type="strategy"):
        """總回報率計算"""
        data = self._get_data_source(source_type)
        equity = data["equity"]
        return equity.iloc[-1] / equity.iloc[0] - 1

    def _calculate_annualized_return(self, source_type="strategy"):
        """年化回報率計算"""
        tr = self._calculate_total_return(source_type)
        return self._safe_power(1 + tr, 1 / self.years) - 1

    def _calculate_cagr(self, source_type="strategy"):
        """CAGR計算"""
        data = self._get_data_source(source_type)
        equity = data["equity"]
        return self._safe_power(equity.iloc[-1] / equity.iloc[0], 1 / self.years) - 1

    def _calculate_std(self, source_type="strategy"):
        """標準差計算"""
        data = self._get_data_source(source_type)
        returns = data["returns"]
        return np.std(returns, ddof=1)

    def _calculate_annualized_std(self, source_type="strategy"):
        """年化標準差計算"""
        std = self._calculate_std(source_type)
        return std * self._safe_sqrt(self.time_unit)

    def _calculate_downside_risk(self, source_type="strategy", target=0):
        """下行風險計算"""
        data = self._get_data_source(source_type)
        returns = data["returns"]
        downside = returns[returns < target]
        if len(downside) == 0:
            return 0.0
        return self._safe_sqrt(np.mean((downside - target) ** 2))

    def _calculate_annualized_downside_risk(self, source_type="strategy", target=0):
        """年化下行風險計算"""
        downside = self._calculate_downside_risk(source_type, target)
        return downside * self._safe_sqrt(self.time_unit)

    def _calculate_max_drawdown(self, source_type="strategy"):
        """最大回撤計算"""
        data = self._get_data_source(source_type)
        if source_type == "bah" and data["drawdown"] is not None:
            return data["drawdown"].min()

        equity = data["equity"]
        roll_max = equity.cummax()
        drawdown = (equity - roll_max) / roll_max
        return drawdown.min()

    def _calculate_recovery_factor(self, source_type="strategy"):
        """恢復因子計算"""
        tr = self._calculate_total_return(source_type)
        mdd = abs(self._calculate_max_drawdown(source_type))
        if mdd == 0:
            return np.nan
        return self._safe_division(tr, mdd)

    def _calculate_sharpe(self, source_type="strategy"):
        """夏普比率計算"""
        data = self._get_data_source(source_type)
        returns = data["returns"]
        mean = returns.mean()
        std = returns.std(ddof=1)
        rf = self.risk_free_rate / self.time_unit
        if std == 0:
            return np.nan
        return self._safe_division(mean - rf, std) * self._safe_sqrt(self.time_unit)

    def _calculate_sortino(self, source_type="strategy"):
        """索提諾比率計算"""
        data = self._get_data_source(source_type)
        returns = data["returns"]
        mean = returns.mean()
        downside = self._calculate_downside_risk(source_type)
        rf = self.risk_free_rate / self.time_unit
        if downside == 0:
            return np.nan
        return self._safe_division(mean - rf, downside) * self._safe_sqrt(
            self.time_unit
        )

    def _calculate_calmar(self, source_type="strategy"):
        """卡爾馬比率計算"""
        ann = self._calculate_annualized_return(source_type)
        rf_year = self.risk_free_rate
        mdd = abs(self._calculate_max_drawdown(source_type))
        if mdd == 0:
            return np.nan
        return self._safe_division(ann - rf_year, mdd)

    def total_return(self):
        # 總回報率 = (最終淨值 / 初始淨值 - 1)，小數值
        return self._calculate_total_return("strategy")

    def annualized_return(self):
        # 年化回報率 = [(1 + 總回報率)^(1 / 年數) - 1]，小數值
        return self._calculate_annualized_return("strategy")

    def cagr(self):
        # CAGR = [(最終淨值 / 初始淨值)^(1 / 年數) - 1]，小數值
        return self._calculate_cagr("strategy")

    def std(self):
        # 標準差（小數值）
        return self._calculate_std("strategy")

    def annualized_std(self):
        # 年化標準差（小數值）
        return self._calculate_annualized_std("strategy")

    def downside_risk(self, target=0):
        # 下行風險（小數值）
        return self._calculate_downside_risk("strategy", target)

    def annualized_downside_risk(self, target=0):
        # 年化下行風險（小數值）
        return self._calculate_annualized_downside_risk("strategy", target)

    def max_drawdown(self):
        # 最大回撤（小數值）
        return self._calculate_max_drawdown("strategy")

    def average_drawdown(self):
        # 平均回撤（小數值）
        equity = self.df["Equity_value"]
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
        return self._calculate_recovery_factor("strategy")

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
        return self._calculate_total_return("bah")

    def bah_annualized_return(self):
        return self._calculate_annualized_return("bah")

    def bah_cagr(self):
        return self._calculate_cagr("bah")

    def bah_std(self):
        return self._calculate_std("bah")

    def bah_annualized_std(self):
        return self._calculate_annualized_std("bah")

    def bah_downside_risk(self, target=0):
        return self._calculate_downside_risk("bah", target)

    def bah_annualized_downside_risk(self, target=0):
        return self._calculate_annualized_downside_risk("bah", target)

    def bah_max_drawdown(self):
        if "BAH_Drawdown" in self.df.columns:
            return self.df["BAH_Drawdown"].min()
        bah_equity = self.df["BAH_Equity"]
        roll_max = bah_equity.cummax()
        drawdown = (bah_equity - roll_max) / roll_max
        return drawdown.min()

    def bah_average_drawdown(self):
        if "BAH_Drawdown" in self.df.columns:
            dd = self.df["BAH_Drawdown"][self.df["BAH_Drawdown"] < 0]
        else:
            bah_equity = self.df["BAH_Equity"]
            roll_max = bah_equity.cummax()
            dd = (bah_equity - roll_max) / roll_max
            dd = dd[dd < 0]
        if len(dd) == 0:
            return 0.0
        return dd.mean()

    def bah_recovery_factor(self):
        return self._calculate_recovery_factor("bah")

    def bah_cov(self):
        mu = self.df["BAH_Return"].mean()
        sigma = self.df["BAH_Return"].std(ddof=1)
        if mu == 0:
            return np.nan
        return self._safe_division(sigma, mu)

    def bah_sharpe(self):
        return self._calculate_sharpe("bah")

    def bah_sortino(self):
        return self._calculate_sortino("bah")

    def bah_calmar(self):
        return self._calculate_calmar("bah")

    def sharpe(self):
        return self._calculate_sharpe("strategy")

    def sortino(self):
        return self._calculate_sortino("strategy")

    def calmar(self):
        return self._calculate_calmar("strategy")

    def information_ratio(self):
        """信息比率 (Information Ratio)：衡量策略相對 Buy & Hold 的超額報酬穩定性"""
        if "Return" not in self.df.columns or "BAH_Return" not in self.df.columns:
            return None
        diff = self.df["Return"] - self.df["BAH_Return"]
        mean_excess = diff.mean()
        tracking_error = diff.std(ddof=1)
        if tracking_error == 0:
            return None
        return mean_excess / tracking_error

    def beta(self):
        """Beta：衡量策略與市場（B&H）的相關性和系統性風險敞口"""
        if "Return" not in self.df.columns or "BAH_Return" not in self.df.columns:
            return None
        x = self.df["Return"]
        y = self.df["BAH_Return"]
        cov = np.cov(x, y, ddof=1)[0, 1]
        var = np.var(y, ddof=1)
        if var == 0:
            return None
        return cov / var

    def alpha(self):
        """Alpha：策略相對市場的超額回報，基於CAPM模型"""
        if "Return" not in self.df.columns or "BAH_Return" not in self.df.columns:
            return None
        rf = (
            self.risk_free_rate / self.time_unit
            if hasattr(self, "risk_free_rate") and hasattr(self, "time_unit")
            else 0
        )
        beta = self.beta()
        mean_return = self.df["Return"].mean()
        mean_bah = self.df["BAH_Return"].mean()
        if beta is None:
            return None
        return mean_return - (rf + beta * (mean_bah - rf))

    def trade_count(self):
        """交易次數 (Trade_count)：只計算開倉次數 (Trade_action == 1)"""
        if "Trade_action" not in self.df.columns:
            return None
        return int((self.df["Trade_action"] == 1).sum())

    def win_rate(self):
        """勝率 (Win_rate)：盈利交易佔總平倉交易的比例"""
        if (
            "Trade_action" not in self.df.columns
            or "Trade_return" not in self.df.columns
        ):
            return None
        closed = self.df[self.df["Trade_action"] == 4]
        if len(closed) == 0:
            return None
        wins = (closed["Trade_return"] > 0).sum()
        return wins / len(closed)

    def profit_factor(self):
        """盈虧比 (Profit_factor)：總盈利除以總虧損"""
        if "Trade_return" not in self.df.columns:
            return None
        profits = self.df["Trade_return"][self.df["Trade_return"] > 0].sum()
        losses = self.df["Trade_return"][self.df["Trade_return"] < 0].sum()
        if losses == 0:
            return None
        return self._safe_division(profits, abs(losses))

    def avg_trade_return(self):
        """平均交易回報 (Avg_trade_return)：每筆交易的平均收益"""
        if "Trade_return" not in self.df.columns:
            return None
        return self.df["Trade_return"].mean()

    # expectancy 方法與相關調用已刪除

    def max_consecutive_losses(self):
        """最大連續虧損 (Max_consecutive_losses)：連續虧損交易的最大次數"""
        if "Trade_return" not in self.df.columns:
            return None
        trade_returns = self.df["Trade_return"].dropna()
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
        if "Position_size" not in self.df.columns:
            return None
        return (
            (self.df["Position_size"] != 0).sum() / len(self.df["Position_size"]) * 100
        )

    def max_holding_period_ratio(self):
        """最長持倉時間比例 (Max_holding_period_ratio)：單次持倉時間的最長持續時間佔總回測時間的比例"""
        if "Position_size" not in self.df.columns:
            return None
        max_period = period = 0
        for p in self.df["Position_size"]:
            if p != 0:
                period += 1
                max_period = max(max_period, period)
            else:
                period = 0
        if len(self.df["Position_size"]) == 0:
            return None
        return max_period / len(self.df["Position_size"])

    def calc_strategy_metrics(self):
        return {
            "Total_return": self.total_return(),
            "Annualized_return (CAGR)": self.annualized_return(),
            "Std": self.std(),
            "Annualized_std": self.annualized_std(),
            "Downside_risk": self.downside_risk(),
            "Annualized_downside_risk": self.annualized_downside_risk(),
            "Max_drawdown": self.max_drawdown(),
            "Average_drawdown": self.average_drawdown(),
            "Recovery_factor": self.recovery_factor(),
            "Sharpe": self.sharpe(),
            "Sortino": self.sortino(),
            "Calmar": self.calmar(),
            "Information_ratio": self.information_ratio(),
            "Alpha": self.alpha(),
            "Beta": self.beta(),
            "Trade_count": self.trade_count(),
            "Win_rate": self.win_rate(),
            "Profit_factor": self.profit_factor(),
            "Avg_trade_return": self.avg_trade_return(),
            "Max_consecutive_losses": self.max_consecutive_losses(),
            "Exposure_time": self.exposure_time(),
            "Max_holding_period_ratio": self.max_holding_period_ratio(),
        }

    def calc_bah_metrics(self):
        return {
            "BAH_Total_return": self.bah_total_return(),
            "BAH_Annualized_return (CAGR)": self.bah_annualized_return(),
            "BAH_Std": self.bah_std(),
            "BAH_Annualized_std": self.bah_annualized_std(),
            "BAH_Downside_risk": self.bah_downside_risk(),
            "BAH_Annualized_downside_risk": self.bah_annualized_downside_risk(),
            "BAH_Max_drawdown": self.bah_max_drawdown(),
            "BAH_Average_drawdown": self.bah_average_drawdown(),
            "BAH_Recovery_factor": self.bah_recovery_factor(),
            # 'BAH_Cov': self.bah_cov(),
            "BAH_Sharpe": self.bah_sharpe(),
            "BAH_Sortino": self.bah_sortino(),
            "BAH_Calmar": self.bah_calmar(),
        }

    def _safe_power(self, base, exponent, fallback=0.0):
        """
        安全的冪運算，處理邊界情況
        Args:
            base: 底數
            exponent: 指數
            fallback: 當計算無效時的返回值
        """
        # 完全避免使用冪運算，改用條件判斷來防止 RuntimeWarning
        try:
            # 處理特殊情況
            if base <= 0 and exponent <= 0:
                return fallback
            if base == 0 and exponent > 0:
                return 0.0
            if base == 0 and exponent < 0:
                return fallback
            if np.isnan(base) or np.isnan(exponent):
                return fallback
            if np.isinf(exponent) and base == 1:
                return 1.0
            if np.isinf(exponent) and base != 1:
                return fallback

            # 特殊情況：當 base 接近 0 且 exponent 很大時，直接返回 0
            if abs(base) < 1e-10 and abs(exponent) > 100:
                return 0.0

            # 特殊情況：當 base 接近 1 且 exponent 很大時，直接返回 1
            if abs(base - 1) < 1e-10:
                return 1.0

            # 防止指數過大導致 overflow
            # 當指數絕對值超過 1000 時，使用對數運算
            if abs(exponent) > 1000:
                # 使用對數運算：base^exponent = exp(exponent * log(base))
                try:
                    with np.errstate(over='raise', invalid='raise'):
                        log_result = exponent * np.log(base)
                        # 防止結果過大
                        if log_result > 700:  # exp(700) 接近 float 的最大值
                            return fallback
                        elif log_result < -700:
                            return 0.0
                        result = np.exp(log_result)
                        if np.isnan(result) or np.isinf(result):
                            return fallback
                        return result
                except (ValueError, OverflowError, FloatingPointError):
                    return fallback

            # 如果所有檢查都通過，才進行冪運算
            if base > 0:  # 只對正數進行冪運算
                # 使用 errstate 捕獲 RuntimeWarning
                with np.errstate(over='ignore', invalid='ignore'):
                    result = np.power(base, exponent)
                    if np.isnan(result) or np.isinf(result):
                        return fallback
                    return result
            else:
                return fallback

        except (ValueError, OverflowError):
            return fallback

    def _safe_sqrt(self, value, fallback=0.0):
        """
        安全的平方根運算
        """
        try:
            if value < 0 or np.isnan(value) or np.isinf(value):
                return fallback
            result = np.sqrt(value)
            if np.isnan(result) or np.isinf(result):
                return fallback
            return result
        except (ValueError, RuntimeWarning):
            return fallback

    def _safe_division(self, numerator, denominator, fallback=0.0):
        """
        安全的除法運算
        """
        try:
            if denominator == 0 or np.isnan(denominator) or np.isinf(denominator):
                return fallback
            if np.isnan(numerator) or np.isinf(numerator):
                return fallback
            result = numerator / denominator
            if np.isnan(result) or np.isinf(result):
                return fallback
            return result
        except (ValueError, RuntimeWarning):
            return fallback
