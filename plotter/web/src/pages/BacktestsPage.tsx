import { useEffect, useMemo, useState } from 'react'
import { useNavigate, useRouterState } from '../routing'
import { useQuery } from '@tanstack/react-query'

import { api } from '../api'
import { makeChartLayout, plotConfig } from '../chartTheme'
import { Language, useCopy } from '../i18n'
import { BenchmarkToggleButton } from '../components/BenchmarkToggleButton'
import { InfoHint } from '../components/InfoHint'
import { Plot, preloadPlotly } from '../components/LazyPlot'
import { MissingState } from '../components/MissingState'
import { SectionCard } from '../components/SectionCard'
import { localizeStrategyRuleValue } from '../components/StrategyRulesPanel'
import { TradeMarkerInfoPanel } from '../components/TradeMarkerInfoPanel'
import { TradeMarkersToggleButton } from '../components/TradeMarkersToggleButton'
import { useAppStore } from '../store'
import {
  PinnedTradeMarker,
  buildTradeMarkerTraces,
  pinnedTradeMarkerFromPlotClick,
} from '../tradeMarkers'
import { benchmarkDisplayLabel, dataHealthLabel, tradeStatusLabel, uiText } from '../uiVocabulary'

type MetricFormat = 'percent' | 'ratio' | 'count'
type EquityScale = 'linear' | 'log'

const KPI_META: Record<
  string,
  {
    label: string
    format: MetricFormat
    tone?: 'performance' | 'risk' | 'neutral'
  }
> = {
  total_return: { label: '總報酬', format: 'percent', tone: 'performance' },
  cagr: { label: '年化報酬', format: 'percent', tone: 'performance' },
  sharpe: { label: '夏普比率', format: 'ratio', tone: 'performance' },
  sortino: { label: '索提諾比率', format: 'ratio', tone: 'performance' },
  calmar: { label: '卡瑪比率', format: 'ratio', tone: 'performance' },
  max_drawdown: { label: '最大回撤', format: 'percent', tone: 'risk' },
  mdd: { label: '最大回撤', format: 'percent', tone: 'risk' },
  average_drawdown: { label: '平均回撤', format: 'percent', tone: 'risk' },
  recovery_factor: { label: '恢復因子', format: 'ratio', tone: 'performance' },
  std: { label: '波動率', format: 'percent', tone: 'neutral' },
  annualized_std: { label: '年化波動率', format: 'percent', tone: 'neutral' },
  annualized_downside_risk: { label: '年化下行風險', format: 'percent', tone: 'risk' },
  information_ratio: { label: '資訊比率', format: 'ratio', tone: 'neutral' },
  benchmark_correlation: { label: '與基準相關性', format: 'ratio', tone: 'neutral' },
  exposure_time: { label: '曝險時間', format: 'percent', tone: 'neutral' },
  avg_gross_exposure: { label: '平均曝險', format: 'percent', tone: 'neutral' },
  avg_turnover: { label: '平均交易換手率', format: 'percent', tone: 'neutral' },
  avg_trade_return: { label: '平均交易報酬', format: 'percent', tone: 'performance' },
  trade_count: { label: '交易次數', format: 'count', tone: 'neutral' },
  trade_events: { label: '交易事件', format: 'count', tone: 'neutral' },
  rebalance_count: { label: '檢查點', format: 'count', tone: 'neutral' },
  win_rate: { label: '勝率', format: 'percent', tone: 'performance' },
  profitable_period_ratio: { label: '獲利月份比例', format: 'percent', tone: 'performance' },
  percent_profitable_trades: { label: '獲利交易比例', format: 'percent', tone: 'performance' },
  profit_factor: { label: '獲利因子', format: 'ratio', tone: 'performance' },
  average_win: { label: '平均獲利', format: 'percent', tone: 'performance' },
  average_loss: { label: '平均虧損', format: 'percent', tone: 'risk' },
  average_win_loss_ratio: { label: '平均盈虧比', format: 'ratio', tone: 'performance' },
  gain_loss_ratio: { label: '盈虧比', format: 'ratio', tone: 'performance' },
  gross_profit: { label: '總盈利', format: 'percent', tone: 'performance' },
  gross_loss: { label: '總虧損', format: 'percent', tone: 'risk' },
  max_consecutive_wins: { label: '最長連勝', format: 'count', tone: 'performance' },
  max_consecutive_losses: { label: '最長連敗', format: 'count', tone: 'risk' },
  max_drawdown_duration_days: { label: '最大回撤持續日數', format: 'count', tone: 'risk' },
  max_drawdown_duration_periods: { label: '最大回撤持續期數', format: 'count', tone: 'risk' },
  skewness: { label: '偏度', format: 'ratio', tone: 'neutral' },
  kurtosis: { label: '峰度', format: 'ratio', tone: 'neutral' },
  var_95: { label: 'VaR 95%', format: 'percent', tone: 'risk' },
  cvar_95: { label: 'CVaR 95%', format: 'percent', tone: 'risk' },
  var_99: { label: 'VaR 99%', format: 'percent', tone: 'risk' },
  cvar_99: { label: 'CVaR 99%', format: 'percent', tone: 'risk' },
  best_month: { label: '最佳月份', format: 'percent', tone: 'performance' },
  worst_month: { label: '最差月份', format: 'percent', tone: 'risk' },
  positive_month_ratio: { label: '正報酬月份比例', format: 'percent', tone: 'performance' },
  bah_total_return: { label: '買入持有報酬', format: 'percent', tone: 'neutral' },
  bah_cagr: { label: '買入持有年化報酬', format: 'percent', tone: 'neutral' },
  bah_sharpe: { label: '買入持有夏普比率', format: 'ratio', tone: 'neutral' },
  bah_calmar: { label: '買入持有卡瑪比率', format: 'ratio', tone: 'neutral' },
  excess_return: { label: '超額報酬', format: 'percent', tone: 'performance' },
  trade_cost_drag: { label: '成本拖累', format: 'percent', tone: 'risk' },
  final_equity: { label: '期末資金', format: 'ratio', tone: 'neutral' },
  entry_time: { label: '入場時間', format: 'ratio', tone: 'neutral' },
  exit_time: { label: '出場時間', format: 'ratio', tone: 'neutral' },
  equity_value: { label: '資金值', format: 'ratio', tone: 'neutral' },
  price_pnl: { label: '每單位價差盈虧', format: 'ratio', tone: 'neutral' },
  holding_period: { label: '持有期', format: 'count', tone: 'neutral' },
}

function asNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value
  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value)
    if (Number.isFinite(parsed)) return parsed
  }
  return null
}

const EN_KPI_LABELS: Record<string, string> = {
  total_return: 'Total Return',
  cagr: 'CAGR',
  sharpe: 'Sharpe',
  sortino: 'Sortino',
  calmar: 'Calmar',
  max_drawdown: 'Max Drawdown',
  mdd: 'Max Drawdown',
  average_drawdown: 'Average Drawdown',
  recovery_factor: 'Recovery Factor',
  std: 'Volatility',
  annualized_std: 'Annualized Volatility',
  annualized_downside_risk: 'Annualized Downside Risk',
  information_ratio: 'Information Ratio',
  benchmark_correlation: 'Benchmark Correlation',
  exposure_time: 'Exposure Time',
  avg_gross_exposure: 'Avg Exposure',
  avg_turnover: 'Avg Trade Turnover',
  avg_trade_return: 'Avg Trade Return',
  trade_count: 'Trade Count',
  trade_events: 'Trade Events',
  rebalance_count: 'Checkpoints',
  win_rate: 'Win Rate',
  profitable_period_ratio: 'Profitable Months',
  percent_profitable_trades: 'Profitable Trades',
  profit_factor: 'Profit Factor',
  average_win: 'Average Win',
  average_loss: 'Average Loss',
  average_win_loss_ratio: 'Average Win / Loss',
  gain_loss_ratio: 'Gain-Loss Ratio',
  gross_profit: 'Gross Profit',
  gross_loss: 'Gross Loss',
  max_consecutive_wins: 'Max Consecutive Wins',
  max_consecutive_losses: 'Max Consecutive Losses',
  max_drawdown_duration_days: 'Max DD Duration',
  max_drawdown_duration_periods: 'Max DD Duration',
  skewness: 'Skewness',
  kurtosis: 'Kurtosis',
  var_95: 'VaR 95%',
  cvar_95: 'CVaR 95%',
  var_99: 'VaR 99%',
  cvar_99: 'CVaR 99%',
  best_month: 'Best Month',
  worst_month: 'Worst Month',
  positive_month_ratio: 'Positive Months',
  bah_total_return: 'BAH Return',
  bah_cagr: 'BAH CAGR',
  bah_sharpe: 'BAH Sharpe',
  bah_calmar: 'BAH Calmar',
  excess_return: 'Excess Return',
  trade_cost_drag: 'Cost Drag',
  final_equity: 'Final Equity',
  entry_time: 'Entry Time',
  exit_time: 'Exit Time',
  equity_value: 'Equity',
  price_pnl: 'Price PnL / Unit',
  holding_period: 'Holding Period',
}

function chartValue(value: unknown, scale: EquityScale): number | null {
  const numeric = asNumber(value)
  if (numeric === null) return null
  if (scale === 'log' && numeric <= 0) return null
  return numeric
}

function formatMetric(key: string, value: unknown): string {
  if (typeof value === 'number' && Number.isNaN(value) && key === 'profit_factor') return 'inf'
  const numeric = asNumber(value)
  if (numeric === null) return 'n/a'
  const meta = KPI_META[key]
  if (!meta) return numeric.toFixed(3)
  if (meta.format === 'count') return numeric.toFixed(0)
  if (meta.format === 'percent') return `${(numeric * 100).toFixed(1)}%`
  return numeric.toFixed(3)
}

function formatNumber(value: unknown, decimals = 3): string {
  const numeric = asNumber(value)
  return numeric === null ? 'n/a' : numeric.toFixed(decimals)
}

function formatOptionalPercent(value: unknown): string {
  const numeric = asNumber(value)
  if (numeric === null) return ''
  return `${(numeric * 100).toFixed(1)}%`
}

function formatWholeOrDecimal(value: unknown, decimals = 1): string {
  const numeric = asNumber(value)
  if (numeric === null) return 'n/a'
  const rounded = Math.round(numeric)
  if (Math.abs(numeric - rounded) < 1e-9) return String(rounded)
  return numeric.toFixed(decimals)
}

function toneClass(key: string, value: unknown): string {
  const numeric = asNumber(value)
  if (numeric === null) return 'tone-neutral'
  const tone = KPI_META[key]?.tone || 'neutral'
  const negativeIsLoss = new Set([
    'average_loss',
    'gross_loss',
    'worst_month',
    'var_95',
    'cvar_95',
    'var_99',
    'cvar_99',
  ])
  if (negativeIsLoss.has(key)) {
    if (numeric < 0) return 'tone-negative'
    if (numeric > 0) return 'tone-positive'
    return 'tone-neutral'
  }
  if (key === 'positive_month_ratio') {
    if (numeric < 0.45) return 'tone-negative'
    if (numeric < 0.5) return 'tone-warning'
    return 'tone-positive'
  }
  if (key === 'max_drawdown_duration_days' || key === 'max_drawdown_duration_periods') {
    if (numeric >= 504) return 'tone-negative'
    if (numeric >= 252) return 'tone-warning'
    return 'tone-neutral'
  }
  if (key === 'max_consecutive_losses') {
    if (numeric >= 10) return 'tone-negative'
    if (numeric >= 5) return 'tone-warning'
    return 'tone-neutral'
  }
  if (key === 'kurtosis') {
    if (numeric >= 10) return 'tone-negative'
    if (numeric >= 6) return 'tone-warning'
    return 'tone-neutral'
  }
  if (key === 'annualized_std' || key === 'annualized_downside_risk') {
    if (numeric >= 0.8) return 'tone-negative'
    if (numeric >= 0.4) return 'tone-warning'
    return 'tone-neutral'
  }
  if (tone === 'performance') {
    if (numeric > 0) return 'tone-positive'
    if (numeric < 0) return 'tone-negative'
  }
  if (tone === 'risk') {
    if (numeric < 0) return 'tone-negative'
    if (numeric > 0) return 'tone-warning'
  }
  return 'tone-neutral'
}

function metricLabel(key: string, language: Language = 'zh-Hant'): string {
  return language === 'zh-Hant'
    ? KPI_META[key]?.label || uiText(language, 'vocabulary_update_needed')
    : EN_KPI_LABELS[key] || uiText(language, 'vocabulary_update_needed')
}

const METRIC_HELP_EN: Record<string, string> = {
  total_return: 'Total percentage gain or loss produced by the strategy over the selected backtest period.',
  cagr: 'Annualized growth rate that smooths total return into an average yearly compound return.',
  sharpe: 'Risk-adjusted return measured against total volatility; higher values imply better return per unit of risk.',
  max_drawdown: 'Largest peak-to-trough equity decline observed during the backtest period.',
  average_drawdown: 'Average depth of drawdown episodes across the backtest, summarizing typical equity stress.',
  volatility: 'Variation in returns over the measured period, before annualizing the estimate.',
  annualized_volatility: 'Estimated yearly variation in returns, showing how widely strategy performance tends to fluctuate over time.',
  annualized_downside_risk: 'Estimated yearly downside volatility, focusing only on returns below the target or zero threshold.',
  sortino: 'Risk-adjusted return that penalizes downside volatility while ignoring upside variation.',
  calmar: 'Annualized return divided by maximum drawdown, highlighting reward relative to worst peak-to-trough loss.',
  max_drawdown_duration: 'Longest time the strategy stayed below a previous equity high before recovering.',
  recovery_factor: 'Net profit divided by maximum drawdown, indicating how efficiently losses were recovered.',
  information_ratio: 'Excess return relative to the benchmark divided by tracking error.',
  exposure_time: 'Share of the backtest period where the strategy held market exposure rather than staying fully in cash.',
  average_holdings: 'Average number of assets held at each portfolio checkpoint or rebalance state.',
  average_exposure: 'Average gross market exposure carried by the strategy during the backtest.',
  average_turnover: 'Average traded weight required to move from previous holdings to the next target state.',
  active_rebalances: 'Number of actual portfolio changes where target holdings or weights changed enough to rebalance.',
  average_trade_return: 'Mean return per completed trade across the available closed-trade records.',
  trade_count: 'Number of completed trades recorded for this strategy or selected backtest row.',
  skewness: 'Measures whether returns lean toward larger upside or downside outliers.',
  kurtosis: 'Measures how extreme or fat-tailed the return distribution is compared with normal returns.',
  var_95: 'Estimated loss threshold that daily returns exceed only 5% of the time.',
  cvar_95: 'Average loss when returns fall beyond the 95% VaR threshold.',
  var_99: 'Estimated loss threshold that daily returns exceed only 1% of the time.',
  cvar_99: 'Average loss when returns fall beyond the 99% VaR threshold.',
  worst_month: 'Lowest monthly return observed during the backtest period.',
  best_month: 'Highest monthly return observed during the backtest period.',
  positive_months: 'Share or count of months with returns above zero.',
  win_rate: 'Percentage of completed trades or periods that ended with a positive result.',
  profit_factor: 'Gross profit divided by gross loss, showing how much profit was earned per unit lost.',
  average_win: 'Mean gain across profitable trades or winning periods.',
  average_loss: 'Mean loss across unprofitable trades or losing periods.',
  average_win_loss: 'Average win divided by average loss, comparing typical upside against typical downside.',
  gross_profit: 'Total profit from all winning trades before subtracting losing trades.',
  gross_loss: 'Total loss from all losing trades before offsetting winning trades.',
  max_consecutive_wins: 'Longest streak of winning trades or positive outcomes in sequence.',
  max_consecutive_losses: 'Longest streak of losing trades or negative outcomes in sequence.',
  buy_and_hold_return: 'Total return from holding the asset or benchmark for the full backtest period.',
  buy_and_hold_cagr: 'Annualized growth rate from holding the asset or benchmark throughout the backtest.',
  buy_and_hold_sharpe: 'Risk-adjusted buy-and-hold return measured against volatility over the same period.',
  buy_and_hold_calmar: 'Buy-and-hold annualized return divided by its maximum drawdown over the same period.',
  benchmark_correlation: 'Correlation between strategy returns and benchmark returns, showing how closely they move together.',
  excess_return: 'Strategy return above or below the selected benchmark over the same period.',
  validation_status: 'Indicates whether the backtest inputs, data coverage, and required checks passed validation.',
  expected_assets: 'Number of assets the run expected to load from the configuration.',
  loaded_assets: 'Number of expected assets successfully loaded into the backtest.',
  missing_assets: 'Expected assets that were unavailable or failed to load for the run.',
  checkpoints: 'Saved progress markers or validation stages recorded during the backtest workflow.',
  trade_events: 'Recorded entries, exits, and position changes generated by the strategy.',
  cost_settings: 'Trading cost assumptions applied to results, including fees, slippage, or related execution costs.',
  asset: 'The asset or symbol whose contribution is being measured in this portfolio result.',
  portfolio_return: 'The selected portfolio backtest return, including allocation effects and compounding over the full period.',
  total_asset_contribution: 'Sum of estimated asset-level return contributions before residual and compounding effects.',
  residual_compounding: 'Difference between portfolio return and summed asset contributions, mainly from compounding, timing, and estimation residuals.',
  return_contribution: 'Estimated contribution from this asset to the portfolio return using its weights and returns.',
  contribution_share: 'Share of total asset contribution attributed to this asset.',
  average_weight: 'Average portfolio weight assigned to this asset while it was part of the strategy.',
  active_days: 'Number of days this asset was held or carried a non-zero target weight.',
  lag_1_serial_corr: 'Correlation between each return and the previous return, indicating short-term persistence or reversal.',
  top_20_profit_share: 'Share of total profit contributed by the most profitable 20% of trades.',
  profit_gini: 'Concentration score showing whether profits are evenly distributed or dominated by a few trades.',
  concentration_sample: 'Sample size used to estimate profit concentration statistics.',
  p50_drawdown_recovery_time: 'Median time needed for recovered drawdowns to return to a prior equity high.',
  recovered_drawdowns: 'Number of drawdowns that fully returned to a previous equity high.',
  unrecovered_drawdowns: 'Number of drawdowns still below their prior equity high by the end of the test.',
  p75_drawdown_recovery_time: 'Time by which 75% of recovered drawdowns returned to a prior equity high.',
  p90_drawdown_recovery_time: 'Time by which 90% of recovered drawdowns returned to a prior equity high.',
  longest_drawdown_recovery_time: 'Longest observed time needed for a drawdown to recover to a previous equity high.',
}

const METRIC_HELP_ZH_HANT: Record<string, string> = {
  total_return: '策略在所選回測期間產生的總百分比收益或虧損。',
  cagr: '把總回報平滑成平均每年的複利增長率。',
  sharpe: '以總波動率衡量的風險調整回報，數值越高代表每單位風險回報越好。',
  max_drawdown: '回測期間資金曲線由高位跌至低位的最大跌幅。',
  average_drawdown: '回測中各段回撤的平均深度，用來概括典型資金壓力。',
  volatility: '回報在量度期間的波動幅度，尚未年化處理。',
  annualized_volatility: '估算年度回報波動幅度，反映策略表現隨時間的起伏程度。',
  annualized_downside_risk: '估算年度下行波動，只聚焦低於目標或零值的回報。',
  sortino: '只懲罰下行波動、忽略上行波動的風險調整回報。',
  calmar: '年化回報除以最大回撤，衡量相對最差跌幅的回報表現。',
  max_drawdown_duration: '策略從前一資金高位跌落後，恢復前持續低於該高位的最長時間。',
  recovery_factor: '淨利潤除以最大回撤，反映虧損恢復效率。',
  information_ratio: '相對基準的超額回報除以追蹤誤差。',
  exposure_time: '策略在回測期間持有市場曝險、而非完全空倉的時間比例。',
  average_holdings: '每個投資組合檢查點或再平衡狀態下的平均持倉資產數。',
  average_exposure: '策略在回測期間平均承擔的總市場曝險。',
  average_turnover: '由上一個持倉狀態調整至下一個目標狀態所需的平均交易權重。',
  active_rebalances: '目標持倉或權重變動足以觸發調整的實際再平衡次數。',
  average_trade_return: '所有可用已平倉交易的平均單筆回報。',
  trade_count: '此策略或目前回測列記錄到的已完成交易數量。',
  skewness: '衡量回報是否偏向較大的上行或下行極端值。',
  kurtosis: '衡量回報分佈相對常態分佈的極端程度或肥尾程度。',
  var_95: '估算每日回報只有 5% 機率會超過的虧損門檻。',
  cvar_95: '回報跌穿 95% VaR 門檻時的平均虧損。',
  var_99: '估算每日回報只有 1% 機率會超過的虧損門檻。',
  cvar_99: '回報跌穿 99% VaR 門檻時的平均虧損。',
  worst_month: '回測期間觀察到的最低月度回報。',
  best_month: '回測期間觀察到的最高月度回報。',
  positive_months: '回報高於零的月份比例或數量。',
  win_rate: '已完成交易或期間中錄得正回報的百分比。',
  profit_factor: '總盈利除以總虧損，顯示每承擔一單位虧損帶來多少盈利。',
  average_win: '盈利交易或勝出期間的平均收益。',
  average_loss: '虧損交易或落敗期間的平均虧損。',
  average_win_loss: '平均盈利除以平均虧損，用於比較典型上行與下行幅度。',
  gross_profit: '扣除虧損交易前，所有盈利交易的總利潤。',
  gross_loss: '以盈利交易抵銷前，所有虧損交易的總虧損。',
  max_consecutive_wins: '連續盈利交易或正結果的最長次數。',
  max_consecutive_losses: '連續虧損交易或負結果的最長次數。',
  buy_and_hold_return: '整個回測期間持有資產或基準的總回報。',
  buy_and_hold_cagr: '整個回測期間持有資產或基準的年化增長率。',
  buy_and_hold_sharpe: '以同期波動率衡量的買入並持有風險調整回報。',
  buy_and_hold_calmar: '買入並持有的年化回報除以同期間最大回撤。',
  benchmark_correlation: '策略回報與基準回報的相關性，反映兩者走勢有多接近。',
  excess_return: '策略在同一期間相對所選基準的超額或落後回報。',
  validation_status: '顯示回測輸入、數據覆蓋及必要檢查是否通過驗證。',
  expected_assets: '本次運行預期按設定載入的資產數量。',
  loaded_assets: '已成功載入回測的預期資產數量。',
  missing_assets: '本次運行中無法取得或載入失敗的預期資產。',
  checkpoints: '回測流程中記錄的進度標記或驗證階段。',
  trade_events: '策略產生的進場、出場及持倉變動紀錄。',
  cost_settings: '套用於結果的交易成本假設，包括手續費、滑價及相關執行成本。',
  asset: '正在量度貢獻的投資組合資產或代號。',
  portfolio_return: '目前投資組合回測的總回報，已包含配置效果與全期間複利影響。',
  total_asset_contribution: '各資產層級估算回報貢獻的總和，未計殘差與複利效果。',
  residual_compounding: '投資組合回報與資產貢獻總和之間的差額，主要來自複利、時點與估算殘差。',
  return_contribution: '根據此資產的權重與回報估算出來，對投資組合回報的貢獻。',
  contribution_share: '此資產佔總資產貢獻的比例。',
  average_weight: '此資產在策略持有期間的平均投資組合權重。',
  active_days: '此資產被持有或具有非零目標權重的日數。',
  lag_1_serial_corr: '每期回報與上一期回報的相關性，反映短期延續或反轉傾向。',
  top_20_profit_share: '盈利最高 20% 交易對總盈利的貢獻比例。',
  profit_gini: '盈利集中度分數，顯示利潤是否平均分佈或由少數交易主導。',
  concentration_sample: '用於估算盈利集中度統計的樣本數。',
  p50_drawdown_recovery_time: '已恢復回撤回到前一資金高位所需時間的中位數。',
  recovered_drawdowns: '已完全回到前一資金高位的回撤次數。',
  unrecovered_drawdowns: '測試結束時仍低於前一資金高位的回撤次數。',
  p75_drawdown_recovery_time: '75% 已恢復回撤回到前一資金高位所需的時間。',
  p90_drawdown_recovery_time: '90% 已恢復回撤回到前一資金高位所需的時間。',
  longest_drawdown_recovery_time: '觀察到回撤恢復至前一資金高位所需的最長時間。',
}

const METRIC_HELP_KEY_BY_METRIC_KEY: Record<string, string> = {
  std: 'volatility',
  annualized_std: 'annualized_volatility',
  annualized_downside_risk: 'annualized_downside_risk',
  max_drawdown: 'max_drawdown',
  mdd: 'max_drawdown',
  average_drawdown: 'average_drawdown',
  max_drawdown_duration_days: 'max_drawdown_duration',
  max_drawdown_duration_periods: 'max_drawdown_duration',
  avg_holdings: 'average_holdings',
  avg_gross_exposure: 'average_exposure',
  avg_turnover: 'average_turnover',
  avg_trade_return: 'average_trade_return',
  trade_count: 'trade_count',
  exposure_time: 'exposure_time',
  information_ratio: 'information_ratio',
  bah_total_return: 'buy_and_hold_return',
  bah_cagr: 'buy_and_hold_cagr',
  bah_sharpe: 'buy_and_hold_sharpe',
  bah_calmar: 'buy_and_hold_calmar',
  positive_month_ratio: 'positive_months',
  average_win_loss_ratio: 'average_win_loss',
  rebalance_count: 'active_rebalances',
  trade_events: 'trade_events',
}

function helpBody(helpKey: string | undefined, language: Language): string | undefined {
  if (!helpKey) return undefined
  return language === 'zh-Hant' ? METRIC_HELP_ZH_HANT[helpKey] : METRIC_HELP_EN[helpKey]
}

function labelWithHint(
  label: string,
  helpKey: string | undefined,
  language: Language,
  side: 'left' | 'right' = 'right',
) {
  const body = helpBody(helpKey, language)
  return (
    <>
      {label}
      {body ? <InfoHint label={label} body={body} side={side} /> : null}
    </>
  )
}

function metricLabelWithHint(key: string, language: Language, side?: 'left' | 'right') {
  return labelWithHint(metricLabel(key, language), METRIC_HELP_KEY_BY_METRIC_KEY[key] || key, language, side)
}

function isClosedTradeRow(row: any): boolean {
  const status = String(row?.status || '').trim().toLowerCase()
  const hasExit = Boolean(String(row?.exit_time || '').trim())
  return status.includes('closed') || status === '' || (hasExit && !status.includes('open'))
}

function nonNegativeCount(value: unknown): number | null {
  const numeric = asNumber(value)
  if (numeric === null || numeric < 0) return null
  return Math.round(numeric)
}

function buildTradeOutcomeSummary(rows: any[], sourceSummary?: any) {
  const hasTradeReturnField = rows.some((row) => row?.trade_return !== undefined && asNumber(row.trade_return) !== null)
  const returns = rows
    .filter(isClosedTradeRow)
    .map((row) => asNumber(row.trade_return))
    .filter((value: number | null): value is number => value !== null && Number.isFinite(value))
  const wins = returns.filter((value) => value > 1e-12)
  const losses = returns.filter((value) => value < -1e-12)
  const breakeven = returns.filter((value) => Math.abs(value) <= 1e-12)
  const grossProfit = wins.reduce((total, value) => total + value, 0)
  const grossLoss = losses.reduce((total, value) => total + value, 0)
  const sourceWinCount = nonNegativeCount(sourceSummary?.win_count)
  const sourceLossCount = nonNegativeCount(sourceSummary?.loss_count)
  const sourceBreakevenCount = nonNegativeCount(sourceSummary?.breakeven_count)
  const sourceClosedCount = nonNegativeCount(sourceSummary?.closed_trade_count)
  const winCount = sourceWinCount ?? wins.length
  const lossCount = sourceLossCount ?? losses.length
  const breakevenCount = sourceBreakevenCount ?? breakeven.length
  const outcomeTotal = sourceClosedCount ?? winCount + lossCount + breakevenCount
  return {
    hasTradeReturnField: Boolean(sourceSummary?.available || hasTradeReturnField),
    returns,
    closedCount: sourceClosedCount ?? returns.length,
    chartReady: returns.length >= 5,
    insufficient: returns.length > 0 && returns.length < 5,
    wins,
    losses,
    breakeven,
    winCount,
    lossCount,
    breakevenCount,
    outcomeTotal,
    grossProfit,
    grossLoss,
    avgWin: asNumber(sourceSummary?.average_win) ?? (wins.length ? grossProfit / wins.length : null),
    avgLoss: asNumber(sourceSummary?.average_loss) ?? (losses.length ? grossLoss / losses.length : null),
    profitFactor: asNumber(sourceSummary?.profit_factor) ?? (grossLoss < 0 ? grossProfit / Math.abs(grossLoss) : null),
  }
}

type TradeOutcomeSummary = ReturnType<typeof buildTradeOutcomeSummary>

function formatOutcomePercent(count: number, total: number): string {
  if (!Number.isFinite(count) || !Number.isFinite(total) || total <= 0) return '0.0%'
  return `${((count / total) * 100).toFixed(1)}%`
}

function formatOutcomeCount(count: number, language: Language): string {
  const value = formatMetric('trade_count', count)
  return language === 'zh-Hant' ? `${value}筆交易` : `${value} trades`
}

function TradeOutcomeStats({ outcome, language }: { outcome: TradeOutcomeSummary; language: Language }) {
  const bt = (zh: string, en: string) => (language === 'zh-Hant' ? zh : en)
  const total = outcome.outcomeTotal > 0 ? outcome.outcomeTotal : outcome.winCount + outcome.lossCount + outcome.breakevenCount
  const rows = [
    { key: 'wins', label: bt('勝場', 'Wins'), count: outcome.winCount, tone: 'tone-positive' },
    { key: 'losses', label: bt('虧損', 'Losses'), count: outcome.lossCount, tone: 'tone-negative' },
    { key: 'breakeven', label: bt('損益平衡', 'Breakeven'), count: outcome.breakevenCount, tone: 'tone-neutral' },
  ]
  return (
    <div className="trade-outcome-stat-stack" aria-label={bt('交易結果統計', 'Trade outcome statistics')}>
      {rows.map((row) => (
        <div key={row.key} className={`trade-outcome-stat-card ${row.key}`} tabIndex={0}>
          <div className="trade-outcome-stat-header">
            <span className={`trade-outcome-stat-dot ${row.key}`} />
            <span className="trade-outcome-stat-label">{row.label}</span>
          </div>
          <div className={`trade-outcome-stat-percent ${row.tone}`}>{formatOutcomePercent(row.count, total)}</div>
          <div className="trade-outcome-stat-count">{formatOutcomeCount(row.count, language)}</div>
        </div>
      ))}
    </div>
  )
}

function tradeOutcomeUnavailableMessage({
  applicable,
  closedCount,
  language,
}: {
  applicable: boolean
  closedCount: number
  language: Language
}): string {
  if (!applicable) {
    return language === 'zh-Hant'
      ? '這份結果暫時沒有足夠逐筆已平倉交易資料；請先看配置時間線、再平衡紀錄、回撤與換手率診斷。'
      : 'This result does not have enough closed-trade data yet. Review the allocation timeline, rebalance records, drawdown, and turnover diagnostics.'
  }
  return language === 'zh-Hant'
    ? `只有 ${closedCount} 筆已平倉交易；至少需要 5 筆才會顯示分佈與勝負甜甜圈。`
    : `Only ${closedCount} closed trade(s) are available; at least 5 are required for distribution and outcome charts.`
}

function toTimeMs(value: unknown): number | null {
  if (value === undefined || value === null || value === '') return null
  const parsed = Date.parse(String(value))
  return Number.isFinite(parsed) ? parsed : null
}

function pickLongestRecoveryEpisode(episodes: any[]): any | null {
  return episodes.reduce((best, episode) => {
    const duration = asNumber(episode?.duration_periods)
    if (duration === null) return best
    const bestDuration = asNumber(best?.duration_periods)
    return bestDuration === null || duration > bestDuration ? episode : best
  }, null)
}

function recoveryHighlightRows(equityRows: any[], episode: any | null): any[] {
  if (!episode || !equityRows.length) return []
  const start = toTimeMs(episode.peak_time)
  const fallbackEnd = toTimeMs(equityRows[equityRows.length - 1]?.time)
  const end = toTimeMs(episode.recovery_time) ?? fallbackEnd
  if (start === null || end === null) return []
  return equityRows.filter((row) => {
    const timestamp = toTimeMs(row?.time)
    return timestamp !== null && timestamp >= start && timestamp <= end
  })
}

function riskReturnSourceLabel(source: unknown, language: Language): string {
  const value = String(source || '').trim()
  if (value === 'closed_trades') return language === 'zh-Hant' ? '已平倉交易' : 'Closed trades'
  if (value === 'equity_periods') return language === 'zh-Hant' ? '資金曲線期報酬' : 'Equity-period returns'
  return '-'
}

function RiskDiagnosticsPanel({
  diagnostics,
  equitySeries,
  language,
}: {
  diagnostics: any
  equitySeries: any[]
  language: Language
}) {
  const bt = (zh: string, en: string) => (language === 'zh-Hant' ? zh : en)
  const serial = diagnostics?.serial_correlation || {}
  const concentration = diagnostics?.profit_concentration || {}
  const recovery = diagnostics?.recovery_time || {}
  const acfRows = Array.isArray(serial.lags) ? serial.lags.filter((row: any) => asNumber(row.acf) !== null) : []
  const lorenzRows = Array.isArray(concentration.lorenz_curve) ? concentration.lorenz_curve : []
  const equityRows = Array.isArray(equitySeries)
    ? equitySeries.filter((row: any) => row?.time && asNumber(row?.value) !== null)
    : []
  const recoveryEpisodes = Array.isArray(recovery.episodes) ? recovery.episodes : []
  const longestRecoveryEpisode = pickLongestRecoveryEpisode(recoveryEpisodes)
  const highlightedRecoveryRows = recoveryHighlightRows(equityRows, longestRecoveryEpisode)
  const band = asNumber(serial.significance_band)
  const recoveryPercentiles = recovery.percentiles || {}
  const concentrationSource = riskReturnSourceLabel(concentration.return_source, language)
  const concentrationSampleCount = asNumber(concentration.profitable_trade_count)

  if (!diagnostics) {
    return (
      <SectionCard
        captureId="risk_diagnostics"
        title={bt('風險診斷：序列、集中度、回撤恢復時間', 'Risk Diagnostics: Serial, Concentration, Drawdown Recovery Time')}
      >
        <MissingState message={bt(
          '因為這份結果沒有輸出 risk_diagnostics，多數是舊版結果或執行尚未產生診斷資料，所以序列相關、獲利集中度與回撤恢復時間暫時不適用。重新執行回測後會自動顯示。',
          'Because this result does not include risk_diagnostics, usually from an older result or a run that has not produced diagnostics yet, serial correlation, profit concentration, and drawdown recovery time are not applicable for now. Re-run the backtest to populate this card.',
        )} />
      </SectionCard>
    )
  }

  return (
    <SectionCard
      captureId="risk_diagnostics"
      title={bt('風險診斷：序列、集中度、回撤恢復時間', 'Risk Diagnostics: Serial, Concentration, Drawdown Recovery Time')}
      subtitle={bt('由已平倉交易報酬與資金曲線產生，用來檢查報酬是否連續偏態、獲利是否過度集中，以及回撤後需要多久才恢復。', 'Built from closed-trade returns and the equity curve to inspect return persistence, profit concentration, and how long drawdowns take to recover.')}
    >
      <div className="portfolio-context-rail">
        <div className="portfolio-context-item">
          <span className="portfolio-context-label">{labelWithHint(bt('Lag 1 序列相關', 'Lag 1 Serial Corr'), 'lag_1_serial_corr', language)}</span>
          <span className="portfolio-context-value">{formatNumber(serial.lag1)}</span>
        </div>
        <div className="portfolio-context-item">
          <span className="portfolio-context-label">{labelWithHint(bt('Top 20% 獲利貢獻', 'Top 20% Profit Share'), 'top_20_profit_share', language)}</span>
          <span className="portfolio-context-value">{formatMetric('total_return', concentration.top_20_contribution)}</span>
        </div>
        <div className="portfolio-context-item">
          <span className="portfolio-context-label">{labelWithHint(bt('獲利 Gini', 'Profit Gini'), 'profit_gini', language)}</span>
          <span className="portfolio-context-value">{formatNumber(concentration.gini)}</span>
        </div>
        <div className="portfolio-context-item">
          <span className="portfolio-context-label">{labelWithHint(bt('集中度樣本', 'Concentration Sample'), 'concentration_sample', language)}</span>
          <span className="portfolio-context-value">
            {concentrationSampleCount === null
              ? concentrationSource
              : `${concentrationSource} · ${formatMetric('trade_count', concentrationSampleCount)}`}
          </span>
        </div>
        <div className="portfolio-context-item">
          <span className="portfolio-context-label">{labelWithHint(bt('P50 回撤恢復時間', 'P50 Drawdown Recovery Time'), 'p50_drawdown_recovery_time', language)}</span>
          <span className="portfolio-context-value">{formatWholeOrDecimal(recoveryPercentiles.p50_periods)}</span>
        </div>
      </div>

      <div className="two-column-chart-grid" style={{ marginTop: '1rem' }}>
        {acfRows.length ? (
          <Plot
            data={[{
              type: 'bar',
              x: acfRows.map((row: any) => row.lag),
              y: acfRows.map((row: any) => asNumber(row.acf)),
              marker: { color: '#45d7ff' },
              hovertemplate: 'Lag %{x}<br>ACF %{y:.3f}<extra></extra>',
            }]}
            layout={makeChartLayout({
              xTitle: bt('落後期數', 'Lag'),
              yTitle: bt('自相關', 'Autocorrelation'),
              yaxis: { range: [-1, 1] },
              margin: { l: 70, r: 28, t: 24, b: 58 },
              shapes: band === null ? [] : [
                { type: 'line', x0: 0, x1: 1, y0: band, y1: band, xref: 'paper', yref: 'y', line: { color: 'rgba(223, 230, 245, 0.65)', width: 1, dash: 'dot' } },
                { type: 'line', x0: 0, x1: 1, y0: -band, y1: -band, xref: 'paper', yref: 'y', line: { color: 'rgba(223, 230, 245, 0.65)', width: 1, dash: 'dot' } },
              ],
            })}
            config={plotConfig}
            className="plot-card"
            useResizeHandler
            style={{ width: '100%', height: '320px' }}
          />
        ) : (
          <MissingState message={bt('已平倉交易數不足，無法計算交易報酬 ACF。', 'Not enough closed trades to compute trade-return ACF.')} />
        )}

        {lorenzRows.length ? (
          <Plot
            data={[
              {
                type: 'scatter',
                mode: 'lines',
                name: bt('Lorenz 曲線', 'Lorenz Curve'),
                x: lorenzRows.map((row: any) => asNumber(row.trade_share)),
                y: lorenzRows.map((row: any) => asNumber(row.profit_share)),
                line: { color: '#e1b12c', width: 3 },
                hovertemplate: `${bt('交易占比', 'Trade Share')} %{x:.1%}<br>${bt('獲利占比', 'Profit Share')} %{y:.1%}<extra></extra>`,
              },
              {
                type: 'scatter',
                mode: 'lines',
                name: bt('平均線', 'Equality'),
                x: [0, 1],
                y: [0, 1],
                line: { color: 'rgba(223, 230, 245, 0.45)', dash: 'dash' },
                hoverinfo: 'skip',
              },
            ]}
            layout={makeChartLayout({
              xTitle: bt('累積獲利交易占比', 'Cumulative Winning Trades'),
              yTitle: bt('累積獲利占比', 'Cumulative Profit'),
              xaxis: { tickformat: '.0%', range: [0, 1] },
              yaxis: { tickformat: '.0%', range: [0, 1] },
              margin: { l: 70, r: 28, t: 24, b: 58 },
            })}
            config={plotConfig}
            className="plot-card"
            useResizeHandler
            style={{ width: '100%', height: '320px' }}
          />
        ) : (
          <MissingState message={bt('沒有正報酬交易，無法建立獲利集中度 Lorenz 曲線。', 'No profitable trades are available for a profit-concentration Lorenz curve.')} />
        )}

        {equityRows.length && highlightedRecoveryRows.length ? (
          <Plot
            data={[
              {
                type: 'scatter',
                mode: 'lines',
                x: equityRows.map((row: any) => row.time),
                y: equityRows.map((row: any) => asNumber(row.value)),
                line: { color: 'rgba(148, 163, 184, 0.5)', width: 2 },
                hovertemplate: `${bt('資金', 'Equity')} %{y:.2f}<br>%{x}<extra></extra>`,
                showlegend: false,
              },
              {
                type: 'scatter',
                mode: 'lines',
                x: highlightedRecoveryRows.map((row: any) => row.time),
                y: highlightedRecoveryRows.map((row: any) => asNumber(row.value)),
                line: { color: '#7ae0a4', width: 4 },
                hovertemplate: `${bt('最長回撤恢復時間', 'Longest drawdown recovery time')}<br>%{x}<br>${bt('資金', 'Equity')} %{y:.2f}<extra></extra>`,
                showlegend: false,
              },
            ]}
            layout={makeChartLayout({
              xTitle: bt('日期', 'Date'),
              yTitle: bt('資金曲線', 'Equity Curve'),
              margin: { l: 70, r: 28, t: 24, b: 58 },
              showlegend: false,
            })}
            config={plotConfig}
            className="plot-card"
            useResizeHandler
            style={{ width: '100%', height: '320px' }}
          />
        ) : (
          <MissingState message={bt('尚無足夠資金曲線或回撤段可標示最長回撤恢復時間。', 'Not enough equity-curve or drawdown episode data to highlight the longest drawdown recovery time.')} />
        )}

        <div className="kpi-panel">
          <div className="kpi-panel-title">{bt('回撤恢復時間摘要', 'Drawdown Recovery Time Summary')}</div>
          <div className="kpi-compact-list">
            {[
              { label: bt('已恢復回撤段數', 'Recovered Drawdowns'), value: recovery.recovered_count, helpKey: 'recovered_drawdowns' },
              { label: bt('未恢復回撤段數', 'Unrecovered Drawdowns'), value: recovery.unrecovered_count, helpKey: 'unrecovered_drawdowns' },
              { label: bt('P75 回撤恢復時間', 'P75 Drawdown Recovery Time'), value: recoveryPercentiles.p75_periods, helpKey: 'p75_drawdown_recovery_time' },
              { label: bt('P90 回撤恢復時間', 'P90 Drawdown Recovery Time'), value: recoveryPercentiles.p90_periods, helpKey: 'p90_drawdown_recovery_time' },
              { label: bt('最長回撤恢復時間', 'Longest Drawdown Recovery Time'), value: recoveryPercentiles.max_periods, helpKey: 'longest_drawdown_recovery_time' },
            ].map(({ label, value, helpKey }) => (
              <div key={String(label)} className="kpi-compact-row">
                <span className="kpi-compact-label">{labelWithHint(label, helpKey, language)}</span>
                <span className="kpi-compact-value">{formatWholeOrDecimal(value)}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </SectionCard>
  )
}

function PortfolioBacktestDetail({ detail }: { detail: any }) {
  const benchmarkVisible = useAppStore((state) => state.benchmarkVisible)
  const setBenchmarkVisible = useAppStore((state) => state.setBenchmarkVisible)
  const language = useAppStore((state) => state.language)
  const t = useCopy(language)
  const bt = (zh: string, en: string) => (language === 'zh-Hant' ? zh : en)
  const [equityScale, setEquityScale] = useState<EquityScale>('linear')
  const [showTradeMarkers, setShowTradeMarkers] = useState(true)
  const [pinnedTradeMarker, setPinnedTradeMarker] = useState<PinnedTradeMarker | null>(null)
  const [rebalancePage, setRebalancePage] = useState(1)
  const [rebalancePageSize, setRebalancePageSize] = useState(10)
  const [holdingsPage, setHoldingsPage] = useState(1)
  const [holdingsPageSize, setHoldingsPageSize] = useState(10)
  const metrics = detail.metrics_matrix || {}
  const strategySummary = detail.strategy_summary || {}
  const dataQuality = detail.data_quality || {}
  const dataHealthStatus = dataQuality.status || (dataQuality.legacy_missing_validation ? 'legacy_missing_validation' : '-')
  const dataHealthStatusText = dataHealthLabel(dataHealthStatus, language)
  const hasLegacyValidationWarning = Boolean(dataQuality.legacy_missing_validation || dataQuality.validation_available === false)
  const contributionSummary = detail.asset_contribution_summary || {}
  const turnoverSummary = detail.turnover_summary || {}
  const riskGateSummary = detail.risk_gate_summary || {}
  const riskGateRows = Array.isArray(detail.risk_gate_rows) ? detail.risk_gate_rows : []
  const riskGateEventCount = asNumber(riskGateSummary.event_count) ?? riskGateRows.length
  const configuredRiskGates = Array.isArray(riskGateSummary.configured_gates) ? riskGateSummary.configured_gates : []
  const riskGateConfigured = Boolean(riskGateSummary.enabled || configuredRiskGates.length || riskGateRows.length)
  const riskGateActionLabel = (value: unknown): string => {
    const action = String(value ?? '').trim()
    if (!action) return '-'
    if (action === 'shadow_until_recovery') return bt('停止真實交易，改為虛擬交易，直至收復前高', 'Stop live trading and continue virtual trading until the prior peak is recovered')
    if (action === 'shadow_recovery_armed') return bt('虛擬交易已收復前高，等待下一次再平衡恢復真實交易', 'Virtual trading recovered the prior peak and is waiting for the next rebalance to resume live trading')
    if (action === 'shadow_recovery_resumed') return bt('已恢復真實交易', 'Live trading resumed')
    if (action === 'flatten') return bt('清倉', 'Flatten')
    if (action === 'permanent_stop') return bt('永久停止交易', 'Permanent Trading Stop')
    if (action === 'block_new_orders') return bt('阻止新開倉', 'Block New Orders')
    if (action === 'reduce_exposure') return bt('降低曝險', 'Reduce Exposure')
    if (action === 'reduce_selected_positions') return bt('減少持倉數', 'Reduce Selected Positions')
    if (action === 'clamp_order_delta') return bt('限制單次調倉幅度', 'Clamp Order Delta')
    return action
  }
  const shadowRecoveryRows = riskGateRows.filter((row: any) => {
    const action = String(row.Action || row.action || '')
    return action === 'shadow_until_recovery' || action.startsWith('shadow_recovery_')
  })
  const latestShadowRecoveryAction = shadowRecoveryRows.length
    ? String(shadowRecoveryRows[shadowRecoveryRows.length - 1].Action || shadowRecoveryRows[shadowRecoveryRows.length - 1].action || '')
    : ''
  const shadowRecoveryStatus = latestShadowRecoveryAction === 'shadow_recovery_resumed'
    ? bt('已恢復真實交易', 'Live trading resumed')
    : latestShadowRecoveryAction === 'shadow_recovery_armed'
      ? bt('虛擬交易已收復前高，等待下一次再平衡恢復', 'Virtual trading recovered the prior peak and is waiting for the next rebalance to resume')
      : latestShadowRecoveryAction === 'shadow_until_recovery'
        ? bt('停機中，虛擬交易正在追蹤恢復', 'Trading halted while virtual trading tracks recovery')
        : ''
  const hasShadowRecoveryMode = shadowRecoveryRows.length > 0
  const riskGateEventPhaseLabel = (row: any): string => {
    const action = String(row?.Action || row?.action || '').trim()
    if (!action) return '-'
    if (action === 'shadow_until_recovery') return bt('觸發停機', 'Trading Halt Triggered')
    if (action === 'shadow_recovery_armed') return bt('虛擬交易收復前高', 'Virtual Recovery Armed')
    if (action === 'shadow_recovery_resumed') return bt('恢復真實交易', 'Live Trading Resumed')
    return bt('一般風控事件', 'Risk Gate Event')
  }
  const riskGateThresholdLabel = (row: any): string => {
    const action = String(row?.Action || row?.action || '').trim()
    const raw = row?.Threshold ?? row?.threshold
    if (action === 'shadow_until_recovery') {
      return bt(`最大回撤門檻 ${formatOptionalPercent(raw) || formatFixed(raw)}`, `Max drawdown limit ${formatOptionalPercent(raw) || formatFixed(raw)}`)
    }
    if (action === 'shadow_recovery_armed') {
      return bt(`恢復目標前高 ${formatFixed(raw)}`, `Recovery target peak ${formatFixed(raw)}`)
    }
    if (action === 'shadow_recovery_resumed') {
      return bt(`恢復判定前高 ${formatFixed(raw)}`, `Recovery trigger peak ${formatFixed(raw)}`)
    }
    return formatFixed(raw)
  }
  const riskGateObservedLabel = (row: any): string => {
    const action = String(row?.Action || row?.action || '').trim()
    const raw = row?.Observed ?? row?.observed
    if (action === 'shadow_until_recovery') {
      return bt(`當前回撤 ${formatOptionalPercent(raw) || formatFixed(raw)}`, `Observed drawdown ${formatOptionalPercent(raw) || formatFixed(raw)}`)
    }
    if (action === 'shadow_recovery_armed') {
      return bt(`虛擬交易資金 ${formatFixed(raw)}`, `Virtual equity ${formatFixed(raw)}`)
    }
    if (action === 'shadow_recovery_resumed') {
      return bt(`恢復當下真實資金 ${formatFixed(raw)}`, `Live equity at resume ${formatFixed(raw)}`)
    }
    return formatFixed(raw)
  }
  const holdingsRows = detail.holding_rows || detail.trade_rows || []
  const allocationChangeRows = detail.allocation_change_rows?.length ? detail.allocation_change_rows : holdingsRows
  const portfolioTradeMarkerTraces = showTradeMarkers
    ? buildTradeMarkerTraces({
      equitySeries: detail.equity_series || [],
      tradeRows: allocationChangeRows,
      equityScale,
      chartValue,
      language,
      includePrice: false,
    }).map((trace: any) => ({
      ...trace,
      showlegend: false,
      legendgroup: 'portfolio-detail',
    }))
    : []
  const rebalanceRows = detail.rebalance_rows || []
  const rawAssetContributionRows = Array.isArray(detail.asset_contribution_rows) ? detail.asset_contribution_rows : []
  const assetContributionRows = useMemo(
    () =>
      [...rawAssetContributionRows].sort((left: any, right: any) => {
        const leftShare = asNumber(left?.contribution_share) ?? Number.NEGATIVE_INFINITY
        const rightShare = asNumber(right?.contribution_share) ?? Number.NEGATIVE_INFINITY
        if (leftShare !== rightShare) return rightShare - leftShare
        const leftContribution = asNumber(left?.return_contribution) ?? Number.NEGATIVE_INFINITY
        const rightContribution = asNumber(right?.return_contribution) ?? Number.NEGATIVE_INFINITY
        if (leftContribution !== rightContribution) return rightContribution - leftContribution
        return String(left?.asset || '').localeCompare(String(right?.asset || ''))
      }),
    [rawAssetContributionRows],
  )
  const visualAvailability = detail.portfolio_visual_availability || {}
  const drawdownSeries = detail.drawdown_series || []
  const turnoverDistributionRows = detail.turnover_distribution || []
  const rebalanceTotalPages = Math.max(1, Math.ceil(rebalanceRows.length / rebalancePageSize))
  const holdingsTotalPages = Math.max(1, Math.ceil(allocationChangeRows.length / holdingsPageSize))
  const rebalancePageRows = rebalanceRows.slice((rebalancePage - 1) * rebalancePageSize, rebalancePage * rebalancePageSize)
  const holdingsPageRows = allocationChangeRows.slice((holdingsPage - 1) * holdingsPageSize, holdingsPage * holdingsPageSize)
  const firstTime = detail.date_range_start || detail.equity_series?.[0]?.time || 'n/a'
  const lastTime = detail.date_range_end || detail.equity_series?.[detail.equity_series.length - 1]?.time || 'n/a'
  const strategyValue = (key: string, fallback = '-') =>
    localizeStrategyRuleValue(key, String(strategySummary?.[key] || fallback), language)
  const benchmarkLabel = benchmarkDisplayLabel(
    localizeStrategyRuleValue(
      'benchmark_label',
      String(metrics.benchmark_label || strategySummary.benchmark_label || (language === 'zh-Hant' ? '基準' : 'Benchmark')),
      language,
    ),
    language === 'zh-Hant' ? '基準' : 'Benchmark',
  )
  const rowValue = (row: any, ...keys: string[]) => {
    for (const key of keys) {
      if (row?.[key] !== undefined && row?.[key] !== null) return row[key]
    }
    return null
  }
  const rowText = (value: unknown): string => {
    if (Array.isArray(value)) return value.length ? value.map((item) => String(item)).join(', ') : '-'
    if (value && typeof value === 'object') {
      const text = Object.values(value as Record<string, unknown>).map((item) => String(item)).join(', ')
      return text || '-'
    }
    const text = String(value ?? '').trim()
    return text || '-'
  }
  const formatExchangeTime = (row: any): string => {
    const explicit = String(rowValue(row, 'Event_timestamp_local', 'event_timestamp_local') || '').trim()
    if (explicit) return explicit
    const raw = String(rowValue(row, 'Time', 'time', 'entry_time') || '').trim()
    if (!raw) return '-'
    return raw.includes('T') ? raw.replace('T', ' ').slice(0, 16) : raw.slice(0, 10)
  }
  const formatWhole = (value: unknown): string => {
    const numeric = asNumber(value)
    return numeric === null ? '-' : numeric.toFixed(0)
  }
  const formatFixed = (value: unknown, decimals = 3): string => {
    const numeric = asNumber(value)
    return numeric === null ? '-' : numeric.toFixed(decimals)
  }
  const strategyModeId = String(strategySummary.strategy_mode_id || '').trim()
  const strategyProfileId = String(strategySummary.strategy_profile_id || '').trim()
  const strategyPresetId = String(strategySummary.strategy_preset_id || '').trim()
  const closedTradeRows = Array.isArray(detail.closed_trade_rows) ? detail.closed_trade_rows : []
  const portfolioTradeOutcome = buildTradeOutcomeSummary(closedTradeRows, detail.trade_outcome_summary)
  const portfolioTradeOutcomeApplicable = portfolioTradeOutcome.hasTradeReturnField || closedTradeRows.length > 0
  const portfolioTradeOutcomeMessage = tradeOutcomeUnavailableMessage({
    applicable: portfolioTradeOutcomeApplicable,
    closedCount: portfolioTradeOutcome.closedCount,
    language,
  })
  const hasSameSessionLegs = allocationChangeRows.some((row: any) => {
    const reason = String(rowValue(row, 'Reason', 'reason') || '').toLowerCase()
    const action = String(rowValue(row, 'Action', 'action') || '').toLowerCase()
    return reason.includes('same-session') || action.includes('short')
  })
  const isCalendarEventStrategy =
    strategyProfileId === 'calendar_event_portfolio' || hasSameSessionLegs
  const isSignalStrategy =
    strategyPresetId === 'single_asset_signal' ||
    strategyProfileId === 'selection_timing_portfolio'
  const eventCountLabel = isCalendarEventStrategy
    ? bt('事件交易日', 'Event Trading Days')
    : isSignalStrategy
      ? bt('訊號事件', 'Signal Events')
      : bt('有效再平衡', 'Active Rebalances')
  const eventCountValue = turnoverSummary.active_rebalance_events ?? metrics.rebalance_count
  const eventPanelTitle = eventCountLabel
  const eventPanelSubtitle = isCalendarEventStrategy
    ? bt('每列代表一個由日曆觸發的交易時段。來回換手率包含開倉與平倉曝險；同一時段做空 100% 後平倉會顯示為 200%。', 'Each row is a calendar-triggered trading session. Round-trip turnover includes opening and closing exposure; a 100% same-session short then close appears as 200%.')
    : isSignalStrategy
      ? bt('每列代表一次策略訊號狀態變化。交易換手率是由前一持倉調整至下一個目標所需的買賣權重。', 'Each row is a strategy signal state change. Trade turnover is the buy/sell weight needed to move from prior holdings to the next target.')
      : bt('每列代表一次實際目標權重變化。沒有目標資產、沒有交易且 0% 換手的排程檢查會計入檢查點，但不在此表顯示。', 'Each row is an actual target-weight change. Scheduled checks with no target assets, no trades, and 0% turnover count as checkpoints but are hidden here.')
  const eventAssetsLabel = isCalendarEventStrategy ? bt('交易資產', 'Traded Assets') : bt('目標資產', 'Target Assets')
  const eventCountColumnLabel = isCalendarEventStrategy || isSignalStrategy ? bt('有效資產', 'Active Assets') : bt('目標持倉', 'Target Holdings')
  const eventTurnoverLabel = isCalendarEventStrategy
    ? bt('來回換手率', 'Round-Trip Turnover')
    : isSignalStrategy
      ? bt('交易換手率', 'Trade Turnover')
      : bt('再平衡換手率', 'Rebalance Turnover')
  const tradeLegsLabel = isCalendarEventStrategy ? bt('交易分段', 'Trade Legs') : bt('交易事件', 'Trade Events')
  const weightLabel = bt('權重', 'Weight')
  const allocationDates = Array.from(new Set(holdingsRows.map((row: any) => String(row.time || '').slice(0, 10)).filter(Boolean))).sort()
  const allocationAssets = Array.from(new Set(holdingsRows.map((row: any) => String(row.asset || '')).filter(Boolean))).sort()
  const allocationSeries: any[] = allocationAssets.map((asset) => ({
    type: 'scatter',
    mode: 'lines',
    stackgroup: 'one',
    name: asset,
    x: allocationDates,
    y: allocationDates.map((date) => {
      const row = holdingsRows.find((item: any) => String(item.time || '').slice(0, 10) === date && String(item.asset || '') === asset)
      return row ? asNumber(row.target_weight) : 0
    }),
    hovertemplate: `${asset}<br>%{x}<br>${weightLabel} %{y:.1%}<extra></extra>`,
  }))
  const auditItems = [
    { label: bt('驗證狀態', 'Validation'), value: dataHealthStatusText, helpKey: 'validation_status' },
    {
      label: bt('預期資產', 'Expected Assets'),
      value: Array.isArray(dataQuality.expected_symbols) && dataQuality.expected_symbols.length
        ? dataQuality.expected_symbols.join(', ')
        : strategyValue('asset_label'),
      helpKey: 'expected_assets',
    },
    {
      label: bt('已載入資產', 'Loaded Assets'),
      value: Array.isArray(dataQuality.loaded_symbols) && dataQuality.loaded_symbols.length
        ? dataQuality.loaded_symbols.join(', ')
        : hasLegacyValidationWarning ? bt('未記錄', 'not recorded') : '-',
      helpKey: 'loaded_assets',
    },
    {
      label: bt('缺少資產', 'Missing Assets'),
      value: Array.isArray(dataQuality.missing_symbols) && dataQuality.missing_symbols.length
        ? dataQuality.missing_symbols.join(', ')
        : bt('沒有缺漏記錄', 'No missing assets recorded'),
      helpKey: 'missing_assets',
    },
    { label: bt('檢查點', 'Checkpoints'), value: formatNumber(turnoverSummary.checkpoint_events ?? turnoverSummary.scheduled_events, 0), helpKey: 'checkpoints' },
    { label: tradeLegsLabel, value: formatNumber(turnoverSummary.trade_events, 0), helpKey: 'trade_events' },
    { label: bt('成本設定', 'Cost Settings'), value: strategyValue('cost_label'), helpKey: 'cost_settings' },
  ] as const
  const operatingProfileItems = [
    [bt('平均持倉數', 'Avg Holdings'), 'avg_holdings'],
    [bt('平均曝險', 'Avg Exposure'), 'avg_gross_exposure'],
    [bt('平均交易換手率', 'Avg Trade Turnover'), 'avg_turnover'],
    [bt('有效再平衡', 'Active Rebalances'), 'rebalance_count'],
  ] as const
  const riskDistributionItems = [
    'annualized_std',
    'sortino',
    'calmar',
    'max_drawdown_duration_days',
    'recovery_factor',
    'skewness',
    'kurtosis',
  ] as const
  const tailRiskItems = ['var_95', 'cvar_95', 'var_99', 'cvar_99', 'worst_month', 'best_month', 'positive_month_ratio'] as const
  const returnQualityItems = [
    'win_rate',
    'profit_factor',
    'average_win',
    'average_loss',
    'average_win_loss_ratio',
    'gross_profit',
    'gross_loss',
    'max_consecutive_wins',
    'max_consecutive_losses',
  ] as const
  const benchmarkItems = ['bah_total_return', 'bah_cagr', 'bah_sharpe', 'benchmark_correlation', 'excess_return'] as const
  const renderMetricRow = (key: string) => (
    <div key={key} className="kpi-compact-row">
      <span className="kpi-compact-label">{metricLabelWithHint(key, language)}</span>
      <span className={`kpi-compact-value ${toneClass(key, metrics[key])}`}>{formatMetric(key, metrics[key])}</span>
    </div>
  )
  const monthlyReturnRows = (detail.monthly_return_rows || []).map((row: any) => {
    const period = String(row.period || '')
    const [periodYear, periodMonth] = period.split('-')
    return {
      ...row,
      year: asNumber(row.year) ?? asNumber(periodYear),
      month: asNumber(row.month) ?? asNumber(periodMonth),
      return: asNumber(row.return ?? row.Return),
    }
  }).filter((row: any) => row.year !== null && row.month !== null)
  const yearlyReturnRows = detail.yearly_return_rows || []
  const monthlyYears = Array.from(new Set<number>(monthlyReturnRows.map((row: any) => Number(row.year)))).sort((left, right) => left - right)
  const monthLabels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
  const monthlyHeatmapZ = monthLabels.map((_, monthIndex) =>
    monthlyYears.map((year) => {
      const row = monthlyReturnRows.find((item: any) => item.year === year && item.month === monthIndex + 1)
      return row?.return ?? null
    }),
  )
  const turnoverValues = turnoverDistributionRows
    .map((row: any) => asNumber(row.turnover ?? row.Turnover ?? row.trade_turnover ?? row.Rebalance_turnover))
    .filter((value: number | null): value is number => value !== null && Number.isFinite(value) && value > 0)
  const allocationChartAvailable = Boolean(
    visualAvailability.allocation_timeline ?? (allocationSeries.length && allocationDates.length),
  )

  useEffect(() => {
    setRebalancePage((current) => Math.min(current, rebalanceTotalPages))
  }, [rebalanceTotalPages])

  useEffect(() => {
    setHoldingsPage((current) => Math.min(current, holdingsTotalPages))
  }, [holdingsTotalPages])

  return (
    <div className="page-stack">
      <SectionCard captureId="best_summary" title={t('backtests.portfolioSummary')} subtitle={t('backtests.portfolioSummarySubtitle')}>
        {hasLegacyValidationWarning ? (
          <div className="page-warning portfolio-warning">
            {bt(
              '舊版投資組合回測：配置與貢獻資料可用，但這份結果是在新版執行驗證報告建立前產生。',
              'Legacy portfolio backtest: allocation and contribution data are available, but this result was created before the newer execution validation report existed.',
            )}
          </div>
        ) : null}
        <div className="snapshot-hero-shell">
          <div className="snapshot-identity-card">
            <div className="snapshot-eyebrow">{bt('目前選取', 'Current Selection')}</div>
            <div className="snapshot-strategy-title" data-private-strategy>{detail.label}</div>
            <div className="snapshot-strategy-subtitle" data-private-strategy>
              {bt(
                '投資組合回測摘要；再平衡事件與目標持倉會取代單資產入場 / 出場交易紀錄。',
                'Portfolio backtest summary. Rebalance events and target holdings replace single-asset entry and exit trade records.',
              )}
            </div>
            <div className="snapshot-meta-grid">
              <div className="snapshot-meta-pill">
                <span className="snapshot-meta-label">{bt('回測期間', 'Backtest Window')}</span>
                <span className="snapshot-meta-value">{String(firstTime).slice(0, 10)} {'->'} {String(lastTime).slice(0, 10)}</span>
              </div>
              <div className="snapshot-meta-pill">
                <span className="snapshot-meta-label">{bt('資產範圍', 'Asset Universe')}</span>
                <span className="snapshot-meta-value">{strategyValue('asset_label')}</span>
              </div>
              <div className="snapshot-meta-pill">
                <span className="snapshot-meta-label">{eventCountLabel}</span>
                <span className="snapshot-meta-value">{formatMetric('trade_count', eventCountValue)}</span>
              </div>
              <div className="snapshot-meta-pill">
                <span className="snapshot-meta-label">{bt('基準', 'Benchmark')}</span>
                <span className="snapshot-meta-value">{benchmarkLabel}</span>
              </div>
              <div className="snapshot-meta-pill">
                <span className="snapshot-meta-label">{bt('資料健康度', 'Data Health')}</span>
                <span className="snapshot-meta-value">{dataHealthStatusText}</span>
              </div>
              <div className="snapshot-meta-pill">
                <span className="snapshot-meta-label">{bt('有效起始日', 'Effective Start')}</span>
                <span className="snapshot-meta-value">{dataQuality.effective_start_date || '-'}</span>
              </div>
            </div>
          </div>

          <div className="snapshot-primary-grid">
            {[
              { key: 'total_return', accent: bt('主要表現', 'Primary Performance'), meta: `${bt('基準', 'Benchmark')} ${formatMetric('bah_total_return', metrics.bah_total_return)}` },
              { key: 'cagr', accent: bt('年化表現', 'Annualized Performance'), meta: `${bt('基準', 'Benchmark')} ${formatMetric('bah_cagr', metrics.bah_cagr)}` },
              { key: 'sharpe', accent: bt('風險調整', 'Risk Adjusted'), meta: `${bt('基準', 'Benchmark')} ${formatMetric('bah_sharpe', metrics.bah_sharpe)}` },
              { key: 'max_drawdown', accent: bt('資金壓力', 'Equity Stress'), meta: `Calmar ${formatMetric('calmar', metrics.calmar)}` },
            ].map((item) => (
              <div key={item.key} className="snapshot-primary-card">
                <div className="snapshot-primary-accent">{item.accent}</div>
                <div className="snapshot-primary-label">{metricLabelWithHint(item.key, language)}</div>
                <div className={`snapshot-primary-value ${toneClass(item.key === 'max_drawdown' ? 'mdd' : item.key, metrics[item.key])}`}>{item.key === 'max_drawdown' ? formatMetric('mdd', metrics[item.key]) : formatMetric(item.key, metrics[item.key])}</div>
                <div className="snapshot-primary-meta">{item.meta}</div>
              </div>
            ))}
          </div>
        </div>
      </SectionCard>

      <SectionCard captureId="portfolio_context" title={bt('投資組合背景資料', 'Portfolio Context')} subtitle={bt('目前策略列的操作、風險、基準與報酬品質指標。', 'Operating, risk, benchmark, and return-quality metrics for the active strategy row.')}>
        <div className="portfolio-context-rail">
          {operatingProfileItems.map(([label, key]) => (
            <div key={key} className="portfolio-context-item">
              <span className="portfolio-context-label">{labelWithHint(label, METRIC_HELP_KEY_BY_METRIC_KEY[key] || key, language)}</span>
              <span className={`portfolio-context-value ${toneClass(key, metrics[key])}`}>
                {formatMetric(key, metrics[key])}
              </span>
            </div>
          ))}
        </div>
        <div className="snapshot-context-grid portfolio-stat-grid">
          <div className="kpi-panel">
            <div className="kpi-panel-title">{bt('風險與分佈', 'Risk & Distribution')}</div>
            <div className="kpi-compact-list">{riskDistributionItems.map(renderMetricRow)}</div>
          </div>
          <div className="kpi-panel">
            <div className="kpi-panel-title">{bt('尾端風險與日曆', 'Tail Risk & Calendar')}</div>
            <div className="kpi-compact-list">{tailRiskItems.map(renderMetricRow)}</div>
          </div>
          <div className="kpi-panel">
            <div className="kpi-panel-title">{bt('報酬品質', 'Return Quality')}</div>
            <div className="kpi-compact-list">{returnQualityItems.map(renderMetricRow)}</div>
          </div>
          <div className="kpi-panel">
            <div className="kpi-panel-title">{bt('基準', 'Benchmark')}</div>
            <div className="kpi-compact-list">{benchmarkItems.map(renderMetricRow)}</div>
          </div>
        </div>
        <div className="portfolio-audit-note">
          {bt(
            '淨值與毛值比較、滑價敏感度需要以不同成本假設重新執行；目前這份結果只包含已扣成本後的資金曲線。',
            'Net-versus-gross comparisons and slippage sensitivity require reruns with alternate cost assumptions; this result currently includes only the post-cost equity curve.',
          )}
        </div>
      </SectionCard>

      <SectionCard title={bt('資料健康與成本稽核', 'Data Health & Cost Audit')} subtitle={bt('顯示資料覆蓋、檢查點記帳與本次投資組合回測的成本摩擦。', 'Shows data coverage, checkpoint accounting, and cost friction for this portfolio backtest.')}>
        <div className="portfolio-audit-list">
          {auditItems.map(({ label, value, helpKey }) => (
            <div key={label} className="portfolio-audit-row">
              <span className="portfolio-audit-label">{labelWithHint(label, helpKey, language)}</span>
              <span className="portfolio-audit-value">{value}</span>
            </div>
          ))}
        </div>
        <div className="portfolio-audit-note">{eventPanelSubtitle}</div>
      </SectionCard>

      <RiskDiagnosticsPanel
        diagnostics={detail.risk_diagnostics}
        equitySeries={detail.equity_series || []}
        language={language}
      />

      <SectionCard
        captureId="risk_gate_audit"
        title={bt('風控門檻稽核', 'Risk Gate Audit')}
        subtitle={riskGateConfigured
          ? bt('只顯示已設定的安全門檻。若觸發事件為 0，代表門檻有啟用，但這次選取的回測沒有需要介入。', 'Shows configured risk gates only. If triggered events are 0, the gates were enabled but did not need to intervene in this run.')
          : bt('因為這份策略沒有設定風控門檻，所以風控門檻稽核不適用。', 'Because this strategy does not configure risk gates, the risk-gate audit is not applicable.')}
      >
        {riskGateConfigured ? (
          <>
            <div className="portfolio-context-rail">
              <div className="portfolio-context-item">
                <span className="portfolio-context-label">{bt('已設定門檻', 'Configured Gates')}</span>
                <span className="portfolio-context-value">
                  {configuredRiskGates.length ? configuredRiskGates.join(', ') : bt('未記錄（舊版輸出）', 'Not recorded (legacy output)')}
                </span>
              </div>
              <div className="portfolio-context-item">
                <span className="portfolio-context-label">{bt('觸發事件', 'Triggered Events')}</span>
                <span className={`portfolio-context-value ${riskGateEventCount > 0 ? 'tone-negative' : 'tone-neutral'}`}>
                  {formatWhole(riskGateEventCount)}
                </span>
              </div>
              {hasShadowRecoveryMode ? (
                <div className="portfolio-context-item">
                  <span className="portfolio-context-label">{bt('恢復狀態', 'Recovery Status')}</span>
                  <span className={`portfolio-context-value ${latestShadowRecoveryAction === 'shadow_recovery_resumed' ? 'tone-positive' : latestShadowRecoveryAction === 'shadow_recovery_armed' ? 'tone-caution' : 'tone-negative'}`}>
                    {shadowRecoveryStatus || bt('未有恢復事件', 'No recovery event yet')}
                  </span>
                </div>
              ) : null}
            </div>
            {hasShadowRecoveryMode ? (
              <>
                <div className="portfolio-audit-note" style={{ marginTop: '0.75rem' }}>
                  {bt(
                    'max_drawdown 觸發後會先停止真實交易並清倉，之後由 shadow 組合繼續模擬；當 shadow equity 收復之前高位後，等下一次再平衡才恢復真實交易。',
                    'After max drawdown is breached, the live portfolio is flattened and halted based on live equity drawdown, while a virtual portfolio continues to simulate. Once virtual equity recovers the prior peak, live trading resumes at the next rebalance. Some rows therefore show drawdown percentages, while others show recovery equity levels.',
                  )}
                </div>
                {language === 'zh-Hant' ? (
                  <div className="portfolio-audit-note" style={{ marginTop: '0.5rem' }}>
                    {'最大回撤一旦觸發，系統會按真實交易組合的資金回撤停機並清倉，之後改由虛擬交易繼續模擬。當虛擬交易資金收復之前高位後，只會在下一次再平衡時恢復真實交易。所以表內有些列顯示的是回撤百分比，有些列顯示的是恢復用的資金高水位，兩者不是同一種數值。'}
                  </div>
                ) : null}
              </>
            ) : null}
            {riskGateRows.length ? (
              <div className="data-table-wrap paged-table-wrap" style={{ marginTop: '1rem' }}>
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>{bt('時間', 'Time')}</th>
                      <th>{bt('門檻', 'Gate')}</th>
                      <th>{bt('事件', 'Event')}</th>
                      <th>{bt('設定值 / 目標', 'Configured / Target')}</th>
                      <th>{bt('觀察值 / 結果', 'Observed / Result')}</th>
                      <th>{bt('動作', 'Action')}</th>
                      <th>{bt('受影響資產', 'Affected Assets')}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {riskGateRows.slice(0, 20).map((row: any, index: number) => (
                      <tr key={`${row.Time || row.time}-${row.Gate || row.gate}-${index}`}>
                        <td>{String(row.Time || row.time || '').slice(0, 10) || '-'}</td>
                        <td>{row.Gate || row.gate || '-'}</td>
                        <td>{riskGateEventPhaseLabel(row)}</td>
                        <td>{riskGateThresholdLabel(row)}</td>
                        <td>{riskGateObservedLabel(row)}</td>
                        <td>{riskGateActionLabel(row.Action || row.action || '-')}</td>
                        <td>{rowText(row.Affected_assets ?? row.affected_assets)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="portfolio-audit-note" style={{ marginTop: '1rem' }}>
                {bt('這次選取的投資組合回測未需要風控門檻介入。', 'This selected portfolio backtest did not require risk-gate intervention.')}
              </div>
            )}
          </>
        ) : (
          <MissingState message={bt('因為策略沒有設定 max_drawdown、exposure、turnover 等風控門檻，所以這張卡片只保留作狀態說明。', 'Because the strategy does not configure risk gates such as max drawdown, exposure, or turnover limits, this card is kept as a status note only.')} />
        )}
      </SectionCard>

      <SectionCard
        title={bt('投資組合資金曲線與基準比較', 'Portfolio Equity vs Benchmark')}
        actions={
          <div className="inline-actions">
            <BenchmarkToggleButton
              visible={benchmarkVisible}
              language={language}
              onChange={setBenchmarkVisible}
            />
            <TradeMarkersToggleButton
              visible={showTradeMarkers}
              language={language}
              onChange={setShowTradeMarkers}
            />
            <select className="text-input text-input-compact" value={equityScale} onChange={(event) => setEquityScale(event.target.value as EquityScale)}>
              <option value="linear">{bt('線性刻度', 'Linear Scale')}</option>
              <option value="log">{bt('對數刻度', 'Log Scale')}</option>
            </select>
          </div>
        }
      >
        <Plot
          data={[
            {
              type: 'scatter',
              mode: 'lines',
              name: bt('投資組合', 'Portfolio'),
              x: detail.equity_series.map((item: any) => item.time),
              y: detail.equity_series.map((item: any) => chartValue(item.value, equityScale)),
              line: { color: '#e1b12c' },
              legendgroup: 'portfolio-detail',
            },
            ...(benchmarkVisible && detail.benchmark_series?.length ? [{
              type: 'scatter',
              mode: 'lines',
              name: benchmarkLabel,
              x: detail.benchmark_series.map((item: any) => item.time),
              y: detail.benchmark_series.map((item: any) => chartValue(item.value, equityScale)),
              line: { color: '#7e9bcc', dash: 'dash' },
            }] : []),
            ...portfolioTradeMarkerTraces,
          ]}
          onClick={(event: any) => {
            const marker = pinnedTradeMarkerFromPlotClick(event)
            if (marker) setPinnedTradeMarker(marker)
          }}
          onHover={(event: any) => {
            const marker = pinnedTradeMarkerFromPlotClick(event)
            if (marker) setPinnedTradeMarker(marker)
          }}
          layout={makeChartLayout({
            xTitle: bt('日期', 'Date'),
            yTitle: equityScale === 'log' ? bt('資金曲線（對數）', 'Equity Curve (Log)') : bt('資金曲線', 'Equity Curve'),
            yaxis: { type: equityScale },
            uirevision: `backtest-portfolio-equity-${detail.backtest_id || detail.label || 'default'}-${equityScale}-${benchmarkVisible ? 'benchmark' : 'no-benchmark'}-${showTradeMarkers ? 'markers' : 'no-markers'}`,
          })}
          config={plotConfig}
          className="plot-card"
          useResizeHandler
          style={{ width: '100%', height: '360px' }}
        />
        {showTradeMarkers ? <TradeMarkerInfoPanel marker={pinnedTradeMarker} language={language} /> : null}
      </SectionCard>

      <div className="screenshot-section-group" data-screenshot-section="metrics2">
      <SectionCard
        captureId="trade_return_distribution"
        title={t('backtests.tradeReturnDistribution')}
        subtitle={portfolioTradeOutcomeApplicable
          ? bt('下方顯示逐筆已平倉交易的報酬與勝負分類。', 'Closed-trade returns and outcomes are shown below.')
          : undefined}
      >
        {portfolioTradeOutcomeApplicable && portfolioTradeOutcome.chartReady ? (
          <div className="two-column-chart-grid trade-return-chart-grid">
            <Plot
              data={[{
                type: 'histogram',
                x: portfolioTradeOutcome.returns,
                nbinsx: Math.min(40, Math.max(10, Math.ceil(Math.sqrt(portfolioTradeOutcome.returns.length)))),
                marker: { color: '#e1b12c' },
                hovertemplate: `${bt('報酬', 'Return')} %{x:.2%}<br>${bt('交易數', 'Trades')} %{y}<extra></extra>`,
              }]}
              layout={makeChartLayout({
                xTitle: bt('已平倉交易報酬', 'Closed Trade Return'),
                yTitle: bt('交易數', 'Trades'),
                xaxis: { tickformat: '.1%' },
                shapes: [{
                  type: 'line',
                  x0: 0,
                  x1: 0,
                  y0: 0,
                  y1: 1,
                  xref: 'x',
                  yref: 'paper',
                  line: { color: 'rgba(223, 230, 245, 0.55)', width: 1, dash: 'dot' },
                }],
              })}
              config={plotConfig}
              className="plot-card"
              useResizeHandler
              style={{ width: '100%', height: '320px' }}
            />
            <div className="trade-outcome-panel">
              <Plot
                data={[{
                  type: 'pie',
                  hole: 0.58,
                  labels: [bt('獲利', 'Win'), bt('虧損', 'Loss'), bt('打平', 'Breakeven')],
                  values: [portfolioTradeOutcome.winCount, portfolioTradeOutcome.lossCount, portfolioTradeOutcome.breakevenCount],
                  marker: { colors: ['#7ae0a4', '#ff9b8d', '#94a3b8'] },
                  textinfo: 'none',
                  hovertemplate: '%{label}<br>%{value} (%{percent})<extra></extra>',
                }]}
                layout={makeChartLayout({
                  margin: { l: 24, r: 24, t: 24, b: 24 },
                  showlegend: false,
                })}
                config={plotConfig}
                className="plot-card"
                useResizeHandler
                style={{ width: '100%', height: '320px' }}
              />
              <TradeOutcomeStats outcome={portfolioTradeOutcome} language={language} />
            </div>
          </div>
        ) : (
          <MissingState message={portfolioTradeOutcomeMessage} />
        )}
      </SectionCard>

      <SectionCard
        captureId="drawdown_turnover"
        title={bt('回撤與換手率診斷', 'Drawdown & Turnover Diagnostics')}
        subtitle={bt('由投資組合資金曲線與實際再平衡列生成；缺少來源列時只顯示資料不足說明。', 'Generated from portfolio equity and actual rebalance rows; missing sources render as an explicit data note.')}
      >
        <div className="two-column-chart-grid">
          {drawdownSeries.length ? (
            <div data-testid="drawdown-diagnostics-chart">
              <Plot
                data={[{
                  type: 'scatter',
                  mode: 'lines',
                  name: bt('回撤', 'Drawdown'),
                  x: drawdownSeries.map((item: any) => item.time),
                  y: drawdownSeries.map((item: any) => asNumber(item.drawdown)),
                  line: { color: '#ff9b8d' },
                  hovertemplate: `${bt('回撤', 'Drawdown')} %{y:.1%}<br>%{x}<extra></extra>`,
                }]}
                layout={makeChartLayout({
                  xTitle: bt('日期', 'Date'),
                  yTitle: bt('回撤', 'Drawdown'),
                  yaxis: { tickformat: '.0%' },
                  margin: { l: 70, r: 28, t: 24, b: 58 },
                })}
                config={plotConfig}
                className="plot-card"
                useResizeHandler
                style={{ width: '100%', height: '320px' }}
              />
            </div>
          ) : (
            <MissingState message={bt('這份投資組合結果沒有可用的資金曲線，無法計算回撤。', 'This portfolio result has no usable equity curve for drawdown.')} />
          )}
          {turnoverValues.length ? (
            <Plot
              data={[{
                type: 'histogram',
                x: turnoverValues,
                nbinsx: Math.min(30, Math.max(8, Math.ceil(Math.sqrt(turnoverValues.length)))),
                name: bt('換手率', 'Turnover'),
                marker: { color: '#7ae0a4' },
                hovertemplate: `${bt('換手率', 'Turnover')} %{x:.1%}<br>${bt('事件數', 'Events')} %{y}<extra></extra>`,
              }]}
              layout={makeChartLayout({
                xTitle: eventTurnoverLabel,
                yTitle: bt('事件數', 'Events'),
                xaxis: { tickformat: '.0%' },
                margin: { l: 70, r: 28, t: 24, b: 58 },
              })}
              config={plotConfig}
              className="plot-card"
              useResizeHandler
              style={{ width: '100%', height: '320px' }}
            />
          ) : (
            <MissingState message={bt('這份結果沒有實際再平衡換手率列；固定排程檢查點不會被當成交易換手率。', 'This result has no actual rebalance-turnover rows; scheduled checkpoints are not treated as trading turnover.')} />
          )}
        </div>
      </SectionCard>

      </div>

      <div className="screenshot-section-group" data-screenshot-section="metrics3">
      <SectionCard captureId="monthly_yearly_returns" title={bt('月度 / 年度報酬', 'Monthly / Yearly Returns')} subtitle={bt('由目前選取的投資組合資金曲線計算日曆期間報酬。', 'Calendar-period returns computed from the selected portfolio equity curve.')}>
        {monthlyReturnRows.length || yearlyReturnRows.length ? (
          <div className="two-column-chart-grid">
            {monthlyReturnRows.length ? (
              <Plot
                data={[{
                  type: 'heatmap',
                  x: monthlyYears,
                  y: monthLabels,
                  z: monthlyHeatmapZ,
                  colorscale: [
                    [0, '#ff9b8d'],
                    [0.5, '#1b1d26'],
                    [1, '#7ae0a4'],
                  ],
                  zmid: 0,
                  hovertemplate: `%{y} %{x}<br>${bt('報酬', 'Return')} %{z:.1%}<extra></extra>`,
                  colorbar: { tickformat: '.0%' },
                }]}
                layout={makeChartLayout({
                  xTitle: bt('年份', 'Year'),
                  yTitle: bt('月份', 'Month'),
                  margin: { l: 70, r: 28, t: 24, b: 58 },
                })}
                config={plotConfig}
                className="plot-card"
                useResizeHandler
                style={{ width: '100%', height: '360px' }}
              />
            ) : (
              <MissingState message={bt('這份結果沒有月度報酬列。', 'This result has no monthly return rows.')} />
            )}
            {yearlyReturnRows.length ? (
              <div className="data-table-wrap paged-table-wrap asset-contribution-table-frame">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>{bt('年份', 'Year')}</th>
                      <th>{bt('報酬', 'Return')}</th>
                      <th>{bt('期初資金', 'Start Equity')}</th>
                      <th>{bt('期末資金', 'End Equity')}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {yearlyReturnRows.map((row: any) => (
                      <tr key={row.period}>
                        <td>{row.period}</td>
                        <td className={toneClass('total_return', row.return)}>{formatMetric('total_return', row.return)}</td>
                        <td>{formatNumber(row.start_equity)}</td>
                        <td>{formatNumber(row.end_equity)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <MissingState message={bt('這份結果沒有年度報酬列。', 'This result has no yearly return rows.')} />
            )}
          </div>
        ) : (
          <MissingState message={bt('這份結果尚未提供月度與年度報酬表。', 'This result does not provide monthly or yearly return rows yet.')} />
        )}
      </SectionCard>

      <SectionCard
        captureId="allocation_timeline"
        title={bt('配置時間線', 'Allocation Timeline')}
        subtitle={bt('按再平衡日期顯示目標權重；固定配置會把排程檢查點與實際權重變化分開理解。', 'Shows target weights by rebalance date; fixed-allocation checkpoints are interpreted separately from actual weight changes.')}
      >
        {allocationChartAvailable ? (
          <div data-testid="allocation-timeline-chart">
            <Plot
              data={allocationSeries}
              layout={makeChartLayout({
                xTitle: bt('日期', 'Date'),
                yTitle: bt('目標權重', 'Target Weight'),
                yaxis: { tickformat: '.0%' },
              })}
              config={plotConfig}
              className="plot-card"
              useResizeHandler
              style={{ width: '100%', height: '360px' }}
            />
          </div>
        ) : (
          <MissingState message={bt('這份投資組合結果沒有 holdings 或 target-weight rows，無法繪製配置時間線。', 'This portfolio result has no holdings or target-weight rows for an allocation timeline.')} />
        )}
      </SectionCard>

      </div>

      <div className="screenshot-section-group" data-screenshot-section="metrics4">
      <SectionCard
        captureId="effective_rebalance"
        title={eventPanelTitle}
        subtitle={eventPanelSubtitle}
        actions={
          <select className="text-input text-input-compact" value={String(rebalancePageSize)} onChange={(event) => { setRebalancePageSize(Number(event.target.value)); setRebalancePage(1) }}>
            {[10, 20, 50, 100].map((size) => <option key={size} value={size}>{language === 'zh-Hant' ? `每頁 ${size} 筆` : `${size} / page`}</option>)}
          </select>
        }
      >
        {rebalanceRows.length ? (
          <>
            <div className="data-table-wrap paged-table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>{isCalendarEventStrategy ? bt('時間 (美國交易所)', 'Time (Exchange)') : bt('時間', 'Time')}</th>
                    <th>{eventAssetsLabel}</th>
                    <th>{eventCountColumnLabel}</th>
                    <th>{eventTurnoverLabel}</th>
                    <th>{bt('成本率', 'Cost Rate')}</th>
                    <th>{bt('交易成本', 'Trade Cost')}</th>
                    <th>{bt('資金值', 'Equity')}</th>
                  </tr>
                </thead>
                <tbody>
                  {rebalancePageRows.map((row: any, index: number) => (
                    <tr key={`${row.Time || row.time}-${index}`}>
                      <td>{formatExchangeTime(row)}</td>
                      <td>{rowText(rowValue(row, 'Selected_assets', 'selected_assets', 'Target_assets', 'target_assets'))}</td>
                      <td>{formatWhole(rowValue(row, 'Selected_count', 'selected_count', 'Holdings_count', 'holdings_count'))}</td>
                      <td>{formatMetric('total_return', rowValue(row, 'Turnover', 'turnover'))}</td>
                      <td>{formatMetric('total_return', rowValue(row, 'Cost_rate', 'cost_rate'))}</td>
                      <td>{formatFixed(rowValue(row, 'Trade_cost', 'trade_cost'))}</td>
                      <td>{formatFixed(rowValue(row, 'Equity_value', 'equity_value'))}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="pagination-row">
              <button className="ghost-button" disabled={rebalancePage <= 1} onClick={() => setRebalancePage((current) => Math.max(1, current - 1))}>{bt('上一頁', 'Previous')}</button>
              <span className="muted">{bt('第', 'Page')} {rebalancePage} / {rebalanceTotalPages} {bt('頁', '')}</span>
              <button className="ghost-button" disabled={rebalancePage >= rebalanceTotalPages} onClick={() => setRebalancePage((current) => Math.min(rebalanceTotalPages, current + 1))}>{bt('下一頁', 'Next')}</button>
            </div>
          </>
        ) : (
          <MissingState message={bt('這份結果沒有實際再平衡列；若為固定排程策略，請看檢查點摘要與配置時間線。', 'This result has no actual rebalance rows; for fixed-schedule strategies, review the checkpoint summary and allocation timeline.')} />
        )}
      </SectionCard>

      <SectionCard captureId="asset_contribution" title={bt('資產貢獻', 'Asset Contribution')} subtitle={bt('根據每日目標權重與資產報酬，估算各資產對投資組合報酬的貢獻。', 'Estimates each asset contribution to portfolio return from daily target weights and asset returns.')}>
        {assetContributionRows.length ? (
          <>
            <div className="metric-grid metric-grid-tight">
              <div className="metric-card compact">
                <div className="metric-label">{labelWithHint(bt('投資組合報酬', 'Portfolio Return'), 'portfolio_return', language)}</div>
                <div className="metric-value">{formatMetric('total_return', contributionSummary.portfolio_total_return)}</div>
              </div>
              <div className="metric-card compact">
                <div className="metric-label">{labelWithHint(bt('資產貢獻總和', 'Total Asset Contribution'), 'total_asset_contribution', language)}</div>
                <div className="metric-value">{formatMetric('total_return', contributionSummary.total_asset_contribution)}</div>
              </div>
              <div className="metric-card compact">
                <div className="metric-label">{labelWithHint(bt('殘差 / 複利效果', 'Residual / Compounding'), 'residual_compounding', language)}</div>
                <div className="metric-value">{formatMetric('total_return', contributionSummary.residual_and_compounding)}</div>
              </div>
            </div>
            <div className="two-column-chart-grid">
              <Plot
                data={[{
                  type: 'bar',
                  x: assetContributionRows.map((row: any) => row.asset),
                  y: assetContributionRows.map((row: any) => asNumber(row.return_contribution)),
                  marker: { color: '#e1b12c' },
                  hovertemplate: `%{x}<br>${bt('貢獻', 'Contribution')} %{y:.2%}<extra></extra>`,
                }]}
                layout={makeChartLayout({
                  xTitle: bt('資產', 'Asset'),
                  yTitle: bt('報酬貢獻', 'Return Contribution'),
                  yaxis: { tickformat: '.0%' },
                })}
                config={plotConfig}
                className="plot-card"
                useResizeHandler
                style={{ width: '100%', height: '320px' }}
              />
              <div className="data-table-wrap paged-table-wrap">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>{labelWithHint(bt('資產', 'Asset'), 'asset', language)}</th>
                      <th>{labelWithHint(bt('報酬貢獻', 'Return Contribution'), 'return_contribution', language)}</th>
                      <th>{labelWithHint(bt('貢獻佔比', 'Contribution Share'), 'contribution_share', language)}</th>
                      <th>{labelWithHint(bt('平均權重', 'Average Weight'), 'average_weight', language)}</th>
                      <th>{labelWithHint(bt('持有日數', 'Active Days'), 'active_days', language)}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {assetContributionRows.map((row: any) => (
                      <tr key={row.asset}>
                        <td>{row.asset}</td>
                        <td>{formatMetric('total_return', row.return_contribution)}</td>
                        <td>{formatMetric('total_return', row.contribution_share)}</td>
                        <td>{formatMetric('total_return', row.avg_weight)}</td>
                        <td>{asNumber(row.active_days)?.toFixed(0) ?? '-'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        ) : (
          <MissingState message={bt('重新使用新版引擎執行後，這份投資組合結果會提供資產貢獻資料。', 'Rerun with the newer engine to populate asset-contribution data for this portfolio result.')} />
        )}
      </SectionCard>

      <SectionCard
        captureId="allocation_changes"
        title={bt('配置變化', 'Allocation Changes')}
        subtitle={bt('每次再平衡時，各資產由漂移後權重調整至目標權重所產生的買賣變化。', 'Buy and sell changes created when each rebalance moves assets from drifted weights to target weights.')}
        actions={
          <select className="text-input text-input-compact" value={String(holdingsPageSize)} onChange={(event) => { setHoldingsPageSize(Number(event.target.value)); setHoldingsPage(1) }}>
            {[10, 20, 50, 100].map((size) => <option key={size} value={size}>{language === 'zh-Hant' ? `每頁 ${size} 筆` : `${size} / page`}</option>)}
          </select>
        }
      >
        <div className="data-table-wrap paged-table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>{isCalendarEventStrategy ? bt('時間 (美國交易所)', 'Time (Exchange)') : bt('時間', 'Time')}</th>
                <th>{bt('資產', 'Asset')}</th>
                <th>{bt('動作', 'Action')}</th>
                <th>{bt('調整前權重', 'Before Weight')}</th>
                <th>{bt('目標權重', 'Target Weight')}</th>
                <th>{bt('交易變化', 'Trade Delta')}</th>
                <th>{bt('成本', 'Cost')}</th>
                <th>{bt('單筆盈利', 'Trade PnL')}</th>
              </tr>
            </thead>
            <tbody>
              {holdingsPageRows.map((row: any) => (
                <tr key={`${row.Time || row.entry_time}-${row.Asset || row.asset}-${row.Action || row.status}`}>
                  <td>{formatExchangeTime(row)}</td>
                  <td>{row.Asset || row.asset || '-'}</td>
                  <td>{row.Action || row.action || '-'}</td>
                  <td>{formatMetric('total_return', row.Before_weight ?? row.before_weight)}</td>
                  <td>{formatMetric('total_return', row.Target_weight ?? row.target_weight)}</td>
                  <td>{formatMetric('total_return', row.Trade_delta ?? row.trade_delta)}</td>
                  <td>{asNumber(row.Allocated_cost ?? row.allocated_cost)?.toFixed(4) ?? '-'}</td>
                  <td>{formatOptionalPercent(row.Trade_pnl_pct ?? row.trade_pnl_pct)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="pagination-row">
          <button className="ghost-button" disabled={holdingsPage <= 1} onClick={() => setHoldingsPage((current) => Math.max(1, current - 1))}>{bt('上一頁', 'Previous')}</button>
          <span className="muted">{language === 'zh-Hant' ? `第 ${holdingsPage} / ${holdingsTotalPages} 頁` : `Page ${holdingsPage} / ${holdingsTotalPages}`}</span>
          <button className="ghost-button" disabled={holdingsPage >= holdingsTotalPages} onClick={() => setHoldingsPage((current) => Math.min(holdingsTotalPages, current + 1))}>{bt('下一頁', 'Next')}</button>
        </div>
      </SectionCard>
      </div>
    </div>
  )
}

export function BacktestsPage() {
  const navigate = useNavigate()
  const pathname = useRouterState({ select: (state) => state.location.pathname })
  const search = useRouterState({ select: (state) => state.location.search }) as Record<string, string | undefined>
  const selectedMetricsRunId = useAppStore((state) => state.selectedMetricsRunId)
  const setSelectedMetricsRunId = useAppStore((state) => state.setSelectedMetricsRunId)
  const selectedBacktestId = useAppStore((state) => state.selectedBacktestId)
  const setSelectedBacktestId = useAppStore((state) => state.setSelectedBacktestId)
  const benchmarkVisible = useAppStore((state) => state.benchmarkVisible)
  const setBenchmarkVisible = useAppStore((state) => state.setBenchmarkVisible)
  const language = useAppStore((state) => state.language)
  const t = useCopy(language)
  const bt = (zh: string, en: string) => (language === 'zh-Hant' ? zh : en)
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(10)
  const [sideFilter, setSideFilter] = useState('all')
  const [assetFilter, setAssetFilter] = useState('')
  const [entryFrom, setEntryFrom] = useState('')
  const [entryTo, setEntryTo] = useState('')
  const [minEquityValue, setMinEquityValue] = useState('')
  const [minHolding, setMinHolding] = useState('')
  const [sortKey, setSortKey] = useState('entry_time')
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('asc')
  const [equityScale, setEquityScale] = useState<EquityScale>('linear')

  useEffect(() => {
    preloadPlotly()
  }, [])

  const metricsRunsQuery = useQuery({ queryKey: ['metrics-runs'], queryFn: api.metricsRuns, staleTime: 60000 })
  const runId = search.runId || selectedMetricsRunId || metricsRunsQuery.data?.[0]?.run_id || ''
  const availableRunIds = useMemo(
    () => (metricsRunsQuery.data || []).map((run: any) => run.run_id),
    [metricsRunsQuery.data],
  )
  const hasResolvedRun = Boolean(runId && availableRunIds.includes(String(runId)))
  const searchBacktestId = typeof search.backtestId === 'string' ? search.backtestId : ''

  useEffect(() => {
    if (runId && runId !== selectedMetricsRunId) setSelectedMetricsRunId(runId)
  }, [runId, selectedMetricsRunId, setSelectedMetricsRunId])

  const overviewQuery = useQuery({
    queryKey: ['metrics-overview', runId],
    queryFn: () => api.metricsOverview(runId),
    enabled: hasResolvedRun,
    staleTime: 60000,
  })

  useEffect(() => {
    if (pathname !== '/metrics/backtests') return
    if (!runId || searchBacktestId || !overviewQuery.data?.rows?.[0]?.backtest_id) return
    if (search.runId === runId && search.backtestId === overviewQuery.data.rows[0].backtest_id) return
    navigate({
      to: '/metrics/backtests',
      search: { runId, backtestId: overviewQuery.data.rows[0].backtest_id },
      replace: true,
    })
  }, [pathname, search, searchBacktestId, navigate, overviewQuery.data, runId])

  const availableBacktestIds = useMemo(
    () => (overviewQuery.data?.rows || []).map((row: any) => row.backtest_id),
    [overviewQuery.data],
  )
  const preferredStoreBacktestId = availableBacktestIds.includes(String(selectedBacktestId))
    ? String(selectedBacktestId)
    : ''
  const backtestId = availableBacktestIds.includes(String(searchBacktestId))
    ? String(searchBacktestId)
    : preferredStoreBacktestId || overviewQuery.data?.rows?.[0]?.backtest_id || ''

  useEffect(() => {
    if (backtestId && backtestId !== selectedBacktestId) setSelectedBacktestId(backtestId)
  }, [backtestId, selectedBacktestId, setSelectedBacktestId])

  useEffect(() => {
    if (pathname !== '/metrics/backtests') return
    if (!runId || !backtestId) return
    if (searchBacktestId === backtestId) return
    if (search.runId === runId && search.backtestId === backtestId) return
    navigate({
      to: '/metrics/backtests',
      search: { runId, backtestId },
      replace: true,
    })
  }, [backtestId, navigate, pathname, runId, search, searchBacktestId])

  const detailQuery = useQuery({
    queryKey: ['backtest-detail', runId, backtestId],
    queryFn: () => api.backtestDetail(runId, backtestId),
    enabled: Boolean(hasResolvedRun && backtestId),
    staleTime: 60000,
  })

  const detail = detailQuery.data

  const tradeRows = useMemo(() => {
    const rows = detailQuery.data?.trade_rows || []
    return [...rows]
      .filter((row: any) => (sideFilter === 'all' ? true : row.side === sideFilter))
      .filter((row: any) => (assetFilter ? String(row.asset || '').toLowerCase().includes(assetFilter.toLowerCase()) : true))
      .filter((row: any) => (entryFrom ? String(row.entry_time || '') >= entryFrom : true))
      .filter((row: any) => (entryTo ? String(row.entry_time || '') <= `${entryTo}T23:59:59` : true))
      .filter((row: any) => (minEquityValue ? (row.equity_value ?? Number.NEGATIVE_INFINITY) >= Number(minEquityValue) : true))
      .filter((row: any) => (minHolding ? (row.holding_period ?? Number.NEGATIVE_INFINITY) >= Number(minHolding) : true))
      .sort((left: any, right: any) => {
        const leftValue = left[sortKey]
        const rightValue = right[sortKey]
        if (leftValue === rightValue) return 0
        const result = leftValue > rightValue ? 1 : -1
        return sortDirection === 'asc' ? result : -result
      })
  }, [detailQuery.data, sideFilter, assetFilter, entryFrom, entryTo, minEquityValue, minHolding, sortKey, sortDirection])

  const totalPages = Math.max(1, Math.ceil(tradeRows.length / pageSize))
  const pageRows = tradeRows.slice((page - 1) * pageSize, page * pageSize)
  const tradeOutcome = useMemo(() => {
    const rows = detailQuery.data?.trade_rows || []
    return buildTradeOutcomeSummary(rows, detailQuery.data?.trade_outcome_summary)
  }, [detailQuery.data])
  const tradeOutcomeApplicable = tradeOutcome.hasTradeReturnField && tradeOutcome.closedCount > 0
  const tradeOutcomeMessage = tradeOutcome.hasTradeReturnField
    ? tradeOutcomeUnavailableMessage({ applicable: true, closedCount: tradeOutcome.closedCount, language })
    : bt(
      '因為這份回測沒有輸出逐筆已平倉交易報酬欄位，所以交易報酬分佈與勝負甜甜圈不適用。請改看交易紀錄、資金曲線與風險診斷。',
      'Because this backtest does not output closed-trade return rows, trade-return distribution and the win/loss donut are not applicable. Review the trade table, equity curve, and risk diagnostics instead.',
    )

  useEffect(() => {
    if (page > totalPages) setPage((current) => (current === totalPages ? current : totalPages))
  }, [page, totalPages])

  const entryExitMarkers = useMemo(() => {
    const ohlcRows = detail?.ohlc || []
    const candleMap = new Map(
      ohlcRows.map((item: any) => [String(item.time), { low: Number(item.low), high: Number(item.high) }]),
    )
    const lows = ohlcRows.map((item: any) => Number(item.low)).filter((value: number) => Number.isFinite(value))
    const highs = ohlcRows.map((item: any) => Number(item.high)).filter((value: number) => Number.isFinite(value))
    const range = lows.length && highs.length ? Math.max(...highs) - Math.min(...lows) : 0
    const offset = range > 0 ? range * 0.018 : 1

    const buys = (detail?.buy_markers || []).map((item: any) => {
      const candle = candleMap.get(String(item.time)) as { low: number; high: number } | undefined
      const base = candle ? candle.low : Number(item.price)
      return {
        time: item.time,
        price: base - offset,
        hoverPrice: Number(item.price),
      }
    })

    const sells = (detail?.sell_markers || []).map((item: any) => {
      const candle = candleMap.get(String(item.time)) as { low: number; high: number } | undefined
      const base = candle ? candle.high : Number(item.price)
      return {
        time: item.time,
        price: base + offset,
        hoverPrice: Number(item.price),
      }
    })

    return { buys, sells }
  }, [detail])

  if (metricsRunsQuery.isLoading || (!hasResolvedRun && metricsRunsQuery.data?.length) || overviewQuery.isLoading || detailQuery.isLoading) {
    return <div className="page-loading">{t('common.loading.backtestDetail')}</div>
  }
  if (!runId) return <MissingState message={t('backtests.selectMetricsFirst')} />
  if (!backtestId) return <MissingState message={t('backtests.noBacktestSelected')} />
  if (detailQuery.error || !detailQuery.data) return <div className="page-error">{bt('無法載入回測詳細資料。', 'Unable to load backtest detail.')}</div>
  if (detail?.result_type === 'portfolio' || detail?.contract_id === 'lo2cin4bt-app-portfolio-detail-payload-v1') {
    return <PortfolioBacktestDetail detail={detail} />
  }

  const metrics = detail.metrics_matrix || {}
  const firstTime = detail.ohlc?.[0]?.time || 'n/a'
  const lastTime = detail.ohlc?.[detail.ohlc.length - 1]?.time || 'n/a'
  const benchmarkLabel = benchmarkDisplayLabel(
    metrics.benchmark_label || detail.strategy_summary?.benchmark_label,
    language === 'zh-Hant' ? '基準' : 'Benchmark',
  )

  const heroMetrics = [
    {
      key: 'total_return',
      accent: bt('主要表現', 'Primary Performance'),
      meta: `${bt('基準', 'Benchmark')} ${formatMetric('bah_total_return', metrics.bah_total_return)}`,
      delta: `${bt('超額', 'Excess')} ${formatMetric('excess_return', metrics.excess_return)}`,
    },
    {
      key: 'cagr',
      accent: bt('年化表現', 'Annualized Performance'),
      meta: `${bt('基準', 'Benchmark')} ${formatMetric('bah_cagr', metrics.bah_cagr)}`,
      delta: null,
    },
    {
      key: 'sharpe',
      accent: bt('風險調整', 'Risk Adjusted'),
      meta: `${bt('基準', 'Benchmark')} ${formatMetric('bah_sharpe', metrics.bah_sharpe)}`,
      delta: null,
    },
    {
      key: 'mdd',
      accent: bt('資金壓力', 'Equity Stress'),
      meta: `Calmar ${formatMetric('calmar', metrics.calmar)}`,
      delta: `${bt('平均回撤', 'Average Drawdown')} ${formatMetric('average_drawdown', metrics.average_drawdown)}`,
    },
  ]

  const tradeQualityKeys = ['win_rate', 'profit_factor', 'avg_trade_return', 'trade_count', 'exposure_time']
  const riskKeys = ['sortino', 'calmar', 'recovery_factor', 'annualized_std', 'information_ratio']
  const benchmarkKeys = ['bah_total_return', 'bah_cagr', 'bah_sharpe', 'bah_calmar', 'excess_return']
  const advancedKeys = ['std', 'average_drawdown']

  return (
    <div className="page-stack">
      <SectionCard title={t('backtests.summary')} subtitle={t('backtests.summarySubtitle')}>
        <div className="snapshot-hero-shell">
          <div className="snapshot-identity-card">
            <div className="snapshot-eyebrow">{bt('目前選取', 'Current Selection')}</div>
            <div className="snapshot-strategy-title">{detail.label}</div>
            <div className="snapshot-strategy-subtitle">
              {language === 'zh-Hant'
                ? '目前參數組合的回測摘要，先檢視主要指標，再深入圖表與交易紀錄。'
                : 'Backtest summary for the active parameter set. Read headline metrics first, then inspect charts and trade records.'}
            </div>
            <div className="snapshot-meta-grid">
              <div className="snapshot-meta-pill">
                <span className="snapshot-meta-label">{language === 'zh-Hant' ? '回測期間' : 'Backtest Window'}</span>
                <span className="snapshot-meta-value" data-private-strategy>{firstTime} {'->'} {lastTime}</span>
              </div>
              <div className="snapshot-meta-pill">
                <span className="snapshot-meta-label">{language === 'zh-Hant' ? '交易次數' : 'Trades'}</span>
                <span className="snapshot-meta-value" data-private-strategy>{formatMetric('trade_count', metrics.trade_count)}</span>
              </div>
              <div className="snapshot-meta-pill">
                <span className="snapshot-meta-label">{language === 'zh-Hant' ? '勝率' : 'Win Rate'}</span>
                <span className={`snapshot-meta-value ${toneClass('win_rate', metrics.win_rate)}`} data-private-strategy>{formatMetric('win_rate', metrics.win_rate)}</span>
              </div>
              <div className="snapshot-meta-pill">
                <span className="snapshot-meta-label">{language === 'zh-Hant' ? '相對買入持有超額' : 'Excess vs BAH'}</span>
                <span className={`snapshot-meta-value ${toneClass('excess_return', metrics.excess_return)}`} data-private-strategy>{formatMetric('excess_return', metrics.excess_return)}</span>
              </div>
            </div>
          </div>

          <div className="snapshot-primary-grid">
            {heroMetrics.map((item) => (
              <div key={item.key} className="snapshot-primary-card">
                <div className="snapshot-primary-accent">{item.accent}</div>
                <div className="snapshot-primary-label">{metricLabelWithHint(item.key, language)}</div>
                <div className={`snapshot-primary-value ${toneClass(item.key, metrics[item.key])}`}>{formatMetric(item.key, metrics[item.key])}</div>
                <div className="snapshot-primary-meta">{item.meta}</div>
                {item.delta ? <div className="snapshot-primary-delta">{item.delta}</div> : null}
              </div>
            ))}
          </div>
        </div>
      </SectionCard>

      <SectionCard
        title={t('backtests.contextPanels')}
        subtitle={language === 'zh-Hant' ? '將交易品質、風險與基準資訊分組顯示，方便快速檢視。' : 'Grouped trade quality, risk, and benchmark information for quick review.'}
      >
        <div className="snapshot-context-grid">
          <div className="kpi-panel" data-private-strategy>
            <div className="kpi-panel-title">{language === 'zh-Hant' ? '交易品質' : 'Trade Quality'}</div>
            <div className="kpi-compact-list">
              {tradeQualityKeys.map((key) => (
                <div key={key} className="kpi-compact-row">
                  <span className="kpi-compact-label">{metricLabelWithHint(key, language)}</span>
                  <span className={`kpi-compact-value ${toneClass(key, metrics[key])}`}>{formatMetric(key, metrics[key])}</span>
                </div>
              ))}
            </div>
          </div>

          <div className="kpi-panel" data-private-strategy>
            <div className="kpi-panel-title">{language === 'zh-Hant' ? '風險與穩定度' : 'Risk & Stability'}</div>
            <div className="kpi-compact-list">
              {riskKeys.map((key) => (
                <div key={key} className="kpi-compact-row">
                  <span className="kpi-compact-label">{metricLabelWithHint(key, language)}</span>
                  <span className={`kpi-compact-value ${toneClass(key, metrics[key])}`}>{formatMetric(key, metrics[key])}</span>
                </div>
              ))}
            </div>
          </div>

          <div className="kpi-panel" data-private-strategy>
            <div className="kpi-panel-title">{language === 'zh-Hant' ? '基準比較' : 'Benchmark Context'}</div>
            <div className="kpi-compact-list">
              {benchmarkKeys.map((key) => (
                <div key={key} className="kpi-compact-row">
                  <span className="kpi-compact-label">{metricLabelWithHint(key, language)}</span>
                  <span className={`kpi-compact-value ${toneClass(key, metrics[key])}`}>{formatMetric(key, metrics[key])}</span>
                </div>
              ))}
            </div>
          </div>
        </div>

        <details className="advanced-metrics">
          <summary>
            <span className="advanced-metrics-chevron" aria-hidden="true">▾</span>
            <span>{language === 'zh-Hant' ? '顯示進階原始指標' : 'Show Advanced Raw Metrics'}</span>
          </summary>
          <div className="kpi-compact-list kpi-compact-list-full">
            {advancedKeys.map((key) => (
              <div key={key} className="kpi-compact-row">
                <span className="kpi-compact-label">{metricLabelWithHint(key, language)}</span>
                <span className={`kpi-compact-value ${toneClass(key, metrics[key])}`}>{formatMetric(key, metrics[key])}</span>
              </div>
            ))}
          </div>
        </details>
      </SectionCard>

      <RiskDiagnosticsPanel
        diagnostics={detail.risk_diagnostics}
        equitySeries={detail.equity_series || []}
        language={language}
      />

      <SectionCard
        title={t('backtests.equityVsBenchmark')}
        actions={
          <div className="inline-actions">
            <BenchmarkToggleButton
              visible={benchmarkVisible}
              language={language}
              onChange={setBenchmarkVisible}
            />
            <select
              className="text-input text-input-compact"
              value={equityScale}
              onChange={(event) => setEquityScale(event.target.value as EquityScale)}
              aria-label={bt('資金曲線刻度', 'Equity scale')}
            >
              <option value="linear">{bt('線性刻度', 'Linear Scale')}</option>
              <option value="log">{bt('對數刻度', 'Log Scale')}</option>
            </select>
          </div>
        }
      >
        <Plot
          data={[
            {
              type: 'scatter',
              mode: 'lines',
              name: bt('資金曲線', 'Equity Curve'),
              x: detail.equity_series.map((item: any) => item.time),
              y: detail.equity_series.map((item: any) => chartValue(item.value, equityScale)),
            },
            ...(benchmarkVisible && detail.benchmark_series?.length
              ? [
                  {
                    type: 'scatter',
                    mode: 'lines',
                    name: benchmarkLabel,
                    x: detail.benchmark_series.map((item: any) => item.time),
                    y: detail.benchmark_series.map((item: any) => chartValue(item.value, equityScale)),
                    line: { dash: 'dash' },
                  },
                ]
              : []),
          ]}
          layout={makeChartLayout({
            xTitle: bt('日期', 'Date'),
            yTitle: equityScale === 'log' ? bt('資金曲線（對數）', 'Equity Curve (Log)') : bt('資金曲線', 'Equity Curve'),
            yaxis: { type: equityScale },
          })}
          config={plotConfig}
          className="plot-card"
          useResizeHandler
          style={{ width: '100%', height: '280px' }}
        />
      </SectionCard>

      <SectionCard title={t('backtests.entryExits')}>
        <Plot
          data={[
            {
              type: 'candlestick',
              x: detail.ohlc.map((item: any) => item.time),
              open: detail.ohlc.map((item: any) => item.open),
              high: detail.ohlc.map((item: any) => item.high),
              low: detail.ohlc.map((item: any) => item.low),
              close: detail.ohlc.map((item: any) => item.close),
              name: bt('價格', 'Price'),
            },
            {
              type: 'scatter',
              mode: 'markers',
              x: entryExitMarkers.buys.map((item: any) => item.time),
              y: entryExitMarkers.buys.map((item: any) => item.price),
              customdata: entryExitMarkers.buys.map((item: any) => item.hoverPrice),
              name: bt('買入', 'Buy'),
              marker: {
                color: '#45d7ff',
                size: 14,
                symbol: 'triangle-up',
                line: { color: '#ffffff', width: 1.8 },
              },
              hovertemplate: `${bt('買入', 'Buy')}<br>%{x}<br>${bt('價格', 'Price')} %{customdata:.3f}<extra></extra>`,
            },
            {
              type: 'scatter',
              mode: 'markers',
              x: entryExitMarkers.sells.map((item: any) => item.time),
              y: entryExitMarkers.sells.map((item: any) => item.price),
              customdata: entryExitMarkers.sells.map((item: any) => item.hoverPrice),
              name: bt('賣出', 'Sell'),
              marker: {
                color: '#ffcc33',
                size: 14,
                symbol: 'triangle-down',
                line: { color: '#111217', width: 1.8 },
              },
              hovertemplate: `${bt('賣出', 'Sell')}<br>%{x}<br>${bt('價格', 'Price')} %{customdata:.3f}<extra></extra>`,
            },
          ]}
          layout={makeChartLayout({
            xTitle: bt('日期', 'Date'),
            yTitle: bt('價格', 'Price'),
            xaxis: { rangeslider: { visible: false } },
            margin: { l: 64, r: 24, t: 24, b: 58 },
          })}
          config={plotConfig}
          className="plot-card"
          useResizeHandler
          style={{ width: '100%', height: '460px' }}
        />
      </SectionCard>

      <SectionCard title={t('backtests.tradeReturnDistribution')} subtitle={bt('目前回測已平倉交易的報酬分佈與勝負分類。', 'Closed-trade return distribution and outcome mix for the selected backtest.')}>
        {tradeOutcomeApplicable && tradeOutcome.chartReady ? (
          <div className="two-column-chart-grid trade-return-chart-grid">
            <Plot
              data={[{
                type: 'histogram',
                x: tradeOutcome.returns,
                nbinsx: Math.min(40, Math.max(10, Math.ceil(Math.sqrt(tradeOutcome.returns.length)))),
                marker: { color: '#e1b12c' },
                hovertemplate: `${bt('報酬', 'Return')} %{x:.2%}<br>${bt('交易數', 'Trades')} %{y}<extra></extra>`,
              }]}
              layout={makeChartLayout({
                xTitle: bt('已平倉交易報酬', 'Closed Trade Return'),
                yTitle: bt('交易數', 'Trades'),
                xaxis: { tickformat: '.1%' },
                shapes: [{
                  type: 'line',
                  x0: 0,
                  x1: 0,
                  y0: 0,
                  y1: 1,
                  xref: 'x',
                  yref: 'paper',
                  line: { color: 'rgba(223, 230, 245, 0.55)', width: 1, dash: 'dot' },
                }],
              })}
              config={plotConfig}
              className="plot-card"
              useResizeHandler
              style={{ width: '100%', height: '320px' }}
            />
            <div className="trade-outcome-panel">
              <Plot
                data={[{
                  type: 'pie',
                  hole: 0.58,
                  labels: [bt('獲利', 'Win'), bt('虧損', 'Loss'), bt('打平', 'Breakeven')],
                  values: [tradeOutcome.winCount, tradeOutcome.lossCount, tradeOutcome.breakevenCount],
                  marker: { colors: ['#7ae0a4', '#ff9b8d', '#94a3b8'] },
                  textinfo: 'none',
                  hovertemplate: '%{label}<br>%{value} (%{percent})<extra></extra>',
                }]}
                layout={makeChartLayout({
                  margin: { l: 24, r: 24, t: 24, b: 24 },
                  showlegend: false,
                })}
                config={plotConfig}
                className="plot-card"
                useResizeHandler
                style={{ width: '100%', height: '320px' }}
              />
              <TradeOutcomeStats outcome={tradeOutcome} language={language} />
            </div>
            <div className="portfolio-context-rail" style={{ gridColumn: '1 / -1' }}>
              <div className="portfolio-context-item">
                <span className="portfolio-context-label">{bt('已平倉交易', 'Closed Trades')}</span>
                <span className="portfolio-context-value">{formatMetric('trade_count', tradeOutcome.closedCount)}</span>
              </div>
              <div className="portfolio-context-item">
                <span className="portfolio-context-label">{bt('平均獲利', 'Average Win')}</span>
                <span className="portfolio-context-value tone-positive">{formatMetric('average_win', tradeOutcome.avgWin)}</span>
              </div>
              <div className="portfolio-context-item">
                <span className="portfolio-context-label">{bt('平均虧損', 'Average Loss')}</span>
                <span className="portfolio-context-value tone-negative">{formatMetric('average_loss', tradeOutcome.avgLoss)}</span>
              </div>
              <div className="portfolio-context-item">
                <span className="portfolio-context-label">{bt('獲利因子', 'Profit Factor')}</span>
                <span className="portfolio-context-value">{formatMetric('profit_factor', tradeOutcome.profitFactor)}</span>
              </div>
            </div>
          </div>
        ) : (
          <MissingState message={tradeOutcomeMessage} />
        )}
      </SectionCard>

      <SectionCard
        title={t('backtests.tradeSummary')}
        subtitle={t('backtests.tradeSummarySubtitle')}
        actions={
          <select className="text-input text-input-compact" value={String(pageSize)} onChange={(event) => { setPageSize(Number(event.target.value)); setPage(1) }}>
            {[10, 20, 50, 100].map((size) => <option key={size} value={size}>{language === 'zh-Hant' ? `${size} / 頁` : `${size} / page`}</option>)}
          </select>
        }
      >
        <div className="filter-grid trade-table-toolbar">
          <select className="text-input" value={sideFilter} onChange={(event) => { setSideFilter(event.target.value); setPage(1) }}>
            <option value="all">{bt('全部方向', 'All Sides')}</option>
            <option value="long">{bt('做多', 'Long')}</option>
            <option value="short">{bt('做空', 'Short')}</option>
          </select>
          <input className="text-input" placeholder={bt('資產包含...', 'Asset contains...')} value={assetFilter} onChange={(event) => { setAssetFilter(event.target.value); setPage(1) }} />
          <input className="text-input" type="date" value={entryFrom} onChange={(event) => { setEntryFrom(event.target.value); setPage(1) }} />
          <input className="text-input" type="date" value={entryTo} onChange={(event) => { setEntryTo(event.target.value); setPage(1) }} />
          <input className="text-input" placeholder={bt('資金值 >= ', 'Equity >= ')} value={minEquityValue} onChange={(event) => { setMinEquityValue(event.target.value); setPage(1) }} />
          <input className="text-input" placeholder={bt('持有期 >= ', 'Holding Period >= ')} value={minHolding} onChange={(event) => { setMinHolding(event.target.value); setPage(1) }} />
          <select className="text-input" value={sortKey} onChange={(event) => setSortKey(event.target.value)}>
            {['entry_time', 'exit_time', 'equity_value', 'price_pnl', 'trade_return', 'holding_period'].map((key) => (
              <option key={key} value={key}>{language === 'zh-Hant' ? `按 ${metricLabel(key, language)} 排序` : `Sort by ${metricLabel(key, language)}`}</option>
            ))}
          </select>
          <select className="text-input" value={sortDirection} onChange={(event) => setSortDirection(event.target.value as 'asc' | 'desc')}>
            <option value="asc">{bt('由小至大', 'Ascending')}</option>
            <option value="desc">{bt('由大至小', 'Descending')}</option>
          </select>
        </div>
        <div className="data-table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>{bt('排名', 'Rank')}</th>
                <th>{bt('資產', 'Asset')}</th>
                <th>{bt('方向', 'Side')}</th>
                <th>{bt('入場時間', 'Entry Time')}</th>
                <th>{bt('出場時間', 'Exit Time')}</th>
                <th>{bt('入場價格', 'Entry Price')}</th>
                <th>{bt('出場價格', 'Exit Price')}</th>
                <th>{bt('持有期', 'Holding Period')}</th>
                <th>{bt('每單位價差盈虧', 'Price PnL / Unit')}</th>
                <th>{bt('交易報酬', 'Trade Return')}</th>
                <th>{bt('資金值', 'Equity')}</th>
                <th>{bt('狀態', 'Status')}</th>
              </tr>
            </thead>
            <tbody>
              {pageRows.map((row: any) => (
                <tr key={row.trade_group_id}>
                  <td>{row.rank}</td>
                  <td>{row.asset}</td>
                  <td>{row.side}</td>
                  <td>{row.entry_time}</td>
                  <td>{row.exit_time}</td>
                  <td>{row.entry_price?.toFixed?.(3) ?? row.entry_price}</td>
                  <td>{row.exit_price?.toFixed?.(3) ?? row.exit_price}</td>
                  <td>{row.holding_period?.toFixed?.(0) ?? row.holding_period}</td>
                  <td>{row.price_pnl?.toFixed?.(3) ?? row.price_pnl}</td>
                  <td>{row.trade_return?.toFixed?.(3) ?? row.trade_return}</td>
                  <td>{row.equity_value?.toFixed?.(3) ?? row.equity_value}</td>
                  <td>{tradeStatusLabel(row.status, language)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="pagination-row">
          <button className="ghost-button" disabled={page <= 1} onClick={() => setPage((current) => Math.max(1, current - 1))}>{bt('上一頁', 'Previous')}</button>
          <span className="muted">{language === 'zh-Hant' ? `第 ${page} / ${totalPages} 頁` : `Page ${page} / ${totalPages}`}</span>
          <button className="ghost-button" disabled={page >= totalPages} onClick={() => setPage((current) => Math.min(totalPages, current + 1))}>{bt('下一頁', 'Next')}</button>
        </div>
      </SectionCard>
    </div>
  )
}
