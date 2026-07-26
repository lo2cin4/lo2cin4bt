import type { Language } from '../i18n'
import { useAppStore } from '../store'

type StrategyRulesPanelProps = {
  summary?: Record<string, any>
  loading?: boolean
  className?: string
}

const strategyRulesCopy = {
  en: {
    title: 'Strategy Rules',
    asset: 'Asset',
    mode: 'Mode',
    workflow: 'Workflow',
    execution: 'Execution',
    costs: 'Costs',
    entry: 'Entry',
    exit: 'Exit',
    domain: 'Parameter Range',
    calendar: 'Trading Calendar',
    unavailable: 'Not provided',
    loading: 'Loading...',
  },
  'zh-Hant': {
    title: '策略規則',
    asset: '標的',
    mode: '模式',
    workflow: '流程',
    execution: '執行',
    costs: '成本',
    entry: '入場',
    exit: '出場',
    domain: '參數範圍',
    calendar: '交易日曆',
    unavailable: '未提供',
    loading: '載入中...',
  },
} as const

export function hasRenderableStrategySummary(summary: any) {
  if (!summary) return false
  return [
    'asset_label',
    'period_label',
    'mode_label',
    'workflow_label',
    'execution_label',
    'cost_label',
    'entry_rule',
    'exit_rule',
    'parameter_domain_label',
  ].some((key) => String(summary?.[key] || '').trim())
}

function strategyRuleValue(summary: any, key: string, loading: boolean, unavailable: string, loadingText: string) {
  const value = String(summary?.[key] || '').trim()
  if (value) return value
  return loading ? loadingText : unavailable
}

function visibleValues(values: string[], unavailable: string, loadingText: string) {
  return values.filter((value) => value && value !== unavailable && value !== loadingText)
}

export function localizeStrategyRuleValue(key: string, rawValue: string, language: Language) {
  if (language !== 'zh-Hant') return rawValue
  let value = rawValue
  const normalized = value.trim().toLowerCase()
  const exactZh: Record<string, string> = {
    'single-asset signal strategy': '單資產訊號策略',
    'multi-asset portfolio': '多資產投資組合',
    'multi-asset signal portfolio': '多資產訊號投資組合',
    'calendar/session event trading': '日曆 / 交易時段事件策略',
    'parameter matrix': '參數矩陣',
    'single backtest': '單次回測',
    'portfolio backtest': '投資組合回測',
    'walk-forward analysis': '前向分析 (WFA)',
    'rolling validation': '前向分析 (WFA)',
    'signal close for next bar at close to close': '訊號於收盤確認，下一根 K 線以收盤價執行',
    'next bar after signal at open': '訊號後下一根 K 線開盤執行',
    'signal bar at open': '訊號當根 K 線開盤執行',
    'signal bar at close': '訊號當根 K 線收盤執行',
    'rebalance on signal.change': '訊號狀態改變時才交易',
    'rebalance on calendar.every_session': '每個交易日檢查並再平衡',
    'rebalance on calendar.year_start': '每年第一個交易日再平衡',
    'replaced or resized at next rebalance': '於下一次再平衡時替換或調整持倉',
    'fixed weights': '固定權重',
  }
  if (exactZh[normalized]) return exactZh[normalized]

  if (key === 'asset_label') {
    return value
      .replace(/\blatest available\b/gi, '最新可用')
      .replace(/\bassets\b/gi, '個標的')
  }

  if (key === 'cost_label') {
    return value
      .replace(/\btransaction cost\b/gi, '交易成本')
      .replace(/\bslippage\b/gi, '滑價')
      .replace(/;/g, '；')
  }

  if (key === 'parameter_domain_label') {
    return value
      .replace(/:\s*/g, '：')
      .replace(/\bto\b/gi, '至')
      .replace(/\bstep\b/gi, '每次')
      .replace(/\brank_by\b/gi, '排序依據')
      .replace(/\btop_n\b/gi, '選取數量')
      .replace(/\bposition_limit\b/gi, '持倉上限')
  }

  if (key === 'entry_rule' || key === 'exit_rule') {
    value = value.replace(
      /^For ([A-Z0-9_,\s-]+), target 100% ([A-Z0-9]+) when its close is above its own SMA$/i,
      '$1 收盤價高於自身 SMA 時，目標持倉為 100% $2',
    )
    value = value.replace(
      /^For ([A-Z0-9_,\s-]+) and ([A-Z0-9_,\s-]+), target the asset when its close is above its own SMA$/i,
      '$1 與 $2 收盤價高於自身 SMA 時，將該資產納入目標持倉',
    )
    value = value.replace(
      /^Move to cash when ([A-Z0-9]+) close is below its own SMA$/i,
      '$1 收盤價低於自身 SMA 時轉為現金',
    )
    value = value.replace(
      /^Set the asset back to cash when its close is below its own SMA$/i,
      '資產收盤價低於自身 SMA 時轉為現金',
    )
    value = value.replace(
      /^Short ([A-Z0-9]+) at the open on dates listed in ([^;]+);\s*skip non-trading dates$/i,
      '$1 在 $2 列出的日期開盤放空；非交易日會略過',
    )
    value = value.replace(/^Close the short at the same session close$/i, '同一交易時段收盤平倉')
    value = localizeStrategyRuleValue('', value, language)
  }

  return value
}

export function StrategyRulesPanel({ summary = {}, loading = false, className = '' }: StrategyRulesPanelProps) {
  const language = useAppStore((state) => state.language)
  const copy = strategyRulesCopy[language]
  const valueFor = (key: string, useLoading = loading) =>
    localizeStrategyRuleValue(key, strategyRuleValue(summary, key, useLoading, copy.unavailable, copy.loading), language)
  const context = visibleValues([
    valueFor('asset_label'),
    valueFor('period_label'),
    valueFor('frequency_label'),
  ], copy.unavailable, copy.loading)
  const calendar = visibleValues([
    valueFor('calendar_label', false),
    valueFor('timezone_label', false),
  ], copy.unavailable, copy.loading)

  return (
    <div className={`metrics-strategy-summary ${className}`} data-private-strategy>
      <div className="metrics-header-label">{copy.title}</div>
      <div className="metrics-strategy-line">
        <span>{copy.asset}</span>
        <strong>{context.length ? context.join(' | ') : valueFor('asset_label')}</strong>
      </div>
      <div className="metrics-strategy-line">
        <span>{copy.mode}</span>
        <strong>{valueFor('mode_label')}</strong>
      </div>
      <div className="metrics-strategy-line">
        <span>{copy.workflow}</span>
        <strong>{valueFor('workflow_label')}</strong>
      </div>
      <div className="metrics-strategy-line">
        <span>{copy.execution}</span>
        <strong>{valueFor('execution_label')}</strong>
      </div>
      <div className="metrics-strategy-line">
        <span>{copy.costs}</span>
        <strong>{valueFor('cost_label')}</strong>
      </div>
      <div className="metrics-strategy-line">
        <span>{copy.entry}</span>
        <strong>{valueFor('entry_rule')}</strong>
      </div>
      <div className="metrics-strategy-line">
        <span>{copy.exit}</span>
        <strong>{valueFor('exit_rule')}</strong>
      </div>
      <div className="metrics-strategy-line">
        <span>{copy.domain}</span>
        <strong>{valueFor('parameter_domain_label')}</strong>
      </div>
      {calendar.length ? (
        <div className="metrics-strategy-line">
          <span>{copy.calendar}</span>
          <strong>{calendar.join(' | ')}</strong>
        </div>
      ) : null}
    </div>
  )
}
