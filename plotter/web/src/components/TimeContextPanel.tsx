import { SectionCard } from './SectionCard'
import { useAppStore } from '../store'

type TimeContextPanelProps = {
  summary?: Record<string, any>
}

function barSpecLabel(stream: any): string {
  const spec = stream?.bar_spec || {}
  const step = Number(spec.step)
  const unit = String(spec.unit || '')
  if (!Number.isFinite(step) || step <= 0 || !unit) return '-'
  const frequency = `${step} ${step === 1 ? unit : `${unit}s`}`
  const alignment = String(spec.alignment || '')
  const priceType = String(spec.price_type || '')
  return [frequency, alignment, priceType].filter(Boolean).join(' | ')
}

function sourceLabel(stream: any): string {
  const source = stream?.source || {}
  if (source.kind === 'derived') {
    return `derived from ${source.parent_stream_id || '-'} | ${source.aggregation_engine || '-'}`
  }
  return [source.kind, source.provider_id].filter(Boolean).join(' | ') || '-'
}

export function TimeContextPanel({ summary = {} }: TimeContextPanelProps) {
  const language = useAppStore((state) => state.language)
  const context = summary.time_context || {}
  const execution = context.execution || {}
  const decision = context.decision || {}
  const session = context.session || {}
  const timestamp = context.timestamp || {}
  const annualization = summary.annualization || {}
  if (!execution.stream_id || !decision.stream_id) return null
  const copy = language === 'zh-Hant'
    ? {
        title: '周期、時間與績效口徑',
        subtitle: '直接顯示正式 typed contract；不由前端推斷周期或年化方式。',
        execution: '執行 K 線',
        decision: '決策 K 線',
        timestamp: '時間戳語義',
        session: '交易時段',
        annualization: '績效／年化基礎',
        periods: '期／年',
        riskFree: '年化無風險率',
      }
    : {
        title: 'Timeframe, Time, and Performance Basis',
        subtitle: 'Projected from the typed contract; the frontend does not infer timeframe or annualization.',
        execution: 'Execution Bars',
        decision: 'Decision Bars',
        timestamp: 'Timestamp Semantics',
        session: 'Session',
        annualization: 'Performance / Annualization Basis',
        periods: 'periods/year',
        riskFree: 'annual risk-free rate',
      }
  const executionSemantics = execution.timestamp_semantics || {}
  const decisionSemantics = decision.timestamp_semantics || {}
  const timestampValue = [
    timestamp.time_standard,
    `execution ${executionSemantics.timestamp_convention || '-'}`,
    `available ${executionSemantics.availability_policy || '-'}`,
    execution.stream_id === decision.stream_id
      ? null
      : `decision ${decisionSemantics.timestamp_convention || '-'} / available ${decisionSemantics.availability_policy || '-'}`,
  ].filter(Boolean).join(' | ')
  const sessionValue = [
    session.calendar_id,
    session.timezone,
    session.session_scope,
    session.session_label_policy,
  ].filter(Boolean).join(' | ')
  const annualizationValue = annualization.schema_version === 'metrics_annualization.v1'
    ? [
        annualization.basis,
        annualization.projection_policy,
        `${annualization.periods_per_year} ${copy.periods}`,
        `${copy.riskFree} ${Number(annualization.risk_free_rate_annual).toLocaleString(undefined, { style: 'percent', maximumFractionDigits: 4 })}`,
      ].join(' | ')
    : annualization.status === 'unavailable'
      ? `${annualization.status} | ${annualization.reason || '-'}`
      : '-'

  const rows = [
    {
      label: copy.execution,
      value: `${execution.stream_id} | ${barSpecLabel(execution)} | ${sourceLabel(execution)}`,
    },
    {
      label: copy.decision,
      value: `${decision.stream_id} | ${barSpecLabel(decision)} | ${sourceLabel(decision)}`,
    },
    { label: copy.timestamp, value: timestampValue || '-' },
    { label: copy.session, value: sessionValue || '-' },
    { label: copy.annualization, value: annualizationValue },
  ]

  return (
    <SectionCard title={copy.title} subtitle={copy.subtitle}>
      <div className="metrics-strategy-summary time-context-panel" data-testid="time-context-panel">
        {rows.map((row) => (
          <div className="metrics-strategy-line" key={row.label}>
            <span>{row.label}</span>
            <strong>{row.value}</strong>
          </div>
        ))}
      </div>
    </SectionCard>
  )
}
