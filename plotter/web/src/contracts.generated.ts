// Generated contract surface from chart-payload-v1 and metrics overview schemas.
export interface PlotSeriesV1 {
  series_id: string
  label: string
  x: string[]
  y: number[]
  annotations: string[]
}

export interface PlotBundleV1 {
  schema_version: 'plot_bundle.v1'
  contract_id: 'lo2cin4bt.plot_bundle.v1'
  run_id: string
  chart_type: string
  title: string
  series: PlotSeriesV1[]
  axes: { x: string; y: string }
  legend: string[]
  source_hashes: string[]
  artifact_source_refs: string[]
  generated_at: string
}

export interface MetricsOverviewRowV1 {
  backtest_id: string
  label: string
  label_source: string
  strategy_id: string
  semantic_combo: Record<string, unknown>
  semantic_fields: unknown[]
  total_return: number | null
  cagr: number | null
  sharpe: number | null
  sortino: number | null
  calmar: number | null
  max_drawdown: number | null
  intraday_max_drawdown?: number | null
  projected_session_count?: number | null
  projected_return_interval_count?: number | null
  [key: string]: unknown
}

export interface BarSpecV1 {
  aggregation: 'time'
  step: number
  unit: 'minute' | 'hour' | 'day' | 'week' | 'month'
  price_type: 'last' | 'mark' | 'mid'
  alignment: string
}

export interface BoundTimeStreamV1 {
  stream_id: string
  role: 'execution' | 'decision'
  source: Record<string, unknown>
  bar_spec: BarSpecV1
  timestamp_semantics: {
    timestamp_convention: string
    interval_boundary: string
    bar_open_time_column: string
    bar_close_time_column: string
    available_time_column: string
    session_label_column: string
    availability_policy: string
  }
}

export interface TimeContextV1 {
  execution: BoundTimeStreamV1
  decision: BoundTimeStreamV1
  session: Record<string, unknown>
  timestamp: Record<string, unknown>
}

export interface MetricsAnnualizationV1 {
  schema_version: 'metrics_annualization.v1'
  basis: 'session_close_projection'
  projection_policy: 'last_accepted_equity_per_session'
  periods_per_year: number
  risk_free_rate_annual: number
}

export interface MetricsOverviewPayloadV1 {
  schema_version: '1.28'
  contract_id: 'lo2cin4bt-app-metrics-overview-payload-v1'
  projection_source: 'validated_json_contracts'
  run_id: string
  result_type?: 'portfolio'
  strategy_summary: {
    strategy_id?: string | null
    display_label?: string | null
    symbol?: string | null
    execution_stream_id?: string | null
    execution_bar_spec?: BarSpecV1 | null
    decision_stream_id?: string | null
    decision_bar_spec?: BarSpecV1 | null
    time_context?: TimeContextV1
    annualization?: MetricsAnnualizationV1
    frequency_label?: string | null
    entry_rule?: string | null
    exit_rule?: string | null
    parameter_domain_label?: string | null
    benchmark_label?: string | null
    logic_steps?: Array<{ kind: string; label: string; detail: string }>
    [key: string]: unknown
  }
  time_context: TimeContextV1
  annualization: MetricsAnnualizationV1
  default_category: string
  available_categories: Array<{ id: string; label: string }>
  rows: MetricsOverviewRowV1[]
  series: Array<{ backtest_id: string; label: string; x: string[]; y: number[] }>
  benchmark_series: { series_id: string; label: string; x: string[]; y: number[] } | null
  categories: Record<string, string[]>
  generated_at: string
  source_hashes: string[]
  artifact_source_refs: string[]
}
