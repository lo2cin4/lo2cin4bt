import type { ChangeEvent } from 'react'
import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import { useNavigate, useRouterState } from '../routing'

import { api } from '../api'
import { makeChartLayout, plotConfig } from '../chartTheme'
import { useCopy } from '../i18n'
import { InfoHint } from '../components/InfoHint'
import { Plot, preloadPlotly } from '../components/LazyPlot'
import { MissingState } from '../components/MissingState'
import { SectionCard } from '../components/SectionCard'
import { useAppStore } from '../store'
import {
  noParameterReasonLabel as controlledNoParameterReasonLabel,
  operationErrorLabel,
  parameterAxisLabel,
  parameterLabel,
  reviewReasonLabel as controlledReviewReasonLabel,
  uiText,
} from '../uiVocabulary'

type HeatmapRow = { backtest_id: string; label: string; params: Record<string, unknown>; sharpe?: number; total_return?: number; cagr?: number; calmar?: number; max_drawdown?: number; profit_factor?: number | null; win_rate?: number; robust_score?: number; trade_count?: number; rebalance_count?: number | null; risk_gate_event_count?: number | null; exposure_time?: number; final_equity?: number | null; excess_return?: number | null; local_plateau_score?: number; strategy_id?: string; strategy_display_label?: string | null; date_range_start?: string; date_range_end?: string }
type ShortlistRow = { select_default?: boolean; suggested_for_wfa?: boolean; rank: number; representative_type: string; label: string; params: Record<string, unknown>; source: string; robust_score?: number | null; mean_oos_sharpe?: number | null; oos_is_ratio?: number | null; stability_score?: number | null; cluster_id?: number | null; cluster_size?: number | null; local_plateau_score?: number | null; total_return?: number | null; cagr?: number | null; calmar?: number | null; trade_count?: number | null; rebalance_count?: number | null; risk_gate_event_count?: number | null; exposure_time?: number | null; final_equity?: number | null; excess_return?: number | null; max_drawdown?: number | null; profit_factor?: number | null; win_rate?: number | null; acceptance?: string; reason?: string; backtest_id: string; candidate_key?: string }
type ParameterImportanceRow = { parameter: string; importance: number; unique_values: number }
type ClusterSummaryRow = { cluster_id: number; size: number; representative_params: Record<string, unknown>; representative_combo_label?: string; mean_oos_sharpe?: number | null; stability_std?: number | null }
type StudySummary = { sampler: string; mode: string; objective: string; n_trials: number; n_startup_trials: number; completed_trials: number; pruned_trials: number; best_robust_score?: number | null; accepted_candidate_count?: number | null; cluster_count?: number | null; warnings?: string[] }
type AcceptanceConfig = { min_oos_is_ratio?: number | null; max_drawdown_floor?: number | null; min_profit_factor?: number | null; min_win_rate?: number | null; min_trade_count?: number | null }
type RankingConfig = { profile?: string; weights?: Record<string, number | null>; sort_priority?: string[] }
type RobustSelectionConfig = { enabled?: boolean; cluster_method?: string | null; top_n_candidates?: number | null; pick?: string | null }
type FutureLiveSearchConfig = { label?: string; source_filename?: string; config_path?: string; mode?: string | null; sampler?: string | null; n_trials?: number | null; n_startup_trials?: number | null; multivariate?: boolean | null; timeout_seconds?: number | null; note?: string | null; ranking?: RankingConfig; acceptance?: AcceptanceConfig; robust_selection?: RobustSelectionConfig }
type ParameterReviewTemplate = { name: string; acceptance?: AcceptanceConfig; ranking?: RankingConfig; updated_at?: string; is_default?: boolean }
type ParameterReviewTemplatePayload = { schema_version?: string; default_template_name?: string; templates?: ParameterReviewTemplate[] }
type HeatmapPayload = { rows: HeatmapRow[]; shortlist_rows: ShortlistRow[]; cluster_summary: ClusterSummaryRow[]; parameter_importance: ParameterImportanceRow[]; study_summary: StudySummary; objectives: string[]; param_axes: string[]; default_x_axis: string; default_y_axis: string; aggregation_modes: string[]; reduction_modes: string[]; axis_values: Record<string, Array<string | number>>; search_source_options: Array<{ id: string; label: string }>; default_search_source: string; ml_search_status: string; selected_representative_mode: string; availability?: string; reason?: string; source_row_count?: number; result_type?: string; artifact_type?: string; plateau_summary?: { top_cells?: Array<Record<string, unknown>> }; future_live_search_config?: FutureLiveSearchConfig; ranking_config?: RankingConfig; pre_review_acceptance_config?: AcceptanceConfig; strategy_summary?: { asset_label?: string; display_label?: string } }
type SelectedHeatmapCell = { x: string; y: string }

const formatLabel = (value: string) => value.replace(/_/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase())
const displayLabel = (value: string | undefined | null, language: string = 'zh-Hant') => {
  const raw = String(value || '').trim()
  const normalized = raw.toLowerCase().replace(/[\s-]+/g, '_')
  const zhLabels: Record<string, string> = {
    completed: '已完成', running: '執行中', failed: '失敗', pending: '等待中',
    single_objective: '單一目標', multi_objective: '多目標', balanced: '平衡',
    stability_first: '優先穩定度', performance_first: '優先表現', drawdown_aware: '回撤敏感',
    median: '中位數', mean: '平均值', best: '最佳值', worst: '最差值', std: '標準差',
    fixed: '手動固定', top_n_median: '高排名中位數', cluster_median: '叢集中位數',
    robust_score: '穩健分數', total_return: '總報酬', cagr: '年化報酬', max_drawdown: '最大回撤',
    excess_return: '超額報酬', win_rate: '勝率', exposure_time: '曝險時間', profit_factor: '獲利因子',
    trade_count: '交易數', rebalance_count: '再平衡數', local_plateau_score: '平台區分數',
    mean_oos_sharpe: '平均 OOS 夏普比率', oos_is_ratio: 'OOS / IS 比率', sharpe: '夏普比率', calmar: '卡瑪比率',
    all_existing_results: '全部已完成結果', accepted_candidates: '已通過候選組合',
    optuna_suggested_candidates: '模型建議候選組合', table_only_single_axis: '單軸表格檢視',
    single_axis_table_only: '單軸表格檢視', single_axis_table: '單軸表格檢視',
    single_axis_parameter_review: '單軸參數檢視', post_run_review: '回測後檢視', not_applicable: '不適用',
  }
  const enLabels: Record<string, string> = {
    completed: 'Completed', running: 'Running', failed: 'Failed', pending: 'Pending',
    single_objective: 'Single Objective', multi_objective: 'Multi Objective', balanced: 'Balanced',
    stability_first: 'Stability First', performance_first: 'Performance First', drawdown_aware: 'Drawdown Aware',
    median: 'Median', mean: 'Mean', best: 'Best', worst: 'Worst', std: 'Standard Deviation',
    fixed: 'Manual Fixed', top_n_median: 'Top-N Median', cluster_median: 'Cluster Median',
    robust_score: 'Robust Score', total_return: 'Total Return', cagr: 'CAGR', max_drawdown: 'Max Drawdown',
    excess_return: 'Excess Return', win_rate: 'Win Rate', exposure_time: 'Exposure Time', profit_factor: 'Profit Factor',
    trade_count: 'Trade Count', rebalance_count: 'Rebalance Count', local_plateau_score: 'Plateau Score',
    mean_oos_sharpe: 'Mean OOS Sharpe', oos_is_ratio: 'OOS / IS Ratio', sharpe: 'Sharpe', calmar: 'Calmar',
    all_existing_results: 'All Completed Results', accepted_candidates: 'Accepted Candidates',
    optuna_suggested_candidates: 'Suggested Candidates', table_only_single_axis: 'Single-Axis Table View',
    single_axis_table_only: 'Single-Axis Table View', single_axis_table: 'Single-Axis Table View',
    single_axis_parameter_review: 'Single-Axis Parameter Review', post_run_review: 'Post-Run Review',
    not_applicable: 'Not applicable',
  }
  const labels = language === 'zh-Hant' ? zhLabels : enLabels
  return labels[normalized] || parameterLabel(raw, language as any)
}
const asNumber = (value: unknown): number | null => { if (typeof value === 'number' && Number.isFinite(value)) return value; if (typeof value === 'string' && value.trim()) { const parsed = Number(value); return Number.isFinite(parsed) ? parsed : null } return null }
const formatMetric = (value: unknown, digits = 3) => { const numeric = asNumber(value); if (numeric === null) return '-'; if (!Number.isFinite(numeric)) return 'inf'; return numeric.toFixed(digits) }
const formatInteger = (value: unknown) => { const numeric = asNumber(value); if (numeric === null) return '-'; return String(Math.round(numeric)) }
const formatPercentMetric = (value: unknown, digits = 1) => { const numeric = asNumber(value); if (numeric === null) return '-'; return `${(numeric * 100).toFixed(digits)}%` }
const PERCENT_OBJECTIVES = new Set(['total_return', 'cagr', 'max_drawdown', 'excess_return', 'win_rate', 'exposure_time'])
const asNullableNumber = (value: string) => {
  const trimmed = value.trim()
  if (!trimmed) return null
  const parsed = Number(trimmed)
  return Number.isFinite(parsed) ? parsed : null
}
const formatInputNumber = (value: number | null | undefined, digits = 2) => {
  const numeric = asNumber(value)
  if (numeric === null) return ''
  const rounded = Number(numeric.toFixed(digits))
  return Number.isInteger(rounded) ? String(rounded) : String(rounded)
}
const formatPercentInput = (value: number | null | undefined) => {
  const numeric = asNumber(value)
  if (numeric === null) return ''
  return formatInputNumber(numeric * 100, 2)
}
const formatDrawdownPercentInput = (value: number | null | undefined) => {
  const numeric = asNumber(value)
  if (numeric === null) return ''
  return formatInputNumber(Math.abs(numeric) * 100, 2)
}
const parsePercentToRatio = (value: string) => {
  const parsed = asNullableNumber(value)
  return parsed === null ? null : parsed / 100
}
const parseDrawdownPercentToFloor = (value: string) => {
  const parsed = asNullableNumber(value)
  return parsed === null ? null : -Math.abs(parsed / 100)
}

const formatParams = (params: Record<string, unknown> | undefined) => !params || !Object.keys(params).length ? '-' : Object.entries(params).map(([key, value]) => `${key}=${String(value)}`).join(' | ')
const mostFrequentValue = (values: unknown[]) => {
  const counts = new Map<string, { raw: unknown; count: number }>()
  values.forEach((value) => {
    if (value === null || value === undefined || value === '') return
    const key = String(value)
    const existing = counts.get(key)
    if (existing) existing.count += 1
    else counts.set(key, { raw: value, count: 1 })
  })
  return [...counts.values()].sort((left, right) => right.count - left.count)[0]?.raw
}
const medianValue = (values: unknown[]) => {
  const numeric = values.map((value) => asNumber(value)).filter((value): value is number => value !== null).sort((left, right) => left - right)
  if (numeric.length) {
    const mid = Math.floor(numeric.length / 2)
    return numeric.length % 2 ? numeric[mid] : numeric[mid - 1]
  }
  return mostFrequentValue(values)
}
const aggregate = (values: number[], mode: string) => { if (!values.length) return null; const sorted = [...values].sort((left, right) => left - right); if (mode === 'mean') return sorted.reduce((sum, value) => sum + value, 0) / sorted.length; if (mode === 'best') return sorted[sorted.length - 1]; if (mode === 'worst') return sorted[0]; if (mode === 'std') { const mean = sorted.reduce((sum, value) => sum + value, 0) / sorted.length; return Math.sqrt(sorted.reduce((sum, value) => sum + (value - mean) ** 2, 0) / sorted.length) } if (mode === 'median') { const mid = Math.floor(sorted.length / 2); return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2 } return sorted.reduce((sum, value) => sum + value, 0) / sorted.length }
const shortlistKey = (row: ShortlistRow) => `${row.representative_type}:${row.backtest_id}`
const displayRepresentativeType = (value: string, language = 'zh-Hant') => {
  const normalized = String(value || '').trim().toLowerCase()
  if (normalized === 'top trial') return language === 'zh-Hant' ? '最高排名' : 'Top Ranked'
  if (normalized === 'cluster median') return language === 'zh-Hant' ? '叢集中位' : 'Cluster Median'
  if (normalized === 'plateau center') return language === 'zh-Hant' ? '最佳平台區' : 'Best Plateau'
  if (normalized === 'plateau edge') return language === 'zh-Hant' ? '平台邊緣' : 'Plateau Edge'
  return displayLabel(String(value || (language === 'zh-Hant' ? '候選組合' : 'candidate')), language)
}
const displaySourceLabel = (value: string, derivedFromExistingResults: boolean, language = 'zh-Hant') => {
  const normalized = String(value || '').trim().toLowerCase()
  if (normalized === 'optuna') return derivedFromExistingResults ? (language === 'zh-Hant' ? '排名' : 'Ranking') : (language === 'zh-Hant' ? '搜尋' : 'Search')
  if (normalized === 'cluster') return language === 'zh-Hant' ? '叢集分析' : 'Clustering'
  if (normalized === 'heatmap plateau') return language === 'zh-Hant' ? '平台區分析' : 'Plateau Analysis'
  return displayLabel(String(value || 'analysis'), language)
}
const representativeMarkerTone = (value: string) => {
  const normalized = value.toLowerCase()
  if (normalized.includes('top ranked')) return 'marker-top-trial'
  if (normalized.includes('cluster median')) return 'marker-cluster-median'
  if (normalized.includes('cluster top-ranked')) return 'marker-cluster-center'
  if (normalized.includes('top plateau')) return 'marker-plateau-center'
  if (normalized.includes('next plateau')) return 'marker-plateau-edge'
  return 'marker-generic'
}
const formatAnalystNote = (value: string | undefined, language = 'zh-Hant') => {
  if (!value) return language === 'zh-Hant' ? '分析備註：尚未記錄檢視備註。' : 'Analyst note: no review note was recorded.'
  const normalized = value.replace(/_/g, ' ').trim()
  if (normalized === 'meets acceptance gates') return language === 'zh-Hant' ? '分析備註：已通過目前接受門檻，可以進入前向分析 (WFA) 檢視。' : 'Analyst note: this candidate meets the current acceptance gates and is ready for WFA review.'
  if (normalized === 'review borderline metrics') return language === 'zh-Hant' ? '分析備註：表現值得留意，但仍需要人工檢視。' : 'Analyst note: metrics are borderline and need manual review.'
  if (normalized === 'below threshold') return language === 'zh-Hant' ? '分析備註：未達目前接受門檻，暫時不應優先處理。' : 'Analyst note: this candidate is below the current acceptance threshold.'
  return controlledReviewReasonLabel(value, language as any)
}
const searchSourceCopy = (value: string | undefined, language = 'zh-Hant') => {
  const normalized = String(value || '').toLowerCase()
  if (normalized === 'optuna_suggested_candidates') return language === 'zh-Hant' ? '只顯示模型建議的候選組合。' : 'Shows only model-suggested candidates.'
  if (normalized === 'accepted_candidates') return language === 'zh-Hant' ? '只顯示目前通過接受門檻的候選組合。' : 'Shows only candidates that pass the current acceptance gates.'
  return language === 'zh-Hant' ? '顯示完整結果集；需要最完整的參數背景時使用。' : 'Shows the full result set when you need the broadest parameter context.'
}
const describeReductionMode = (mode: string, language = 'zh-Hant') => {
  const normalized = String(mode || '').toLowerCase()
  if (normalized === 'fixed') return language === 'zh-Hant' ? '由你手動指定非 X/Y 軸參數。' : 'Manually choose non-X/Y parameters.'
  if (normalized === 'top_n_median') return language === 'zh-Hant' ? '其餘參數使用高排名列的中位數。' : 'Other parameters use the median of top-ranked rows.'
  if (normalized === 'cluster_median') return language === 'zh-Hant' ? '其餘參數使用叢集中位候選列的中位數。' : 'Other parameters use the cluster-median candidate row.'
  return language === 'zh-Hant' ? '投影成二維熱圖前，自動固定其餘參數。' : 'Automatically fixes other parameters before projecting to a two-dimensional heatmap.'
}
const formatGateChip = (label: string, value: unknown, language = 'zh-Hant') =>
  value === null || value === undefined || value === ''
    ? `${label}: ${uiText(language as any, 'disabled_by_config')}`
    : `${label}: ${typeof value === 'number' ? formatMetric(value, 3) : String(value)}`
const buildSelectionRationale = (row: ShortlistRow, derivedFromExistingResults: boolean, language = 'zh-Hant') => {
  const normalized = String(row.representative_type || '').trim().toLowerCase()
  const sourceLabel = displaySourceLabel(row.source, derivedFromExistingResults, language)
  if (normalized === 'top trial') return language === 'zh-Hant' ? '在目前檢視池中，經穩定度與平台區加權後的綜合分數最高。' : 'This is the highest composite score in the current review pool after stability, plateau, and drawdown adjustments.'
  if (normalized === 'cluster median') return language === 'zh-Hant' ? `第 ${row.cluster_id ?? '-'} 個叢集的典型代表，附近有 ${row.cluster_size ?? '-'} 個相似候選組合。` : `Representative candidate for cluster ${row.cluster_id ?? '-'}, with ${row.cluster_size ?? '-'} nearby similar candidates.`
  if (normalized === 'plateau center') return language === 'zh-Hant' ? '目前候選清單中平台區分數最高，用來代表最穩定的局部區域。' : 'This candidate has the strongest plateau score and represents the most stable local region.'
  if (normalized === 'plateau edge') return language === 'zh-Hant' ? '保留平台區邊緣候選組合，用來檢查穩定區域邊界是否仍有表現。' : 'This plateau-edge candidate is kept to test whether performance still holds near the stable-region boundary.'
  if (row.reason) return formatAnalystNote(row.reason, language)
  return language === 'zh-Hant' ? `由${sourceLabel}選入目前前向分析 (WFA) 檢視池。` : `Selected into the current WFA review pool by ${sourceLabel}.`
}
const extractAssetLabelFromRunLabel = (value: unknown) => {
  const raw = String(value || '').trim()
  if (!raw) return ''
  const parts = raw.split('|').map((part) => part.trim()).filter(Boolean)
  const ignoredTokens = new Set([
    'BACKTEST',
    '回測',
    'WALK-FORWARD',
    'WALK FORWARD',
    '前向分析',
    '前向分析 (WFA)',
    'FACTOR ANALYSIS',
    '因子分析',
    'PREDICTOR ANALYSIS',
    'PRICE',
    '價格',
    'CALENDAR',
    '日曆',
    'FACTOR',
    '因子',
    'PARAMETER MATRIX',
    '參數矩陣',
    'SINGLE BACKTEST',
    '單次回測',
    'PORTFOLIO',
    '投資組合',
    'SUMMARY',
    '摘要',
  ])
  const isDateToken = (part: string) => /^20\d{2}[-_]\d{2}[-_]\d{2}$|^20\d{6}$/.test(part)
  const isAssetToken = (part: string) => /^[A-Z0-9]+(?:[-_/][A-Z0-9]+)*$/.test(part) && /[A-Z]/.test(part)
  for (const part of parts) {
    const upper = part.toUpperCase()
    if (isDateToken(part) || ignoredTokens.has(upper) || upper.startsWith('RUN ') || upper.startsWith('CFG ')) continue
    if (isAssetToken(upper)) return upper.replace(/\//g, '-')
  }
  const matches = raw.toUpperCase().match(/\b[A-Z0-9]{2,12}(?:[-_/][A-Z0-9]{2,12})*\b/g) || []
  for (const token of matches) {
    if (!ignoredTokens.has(token) && !isDateToken(token)) return token.replace(/\//g, '-')
  }
  return ''
}
const inferDatasetLabel = (rows: HeatmapRow[], language = 'zh-Hant') => {
  const row = rows[0]
  const assetLabel = extractAssetLabelFromRunLabel(row?.strategy_display_label)
    || extractAssetLabelFromRunLabel(row?.label)
    || extractAssetLabelFromRunLabel(row?.strategy_id)
  if (assetLabel) return assetLabel
  const strategyId = String(row?.strategy_id || '').trim()
  if (strategyId) {
    const firstToken = strategyId.split(/[_-]/)[0]
    if (firstToken) return firstToken.toUpperCase()
  }
  return language === 'zh-Hant' ? '目前回測' : 'Current Run'
}
const formatDateRangeLabel = (start: unknown, end: unknown, language = 'zh-Hant') => {
  const left = String(start || '').slice(0, 10)
  const right = String(end || '').slice(0, 10)
  if (left && right) return `${left} | ${right}`
  if (left) return left
  if (right) return right
  return language === 'zh-Hant' ? '日期範圍未提供' : 'Date range unavailable'
}
const isExistingResultsReview = (payload?: HeatmapPayload) =>
  !!payload?.study_summary?.warnings?.includes('study_summary_derived_from_existing_results')

function formatNoParameterReason(reason: string | undefined, language: string) {
  return controlledNoParameterReasonLabel(reason, language as any)
}

function clusterMetricHelp(row: ShortlistRow, language = 'zh-Hant') {
  if (row.cluster_id === null || row.cluster_id === undefined) {
    return language === 'zh-Hant'
      ? '此候選組合目前未歸入參數家族；只有進入叢集家族後才會顯示叢集編號與大小。'
      : 'This candidate is not assigned to a parameter family yet; cluster ID and size are shown only after it joins a cluster family.'
  }
  return language === 'zh-Hant'
    ? `叢集編號是目前檢視池中的參數家族標籤；叢集大小代表同一家族內相似候選組合數量。此處代表第 ${row.cluster_id} 個家族目前有 ${row.cluster_size ?? '-'} 個鄰近候選組合。`
    : `The cluster ID is the parameter-family label in the current review pool; cluster size is the number of similar candidates in that family. Family ${row.cluster_id} currently has ${row.cluster_size ?? '-'} nearby candidates.`
}

export function ParameterMatrixPage() {
  const navigate = useNavigate()
  const search = useRouterState({ select: (state) => state.location.search }) as Record<string, string | undefined>
  const selectedMetricsRunId = useAppStore((state) => state.selectedMetricsRunId)
  const setSelectedMetricsRunId = useAppStore((state) => state.setSelectedMetricsRunId)
  const searchSource = useAppStore((state) => state.parameterMatrixSearchSource)
  const setSearchSource = useAppStore((state) => state.setParameterMatrixSearchSource)
  const language = useAppStore((state) => state.language)
  const t = useCopy(language)
  const pm = (zh: string, en: string) => (language === 'zh-Hant' ? zh : en)
  const dl = (value: string | undefined | null) => displayLabel(value, language)
  const axisLabel = (value: string | undefined | null) => parameterAxisLabel(value, language)
  const reportReviewError = (code: string, error: unknown) => {
    console.debug('[ParameterMatrix]', code, error)
    setReviewRulesFeedback(operationErrorLabel(code, language))
  }
  const runsQuery = useQuery({ queryKey: ['metrics-runs'], queryFn: api.metricsRuns, staleTime: 60000 })
  const runId = search.runId || selectedMetricsRunId || runsQuery.data?.[0]?.run_id || ''
  const availableRunIds = (runsQuery.data || []).map((run: any) => run.run_id)
  const hasResolvedRun = Boolean(runId && availableRunIds.includes(String(runId)))
  const highlightedBacktestId = search.backtestId || ''

  const [objective, setObjective] = useState('robust_score')
  const [xAxis, setXAxis] = useState('')
  const [yAxis, setYAxis] = useState('')
  const [aggregation, setAggregation] = useState('median')
  const [reductionMode, setReductionMode] = useState('cluster_median')
  const [viewMode, setViewMode] = useState<'heatmap' | 'contour'>('heatmap')
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(10)
  const [fixedParams, setFixedParams] = useState<Record<string, string>>({})
  const [selectedHeatmapCell, setSelectedHeatmapCell] = useState<SelectedHeatmapCell | null>(null)
  const [selectedCellBacktestId, setSelectedCellBacktestId] = useState('')
  const [reviewRulesFeedback, setReviewRulesFeedback] = useState('')
  const [previewPayload, setPreviewPayload] = useState<HeatmapPayload | null>(null)
  const [templateName, setTemplateName] = useState('')
  const [selectedTemplateName, setSelectedTemplateName] = useState('')
  const [gateDraft, setGateDraft] = useState<AcceptanceConfig>({})
  const [rankingDraft, setRankingDraft] = useState<RankingConfig>({})

  useEffect(() => {
    preloadPlotly()
  }, [])

  useEffect(() => { if (runId && runId !== selectedMetricsRunId) setSelectedMetricsRunId(runId) }, [runId, selectedMetricsRunId, setSelectedMetricsRunId])

  const query = useQuery({ queryKey: ['parameter-heatmap', runId], queryFn: () => api.parameterMatrix(runId), enabled: hasResolvedRun, staleTime: 60000 })
  const serverPayload = query.data as HeatmapPayload | undefined
  const payload = previewPayload || serverPayload
  const isTableOnlyParameterReview = payload?.availability === 'table_only_single_axis'
  const templatesQuery = useQuery({
    queryKey: ['parameter-review-templates'],
    queryFn: api.listParameterReviewTemplates,
    staleTime: 10000,
  })
  const previewMutation = useMutation({
    mutationFn: (overrides?: { acceptance?: AcceptanceConfig; ranking?: RankingConfig }) =>
      api.parameterMatrixReviewPreview(runId, {
        acceptance: overrides?.acceptance || gateDraft,
        ranking: overrides?.ranking || rankingDraft,
    }),
    onSuccess: (result) => {
      setPreviewPayload(result as HeatmapPayload)
      setReviewRulesFeedback(pm('已把進階檢視規則套用到目前參數研究預覽。', 'Advanced review rules were applied to the current parameter research preview.'))
    },
    onError: (error: Error) => reportReviewError('parameter_review_preview_failed', error),
  })
  const saveTemplateMutation = useMutation({
    mutationFn: () =>
      api.saveParameterReviewTemplate({
        name: templateName.trim(),
        acceptance: gateDraft,
        ranking: rankingDraft,
      }),
    onSuccess: () => {
      templatesQuery.refetch()
      setSelectedTemplateName(templateName.trim())
      setReviewRulesFeedback(pm(`已儲存檢視範本「${templateName.trim()}」。`, `Saved review template "${templateName.trim()}".`))
    },
    onError: (error: Error) => reportReviewError('parameter_template_save_failed', error),
  })
  const deleteTemplateMutation = useMutation({
    mutationFn: (name: string) => api.deleteParameterReviewTemplate(name),
    onSuccess: (_result, name) => {
      templatesQuery.refetch()
      setSelectedTemplateName('')
      setTemplateName('')
      setReviewRulesFeedback(pm(`已刪除檢視範本「${name}」。`, `Deleted review template "${name}".`))
    },
    onError: (error: Error) => reportReviewError('parameter_template_delete_failed', error),
  })
  const setDefaultTemplateMutation = useMutation({
    mutationFn: (name: string) => api.setDefaultParameterReviewTemplate(name),
    onSuccess: (_result, name) => {
      templatesQuery.refetch()
      query.refetch()
      setReviewRulesFeedback(pm(`已將「${name}」設為新參數研究的預設檢視範本。`, `Set "${name}" as the default parameter research review template.`))
    },
    onError: (error: Error) => reportReviewError('parameter_template_default_failed', error),
  })

  useEffect(() => {
    if (!serverPayload) return
    setPreviewPayload(null)
    setSelectedHeatmapCell(null)
    setSelectedCellBacktestId('')
    setObjective((current) => (serverPayload.objectives.includes(current) ? current : serverPayload.study_summary?.objective || serverPayload.objectives[0] || 'robust_score'))
    setXAxis((current) => {
      if (serverPayload.param_axes.includes(current)) return current
      return serverPayload.default_x_axis || serverPayload.param_axes[0] || ''
    })
    setYAxis((current) => {
      const candidate = serverPayload.param_axes.includes(current) ? current : (serverPayload.default_y_axis || serverPayload.param_axes[1] || serverPayload.param_axes[0] || '')
      if (candidate && candidate !== (serverPayload.default_x_axis || serverPayload.param_axes[0] || '')) return candidate
      return serverPayload.param_axes.find((axis) => axis !== (serverPayload.default_x_axis || serverPayload.param_axes[0] || '')) || candidate
    })
    const nextSearchSource = searchSource || serverPayload.default_search_source || 'all_existing_results'
    if (nextSearchSource !== searchSource) setSearchSource(nextSearchSource)
    setFixedParams((current) => Object.fromEntries(Object.entries(current).filter(([key]) => serverPayload.param_axes.includes(key))))
    setGateDraft(serverPayload.pre_review_acceptance_config || {})
    setRankingDraft(serverPayload.ranking_config || {})
    setPage((current) => (current === 1 ? current : 1))
  }, [serverPayload, searchSource, setSearchSource])

  useEffect(() => {
    if (!payload || !xAxis || !yAxis || xAxis !== yAxis) return
    const fallbackYAxis = payload.param_axes.find((axis) => axis !== xAxis) || yAxis
    if (fallbackYAxis !== yAxis) setYAxis(fallbackYAxis)
  }, [payload, xAxis, yAxis])

  const shortlistRows = payload?.shortlist_rows || []
  const derivedFromExistingResults = isExistingResultsReview(payload)
  const isPortfolioMatrix = payload?.result_type === 'portfolio' || payload?.artifact_type === 'multi_asset_portfolio_backtest'
  const actualClusterCount = payload?.cluster_summary?.length || 0
  const summaryTitle = derivedFromExistingResults
    ? pm('已完成參數測試', 'Completed Parameter Tests')
    : pm('即時自適應搜尋', 'Live Adaptive Search')
  const summaryCopy = derivedFromExistingResults
    ? pm('此頁會把已完成的參數組合回測壓縮成較小的候選池，用來判斷哪些組合值得再做前向分析 (WFA)。', 'This page compresses completed parameter-test results into a smaller review pool so you can decide which candidates deserve Walk-Forward Analysis (WFA).')
    : pm('此頁用來檢視模型建議的候選參數，確認是否值得進入前向分析 (WFA)。', 'This page reviews model-suggested parameter candidates before deciding whether they deserve Walk-Forward Analysis (WFA).')
  const modeLabel = derivedFromExistingResults
    ? pm('回測後排名', 'Post-Run Ranking')
    : dl(payload?.study_summary?.mode || 'single_objective')
  const searchMethodLabel = derivedFromExistingResults
    ? pm('不適用', 'Not applicable')
    : dl(payload?.study_summary?.sampler || 'tpe')
  const searchMethodHelp = derivedFromExistingResults
    ? pm(
      '目前沒有即時 optimizer 正在搜尋；這頁是把已完成回測結果重新評分、聚類與篩選。若使用 live Optuna / WFA optimizer，這裡才會顯示 TPE、NSGA-II 或 GP 等 sampler。',
      'No live optimizer is searching here; this page re-scores, clusters, and filters completed backtest results. TPE, NSGA-II, or GP only appear here when a live Optuna/WFA optimizer payload is used.',
    )
    : pm(
      '用來選擇潛力區域的搜尋方式；TPE 代表搜尋會偏向已經看起來優於平均的區域。',
      'The method used to search promising regions; TPE-style search favours regions that already look stronger than average.',
    )
  const completedTrialsLabel = derivedFromExistingResults ? pm('已分析既有結果', 'Existing Results Analysed') : pm('已完成試驗', 'Completed Trials')
  const activeAcceptanceConfig = payload?.pre_review_acceptance_config
  const activeRankingConfig = payload?.ranking_config
  const warningText = derivedFromExistingResults
    ? pm('此摘要來自既有參數測試結果；候選清單是真實結果，但研究指標屬分析摘要，不是即時自適應搜尋。', 'This summary is derived from completed parameter tests. The candidate list is real, while the study metrics are analytical summaries rather than a live adaptive search.')
    : (payload?.study_summary?.warnings || []).map((item) => dl(item)).join(' | ')
  const acceptanceSummary = useMemo(() => shortlistRows.reduce((summary, row) => {
    const normalized = String(row.acceptance || 'review').toLowerCase()
    if (normalized === 'pass') summary.pass += 1
    else if (normalized === 'fail') summary.fail += 1
    else summary.review += 1
    return summary
  }, { pass: 0, review: 0, fail: 0 }), [shortlistRows])
  const distinctAcceptanceCount = [acceptanceSummary.pass, acceptanceSummary.review, acceptanceSummary.fail].filter((count) => count > 0).length
  const totalCombinations = payload?.rows?.length || 0
  const datasetLabel = String(payload?.strategy_summary?.asset_label || '').trim()
    || inferDatasetLabel(payload?.rows || [], language)
  const scopeDateRange = formatDateRangeLabel(payload?.rows?.[0]?.date_range_start, payload?.rows?.[0]?.date_range_end, language)
  const freeParamCount = payload?.param_axes?.length || 0
  const plateauTopCount = payload?.plateau_summary?.top_cells?.length || 0
  const searchBasisLabel = derivedFromExistingResults ? pm('不適用', 'Not applicable') : pm('即時自適應搜尋', 'Live Adaptive Search')
  const searchBasisHelp = derivedFromExistingResults
    ? pm(
      '搜尋依據只適用於即時自適應搜尋。這頁正在檢視已完成的參數測試結果，所以沒有 live search basis；候選組合由回測後排名、穩定區域與聚類代表產生。',
      'Search basis only applies to live adaptive search. This page is reviewing completed parameter-test results, so there is no live search basis; candidates come from post-run ranking, stable regions, and cluster representatives.',
    )
    : pm(
      '顯示候選組合是否由即時 Optuna / 自適應搜尋產生，而不是由已完成的參數測試結果回推。',
      'Shows whether candidates are being generated by live Optuna/adaptive search rather than derived from completed parameter-test results.',
    )
  const activeGateChips = [
    formatGateChip(pm('最低 PF', 'Min PF'), activeAcceptanceConfig?.min_profit_factor, language),
    activeAcceptanceConfig?.min_win_rate === null || activeAcceptanceConfig?.min_win_rate === undefined
      ? `${pm('最低勝率', 'Min Win Rate')}: ${uiText(language as any, 'disabled_by_config')}`
      : `${pm('最低勝率', 'Min Win Rate')}: ${formatInputNumber((activeAcceptanceConfig.min_win_rate || 0) * 100, 1)}%`,
    formatGateChip(pm('最低交易數', 'Min Trades'), activeAcceptanceConfig?.min_trade_count, language),
    activeAcceptanceConfig?.max_drawdown_floor === null || activeAcceptanceConfig?.max_drawdown_floor === undefined
      ? `${pm('最大回撤下限', 'Max Drawdown Floor')}: ${uiText(language as any, 'disabled_by_config')}`
      : `${pm('最大回撤下限', 'Max Drawdown Floor')}: ${formatInputNumber(Math.abs(activeAcceptanceConfig.max_drawdown_floor || 0) * 100, 1)}%`,
  ]
  const snapshotChip = (label: string, value: string, help: string) => (
    <span className="snapshot-chip" key={label}>
      <InfoHint label={label} body={help} />
      <span className="snapshot-chip-text">{label} {value}</span>
    </span>
  )
  const renderSnapshotChips = (row: HeatmapRow | ShortlistRow) => {
    if (isPortfolioMatrix) {
      const riskGateEventCount = asNumber(row.risk_gate_event_count)
      return (
        <>
          {snapshotChip(pm('有效再平衡', 'Active Rebalances'), formatInteger(row.trade_count), pm('目標持倉或目標權重出現足夠變化，並實際產生投資組合調整的檢查日數。', 'Number of checkpoints where target holdings or target weights changed enough to trigger a portfolio adjustment.'))}
          {snapshotChip(pm('檢查點', 'Checkpoints'), formatInteger(row.rebalance_count), pm('策略檢查是否需要再平衡的排程日總數；包含沒有變動的檢查日。', 'Total scheduled checkpoints where the strategy checked whether a rebalance was needed, including no-change checks.'))}
          {riskGateEventCount && riskGateEventCount > 0
            ? snapshotChip(pm('風控門檻', 'Risk Gates'), formatInteger(riskGateEventCount), pm('此投資組合候選組合中記錄到的風控介入次數；解讀主要報酬或回撤前應先檢視。', 'Recorded risk-gate interventions for this portfolio candidate; review these before reading headline return or drawdown.'))
            : null}
          {snapshotChip(dl('cagr'), formatPercentMetric(row.cagr), pm('此投資組合候選組合在回測期間的年化成長率。', 'Annualized growth rate for this portfolio candidate over the backtest period.'))}
          {snapshotChip(dl('calmar'), formatMetric(row.calmar), pm('年化報酬除以最大回撤絕對值；越高代表每承受一單位回撤換來的報酬越好。', 'CAGR divided by absolute max drawdown; higher means better return per unit of drawdown.'))}
          {snapshotChip(dl('max_drawdown'), formatPercentMetric(row.max_drawdown), pm('回測期間資金曲線從高點到低點的最大跌幅。', 'Largest peak-to-trough equity decline during the backtest.'))}
          {snapshotChip(pm('曝險', 'Exposure'), formatPercentMetric(row.exposure_time), pm('投資組合持有非現金曝險的交易日比例。', 'Share of trading days where the portfolio carried non-cash exposure.'))}
        </>
      )
    }
    return (
      <>
        {snapshotChip(pm('交易數', 'Trades'), String(row.trade_count ?? '-'), pm('此單一策略候選組合記錄到的已平倉交易數。', 'Closed trade count recorded for this single-strategy candidate.'))}
        {snapshotChip(dl('profit_factor'), formatMetric(row.profit_factor), pm('獲利因子：已平倉交易的總盈利除以總虧損。', 'Profit Factor: gross profit divided by gross loss across closed trades.'))}
        {snapshotChip(pm('勝率', 'Win Rate'), formatMetric(row.win_rate), pm('已平倉交易中的獲利比例。', 'Share of closed trades that were profitable.'))}
        {snapshotChip(dl('max_drawdown'), formatPercentMetric(row.max_drawdown), pm('回測期間資金曲線從高點到低點的最大跌幅。', 'Largest peak-to-trough equity decline during the backtest.'))}
      </>
    )
  }
  const snapshotHelp = isPortfolioMatrix
    ? pm('投資組合候選組合若沒有專門的已平倉交易紀錄，就不會有單筆交易的獲利因子或勝率；此快照會改用再平衡、年化報酬、卡瑪比率、回撤與曝險。', 'Portfolio candidates may not have a closed-trade log, so this snapshot uses rebalances, CAGR, Calmar, drawdown, and exposure instead.')
    : pm('此候選組合的交易數、獲利因子、勝率與回撤。', 'Trade count, Profit Factor, win rate, and drawdown for this candidate.')
  const rankingWeightChips = [
    formatGateChip(dl('sharpe'), activeRankingConfig?.weights?.sharpe_weight, language),
    formatGateChip(pm('平台區', 'Plateau'), activeRankingConfig?.weights?.plateau_weight, language),
    formatGateChip(pm('回撤懲罰', 'Drawdown Penalty'), activeRankingConfig?.weights?.drawdown_penalty_weight, language),
  ]
  const paramAxisLabel = (payload?.param_axes || []).map((axis) => axisLabel(axis)).join(', ') || '-'
  const scopeDetail = [
    `${datasetLabel} | ${scopeDateRange}`,
    pm(`${freeParamCount} 個自由參數：${paramAxisLabel}`, `${freeParamCount} free parameter(s): ${paramAxisLabel}`),
    pm(`此研究回測共有 ${formatInteger(totalCombinations)} 個組合`, `${formatInteger(totalCombinations)} combinations in this research run`),
  ].join('\n')
  const compressionLine = `${formatInteger(totalCombinations)} -> ${formatInteger(shortlistRows.length)}`
  const outcomeDetail = [
    pm(`${formatInteger(totalCombinations)} 個已測試 -> ${formatInteger(shortlistRows.length)} 個進入候選短名單`, `${formatInteger(totalCombinations)} tested -> ${formatInteger(shortlistRows.length)} shortlisted`),
    pm(`辨識到 ${formatInteger(actualClusterCount)} 個參數家族`, `${formatInteger(actualClusterCount)} parameter families identified`),
    pm(`目前熱圖找到 ${formatInteger(plateauTopCount)} 個強平台區塊`, `${formatInteger(plateauTopCount)} strong plateau cells found`),
  ].join('\n')
  const showAcceptanceSignals = distinctAcceptanceCount > 1
  const readinessDetail = [
    pm(`候選短名單中有 ${formatInteger(shortlistRows.length)} 個參數組合`, `${formatInteger(shortlistRows.length)} candidates are in the shortlist`),
    pm('前向分析 (WFA) 會由執行中心使用策略參數範圍另外執行', 'Walk-Forward Analysis (WFA) runs separately from Run Center using the strategy parameter range'),
    showAcceptanceSignals
      ? pm(`${formatInteger(acceptanceSummary.review)} 個需要檢視 | 未達門檻`, `${formatInteger(acceptanceSummary.review)} need review | below threshold`)
      : pm('目前候選短名單已高於接受門檻', 'The current shortlist is above the acceptance bar'),
  ].join('\n')
  const readinessValue = showAcceptanceSignals
    ? pm(`${formatInteger(acceptanceSummary.pass)} 通過 / ${formatInteger(acceptanceSummary.review)} 需檢視`, `${formatInteger(acceptanceSummary.pass)} pass / ${formatInteger(acceptanceSummary.review)} needs review`)
    : pm('就緒', 'Ready')
  const searchSourceFilteredShortlist = useMemo(() => shortlistRows.filter((row) => { if (searchSource === 'accepted_candidates') return String(row.acceptance || '').toLowerCase() === 'pass'; if (searchSource === 'optuna_suggested_candidates') return ['optuna', 'cluster', 'heatmap plateau'].includes(String(row.source || '').toLowerCase()); return true }), [searchSource, shortlistRows])
  const shortlistViewRows = searchSourceFilteredShortlist
  const sourceBacktestIds = useMemo(() => searchSource === 'all_existing_results' ? null : new Set(searchSourceFilteredShortlist.map((row) => row.backtest_id)), [searchSource, searchSourceFilteredShortlist])
  const effectiveFixedParams = useMemo(() => {
    if (!payload) return {}
    const remainingAxes = (payload.param_axes || []).filter((axis) => axis !== xAxis && axis !== yAxis)
    if (reductionMode === 'fixed') return fixedParams
    const pool = reductionMode === 'top_n_median'
      ? (payload.rows || []).filter((row) => !sourceBacktestIds || sourceBacktestIds.has(row.backtest_id)).slice(0, Math.min(40, Math.max(shortlistViewRows.length, 10)))
      : shortlistViewRows.filter((row) => {
          const normalized = String(row.representative_type || '').toLowerCase()
          if (reductionMode === 'cluster_median') return normalized === 'cluster median'
          return false
        })
    const fallbackPool = shortlistViewRows.length ? shortlistViewRows : payload.rows || []
    const rowsForReduction = pool.length ? pool : fallbackPool
    return Object.fromEntries(remainingAxes.map((axis) => {
      const derived = medianValue(rowsForReduction.map((row: any) => row.params?.[axis]))
      return [axis, derived === null || derived === undefined ? '' : String(derived)]
    }))
  }, [fixedParams, payload, reductionMode, shortlistViewRows, sourceBacktestIds, xAxis, yAxis])
  const filteredRows = useMemo(() => (payload?.rows || []).filter((row) => {
    if (sourceBacktestIds && !sourceBacktestIds.has(row.backtest_id)) return false
    return Object.entries(effectiveFixedParams).every(([key, value]) => !value || key === xAxis || key === yAxis || String(row.params?.[key] ?? '') === value)
  }), [effectiveFixedParams, payload, sourceBacktestIds, xAxis, yAxis])
  const heatmapMatrix = useMemo(() => {
    if (!payload || !xAxis || !yAxis) return null
    const xValues = payload.axis_values?.[xAxis] || []
    const yValues = payload.axis_values?.[yAxis] || []
    const z = yValues.map((yValue) => xValues.map((xValue) => {
      const values = filteredRows.filter((row) => row.params?.[xAxis] === xValue && row.params?.[yAxis] === yValue).map((row) => asNumber(row[objective as keyof HeatmapRow])).filter((value): value is number => value !== null)
      return aggregate(values, aggregation)
    }))
    const counts = yValues.map((yValue) => xValues.map((xValue) => filteredRows.filter((row) => row.params?.[xAxis] === xValue && row.params?.[yAxis] === yValue).length))
    const plateau = yValues.map((yValue) => xValues.map((xValue) => {
      const values = filteredRows.filter((row) => row.params?.[xAxis] === xValue && row.params?.[yAxis] === yValue).map((row) => asNumber(row.local_plateau_score)).filter((value): value is number => value !== null)
      return aggregate(values, 'mean')
    }))
    return { xValues, yValues, z, counts, plateau }
  }, [aggregation, filteredRows, objective, payload, xAxis, yAxis])
  const selectedCellRows = useMemo(() => {
    if (!selectedHeatmapCell) return [] as HeatmapRow[]
    return filteredRows.filter((row) => String(row.params?.[xAxis] ?? '') === selectedHeatmapCell.x && String(row.params?.[yAxis] ?? '') === selectedHeatmapCell.y)
  }, [filteredRows, selectedHeatmapCell, xAxis, yAxis])
  useEffect(() => {
    if (!selectedCellRows.length) {
      setSelectedCellBacktestId('')
      return
    }
    if (selectedCellBacktestId && selectedCellRows.some((row) => row.backtest_id === selectedCellBacktestId)) return
    const bestRow = [...selectedCellRows].sort((left, right) => {
      const rightValue = asNumber(right[objective as keyof HeatmapRow]) ?? -Infinity
      const leftValue = asNumber(left[objective as keyof HeatmapRow]) ?? -Infinity
      return rightValue - leftValue
    })[0]
    setSelectedCellBacktestId(bestRow?.backtest_id || '')
  }, [objective, selectedCellBacktestId, selectedCellRows])
  const selectedCellRow = useMemo(
    () => selectedCellRows.find((row) => row.backtest_id === selectedCellBacktestId) || selectedCellRows[0] || null,
    [selectedCellBacktestId, selectedCellRows],
  )
  const topTrial = shortlistRows.find((row) => row.representative_type === 'Top Trial') || shortlistRows[0]
  const objectiveHoverValue = PERCENT_OBJECTIVES.has(objective) ? '%{z:.1%}' : '%{z:.3f}'
  const totalPages = Math.max(1, Math.ceil(shortlistViewRows.length / pageSize))
  const pageRows = shortlistViewRows.slice((page - 1) * pageSize, page * pageSize)
  useEffect(() => { if (page > totalPages) setPage(totalPages) }, [page, totalPages])
  const templatesPayload = (templatesQuery.data as ParameterReviewTemplatePayload | undefined)
  const templateItems = (templatesPayload?.templates || []) as ParameterReviewTemplate[]
  const defaultTemplateName = String(templatesPayload?.default_template_name || '')
  const selectedTemplate = templateItems.find((item) => item.name === selectedTemplateName)
  const applySelectedTemplate = () => {
    if (!selectedTemplate) return
    const nextAcceptance = selectedTemplate.acceptance || {}
    const nextRanking = selectedTemplate.ranking || {}
    setGateDraft(nextAcceptance)
    setRankingDraft(nextRanking)
    setTemplateName(selectedTemplate.name)
    previewMutation.mutate({ acceptance: nextAcceptance, ranking: nextRanking })
  }
  const resetAdvancedRules = () => {
    setPreviewPayload(null)
    setGateDraft(serverPayload?.pre_review_acceptance_config || {})
    setRankingDraft(serverPayload?.ranking_config || {})
    setTemplateName('')
    setSelectedTemplateName('')
    setReviewRulesFeedback(pm('已將進階檢視規則重設為本次回測的預設值。', 'Advanced review rules were reset to this run\'s defaults.'))
  }

  if (runsQuery.isLoading || (!hasResolvedRun && runsQuery.data?.length) || query.isLoading) return <div className="page-loading">{t('common.loading.parameterMatrix')}</div>
  if (!runId) return <MissingState message={t('parameterMatrix.selectMetricsFirst')} />
  if (query.error) return <MissingState message={t('parameterMatrix.noMatrix')} />
  if (!payload) return <div className="page-error">{pm('無法載入參數研究資料。', 'Unable to load parameter research data.')}</div>
  if (payload.availability === 'no_parameter_domain') {
    return <MissingState message={formatNoParameterReason(payload.reason, language)} />
  }
  if ((payload.param_axes || []).length < 2 && !isTableOnlyParameterReview) return <MissingState message={t('parameterMatrix.notEnoughParams')} />

  return (
    <div className="page-stack">
      <SectionCard title={t('parameterMatrix.title')} subtitle={t('parameterMatrix.subtitle')}>
        <div className="parameter-workspace-topbar">
          <div className="parameter-status-strip">
            <div className="research-status-pill"><span className="research-status-label">{pm('分析狀態', 'Analysis Status')}</span><strong>{dl(payload.ml_search_status || 'completed')}</strong></div>
            <div className="research-status-pill"><span className="research-status-label">{pm('候選池', 'Candidate Pool')}</span><strong>{shortlistRows.length}</strong></div>
            <div className="research-status-pill"><span className="research-status-label">{pm('叢集', 'Clusters')}</span><strong>{formatInteger(actualClusterCount)}</strong></div>
            <div className="research-status-pill"><span className="research-status-label">{pm('自由參數', 'Free Params')}</span><strong>{formatInteger(freeParamCount)}</strong></div>
          </div>
        </div>

        <div className="research-overview-grid">
          <div className="research-overview-hero">
            <div className="research-overview-eyebrow">{pm('決策摘要', 'Decision Summary')}</div>
            <div className="research-overview-title">{summaryTitle}</div>
            <div className="research-summary-grid">
              <div className="research-summary-block">
                <div className="research-summary-title">{pm('目前回測', 'Current Run')} <InfoHint label={pm('目前回測', 'Current Run')} body={`${summaryCopy}\n\n${scopeDetail}`} /></div>
                <div className="research-summary-kpi">{datasetLabel}</div>
                <div className="research-summary-meta">{pm(`${freeParamCount} 個參數 | ${formatInteger(totalCombinations)} 個組合`, `${freeParamCount} parameter(s) | ${formatInteger(totalCombinations)} combinations`)}</div>
              </div>
              <div className="research-summary-block">
                <div className="research-summary-title">{pm('結果', 'Result')} <InfoHint label={pm('研究結果', 'Research Result')} body={outcomeDetail} /></div>
                <div className="research-summary-kpi">{compressionLine}</div>
                <div className="research-summary-meta">{pm(`${formatInteger(actualClusterCount)} 個家族 | ${formatInteger(plateauTopCount)} 個平台區`, `${formatInteger(actualClusterCount)} families | ${formatInteger(plateauTopCount)} plateau cells`)}</div>
              </div>
              <div className="research-summary-block">
                <div className="research-summary-title">{pm('前向分析檢查', 'WFA Readiness')} <InfoHint label={pm('前向分析檢查', 'WFA Readiness')} body={readinessDetail} /></div>
                <div className="research-summary-kpi">{readinessValue}</div>
                <div className="research-summary-meta">{pm('需要獨立前向分析', 'Needs independent WFA')}</div>
              </div>
            </div>
            <div className="research-overview-highlight">
              <div className="research-highlight-label">{pm('最佳穩健分數', 'Best Robust Score')} <InfoHint label={pm('最佳穩健分數', 'Best Robust Score')} body={pm('結合原始表現、穩定度、平台區品質與回撤懲罰後，目前最高的綜合分數。', 'The highest composite score after combining raw performance, stability, plateau quality, and drawdown penalty.')} /></div>
              <div className="research-highlight-value">{formatMetric(payload.study_summary?.best_robust_score, 3)}</div>
            </div>
          </div>
          <div className="research-overview-metrics">
            <div className="research-metric-card"><div className="research-metric-label">{pm('搜尋依據', 'Search Basis')} <InfoHint side="left" label={pm('搜尋依據', 'Search Basis')} body={searchBasisHelp} /></div><div className="research-metric-value">{searchBasisLabel}</div></div>
            <div className="research-metric-card"><div className="research-metric-label">{pm('排名方法', 'Ranking Method')} <InfoHint side="left" label={pm('排名方法', 'Ranking Method')} body={pm('說明候選組合如何被篩選；這裡代表回測後的綜合排名，不是逐次試驗的即時搜尋。', 'Explains how candidates were filtered; here it means post-run composite ranking, not trial-by-trial live search.')} /></div><div className="research-metric-value">{modeLabel}</div></div>
            <div className="research-metric-card"><div className="research-metric-label">{pm('搜尋方法', 'Search Method')} <InfoHint side="left" label={pm('搜尋方法', 'Search Method')} body={searchMethodHelp} /></div><div className="research-metric-value">{searchMethodLabel}</div></div>
            {!derivedFromExistingResults ? (
              <div className="research-metric-card">
                <div className="research-metric-label">{pm('啟動試驗數', 'Startup Trials')} <InfoHint side="left" label={pm('啟動試驗數', 'Startup Trials')} body={pm('即時自適應搜尋在開始偏向潛力參數區域前，先進行的暖身樣本數。', 'Warm-up samples used before live adaptive search starts favouring promising parameter regions.')} /></div>
                <div className="research-metric-value">{formatInteger(payload.study_summary?.n_startup_trials || 0)}</div>
              </div>
            ) : null}
          </div>
        </div>

        {!!warningText && !derivedFromExistingResults ? <div className="research-warning-banner"><strong>{pm('警告', 'Warning')}</strong><span title={warningText}>{warningText}</span></div> : null}
      </SectionCard>

      {isTableOnlyParameterReview ? (
        <SectionCard title={t('parameterMatrix.rankedReview')} subtitle={pm('此回測只改變一個參數軸，因此以排名候選組合呈現，而不是二維熱圖。', 'This run only varies one parameter axis, so candidates are shown as a ranked table rather than a two-dimensional heatmap.')}>
          <div className="helper-text">
            {pm(`軸線：${paramAxisLabel}。請用下方候選卡比較表現、投資組合狀態與回測詳情。`, `Axis: ${paramAxisLabel}. Use the candidate cards below to compare performance, portfolio state, and backtest details.`)}
          </div>
        </SectionCard>
      ) : (
      <SectionCard captureId="parameter_matrix" title={t('parameterMatrix.heatmapDiagnostics')} subtitle={pm('檢查強勢候選組合是否位於穩定的局部區域，而不是孤立尖峰。', 'Check whether strong candidates sit inside stable local regions rather than isolated spikes.')}>
        <div className="research-heatmap-layout">
          <div className="research-heatmap-main">
            <div className="research-control-grid">
              <label className="research-control-card research-control-card-wide"><div className="research-control-label">{pm('搜尋來源', 'Search Source')}</div><select className="text-input" value={searchSource} onChange={(event) => setSearchSource(event.target.value)}>{(payload.search_source_options || []).map((option) => <option key={option.id} value={option.id}>{dl(option.id || option.label)}</option>)}</select><div className="research-control-helper">{searchSourceCopy(searchSource, language)}</div></label>
              <label className="research-control-card"><div className="research-control-label">{pm('目標指標', 'Objective')}</div><select className="text-input" value={objective} onChange={(event) => setObjective(event.target.value)}>{(payload.objectives || []).map((item) => <option key={item} value={item}>{dl(item)}</option>)}</select></label>
              <label className="research-control-card"><div className="research-control-label">X {pm('軸', 'Axis')}</div><select className="text-input" value={xAxis} onChange={(event) => setXAxis(event.target.value)}>{(payload.param_axes || []).map((axis) => <option key={axis} value={axis}>{axisLabel(axis)}</option>)}</select></label>
              <label className="research-control-card"><div className="research-control-label">Y {pm('軸', 'Axis')}</div><select className="text-input" value={yAxis} onChange={(event) => setYAxis(event.target.value)}>{(payload.param_axes || []).map((axis) => <option key={axis} value={axis}>{axisLabel(axis)}</option>)}</select></label>
              <label className="research-control-card"><div className="research-control-label">{pm('降維方式', 'Reduction')}</div><select className="text-input" value={reductionMode} onChange={(event) => setReductionMode(event.target.value)}>{(payload.reduction_modes || []).map((mode) => <option key={mode} value={mode}>{dl(mode)}</option>)}</select></label>
              <label className="research-control-card"><div className="research-control-label">{pm('聚合方式', 'Aggregation')}</div><select className="text-input" value={aggregation} onChange={(event) => setAggregation(event.target.value)}>{(payload.aggregation_modes || []).map((mode) => <option key={mode} value={mode}>{dl(mode)}</option>)}</select></label>
              <label className="research-control-card"><div className="research-control-label">{pm('顯示方式', 'View Mode')}</div><select className="text-input" value={viewMode} onChange={(event) => setViewMode(event.target.value as 'heatmap' | 'contour')}><option value="heatmap">{pm('熱圖', 'Heatmap')}</option><option value="contour">{pm('等高線', 'Contour')}</option></select></label>
            </div>
            <Plot
              data={[
                viewMode === 'contour'
                  ? { type: 'contour', x: heatmapMatrix?.xValues, y: heatmapMatrix?.yValues, z: heatmapMatrix?.z, customdata: heatmapMatrix?.plateau?.map((row, rowIndex) => row.map((plateauScore, colIndex) => [heatmapMatrix?.counts?.[rowIndex]?.[colIndex] ?? null, plateauScore])), colorscale: 'Viridis', contours: { coloring: 'heatmap' }, hovertemplate: `${axisLabel(xAxis)}=%{x}<br>${axisLabel(yAxis)}=%{y}<br>${dl(objective)}=${objectiveHoverValue}<br>${pm('樣本數', 'Samples')}=%{customdata[0]}<br>${pm('平台區分數', 'Plateau Score')}=%{customdata[1]:.3f}<extra></extra>` }
                  : { type: 'heatmap', x: heatmapMatrix?.xValues, y: heatmapMatrix?.yValues, z: heatmapMatrix?.z, customdata: heatmapMatrix?.plateau?.map((row, rowIndex) => row.map((plateauScore, colIndex) => [heatmapMatrix?.counts?.[rowIndex]?.[colIndex] ?? null, plateauScore])), colorscale: 'Viridis', hovertemplate: `${axisLabel(xAxis)}=%{x}<br>${axisLabel(yAxis)}=%{y}<br>${dl(objective)}=${objectiveHoverValue}<br>${pm('樣本數', 'Samples')}=%{customdata[0]}<br>${pm('平台區分數', 'Plateau Score')}=%{customdata[1]:.3f}<extra></extra>` },
              ]}
              layout={makeChartLayout({
                xTitle: axisLabel(xAxis),
                yTitle: axisLabel(yAxis),
                margin: { l: 72, r: 28, t: 30, b: 68 },
              })}
              config={plotConfig}
              className="plot-card"
              useResizeHandler
              style={{ width: '100%', height: '560px' }}
              onClick={(event: any) => {
                const point = event?.points?.[0]
                if (!point) return
                setSelectedHeatmapCell({ x: String(point.x), y: String(point.y) })
              }}
            />
          </div>

          <aside className="research-heatmap-sidebar">
            <div className="research-side-panel">
              <div className="research-side-title">{pm('其餘參數', 'Other Parameters')}</div>
            <div className="research-side-copy">
              {reductionMode === 'fixed'
                ? pm('固定模式讓你手動選擇所有非 X/Y 軸參數。若回測包含 A、B、C、D，而你畫 A 對 B，這裡就要手動指定 C 與 D。', 'Fixed mode lets you manually choose every non-X/Y parameter. If a run contains A, B, C, and D while you plot A vs B, choose C and D here.')
                : pm(`${dl(reductionMode)} 會自動固定本回測所有非 X/Y 軸參數。若回測包含 A、B、C、D，而你畫 A 對 B，系統會先自動選擇 C 與 D 再畫熱圖。`, `${dl(reductionMode)} automatically fixes every non-X/Y parameter before plotting the heatmap.`)}
            </div>
            <div className="research-metric-footnote">{describeReductionMode(reductionMode, language)}</div>
              <div className="research-filter-stack">
                {(payload.param_axes || []).filter((axis) => axis !== xAxis && axis !== yAxis).map((axis) => (
                  <label key={axis} className="research-filter-label">
                    <span>{axisLabel(axis)}</span>
                    <select className="text-input" value={reductionMode === 'fixed' ? (fixedParams[axis] || '') : (effectiveFixedParams[axis] || '')} onChange={(event) => { setFixedParams((current) => ({ ...current, [axis]: event.target.value })); setPage(1) }} disabled={reductionMode !== 'fixed'}>
                      <option value="">{pm('全部數值', 'All Values')}</option>
                      {(payload.axis_values?.[axis] || []).map((value) => <option key={`${axis}-${String(value)}`} value={String(value)}>{String(value)}</option>)}
                    </select>
                  </label>
                ))}
              </div>
            </div>

            <div className="research-side-panel research-mini-stats">
              <div><div className="research-side-title">{pm('目前列數', 'Current Rows')}</div><div className="research-side-value">{formatInteger(filteredRows.length)}</div></div>
              <div><div className="research-side-title">{pm('候選組合', 'Candidates')}</div><div className="research-side-value">{formatInteger(shortlistViewRows.length)}</div></div>
              <div><div className="research-side-title">{pm('叢集', 'Clusters')}</div><div className="research-side-value">{formatInteger(actualClusterCount)}</div></div>
            </div>

            <div className="research-side-panel">
              <div className="research-side-title">{pm('已選區塊細節', 'Selected Cell Details')}</div>
              {selectedHeatmapCell ? (
                selectedCellRow ? (
                  <>
                    <div className="research-side-copy">
                      {axisLabel(xAxis)}={selectedHeatmapCell.x} | {axisLabel(yAxis)}={selectedHeatmapCell.y}
                    </div>
                    {selectedCellRows.length > 1 ? (
                      <label className="research-filter-label">
                        <span>{pm('區塊內列', 'Rows in Cell')}</span>
                        <select className="text-input" value={selectedCellBacktestId} onChange={(event: ChangeEvent<HTMLSelectElement>) => setSelectedCellBacktestId(event.target.value)}>
                          {selectedCellRows.map((row) => (
                            <option key={row.backtest_id} value={row.backtest_id}>{row.label}</option>
                          ))}
                        </select>
                      </label>
                    ) : null}
                    <div className="candidate-metric-strip-grid candidate-metric-strip-grid-sidebar">
                      <div className="candidate-metric-strip">
                        <div className="candidate-metric-label">{pm('分數快照', 'Score Snapshot')}</div>
                        <div className="candidate-strip-values">
                          <span>{pm('穩健', 'Robust')} {formatMetric(selectedCellRow.robust_score)}</span>
                          <span>{pm('平台區', 'Plateau')} {formatMetric(selectedCellRow.local_plateau_score)}</span>
                          <span>{dl('sharpe')} {formatMetric(selectedCellRow.sharpe)}</span>
                          <span>{pm('報酬', 'Return')} {formatPercentMetric(selectedCellRow.total_return)}</span>
                        </div>
                      </div>
                      <div className="candidate-metric-strip">
                        <div className="candidate-metric-label">{isPortfolioMatrix ? pm('投資組合快照', 'Portfolio Snapshot') : pm('交易快照', 'Trade Snapshot')}</div>
                        <div className="candidate-strip-values">
                          {renderSnapshotChips(selectedCellRow)}
                        </div>
                      </div>
                    </div>
                    <div className="candidate-actions candidate-actions-sidebar">
                      <button className="inline-action-button shortlist-action-button" onClick={() => navigate({ to: '/metrics/backtests', search: { runId, backtestId: selectedCellRow.backtest_id } })}>{pm('回測', 'Backtest')}</button>
                    </div>
                  </>
                ) : (
                  <div className="research-side-copy">{pm('此熱圖區塊目前沒有可檢視的具體列。', 'This heatmap cell has no reviewable row right now.')}</div>
                )
              ) : (
                <div className="research-side-copy">{pm('點擊任一熱圖區塊，即可在此檢視該策略，不需要離開診斷面板。', 'Click any heatmap cell to inspect that strategy without leaving the diagnostics panel.')}</div>
              )}
            </div>
          </aside>
        </div>
      </SectionCard>
      )}

      <SectionCard title={t('parameterMatrix.candidateReview')} subtitle={t('parameterMatrix.candidateReviewSubtitle')} actions={
        <div className="candidate-review-toolbar">
          <div className="candidate-review-toolbar-group candidate-review-toolbar-group-secondary">
            <select className="text-input text-input-compact" value={String(pageSize)} onChange={(event) => { setPageSize(Number(event.target.value)); setPage(1) }}>
              {[10, 20, 50].map((size) => <option key={size} value={size}>{language === 'zh-Hant' ? `每頁 ${size} 筆` : `${size} / page`}</option>)}
            </select>
          </div>
        </div>
      }>
        {showAcceptanceSignals ? <div className="shortlist-summary-strip">
          <div className="shortlist-summary-card"><div className="summary-chip-label">{pm('通過', 'Pass')} <InfoHint label={pm('通過', 'Pass')} body={pm('通過代表候選組合已達目前參數判定門檻；仍需另外和前向分析 (WFA) 結果交叉檢查。', 'Pass means the candidate meets the current parameter verdict gates; it still needs to be checked against WFA results.')} /></div><div className="summary-chip-value tone-positive">{acceptanceSummary.pass}</div></div>
          <div className="shortlist-summary-card"><div className="summary-chip-label">{pm('需檢視', 'Needs Review')} <InfoHint label={pm('需檢視', 'Needs Review')} body={pm('需檢視代表候選組合未完全通過接受門檻，但仍有平台區支持或原始強度，值得人工再看。', 'Needs Review means the candidate does not fully pass the gates but has enough plateau support or raw strength to inspect manually.')} /></div><div className="summary-chip-value tone-neutral">{acceptanceSummary.review}</div></div>
          <div className="shortlist-summary-card"><div className="summary-chip-label">{pm('未通過', 'Fail')} <InfoHint label={pm('未通過', 'Fail')} body={pm('未通過代表候選組合未達接受門檻，也缺少足夠後備強度留在檢視層。', 'Fail means the candidate does not meet the gates and lacks enough reserve strength to stay in review.')} /></div><div className="summary-chip-value tone-negative">{acceptanceSummary.fail}</div></div>
          <div className="shortlist-summary-card"><div className="summary-chip-label">{pm('候選組合', 'Candidates')}</div><div className="summary-chip-value">{shortlistViewRows.length}</div></div>
        </div> : null}
        <details className="research-advanced-panel">
          <summary>
            <span className="research-advanced-chevron" aria-hidden="true">▾</span>
            <span>{pm('進階檢視規則', 'Advanced Review Rules')}</span>
          </summary>
          <div className="research-advanced-copy">
            {pm('這些是本頁目前使用的前向分析 (WFA) 前置篩選規則；只用來在 WFA 前分類候選組合，因此此階段不套用 OOS/IS 比率門檻。', 'These are the current pre-WFA review rules. They only classify candidates before WFA, so this stage does not apply OOS/IS ratio gates.')}
          </div>
          <div className="research-summary-grid research-summary-grid-advanced">
            <div className="research-summary-block">
              <div className="research-summary-title">{pm('已啟用門檻', 'Active Gates')}</div>
              <div className="research-metric-inline-list">
                {activeGateChips.map((item) => <span key={item} className="research-metric-inline-chip">{item}</span>)}
              </div>
              <div className="research-metric-footnote">{pm('WFA 前置篩選會使用獲利因子、勝率、交易數與回撤下限等已知指標；此階段刻意不使用 OOS/IS 比率。', 'Pre-WFA filtering uses known metrics such as Profit Factor, win rate, trade count, and drawdown floor. OOS/IS ratio is intentionally not used here.')}</div>
            </div>
            <div className="research-summary-block research-summary-block-wide">
              <div className="research-summary-title">{pm('已啟用評分', 'Active Scoring')}</div>
              <div className="research-metric-inline-list">
                <span className="research-metric-inline-chip">{pm('設定檔', 'Profile')}: {dl(activeRankingConfig?.profile || 'balanced')}</span>
                {rankingWeightChips.map((item) => <span key={item} className="research-metric-inline-chip">{item}</span>)}
              </div>
              <div className="research-metric-footnote">{pm('此處穩健分數是 WFA 前的綜合分數，由夏普比率、平台區支持與回撤懲罰組成；它不是即時 Optuna 試驗分數，也不再包含 OOS/IS 比率。', 'The robust score here is a pre-WFA composite score built from Sharpe, plateau support, and drawdown penalty. It is not a live Optuna trial score and no longer includes OOS/IS ratio.')}{defaultTemplateName ? pm(` 預設範本：${defaultTemplateName}。`, ` Default template: ${defaultTemplateName}.`) : ''}</div>
            </div>
          </div>
          <div className="advanced-rule-grid">
            <label className="advanced-rule-field">
              <span>{pm('最低獲利因子', 'Minimum Profit Factor')}</span>
              <input
                className="text-input"
                type="number"
                step="0.1"
                value={gateDraft.min_profit_factor ?? ''}
                onChange={(event) => setGateDraft((current) => ({ ...current, min_profit_factor: asNullableNumber(event.target.value) }))}
              />
            </label>
            <label className="advanced-rule-field">
              <span>{pm('最低勝率 (%)', 'Minimum Win Rate (%)')}</span>
              <input
                className="text-input"
                type="number"
                step="1"
                value={formatPercentInput(gateDraft.min_win_rate)}
                onChange={(event) => setGateDraft((current) => ({ ...current, min_win_rate: parsePercentToRatio(event.target.value) }))}
              />
            </label>
            <label className="advanced-rule-field">
              <span>{pm('最低交易數', 'Minimum Trades')}</span>
              <input
                className="text-input"
                type="number"
                step="1"
                value={gateDraft.min_trade_count ?? ''}
                onChange={(event) => setGateDraft((current) => ({ ...current, min_trade_count: asNullableNumber(event.target.value) }))}
              />
            </label>
            <label className="advanced-rule-field">
              <span>{pm('最大回撤下限 (%)', 'Max Drawdown Floor (%)')}</span>
              <input
                className="text-input"
                type="number"
                step="1"
                value={formatDrawdownPercentInput(gateDraft.max_drawdown_floor)}
                onChange={(event) => setGateDraft((current) => ({ ...current, max_drawdown_floor: parseDrawdownPercentToFloor(event.target.value) }))}
              />
            </label>
            <label className="advanced-rule-field">
              <span>{pm('排名設定檔', 'Ranking Profile')}</span>
              <select
                className="text-input"
                value={String(rankingDraft.profile || activeRankingConfig?.profile || 'balanced')}
                onChange={(event) => setRankingDraft((current) => ({ ...current, profile: event.target.value }))}
              >
                {['balanced', 'stability_first', 'performance_first', 'drawdown_aware'].map((profile) => (
                  <option key={profile} value={profile}>{dl(profile)}</option>
                ))}
              </select>
            </label>
            <label className="advanced-rule-field">
              <span>{pm('夏普比率權重', 'Sharpe Weight')}</span>
              <input
                className="text-input"
                type="number"
                step="0.05"
                value={rankingDraft.weights?.sharpe_weight ?? ''}
                onChange={(event) => setRankingDraft((current) => ({
                  ...current,
                  weights: { ...(current.weights || {}), sharpe_weight: asNullableNumber(event.target.value) },
                }))}
              />
            </label>
            <label className="advanced-rule-field">
              <span>{pm('平台區權重', 'Plateau Weight')}</span>
              <input
                className="text-input"
                type="number"
                step="0.05"
                value={rankingDraft.weights?.plateau_weight ?? ''}
                onChange={(event) => setRankingDraft((current) => ({
                  ...current,
                  weights: { ...(current.weights || {}), plateau_weight: asNullableNumber(event.target.value) },
                }))}
              />
            </label>
            <label className="advanced-rule-field">
              <span>{pm('回撤懲罰', 'Drawdown Penalty')}</span>
              <input
                className="text-input"
                type="number"
                step="0.05"
                value={rankingDraft.weights?.drawdown_penalty_weight ?? ''}
                onChange={(event) => setRankingDraft((current) => ({
                  ...current,
                  weights: { ...(current.weights || {}), drawdown_penalty_weight: asNullableNumber(event.target.value) },
                }))}
              />
            </label>
          </div>
          <div className="advanced-template-row">
            <label className="advanced-rule-field advanced-rule-field-grow">
              <span>{pm('範本名稱', 'Template Name')}</span>
              <input
                className="text-input"
                value={templateName}
                onChange={(event) => setTemplateName(event.target.value)}
                placeholder={pm('例如 stability-first-manual', 'For example: stability-first-manual')}
              />
            </label>
            <label className="advanced-rule-field advanced-rule-field-grow">
              <span>{pm('已儲存範本', 'Saved Templates')}</span>
              <select
                className="text-input"
                value={selectedTemplateName}
                onChange={(event) => setSelectedTemplateName(event.target.value)}
              >
                <option value="">{pm('選擇範本', 'Choose Template')}</option>
                {templateItems.map((template) => (
                  <option key={template.name} value={template.name}>{template.is_default ? `${template.name} | ${pm('預設', 'Default')}` : template.name}</option>
                ))}
              </select>
            </label>
          </div>
          <div className="candidate-review-toolbar candidate-review-toolbar-advanced">
            <div className="candidate-review-toolbar-group candidate-review-toolbar-group-primary">
              <button className="ghost-button shortlist-toolbar-button" onClick={() => previewMutation.mutate(undefined)} disabled={previewMutation.isPending}>
                {previewMutation.isPending ? pm('套用中...', 'Applying...') : pm('套用檢視規則', 'Apply Review Rules')}
              </button>
              <button className="ghost-button shortlist-toolbar-button" onClick={applySelectedTemplate} disabled={!selectedTemplate}>
                {pm('載入範本', 'Load Template')}
              </button>
              <button className="ghost-button shortlist-toolbar-button" onClick={() => saveTemplateMutation.mutate()} disabled={saveTemplateMutation.isPending || !templateName.trim()}>
                {saveTemplateMutation.isPending ? pm('儲存中...', 'Saving...') : pm('儲存範本', 'Save Template')}
              </button>
              <button
                className="ghost-button shortlist-toolbar-button"
                onClick={() => { if (selectedTemplate) setDefaultTemplateMutation.mutate(selectedTemplate.name) }}
                disabled={!selectedTemplate || setDefaultTemplateMutation.isPending}
              >
                {setDefaultTemplateMutation.isPending ? pm('設定中...', 'Setting...') : pm('設為預設', 'Set as Default')}
              </button>
              <button
                className="ghost-button shortlist-toolbar-button"
                onClick={() => { if (selectedTemplate) deleteTemplateMutation.mutate(selectedTemplate.name) }}
                disabled={!selectedTemplate || deleteTemplateMutation.isPending}
              >
                {deleteTemplateMutation.isPending ? pm('刪除中...', 'Deleting...') : pm('刪除範本', 'Delete Template')}
              </button>
            </div>
            <div className="candidate-review-toolbar-group candidate-review-toolbar-group-secondary">
              <button className="ghost-button shortlist-toolbar-button" onClick={resetAdvancedRules}>
                {pm('重設', 'Reset')}
              </button>
            </div>
          </div>
          {reviewRulesFeedback ? <div className="research-advanced-feedback">{reviewRulesFeedback}</div> : null}
        </details>

        <div className="candidate-card-list">
          {pageRows.map((row) => {
            const key = String(row.candidate_key || shortlistKey(row))
            const displayType = displayRepresentativeType(row.representative_type, language)
            const displaySource = displaySourceLabel(row.source, derivedFromExistingResults, language)
            const rationale = buildSelectionRationale(row, derivedFromExistingResults, language)
            return (
              <div key={key} className={`candidate-card ${row.backtest_id === highlightedBacktestId ? 'candidate-card-selected' : ''}`}>
                <div className="candidate-card-header">
                  <div className="candidate-card-ident">
                    <div className="candidate-rank-chip">#{row.rank}</div>
                <div className="candidate-card-copy">
                  <div className="candidate-card-title" data-private-strategy>{row.label}</div>
                  <div className="candidate-card-subtitle">{displayType} | {displaySource}</div>
                  <div className="candidate-context-line">{rationale}</div>
                </div>
              </div>
                </div>
                <div className="candidate-marker-row">
                  <span className={`candidate-marker-chip ${representativeMarkerTone(displayType)}`}>{displayType}</span>
                  <span className="candidate-marker-chip marker-source-chip">{displaySource}</span>
                  {row.cluster_id !== null && row.cluster_id !== undefined ? <span className="candidate-marker-chip marker-cluster-id">{pm('叢集', 'Cluster')} {row.cluster_id}</span> : null}
                </div>
                <div className="candidate-metric-strip-grid">
                  <div className="candidate-metric-strip">
                    <div className="candidate-metric-label">{pm('分數快照', 'Score Snapshot')}</div>
                    <div className="candidate-strip-values">
                      <span>{pm('穩健', 'Robust')} {formatMetric(row.robust_score)}</span>
                      <span>{pm('平台區', 'Plateau')} {formatMetric(row.local_plateau_score)}</span>
                      <span>{pm('穩定度', 'Stability')} {formatMetric(row.stability_score)}</span>
                      <span>{derivedFromExistingResults ? dl('sharpe') : pm('OOS 夏普比率', 'OOS Sharpe')} {formatMetric(row.mean_oos_sharpe)}</span>
                      {!derivedFromExistingResults ? <span>OOS / IS {formatMetric(row.oos_is_ratio)}</span> : null}
                    </div>
                  </div>
                  <div className="candidate-metric-strip">
                    <div className="candidate-metric-label">{isPortfolioMatrix ? pm('投資組合快照', 'Portfolio Snapshot') : pm('交易快照', 'Trade Snapshot')} <InfoHint label={isPortfolioMatrix ? pm('投資組合快照', 'Portfolio Snapshot') : pm('交易快照', 'Trade Snapshot')} body={snapshotHelp} /></div>
                    <div className="candidate-strip-values">
                      {renderSnapshotChips(row)}
                    </div>
                  </div>
                </div>
                <div className="candidate-card-footer">
                  <div className="candidate-actions">
                    <button className="inline-action-button shortlist-action-button" onClick={() => navigate({ to: '/metrics/backtests', search: { runId, backtestId: row.backtest_id } })}>{pm('回測', 'Backtest')}</button>
                  </div>
                </div>
              </div>
            )
          })}
        </div>

        {!shortlistViewRows.length ? <div className="helper-text">{pm('目前來源沒有符合條件的檢視候選組合。', 'No review candidates match the current source.')}</div> : null}

        <div className="pagination-row">
          <button className="ghost-button" disabled={page <= 1} onClick={() => setPage((current) => Math.max(1, current - 1))}>{t('common.previous')}</button>
          <span className="muted">{language === 'zh-Hant' ? `第 ${page} / ${totalPages} 頁` : `Page ${page} / ${totalPages}`}</span>
          <button className="ghost-button" disabled={page >= totalPages} onClick={() => setPage((current) => Math.min(totalPages, current + 1))}>{t('common.next')}</button>
        </div>
      </SectionCard>

      <SectionCard title={t('parameterMatrix.candidateEvidence')} subtitle={pm('說明這些參數組合為何在執行前向分析 (WFA) 前被保留檢視。', 'Explains why these parameter combinations were kept for review before Walk-Forward Analysis (WFA).')}>
        <div className="research-diagnostics-grid">
          <div className="research-diagnostics-card"><div className="research-diagnostics-title">{pm('最高排名摘要', 'Top-Ranked Summary')}</div><div className="research-diagnostics-hero" data-private-strategy>{topTrial?.label || '-'}</div><div className="research-diagnostics-copy" data-private-strategy>{topTrial ? formatParams(topTrial.params) : pm('沒有可用候選組合。', 'No candidate is available.')}</div><div className="research-diagnostics-meta">{pm('穩健', 'Robust')} {formatMetric(topTrial?.robust_score)} | OOS {formatMetric(topTrial?.mean_oos_sharpe)}</div></div>
          <div className="research-diagnostics-card"><div className="research-diagnostics-title">{pm('參數重要度', 'Parameter Importance')} <InfoHint label={pm('參數重要度', 'Parameter Importance')} body={pm('用來粗略顯示哪些參數較能解釋目前候選結果的差異。數值越高代表該參數在這批回測結果中的影響較明顯；它不是單獨的買賣理由，也不是 WFA 通過證據。', 'A rough read of which parameters explain more of the variation in the current candidate results. Higher values mean the parameter mattered more inside this batch; it is not a standalone trading reason or WFA pass evidence.')} /></div>{(payload.parameter_importance || []).slice(0, 5).map((item) => <div key={item.parameter} className="research-diagnostics-row" data-private-strategy><span>{axisLabel(item.parameter)}</span><strong>{formatMetric(item.importance, 3)}</strong></div>)}</div>
          <div className="research-diagnostics-card"><div className="research-diagnostics-title">{pm('叢集摘要', 'Cluster Summary')} <InfoHint label={pm('叢集摘要', 'Cluster Summary')} body={pm('把表現相近或參數位置相近的候選組合分成幾個群組，幫你看結果是否集中在少數孤立點，還是附近有一片相似區域。叢集分數只適合做篩選參考，仍需要獨立 WFA 或滾動驗證。', 'Groups similar candidate combinations so you can see whether a result is an isolated point or part of a nearby region. Cluster scores are screening context only and still need independent WFA or rolling validation.')} /></div>{(payload.cluster_summary || []).slice(0, 4).map((cluster) => <div key={cluster.cluster_id} className="research-diagnostics-row" data-private-strategy><span>{pm('叢集', 'Cluster')} {cluster.cluster_id} | {pm('大小', 'Size')} {cluster.size}</span><strong>{formatMetric(cluster.mean_oos_sharpe)}</strong></div>)}</div>
        </div>
      </SectionCard>
    </div>
  )
}
