import { Fragment, useEffect, useMemo, useState } from 'react'
import { Link, useNavigate, useRouterState } from '../routing'
import { useQuery } from '@tanstack/react-query'

import { api } from '../api'
import { makeChartLayout, plotConfig } from '../chartTheme'
import { Language, useCopy } from '../i18n'
import { CustomSelect } from '../components/CustomSelect'
import { InfoHint } from '../components/InfoHint'
import { Plot, preloadPlotly } from '../components/LazyPlot'
import { MissingState } from '../components/MissingState'
import { SectionCard } from '../components/SectionCard'
import { StrategyRulesPanel } from '../components/StrategyRulesPanel'
import { useAppStore } from '../store'
import {
  candidateFilterLabel,
  parameterLabel,
  reviewReasonLabel as controlledReviewReasonLabel,
  selectionEvidenceLabel as controlledSelectionEvidenceLabel,
  uiText,
  windowSizingLabel as controlledWindowSizingLabel,
} from '../uiVocabulary'

type WfaRow = {
  window_id: number
  semantic_combo: Record<string, unknown>
  is_sharpe?: number | null
  is_calmar?: number | null
  oos_sharpe?: number | null
  oos_calmar?: number | null
  oos_total_return?: number | null
  oos_profit_factor?: number | null
  oos_win_rate?: number | null
  oos_max_drawdown?: number | null
  test_start_date?: string | null
  test_end_date?: string | null
  train_start_date?: string | null
  train_end_date?: string | null
  selection_evidence?: string | null
  selection_source?: string | null
  selection_rank?: number | null
  selection_metric?: string | number | null
  candidate_count?: number | null
  wfa_row_type?: string | null
  linked_backtest?: { run_id: string; backtest_id: string } | null
  oos_portfolio?: WfaPortfolioSnapshot | null
  oos_rebalance_count?: number | null
  oos_avg_exposure?: number | null
  oos_avg_holdings?: number | null
  oos_total_turnover?: number | null
  oos_cost_drag?: number | null
  is_risk_gate_event_count?: number | null
  oos_risk_gate_event_count?: number | null
  oos_risk_gate_summary?: Record<string, unknown> | null
}

type WfaPortfolioWeight = {
  asset: string
  avg_weight?: number | null
  last_weight?: number | null
  active_days?: number | null
}

type WfaPortfolioContribution = {
  asset: string
  return_contribution?: number | null
  avg_weight?: number | null
}

type WfaPortfolioSnapshot = {
  asset_count?: number | null
  allocation?: WfaPortfolioWeight[]
  contribution?: WfaPortfolioContribution[]
  active_rebalance_count?: number | null
  checkpoint_count?: number | null
  avg_exposure?: number | null
  avg_holdings?: number | null
  total_turnover?: number | null
  cost_drag?: number | null
  risk_gate_event_count?: number | null
  risk_gate_summary?: Record<string, unknown> | null
}

type WfaPortfolioWindowSummary = {
  is_portfolio_wfa?: boolean
  allocation_by_window?: Array<{
    window_id: number
    semantic_combo?: Record<string, unknown>
    test_start_date?: string | null
    test_end_date?: string | null
    avg_exposure?: number | null
    avg_holdings?: number | null
    active_rebalance_count?: number | null
    risk_gate_event_count?: number | null
    weights: WfaPortfolioWeight[]
  }>
  contribution_by_window?: Array<{
    window_id: number
    semantic_combo?: Record<string, unknown>
    test_start_date?: string | null
    test_end_date?: string | null
    contributions: WfaPortfolioContribution[]
  }>
  asset_summary?: Array<{
    asset: string
    mean_avg_weight?: number | null
    mean_last_weight?: number | null
    active_windows?: number | null
    return_contribution?: number | null
  }>
}

type WfaComboGroup = {
  combo_key: string
  label: string
  params: Record<string, unknown>
  representative_type?: string
  source?: string
  cluster_id?: number | null
  cluster_size?: number | null
  local_plateau_score?: number | null
  candidate_key?: string
  wfa_pack_inclusion_reason?: string
  mean_is_sharpe?: number | null
  mean_is_calmar?: number | null
  mean_oos_sharpe?: number | null
  mean_oos_calmar?: number | null
  oos_is_ratio?: number | null
  trade_count?: number | null
  selection_evidence?: string | null
  selected_window_count?: number | null
  robust_score?: number | null
  accepted?: boolean
  acceptance_reasons?: string[]
  rows: WfaRow[]
}

function comboLabel(combo: Record<string, unknown>, language: Language) {
  const keys = Object.keys(combo || {}).sort()
  if (!keys.length) return uiText(language, 'not_recorded')
  return keys.map((key) => `${key}=${String(combo[key])}`).join(' | ')
}

function mean(values: Array<number | null | undefined>) {
  const numeric = values.filter((value): value is number => typeof value === 'number' && Number.isFinite(value))
  if (!numeric.length) return null
  return numeric.reduce((sum, value) => sum + value, 0) / numeric.length
}

function median(values: Array<number | null | undefined>) {
  const numeric = values.filter((value): value is number => typeof value === 'number' && Number.isFinite(value)).sort((left, right) => left - right)
  if (!numeric.length) return null
  const middle = Math.floor(numeric.length / 2)
  return numeric.length % 2 ? numeric[middle] : (numeric[middle - 1] + numeric[middle]) / 2
}

function formatMetric(value: number | null | undefined, digits = 3) {
  return typeof value === 'number' && Number.isFinite(value) ? value.toFixed(digits) : '-'
}

function formatPercent(value: number | null | undefined) {
  return typeof value === 'number' && Number.isFinite(value) ? `${Math.round(value * 100)}%` : '-'
}

function formatPercentPrecise(value: number | null | undefined, digits = 1) {
  return typeof value === 'number' && Number.isFinite(value) ? `${(value * 100).toFixed(digits)}%` : '-'
}

function ratio(left: number | null | undefined, right: number | null | undefined) {
  if (typeof left !== 'number' || typeof right !== 'number' || !Number.isFinite(left) || !Number.isFinite(right) || right === 0) return null
  return left / right
}

function reviewReasonLabel(reason: string, language: Language) {
  return controlledReviewReasonLabel(reason, language)
}

function selectionEvidenceLabel(value: string | null | undefined, language: Language) {
  return controlledSelectionEvidenceLabel(value, language)
}

function formatLabel(value: string) {
  return value.replace(/_/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase())
}

function displayDateToken(value: string) {
  return /^(\d{4})(\d{2})(\d{2})$/.test(value)
    ? value.replace(/^(\d{4})(\d{2})(\d{2})$/, '$1-$2-$3')
    : value
}

function displayFactorToken(value: string, language: Language) {
  const normalized = value.trim().toUpperCase()
  if (normalized === 'PRICE') return language === 'zh-Hant' ? '價格' : 'Price'
  return normalized.replace(/-/g, ' + ')
}

function displayAssetToken(value: string, language: Language) {
  const normalized = value.trim().toUpperCase()
  if (['LOCAL', 'DATASET', 'ASSET'].includes(normalized)) return language === 'zh-Hant' ? '資料集' : 'Dataset'
  return value
}

function displayModeToken(value: string, language: Language) {
  const normalized = value.trim().toLowerCase()
  if (normalized === 'windows') return language === 'zh-Hant' ? '前向分析視窗' : 'Rolling Windows'
  if (normalized === 'matrix') return language === 'zh-Hant' ? '參數矩陣' : 'Parameter Matrix'
  if (normalized === 'single') return language === 'zh-Hant' ? '單次回測' : 'Single Backtest'
  if (normalized === 'summary') return language === 'zh-Hant' ? '摘要' : 'Summary'
  return parameterLabel(value, language)
}

function runShortId(run: any, rawLabel?: string) {
  const runId = String(run?.run_id || '').trim()
  const fromRunId = runId.includes('_') ? runId.split('_', 2)[1] : runId
  if (fromRunId) return fromRunId.slice(0, 6)
  const fromLabel = String(rawLabel || '').match(/\b(?:batch|run)\s+([a-z0-9]{6,})\b/i)?.[1]
  return fromLabel ? fromLabel.slice(0, 6) : ''
}

function labelFromStructuredName(rawLabel: string, run: any, language: Language) {
  const clean = basename(rawLabel)
    .replace(/\.(user\.)?json$/i, '')
    .replace(/\.parquet$/i, '')
    .replace(/\s+-\s+(?:batch|run)\s+[a-z0-9]+$/i, '')
  const parts = clean.split('_').filter(Boolean)
  const modeIndex = parts.findIndex((part, index) =>
    index >= 4 && ['windows', 'matrix', 'single', 'summary'].includes(part.toLowerCase()),
  )
  if (!parts.length || modeIndex < 0 || !/^\d{8}$/.test(parts[1] || '')) return ''
  const workflow = parts[0].toLowerCase() === 'wfa'
    ? language === 'zh-Hant' ? '前向分析 (WFA)' : 'Walk-Forward'
    : parts[0].toLowerCase() === 'backtest'
    ? language === 'zh-Hant' ? '回測' : 'Backtest'
    : formatLabel(parts[0])
  const runId = runShortId(run, rawLabel)
  return [
    workflow,
    displayDateToken(parts[1]),
    displayAssetToken(parts[2], language),
    displayFactorToken(parts[3] || '', language),
    displayModeToken(parts[modeIndex], language),
    runId ? `${language === 'zh-Hant' ? '執行' : 'run'} ${runId}` : '',
  ]
    .filter(Boolean)
    .join(' | ')
}

function displayWfaRunLabel(run: any, language: Language) {
  const selectorLabel = String(run?.selector_label || '').trim()
  const displayLabel = String(run?.display_label || '').trim()
  for (const label of [displayLabel, selectorLabel]) {
    if (!label) continue
    const structuredLabel = labelFromStructuredName(label, run, language)
    if (structuredLabel) return structuredLabel
    if (!/\b(?:ma-cross|hold-reset|threshold-hold-reset|batch)\b/i.test(label)) return label
  }
  const base = String(run?.primary_artifact_name || run?.config_filename || run?.run_id || '').trim()
    .replace(/\.user\.json$/i, '')
    .replace(/\.json$/i, '')
    .replace(/\.parquet$/i, '')
  const structuredLabel = labelFromStructuredName(base, run, language)
  if (structuredLabel) return structuredLabel
  const suffix = runShortId(run, base)
  return suffix ? `${base} | ${language === 'zh-Hant' ? '執行' : 'run'} ${suffix}` : base
}

function basename(path: unknown) {
  return String(path || '').split(/[\\/]/).filter(Boolean).pop() || ''
}

export function WFAPage() {
  const navigate = useNavigate()
  const search = useRouterState({
    select: (state) => state.location.search,
  }) as Record<string, string | undefined>
  const selectedWfaRunId = useAppStore((state) => state.selectedWfaRunId)
  const setSelectedWfaRunId = useAppStore((state) => state.setSelectedWfaRunId)
  const language = useAppStore((state) => state.language)
  const t = useCopy(language)
  const wf = (zh: string, en: string) => (language === 'zh-Hant' ? zh : en)
  const runsQuery = useQuery({
    queryKey: ['wfa-runs'],
    queryFn: api.wfaRuns,
    staleTime: 10000,
  })
  const [selectedMetricFamily, setSelectedMetricFamily] = useState<'sharpe' | 'calmar'>('sharpe')
  const [expandedComboKeys, setExpandedComboKeys] = useState<string[]>([])

  useEffect(() => {
    preloadPlotly()
  }, [])

  const runId = search.runId || selectedWfaRunId || runsQuery.data?.[0]?.run_id || ''
  const availableRunIds = useMemo(
    () => (runsQuery.data || []).map((run: any) => run.run_id),
    [runsQuery.data],
  )
  const hasResolvedRun = Boolean(runId && availableRunIds.includes(String(runId)))
  const query = useQuery({
    queryKey: ['wfa-dashboard', runId],
    queryFn: () => api.wfaDashboard(runId),
    enabled: hasResolvedRun,
    staleTime: 10000,
  })
  const isReadmeDemoPayload = query.data?.schema_version === 'readme-demo'
  const rows = Array.isArray(query.data?.rows) ? (query.data.rows as WfaRow[]) : []
  const strategySummary = query.data?.strategy_summary || {}
  const batchMetadata = (query.data?.batch_metadata || {}) as Record<string, any>
  const windowingMetadata = (batchMetadata.windowing || {}) as Record<string, any>
  const selectionConstraintsMetadata = (batchMetadata.selection_constraints || {}) as Record<string, any>
  const portfolioSummary = (query.data?.portfolio_window_summary || {}) as WfaPortfolioWindowSummary
  const portfolioAllocationWindows = Array.isArray(portfolioSummary.allocation_by_window) ? portfolioSummary.allocation_by_window : []
  const portfolioContributionWindows = Array.isArray(portfolioSummary.contribution_by_window) ? portfolioSummary.contribution_by_window : []
  const portfolioAssetSummary = Array.isArray(portfolioSummary.asset_summary) ? portfolioSummary.asset_summary : []
  const portfolioAssets = Array.from(
    new Set(
      portfolioAllocationWindows.flatMap((window) =>
        Array.isArray(window.weights) ? window.weights.map((item) => String(item.asset || '')).filter(Boolean) : [],
      ),
    ),
  ).sort()
  const showPortfolioPanels = Boolean(portfolioSummary.is_portfolio_wfa && portfolioAllocationWindows.length && portfolioAssets.length > 1)
  const isRollingValidation = batchMetadata.workflow === 'rolling_validation'
  const legacyGridDetected = Boolean(batchMetadata.legacy_grid_detected)
  const diagnosticRowCount = Number(batchMetadata.diagnostic_row_count || 0)
  const windowSizingSource = String(windowingMetadata.sizing_source || windowingMetadata.size_mode || 'unknown')
  const windowingMetadataSource = String(windowingMetadata.metadata_source || '')
  const selectionMetadataSource = String(selectionConstraintsMetadata.metadata_source || '')
  const isInferredLegacyWindowing = windowSizingSource === 'artifact_dates' || windowingMetadataSource === 'artifact_dates'
  const isSelectionFilterUnrecorded = selectionMetadataSource === 'artifact_rows' && !selectionConstraintsMetadata.enabled
  const isAutoWindowSizing = windowSizingSource === 'auto' || windowingMetadata.size_mode === 'auto'
  const windowSizingLabel = controlledWindowSizingLabel(
    isAutoWindowSizing ? 'auto' : isInferredLegacyWindowing ? 'artifact_dates' : windowSizingSource,
    language,
  )
  const windowSizingBody = isAutoWindowSizing
    ? wf(
        `自動模式會根據總資料筆數、目標視窗數、訓練/測試(train/test)比例提示、策略最大回看期(lookback)、最低訓練長度，以及步長(step)是否有輸入來估算樣本內/樣本外(IS/OOS)長度。今次資料筆數 ${windowingMetadata.total_observations ?? '-'}，目標視窗 ${windowingMetadata.target_window_count ?? '-'}，最大回看期(lookback) ${windowingMetadata.strategy_max_lookback ?? '-' }。`,
        `Auto mode estimates IS/OOS lengths from total observations, target window count, train/test ratio hints, the strategy's largest lookback, minimum train length, and whether step size was supplied. This run has ${windowingMetadata.total_observations ?? '-'} observations, target ${windowingMetadata.target_window_count ?? '-'} windows, and max lookback ${windowingMetadata.strategy_max_lookback ?? '-'}.`,
      )
    : isInferredLegacyWindowing
      ? wf(
          '這是新版中繼資料(metadata)出現前產生的前向分析(WFA)結果。系統只能從產物(artifact)內的訓練/測試(train/test)日期推斷樣本內/樣本外(IS/OOS)長度，無法知道當時是自動模式、輸入比例，還是輸入日數。',
          'This WFA result was generated before the new metadata existed. The system can infer IS/OOS lengths from the train/test dates in the artifact, but cannot know whether the run used auto sizing, input ratios, or input sizes.',
        )
    : wf(
        '目前使用設定檔(config)內輸入的視窗(window)數字；系統只會做基本安全修正，例如避免樣本內(IS) + 樣本外(OOS)長度超出資料範圍。',
        'This run uses the window numbers supplied by the config; the system only applies basic safety corrections, such as preventing IS + OOS from exceeding the available data.',
      )
  const effectiveWindowText = wf(
    `樣本內(IS) ${windowingMetadata.effective_train_size ?? '-'} / 樣本外(OOS) ${windowingMetadata.effective_test_size ?? '-'} / 步長(step) ${windowingMetadata.effective_step_size ?? '-'}`,
    `IS ${windowingMetadata.effective_train_size ?? '-'} / OOS ${windowingMetadata.effective_test_size ?? '-'} / step ${windowingMetadata.effective_step_size ?? '-'}`,
  )
  const selectionConstraintsEnabled = Boolean(selectionConstraintsMetadata.enabled)
  const selectionConstraintText = candidateFilterLabel(
    selectionConstraintsEnabled ? 'enabled' : isSelectionFilterUnrecorded ? 'not_recorded' : 'disabled_by_config',
    language,
  )
  const selectionConstraintBody = selectionConstraintsEnabled
    ? wf(
        `選參數前會先檢查樣本內(IS)候選是否有足夠交易活動。門檻：有效再平衡(active rebalance) >= ${selectionConstraintsMetadata.min_is_active_rebalances ?? 0}，曝險比例(exposure ratio) >= ${formatPercentPrecise(selectionConstraintsMetadata.min_is_exposure_ratio ?? 0)}，非零回報日 >= ${selectionConstraintsMetadata.min_is_nonzero_return_days ?? 0}，最大回看期/樣本內(lookback / IS) <= ${formatPercentPrecise(selectionConstraintsMetadata.max_lookback_fraction_of_train ?? 0)}。這只使用樣本內(IS)資訊，不偷看樣本外(OOS)。`,
        `Before ranking parameters, IS candidates must show enough trading activity. Gates: active rebalances >= ${selectionConstraintsMetadata.min_is_active_rebalances ?? 0}, exposure ratio >= ${formatPercentPrecise(selectionConstraintsMetadata.min_is_exposure_ratio ?? 0)}, non-zero return days >= ${selectionConstraintsMetadata.min_is_nonzero_return_days ?? 0}, and max lookback / IS <= ${formatPercentPrecise(selectionConstraintsMetadata.max_lookback_fraction_of_train ?? 0)}. This uses IS information only and does not inspect OOS.`,
      )
    : isSelectionFilterUnrecorded
      ? wf(
          '這是新版樣本內(IS)候選篩選中繼資料(metadata)出現前產生的結果。產物(artifact)沒有記錄當時是否套用了可交易性篩選；重新執行新版前向分析(WFA)設定檔(config)後會顯示已啟用或已關閉。',
          'This result was generated before IS candidate-filter metadata existed. The artifact does not record whether a viability filter was applied; rerun the updated WFA config to show Enabled or Disabled.',
        )
    : wf(
        '目前未在樣本內(IS)排名之前篩走低活動候選；所有候選都會直接按目標指標排名。',
        'No low-activity IS candidate filter is applied before ranking; all candidates are ranked directly by the selected objective.',
      )
  const groupedCombos = useMemo(() => {
    const payloadGroups = Array.isArray(query.data?.combo_groups) ? (query.data.combo_groups as WfaComboGroup[]) : null
    if (payloadGroups?.length) {
        return payloadGroups.map((group) => ({
          comboKey: group.combo_key,
          comboLabel: group.label || comboLabel(group.params || {}, language),
          combo: group.params || {},
          representativeType: group.representative_type,
          source: group.source,
          clusterId: group.cluster_id,
          clusterSize: group.cluster_size,
          localPlateauScore: group.local_plateau_score,
          candidateKey: group.candidate_key,
          inclusionReason: group.selection_evidence || group.wfa_pack_inclusion_reason,
          selectedWindowCount: group.selected_window_count ?? group.rows?.length ?? 0,
          rows: (group.rows || []).sort((left, right) => Number(left.window_id) - Number(right.window_id)),
          avg_is_sharpe: group.mean_is_sharpe ?? mean((group.rows || []).map((row) => row.is_sharpe)),
        avg_is_calmar: group.mean_is_calmar ?? mean((group.rows || []).map((row) => row.is_calmar)),
        avg_oos_sharpe: group.mean_oos_sharpe ?? mean((group.rows || []).map((row) => row.oos_sharpe)),
        avg_oos_calmar: group.mean_oos_calmar ?? mean((group.rows || []).map((row) => row.oos_calmar)),
        oos_is_ratio: group.oos_is_ratio,
        robust_score: group.robust_score,
        accepted: group.accepted,
        acceptanceReasons: group.acceptance_reasons || [],
      }))
    }
    const mapping = new Map<string, { comboKey: string; comboLabel: string; combo: Record<string, unknown>; rows: WfaRow[] }>()
    rows.forEach((row) => {
      const combo = row.semantic_combo || {}
      const comboKey = JSON.stringify(combo)
      if (!mapping.has(comboKey)) {
        mapping.set(comboKey, {
          comboKey,
          comboLabel: comboLabel(combo, language),
          combo,
          rows: [],
        })
      }
      mapping.get(comboKey)!.rows.push(row)
    })
    return Array.from(mapping.values()).map((group) => ({
      ...group,
        rows: group.rows.sort((left, right) => Number(left.window_id) - Number(right.window_id)),
        representativeType: undefined,
        source: undefined,
        clusterId: null,
        clusterSize: null,
        localPlateauScore: null,
        candidateKey: undefined,
        inclusionReason: undefined,
        selectedWindowCount: group.rows.length,
        avg_is_sharpe: mean(group.rows.map((row) => row.is_sharpe)),
      avg_is_calmar: mean(group.rows.map((row) => row.is_calmar)),
      avg_oos_sharpe: mean(group.rows.map((row) => row.oos_sharpe)),
      avg_oos_calmar: mean(group.rows.map((row) => row.oos_calmar)),
      oos_is_ratio: null,
      robust_score: null,
      accepted: false,
      acceptanceReasons: [],
    }))
  }, [rows, language, query.data?.combo_groups])
  const chartRows = [...rows].sort((left, right) => Number(left.window_id) - Number(right.window_id))
  const windowLabel = (windowId: number | string) => wf(`視窗 ${windowId}`, `Window ${windowId}`)
  const windowLabels = chartRows.map((row) => windowLabel(row.window_id))
  const passedReviewCount = groupedCombos.filter((group) => group.accepted).length
  const mostSelectedCombo = groupedCombos.reduce<typeof groupedCombos[number] | undefined>((best, group) => {
    if (!best) return group
    const bestCount = best.selectedWindowCount ?? best.rows.length
    const groupCount = group.selectedWindowCount ?? group.rows.length
    return groupCount > bestCount ? group : best
  }, undefined)
  const isKey = selectedMetricFamily === 'sharpe' ? 'is_sharpe' : 'is_calmar'
  const oosKey = selectedMetricFamily === 'sharpe' ? 'oos_sharpe' : 'oos_calmar'
  const metricLabelEn = selectedMetricFamily === 'sharpe' ? 'Sharpe' : 'Calmar'
  const metricLabel = selectedMetricFamily === 'sharpe' ? wf('夏普率(Sharpe)', 'Sharpe') : wf('卡瑪比率(Calmar)', 'Calmar')
  const isLabel = wf('樣本內(IS)', 'IS')
  const oosLabel = wf('樣本外(OOS)', 'OOS')
  const isOosRatioLabel = wf('樣本外/樣本內(OOS / IS)', 'OOS / IS')
  const averageIsMetricLabel = wf(`平均樣本內(IS) ${metricLabel}`, `Average IS ${metricLabelEn}`)
  const averageOosMetricLabel = wf(`平均樣本外(OOS) ${metricLabel}`, `Average OOS ${metricLabelEn}`)
  const isMetricLabel = wf(`樣本內(IS) ${metricLabel}`, `IS ${metricLabelEn}`)
  const oosMetricLabel = wf(`樣本外(OOS) ${metricLabel}`, `OOS ${metricLabelEn}`)
  const metricHelpBody = selectedMetricFamily === 'sharpe'
    ? wf(
      '夏普率(Sharpe)是報酬相對波動的比率。前向分析(WFA)會分開顯示樣本內(IS，用來選參數)與樣本外(OOS，用來驗證)的夏普率；樣本外(OOS)才是防止過度配適時最需要看的部分。',
      'Sharpe is return relative to volatility. WFA separates IS, the in-sample period used to select parameters, from OOS, the out-of-sample period used to validate them; OOS is the more important anti-overfit check.',
    )
    : wf(
      '卡瑪比率(Calmar)是年化報酬除以最大回撤絕對值。前向分析(WFA)會分開顯示樣本內(IS，用來選參數)與樣本外(OOS，用來驗證)的卡瑪比率；樣本外(OOS)才是防止過度配適時最需要看的部分。',
      'Calmar is CAGR divided by absolute maximum drawdown. WFA separates IS, the in-sample period used to select parameters, from OOS, the out-of-sample period used to validate them; OOS is the more important anti-overfit check.',
    )
  const oosPositiveHelpBody = wf(
    '樣本外(OOS) > 0 是被選中視窗中，樣本外指標為正數的比例。它不是盈利保證，只是檢查表現是否集中在少數視窗。',
    'OOS > 0 is the share of selected windows with a positive out-of-sample metric. It is not a profit guarantee; it checks whether performance is concentrated in only a few windows.',
  )
  const metricLabelWithHelp = (label: string, body = metricHelpBody) => (
    <span className="metric-label-with-help">
      <span>{label}</span>
      <InfoHint label={label} body={body} />
    </span>
  )
  const wfaFieldHelp = (key: string) => {
    const copy: Record<string, string> = {
      oos_window_quality: wf('被選中視窗的樣本外(OOS)指標整體品質。這是穩健性線索，不是獲利承諾。', 'Overall quality of the selected windows by OOS metric. This is robustness context, not a profit claim.'),
      positive_oos_share: wf('樣本外(OOS)指標大於 0 的被選中視窗比例。比例高代表結果較不集中在少數視窗，但仍要看平均、最差視窗和參數穩定度。', 'Share of selected windows whose OOS metric is above 0. Higher is better distributed, but still read it with average, worst window, and parameter stability.'),
      median_oos: wf('被選中視窗樣本外(OOS)指標的中位數，比平均值較不容易被單一極端視窗扭曲。', 'Median OOS metric across selected windows; less sensitive to one extreme window than the average.'),
      worst_oos: wf('被選中視窗中最差的樣本外(OOS)指標，用來檢查弱視窗或市場狀態轉變(regime shift)。', 'Worst OOS metric among selected windows; use it to spot weak windows or regime shifts.'),
      average_oos_is: wf('平均樣本外(OOS)指標除以平均樣本內(IS)指標。過低可能代表過度擬合；過高也可能代表樣本或市場狀態(regime)不穩。', 'Average OOS divided by average IS. Low values may suggest overfit; very high values can also signal unstable samples or regimes.'),
      average_is: wf('樣本內(IS)是用來選參數的樣本內表現。它可以解釋選擇原因，但不能單獨證明策略有效。', 'IS is the in-sample performance used to select parameters. It explains selection, but does not prove the strategy by itself.'),
      average_oos: wf('樣本外(OOS)是參數選出後在樣本外視窗的表現，是閱讀前向分析(WFA)時最重要的驗證指標。', 'OOS is the out-of-sample performance after parameters are selected. It is the key validation metric in WFA.'),
      parameter_stability: wf('最常被選中的參數組佔所有視窗的比例。比例高代表參數選擇較集中，但仍要確認樣本外(OOS)表現。', 'Share of windows covered by the most selected parameter set. Higher concentration suggests stability, but still check OOS performance.'),
      selected_windows: wf('前向分析(WFA)中實際有「被選中最佳組合(selected optimum)」或固定策略驗證結果的視窗數。不是月份數，也不是交易次數。', 'Number of WFA windows with selected-optimum or fixed-policy validation rows. It is not months or trades.'),
      unique_sets: wf('不同的參數組或固定策略組數。數量太多可能代表參數不穩。', 'Number of distinct parameter or policy sets. Too many can indicate unstable selection.'),
      most_selected_set: wf('最多視窗選中的參數組。它代表穩定性線索，不代表一定是最佳或可實盤。', 'The set selected by the most windows. It is a stability clue, not proof that it is best or tradable.'),
      passing_sets: wf('逐個參數組按目前前向分析(WFA)的「被選中最佳組合(selected-optimum)」門檻統計的通過數量，不等於整份前向分析(WFA)通過。', 'Count of parameter sets passing the current selected-optimum gates. It is not the overall WFA pass verdict.'),
      timeline: wf('逐視窗比較樣本內(IS)選參數表現與樣本外(OOS)驗證表現。重點是樣本外(OOS)是否穩定，而不是樣本內(IS)是否漂亮。', 'Compares IS selection performance and OOS validation performance window by window. The focus is OOS stability, not attractive IS numbers.'),
      parameter_drift: wf('顯示每個視窗選出的參數如何變動。大幅漂移可能代表策略對資料區間敏感。', 'Shows how selected parameters change across windows. Large drift can mean the strategy is sensitive to the sample period.'),
      oos_allocation: wf('每個樣本外(OOS)視窗內的平均資產權重。權重是持倉狀態，不等於該資產貢獻。', 'Average asset weights inside each OOS window. Weight is exposure state, not the asset contribution.'),
      asset_contribution: wf('資產對樣本外(OOS)結果的回報貢獻。正權重資產也可以有負貢獻。', 'Asset return contribution to OOS results. An asset with positive weight can still contribute negatively.'),
      asset: wf('資料供應商中的資產代號(symbol)。跨供應商比較前要確認代號定義一致。', 'Asset symbol from the data provider. Confirm symbol meaning before comparing across providers.'),
      avg_weight: wf('資產在視窗內的平均權重。高平均權重可能仍然伴隨負貢獻。', 'Average asset weight in the window. High average weight can still have negative contribution.'),
      active_windows: wf('該資產在多少個前向分析(WFA)視窗出現。這是穩定性線索，不是報酬證明。', 'Number of WFA windows where the asset appears. This is stability context, not return proof.'),
      contribution: wf('資產或視窗對總回報的貢獻。它可能受權重、時點和成本共同影響。', 'Contribution to total return by asset or window. It can be affected by weights, timing, and costs.'),
      top_allocation: wf('該視窗權重最大的幾個資產，用來快速看持倉集中度。', 'Largest weights in the window, useful for checking concentration quickly.'),
      top_contribution: wf('該視窗貢獻最大的幾個資產，用來分辨回報來源。', 'Largest contribution sources in the window, useful for separating return drivers.'),
      avg_exposure: wf('樣本外(OOS)視窗內平均曝險。可能包含多空或總曝險(gross exposure)效果。', 'Average exposure inside the OOS window. It may include long/short or gross-exposure effects.'),
      active_rebalances: wf('視窗內實際有持倉變化的再平衡次數。次數不代表換手率(turnover)大小。', 'Number of active rebalances in the window. Count does not show turnover size.'),
      risk_gates: wf('風控門檻觸發次數。觸發不一定自動淘汰，但需要看門檻(gate)設定和上下文。', 'Number of risk-gate events. Events do not automatically reject a run, but require reading the gate settings and context.'),
      family: wf('把相似參數組歸在一起的群組。群組只是閱讀輔助，不是獨立驗證。', 'Group of similar parameter sets. It is a reading aid, not independent validation.'),
      representative_set: wf('此族群的代表參數組。它未必是唯一最佳，只是用來代表該區域。', 'Representative set for the family. It may not be the single best row; it represents the region.'),
      avg_oos_sharpe: wf('族群或參數組在被選中視窗的平均樣本外(OOS)夏普率(Sharpe)。平均值會遮住弱視窗。', 'Average OOS Sharpe for a family or set across selected windows. Averages can hide weak windows.'),
      avg_oos_calmar: wf('族群或參數組在被選中視窗的平均樣本外(OOS)卡瑪比率(Calmar)，對最大回撤估算較敏感。', 'Average OOS Calmar for a family or set across selected windows. It is sensitive to drawdown estimation.'),
      semantic_set: wf('實際被選中的參數或策略語義組合。讀結論時應回到這個規則，而不是只看列 ID(row id)。', 'Actual selected parameter or strategy semantic combination. Interpret conclusions through this rule, not just row ids.'),
      selection_evidence: wf('此組合為何被選中或納入前向分析候選包(WFA pack)。它是來源說明，不等於通過。', 'Why this set was selected or included in the WFA pack. It explains provenance, not acceptance.'),
      robust_score: wf('穩健性綜合分數。公式受設定影響，應與樣本外(OOS)指標和原因一起看。', 'Composite robustness score. The formula depends on configuration, so read it with OOS metrics and reasons.'),
      windows_action: wf('展開後查看每個視窗的樣本內/樣本外(IS/OOS)日期、指標和連結回測。', 'Expand to inspect each window’s IS/OOS dates, metrics, and linked backtest.'),
      window: wf('前向分析(WFA)視窗編號。它是滾動切分序號，不是自然月。', 'WFA window number. It is a rolling split index, not a calendar month.'),
      train: wf('樣本內(IS)訓練/尋優期間，只應用來選參數，不應包含樣本外(OOS)證據。', 'IS training/search period. It selects parameters and should not include OOS evidence.'),
      test: wf('樣本外(OOS)驗證期間，是檢查參數是否泛化的主要證據。', 'OOS validation period. This is the main evidence for whether parameters generalize.'),
      full_backtest: wf('打開該視窗選中參數的完整回測詳情。連結是證據入口，不是額外驗證。', 'Open the full backtest for the selected window policy. The link is an evidence pointer, not extra validation.'),
    }
    return copy[key] || wf('這個欄位是前向分析(WFA)證據的一部分，請與視窗、參數、樣本外(OOS)和成本一起閱讀。', 'This field is part of WFA evidence; read it together with windows, parameters, OOS, and costs.')
  }
  const wfaLabelWithHelp = (label: string, key: string, side: 'left' | 'right' = 'right') => (
    <span className="metric-label-with-help">
      <span>{label}</span>
      <InfoHint side={side} label={label} body={wfaFieldHelp(key)} />
    </span>
  )
  const oosValues = chartRows.map((row) => row[oosKey])
  const isValues = chartRows.map((row) => row[isKey])
  const meanOos = mean(oosValues)
  const medianOos = median(oosValues)
  const worstOos = oosValues.filter((value): value is number => typeof value === 'number' && Number.isFinite(value)).reduce<number | null>((worst, value) => worst === null || value < worst ? value : worst, null)
  const positiveOosCount = oosValues.filter((value) => typeof value === 'number' && Number.isFinite(value) && value > 0).length
  const positiveOosRatio = chartRows.length ? positiveOosCount / chartRows.length : null
  const totalRiskGateEvents = chartRows.reduce((total, row) => total + (Number(row.oos_risk_gate_event_count || 0) || 0), 0)
  const hasLinkedBacktests = rows.some((row) => row.linked_backtest?.run_id && row.linked_backtest?.backtest_id)
  const meanIs = mean(isValues)
  const meanOosIsRatio = ratio(meanOos, meanIs)
  const mostSelectedShare = chartRows.length && mostSelectedCombo ? (mostSelectedCombo.selectedWindowCount ?? mostSelectedCombo.rows.length) / chartRows.length : null
  const hasPositiveMeanOos = meanOos !== null && meanOos > 0
  const passesOosWindowShare = positiveOosRatio !== null && positiveOosRatio >= 0.7
  const passesParameterStability = mostSelectedShare !== null && mostSelectedShare >= 0.5
  const passesVerdict = !isReadmeDemoPayload && hasPositiveMeanOos && passesOosWindowShare && passesParameterStability
  const verdictReasons = [
    hasPositiveMeanOos
      ? wf(`${averageOosMetricLabel} 為正數（${formatMetric(meanOos)}）。`, `${averageOosMetricLabel} is positive (${formatMetric(meanOos)}).`)
      : wf(`${averageOosMetricLabel} 不是正數或缺少資料。`, `${averageOosMetricLabel} is not positive or is missing.`),
    positiveOosRatio !== null
      ? wf(`${formatPercent(positiveOosRatio)} 被選中視窗的樣本外(OOS) ${metricLabel} 為正數。`, `${formatPercent(positiveOosRatio)} of selected windows have positive OOS ${metricLabelEn}.`)
      : wf('無法計算樣本外(OOS)正數視窗比例。', 'Unable to calculate the share of positive OOS windows.'),
    mostSelectedShare !== null
      ? wf(`最多次被選中的參數組合出現在 ${formatPercent(mostSelectedShare)} 視窗。`, `The most selected parameter set appears in ${formatPercent(mostSelectedShare)} of windows.`)
      : wf('無法計算參數組合集中度。', 'Unable to calculate parameter-set concentration.'),
    groupedCombos.length > 1
      ? wf(`共有 ${groupedCombos.length} 組精確參數被選中，需要檢視參數穩定度。`, `${groupedCombos.length} exact parameter sets were selected; review parameter stability.`)
      : wf('同一組精確參數支配所有被選中視窗。', 'One exact parameter set dominates all selected windows.'),
  ]
  const verdictBlockers = [
    meanOos === null
      ? wf(`${averageOosMetricLabel} 缺少資料。`, `${averageOosMetricLabel} is missing.`)
      : meanOos <= 0
        ? wf(`${averageOosMetricLabel} 為 ${formatMetric(meanOos)}，未高於 0。`, `${averageOosMetricLabel} is ${formatMetric(meanOos)}, not above 0.`)
        : null,
    positiveOosRatio === null
      ? wf('缺少樣本外(OOS)正數視窗比例。', 'Positive OOS window share is missing.')
      : positiveOosRatio < 0.7
        ? wf(`樣本外(OOS)正數視窗比例 ${formatPercent(positiveOosRatio)}，低於 70% 門檻。`, `Positive OOS window share is ${formatPercent(positiveOosRatio)}, below the 70% threshold.`)
        : null,
    mostSelectedShare === null
      ? wf('缺少參數穩定度。', 'Parameter stability is missing.')
      : mostSelectedShare < 0.5
        ? wf(`參數穩定度 ${formatPercent(mostSelectedShare)}，低於 50% 門檻。`, `Parameter stability is ${formatPercent(mostSelectedShare)}, below the 50% threshold.`)
        : null,
  ].filter((item): item is string => Boolean(item))
  const verdictStatus = isReadmeDemoPayload
    ? wf('示範：未驗證', 'Demo: not validated')
    : passesVerdict
    ? wf('通過', 'Pass')
    : hasPositiveMeanOos
      ? wf('需檢視', 'Needs Review')
      : wf('未通過', 'Fail')
  const verdictReasonSummary = isReadmeDemoPayload
    ? wf(
        'README 合成示範只用來展示前向分析(WFA)版面與閱讀流程；不是正式通過、不代表可交易。',
        'This README synthetic demo only shows the WFA layout and reading workflow; it is not a formal pass and does not imply tradability.',
      )
    : passesVerdict
    ? wf(
        `通過：平均樣本外(OOS) ${metricLabel} 為正，樣本外(OOS)正數視窗比例 ${formatPercent(positiveOosRatio)}，參數穩定度 ${formatPercent(mostSelectedShare)}。`,
        `Pass: average OOS ${metricLabelEn} is positive, positive OOS window share is ${formatPercent(positiveOosRatio)}, and parameter stability is ${formatPercent(mostSelectedShare)}.`,
      )
    : hasPositiveMeanOos
      ? wf(`需檢視：${verdictBlockers.join(' ')}`, `Needs Review: ${verdictBlockers.join(' ')}`)
      : wf(`未通過：${verdictBlockers.join(' ')}`, `Fail: ${verdictBlockers.join(' ')}`)
  const driftParamKeys = Array.from(
    new Set(
      chartRows.flatMap((row) =>
        Object.entries(row.semantic_combo || {})
          .filter(([, value]) => typeof value === 'number' && Number.isFinite(value))
          .map(([key]) => key),
      ),
    ),
  ).sort()

  useEffect(() => {
    if (!runsQuery.data?.length) return
    const availableRunIds = runsQuery.data.map((run: any) => run.run_id)
    if (!runId || !availableRunIds.includes(runId)) {
      const fallbackRunId = runsQuery.data[0].run_id
      setSelectedWfaRunId(fallbackRunId)
      if (search.runId === fallbackRunId) return
      navigate({ to: '/wfa', search: { runId: fallbackRunId }, replace: true })
    }
  }, [navigate, runId, runsQuery.data, search.runId, setSelectedWfaRunId])

  useEffect(() => {
    if (runId && runId !== selectedWfaRunId) {
      setSelectedWfaRunId(runId)
    }
  }, [runId, selectedWfaRunId, setSelectedWfaRunId])

  useEffect(() => {
    if (!query.error) return
    const fallbackRunId = runsQuery.data?.[0]?.run_id || ''
    if (fallbackRunId && fallbackRunId !== runId) {
      setSelectedWfaRunId(fallbackRunId)
      if (search.runId === fallbackRunId) return
      navigate({ to: '/wfa', search: { runId: fallbackRunId }, replace: true })
    }
  }, [navigate, query.error, runId, runsQuery.data, search.runId, setSelectedWfaRunId])

  if (runsQuery.isLoading || (!hasResolvedRun && runsQuery.data?.length) || query.isLoading) {
    return <div className="page-loading">{t('common.loading.wfa')}</div>
  }
  if (runsQuery.data && runsQuery.data.length === 0) {
    return <MissingState message={t('wfa.noOutputs')} />
  }
  if (runId && runsQuery.data?.length && !hasResolvedRun) {
    return <MissingState message={t('wfa.missingRun')} />
  }
  if (!runId) {
    return <MissingState message={t('wfa.noManagedRun')} />
  }
  if (query.error || !query.data) {
    return <div className="page-error">{wf('無法載入前向分析 (WFA) 資料。', 'Unable to load WFA data.')}</div>
  }

  const toggleExpanded = (comboKey: string) => {
    setExpandedComboKeys((current) =>
      current.includes(comboKey)
        ? current.filter((item) => item !== comboKey)
        : [...current, comboKey],
    )
  }

  return (
    <div className="page-stack">
      <SectionCard
        title={isRollingValidation ? t('wfa.rollingValidationTitle') : t('wfa.title')}
        subtitle={
          isRollingValidation
            ? wf('每個視窗會把固定策略放到對應的樣本外(OOS)區間驗證，不會在樣本內(IS)區間重新尋找參數。', 'Each window validates the fixed strategy on its matching OOS period without re-optimizing parameters in IS.')
            : wf('每個視窗先在樣本內(IS)區間尋找最佳參數，再把該視窗選出的參數放到對應樣本外(OOS)區間測試。', 'Each window first finds the best parameters in IS, then tests that selected set on the matching OOS period.')
        }
      >
        <div className="selector-shell">
          <div className="selector-shell-title">{wf('前向分析檔案選取', 'WFA File Selection')}</div>
          <div className="selector-shell-subtitle">
            {isRollingValidation
              ? wf('在此選取由執行中心產生的固定策略視窗驗證結果。', 'Select fixed-strategy window validation results produced by Run Center.')
              : wf('在此選取由執行中心產生的前向分析 (WFA) 結果。', 'Select Walk-Forward Analysis results produced by Run Center.')}
          </div>
          <div className="wfa-selector-grid">
            <div className="wfa-selector-controls">
              <CustomSelect
                className="wfa-run-select"
                value={runId}
                options={(runsQuery.data || []).map((run: any) => ({
                  value: run.run_id,
                  label: displayWfaRunLabel(run, language),
                }))}
                onChange={(nextRunId) => navigate({ to: '/wfa', search: { runId: nextRunId } })}
              />
              <div className="control-row">
                <select
                  className="text-input"
                  value={selectedMetricFamily}
                  onChange={(event) => setSelectedMetricFamily(event.target.value as 'sharpe' | 'calmar')}
                >
                  <option value="sharpe">{wf('夏普率(Sharpe)', 'Sharpe')}</option>
                  <option value="calmar">{wf('卡瑪比率(Calmar)', 'Calmar')}</option>
                </select>
              </div>
            </div>
            <StrategyRulesPanel summary={strategySummary} loading={query.isLoading} className="wfa-strategy-summary" />
          </div>
        </div>
        {legacyGridDetected ? (
          <div className="research-feedback-banner" style={{ marginBottom: '0.75rem' }}>
            {wf('此檔案包含網格診斷列，並不是嚴格的一視窗一組最佳參數的前向分析 (WFA) 輸出。可查看候選診斷，但不應作為正式通過 / 不通過依據。', 'This file includes grid diagnostic rows rather than strict one-best-parameter-set-per-window WFA output. You can inspect candidate diagnostics, but should not use them as the formal pass/fail basis.')}
          </div>
        ) : null}
        <div className="wfa-decision-row">
          <div className={`shortlist-summary-card wfa-verdict-card ${passesVerdict ? 'is-pass' : hasPositiveMeanOos ? 'is-review' : 'is-fail'}`}>
            <div className="summary-chip-label">
              {wf('前向分析 (WFA) 判定', 'WFA Verdict')}
              <InfoHint
                label={wf('前向分析 (WFA) 判定', 'WFA Verdict')}
                body={
                  isRollingValidation
                    ? wf('此判斷只使用固定策略的視窗驗證列，檢查策略在不同樣本外(OOS)視窗是否保持正向表現。通過門檻：平均樣本外(OOS) > 0、樣本外(OOS)正數視窗比例 >= 70%、參數穩定度 >= 50%。', 'This verdict uses only fixed-strategy validation rows to check whether the strategy stays positive across OOS windows. Pass gates: average OOS > 0, positive OOS window share >= 70%, and parameter stability >= 50%.')
                    : wf('此判斷只使用每個樣本內(IS)視窗選出的最佳參數列，檢查樣本外(OOS)表現是否為正、正向樣本外(OOS)視窗比例，以及參數是否集中在穩定組合或族群。通過門檻：平均樣本外(OOS) > 0、樣本外(OOS)正數視窗比例 >= 70%、參數穩定度 >= 50%。', 'This verdict uses the best parameter row selected by each IS window, checking positive OOS performance, the positive-window share, and whether parameters concentrate in stable sets or families. Pass gates: average OOS > 0, positive OOS window share >= 70%, and parameter stability >= 50%.')
                }
              />
            </div>
            <div className="wfa-verdict-value">{verdictStatus}</div>
            <div className="wfa-verdict-reason">
              {verdictReasonSummary}
            </div>
          </div>
          <div className="wfa-mode-side-panel">
            <div className="wfa-mode-item">
              <div className="summary-chip-label">
                {wf('視窗切分模式', 'Window Sizing')}
                <InfoHint label={wf('視窗切分模式', 'Window Sizing')} body={windowSizingBody} />
              </div>
              <strong>{windowSizingLabel}</strong>
              <span>{effectiveWindowText}</span>
            </div>
            <div className="wfa-mode-item">
              <div className="summary-chip-label">
                {wf('樣本內(IS)候選篩選', 'IS Candidate Filter')}
                <InfoHint label={wf('樣本內(IS)候選篩選', 'IS Candidate Filter')} body={selectionConstraintBody} />
              </div>
              <strong>{selectionConstraintText}</strong>
              <span>
                {selectionConstraintsEnabled
                  ? wf('先篩走低活動候選，再按樣本內(IS)指標排名。', 'Low-activity candidates are filtered before IS ranking.')
                  : isSelectionFilterUnrecorded
                    ? wf('這份舊版結果沒有記錄此欄位。', 'This legacy result did not record this field.')
                  : wf('直接按樣本內(IS)指標排名所有候選。', 'All candidates are ranked directly by IS metric.')}
              </span>
            </div>
          </div>
        </div>
        <div className="shortlist-summary-strip wfa-supporting-strip" style={{ marginBottom: '1rem' }}>
          <div className="shortlist-summary-card">
            <div className="summary-chip-label">{wfaLabelWithHelp(wf('樣本外(OOS)視窗品質', 'OOS Window Quality'), 'oos_window_quality')}</div>
            <div className="summary-chip-value">{formatPercent(positiveOosRatio)}</div>
            <div className="muted" style={{ marginTop: '0.35rem' }}>
              {wfaLabelWithHelp(wf('中位數', 'Median'), 'median_oos')} {metricLabel}: {formatMetric(medianOos)} | {wfaLabelWithHelp(wf('最差', 'Worst'), 'worst_oos')}: {formatMetric(worstOos)}
            </div>
          </div>
          <div className="shortlist-summary-card">
            <div className="summary-chip-label">{wfaLabelWithHelp(wf('平均樣本外/樣本內(OOS / IS)', 'Average OOS / IS'), 'average_oos_is')}</div>
            <div className="summary-chip-value">{formatMetric(meanOosIsRatio)}</div>
            <div className="muted" style={{ marginTop: '0.35rem' }}>
              {wfaLabelWithHelp(wf('平均樣本內(IS)', 'Average IS'), 'average_is')} {formatMetric(meanIs)} | {wfaLabelWithHelp(wf('平均樣本外(OOS)', 'Average OOS'), 'average_oos')} {formatMetric(meanOos)}
            </div>
          </div>
          <div className="shortlist-summary-card">
            <div className="summary-chip-label">{wfaLabelWithHelp(wf('參數穩定度', 'Parameter Stability'), 'parameter_stability')}</div>
            <div className="summary-chip-value">{formatPercent(mostSelectedShare)}</div>
            <div className="muted" style={{ marginTop: '0.35rem' }}>
              {wf('最常被選中參數組在所有視窗中的佔比。', 'Share of all windows covered by the most selected parameter set.')}
            </div>
          </div>
        </div>
        <div className="shortlist-summary-strip" style={{ marginBottom: '1rem' }}>
          <div className="shortlist-summary-card">
            <div className="summary-chip-label">{wfaLabelWithHelp(wf('被選中視窗', 'Selected Windows'), 'selected_windows')}</div>
            <div className="summary-chip-value">{chartRows.length}</div>
          </div>
          <div className="shortlist-summary-card">
            <div className="summary-chip-label">{wfaLabelWithHelp(isRollingValidation ? wf('已驗證策略', 'Validated Strategies') : wf('獨立參數組', 'Unique Parameter Sets'), 'unique_sets')}</div>
            <div className="summary-chip-value">{groupedCombos.length}</div>
          </div>
          <div className="shortlist-summary-card">
            <div className="summary-chip-label">{wfaLabelWithHelp(wf('最多次被選中的組合', 'Most Selected Set'), 'most_selected_set')}</div>
            <div className="summary-chip-value">{mostSelectedCombo?.comboLabel || '-'}</div>
            <div className="muted" style={{ marginTop: '0.35rem' }}>
              {mostSelectedCombo ? wf(`${mostSelectedCombo.selectedWindowCount ?? mostSelectedCombo.rows.length} 個視窗`, `${mostSelectedCombo.selectedWindowCount ?? mostSelectedCombo.rows.length} windows`) : '-'}
            </div>
          </div>
          <div className="shortlist-summary-card">
            <div className="summary-chip-label">
              {wf('通過參數組', 'Passing Parameter Sets')}
              <InfoHint
                label={wf('通過參數組', 'Passing Parameter Sets')}
                body={wf('這是逐個參數組的通過數量，不是整份前向分析(WFA)的總判定。每組參數會按目前「被選中最佳組合(selected-optimum)」門檻檢查；上方「前向分析(WFA)判定」則會再整合平均樣本外(OOS)、樣本外(OOS)正數視窗比例與參數穩定度。', 'This counts parameter sets that pass their own gates; it is not the overall WFA verdict. Each set is checked against the current WFA selected-optimum gates, while the top WFA Verdict combines average OOS, positive OOS window share, and parameter stability.')}
              />
            </div>
            <div className="summary-chip-value">{passedReviewCount}</div>
            <div className="muted" style={{ marginTop: '0.35rem' }}>
              {wf('逐個參數組計算；不等於整份前向分析(WFA)已通過。', 'Calculated per parameter set; this does not mean the full WFA run has passed.')}
            </div>
          </div>
        </div>
        <div className="section-subheading">
          {isRollingValidation ? wf('視窗驗證時間線', 'Window Validation Timeline') : wf('視窗最佳參數時間線', 'Window Best-Parameter Timeline')}
          <InfoHint label={wf('樣本內/樣本外(IS/OOS)指標', 'IS / OOS Metrics')} body={`${wfaFieldHelp('timeline')}\n\n${metricHelpBody}`} />
        </div>
        <Plot
          data={[
            {
              type: 'bar',
              name: `${isLabel} ${metricLabel}`,
              x: windowLabels,
              y: chartRows.map((row) => row[isKey] ?? null),
              customdata: chartRows.map((row) => [
                row.window_id,
                row.train_start_date || '-',
                row.train_end_date || '-',
                row.test_start_date || '-',
                row.test_end_date || '-',
                comboLabel(row.semantic_combo || {}, language),
                row[isKey],
                row[oosKey],
                ratio(row[oosKey], row[isKey]),
                row.oos_total_return,
                row.oos_calmar,
                row.candidate_count,
                selectionEvidenceLabel(row.selection_evidence, language),
              ]),
              hovertemplate:
                `<b>${wf('視窗', 'Window')} %{customdata[0]}</b><br>${wf('選中組合', 'Selected Set')}: %{customdata[5]}<br>${wf('訓練區間', 'Train')}: %{customdata[1]} -> %{customdata[2]}<br>${wf('測試區間', 'Test')}: %{customdata[3]} -> %{customdata[4]}<br>${wf('依據', 'Basis')}: %{customdata[12]}<br>${wf('候選數', 'Candidates')}: %{customdata[11]}<br><br>${isLabel} ` +
                metricLabel +
                `: %{customdata[6]:.3f}<br>${oosLabel} ` +
                metricLabel +
                `: %{customdata[7]:.3f}<br>${isOosRatioLabel}: %{customdata[8]:.3f}<br>${wf('樣本外(OOS)報酬', 'OOS Return')}: %{customdata[9]:.1%}<br>${wf('樣本外(OOS)卡瑪比率(Calmar)', 'OOS Calmar')}: %{customdata[10]:.3f}<extra></extra>`,
              marker: { color: '#7e9bcc' },
            },
            {
              type: 'bar',
              name: `${oosLabel} ${metricLabel}`,
              x: windowLabels,
              y: chartRows.map((row) => row[oosKey] ?? null),
              customdata: chartRows.map((row) => [
                row.window_id,
                row.train_start_date || '-',
                row.train_end_date || '-',
                row.test_start_date || '-',
                row.test_end_date || '-',
                comboLabel(row.semantic_combo || {}, language),
                row[isKey],
                row[oosKey],
                ratio(row[oosKey], row[isKey]),
                row.oos_total_return,
                row.oos_calmar,
                row.candidate_count,
                selectionEvidenceLabel(row.selection_evidence, language),
              ]),
              hovertemplate:
                `<b>${wf('視窗', 'Window')} %{customdata[0]}</b><br>${wf('選中組合', 'Selected Set')}: %{customdata[5]}<br>${wf('訓練區間', 'Train')}: %{customdata[1]} -> %{customdata[2]}<br>${wf('測試區間', 'Test')}: %{customdata[3]} -> %{customdata[4]}<br>${wf('依據', 'Basis')}: %{customdata[12]}<br>${wf('候選數', 'Candidates')}: %{customdata[11]}<br><br>${isLabel} ` +
                metricLabel +
                `: %{customdata[6]:.3f}<br>${oosLabel} ` +
                metricLabel +
                `: %{customdata[7]:.3f}<br>${isOosRatioLabel}: %{customdata[8]:.3f}<br>${wf('樣本外(OOS)報酬', 'OOS Return')}: %{customdata[9]:.1%}<br>${wf('樣本外(OOS)卡瑪比率(Calmar)', 'OOS Calmar')}: %{customdata[10]:.3f}<extra></extra>`,
              marker: { color: '#dbac30' },
            },
          ]}
          layout={makeChartLayout({
            xTitle: wf('前向分析 (WFA) 視窗', 'WFA Window'),
            yTitle: metricLabel,
            barmode: 'group',
            xaxis: { type: 'category', categoryorder: 'array', categoryarray: windowLabels },
          })}
          config={plotConfig}
          className="plot-card"
          useResizeHandler
          style={{ width: '100%', height: '360px' }}
        />
        {driftParamKeys.length ? (
          <>
            <div className="section-subheading" style={{ marginTop: '1rem' }}>{wfaLabelWithHelp(wf('參數漂移', 'Parameter Drift'), 'parameter_drift')}</div>
            <Plot
              data={driftParamKeys.map((key, index) => ({
                type: 'scatter',
                mode: 'lines+markers',
                name: key,
                x: windowLabels,
                y: chartRows.map((row) => {
                  const value = row.semantic_combo?.[key]
                  return typeof value === 'number' && Number.isFinite(value) ? value : null
                }),
                customdata: chartRows.map((row) => [
                  row.window_id,
                  comboLabel(row.semantic_combo || {}, language),
                  row.train_start_date || '-',
                  row.train_end_date || '-',
                  row.test_start_date || '-',
                  row.test_end_date || '-',
                  row[isKey],
                  row[oosKey],
                ]),
                hovertemplate:
                  `<b>${wf('視窗', 'Window')} %{customdata[0]}</b><br>${wf('選中組合', 'Selected Set')}: %{customdata[1]}<br>${wf('訓練區間', 'Train')}: %{customdata[2]} -> %{customdata[3]}<br>${wf('測試區間', 'Test')}: %{customdata[4]} -> %{customdata[5]}<br><br>` +
                  key +
                  `: %{y}<br>${isLabel} ` +
                  metricLabel +
                  `: %{customdata[6]:.3f}<br>${oosLabel} ` +
                  metricLabel +
                  ': %{customdata[7]:.3f}<extra></extra>',
                marker: { color: ['#7e9bcc', '#dbac30', '#79b77a', '#d17878', '#a98bd8'][index % 5] },
              }))}
              layout={makeChartLayout({
                xTitle: wf('前向分析 (WFA) 視窗', 'WFA Window'),
                yTitle: wf('參數值', 'Parameter Value'),
                xaxis: { type: 'category', categoryorder: 'array', categoryarray: windowLabels },
                legend: { orientation: 'h' },
              })}
              config={plotConfig}
              className="plot-card"
              useResizeHandler
              style={{ width: '100%', height: '300px' }}
            />
          </>
        ) : null}
        {diagnosticRowCount > 0 ? (
          <details className="diagnostic-details" style={{ marginTop: '1rem' }}>
            <summary>{wf(`候選診斷（${diagnosticRowCount} 列）`, `Candidate Diagnostics (${diagnosticRowCount} rows)`)}</summary>
            <div className="muted" style={{ marginTop: '0.5rem' }}>
              {wf('候選診斷會保留網格與候選組合的證據，但不會用於正式前向分析(WFA)通過 / 不通過判斷。', 'Candidate diagnostics retain grid and candidate-set evidence, but are not used for formal WFA pass/fail decisions.')}
            </div>
          </details>
        ) : null}
      </SectionCard>

      {showPortfolioPanels ? (
        <SectionCard
          title={isRollingValidation ? t('wfa.windowPortfolioValidation') : t('wfa.windowPortfolioEvidence')}
          subtitle={t('wfa.windowPortfolioSubtitle')}
        >
          <div className="section-subheading">{wfaLabelWithHelp(wf('樣本外(OOS)視窗配置', 'OOS Window Allocation'), 'oos_allocation')}</div>
          <Plot
            data={portfolioAssets.map((asset, index) => ({
              type: 'bar',
              name: asset,
              x: portfolioAllocationWindows.map((window) => windowLabel(window.window_id)),
              y: portfolioAllocationWindows.map((window) => {
                const weight = window.weights.find((item) => item.asset === asset)
                return typeof weight?.avg_weight === 'number' ? weight.avg_weight : 0
              }),
              customdata: portfolioAllocationWindows.map((window) => {
                const weight = window.weights.find((item) => item.asset === asset)
                return [
                  window.window_id,
                  comboLabel(window.semantic_combo || {}, language),
                  window.test_start_date || '-',
                  window.test_end_date || '-',
                  weight?.avg_weight ?? null,
                  weight?.last_weight ?? null,
                  window.avg_exposure ?? null,
                  window.active_rebalance_count ?? null,
                ]
              }),
              hovertemplate:
                `<b>%{fullData.name}</b><br>${wf('視窗', 'Window')} %{customdata[0]}<br>${wf('策略', 'Strategy')}: %{customdata[1]}<br>${wf('測試區間', 'Test')}: %{customdata[2]} -> %{customdata[3]}<br>${wf('平均權重', 'Average Weight')}: %{customdata[4]:.1%}<br>${wf('最後權重', 'Last Weight')}: %{customdata[5]:.1%}<br>${wf('平均曝險', 'Average Exposure')}: %{customdata[6]:.1%}<br>${wf('有效再平衡', 'Active Rebalances')}: %{customdata[7]}<extra></extra>`,
              marker: { color: ['#7e9bcc', '#dbac30', '#79b77a', '#d17878', '#a98bd8', '#63b3b8'][index % 6] },
            }))}
            layout={makeChartLayout({
              xTitle: wf('前向分析 (WFA) 視窗', 'WFA Window'),
              yTitle: wf('平均樣本外(OOS)權重', 'Average OOS Weight'),
              barmode: 'stack',
              xaxis: { type: 'category' },
              yaxis: { tickformat: '.0%' },
              legend: { orientation: 'h' },
            })}
            config={plotConfig}
            className="plot-card"
            useResizeHandler
            style={{ width: '100%', height: '340px' }}
          />
          <div className="section-subheading" style={{ marginTop: '1rem' }}>{wfaLabelWithHelp(wf('樣本外(OOS)資產貢獻', 'OOS Asset Contribution'), 'asset_contribution')}</div>
          <div className="grid-two">
            <Plot
              data={[
                {
                  type: 'bar',
                  name: wf('報酬貢獻', 'Return Contribution'),
                  x: portfolioAssetSummary.map((item) => item.asset),
                  y: portfolioAssetSummary.map((item) => item.return_contribution),
                  customdata: portfolioAssetSummary.map((item) => [
                    item.asset,
                    item.mean_avg_weight ?? null,
                    item.active_windows ?? null,
                    item.return_contribution ?? null,
                  ]),
                  hovertemplate:
                    `<b>%{customdata[0]}</b><br>${wf('報酬貢獻', 'Return Contribution')}: %{customdata[3]:.2%}<br>${wf('平均權重', 'Average Weight')}: %{customdata[1]:.1%}<br>${wf('有效視窗', 'Active Windows')}: %{customdata[2]}<extra></extra>`,
                  marker: { color: '#dbac30' },
                },
              ]}
              layout={makeChartLayout({
                xTitle: wf('資產', 'Asset'),
                yTitle: wf('報酬貢獻', 'Return Contribution'),
                yaxis: { tickformat: '.0%' },
              })}
              config={plotConfig}
              className="plot-card"
              useResizeHandler
              style={{ width: '100%', height: '300px' }}
            />
            <div className="data-table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>{wfaLabelWithHelp(wf('資產', 'Asset'), 'asset')}</th>
                    <th>{wfaLabelWithHelp(wf('平均權重', 'Average Weight'), 'avg_weight')}</th>
                    <th>{wfaLabelWithHelp(wf('活躍視窗', 'Active Windows'), 'active_windows')}</th>
                    <th>{wfaLabelWithHelp(wf('貢獻', 'Contribution'), 'contribution')}</th>
                  </tr>
                </thead>
                <tbody>
                  {portfolioAssetSummary.map((item) => (
                    <tr key={item.asset}>
                      <td>{item.asset}</td>
                      <td>{formatPercentPrecise(item.mean_avg_weight)}</td>
                      <td>{item.active_windows ?? '-'}</td>
                      <td>{formatPercentPrecise(item.return_contribution)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
          {portfolioContributionWindows.length ? (
            <div className="data-table-wrap" style={{ marginTop: '1rem' }}>
              <table className="data-table">
                <thead>
                  <tr>
                    <th>{wfaLabelWithHelp(wf('視窗', 'Window'), 'window')}</th>
                    <th>{wf('策略', 'Strategy')}</th>
                    <th>{wfaLabelWithHelp(wf('主要配置', 'Top Allocation'), 'top_allocation')}</th>
                    <th>{wfaLabelWithHelp(wf('主要貢獻', 'Top Contribution'), 'top_contribution')}</th>
                    <th>{wfaLabelWithHelp(wf('平均曝險', 'Average Exposure'), 'avg_exposure')}</th>
                    <th>{wfaLabelWithHelp(wf('活躍再平衡', 'Active Rebalances'), 'active_rebalances')}</th>
                    <th>{wfaLabelWithHelp(wf('風控門檻', 'Risk Gates'), 'risk_gates')}</th>
                  </tr>
                </thead>
                <tbody>
                  {portfolioAllocationWindows.map((window) => {
                    const contributionWindow = portfolioContributionWindows.find((item) => item.window_id === window.window_id)
                    const topWeights = [...(window.weights || [])]
                      .sort((left, right) => Math.abs(Number(right.avg_weight || 0)) - Math.abs(Number(left.avg_weight || 0)))
                      .slice(0, 3)
                      .map((item) => `${item.asset} ${formatPercentPrecise(item.avg_weight)}`)
                      .join(' | ')
                    const topContributions = [...(contributionWindow?.contributions || [])]
                      .sort((left, right) => Math.abs(Number(right.return_contribution || 0)) - Math.abs(Number(left.return_contribution || 0)))
                      .slice(0, 3)
                      .map((item) => `${item.asset} ${formatPercentPrecise(item.return_contribution)}`)
                      .join(' | ')
                    return (
                      <tr key={window.window_id}>
                        <td>{window.window_id}</td>
                        <td><span className="mosaic-private-text" data-private-strategy>{comboLabel(window.semantic_combo || {}, language)}</span></td>
                        <td>{topWeights || '-'}</td>
                        <td>{topContributions || '-'}</td>
                        <td>{formatPercentPrecise(window.avg_exposure)}</td>
                        <td>{window.active_rebalance_count ?? '-'}</td>
                        <td>{window.risk_gate_event_count ?? 0}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          ) : null}
        </SectionCard>
      ) : null}

      {Array.isArray(query.data?.cluster_summary?.clusters) && query.data.cluster_summary.clusters.length ? (
        <SectionCard title={t('wfa.parameterFamilySummary')} subtitle={t('wfa.parameterFamilySubtitle')}>
          <div className="data-table-wrap family-summary-wrap">
            <table className="data-table family-summary-table">
              <thead>
                <tr>
                  <th>{wfaLabelWithHelp(t('wfa.family'), 'family')}</th>
                  <th>{wfaLabelWithHelp(wf('獨立參數組', 'Unique Sets'), 'unique_sets')}</th>
                  <th>{wfaLabelWithHelp(t('wfa.selectedWindows'), 'selected_windows')}</th>
                  <th>{wfaLabelWithHelp(wf('代表組合', 'Representative Set'), 'representative_set')}</th>
                  <th>{wfaLabelWithHelp(wf('平均樣本外(OOS)夏普率(Sharpe)', 'Average OOS Sharpe'), 'avg_oos_sharpe')}</th>
                  <th>{wfaLabelWithHelp(wf('平均樣本外(OOS)卡瑪比率(Calmar)', 'Average OOS Calmar'), 'avg_oos_calmar')}</th>
                  <th>{wfaLabelWithHelp(isOosRatioLabel, 'average_oos_is')}</th>
                  <th>
                    {wf('穩定度', 'Stability')}
                    <InfoHint
                      side="left"
                      label={wf('族群穩定度', 'Family Stability')}
                      body={wf('穩定度是同一族群內參數組平均樣本外(OOS)指標的標準差。數值越低，代表族群成員表現越接近；只有一組參數時會顯示 0.000。', 'Stability is the standard deviation of mean OOS metrics within the same parameter family. Lower values mean family members behave more similarly; a single parameter set displays 0.000.')}
                    />
                  </th>
                </tr>
              </thead>
              <tbody>
                {(query.data.cluster_summary.clusters || []).map((cluster: any) => (
                  <tr key={cluster.cluster_id}>
                    <td>{cluster.cluster_id}</td>
                    <td>{cluster.unique_set_count ?? cluster.size}</td>
                    <td>{cluster.selected_window_count ?? '-'}</td>
                    <td><span className="mosaic-private-text" data-private-strategy>{comboLabel(cluster.representative_params || {}, language)}</span></td>
                    <td>{cluster.mean_oos_sharpe?.toFixed?.(3) ?? '-'}</td>
                    <td>{cluster.mean_oos_calmar?.toFixed?.(3) ?? '-'}</td>
                    <td>{cluster.mean_oos_is_ratio?.toFixed?.(3) ?? '-'}</td>
                    <td>{cluster.stability_std?.toFixed?.(3) ?? '-'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </SectionCard>
      ) : null}

      <SectionCard title={t('wfa.isParameterSets')} subtitle={t('wfa.isParameterSetsSubtitle')}>
        <div className="data-table-wrap wfa-table-wrap">
          <table className="data-table wfa-table">
            <thead>
                <tr>
                  <th>{wfaLabelWithHelp(wf('語義組合', 'Semantic Set'), 'semantic_set')}</th>
                  <th>{wfaLabelWithHelp(t('wfa.family'), 'family')}</th>
                  <th>{wfaLabelWithHelp(t('wfa.selectionEvidence'), 'selection_evidence')}</th>
                  <th>{wfaLabelWithHelp(t('wfa.selectedWindows'), 'selected_windows')}</th>
                  <th>{metricLabelWithHelp(averageIsMetricLabel)}</th>
                  <th>{metricLabelWithHelp(averageOosMetricLabel)}</th>
                  <th>{metricLabelWithHelp(wf('樣本外(OOS) > 0', 'OOS > 0'), oosPositiveHelpBody)}</th>
                  <th>{wfaLabelWithHelp(t('wfa.worstOos'), 'worst_oos')}</th>
                  {totalRiskGateEvents > 0 ? <th>{wfaLabelWithHelp(t('wfa.riskGates'), 'risk_gates')}</th> : null}
                  <th>{wfaLabelWithHelp(t('wfa.robustScore'), 'robust_score')}</th>
                  <th>
                    {t('wfa.reviewResult')}
                    <InfoHint
                      side="left"
                      label={t('wfa.reviewResult')}
                      body={wf('通過代表該組被選參數符合目前前向分析(WFA)判定門檻；需檢視代表至少一個門檻需要人工判讀。候選診斷不會用於此判斷。', 'Pass means the selected parameter set meets the current WFA verdict gates; Needs Review means at least one gate needs manual interpretation. Candidate diagnostics are not used for this verdict.')}
                    />
                  </th>
                  <th>{wfaLabelWithHelp(t('wfa.windows'), 'windows_action', 'left')}</th>
              </tr>
            </thead>
            <tbody>
                {groupedCombos.map((group) => (
                  <Fragment key={group.comboKey}>
                    <tr key={group.comboKey}>
                      <td>
                        <div className="mosaic-private-text" data-private-strategy>{group.comboLabel}</div>
                      </td>
                      <td>{group.clusterId !== null && group.clusterId !== undefined ? group.clusterId : '-'}</td>
                      <td>{selectionEvidenceLabel(group.inclusionReason, language)}</td>
                      <td>{group.selectedWindowCount ?? group.rows.length}</td>
                      <td>{(selectedMetricFamily === 'sharpe' ? group.avg_is_sharpe : group.avg_is_calmar)?.toFixed?.(3) ?? '-'}</td>
                    <td>{(selectedMetricFamily === 'sharpe' ? group.avg_oos_sharpe : group.avg_oos_calmar)?.toFixed?.(3) ?? '-'}</td>
                    <td>{formatPercent(group.rows.length ? group.rows.filter((row) => {
                      const value = row[oosKey]
                      return typeof value === 'number' && Number.isFinite(value) && value > 0
                    }).length / group.rows.length : null)}</td>
                    <td>{formatMetric(group.rows.map((row) => row[oosKey]).filter((value): value is number => typeof value === 'number' && Number.isFinite(value)).reduce<number | null>((worst, value) => worst === null || value < worst ? value : worst, null))}</td>
                    {totalRiskGateEvents > 0 ? (
                      <td>{group.rows.reduce((sum, row) => sum + (Number(row.oos_risk_gate_event_count || 0) || 0), 0)}</td>
                    ) : null}
                    <td>{group.robust_score?.toFixed?.(3) ?? '-'}</td>
                    <td>
                      {group.accepted ? t('wfa.pass') : group.acceptanceReasons.length ? t('wfa.needsReview') : '-'}
                      <InfoHint
                        side="left"
                        label={group.accepted ? t('wfa.pass') : t('wfa.needsReview')}
                        body={
                          group.accepted
                            ? wf('此組參數通過目前前向分析(WFA)的「被選中最佳組合(selected-optimum)」門檻。', 'This parameter set passes the current WFA selected-optimum gates.')
                            : group.acceptanceReasons.length
                              ? group.acceptanceReasons.map((reason) => reviewReasonLabel(reason, language)).join(' ')
                              : wf('此組沒有可用的判定門檻結果。', 'This set has no available verdict-gate result.')
                        }
                      />
                    </td>
                    <td>
                      <div className="inline-actions">
                        <button
                          className="inline-action-button inline-action-button-compact"
                          onClick={() => toggleExpanded(group.comboKey)}
                        >
                          {expandedComboKeys.includes(group.comboKey) ? t('wfa.hideWindows') : t('wfa.showWindows')}
                        </button>
                      </div>
                    </td>
                  </tr>
                  {expandedComboKeys.includes(group.comboKey) ? (
                    <tr key={`${group.comboKey}-detail`} className="detail-row">
                        <td colSpan={totalRiskGateEvents > 0 ? 12 : 11}>
                        <div className="nested-table-wrap">
                          <table className="data-table nested-table">
                            <thead>
                              <tr>
                                <th>{wfaLabelWithHelp(t('wfa.window'), 'window')}</th>
                                <th>{wfaLabelWithHelp(t('wfa.train'), 'train')}</th>
                                <th>{wfaLabelWithHelp(t('wfa.test'), 'test')}</th>
                                <th>{metricLabelWithHelp(isMetricLabel)}</th>
                                <th>{metricLabelWithHelp(oosMetricLabel)}</th>
                                {totalRiskGateEvents > 0 ? <th>{wfaLabelWithHelp(t('wfa.riskGates'), 'risk_gates')}</th> : null}
                                {hasLinkedBacktests ? <th>{wfaLabelWithHelp(wf('完整回測', 'Full Backtest'), 'full_backtest', 'left')}</th> : null}
                              </tr>
                            </thead>
                            <tbody>
                              {group.rows.map((row, index) => (
                                <tr key={`${group.comboKey}-${row.window_id}-${index}`}>
                                  <td>{row.window_id}</td>
                                  <td>{row.train_start_date}{' -> '}{row.train_end_date}</td>
                                  <td>{row.test_start_date}{' -> '}{row.test_end_date}</td>
                                  <td>{(selectedMetricFamily === 'sharpe' ? row.is_sharpe : row.is_calmar)?.toFixed?.(3) ?? '-'}</td>
                                  <td>{(selectedMetricFamily === 'sharpe' ? row.oos_sharpe : row.oos_calmar)?.toFixed?.(3) ?? '-'}</td>
                                  {totalRiskGateEvents > 0 ? <td>{row.oos_risk_gate_event_count ?? 0}</td> : null}
                                  {hasLinkedBacktests ? (
                                    <td>
                                      {row.linked_backtest?.run_id && row.linked_backtest?.backtest_id ? (
                                        <Link
                                          to="/metrics/backtests"
                                          search={{
                                            runId: row.linked_backtest.run_id,
                                            backtestId: row.linked_backtest.backtest_id,
                                          }}
                                          className="inline-action"
                                        >
                                          {wf('打開回測', 'Open Backtest')}
                                        </Link>
                                      ) : (
                                        <span className="muted">{wf('未輸出', 'Not exported')}</span>
                                      )}
                                    </td>
                                  ) : null}
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </td>
                    </tr>
                  ) : null}
                </Fragment>
              ))}
            </tbody>
          </table>
        </div>
      </SectionCard>
    </div>
  )
}
