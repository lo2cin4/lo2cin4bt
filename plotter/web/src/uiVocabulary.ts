import type { Language } from './i18n'

type CopyPair = Record<Language, string>
type LabelMap = Record<string, CopyPair>

export function normalizeUiCode(value: unknown): string {
  return String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/[()]/g, '')
    .replace(/[%/]+/g, '_')
    .replace(/[\s-]+/g, '_')
    .replace(/[^a-z0-9_\u4e00-\u9fff]+/gi, '')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '')
}

export function benchmarkDisplayLabel(value: unknown, fallback = 'Benchmark'): string {
  const raw = String(value ?? '').trim() || fallback
  const symbolBenchmark = raw.match(/^([A-Z0-9._-]+)\s+benchmark$/i)
  if (symbolBenchmark) return `${symbolBenchmark[1].toUpperCase()} Buy & Hold`
  return raw
    .replace(/\bbuy\s+(?:and|&)\s+hold\b/gi, 'Buy & Hold')
    .replace(/\bbenchmark\b/gi, 'Benchmark')
}

const GENERIC_LABELS: LabelMap = {
  all_values: {
    en: 'All Values',
    'zh-Hant': '全部數值',
  },
  data_unavailable: {
    en: 'Data unavailable',
    'zh-Hant': '資料不足',
  },
  disabled_by_config: {
    en: 'Disabled by config',
    'zh-Hant': '已由設定關閉',
  },
  enabled: {
    en: 'Enabled',
    'zh-Hant': '已啟用',
  },
  metadata_not_recorded: {
    en: 'Source not recorded',
    'zh-Hant': '來源未記錄',
  },
  not_applicable: {
    en: 'Not applicable',
    'zh-Hant': '不適用',
  },
  not_recorded: {
    en: 'Not recorded',
    'zh-Hant': '未記錄',
  },
  status_not_recorded: {
    en: 'Status not recorded',
    'zh-Hant': '狀態未記錄',
  },
  vocabulary_update_needed: {
    en: 'Not recorded',
    'zh-Hant': '未記錄',
  },
}

export function uiText(language: Language, key: string): string {
  return GENERIC_LABELS[key]?.[language] || GENERIC_LABELS.vocabulary_update_needed[language]
}

export function strictLookup(
  language: Language,
  value: unknown,
  labels: LabelMap,
  fallbackKey = 'vocabulary_update_needed',
): string {
  const code = normalizeUiCode(value)
  return labels[code]?.[language] || uiText(language, fallbackKey)
}

const STATUS_LABELS: LabelMap = {
  canceled: { en: 'canceled', 'zh-Hant': '已停止' },
  completed: { en: 'completed', 'zh-Hant': '完成' },
  default_set: { en: 'default set', 'zh-Hant': '已設為預設' },
  deleted: { en: 'deleted', 'zh-Hant': '已刪除' },
  error: { en: 'error', 'zh-Hant': '錯誤' },
  executed: { en: 'executed', 'zh-Hant': '已執行' },
  fail: { en: 'fail', 'zh-Hant': '未通過' },
  failed: { en: 'failed', 'zh-Hant': '失敗' },
  invalid_contract: { en: 'invalid contract', 'zh-Hant': '合約無效' },
  no_trade: { en: 'no trade', 'zh-Hant': '沒有交易' },
  ok: { en: 'ok', 'zh-Hant': '正常' },
  open: { en: 'open', 'zh-Hant': '持倉中' },
  partial: { en: 'partial', 'zh-Hant': '部分完成' },
  pass: { en: 'pass', 'zh-Hant': '通過' },
  pending: { en: 'pending', 'zh-Hant': '等待中' },
  queued: { en: 'queued', 'zh-Hant': '排隊中' },
  review: { en: 'needs review', 'zh-Hant': '需檢視' },
  running: { en: 'running', 'zh-Hant': '執行中' },
  saved: { en: 'saved', 'zh-Hant': '已儲存' },
  skipped: { en: 'skipped', 'zh-Hant': '已略過' },
  success: { en: 'success', 'zh-Hant': '成功' },
  unknown: { en: 'status not recorded', 'zh-Hant': '狀態未記錄' },
  valid: { en: 'valid', 'zh-Hant': '有效' },
}

export function statusLabel(value: unknown, language: Language): string {
  if (normalizeUiCode(value) === 'canceling') {
    return language === 'zh-Hant' ? '停止中' : 'stopping'
  }
  return strictLookup(language, value || 'unknown', STATUS_LABELS, 'status_not_recorded')
}

const MODULE_LABELS: LabelMap = {
  autorunner: { en: 'Run Center', 'zh-Hant': '執行中心' },
  backtester: { en: 'Backtester', 'zh-Hant': '回測器' },
  data_loader: { en: 'Data Loader', 'zh-Hant': '資料載入' },
  metrics_tracker: { en: 'Metrics Tracker', 'zh-Hant': '指標追蹤器' },
  metricstracker: { en: 'Metrics Tracker', 'zh-Hant': '指標追蹤器' },
  app_export: { en: 'App Export', 'zh-Hant': '應用匯出' },
  statanalyser: { en: 'Stat Analyser', 'zh-Hant': '統計分析器' },
  wfa: { en: 'Walk-Forward Analysis', 'zh-Hant': '前向分析 (WFA)' },
  validation_workflow: { en: 'Validation Workflow', 'zh-Hant': '前向分析' },
  unknown: { en: 'Module not recorded', 'zh-Hant': '模組未記錄' },
}

export function moduleLabel(value: unknown, language: Language): string {
  return strictLookup(language, value || 'unknown', MODULE_LABELS, 'metadata_not_recorded')
}

const BADGE_LABELS: LabelMap = {
  default: { en: 'Default', 'zh-Hant': '預設' },
  draft: { en: 'draft', 'zh-Hant': '草稿' },
  example: { en: 'example', 'zh-Hant': '範例' },
  production: { en: 'production', 'zh-Hant': '正式' },
  test: { en: 'test', 'zh-Hant': '測試' },
}

export function badgeLabel(value: unknown, language: Language): string {
  return strictLookup(language, value, BADGE_LABELS)
}

const PARAMETER_LABELS: LabelMap = {
  accepted_candidates: { en: 'Accepted Candidates', 'zh-Hant': '已接受候選' },
  all_existing_results: { en: 'All Existing Results', 'zh-Hant': '所有既有結果' },
  analysis: { en: 'Analysis', 'zh-Hant': '分析' },
  balanced: { en: 'Balanced', 'zh-Hant': '平衡' },
  best: { en: 'Best', 'zh-Hant': '最佳' },
  below_threshold: { en: 'Below Threshold', 'zh-Hant': '低於門檻' },
  calmar: { en: 'Calmar', 'zh-Hant': '卡瑪比率' },
  candidate: { en: 'Candidate', 'zh-Hant': '候選組合' },
  cagr: { en: 'CAGR', 'zh-Hant': '年化報酬' },
  cluster: { en: 'Cluster', 'zh-Hant': '群集' },
  cluster_median: { en: 'Cluster Median', 'zh-Hant': '群集中位數' },
  completed: { en: 'Completed', 'zh-Hant': '完成' },
  drawdown_aware: { en: 'Drawdown Aware', 'zh-Hant': '回撤優先' },
  excess_return: { en: 'Excess Return', 'zh-Hant': '超額報酬' },
  fail: { en: 'Fail', 'zh-Hant': '未通過' },
  failed: { en: 'Failed', 'zh-Hant': '失敗' },
  fixed: { en: 'Fixed', 'zh-Hant': '固定' },
  gp: { en: 'GP', 'zh-Hant': 'GP' },
  heatmap_plateau: { en: 'Heatmap Plateau', 'zh-Hant': '熱圖平台區' },
  live_adaptive_search: { en: 'Live Adaptive Search', 'zh-Hant': '即時自適應搜尋' },
  local_plateau_score: { en: 'Local Plateau Score', 'zh-Hant': '局部平台分數' },
  max_drawdown: { en: 'Max Drawdown', 'zh-Hant': '最大回撤' },
  mean: { en: 'Mean', 'zh-Hant': '平均數' },
  mean_oos_sharpe: { en: 'Mean OOS Sharpe', 'zh-Hant': '平均 OOS 夏普' },
  median: { en: 'Median', 'zh-Hant': '中位數' },
  multi_objective: { en: 'Multi Objective', 'zh-Hant': '多目標' },
  not_applicable: { en: 'Not applicable', 'zh-Hant': '不適用' },
  nsga2: { en: 'NSGA-II', 'zh-Hant': 'NSGA-II' },
  nsga_ii: { en: 'NSGA-II', 'zh-Hant': 'NSGA-II' },
  optuna: { en: 'Optuna', 'zh-Hant': 'Optuna' },
  optuna_suggested_candidates: { en: 'Optuna Suggested Candidates', 'zh-Hant': 'Optuna 建議候選' },
  oos_is_ratio: { en: 'OOS / IS Ratio', 'zh-Hant': 'OOS / IS 比率' },
  pass: { en: 'Pass', 'zh-Hant': '通過' },
  pending: { en: 'Pending', 'zh-Hant': '等待中' },
  performance_first: { en: 'Performance First', 'zh-Hant': '表現優先' },
  plateau_center: { en: 'Plateau Center', 'zh-Hant': '平台中心' },
  plateau_edge: { en: 'Plateau Edge', 'zh-Hant': '平台邊緣' },
  post_run_ranking: { en: 'Post-Run Ranking', 'zh-Hant': '執行後排序' },
  random: { en: 'Random', 'zh-Hant': '隨機' },
  ranking: { en: 'Ranking', 'zh-Hant': '排序' },
  rebalance_count: { en: 'Rebalance Count', 'zh-Hant': '再平衡次數' },
  review: { en: 'Needs Review', 'zh-Hant': '需檢視' },
  robust_score: { en: 'Robust Score', 'zh-Hant': '穩健分數' },
  running: { en: 'Running', 'zh-Hant': '執行中' },
  search: { en: 'Search', 'zh-Hant': '搜尋' },
  sharpe: { en: 'Sharpe', 'zh-Hant': '夏普比率' },
  single_axis_parameter_review: { en: 'Single-Axis Parameter Review', 'zh-Hant': '單軸參數檢視' },
  single_axis_table: { en: 'Single-Axis Table', 'zh-Hant': '單軸表格' },
  single_axis_table_only: { en: 'Single-Axis Table Only', 'zh-Hant': '僅單軸表格' },
  single_objective: { en: 'Single Objective', 'zh-Hant': '單目標' },
  stability_first: { en: 'Stability First', 'zh-Hant': '穩定優先' },
  std: { en: 'Std', 'zh-Hant': '標準差' },
  study_summary_derived_from_existing_results: { en: 'Completed Parameter Tests', 'zh-Hant': '已完成參數測試' },
  table_only_single_axis: { en: 'Table Only: Single Axis', 'zh-Hant': '僅表格：單軸' },
  top_n_median: { en: 'Top-N Median', 'zh-Hant': '前 N 名中位數' },
  top_ranked: { en: 'Top Ranked', 'zh-Hant': '最高排名' },
  top_trial: { en: 'Top Trial', 'zh-Hant': '最佳試驗' },
  total_return: { en: 'Total Return', 'zh-Hant': '總報酬' },
  tpe: { en: 'TPE', 'zh-Hant': 'TPE' },
  tpe_style: { en: 'TPE-Style', 'zh-Hant': 'TPE 風格' },
  trade_count: { en: 'Trade Count', 'zh-Hant': '交易次數' },
  win_rate: { en: 'Win Rate', 'zh-Hant': '勝率' },
  worst: { en: 'Worst', 'zh-Hant': '最差' },
}

export function parameterLabel(value: unknown, language: Language): string {
  return strictLookup(language, value, PARAMETER_LABELS)
}

export function parameterAxisLabel(value: unknown, language: Language): string {
  const raw = String(value ?? '').trim()
  return raw || uiText(language, 'not_recorded')
}

const WINDOW_SIZING_LABELS: LabelMap = {
  artifact_dates: { en: 'Legacy output: inferred from result dates', 'zh-Hant': '舊版輸出：由日期推算' },
  auto: { en: 'Automatic', 'zh-Hant': '自動判斷' },
  input_numbers: { en: 'Manual day counts', 'zh-Hant': '手動日數' },
  input_ratios: { en: 'Manual ratios', 'zh-Hant': '手動比例' },
  metadata_missing: { en: 'Source not recorded', 'zh-Hant': '來源未記錄' },
  unknown: { en: 'Source not recorded', 'zh-Hant': '來源未記錄' },
}

export function windowSizingLabel(value: unknown, language: Language): string {
  return strictLookup(language, value || 'metadata_missing', WINDOW_SIZING_LABELS, 'metadata_not_recorded')
}

const CANDIDATE_FILTER_LABELS: LabelMap = {
  disabled_by_config: { en: 'Disabled by config', 'zh-Hant': '已由設定關閉' },
  enabled: { en: 'Enabled', 'zh-Hant': '已啟用' },
  metadata_missing: { en: 'Source not recorded', 'zh-Hant': '來源未記錄' },
  not_recorded: { en: 'Not recorded', 'zh-Hant': '未記錄' },
}

export function candidateFilterLabel(value: unknown, language: Language): string {
  return strictLookup(language, value || 'metadata_missing', CANDIDATE_FILTER_LABELS, 'metadata_not_recorded')
}

const REVIEW_REASON_LABELS: LabelMap = {
  below_threshold: { en: 'Below acceptance threshold', 'zh-Hant': '低於接受門檻' },
  calmar_not_positive: { en: 'Calmar is not positive', 'zh-Hant': '卡瑪比率不是正數' },
  max_drawdown_floor_breached: { en: 'Max drawdown limit was breached', 'zh-Hant': '最大回撤超出限制' },
  mean_oos_calmar_not_positive: { en: 'Average OOS Calmar is not positive', 'zh-Hant': '平均 OOS 卡瑪比率不是正數' },
  mean_oos_sharpe_not_positive: { en: 'Average OOS Sharpe is not positive', 'zh-Hant': '平均 OOS 夏普不是正數' },
  meets_acceptance_gates: { en: 'Meets acceptance gates', 'zh-Hant': '通過接受條件' },
  missing_oos_metric: { en: 'OOS metric is missing', 'zh-Hant': '缺少 OOS 指標' },
  needs_wfa_validation: { en: 'Needs WFA validation', 'zh-Hant': '需要 WFA 驗證' },
  oos_calmar_not_positive: { en: 'OOS Calmar is not positive', 'zh-Hant': 'OOS 卡瑪比率不是正數' },
  oos_is_ratio_below_threshold: { en: 'OOS / IS ratio is below threshold', 'zh-Hant': 'OOS / IS 比率低於門檻' },
  oos_sharpe_not_positive: { en: 'OOS Sharpe is not positive', 'zh-Hant': 'OOS 夏普不是正數' },
  profit_factor_below_threshold: { en: 'Profit factor is below threshold', 'zh-Hant': '獲利因子低於門檻' },
  review_borderline_metrics: { en: 'Borderline metrics need review', 'zh-Hant': '邊界指標需要檢視' },
  meets_selection_constraints: { en: 'Meets IS activity gates', 'zh-Hant': '符合 IS 活動門檻' },
  selection_constraints_disabled: { en: 'IS activity gates disabled', 'zh-Hant': 'IS 活動門檻已關閉' },
  trade_count_below_threshold: { en: 'Trade count is below threshold', 'zh-Hant': '交易次數低於門檻' },
  win_rate_below_threshold: { en: 'Win rate is below threshold', 'zh-Hant': '勝率低於門檻' },
}

export function reviewReasonLabel(value: unknown, language: Language): string {
  const raw = String(value ?? '').trim()
  if (!raw) return uiText(language, 'not_recorded')
  const activeMatch = raw.match(/^is_active_rebalances_below_(\d+)$/i)
  if (activeMatch) {
    return language === 'zh-Hant'
      ? `IS active rebalance 少於 ${activeMatch[1]} 次`
      : `IS active rebalances below ${activeMatch[1]}`
  }
  const exposureMatch = raw.match(/^is_exposure_ratio_below_([0-9.]+)$/i)
  if (exposureMatch) {
    const valueText = `${Number(exposureMatch[1]) * 100}%`
    return language === 'zh-Hant'
      ? `IS exposure ratio 低於 ${valueText}`
      : `IS exposure ratio below ${valueText}`
  }
  const nonzeroMatch = raw.match(/^is_nonzero_return_days_below_(\d+)$/i)
  if (nonzeroMatch) {
    return language === 'zh-Hant'
      ? `IS 非零回報日少於 ${nonzeroMatch[1]} 日`
      : `IS non-zero return days below ${nonzeroMatch[1]}`
  }
  const lookbackMatch = raw.match(/^lookback_fraction_above_([0-9.]+)$/i)
  if (lookbackMatch) {
    const valueText = `${Number(lookbackMatch[1]) * 100}%`
    return language === 'zh-Hant'
      ? `最大 lookback / IS 超過 ${valueText}`
      : `max lookback / IS above ${valueText}`
  }
  return strictLookup(language, raw, REVIEW_REASON_LABELS, 'not_recorded')
}

const SELECTION_EVIDENCE_LABELS: LabelMap = {
  cluster_median: { en: 'Cluster median candidate', 'zh-Hant': '群集中位候選' },
  heatmap_plateau: { en: 'Heatmap plateau candidate', 'zh-Hant': '熱圖平台候選' },
  is_best_rank: { en: 'IS optimization rank #1', 'zh-Hant': 'IS 最佳排名 #1' },
  optuna: { en: 'Optuna suggested candidate', 'zh-Hant': 'Optuna 建議候選' },
  plateau_center: { en: 'Plateau center candidate', 'zh-Hant': '平台中心候選' },
  plateau_edge: { en: 'Plateau edge candidate', 'zh-Hant': '平台邊緣候選' },
  ranking: { en: 'Ranking-selected candidate', 'zh-Hant': '排序選出候選' },
  top_ranked: { en: 'Top-ranked candidate', 'zh-Hant': '最高排名候選' },
  top_trial: { en: 'Top trial candidate', 'zh-Hant': '最佳試驗候選' },
}

export function selectionEvidenceLabel(value: unknown, language: Language): string {
  const raw = String(value ?? '').trim()
  if (!raw) return strictLookup(language, 'is_best_rank', SELECTION_EVIDENCE_LABELS)

  const rankedMatch = raw.match(/^rank=(\d+)\s+by\s+IS\s+(.+?)(?:\s+among\s+sampled\s+(\d+)\/(\d+)\s+candidates?)?$/i)
  if (rankedMatch) {
    const rank = rankedMatch[1]
    const metric = parameterLabel(rankedMatch[2], language)
    const sampled = rankedMatch[3]
    const total = rankedMatch[4]
    const base = language === 'zh-Hant'
      ? `IS ${metric} 第 ${rank} 名`
      : `Rank #${rank} by IS ${metric}`
    if (sampled && total) {
      return language === 'zh-Hant'
        ? `${base}（抽樣 ${sampled}/${total} 個候選）`
        : `${base} (sampled ${sampled}/${total} candidates)`
    }
    return base
  }

  return strictLookup(language, raw, SELECTION_EVIDENCE_LABELS)
}

export function noParameterReasonLabel(value: unknown, language: Language): string {
  const raw = String(value ?? '').trim()
  const normalized = normalizeUiCode(raw)
  if (!raw) return uiText(language, 'not_recorded')
  if (normalized.includes('no_free_parameters')) {
    return language === 'zh-Hant'
      ? '此策略沒有可調參數，適合直接回測或 WFA 驗證。'
      : 'This strategy has no free parameters, so direct backtest or WFA validation is appropriate.'
  }
  if (normalized.includes('single_axis') || normalized.includes('only_one_parameter_axis')) {
    return language === 'zh-Hant'
      ? '目前只有一個可調參數軸，系統以單軸表格檢視代替矩陣。'
      : 'Only one parameter axis is available, so the system uses a single-axis table instead of a matrix.'
  }
  if (
    normalized.includes('no_varied_portfolio_parameter_domain')
    || normalized.includes('fixed_portfolios')
    || normalized.includes('single_policy_portfolio')
    || normalized.includes('fixed_strategy_result')
  ) {
    return language === 'zh-Hant'
      ? '這是固定配置或單一策略回測，沒有可繪製成參數矩陣的變動參數；請到「回測」分頁查看配置、再平衡和績效細節。'
      : 'This is a fixed-allocation or single-policy backtest, so there are no varied parameters for a matrix. Open the Backtest tab to review allocation, rebalancing, and performance details.'
  }
  if (
    normalized.includes('no_parameter_domain')
    || normalized.includes('not_expose_enough_varied_semantic_parameters')
  ) {
    return language === 'zh-Hant'
      ? '此執行結果沒有足夠的變動參數可建立參數矩陣；請到「回測」分頁查看單次回測結果。'
      : 'This run does not have enough varied parameters for a parameter matrix. Open the Backtest tab to review the single backtest result.'
  }
  return uiText(language, 'not_recorded')
}

const JOB_STAGE_LABELS: LabelMap = {
  backtester: { en: 'Backtester', 'zh-Hant': '回測器' },
  completed: { en: 'Completed', 'zh-Hant': '完成' },
  config_validation: { en: 'Config Validation', 'zh-Hant': '設定驗證' },
  dataloader: { en: 'Data Loading', 'zh-Hant': '資料載入' },
  data_loading: { en: 'Data Loading', 'zh-Hant': '資料載入' },
  failed: { en: 'Failed', 'zh-Hant': '失敗' },
  factor_analysis: { en: 'Factor Analysis', 'zh-Hant': '因子分析' },
  metricstracker: { en: 'Metrics Tracker', 'zh-Hant': '指標追蹤器' },
  metrics_tracker: { en: 'Metrics Tracker', 'zh-Hant': '指標追蹤器' },
  app_export: { en: 'App Export', 'zh-Hant': '應用匯出' },
  partial: { en: 'Partial', 'zh-Hant': '部分完成' },
  queued: { en: 'Queued', 'zh-Hant': '排隊中' },
  starting: { en: 'Starting', 'zh-Hant': '開始執行' },
  statanalyser: { en: 'Stat Analyser', 'zh-Hant': '統計分析器' },
  wfa: { en: 'Walk-Forward Analysis', 'zh-Hant': '前向分析 (WFA)' },
  validation_workflow: { en: 'Validation Workflow', 'zh-Hant': '前向分析' },
}

export function jobStageLabel(value: unknown, language: Language): string {
  return strictLookup(language, value, JOB_STAGE_LABELS)
}

function jobMessageLabel(message: string, language: Language): string {
  const raw = message.trim()
  const normalized = normalizeUiCode(raw)
  const candidateMatch = raw.match(/\((\d+)\s+parameter candidates?\)/i)
  const candidateSuffix = candidateMatch
    ? language === 'zh-Hant'
      ? `（${candidateMatch[1]} 組參數候選）`
      : ` (${candidateMatch[1]} parameter candidates)`
    : ''
  if (!raw) return ''
  if (normalized.includes('validating')) return language === 'zh-Hant' ? '正在驗證設定' : 'validating config'
  if (normalized.includes('loading_data')) return language === 'zh-Hant' ? '正在載入資料' : 'loading data'
  if (normalized.includes('running_backtest')) return `${language === 'zh-Hant' ? '正在執行回測' : 'running backtest'}${candidateSuffix}`
  if (normalized.includes('running_metrics')) return language === 'zh-Hant' ? '正在計算指標' : 'running metrics'
  if (normalized.includes('running_statanalyser')) return language === 'zh-Hant' ? '正在執行統計分析' : 'running statistical analysis'
  if (normalized.includes('running_wfa')) return `${language === 'zh-Hant' ? '正在執行前向分析' : 'running WFA'}${candidateSuffix}`
  if (normalized.includes('running_factor_analysis')) return language === 'zh-Hant' ? '正在執行因子分析' : 'running factor analysis'
  if (normalized.includes('finalizing_app_registry')) return language === 'zh-Hant' ? '正在整理平台登錄' : 'finalizing app registry'
  if (normalized.includes('queued')) return language === 'zh-Hant' ? '等待執行' : 'queued'
  if (normalized.includes('starting')) return language === 'zh-Hant' ? '開始執行' : 'starting'
  if (normalized.includes('finished_with_status')) {
    const status = normalized.replace(/^.*finished_with_status_?/, '') || 'unknown'
    return language === 'zh-Hant' ? `完成，狀態：${statusLabel(status, language)}` : `finished with status: ${statusLabel(status, language)}`
  }
  return raw
}

export function jobLogLineLabel(value: unknown, language: Language): string {
  const raw = String(value ?? '').trim()
  if (!raw) return uiText(language, 'not_recorded')
  const bracketed = raw.match(/^\[([^\]]+)\]\s*(.*)$/)
  if (!bracketed) return jobStageLabel(raw, language)
  const stage = jobStageLabel(bracketed[1], language)
  const message = jobMessageLabel(bracketed[2] || '', language)
  return message ? `[${stage}] ${message}` : `[${stage}]`
}

const DATA_HEALTH_LABELS: LabelMap = {
  invalid_contract: { en: 'invalid contract', 'zh-Hant': '合約無效' },
  legacy_missing_validation: { en: 'legacy result without validation record', 'zh-Hant': '舊結果沒有驗證記錄' },
  missing_validation: { en: 'validation record missing', 'zh-Hant': '缺少驗證記錄' },
  valid: { en: 'valid', 'zh-Hant': '有效' },
}

export function dataHealthLabel(value: unknown, language: Language): string {
  return strictLookup(language, value, DATA_HEALTH_LABELS, 'not_recorded')
}

const TRADE_STATUS_LABELS: LabelMap = {
  closed: { en: 'closed', 'zh-Hant': '已平倉' },
  executed: { en: 'executed', 'zh-Hant': '已執行' },
  no_trade: { en: 'no trade', 'zh-Hant': '沒有交易' },
  open: { en: 'open', 'zh-Hant': '持倉中' },
  skipped: { en: 'skipped', 'zh-Hant': '已略過' },
}

export function tradeStatusLabel(value: unknown, language: Language): string {
  return strictLookup(language, value, TRADE_STATUS_LABELS, 'not_recorded')
}

export function tradeReasonLabel(value: unknown, language: Language): string {
  const raw = String(value ?? '').trim()
  const normalized = normalizeUiCode(raw)
  if (!raw) return uiText(language, 'not_recorded')
  if (normalized.includes('same_session_entry')) return language === 'zh-Hant' ? '同一交易時段開倉' : 'same-session entry'
  if (normalized.includes('same_session_exit')) return language === 'zh-Hant' ? '同一交易時段平倉' : 'same-session exit'
  if (normalized.includes('target_unchanged')) return language === 'zh-Hant' ? '目標持倉不變' : 'target holding unchanged'
  if (normalized.includes('not_selected')) return language === 'zh-Hant' ? '本次再平衡未被選中' : 'not selected at this rebalance'
  if (normalized.includes('eligible') || normalized.includes('rank')) return language === 'zh-Hant' ? '排名篩選結果' : 'ranking filter result'
  return uiText(language, 'not_recorded')
}

const OPERATION_ERROR_LABELS: LabelMap = {
  parameter_review_preview_failed: {
    en: 'Could not apply the review rules. The raw error was recorded for debugging.',
    'zh-Hant': '未能套用檢視規則；原始錯誤已保留作 debug 用。',
  },
  parameter_template_save_failed: {
    en: 'Could not save the review template. The raw error was recorded for debugging.',
    'zh-Hant': '未能儲存檢視範本；原始錯誤已保留作 debug 用。',
  },
  parameter_template_delete_failed: {
    en: 'Could not delete the review template. The raw error was recorded for debugging.',
    'zh-Hant': '未能刪除檢視範本；原始錯誤已保留作 debug 用。',
  },
  parameter_template_default_failed: {
    en: 'Could not update the default review template. The raw error was recorded for debugging.',
    'zh-Hant': '未能更新預設檢視範本；原始錯誤已保留作 debug 用。',
  },
  run_center_batch_submit_failed: {
    en: 'Could not submit the batch. Check the debug panel for the raw error details.',
    'zh-Hant': '未能提交批次；原始錯誤詳情可在 Debug Panel 查看。',
  },
}

export function operationErrorLabel(value: unknown, language: Language): string {
  return strictLookup(language, value, OPERATION_ERROR_LABELS, 'status_not_recorded')
}
