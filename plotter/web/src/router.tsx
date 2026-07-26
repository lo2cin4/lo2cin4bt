import { lazy, Suspense, useEffect, useMemo } from 'react'
import type { PropsWithChildren } from 'react'
import { useQuery } from '@tanstack/react-query'

import { api } from './api'
import { AppShell } from './components/AppShell'
import { CustomSelect } from './components/CustomSelect'
import { ShareToolbar } from './components/ShareToolbar'
import { StrategyRulesPanel, hasRenderableStrategySummary } from './components/StrategyRulesPanel'
import { Language, useCopy } from './i18n'
import { BrowserRouter, Link, useNavigate, useRouterState } from './routing'
import { useAppStore } from './store'

const CommandCenterPage = lazy(() =>
  import('./pages/CommandCenterPage').then((module) => ({ default: module.CommandCenterPage })),
)
const RunCenterPage = lazy(() =>
  import('./pages/RunCenterPage').then((module) => ({ default: module.RunCenterPage })),
)
const MetricsOverviewPage = lazy(() =>
  import('./pages/MetricsOverviewPage').then((module) => ({ default: module.MetricsOverviewPage })),
)
const ParameterMatrixPage = lazy(() =>
  import('./pages/ParameterMatrixPage').then((module) => ({ default: module.ParameterMatrixPage })),
)
const WFAPage = lazy(() =>
  import('./pages/WFAPage').then((module) => ({ default: module.WFAPage })),
)
const BacktestsPage = lazy(() =>
  import('./pages/BacktestsPage').then((module) => ({ default: module.BacktestsPage })),
)

function RouteFallback() {
  return <div className="page-loading">Loading page...</div>
}

function lazyPage(element: JSX.Element) {
  return <Suspense fallback={<RouteFallback />}>{element}</Suspense>
}

function formatTitleToken(value: string) {
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
  return formatTitleToken(value)
}

function basename(value: string) {
  return String(value || '').split(/[\\/]/).filter(Boolean).pop() || ''
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
    : formatTitleToken(parts[0])
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

function formatRunLabel(run: any, language: Language) {
  const identity = run?.identity || {}
  const identityDate = String(identity.date || '').trim()
  const identityAsset = String(identity.asset || '').trim()
  if (identityDate && identityAsset) {
    const concept = String(
      identity.concept_display || identity.strategy_display || identity.factor_display || '',
    ).trim()
    const mode = displayModeToken(String(identity.mode || ''), language)
    const runId = runShortId(run)
    return [
      displayDateToken(identityDate),
      displayAssetToken(identityAsset, language),
      concept,
      mode,
      runId ? `run ${runId}` : '',
    ]
      .filter(Boolean)
      .join(' | ')
  }
  const display = String(run?.display_label || '').trim()
  const filename = String(run?.config_filename || '').replace(/\.json$/i, '').trim()
  const runIdText = String(run?.run_id || '').trim()
  for (const label of [display, filename]) {
    if (!label) continue
    const structuredLabel = labelFromStructuredName(label, run, language)
    if (structuredLabel) return structuredLabel
    if (!/\b(?:ma-cross|hold-reset|threshold-hold-reset|batch)\b/i.test(label)) return label
  }
  return runIdText
}

function MetricsLayout({ children }: PropsWithChildren) {
  const navigate = useNavigate()
  const pathname = useRouterState({ select: (state) => state.location.pathname })
  const search = useRouterState({ select: (state) => state.location.search }) as Record<string, string | undefined>
  const runId = useAppStore((state) => state.selectedMetricsRunId)
  const setSelectedMetricsRunId = useAppStore((state) => state.setSelectedMetricsRunId)
  const backtestId = useAppStore((state) => state.selectedBacktestId)
  const setSelectedBacktestId = useAppStore((state) => state.setSelectedBacktestId)
  const shareMosaicMode = useAppStore((state) => state.shareMosaicMode)
  const captureMosaicMode = search.captureMosaic === '1'
  const screenshotCaptureMode = search.captureScreenshot === '1'
  const language = useAppStore((state) => state.language)
  const t = useCopy(language)
  const runsQuery = useQuery({
    queryKey: ['metrics-runs'],
    queryFn: api.metricsRuns,
    staleTime: 60000,
  })
  const requestedRunId = search.runId || runId || ''
  const availableRunIds = useMemo(
    () => (runsQuery.data || []).map((run: any) => run.run_id),
    [runsQuery.data],
  )
  const resolvedRunId = availableRunIds.includes(String(requestedRunId))
    ? String(requestedRunId)
    : runsQuery.data?.[0]?.run_id || ''
  const selectedMetricsRun = (runsQuery.data || []).find((run: any) => run.run_id === resolvedRunId) || {}
  const isBacktestsPage = pathname === '/metrics/backtests'
  const needsOverviewForLayout = pathname === '/metrics' || isBacktestsPage
  const overviewQuery = useQuery({
    queryKey: ['metrics-overview', resolvedRunId],
    queryFn: () => api.metricsOverview(resolvedRunId),
    enabled: Boolean(resolvedRunId && needsOverviewForLayout),
    staleTime: 60000,
  })
  const availableBacktestIds = useMemo(
    () => (overviewQuery.data?.rows || []).map((row: any) => row.backtest_id),
    [overviewQuery.data],
  )
  const searchBacktestId = typeof search.backtestId === 'string' ? search.backtestId : ''
  const resolvedBacktestId = availableBacktestIds.includes(String(searchBacktestId))
    ? String(searchBacktestId)
    : availableBacktestIds.includes(String(backtestId))
    ? String(backtestId)
    : overviewQuery.data?.rows?.[0]?.backtest_id || ''
  const overviewStrategySummary = overviewQuery.data?.strategy_summary || {}
  const runStrategySummary = selectedMetricsRun.strategy_summary || {}
  const strategySummary = hasRenderableStrategySummary(overviewStrategySummary)
    ? overviewStrategySummary
    : runStrategySummary
  const strategyLoading = runsQuery.isLoading || (overviewQuery.isLoading && !hasRenderableStrategySummary(strategySummary))

  useEffect(() => {
    if (resolvedRunId && resolvedRunId !== runId) {
      setSelectedMetricsRunId(resolvedRunId)
    }
  }, [resolvedRunId, runId, setSelectedMetricsRunId])

  useEffect(() => {
    if (isBacktestsPage && resolvedBacktestId && resolvedBacktestId !== backtestId) {
      setSelectedBacktestId(resolvedBacktestId)
    }
  }, [backtestId, isBacktestsPage, resolvedBacktestId, setSelectedBacktestId])

  return (
    <div
      className={`page-stack ${shareMosaicMode || captureMosaicMode ? 'share-mosaic-mode' : ''} ${screenshotCaptureMode ? 'screenshot-capture-mode' : ''}`}
      data-share-capture-root
    >
      <div className="metrics-header-shell">
        <div className="metrics-header-title">{t('metrics.title')}</div>
        <div className="metrics-header-subtitle">
          {t('metrics.subtitle')}
        </div>
        <ShareToolbar />
        <div className="metrics-subnav">
          <Link
            className={`subnav-link ${pathname === '/metrics' ? 'active' : ''}`}
            to="/metrics"
            search={resolvedRunId ? { runId: resolvedRunId } : {}}
            activeOptions={{ exact: true }}
          >
            {t('metrics.overview')}
          </Link>
          <Link
            className={`subnav-link ${pathname === '/metrics/parameter-matrix' ? 'active' : ''}`}
            to="/metrics/parameter-matrix"
            search={resolvedRunId ? { runId: resolvedRunId, ...(resolvedBacktestId ? { backtestId: resolvedBacktestId } : {}) } : {}}
            activeOptions={{ exact: true }}
          >
            {t('workflow.parameterMatrix')}
          </Link>
          <Link
            className={`subnav-link ${pathname === '/metrics/backtests' ? 'active' : ''}`}
            to="/metrics/backtests"
            search={resolvedRunId ? { runId: resolvedRunId, ...(resolvedBacktestId ? { backtestId: resolvedBacktestId } : {}) } : {}}
            activeOptions={{ exact: true }}
          >
            {t('workflow.backtests')}
          </Link>
        </div>
        <div className="metrics-header-controls">
          <div className="metrics-selector-stack">
            <div className="metrics-header-field">
              <div className="metrics-header-label">{t('metrics.metricsFileSelection')}</div>
              <CustomSelect
                className="metrics-header-select"
                value={resolvedRunId}
                options={(runsQuery.data || []).map((run: any) => ({
                  value: run.run_id,
                  label: formatRunLabel(run, language),
                }))}
                redactValues
                onChange={(nextRunId) => {
                  navigate({
                    to:
                      pathname === '/metrics/backtests'
                        ? '/metrics/backtests'
                        : pathname === '/metrics/parameter-matrix'
                        ? '/metrics/parameter-matrix'
                        : '/metrics',
                    search: { runId: nextRunId },
                  })
                }}
              />
            </div>
            {isBacktestsPage ? (
              <div className="metrics-header-field">
                <div className="metrics-header-label">{t('metrics.backtestSelection')}</div>
                <CustomSelect
                  className="metrics-header-select"
                  value={resolvedBacktestId}
                  options={(overviewQuery.data?.rows || []).map((row: any) => ({
                    value: row.backtest_id,
                    label: row.label,
                  }))}
                  redactValues
                  onChange={(nextBacktestId) => {
                    navigate({
                      to: '/metrics/backtests',
                      search: { runId: resolvedRunId, backtestId: nextBacktestId },
                    })
                  }}
                />
              </div>
            ) : null}
          </div>
          <StrategyRulesPanel summary={strategySummary} loading={strategyLoading} />
        </div>
      </div>
      {children}
    </div>
  )
}

function RouterView() {
  const pathname = useRouterState({ select: (state) => state.location.pathname })
  const route = pathname === '/'
    ? lazyPage(<CommandCenterPage />)
    : pathname === '/run-center'
    ? lazyPage(<RunCenterPage />)
    : pathname === '/wfa'
    ? lazyPage(<WFAPage />)
    : pathname === '/metrics'
    ? lazyPage(<MetricsOverviewPage />)
    : pathname === '/metrics/parameter-matrix'
    ? lazyPage(<ParameterMatrixPage />)
    : pathname === '/metrics/backtests'
    ? lazyPage(<BacktestsPage />)
    : null

  const content = pathname.startsWith('/metrics') && route
    ? <MetricsLayout>{route}</MetricsLayout>
    : route

  return <AppShell>{content}</AppShell>
}

export function RouterProvider() {
  return (
    <BrowserRouter>
      <RouterView />
    </BrowserRouter>
  )
}
