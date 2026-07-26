import { useQuery } from '@tanstack/react-query'
import { Link } from '../routing'

import { api } from '../api'
import { SectionCard } from '../components/SectionCard'
import { StatusBadge } from '../components/StatusBadge'
import { useCopy } from '../i18n'
import { useAppStore } from '../store'
import type { Language } from '../i18n'
import { badgeLabel, moduleLabel } from '../uiVocabulary'

const zhLabelReplacements: Array<[RegExp, string]> = [
  [/\bPredictor Analysis\b/g, '因子分析'],
  [/\bFactor Analysis\b/g, '因子分析'],
  [/\bBacktests\b/g, '回測'],
  [/\bBacktest\b/g, '回測'],
  [/\bWalk-Forward\b/g, '前向分析 (WFA)'],
  [/\bRolling Windows\b/g, '前向分析視窗'],
  [/\bParameter Matrix\b/g, '參數矩陣'],
  [/\bSingle Backtest\b/g, '單次回測'],
  [/\bPortfolio Backtest\b/g, '投資組合回測'],
  [/\bPortfolio\b/g, '投資組合'],
  [/\bAllocation\b/g, '配置'],
  [/\bCalendar\b/g, '日曆'],
  [/\bPrice\b/g, '價格'],
  [/\bFactor\b/g, '因子'],
  [/\bSummary\b/g, '摘要'],
  [/\bProduction\b/g, '正式'],
  [/\bTest\b/g, '測試'],
  [/\bcfg\b/g, '設定'],
  [/\brun\b/g, '執行'],
]
function formatRunCenterLabel(value: string, language: Language) {
  if (language !== 'zh-Hant') {
    return value.replace(/^Predictor Analysis\b/, 'Factor Analysis')
  }
  return zhLabelReplacements.reduce((label, [pattern, replacement]) => {
    return label.replace(pattern, replacement)
  }, value)
}

function formatModuleName(value: string, t: ReturnType<typeof useCopy>, language: Language) {
  const normalized = value.toLowerCase()
  if (normalized === 'autorunner') return t('workflow.backtests')
  if (normalized === 'validation_workflow' || normalized === 'wfa') return t('workflow.walkForward')
  if (normalized === 'statanalyser') return t('workflow.factorAnalysis')
  return moduleLabel(value, language)
}

export function CommandCenterPage() {
  const language = useAppStore((state) => state.language)
  const t = useCopy(language)
  const { data, isLoading, error } = useQuery({
    queryKey: ['command-center'],
    queryFn: api.commandCenter,
    staleTime: 1000,
    refetchInterval: 2000,
  })

  if (isLoading) {
    return <div className="page-loading">{t('commandCenter.loading')}</div>
  }
  if (error || !data) {
    return <div className="page-error">{t('commandCenter.unable')}</div>
  }

  const activeBatches = Array.isArray(data.active_batches) ? data.active_batches : []
  const recentRuns = Array.isArray(data.recent_runs) ? data.recent_runs : []
  const completedByModule = data.resource_snapshot.completed_by_module || {}
  const failedByModule = data.resource_snapshot.failed_by_module || {}
  const moduleCount = (source: Record<string, number>, key: string) => Number(source[key] || 0)
  const failedBreakdown = [
    moduleCount(failedByModule, 'autorunner'),
    moduleCount(failedByModule, 'validation_workflow'),
    moduleCount(failedByModule, 'statanalyser'),
  ]

  return (
    <div className="page-stack">
      <SectionCard
        title={t('commandCenter.title')}
        subtitle={t('commandCenter.subtitle')}
      >
        <div className="command-center-hero">
          <div className="command-center-copy">
            <div className="hero-line">
              {activeBatches.length > 0
                ? `${activeBatches.length} ${t('commandCenter.activeRunning')}`
                : t('commandCenter.noActiveBatch')}
            </div>
            <div className="hero-line muted">
              {t('commandCenter.recentHint')}
            </div>
          </div>
          <div className="command-center-kpis command-center-kpis-corner">
            <div className="metric-card compact">
              <div className="metric-label">{t('commandCenter.running')}</div>
              <div className="metric-value">{data.resource_snapshot.active_batch_count}</div>
            </div>
            <div className="metric-card compact">
              <div className="metric-label">{t('commandCenter.completedJobs')}</div>
              <div className="metric-value">{data.resource_snapshot.successful_runs ?? data.resource_snapshot.recent_successful_runs}</div>
            </div>
            <div className="metric-card compact">
              <div className="metric-label">{t('commandCenter.failed')}</div>
              <div className="metric-value">{data.resource_snapshot.failed_runs ?? data.resource_snapshot.recent_failed_runs}</div>
            </div>
            <div className="metric-card compact">
              <div className="metric-label">{t('commandCenter.latestResult')}</div>
              <div className="metric-value metric-value-small">
                {data.resource_snapshot.latest_result_time || '-'}
              </div>
            </div>
            <div className="system-detail-line">
              {t('commandCenter.capacity')} {data.resource_snapshot.scheduler_capacity} | {t('commandCenter.cpu')} {data.resource_snapshot.cpu_count}
            </div>
            <div className="system-detail-line">
              {t('commandCenter.completedBreakdown')}: {t('workflow.factorAnalysis')} {moduleCount(completedByModule, 'statanalyser')} | {t('workflow.backtests')} {moduleCount(completedByModule, 'autorunner')} | {t('workflow.walkForward')} {moduleCount(completedByModule, 'validation_workflow')}
            </div>
            {failedBreakdown.some((count) => count > 0) ? (
              <div className="system-detail-line">
                {t('commandCenter.failedBreakdown')}: {t('workflow.factorAnalysis')} {failedBreakdown[2]} | {t('workflow.backtests')} {failedBreakdown[0]} | {t('workflow.walkForward')} {failedBreakdown[1]}
              </div>
            ) : null}
          </div>
        </div>
      </SectionCard>

      <SectionCard title={t('commandCenter.activeBatches')}>
        {activeBatches.length === 0 ? (
          <div className="muted">{t('commandCenter.noActiveBatches')}</div>
        ) : (
          <div className="run-card-list">
            {activeBatches.map((batch: any) => (
              <div key={batch.batch_id} className="run-card">
                <div className="run-card-header">
                  <div>
                    <div className="run-card-title">{batch.batch_id}</div>
                    <div className="run-card-subtitle">{formatModuleName(String(batch.module || ''), t, language)}</div>
                  </div>
                  <StatusBadge status={batch.status} />
                </div>
                <div className="run-card-meta">
                  {Array.isArray(batch?.jobs) ? batch.jobs.length : 0} {t('commandCenter.jobs')}
                </div>
              </div>
            ))}
          </div>
        )}
      </SectionCard>

      <SectionCard title={t('commandCenter.recentRuns')}>
        <div className="data-table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>{t('commandCenter.workflow')}</th>
                <th>{t('commandCenter.configFile')}</th>
                <th>{t('common.status')}</th>
                <th>{t('commandCenter.created')}</th>
                <th>{t('commandCenter.view')}</th>
              </tr>
            </thead>
            <tbody>
              {recentRuns.map((run: any) => (
                <tr key={run.run_id}>
                  <td>{formatModuleName(String(run.module || run.module_display || ''), t, language)}</td>
                  <td>
                    <span className="table-label-main">
                      {formatRunCenterLabel(
                        String(run.display_label || run.config_filename || run.primary_artifact_name || run.run_id),
                        language,
                      )}
                      {(run.label_badges || []).map((badge: string) => (
                        <span key={`${run.run_id}-${badge}`} className="mini-badge">
                          {badgeLabel(badge, language)}
                        </span>
                      ))}
                    </span>
                    <div className="checkbox-label-subtle">{run.run_id}</div>
                  </td>
                  <td>
                    <StatusBadge status={run.status} />
                  </td>
                  <td>{run.created_at}</td>
                  <td>
                    {run.module === 'autorunner' ? (
                      <Link
                        to="/metrics"
                        search={{ runId: run.run_id }}
                        className="inline-action"
                      >
                        {t('commandCenter.viewMetrics')}
                      </Link>
                    ) : run.module === 'validation_workflow' ? (
                      <Link
                        to="/wfa"
                        search={{ runId: run.run_id }}
                        className="inline-action"
                      >
                        {t('commandCenter.viewWalkForward')}
                      </Link>
                    ) : (
                      <Link to="/run-center" className="inline-action">
                        {t('commandCenter.openRunCenter')}
                      </Link>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </SectionCard>
    </div>
  )
}
