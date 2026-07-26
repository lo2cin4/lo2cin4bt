import { expect, test } from '@playwright/test'
import fs from 'node:fs'
import path from 'node:path'

const fixturePath = path.resolve(
  process.cwd(),
  '../../verification/fixtures/backtest_result_contract_recovery/p0_expected_contract.json',
)
const fixture = JSON.parse(fs.readFileSync(fixturePath, 'utf8'))
const runId = String(fixture.run_id)
const candidateId = String(fixture.candidate_id)
const expected = fixture.expected

const metricsUrl = `/metrics?runId=${encodeURIComponent(runId)}`
const matrixUrl = `/metrics/parameter-matrix?runId=${encodeURIComponent(runId)}&backtestId=${encodeURIComponent(candidateId)}`
const detailUrl = `/metrics/backtests?runId=${encodeURIComponent(runId)}&backtestId=${encodeURIComponent(candidateId)}`
const contextCandidateId = String(fixture.context_candidate_id || candidateId)
const contextDetailUrl = `/metrics/backtests?runId=${encodeURIComponent(runId)}&backtestId=${encodeURIComponent(contextCandidateId)}`

async function jsonResponse(response: Awaited<ReturnType<Parameters<typeof test>[0]>>) {
  return response
}

test.describe('P0 backtest result contract recovery red gates', () => {
  test('UI-01 defaults the equity comparison to exactly the best strategy', async ({ page }) => {
    await page.goto(metricsUrl)
    const plot = page.locator('.js-plotly-plot').first()
    await expect(plot).toBeVisible()
    const strategyCurveCount = await plot.evaluate((node: any) =>
      (node.data || []).filter((trace: any) =>
        String(trace?.mode || '').includes('lines') && trace?.line?.dash !== 'dash',
      ).length,
    )
    expect(strategyCurveCount).toBe(expected.default_curve_count)
  })

  test('UI-02 carries the configured benchmark through metrics and the chart toggle', async ({ request }) => {
    const response = await request.get(`/api/app/metrics/${runId}/overview`)
    expect(response.ok()).toBeTruthy()
    const payload = await response.json()
    const row = payload.rows.find((item: any) => item.backtest_id === candidateId)
    expect.soft(payload.benchmark_series).not.toBeNull()
    expect.soft(payload.benchmark_series?.x?.length || 0).toBe(expected.candidate_equity_rows)
    expect.soft(row.bah_total_return).not.toBe(row.total_return)
  })

  test('UI-03 keeps trade markers off by default and overlays events on demand', async ({ page }) => {
    await page.goto(metricsUrl)
    const toggle = page.getByRole('button', { name: /交易標記|Trade Markers/ })
    await expect(toggle).toBeVisible()
    await expect(toggle).toHaveAttribute('aria-pressed', 'false')
    const defaultMarkerTraceCount = await page.locator('.js-plotly-plot').first().evaluate((node: any) =>
      (node.data || []).filter((trace: any) => trace?.meta?.lo2cin4TradeMarker).length,
    )
    expect(defaultMarkerTraceCount).toBe(0)

    await toggle.click()
    await expect(toggle).toHaveAttribute('aria-pressed', 'true')
    await expect.poll(async () => page.locator('.js-plotly-plot').first().evaluate((node: any) =>
      (node.data || []).filter((trace: any) => trace?.meta?.lo2cin4TradeMarker).length,
    )).toBeGreaterThan(0)
  })

  test('UI-04 reconciles the selected summary with the canonical candidate result', async ({ request }) => {
    const payload = await (await request.get(`/api/app/metrics/${runId}/overview`)).json()
    const row = payload.rows.find((item: any) => item.backtest_id === candidateId)
    expect.soft(payload.result_type).toBe(expected.result_type)
    expect.soft(row.semantic_combo).toEqual(expected.candidate_semantic_combo)
    expect.soft(row.trade_count).toBe(expected.candidate_rebalance_rows)
    expect.soft(row.total_return).toBeCloseTo(expected.candidate_total_return, 10)
    expect.soft(row.cagr).toBeCloseTo(expected.candidate_cagr, 10)
    expect.soft(row.sharpe).toBeCloseTo(expected.candidate_sharpe, 10)
    expect.soft(row.max_drawdown).toBeCloseTo(expected.candidate_max_drawdown, 10)
  })

  test('UI-05 projects the complete parameter matrix search space into the heatmap', async ({ request }) => {
    const payload = await (await request.get(`/api/app/metrics/${runId}/parameter-matrix`)).json()
    expect.soft(payload.rows).toHaveLength(expected.matrix_variant_count)
    expect.soft(payload.axis_values[expected.x_axis_name]).toHaveLength(expected.x_axis_count)
    expect.soft(payload.axis_values[expected.y_axis_name]).toHaveLength(expected.y_axis_count)
  })

  test('UI-06 contains long candidate identifiers inside their diagnostic card', async ({ page }) => {
    await page.goto(matrixUrl)
    const hero = page.locator('.research-diagnostics-hero').first()
    await expect(hero).toBeVisible()
    const box = await hero.evaluate((node) => ({
      clientWidth: node.clientWidth,
      scrollWidth: node.scrollWidth,
      overflowWrap: getComputedStyle(node).overflowWrap,
      wordBreak: getComputedStyle(node).wordBreak,
    }))
    expect.soft(box.scrollWidth).toBeLessThanOrEqual(box.clientWidth)
    expect.soft(box.overflowWrap).toBe('anywhere')
    expect.soft(box.wordBreak).not.toBe('normal')
  })

  test('UI-07 exposes normalized portfolio headline metrics instead of n/a', async ({ request }) => {
    const payload = await (
      await request.get(`/api/app/backtests/${runId}/${encodeURIComponent(candidateId)}`)
    ).json()
    expect.soft(payload.result_type).toBe(expected.result_type)
    expect.soft(payload.metrics_matrix.total_return).toBeCloseTo(expected.candidate_total_return, 10)
    expect.soft(payload.metrics_matrix.trade_count).toBe(expected.candidate_rebalance_rows)
    expect.soft(payload.parameter_summary).toEqual(expected.candidate_semantic_combo)
  })

  test('UI-08 returns benchmark and diagnostics without a synthetic entry-exit NAV chart', async ({ page, request }) => {
    const payload = await (
      await request.get(`/api/app/backtests/${runId}/${encodeURIComponent(candidateId)}`)
    ).json()
    expect.soft(payload.benchmark_series).toHaveLength(expected.candidate_equity_rows)
    expect.soft(Object.keys(payload.risk_diagnostics || {}).length).toBeGreaterThan(0)
    await page.goto(detailUrl)
    await expect.soft(page.getByText('入場與出場', { exact: true })).toHaveCount(0)
    const plotCount = await page.locator('.js-plotly-plot').count()
    expect.soft(plotCount).toBeGreaterThan(1)
  })

  test('UI-09 preserves trades, returns, allocation, rebalance and contribution diagnostics', async ({ request }) => {
    const payload = await (
      await request.get(`/api/app/backtests/${runId}/${encodeURIComponent(candidateId)}`)
    ).json()
    expect.soft(payload.holding_rows).toHaveLength(expected.candidate_holding_rows)
    expect.soft(payload.allocation_change_rows).toHaveLength(expected.candidate_rebalance_trade_rows)
    expect.soft(payload.rebalance_rows).toHaveLength(expected.candidate_rebalance_rows)
    expect.soft(payload.trade_rows.length).toBeGreaterThan(0)
    expect.soft(payload.monthly_return_rows.length).toBeGreaterThan(0)
    expect.soft(payload.yearly_return_rows.length).toBeGreaterThan(0)
    expect.soft(payload.asset_contribution_rows.length).toBeGreaterThan(0)
    expect.soft(payload.drawdown_series.length).toBe(expected.candidate_equity_rows)
    expect.soft(payload.turnover_distribution.length).toBeGreaterThan(0)
  })

  test('UI-10 plots the allocation timeline on contract dates with bounded weights', async ({ page }) => {
    await page.goto(detailUrl)
    const chart = page.getByTestId('allocation-timeline-chart').locator('.js-plotly-plot')
    await expect(chart).toBeVisible()
    const allocation = await chart.evaluate((node: any) => ({
      dates: (node.data || []).flatMap((trace: any) => trace.x || []),
      weights: (node.data || []).flatMap((trace: any) => trace.y || []),
    }))
    expect(allocation.dates.length).toBeGreaterThan(0)
    expect(allocation.dates.every((value: unknown) => /^\d{4}-\d{2}-\d{2}$/.test(String(value)))).toBeTruthy()
    expect(Math.min(...allocation.weights)).toBeGreaterThanOrEqual(0)
    expect(Math.max(...allocation.weights)).toBeLessThanOrEqual(1)
  })

  test('UI-11 renders the portfolio context without missing required metrics', async ({ page, request }) => {
    const payload = await (
      await request.get(`/api/app/backtests/${runId}/${encodeURIComponent(contextCandidateId)}`)
    ).json()
    const required = [
      'avg_holdings', 'avg_gross_exposure', 'avg_turnover', 'rebalance_count',
      'annualized_std', 'sortino', 'calmar', 'max_drawdown_duration_days',
      'recovery_factor', 'skewness', 'kurtosis', 'var_95', 'cvar_95',
      'var_99', 'cvar_99', 'worst_month', 'best_month', 'positive_month_ratio',
      'win_rate', 'profit_factor', 'average_win', 'average_loss',
      'average_win_loss_ratio', 'gross_profit', 'gross_loss',
      'max_consecutive_wins', 'max_consecutive_losses', 'bah_total_return',
      'bah_cagr', 'bah_sharpe', 'benchmark_correlation', 'excess_return',
    ]
    expect(required.filter((key) => payload.metrics_matrix[key] == null)).toEqual([])

    await page.goto(contextDetailUrl)
    const context = page.locator('.section-card').filter({ hasText: '投資組合背景資料' })
    await expect(context).toBeVisible()
    await expect(context.getByText('n/a', { exact: true })).toHaveCount(0)
  })

  test('UI-12 renders finite drawdown data and removes the duplicate asset-price chart', async ({ page }) => {
    await page.goto(contextDetailUrl)
    const chart = page.getByTestId('drawdown-diagnostics-chart').locator('.js-plotly-plot')
    await expect(chart).toBeVisible()
    const values = await chart.evaluate((node: any) => (node.data?.[0]?.y || []))
    expect(values).toHaveLength(expected.candidate_equity_rows)
    expect(values.every((value: unknown) => typeof value === 'number' && Number.isFinite(value))).toBeTruthy()
    expect(Math.max(...values)).toBeLessThanOrEqual(0)
    expect(Math.min(...values)).toBeLessThan(0)
    await expect(page.getByText('資產價格與交易點', { exact: true })).toHaveCount(0)
    await expect(page.getByText('Asset Price & Trade Points', { exact: true })).toHaveCount(0)
  })
})
