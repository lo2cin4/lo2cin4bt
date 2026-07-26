import { expect, test, type Page } from '@playwright/test'

const ROUTES = [
  '/',
  '/run-center',
  '/metrics',
  '/wfa',
  '/metrics/backtests',
  '/metrics/parameter-matrix',
] as const

async function expectAppShell(page: Page) {
  await expect(page.locator('.app-shell')).toBeVisible()
  await expect(page.locator('.app-sidebar')).toBeVisible()
  await expect(page.locator('.app-main')).toBeVisible()
  await expect(page.locator('.brand-title')).toHaveText('lo2cin4bt')
  await expect(page.locator('.page-loading')).toHaveCount(0, { timeout: 15_000 })
}

for (const route of ROUTES) {
  test('production route ' + route + ' renders without a blank screen or runtime error', async ({ page }) => {
    const pageErrors: string[] = []
    page.on('pageerror', (error) => pageErrors.push(error.message))
    await page.goto(route)
    await expectAppShell(page)
    await expect(page.locator('.page-error')).toHaveCount(0)
    expect(pageErrors).toEqual([])
  })
}

test('sidebar navigation and language interaction update the visible application', async ({ page }) => {
  await page.goto('/')
  await expectAppShell(page)

  const runCenterLink = page.getByRole('link', { name: '執行中心' })
  await expect(runCenterLink).toHaveCount(1)
  await runCenterLink.click()
  await expect(page).toHaveURL(/\/run-center$/)
  await expectAppShell(page)

  const englishButton = page.getByRole('button', { name: 'EN', exact: true })
  await expect(englishButton).toHaveCount(1)
  await englishButton.click()
  await expect(page.getByRole('link', { name: 'Run Center', exact: true })).toBeVisible()
})

test('metrics renders a real Plotly chart from the public production contract', async ({ page }) => {
  const runId = 'e2e-contract-run'
  await page.route('**/api/app/metrics/runs', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify([{ run_id: runId, display_label: 'E2E Contract Run', status: 'completed' }]),
    })
  })
  await page.route('**/api/app/metrics/' + runId + '/overview', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        schema_version: '1.27',
        contract_id: 'lo2cin4bt-app-metrics-overview-payload-v1',
        projection_source: 'validated_json_contracts',
        run_id: runId,
        result_type: 'portfolio',
        portfolio: { runs: [] },
        strategy_summary: {
          display_label: 'E2E Contract Run',
          symbol: 'QQQ',
          frequency: '1D',
          logic_steps: [{
            kind: 'Selection',
            label: 'selection',
            detail: (
              'rank by: adjusted momentum score; rank order: desc; '
              + 'long top n: 2; short bottom n: 2; tie breaker: symbol'
            ),
          }],
        },
        default_category: 'top_20_sharpe',
        available_categories: [{ id: 'top_20_sharpe', label: 'Top 20 Sharpe' }],
        rows: [{
          backtest_id: 'candidate-1',
          label: 'Candidate 1',
          sharpe: 1.2,
          total_return: 0.15,
          cagr: 0.1,
          max_drawdown: -0.08,
          trade_count: 4,
          exposure_time: 0.7,
          profit_factor: 1.5,
          date_range_start: '2024-01-01',
          date_range_end: '2024-01-03',
        }],
        series: [{
          backtest_id: 'candidate-1',
          label: 'Candidate 1',
          x: ['2024-01-01', '2024-01-02', '2024-01-03'],
          y: [1, 1.05, 1.1],
        }],
        benchmark_series: null,
        categories: { top_20_sharpe: ['candidate-1'] },
        generated_at: '2026-07-12T00:00:00Z',
        source_hashes: ['a'.repeat(64)],
        artifact_source_refs: [],
      }),
    })
  })
  await page.goto('/metrics')
  await expectAppShell(page)
  await expect(page.locator('.js-plotly-plot')).toHaveCount(1, { timeout: 20_000 })
  await expect(page.locator('.js-plotly-plot')).toBeVisible()
  expect(await page.locator('.js-plotly-plot svg.main-svg').count()).toBeGreaterThan(0)
  await expect(page.locator('.portfolio-flow-value')).toHaveText(
    '排名依據：調整後動能分數；排名順序：由高至低；'
    + '做多：選取最強 2 項；做空：選取最弱 2 項；同分處理：按資產代號排序',
  )
})

test('empty and error states render through the same production UI contracts', async ({ page }) => {
  await page.route('**/api/app/wfa/runs', async (route) => {
    await route.fulfill({ status: 200, contentType: 'application/json', body: '[]' })
  })
  await page.goto('/wfa')
  await expectAppShell(page)
  await expect(page.locator('.missing-state')).toBeVisible()

  await page.unroute('**/api/app/wfa/runs')
  await page.route('**/api/app/command-center', async (route) => {
    await route.fulfill({
      status: 500,
      contentType: 'application/json',
      body: JSON.stringify({ detail: 'forced e2e contract failure' }),
    })
  })
  await page.goto('/')
  await expect(page.locator('.app-shell')).toBeVisible()
  await expect(page.locator('.page-error')).toBeVisible()
})
