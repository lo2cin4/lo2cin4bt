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
        schema_version: '1.28',
        contract_id: 'lo2cin4bt-app-metrics-overview-payload-v1',
        projection_source: 'validated_json_contracts',
        run_id: runId,
        result_type: 'portfolio',
        portfolio: { runs: [] },
        strategy_summary: {
          display_label: 'E2E Contract Run',
          symbol: 'QQQ',
          execution_stream_id: 'execution_1m',
          execution_bar_spec: {
            aggregation: 'time',
            step: 1,
            unit: 'minute',
            price_type: 'last',
            alignment: 'session_open',
          },
          decision_stream_id: 'decision_5m',
          decision_bar_spec: {
            aggregation: 'time',
            step: 5,
            unit: 'minute',
            price_type: 'last',
            alignment: 'session_open',
          },
          frequency_label: '1 minute',
          decision_frequency_label: '5 minutes',
          time_context: {
            execution: {
              stream_id: 'execution_1m',
              role: 'execution',
              source: { kind: 'external', provider_id: 'fixture' },
              bar_spec: {
                aggregation: 'time',
                step: 1,
                unit: 'minute',
                price_type: 'last',
                alignment: 'session_open',
              },
              timestamp_semantics: {
                timestamp_convention: 'bar_close',
                interval_boundary: 'left_open_right_closed',
                bar_open_time_column: 'bar_open_timestamp',
                bar_close_time_column: 'bar_close_timestamp',
                available_time_column: 'available_timestamp',
                session_label_column: 'session_label',
                availability_policy: 'bar_close',
              },
            },
            decision: {
              stream_id: 'decision_5m',
              role: 'decision',
              source: {
                kind: 'derived',
                parent_stream_id: 'execution_1m',
                aggregation_engine: 'shared_rust',
              },
              bar_spec: {
                aggregation: 'time',
                step: 5,
                unit: 'minute',
                price_type: 'last',
                alignment: 'session_open',
              },
              timestamp_semantics: {
                timestamp_convention: 'bar_close',
                interval_boundary: 'left_open_right_closed',
                bar_open_time_column: 'bar_open_timestamp',
                bar_close_time_column: 'bar_close_timestamp',
                available_time_column: 'available_timestamp',
                session_label_column: 'session_label',
                availability_policy: 'bar_close',
              },
            },
            session: {
              calendar_id: 'XNYS',
              timezone: 'America/New_York',
              session_scope: 'regular',
              session_label_policy: 'exchange_local_date',
            },
            timestamp: { time_standard: 'UTC' },
          },
          annualization: {
            schema_version: 'metrics_annualization.v1',
            basis: 'session_close_projection',
            projection_policy: 'last_accepted_equity_per_session',
            periods_per_year: 252,
            risk_free_rate_annual: 0.04,
          },
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
          date_range_start: '2024-07-03T13:31:00Z',
          date_range_end: '2024-07-03T13:35:00Z',
        }],
        series: [{
          backtest_id: 'candidate-1',
          label: 'Candidate 1',
          x: ['2024-07-03T13:31:00Z', '2024-07-03T13:35:00Z'],
          y: [1, 1.05],
        }],
        annualization: {
          schema_version: 'metrics_annualization.v1',
          basis: 'session_close_projection',
          projection_policy: 'last_accepted_equity_per_session',
          periods_per_year: 252,
          risk_free_rate_annual: 0.04,
        },
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
  await expect(page.getByTestId('time-context-panel')).toContainText('execution_1m')
  await expect(page.getByTestId('time-context-panel')).toContainText('decision_5m')
  await expect(page.getByTestId('time-context-panel')).toContainText('session_close_projection')
  const chartX = await page.locator('.js-plotly-plot').first().evaluate(
    (element: any) => element.data[0].x,
  )
  expect(chartX).toEqual(['2024-07-03T13:31:00Z', '2024-07-03T13:35:00Z'])
  await expect(page.locator('.portfolio-flow-value')).toHaveText(
    '排名依據：調整後動能分數；排名順序：由高至低；'
    + '做多：選取最強 2 項；做空：選取最弱 2 項；同分處理：按資產代號排序',
  )
})

test('backtest allocation timeline keeps distinct intraday timestamps', async ({ page }) => {
  const runId = 'e2e-intraday-detail'
  const backtestId = 'candidate-a'
  const timeContext = {
    execution: {
      stream_id: 'execution_1m',
      role: 'execution',
      source: { kind: 'external', provider_id: 'fixture' },
      bar_spec: {
        aggregation: 'time',
        step: 1,
        unit: 'minute',
        price_type: 'last',
        alignment: 'session_open',
      },
      timestamp_semantics: {
        timestamp_convention: 'bar_close',
        interval_boundary: 'left_open_right_closed',
        availability_policy: 'bar_close',
      },
    },
    decision: {
      stream_id: 'decision_5m',
      role: 'decision',
      source: {
        kind: 'derived',
        parent_stream_id: 'execution_1m',
        aggregation_engine: 'shared_rust',
      },
      bar_spec: {
        aggregation: 'time',
        step: 5,
        unit: 'minute',
        price_type: 'last',
        alignment: 'session_open',
      },
      timestamp_semantics: {
        timestamp_convention: 'bar_close',
        interval_boundary: 'left_open_right_closed',
        availability_policy: 'bar_close',
      },
    },
    session: {
      calendar_id: 'XNYS',
      timezone: 'America/New_York',
      session_scope: 'regular',
      session_label_policy: 'exchange_local_date',
    },
    timestamp: { time_standard: 'UTC' },
  }
  const annualization = {
    schema_version: 'metrics_annualization.v1',
    basis: 'session_close_projection',
    projection_policy: 'last_accepted_equity_per_session',
    periods_per_year: 252,
    risk_free_rate_annual: 0.04,
  }
  const strategySummary = {
    asset_label: 'QQQ',
    mode_label: 'Selection Timing Portfolio',
    workflow_label: 'Single Backtest',
    execution_label: 'next event open',
    cost_label: 'transaction_cost=0.001',
    entry_rule: 'signal',
    exit_rule: 'signal',
    parameter_domain_label: '-',
    time_context: timeContext,
    annualization,
  }
  await page.route('**/api/app/metrics/runs', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify([
        { run_id: runId, display_label: 'Intraday Detail', status: 'completed' },
      ]),
    })
  })
  await page.route(`**/api/app/metrics/${runId}/overview`, async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        schema_version: '1.28',
        contract_id: 'lo2cin4bt-app-metrics-overview-payload-v1',
        run_id: runId,
        result_type: 'portfolio',
        strategy_summary: strategySummary,
        annualization,
        rows: [{ backtest_id: backtestId, label: 'Candidate A' }],
        series: [],
        categories: {},
        available_categories: [],
      }),
    })
  })
  await page.route(`**/api/app/backtests/${runId}/${backtestId}`, async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        api_projection_schema_version: 'backtest_detail_api.v2',
        contract_id: 'lo2cin4bt-app-portfolio-detail-payload-v1',
        result_type: 'portfolio',
        run_id: runId,
        backtest_id: backtestId,
        label: 'Candidate A',
        strategy_summary: strategySummary,
        time_context: timeContext,
        annualization,
        metrics_matrix: {},
        data_quality: { status: 'valid' },
        turnover_summary: {},
        equity_series: [
          { time: '2024-07-03T13:31:00Z', value: 1 },
          { time: '2024-07-03T13:35:00Z', value: 1.01 },
        ],
        holding_rows: [
          { time: '2024-07-03T13:31:00Z', asset: 'QQQ', target_weight: 1 },
          { time: '2024-07-03T13:35:00Z', asset: 'QQQ', target_weight: 0.5 },
        ],
        portfolio_visual_availability: { allocation_timeline: true },
        rebalance_rows: [],
        allocation_change_rows: [],
        asset_contribution_rows: [],
        drawdown_series: [],
        turnover_distribution: [],
        monthly_return_rows: [],
        yearly_return_rows: [],
      }),
    })
  })

  await page.goto(
    `/metrics/backtests?runId=${runId}&backtestId=${backtestId}`,
  )
  await expectAppShell(page)
  await expect(page.getByTestId('time-context-panel')).toContainText('decision_5m')
  const allocationPlot = page
    .getByTestId('allocation-timeline-chart')
    .locator('.js-plotly-plot')
  await expect(allocationPlot).toBeVisible({ timeout: 20_000 })
  const chartX = await allocationPlot.evaluate(
    (element: any) => element.data[0].x,
  )
  expect(chartX).toEqual([
    '2024-07-03T13:31:00Z',
    '2024-07-03T13:35:00Z',
  ])
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
