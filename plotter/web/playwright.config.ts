import { defineConfig } from '@playwright/test'

const executablePath = process.env.LO2CIN4BT_CHROME_PATH?.trim()

export default defineConfig({
  testDir: './e2e',
  fullyParallel: false,
  workers: 1,
  retries: 0,
  timeout: 30_000,
  expect: {
    timeout: 10_000,
  },
  reporter: [['list'], ['html', { outputFolder: 'playwright-report', open: 'never' }]],
  outputDir: 'test-results',
  use: {
    baseURL: process.env.LO2CIN4BT_E2E_BASE_URL || 'http://127.0.0.1:2424',
    browserName: 'chromium',
    launchOptions: executablePath ? { executablePath } : {},
    headless: true,
    screenshot: 'only-on-failure',
    trace: 'retain-on-failure',
  },
})
