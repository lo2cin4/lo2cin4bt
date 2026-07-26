import fs from 'node:fs/promises'
import path from 'node:path'
import process from 'node:process'
import { fileURLToPath } from 'node:url'

import { chromium } from '../plotter/web/node_modules/playwright/index.mjs'

const scriptDir = path.dirname(fileURLToPath(import.meta.url))
const repoRoot = path.resolve(scriptDir, '..')

function argument(name) {
  const index = process.argv.indexOf(name)
  if (index < 0 || index + 1 >= process.argv.length) {
    throw new Error(`Missing required argument: ${name}`)
  }
  return process.argv[index + 1]
}

function routeUrl(baseUrl, route, runId, backtestId, mosaic) {
  const query = new URLSearchParams({
    runId,
    captureMosaic: mosaic ? '1' : '0',
    captureScreenshot: '1',
  })
  if (backtestId) query.set('backtestId', backtestId)
  const pathname = route === 'overview'
    ? '/metrics'
    : route === 'parameter_matrix'
      ? '/metrics/parameter-matrix'
      : '/metrics/backtests'
  return `${baseUrl}${pathname}?${query.toString()}`
}

async function waitForCaptureReady(page, sectionIds) {
  await page.waitForFunction((ids) => ids.every((id) => {
    const element = document.querySelector(`[data-screenshot-section="${id}"]`)
    return element && element.getBoundingClientRect().height > 0
  }), sectionIds, { timeout: 30_000 })
  await page.waitForFunction(() => !document.querySelector('.page-loading'), null, { timeout: 30_000 })
  await page.waitForFunction(() => Array.from(document.querySelectorAll('.js-plotly-plot')).every((plot) => Array.isArray(plot.data)), null, { timeout: 30_000 })
  await page.waitForTimeout(300)
}

async function captureSectionGroup(page, sectionIds, outputPath) {
  await waitForCaptureReady(page, sectionIds)
  if (sectionIds.length !== 1) {
    throw new Error('Every screenshot output must map to one explicit section container')
  }
  const locator = page.locator(`[data-screenshot-section="${sectionIds[0]}"]`)
  if (await locator.count() !== 1) {
    throw new Error(`Expected exactly one screenshot section: ${sectionIds[0]}`)
  }

  const originalViewport = page.viewportSize()
  if (!originalViewport) {
    throw new Error('Screenshot capture requires an explicit viewport')
  }

  // Chromium otherwise tiles elements taller than the viewport. Fixed and
  // translucent layers are recomposited for every tile, leaving visible bands.
  const measuredHeight = await locator.evaluate((element) => Math.ceil(Math.max(
    element.getBoundingClientRect().height,
    element.scrollHeight,
  )))
  const captureHeight = measuredHeight + 64
  if (captureHeight > 32_000) {
    throw new Error(`Screenshot section is too tall for a single render pass: ${measuredHeight}px`)
  }

  try {
    if (captureHeight > originalViewport.height) {
      await page.setViewportSize({ width: originalViewport.width, height: captureHeight })
      await page.waitForTimeout(100)
    }
    await locator.screenshot({ path: outputPath, animations: 'disabled' })
  } finally {
    if (page.viewportSize()?.height !== originalViewport.height) {
      await page.setViewportSize(originalViewport)
    }
  }
}

async function main() {
  const baseUrl = argument('--base-url').replace(/\/$/, '')
  const runId = argument('--run-id')
  const backtestId = argument('--backtest-id')
  const outputDir = path.resolve(argument('--output-dir'))
  const mosaic = argument('--mosaic') === 'true'
  const contractPath = path.join(repoRoot, 'app', 'contracts', 'screenshot-bundle-v1.contract.json')
  const contract = JSON.parse(await fs.readFile(contractPath, 'utf8'))
  const chromeCandidates = [
    process.env.LO2CIN4BT_CHROME_PATH,
    'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
    'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
  ].filter(Boolean)
  let executablePath
  for (const candidate of chromeCandidates) {
    try {
      await fs.access(candidate)
      executablePath = candidate
      break
    } catch {
      // Continue to Playwright's bundled browser when available.
    }
  }
  await fs.mkdir(outputDir, { recursive: true })
  const browser = await chromium.launch({ headless: true, ...(executablePath ? { executablePath } : {}) })
  const context = await browser.newContext({ viewport: { width: 1920, height: 1080 }, deviceScaleFactor: 1 })
  const page = await context.newPage()
  const captured = []
  try {
    let activeRoute = ''
    for (const capture of contract.captures) {
      if (capture.route !== activeRoute) {
        await page.goto(routeUrl(baseUrl, capture.route, runId, backtestId, mosaic), { waitUntil: 'networkidle', timeout: 30_000 })
        activeRoute = capture.route
      }
      const outputPath = path.join(outputDir, capture.filename)
      try {
        await captureSectionGroup(page, capture.sections, outputPath)
      } catch (error) {
        throw new Error(`Unable to capture ${capture.filename}: ${error?.message || error}`)
      }
      captured.push(capture.filename)
    }
  } finally {
    await browser.close()
  }
  process.stdout.write(JSON.stringify({ captured }))
}

main().catch((error) => {
  process.stderr.write(String(error?.stack || error))
  process.exitCode = 1
})
