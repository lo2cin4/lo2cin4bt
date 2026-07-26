import type { MetricsOverviewPayloadV1 } from './contracts.generated'

const API_BASE = '/api/app'

export class AppApiError extends Error {
  status: number
  path: string
  detail: string

  constructor(path: string, status: number, detail: string) {
    super('app_api_request_failed')
    this.name = 'AppApiError'
    this.status = status
    this.path = path
    this.detail = detail
  }
}

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`)
  if (!response.ok) {
    const detail = await response.text()
    throw new AppApiError(path, response.status, detail)
  }
  return response.json() as Promise<T>
}

async function postJson<T>(path: string, payload: unknown): Promise<T> {
  return requestJson<T>(path, 'POST', payload)
}

async function requestJson<T>(path: string, method: string, payload?: unknown): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: payload === undefined ? undefined : JSON.stringify(payload),
  })
  if (!response.ok) {
    const detail = await response.text()
    throw new AppApiError(path, response.status, detail)
  }
  return response.json() as Promise<T>
}

export const api = {
  commandCenter: () => getJson<any>('/command-center'),
  runCenterConfigs: () => getJson<any>('/run-center/configs'),
  openFolder: (payload: { target: string }) =>
    postJson<any>('/folders/open', payload),
  openWorkspace: (payload: { target: string }) =>
    postJson<any>('/workspace/open', payload),
  createBatch: (payload: { module: string; config_paths: string[] }) =>
    postJson<any>('/batches', payload),
  getBatch: (batchId: string) => getJson<any>(`/batches/${batchId}`),
  cancelBatch: (batchId: string) => postJson<any>(`/batches/${batchId}/cancel`, {}),
  metricsRuns: () => getJson<any[]>('/metrics/runs'),
  captureScreenshotBundle: (payload: { run_id: string; backtest_id: string; mosaic: boolean }) =>
    postJson<any>('/screenshots', payload),
  wfaRuns: () => getJson<any[]>('/wfa/runs'),
  statRuns: () => getJson<any[]>('/statanalyser/runs'),
  metricsOverview: (runId: string) =>
    getJson<MetricsOverviewPayloadV1>(`/metrics/${runId}/overview`),
  parameterMatrix: (runId: string) =>
    getJson<any>(`/metrics/${runId}/parameter-matrix`),
  parameterMatrixReviewPreview: (
    runId: string,
    payload: {
      acceptance?: Record<string, unknown>
      ranking?: Record<string, unknown>
    },
  ) => postJson<any>(`/metrics/${runId}/parameter-matrix/review-preview`, payload),
  listParameterReviewTemplates: () =>
    getJson<any>('/parameter-review/templates'),
  saveParameterReviewTemplate: (payload: {
    name: string
    acceptance?: Record<string, unknown>
    ranking?: Record<string, unknown>
  }) => postJson<any>('/parameter-review/templates', payload),
  deleteParameterReviewTemplate: (name: string) =>
    requestJson<any>('/parameter-review/templates', 'DELETE', { name }),
  setDefaultParameterReviewTemplate: (name: string) =>
    requestJson<any>('/parameter-review/templates/default', 'PUT', { name }),
  wfaDashboard: (runId: string) => getJson<any>(`/wfa/${runId}/dashboard`),
  backtestDetail: (runId: string, backtestId: string) =>
    getJson<any>(`/backtests/${runId}/${backtestId}`),
  backtestCsvUrl: (runId: string, backtestId: string) =>
    `${API_BASE}/backtests/${runId}/${backtestId}/export.csv`,
  statanalyserSummary: (runId: string) =>
    getJson<any>(`/statanalyser/${runId}`),
}

export const createBatchSocket = (batchId: string): WebSocket =>
  new WebSocket(`${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}${API_BASE}/batches/${batchId}/stream`)
