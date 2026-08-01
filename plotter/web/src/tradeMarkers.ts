type EquityPoint = {
  time?: unknown
  value?: unknown
}

type TradeMarkerOptions = {
  equitySeries: EquityPoint[]
  tradeRows: any[]
  equityScale: 'linear' | 'log'
  chartValue: (value: unknown, scale: 'linear' | 'log') => number | null
  language: 'en' | 'zh-Hant'
  includePrice?: boolean
}

type PriceTradeMarkerOptions = {
  tradeRows: any[]
  language: 'en' | 'zh-Hant'
}

type MarkerPoint = {
  x: string
  y: number
  text: string
}

export type PinnedTradeMarker = {
  x: unknown
  y: unknown
  text: string
}

const BUY_ACTIONS = new Set(['buy', 'enter', 'entry', 'increase', 'add', 'new_long', 'cover'])
const SELL_ACTIONS = new Set(['sell', 'exit', 'close', 'flatten', 'reduce', 'decrease', 'close_short'])

function firstValue(row: any, keys: string[]) {
  for (const key of keys) {
    const value = row?.[key]
    if (value !== undefined && value !== null && String(value).trim() !== '') return value
  }
  return null
}

function numericValue(value: unknown): number | null {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function timestampKey(value: unknown): string {
  const raw = String(value ?? '').trim()
  if (!raw) return ''
  const parsed = Date.parse(raw)
  return Number.isFinite(parsed) ? `ms:${parsed}` : `raw:${raw}`
}

function timeValue(value: unknown): number | null {
  const raw = String(value ?? '').trim()
  if (!raw) return null
  const parsed = Date.parse(raw)
  return Number.isFinite(parsed) ? parsed : null
}

function displayTime(row: any): string {
  const explicit = String(firstValue(row, ['Event_timestamp_local', 'event_timestamp_local']) ?? '').trim()
  if (explicit) return explicit
  const raw = String(firstValue(row, ['Time', 'time', 'entry_time', 'exit_time']) ?? '').trim()
  if (!raw) return '-'
  return raw.includes('T') ? raw.replace('T', ' ') : raw
}

function tradePrice(row: any): number | null {
  const candidates = [
    'Fill_price',
    'fill_price',
    'Trade_price',
    'trade_price',
    'Market_price',
    'market_price',
    'Asset_price',
    'asset_price',
    'Event_price',
    'event_price',
    'Execution_price',
    'execution_price',
    'Exit_price',
    'exit_price',
    'Entry_price',
    'entry_price',
    'Close_price',
    'close_price',
    'Open_price',
    'open_price',
    'Price',
    'price',
  ]
  const values = candidates
    .map((key) => numericValue(row?.[key]))
    .filter((value): value is number => value !== null)
  return values.find((value) => Math.abs(value) > 1e-12) ?? null
}

function tradeSide(row: any): 'buy' | 'sell' | null {
  const action = String(firstValue(row, ['Action', 'action', 'Trade_action', 'trade_action']) ?? '').trim().toLowerCase()
  if (BUY_ACTIONS.has(action)) return 'buy'
  if (SELL_ACTIONS.has(action)) return 'sell'
  const before = numericValue(firstValue(row, ['Before_weight', 'before_weight']))
  const target = numericValue(firstValue(row, ['Target_weight', 'target_weight']))
  const delta = numericValue(firstValue(row, ['Trade_delta', 'trade_delta']))
  const inferredDelta = delta ?? (before !== null && target !== null ? target - before : null)
  if (inferredDelta === null || Math.abs(inferredDelta) < 1e-12) return null
  return inferredDelta > 0 ? 'buy' : 'sell'
}

function markerText(row: any, side: 'buy' | 'sell', language: 'en' | 'zh-Hant', includePrice = true): string {
  const asset = String(firstValue(row, ['Asset', 'asset', 'Symbol', 'symbol']) ?? '-')
  const action = String(firstValue(row, ['Action', 'action']) ?? (side === 'buy' ? 'buy' : 'sell'))
  const price = tradePrice(row)
  const before = numericValue(firstValue(row, ['Before_weight', 'before_weight']))
  const target = numericValue(firstValue(row, ['Target_weight', 'target_weight']))
  const phase = String(firstValue(row, ['Event_phase', 'event_phase', 'Phase', 'phase']) ?? '').trim()
  const timeLabel = language === 'zh-Hant' ? '時間' : 'Time'
  const assetLabel = language === 'zh-Hant' ? '資產' : 'Asset'
  const actionLabel = language === 'zh-Hant' ? '動作' : 'Action'
  const priceLabel = language === 'zh-Hant' ? '價格' : 'Price'
  const weightLabel = language === 'zh-Hant' ? '權重' : 'Weight'
  const phaseLabel = language === 'zh-Hant' ? '時點' : 'Phase'
  const lines = [
    `${timeLabel}: ${displayTime(row)}`,
    `${assetLabel}: ${asset}`,
    `${actionLabel}: ${action}`,
  ]
  if (includePrice) lines.push(`${priceLabel}: ${price === null ? '-' : price.toFixed(4)}`)
  if (before !== null || target !== null) {
    lines.push(`${weightLabel}: ${before === null ? '-' : `${(before * 100).toFixed(1)}%`} -> ${target === null ? '-' : `${(target * 100).toFixed(1)}%`}`)
  }
  if (phase) lines.push(`${phaseLabel}: ${phase}`)
  return lines.join('<br>')
}

function compactMarkersByTimestamp(markers: MarkerPoint[], language: 'en' | 'zh-Hant'): MarkerPoint[] {
  const byTimestamp = new Map<string, MarkerPoint & { count: number }>()
  for (const marker of markers) {
    const key = timestampKey(marker.x)
    const existing = byTimestamp.get(key)
    if (!existing) {
      byTimestamp.set(key, { ...marker, count: 1 })
      continue
    }
    existing.count += 1
    existing.text = `${existing.text}<br><br>${marker.text}`
  }
  return [...byTimestamp.values()].map((marker) => ({
    x: marker.x,
    y: marker.y,
    text: marker.count > 1
      ? `${language === 'zh-Hant' ? '同一時點交易' : 'Same-timestamp trades'}: ${marker.count}<br><br>${marker.text}`
      : marker.text,
  }))
}

export function buildTradeMarkerTraces({
  equitySeries,
  tradeRows,
  equityScale,
  chartValue,
  language,
  includePrice = true,
}: TradeMarkerOptions): any[] {
  if (!Array.isArray(equitySeries) || !equitySeries.length || !Array.isArray(tradeRows) || !tradeRows.length) {
    return []
  }
  const equityByTimestamp = new Map<string, MarkerPoint>()
  const equityPoints: Array<MarkerPoint & { t: number }> = []
  for (const point of equitySeries) {
    const key = timestampKey(point.time)
    const y = chartValue(point.value, equityScale)
    if (!key || y === null || !Number.isFinite(Number(y))) continue
    const markerPoint = { x: String(point.time ?? key), y: Number(y), text: '' }
    equityByTimestamp.set(key, markerPoint)
    const t = timeValue(point.time)
    if (t !== null) equityPoints.push({ ...markerPoint, t })
  }
  equityPoints.sort((left, right) => left.t - right.t)

  const nearestEquityPoint = (rawTime: unknown): MarkerPoint | null => {
    const key = timestampKey(rawTime)
    const exact = equityByTimestamp.get(key)
    if (exact) return exact
    const target = timeValue(rawTime)
    if (target === null || !equityPoints.length) return null
    let best = equityPoints[0]
    let bestDistance = Math.abs(best.t - target)
    for (const point of equityPoints) {
      const distance = Math.abs(point.t - target)
      if (distance < bestDistance) {
        best = point
        bestDistance = distance
      }
      if (point.t > target && distance > bestDistance) break
    }
    return best
  }

  const buys: MarkerPoint[] = []
  const sells: MarkerPoint[] = []
  for (const row of tradeRows) {
    const side = tradeSide(row)
    if (!side) continue
    const rawTime = firstValue(row, ['Time', 'time', 'entry_time', 'exit_time'])
    const equityPoint = nearestEquityPoint(rawTime)
    if (!equityPoint) continue
    const marker = {
      x: String(rawTime ?? equityPoint.x),
      y: equityPoint.y,
      text: markerText(row, side, language, includePrice),
    }
    if (side === 'buy') buys.push(marker)
    else sells.push(marker)
  }

  const compactBuys = compactMarkersByTimestamp(buys, language)
  const compactSells = compactMarkersByTimestamp(sells, language)
  const traces: any[] = []
  if (compactBuys.length) {
    traces.push({
      type: 'scatter',
      mode: 'markers',
      name: language === 'zh-Hant' ? '開倉 / 加倉' : 'Open / Add',
      x: compactBuys.map((item) => item.x),
      y: compactBuys.map((item) => item.y),
      text: compactBuys.map((item) => item.text),
      marker: { color: '#58e6b4', size: 10, symbol: 'triangle-up', line: { color: '#d7fff4', width: 1 } },
      meta: { lo2cin4TradeMarker: true },
      hoverinfo: 'none',
    })
  }
  if (compactSells.length) {
    traces.push({
      type: 'scatter',
      mode: 'markers',
      name: language === 'zh-Hant' ? '平倉 / 減倉' : 'Close / Reduce',
      x: compactSells.map((item) => item.x),
      y: compactSells.map((item) => item.y),
      text: compactSells.map((item) => item.text),
      marker: { color: '#ff8a7a', size: 10, symbol: 'triangle-down', line: { color: '#ffe2dc', width: 1 } },
      meta: { lo2cin4TradeMarker: true },
      hoverinfo: 'none',
    })
  }
  return traces
}

export function buildPriceTradeMarkerTraces({
  tradeRows,
  language,
}: PriceTradeMarkerOptions): any[] {
  if (!Array.isArray(tradeRows) || !tradeRows.length) return []
  const buys: MarkerPoint[] = []
  const sells: MarkerPoint[] = []
  for (const row of tradeRows) {
    const side = tradeSide(row)
    if (!side) continue
    const rawTime = firstValue(row, ['Time', 'time', 'entry_time', 'exit_time'])
    const price = tradePrice(row)
    if (!rawTime || price === null) continue
    const marker = {
      x: String(rawTime),
      y: price,
      text: markerText(row, side, language, true),
    }
    if (side === 'buy') buys.push(marker)
    else sells.push(marker)
  }

  const traces: any[] = []
  if (buys.length) {
    traces.push({
      type: 'scatter',
      mode: 'markers',
      name: language === 'zh-Hant' ? '開倉 / 加倉' : 'Open / Add',
      x: buys.map((item) => item.x),
      y: buys.map((item) => item.y),
      text: buys.map((item) => item.text),
      marker: { color: '#58e6b4', size: 11, symbol: 'triangle-up', line: { color: '#d7fff4', width: 1.2 } },
      meta: { lo2cin4TradeMarker: true },
      hoverinfo: 'none',
    })
  }
  if (sells.length) {
    traces.push({
      type: 'scatter',
      mode: 'markers',
      name: language === 'zh-Hant' ? '平倉 / 減倉' : 'Close / Reduce',
      x: sells.map((item) => item.x),
      y: sells.map((item) => item.y),
      text: sells.map((item) => item.text),
      marker: { color: '#ff8a7a', size: 11, symbol: 'triangle-down', line: { color: '#ffe2dc', width: 1.2 } },
      meta: { lo2cin4TradeMarker: true },
      hoverinfo: 'none',
    })
  }
  return traces
}

export function pinnedTradeMarkerFromPlotClick(event: any): PinnedTradeMarker | null {
  const points = Array.isArray(event?.points) ? event.points : []
  const point = points.find((item: any) => item?.data?.meta?.lo2cin4TradeMarker)
  if (!point) return null
  return {
    x: point.x,
    y: point.y,
    text: String(point.text || ''),
  }
}
