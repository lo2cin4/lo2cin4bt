import type { PinnedTradeMarker } from '../tradeMarkers'

type Props = {
  marker: PinnedTradeMarker | null
  language: 'en' | 'zh-Hant'
}

export function TradeMarkerInfoPanel({ marker, language }: Props) {
  const lines = marker?.text
    ? String(marker.text).split(/<br\s*\/?>/i)
    : []
  const visibleLines = lines.filter((line) => line.trim())
  const title = language === 'zh-Hant' ? '交易圖示資料' : 'Trade marker details'
  const emptyText = language === 'zh-Hant'
    ? '移到或點擊圖上的交易圖示，這裡會顯示時間、資產、動作、價格、權重與原因。'
    : 'Hover or click a trade marker to show time, asset, action, price, weight, and reason here.'

  return (
    <div className={`trade-marker-info-panel${visibleLines.length ? '' : ' trade-marker-info-panel-empty'}`} role="status" aria-live="polite">
      <div className="trade-marker-info-panel-title">{title}</div>
      {visibleLines.length ? (
        <div className="trade-marker-info-lines">
          {visibleLines.map((line, index) => (
            <span key={`${line}-${index}`} className="trade-marker-info-line">{line}</span>
          ))}
        </div>
      ) : (
        <div className="trade-marker-info-empty-text">{emptyText}</div>
      )}
    </div>
  )
}
