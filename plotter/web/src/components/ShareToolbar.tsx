import { useState } from 'react'

import { api } from '../api'
import type { Language } from '../i18n'
import { useAppStore } from '../store'

const copy = {
  en: {
    mosaicOn: 'Mosaic on',
    mosaicOff: 'Mosaic off',
    capture: 'Download all',
    capturing: 'Capturing...',
    captured: '10 PNGs saved',
    captureFailed: 'Capture failed',
  },
  'zh-Hant': {
    mosaicOn: 'Mosaic 已開',
    mosaicOff: 'Mosaic 關閉',
    capture: '一鍵下載',
    capturing: '截圖中…',
    captured: '已儲存 10 張 PNG',
    captureFailed: '截圖失敗',
  },
} as const

export function ShareToolbar() {
  const language = useAppStore((state) => state.language)
  const shareMosaicMode = useAppStore((state) => state.shareMosaicMode)
  const setShareMosaicMode = useAppStore((state) => state.setShareMosaicMode)
  const selectedMetricsRunId = useAppStore((state) => state.selectedMetricsRunId)
  const selectedBacktestId = useAppStore((state) => state.selectedBacktestId)
  const [pressed, setPressed] = useState(false)
  const [captureState, setCaptureState] = useState<'idle' | 'running' | 'done' | 'error'>('idle')
  const [capturePath, setCapturePath] = useState('')
  const labels = copy[language as Language]

  return (
    <div className="share-toolbar">
      <button
        type="button"
        className={`inline-action-button inline-action-button-compact ${shareMosaicMode ? 'active' : ''}`}
        aria-pressed={shareMosaicMode}
        onMouseDown={() => setPressed(true)}
        onMouseUp={() => setPressed(false)}
        onMouseLeave={() => setPressed(false)}
        onClick={() => setShareMosaicMode(!shareMosaicMode)}
      >
        {shareMosaicMode || pressed ? labels.mosaicOn : labels.mosaicOff}
      </button>
      <span className="share-toolbar-divider" aria-hidden="true" />
      <button
        type="button"
        className="inline-action-button inline-action-button-compact"
        disabled={!selectedMetricsRunId || !selectedBacktestId || captureState === 'running'}
        title={capturePath || undefined}
        onClick={async () => {
          setCaptureState('running')
          setCapturePath('')
          try {
            const result = await api.captureScreenshotBundle({
              run_id: selectedMetricsRunId,
              backtest_id: selectedBacktestId,
              mosaic: shareMosaicMode,
            })
            setCapturePath(String(result.output_dir || ''))
            setCaptureState('done')
          } catch {
            setCaptureState('error')
          }
        }}
      >
        {captureState === 'running'
          ? labels.capturing
          : captureState === 'done'
            ? labels.captured
            : captureState === 'error'
              ? labels.captureFailed
              : labels.capture}
      </button>
    </div>
  )
}
