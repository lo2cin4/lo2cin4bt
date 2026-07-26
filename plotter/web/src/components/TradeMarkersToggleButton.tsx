import type { Language } from '../i18n'

type TradeMarkersToggleButtonProps = {
  visible: boolean
  language: Language
  onChange: (visible: boolean) => void
}

export function TradeMarkersToggleButton({ visible, language, onChange }: TradeMarkersToggleButtonProps) {
  const stateLabel = visible
    ? language === 'zh-Hant' ? '交易標記：開' : 'Trade Markers: On'
    : language === 'zh-Hant' ? '交易標記：關' : 'Trade Markers: Off'

  return (
    <button
      type="button"
      className={`text-input text-input-compact benchmark-toggle-button trade-markers-toggle-button${visible ? ' active' : ''}`}
      aria-pressed={visible}
      aria-label={stateLabel}
      onClick={() => onChange(!visible)}
    >
      {stateLabel}
    </button>
  )
}
