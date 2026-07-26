import type { Language } from '../i18n'

type BenchmarkToggleButtonProps = {
  visible: boolean
  language: Language
  onChange: (visible: boolean) => void
}

export function BenchmarkToggleButton({ visible, language, onChange }: BenchmarkToggleButtonProps) {
  const label = visible
    ? language === 'zh-Hant'
      ? '顯示基準'
      : 'Show Benchmark'
    : language === 'zh-Hant'
      ? '不顯示基準'
      : 'Hide Benchmark'

  return (
    <button
      type="button"
      className={`text-input text-input-compact benchmark-toggle-button${visible ? ' active' : ''}`}
      aria-pressed={visible}
      aria-label={label}
      onClick={() => onChange(!visible)}
    >
      {label}
    </button>
  )
}
