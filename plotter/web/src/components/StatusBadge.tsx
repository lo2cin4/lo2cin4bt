import { useAppStore } from '../store'
import { statusLabel } from '../uiVocabulary'

type Props = {
  status?: string
}

export function StatusBadge({ status }: Props) {
  const language = useAppStore((state) => state.language)
  const normalized = (status || 'unknown').toLowerCase()
  const tone =
    normalized === 'completed'
      ? 'ok'
      : normalized === 'partial'
        ? 'warn'
        : normalized === 'failed'
        ? 'danger'
        : normalized === 'canceled'
          ? 'neutral'
          : normalized === 'canceling'
            ? 'neutral'
          : normalized === 'running'
            ? 'running'
            : 'neutral'
  const label = statusLabel(normalized, language)
  return <span className={`status-badge ${tone}`}>{label}</span>
}
