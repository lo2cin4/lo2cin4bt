import { PropsWithChildren, type ReactNode } from 'react'

type Props = PropsWithChildren<{
  title?: string
  subtitle?: ReactNode
  actions?: ReactNode
  captureId?: string
}>

export function SectionCard({ title, subtitle, actions, captureId, children }: Props) {
  const hasHeader = Boolean(title || subtitle || actions)
  return (
    <section className="section-card" data-screenshot-section={captureId}>
      {hasHeader ? (
        <div className="section-header">
          <div>
            {title ? <div className="section-title">{title}</div> : null}
            {subtitle ? <div className="section-subtitle">{subtitle}</div> : null}
          </div>
          {actions ? <div className="section-actions">{actions}</div> : null}
        </div>
      ) : null}
      <div className="section-body">{children}</div>
    </section>
  )
}
