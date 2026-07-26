import { useRef, useState } from 'react'
import { createPortal } from 'react-dom'

type InfoHintProps = {
  label: string
  body: string
  side?: 'left' | 'right'
}

type Position = {
  top: number
  left: number
}

const POPOVER_WIDTH = 280
const GAP = 10
const VIEWPORT_PADDING = 12

function resolvePosition(rect: DOMRect, preferredSide: 'left' | 'right'): Position {
  const viewportWidth = window.innerWidth || document.documentElement.clientWidth
  const viewportHeight = window.innerHeight || document.documentElement.clientHeight
  let side = preferredSide
  if (side === 'right' && rect.right + GAP + POPOVER_WIDTH > viewportWidth - VIEWPORT_PADDING) side = 'left'
  if (side === 'left' && rect.left - GAP - POPOVER_WIDTH < VIEWPORT_PADDING) side = 'right'
  const left = side === 'left'
    ? Math.max(VIEWPORT_PADDING, rect.left - GAP - POPOVER_WIDTH)
    : Math.min(viewportWidth - VIEWPORT_PADDING - POPOVER_WIDTH, rect.right + GAP)
  const top = Math.min(
    viewportHeight - VIEWPORT_PADDING,
    Math.max(VIEWPORT_PADDING, rect.top + rect.height / 2),
  )
  return { top, left }
}

export function InfoHint({ label, body, side = 'right' }: InfoHintProps) {
  const triggerRef = useRef<HTMLSpanElement | null>(null)
  const [position, setPosition] = useState<Position | null>(null)

  const open = () => {
    const rect = triggerRef.current?.getBoundingClientRect()
    if (!rect) return
    setPosition(resolvePosition(rect, side))
  }

  return (
    <span
      className={`info-hint-wrap ${side === 'left' ? 'info-hint-wrap-left' : ''}`}
      onMouseEnter={open}
      onMouseLeave={() => setPosition(null)}
      onFocus={open}
      onBlur={() => setPosition(null)}
    >
      <span
        ref={triggerRef}
        className="info-hint-trigger"
        aria-label={`About ${label}`}
        role="img"
        tabIndex={0}
      >
        ?
      </span>
      {position
        ? createPortal(
            <div
              className="info-hint-popover info-hint-portal"
              role="tooltip"
              style={{ top: position.top, left: position.left }}
            >
              <div className="info-hint-title">{label}</div>
              <div className="info-hint-body">{body}</div>
            </div>,
            document.body,
          )
        : null}
    </span>
  )
}
