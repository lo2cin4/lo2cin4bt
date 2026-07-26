import { useEffect, useMemo, useRef, useState } from 'react'

type CustomSelectOption = {
  value: string
  label: string
}

type CustomSelectProps = {
  value: string
  options: CustomSelectOption[]
  onChange: (value: string) => void
  className?: string
  placeholder?: string
  redactValues?: boolean
}

export function CustomSelect({
  value,
  options,
  onChange,
  className = '',
  placeholder = 'Select',
  redactValues = false,
}: CustomSelectProps) {
  const [open, setOpen] = useState(false)
  const rootRef = useRef<HTMLDivElement | null>(null)
  const selected = useMemo(() => options.find((option) => option.value === value), [options, value])

  useEffect(() => {
    if (!open) return
    const handlePointerDown = (event: PointerEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) setOpen(false)
    }
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setOpen(false)
    }
    document.addEventListener('pointerdown', handlePointerDown)
    document.addEventListener('keydown', handleKeyDown)
    return () => {
      document.removeEventListener('pointerdown', handlePointerDown)
      document.removeEventListener('keydown', handleKeyDown)
    }
  }, [open])

  return (
    <div className={`custom-select ${open ? 'open' : ''} ${className}`.trim()} ref={rootRef}>
      <button
        type="button"
        className="custom-select-trigger"
        aria-haspopup="listbox"
        aria-expanded={open}
        onClick={() => setOpen((current) => !current)}
      >
        <span className="custom-select-label" data-private-strategy={redactValues ? 'identity' : undefined}>
          {selected?.label || placeholder}
        </span>
        <span className="custom-select-caret">v</span>
      </button>
      {open ? (
        <div className="custom-select-menu" role="listbox">
          {options.map((option) => (
            <button
              type="button"
              key={option.value}
              className={`custom-select-option ${option.value === value ? 'active' : ''}`}
              data-private-strategy={redactValues ? 'identity' : undefined}
              role="option"
              aria-selected={option.value === value}
              title={option.label}
              onClick={() => {
                onChange(option.value)
                setOpen(false)
              }}
            >
              {option.label}
            </button>
          ))}
        </div>
      ) : null}
    </div>
  )
}
