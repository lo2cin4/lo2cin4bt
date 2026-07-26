import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
} from 'react'
import type {
  AnchorHTMLAttributes,
  MouseEvent,
  PropsWithChildren,
} from 'react'

type SearchValue = string | number | boolean | null | undefined
type SearchRecord = Record<string, SearchValue>

type RouterState = {
  location: {
    pathname: string
    search: Record<string, string | undefined>
  }
}

type NavigateOptions = {
  to: string
  search?: SearchRecord
  replace?: boolean
}

type LinkProps = Omit<AnchorHTMLAttributes<HTMLAnchorElement>, 'href'> & {
  to: string
  search?: SearchRecord
  activeOptions?: unknown
}

type RouterContextValue = {
  state: RouterState
  navigate: (options: NavigateOptions) => void
}

const RouterContext = createContext<RouterContextValue | null>(null)

function searchToQuery(search?: SearchRecord) {
  if (!search) return ''
  const params = new URLSearchParams()
  Object.entries(search).forEach(([key, value]) => {
    if (value === undefined || value === null || value === '') return
    params.set(key, String(value))
  })
  const query = params.toString()
  return query ? `?${query}` : ''
}

function buildPath(to: string, search?: SearchRecord) {
  return `${to || '/'}${searchToQuery(search)}`
}

function readRouterState(): RouterState {
  return {
    location: {
      pathname: window.location.pathname || '/',
      search: Object.fromEntries(new URLSearchParams(window.location.search).entries()),
    },
  }
}

function useRouterContext() {
  const context = useContext(RouterContext)
  if (!context) {
    throw new Error('Router hooks must be used inside BrowserRouter')
  }
  return context
}

export function BrowserRouter({ children }: PropsWithChildren) {
  const [state, setState] = useState<RouterState>(readRouterState)

  useEffect(() => {
    const syncLocation = () => setState(readRouterState())
    window.addEventListener('popstate', syncLocation)
    return () => window.removeEventListener('popstate', syncLocation)
  }, [])

  const value = useMemo<RouterContextValue>(
    () => ({
      state,
      navigate: ({ to, search, replace }) => {
        const nextPath = buildPath(to, search)
        if (replace) {
          window.history.replaceState(null, '', nextPath)
        } else {
          window.history.pushState(null, '', nextPath)
        }
        setState(readRouterState())
      },
    }),
    [state],
  )

  return <RouterContext.Provider value={value}>{children}</RouterContext.Provider>
}

export function useNavigate() {
  return useRouterContext().navigate
}

export function useRouterState<T>({ select }: { select: (state: RouterState) => T }) {
  return select(useRouterContext().state)
}

export function Link({
  to,
  search,
  activeOptions: _activeOptions,
  onClick,
  target,
  ...props
}: LinkProps) {
  const navigate = useNavigate()
  const href = buildPath(to, search)

  const handleClick = (event: MouseEvent<HTMLAnchorElement>) => {
    onClick?.(event)
    if (
      event.defaultPrevented
      || event.button !== 0
      || event.metaKey
      || event.ctrlKey
      || event.shiftKey
      || event.altKey
      || (target && target !== '_self')
    ) {
      return
    }
    event.preventDefault()
    navigate({ to, search })
  }

  return <a {...props} href={href} onClick={handleClick} target={target} />
}
