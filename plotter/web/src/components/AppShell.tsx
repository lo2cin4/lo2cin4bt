import type { PropsWithChildren } from 'react'

import { Link, useRouterState } from '../routing'

import { useCopy } from '../i18n'
import { useAppStore } from '../store'

const NAV_ITEMS = [
  { to: '/', labelKey: 'nav.commandCenter' },
  { to: '/run-center', labelKey: 'nav.runCenter' },
  { to: '/metrics', labelKey: 'nav.metrics' },
  { to: '/wfa', labelKey: 'nav.walkForward' },
] as const

export function AppShell({ children }: PropsWithChildren) {
  const pathname = useRouterState({ select: (state) => state.location.pathname })
  const language = useAppStore((state) => state.language)
  const setLanguage = useAppStore((state) => state.setLanguage)
  const t = useCopy(language)

  return (
    <div className="app-shell">
      <aside className="app-sidebar">
        <div className="brand-wrap">
          <div className="brand-title">lo2cin4bt</div>
        </div>
        <nav className="nav-stack">
          {NAV_ITEMS.map((item) => (
            <Link
              key={item.to}
              to={item.to}
              className={`nav-link ${pathname === item.to || pathname.startsWith(`${item.to}/`) ? 'active' : ''}`}
            >
              {t(item.labelKey)}
            </Link>
          ))}
        </nav>
        <div className="sidebar-bottom">
          <section className="sidebar-cta" aria-label={t('sidebarCta.label')}>
            <p className="sidebar-cta-kicker">{t('sidebarCta.question')}</p>
            <p className="sidebar-cta-copy">{t('sidebarCta.copy')}</p>
            <a
              className="sidebar-cta-primary"
              href="https://lo2cin4.com/membership/"
              target="_blank"
              rel="noreferrer"
            >
              {t('sidebarCta.membership')}
            </a>
            <div className="sidebar-cta-community">
              <div className="sidebar-cta-community-title">{t('sidebarCta.community')}</div>
              <div className="sidebar-cta-community-buttons">
                <a href="https://t.me/lo2cin4group" target="_blank" rel="noreferrer">Telegram</a>
                <a href="https://discord.gg/sSnZuq3DNu" target="_blank" rel="noreferrer">Discord</a>
              </div>
            </div>
          </section>
          <div className="language-toggle" aria-label="Language">
            <button
              className={`language-toggle-button ${language === 'en' ? 'active' : ''}`}
              onClick={() => setLanguage('en')}
              type="button"
            >
              {t('language.en')}
            </button>
            <button
              className={`language-toggle-button ${language === 'zh-Hant' ? 'active' : ''}`}
              onClick={() => setLanguage('zh-Hant')}
              type="button"
            >
              {t('language.zhHant')}
            </button>
          </div>
        </div>
      </aside>
      <main className="app-main">
        {children}
      </main>
    </div>
  )
}
