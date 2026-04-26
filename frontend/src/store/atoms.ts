import { atom } from 'jotai'
import { atomWithStorage } from 'jotai/utils'
import {
  DEFAULT_TRADING_SCHEDULE_DRAFT,
  type TradingScheduleDraft,
} from '../components/tradingPanelFlyoutShared'

// Theme
export type Theme = 'dark' | 'light'
export type ThemePreference = Theme | 'system'

function getSystemTheme(): Theme {
  if (typeof window !== 'undefined' && typeof window.matchMedia === 'function') {
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
  }
  return 'dark'
}

export const systemThemeAtom = atom<Theme>(getSystemTheme())
export const themePreferenceAtom = atomWithStorage<ThemePreference>('theme', 'system')
export const themeAtom = atom<Theme>((get) => {
  const themePreference = get(themePreferenceAtom)
  return themePreference === 'system' ? get(systemThemeAtom) : themePreference
})

// Derive theme class for applying to body
export const themeClassAtom = atom((get) => {
  return get(themeAtom) === 'light' ? 'theme-light' : 'theme-dark'
})

// UI State
export const shortcutsHelpOpenAtom = atom(false)
export const copilotOpenAtom = atom(false)
export const commandBarOpenAtom = atom(false)

// Scanner data freshness
export const lastScanTimeAtom = atom<string | null>(null)
export const lastWebSocketMessageTimeAtom = atom<string | null>(null)

// Data simulation
export const simulationEnabledAtom = atom(true)

// Global account mode (sandbox vs live)
export type AccountMode = 'sandbox' | 'live'
export const accountModeAtom = atomWithStorage<AccountMode>('accountMode', 'sandbox')

// Global selected account
// For sandbox: the simulation account ID (e.g. "sim_abc123")
// For live: "live:polymarket" or "live:kalshi"
export const selectedAccountIdAtom = atomWithStorage<string | null>('selectedAccountId', null)

// Derived: is the selected account a live account?
export const isLiveAccountAtom = atom((get) => {
  const id = get(selectedAccountIdAtom)
  return id?.startsWith('live:') ?? false
})

// Derived: live platform from selected account
export const selectedLivePlatformAtom = atom((get) => {
  const id = get(selectedAccountIdAtom)
  if (!id?.startsWith('live:')) return null
  return id.replace('live:', '') as 'polymarket' | 'kalshi'
})

// AI Chat shared session state
export const activeChatSessionIdAtom = atomWithStorage<string | null>('activeChatSessionId', null)
export const aiTabSubtabAtom = atomWithStorage<'chat' | 'agents' | 'tools' | 'providers' | 'models' | 'activity'>('aiTabSubtab', 'chat')

// Bot config flyout draft text inputs.
// Owned by the flyout inputs themselves so typing doesn't re-render the
// full TradingPanel — only the (memoized) flyout subscribes via useAtomValue.
// TradingPanel writes via useSetAtom (no subscription) and reads inside
// mutation closures via useStore().get() (also non-reactive).
export const draftNameAtom = atom<string>('')
export const draftDescriptionAtom = atom<string>('')
export const draftIntervalAtom = atom<string>('60')

// Trading-schedule editor in the bot config flyout. Owned by the schedule
// fields themselves (time pickers, date pickers, day toggles, mode buttons)
// so a click/tap on any of them re-renders only the flyout — not the whole
// TradingPanel and its query memos. TradingPanel reads at save time via
// useStore().get() and writes at load time via useSetAtom.
export const draftTradingScheduleAtom = atom<TradingScheduleDraft>({ ...DEFAULT_TRADING_SCHEDULE_DRAFT })

// Risk-limit form values for the Risk tab in the bot detail panel. Stored
// as a parsed record (not a JSON string) so editing a single field doesn't
// cycle through JSON.stringify → parse → form re-render on every keystroke.
// Owned by the Risk view itself; TradingPanel reads at save time via
// useStore().get() and writes at load time via useSetAtom.
export const draftRiskValuesAtom = atom<Record<string, unknown>>({})
