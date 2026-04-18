import { useAtom, useAtomValue, useSetAtom } from 'jotai'
import { Sun, Moon } from 'lucide-react'
import { cn } from '../lib/utils'
import { systemThemeAtom, themeAtom, themePreferenceAtom, Theme } from '../store/atoms'
import { useEffect } from 'react'
import { Button } from './ui/button'
import { Tooltip, TooltipContent, TooltipTrigger } from './ui/tooltip'

export default function ThemeToggle({ className = '' }: { className?: string }) {
  const theme = useAtomValue(themeAtom)
  const systemTheme = useAtomValue(systemThemeAtom)
  const [themePreference, setThemePreference] = useAtom(themePreferenceAtom)
  const setSystemTheme = useSetAtom(systemThemeAtom)

  const nextTheme: Theme = theme === 'dark' ? 'light' : 'dark'
  const isFollowingSystem = themePreference === 'system'

  const toggleTheme = () => {
    setThemePreference(nextTheme === systemTheme ? 'system' : nextTheme)
  }

  useEffect(() => {
    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)')
    const updateSystemTheme = (matches: boolean) => {
      setSystemTheme(matches ? 'dark' : 'light')
    }

    updateSystemTheme(mediaQuery.matches)

    const handleChange = (event: MediaQueryListEvent) => {
      updateSystemTheme(event.matches)
    }

    if (typeof mediaQuery.addEventListener === 'function') {
      mediaQuery.addEventListener('change', handleChange)
      return () => mediaQuery.removeEventListener('change', handleChange)
    }

    mediaQuery.addListener(handleChange)
    return () => mediaQuery.removeListener(handleChange)
  }, [setSystemTheme])

  useEffect(() => {
    const root = document.documentElement
    root.classList.toggle('theme-light', theme === 'light')
    root.classList.toggle('theme-dark', theme === 'dark')
    root.classList.toggle('dark', theme === 'dark')
    root.style.colorScheme = theme
  }, [theme])

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Button
          variant="ghost"
          size="sm"
          onClick={toggleTheme}
          aria-label={`Switch to ${nextTheme} mode`}
          className={cn("h-8 px-2", className)}
        >
          {theme === 'dark' ? (
            <Sun className="w-3.5 h-3.5" />
          ) : (
            <Moon className="w-3.5 h-3.5" />
          )}
        </Button>
      </TooltipTrigger>
      <TooltipContent>
        {isFollowingSystem
          ? `Following system (${theme}). Click to switch to ${nextTheme} mode.`
          : `Manual ${theme} mode. Click to switch to ${nextTheme}${nextTheme === systemTheme ? ' and resume system sync' : ''}.`}
      </TooltipContent>
    </Tooltip>
  )
}
