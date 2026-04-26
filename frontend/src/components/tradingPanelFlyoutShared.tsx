import { type ReactNode, useState } from 'react'
import { ChevronDown, ChevronRight } from 'lucide-react'
import { cn } from '../lib/utils'
import { Card } from './ui/card'

export const DEFAULT_STRATEGY_KEY = 'btc_eth_highfreq'

export type TradingScheduleDay = 'mon' | 'tue' | 'wed' | 'thu' | 'fri' | 'sat' | 'sun'

export type TradingScheduleDraft = {
  enabled: boolean
  days: TradingScheduleDay[]
  startTimeUtc: string
  endTimeUtc: string
  startDateUtc: string
  endDateUtc: string
  endAtUtc: string
}

export const TRADING_SCHEDULE_DAYS: TradingScheduleDay[] = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
export const TRADING_SCHEDULE_WEEKDAYS: TradingScheduleDay[] = ['mon', 'tue', 'wed', 'thu', 'fri']
export const TRADING_SCHEDULE_WEEKENDS: TradingScheduleDay[] = ['sat', 'sun']
export const TRADING_SCHEDULE_DAY_LABEL: Record<TradingScheduleDay, string> = {
  mon: 'Mon',
  tue: 'Tue',
  wed: 'Wed',
  thu: 'Thu',
  fri: 'Fri',
  sat: 'Sat',
  sun: 'Sun',
}

export type StrategyOptionDetail = {
  key: string
  label: string
  defaultParams: Record<string, unknown>
  paramFields: Array<Record<string, unknown>>
  version: number | null
  latestVersion: number | null
  versions: number[]
}

export type StrategyCatalogOption = {
  key: string
  label: string
  sourceKey: string
  sourceLabel: string
  detail: StrategyOptionDetail
}

export function normalizeStrategyKey(value: unknown): string {
  const key = String(value || '').trim().toLowerCase()
  return key || DEFAULT_STRATEGY_KEY
}

export function normalizeStrategyVersion(value: unknown): number | null {
  if (value === null || value === undefined) return null
  if (typeof value === 'number' && Number.isFinite(value) && value > 0) {
    return Math.trunc(value)
  }
  const raw = String(value || '').trim().toLowerCase()
  if (!raw || raw === 'latest') return null
  const parsed = Number(raw.startsWith('v') ? raw.slice(1) : raw)
  if (!Number.isFinite(parsed) || parsed <= 0) return null
  return Math.trunc(parsed)
}

export function normalizeVersionList(value: unknown): number[] {
  const raw = Array.isArray(value) ? value : []
  const seen = new Set<number>()
  const out: number[] = []
  for (const item of raw) {
    const normalized = normalizeStrategyVersion(item)
    if (normalized == null || seen.has(normalized)) continue
    seen.add(normalized)
    out.push(normalized)
  }
  out.sort((left, right) => right - left)
  return out
}

export function FlyoutSection({
  title,
  subtitle,
  icon: Icon,
  count,
  defaultOpen = true,
  iconClassName = 'text-orange-500',
  tone = 'default',
  children,
}: {
  title: string
  subtitle?: string
  icon: any
  count?: string
  defaultOpen?: boolean
  iconClassName?: string
  tone?: 'default' | 'danger'
  children: ReactNode
}) {
  const [open, setOpen] = useState(defaultOpen)

  return (
    <Card
      className={cn(
        'rounded-xl shadow-none overflow-hidden',
        tone === 'danger' ? 'bg-red-500/5 border-red-500/25' : 'bg-card/40 border-border/40'
      )}
    >
      <button
        type="button"
        onClick={() => setOpen((current) => !current)}
        className={cn(
          'w-full flex items-center justify-between gap-2 px-3 py-2 transition-colors border-b',
          tone === 'danger'
            ? 'border-red-500/20 hover:bg-red-500/10'
            : 'border-border/40 hover:bg-muted/25'
        )}
      >
        <div className="flex items-center gap-1.5">
          <Icon className={cn('w-3.5 h-3.5', iconClassName)} />
          <h4 className="text-[10px] uppercase tracking-widest font-semibold">{title}</h4>
        </div>
        <div className="flex items-center gap-1.5">
          {count ? (
            <span className="text-[9px] px-1.5 py-0.5 rounded-full bg-muted/60 text-muted-foreground">
              {count}
            </span>
          ) : null}
          {open ? <ChevronDown className="w-3.5 h-3.5 text-muted-foreground" /> : <ChevronRight className="w-3.5 h-3.5 text-muted-foreground" />}
        </div>
      </button>
      {open ? (
        <div className="px-3 py-3 space-y-3">
          {subtitle ? <p className="text-[10px] text-muted-foreground/70 -mt-0.5">{subtitle}</p> : null}
          {children}
        </div>
      ) : null}
    </Card>
  )
}
