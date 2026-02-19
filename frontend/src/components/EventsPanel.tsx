import { useState, useMemo, useEffect, lazy, Suspense } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  AlertTriangle,
  TrendingUp,
  Activity,
  Shield,
  Zap,
  MapPin,
  Radio,
  ChevronRight,
  ChevronDown,
  ChevronUp,
  Swords,
  Wifi,
  Map as MapIcon,
} from 'lucide-react'
import { cn } from '../lib/utils'
import { formatCountry, normalizeCountryCode } from '../lib/worldCountries'
import { Button } from './ui/button'
import { Badge } from './ui/badge'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './ui/select'
import ErrorBoundary from './ErrorBoundary'

const WorldMap = lazy(() => import('./WorldMap'))
import {
  getWorldSignals,
  getInstabilityScores,
  getTensionPairs,
  getConvergenceZones,
  getTemporalAnomalies,
  WorldSignal,
} from '../services/eventsApi'

type WorldSubView = 'map' | 'signals' | 'countries' | 'tensions' | 'convergences' | 'anomalies'

const SIGNAL_TYPE_CONFIG: Record<string, { icon: React.ElementType; color: string; label: string }> = {
  conflict: { icon: Swords, color: 'text-red-400', label: 'Conflict' },
  tension: { icon: Activity, color: 'text-orange-400', label: 'Tension' },
  instability: { icon: AlertTriangle, color: 'text-yellow-400', label: 'Instability' },
  convergence: { icon: Radio, color: 'text-purple-400', label: 'Convergence' },
  anomaly: { icon: Zap, color: 'text-cyan-400', label: 'Anomaly' },
  military: { icon: Shield, color: 'text-blue-400', label: 'Military' },
  infrastructure: { icon: Wifi, color: 'text-emerald-400', label: 'Infrastructure' },
  earthquake: { icon: Zap, color: 'text-amber-400', label: 'Earthquake' },
  news: { icon: Radio, color: 'text-violet-400', label: 'News' },
}

const METADATA_CHIPS_CONFIG: Record<string, Array<{ key: string; label: string; format?: (v: unknown) => string }>> = {
  earthquake: [
    { key: 'magnitude', label: 'Mag', format: (v) => `M${Number(v).toFixed(1)}` },
    { key: 'depth_km', label: 'Depth', format: (v) => `${Number(v).toFixed(0)}km` },
    { key: 'tsunami', label: 'Tsunami', format: (v) => v ? 'Yes' : 'No' },
    { key: 'alert', label: 'Alert' },
  ],
  military: [
    { key: 'activity_type', label: 'Type' },
    { key: 'callsign', label: 'Callsign' },
    { key: 'aircraft_type', label: 'Aircraft' },
    { key: 'region', label: 'Region' },
    { key: 'is_unusual', label: 'Unusual', format: (v) => v ? 'Yes' : 'No' },
  ],
  anomaly: [
    { key: 'z_score', label: 'Z', format: (v) => Number(v).toFixed(1) },
    { key: 'current_value', label: 'Current', format: (v) => String(v) },
    { key: 'baseline_mean', label: 'Baseline', format: (v) => Number(v).toFixed(1) },
  ],
  infrastructure: [
    { key: 'event_type', label: 'Type' },
    { key: 'affected_services', label: 'Services', format: (v) => Array.isArray(v) ? v.join(', ') : String(v) },
    { key: 'cascade_risk_score', label: 'Cascade', format: (v) => `${(Number(v) * 100).toFixed(0)}%` },
  ],
  conflict: [
    { key: 'event_type', label: 'Type' },
    { key: 'sub_event_type', label: 'Sub-type' },
    { key: 'fatalities', label: 'Fatalities', format: (v) => String(v) },
  ],
  tension: [
    { key: 'trend', label: 'Trend' },
    { key: 'event_count', label: 'Events', format: (v) => String(v) },
  ],
  convergence: [
    { key: 'signal_count', label: 'Signals', format: (v) => String(v) },
  ],
}

type SignalsGroupBy = 'none' | 'type' | 'country' | 'severity' | 'source'
type SignalsLayout = 'list' | 'cards'

const SIGNAL_GROUP_OPTIONS: Array<{ value: SignalsGroupBy; label: string }> = [
  { value: 'none', label: 'No grouping' },
  { value: 'type', label: 'Signal type' },
  { value: 'country', label: 'Country' },
  { value: 'severity', label: 'Severity' },
  { value: 'source', label: 'Source' },
]

function severityLevel(severity: number): 'critical' | 'high' | 'medium' | 'low' {
  if (severity >= 0.8) return 'critical'
  if (severity >= 0.6) return 'high'
  if (severity >= 0.3) return 'medium'
  return 'low'
}

function detectedAtValue(detectedAt: string | null | undefined): number {
  if (!detectedAt) return Number.NEGATIVE_INFINITY
  const parsed = Date.parse(detectedAt)
  return Number.isFinite(parsed) ? parsed : Number.NEGATIVE_INFINITY
}

function buildSignalGroups(signals: WorldSignal[], groupBy: SignalsGroupBy): Array<{
  key: string
  label: string
  order: number
  signals: WorldSignal[]
}> {
  const groups = new Map<string, { key: string; label: string; order: number; signals: WorldSignal[] }>()

  for (const signal of signals) {
    let key = 'all'
    let label = 'All signals'
    let order = 0

    if (groupBy === 'type') {
      const typeConfig = SIGNAL_TYPE_CONFIG[signal.signal_type] || SIGNAL_TYPE_CONFIG.conflict
      key = `type:${signal.signal_type}`
      label = typeConfig.label
    } else if (groupBy === 'country') {
      const normalizedCountry = signal.country ? normalizeCountryCode(signal.country) || signal.country.toUpperCase() : 'UNKNOWN'
      key = `country:${normalizedCountry}`
      label = signal.country ? formatCountry(signal.country) : 'Unknown location'
    } else if (groupBy === 'severity') {
      const level = severityLevel(signal.severity)
      key = `severity:${level}`
      if (level === 'critical') {
        label = 'Critical (80%+)'
        order = 0
      } else if (level === 'high') {
        label = 'High (60-79%)'
        order = 1
      } else if (level === 'medium') {
        label = 'Medium (30-59%)'
        order = 2
      } else {
        label = 'Low (<30%)'
        order = 3
      }
    } else if (groupBy === 'source') {
      const source = signal.source || 'Unknown source'
      key = `source:${source.toLowerCase()}`
      label = source
    }

    const existing = groups.get(key)
    if (existing) {
      existing.signals.push(signal)
    } else {
      groups.set(key, { key, label, order, signals: [signal] })
    }
  }

  return [...groups.values()]
    .map((group) => ({
      ...group,
      signals: [...group.signals].sort((a, b) => {
        if (b.severity !== a.severity) return b.severity - a.severity
        return detectedAtValue(b.detected_at) - detectedAtValue(a.detected_at)
      }),
    }))
    .sort((a, b) => {
      if (groupBy === 'severity' && a.order !== b.order) return a.order - b.order
      if (b.signals.length !== a.signals.length) return b.signals.length - a.signals.length
      return a.label.localeCompare(b.label)
    })
}

function SeverityBadge({ severity }: { severity: number }) {
  const level = severityLevel(severity)
  const colors = {
    critical: 'bg-red-500/20 text-red-400 border-red-500/30',
    high: 'bg-orange-500/20 text-orange-400 border-orange-500/30',
    medium: 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30',
    low: 'bg-green-500/20 text-green-400 border-green-500/30',
  }
  return (
    <Badge variant="outline" className={cn('text-[10px] font-mono uppercase', colors[level])}>
      {level} ({(severity * 100).toFixed(0)}%)
    </Badge>
  )
}

function TrendIndicator({ trend }: { trend: string }) {
  if (trend === 'rising') return <TrendingUp className="w-3 h-3 text-red-400" />
  if (trend === 'falling') return <TrendingUp className="w-3 h-3 text-green-400 rotate-180" />
  return <Activity className="w-3 h-3 text-muted-foreground" />
}

function MarketRelevanceBadge({ score }: { score: number | null }) {
  if (score == null) return null
  const color = score >= 0.7
    ? 'bg-green-500/20 text-green-400 border-green-500/30'
    : score >= 0.3
      ? 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30'
      : 'bg-muted/40 text-muted-foreground border-border'
  return (
    <Badge variant="outline" className={cn('text-[10px] font-mono', color)}>
      MR {(score * 100).toFixed(0)}%
    </Badge>
  )
}

function SignalCard({ signal, layout }: { signal: WorldSignal; layout: SignalsLayout }) {
  const [expanded, setExpanded] = useState(false)
  const config = SIGNAL_TYPE_CONFIG[signal.signal_type] || SIGNAL_TYPE_CONFIG.conflict
  const Icon = config.icon

  const metadataChips = useMemo(() => {
    const chipDefs = METADATA_CHIPS_CONFIG[signal.signal_type] || []
    const meta = signal.metadata
    if (!meta) return []
    return chipDefs
      .filter((def) => meta[def.key] != null && meta[def.key] !== '')
      .map((def) => ({
        key: def.key,
        label: def.label,
        value: def.format ? def.format(meta[def.key]) : String(meta[def.key]),
      }))
  }, [signal.signal_type, signal.metadata])

  const contextParts = useMemo(() => {
    const parts: string[] = []
    if (signal.country) parts.push(formatCountry(signal.country))
    if (signal.source) parts.push(signal.source)
    if (signal.detected_at) parts.push(new Date(signal.detected_at).toLocaleString())
    return parts
  }, [signal.country, signal.source, signal.detected_at])

  return (
    <div
      className={cn(
        'rounded-lg transition-colors cursor-pointer',
        layout === 'cards'
          ? 'border border-border bg-card/70 hover:bg-card px-3 py-2.5 h-full'
          : 'py-1.5 px-2 bg-background/50 hover:bg-background/80',
      )}
      onClick={() => setExpanded((v) => !v)}
    >
      <div className="flex items-start gap-2">
        <Icon className={cn('w-4 h-4 mt-0.5 shrink-0', config.color)} />
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-1.5 flex-wrap">
            <span className="text-sm font-medium leading-5">{signal.title}</span>
            <Badge variant="outline" className={cn('text-[9px] h-4 px-1.5 font-data', config.color, 'border-current/20')}>
              {config.label}
            </Badge>
            <SeverityBadge severity={signal.severity} />
            <MarketRelevanceBadge score={signal.market_relevance_score} />
          </div>
          {contextParts.length > 0 && (
            <div className="text-[10px] text-muted-foreground mt-0.5">
              {contextParts.join(' · ')}
            </div>
          )}
          {signal.related_market_ids && signal.related_market_ids.length > 0 && (
            <div className="flex items-center gap-1 mt-1">
              <MapPin className="w-3 h-3 text-primary" />
              <span className="text-[10px] text-primary">{signal.related_market_ids.length} related market{signal.related_market_ids.length > 1 ? 's' : ''}</span>
            </div>
          )}
        </div>
        <div className="shrink-0 mt-0.5 text-muted-foreground">
          {expanded ? <ChevronUp className="w-3.5 h-3.5" /> : <ChevronDown className="w-3.5 h-3.5" />}
        </div>
      </div>
      {expanded && (
        <div className={cn('mt-2 space-y-1.5', layout === 'cards' ? 'pl-0' : 'ml-6')}>
          {signal.description && (
            <p className="text-xs text-muted-foreground">{signal.description}</p>
          )}
          {metadataChips.length > 0 && (
            <div className="flex items-center gap-1.5 flex-wrap">
              {metadataChips.map((chip) => (
                <Badge key={chip.key} variant="outline" className="text-[9px] h-4 px-1.5 bg-muted/30 border-border/40 font-mono">
                  {chip.label}: {chip.value}
                </Badge>
              ))}
            </div>
          )}
          {signal.latitude != null && signal.longitude != null && (
            <div className="text-[10px] text-muted-foreground font-mono">
              {signal.latitude.toFixed(2)}, {signal.longitude.toFixed(2)}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function SignalTypeSummaryBar({ signals }: { signals: WorldSignal[] }) {
  const counts = useMemo(() => {
    const map: Record<string, number> = {}
    for (const s of signals) {
      map[s.signal_type] = (map[s.signal_type] || 0) + 1
    }
    return Object.entries(map).sort((a, b) => b[1] - a[1])
  }, [signals])

  if (counts.length === 0) return null

  return (
    <div className="flex items-center gap-1.5 flex-wrap">
      {counts.map(([type, count]) => {
        const config = SIGNAL_TYPE_CONFIG[type] || SIGNAL_TYPE_CONFIG.conflict
        return (
          <Badge key={type} variant="outline" className={cn('text-[9px] h-5 px-1.5 gap-1 font-data', config.color, 'bg-transparent border-current/20')}>
            {config.label} {count}
          </Badge>
        )
      })}
    </div>
  )
}

// ==================== SIGNALS SUB-VIEW ====================

function SignalsView({ isConnected }: { isConnected: boolean }) {
  const [typeFilter, setTypeFilter] = useState<string>('')
  const [pageSize, setPageSize] = useState<number>(100)
  const [page, setPage] = useState<number>(1)
  const [groupBy, setGroupBy] = useState<SignalsGroupBy>('type')
  const [layout, setLayout] = useState<SignalsLayout>('cards')
  const offset = (page - 1) * pageSize
  const { data, isLoading, isError } = useQuery({
    queryKey: ['world-signals', { signal_type: typeFilter || undefined, limit: pageSize, offset }],
    queryFn: () => getWorldSignals({ signal_type: typeFilter || undefined, limit: pageSize, offset }),
    refetchInterval: isConnected ? false : 120000,
  })
  const signals = data?.signals || []
  const groupedSignals = useMemo(() => buildSignalGroups(signals, groupBy), [signals, groupBy])
  const totalSignals = Math.max(Number(data?.total || 0), signals.length)
  const totalPages = Math.max(1, Math.ceil(totalSignals / pageSize))
  const currentPage = Math.min(page, totalPages)
  const currentOffset = (currentPage - 1) * pageSize
  const pageStart = signals.length === 0 ? 0 : currentOffset + 1
  const pageEnd = signals.length === 0 ? 0 : currentOffset + signals.length

  useEffect(() => {
    if (page > totalPages) {
      setPage(totalPages)
    }
  }, [page, totalPages])

  return (
    <div className="h-full min-h-0 flex flex-col">
      <div className="shrink-0 space-y-3 border-b border-border/40 pb-3">
        <div className="flex flex-wrap items-center gap-2">
          <Select
            value={typeFilter || 'all'}
            onValueChange={(value) => {
              setTypeFilter(value === 'all' ? '' : value)
              setPage(1)
            }}
          >
            <SelectTrigger className="h-8 w-[180px] text-xs">
              <SelectValue placeholder="Signal type" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All signal types</SelectItem>
              {Object.entries(SIGNAL_TYPE_CONFIG).map(([type, config]) => (
                <SelectItem key={type} value={type}>
                  {config.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select
            value={String(pageSize)}
            onValueChange={(value) => {
              setPageSize(Math.max(1, Number(value) || 100))
              setPage(1)
            }}
          >
          <SelectTrigger className="h-8 w-[180px] text-xs">
            <SelectValue placeholder="Page size" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="25">25 rows</SelectItem>
            <SelectItem value="50">50 rows</SelectItem>
            <SelectItem value="100">100 rows</SelectItem>
            <SelectItem value="250">250 rows</SelectItem>
            <SelectItem value="500">500 rows</SelectItem>
            <SelectItem value="1000">1000 rows</SelectItem>
          </SelectContent>
        </Select>
          <Select value={groupBy} onValueChange={(value) => setGroupBy(value as SignalsGroupBy)}>
            <SelectTrigger className="h-8 w-[170px] text-xs">
              <SelectValue placeholder="Group by" />
            </SelectTrigger>
            <SelectContent>
              {SIGNAL_GROUP_OPTIONS.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <div className="inline-flex items-center rounded-md border border-border bg-background p-0.5 ml-auto">
            <Button
              variant={layout === 'list' ? 'secondary' : 'ghost'}
              size="sm"
              className="h-6 px-2 text-[11px]"
              onClick={() => setLayout('list')}
            >
              List
            </Button>
            <Button
              variant={layout === 'cards' ? 'secondary' : 'ghost'}
              size="sm"
              className="h-6 px-2 text-[11px]"
              onClick={() => setLayout('cards')}
            >
              Cards
            </Button>
          </div>
        </div>

        {!isLoading && !isError && signals.length > 0 && (
          <SignalTypeSummaryBar signals={signals} />
        )}

        <div className="flex items-center justify-between gap-2 text-xs text-muted-foreground">
          <span className="font-data">
            Showing {pageStart}-{pageEnd} of {totalSignals}
          </span>
          <div className="flex items-center gap-1">
            <Button
              variant="outline"
              size="sm"
              className="h-7 px-2 text-[11px]"
              onClick={() => setPage(1)}
              disabled={currentPage <= 1 || isLoading}
            >
              First
            </Button>
            <Button
              variant="outline"
              size="sm"
              className="h-7 px-2 text-[11px]"
              onClick={() => setPage((prev) => Math.max(1, prev - 1))}
              disabled={currentPage <= 1 || isLoading}
            >
              Prev
            </Button>
            <span className="px-2 text-[11px] font-mono">
              {currentPage} / {totalPages}
            </span>
            <Button
              variant="outline"
              size="sm"
              className="h-7 px-2 text-[11px]"
              onClick={() => setPage((prev) => Math.min(totalPages, prev + 1))}
              disabled={currentPage >= totalPages || isLoading}
            >
              Next
            </Button>
            <Button
              variant="outline"
              size="sm"
              className="h-7 px-2 text-[11px]"
              onClick={() => setPage(totalPages)}
              disabled={currentPage >= totalPages || isLoading}
            >
              Last
            </Button>
          </div>
        </div>
      </div>

      <div className="flex-1 min-h-0 overflow-y-auto pt-3 pr-1">
        {isLoading ? (
          <div className="text-center text-muted-foreground py-8">Loading signals...</div>
        ) : isError ? (
          <div className="text-center text-red-400 py-8">Failed to load signals</div>
        ) : (
          <div className="space-y-3">
            {groupedSignals.map((group) => (
              <section key={group.key} className="space-y-2">
                {groupBy !== 'none' && (
                  <div className="flex items-center justify-between rounded-md border border-border/60 bg-muted/20 px-2.5 py-1.5">
                    <span className="text-[11px] font-data tracking-wide text-muted-foreground">
                      {group.label}
                    </span>
                    <Badge variant="outline" className="text-[9px] h-4 px-1.5 font-mono">
                      {group.signals.length}
                    </Badge>
                  </div>
                )}
                <div
                  className={cn(
                    layout === 'cards'
                      ? 'grid grid-cols-1 gap-2 xl:grid-cols-2 2xl:grid-cols-3'
                      : 'space-y-2',
                  )}
                >
                  {group.signals.map((signal) => (
                    <SignalCard key={signal.signal_id} signal={signal} layout={layout} />
                  ))}
                </div>
              </section>
            ))}
            {groupedSignals.length === 0 && (
              <div className="text-center text-muted-foreground py-8">No signals matching filter</div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

// ==================== COUNTRIES SUB-VIEW ====================

function CountriesView({ isConnected }: { isConnected: boolean }) {
  const { data, isLoading, isError } = useQuery({
    queryKey: ['world-instability'],
    queryFn: () => getInstabilityScores({ min_score: 0, limit: 100 }),
    refetchInterval: isConnected ? false : 180000,
  })

  if (isLoading) return <div className="text-center text-muted-foreground py-8">Loading instability scores...</div>
  if (isError) return <div className="text-center text-red-400 py-8">Failed to load instability scores</div>

  return (
    <div className="space-y-2">
      <div className="grid grid-cols-12 gap-2 px-2 text-[10px] text-muted-foreground uppercase tracking-wider font-semibold">
        <div className="col-span-1">ISO3</div>
        <div className="col-span-3">Country</div>
        <div className="col-span-2 text-right">CII Score</div>
        <div className="col-span-1 text-center">Trend</div>
        <div className="col-span-2 text-right">24h Change</div>
        <div className="col-span-3">Top Factor</div>
      </div>
      {(data?.scores || []).map((s) => (
        <div key={s.iso3} className="grid grid-cols-12 gap-2 px-2 py-1.5 rounded bg-card border border-border items-center">
          <div className="col-span-1 font-mono text-xs font-bold">{normalizeCountryCode(s.iso3 || s.country) || s.iso3}</div>
          <div className="col-span-3 text-sm truncate">{formatCountry(s.country || s.iso3)}</div>
          <div className={cn('col-span-2 text-right font-mono font-bold text-sm', s.score >= 80 ? 'text-red-400' : s.score >= 60 ? 'text-orange-400' : s.score >= 40 ? 'text-yellow-400' : 'text-green-400')}>
            {s.score.toFixed(1)}
          </div>
          <div className="col-span-1 flex justify-center">
            <TrendIndicator trend={s.trend} />
          </div>
          <div className={cn('col-span-2 text-right font-mono text-xs', (s.change_24h || 0) > 0 ? 'text-red-400' : (s.change_24h || 0) < 0 ? 'text-green-400' : 'text-muted-foreground')}>
            {s.change_24h != null ? `${s.change_24h > 0 ? '+' : ''}${s.change_24h.toFixed(1)}` : '—'}
          </div>
          <div className="col-span-3 text-[10px] text-muted-foreground truncate">
            {s.contributing_signals?.[0] ? JSON.stringify(s.contributing_signals[0]).slice(0, 40) : '—'}
          </div>
        </div>
      ))}
      {(!data?.scores || data.scores.length === 0) && (
        <div className="text-center text-muted-foreground py-8">
          No instability scores available yet. Check source health in Overview.
        </div>
      )}
    </div>
  )
}

// ==================== TENSIONS SUB-VIEW ====================

function TensionsView({ isConnected }: { isConnected: boolean }) {
  const { data, isLoading, isError } = useQuery({
    queryKey: ['world-tensions'],
    queryFn: () => getTensionPairs({ min_tension: 0, limit: 20 }),
    refetchInterval: isConnected ? false : 180000,
  })

  if (isLoading) return <div className="text-center text-muted-foreground py-8">Loading tension data...</div>
  if (isError) return <div className="text-center text-red-400 py-8">Failed to load tension data</div>

  return (
    <div className="space-y-2">
      {(data?.tensions || []).map((t) => (
        <div key={`${t.country_a}-${t.country_b}`} className="p-3 rounded-lg bg-card border border-border">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <Swords className="w-4 h-4 text-orange-400" />
              <span className="text-sm font-bold">{formatCountry(t.country_a_name || t.country_a_iso3 || t.country_a)}</span>
              <ChevronRight className="w-3 h-3 text-muted-foreground" />
              <span className="text-sm font-bold">{formatCountry(t.country_b_name || t.country_b_iso3 || t.country_b)}</span>
            </div>
            <div className="flex items-center gap-2">
              <TrendIndicator trend={t.trend} />
              <span className={cn('font-mono text-lg font-bold', t.tension_score >= 70 ? 'text-red-400' : t.tension_score >= 40 ? 'text-orange-400' : 'text-yellow-400')}>
                {t.tension_score.toFixed(0)}
              </span>
            </div>
          </div>
          <div className="flex items-center gap-3 text-[10px] text-muted-foreground">
            <span>{t.event_count} events</span>
            {t.avg_goldstein_scale != null && <span>Goldstein: {t.avg_goldstein_scale.toFixed(1)}</span>}
            {t.top_event_types?.length > 0 && <span>{t.top_event_types.slice(0, 3).join(', ')}</span>}
          </div>
        </div>
      ))}
      {(!data?.tensions || data.tensions.length === 0) && (
        <div className="text-center text-muted-foreground py-8">
          No tension pairs available yet. Check source health in Overview.
        </div>
      )}
    </div>
  )
}

// ==================== CONVERGENCES SUB-VIEW ====================

function ConvergencesView({ isConnected }: { isConnected: boolean }) {
  const { data, isLoading, isError } = useQuery({
    queryKey: ['world-convergences'],
    queryFn: getConvergenceZones,
    refetchInterval: isConnected ? false : 180000,
  })

  if (isLoading) return <div className="text-center text-muted-foreground py-8">Loading convergence data...</div>
  if (isError) return <div className="text-center text-red-400 py-8">Failed to load convergence data</div>

  return (
    <div className="space-y-2">
      {(data?.zones || []).length === 0 && (
        <div className="text-center text-muted-foreground py-8">No active convergence zones detected</div>
      )}
      {(data?.zones || []).map((z) => (
        <div key={z.grid_key} className="p-3 rounded-lg bg-card border border-border">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <Radio className="w-4 h-4 text-purple-400" />
              <span className="text-sm font-medium">{z.country ? formatCountry(z.country) : `${z.latitude.toFixed(1)}, ${z.longitude.toFixed(1)}`}</span>
            </div>
            <Badge variant="outline" className={cn('text-[10px] font-mono', z.urgency_score >= 70 ? 'bg-red-500/20 text-red-400' : z.urgency_score >= 40 ? 'bg-orange-500/20 text-orange-400' : 'bg-yellow-500/20 text-yellow-400')}>
              Urgency: {z.urgency_score.toFixed(0)}
            </Badge>
          </div>
          <div className="flex items-center gap-2 flex-wrap mb-1">
            {z.signal_types.map((type) => {
              const config = SIGNAL_TYPE_CONFIG[type] || SIGNAL_TYPE_CONFIG.conflict
              return (
                <Badge key={type} variant="outline" className={cn('text-[10px]', config.color)}>
                  {config.label}
                </Badge>
              )
            })}
          </div>
          <div className="text-[10px] text-muted-foreground">
            {z.signal_count} signals · {z.nearby_markets?.length || 0} related markets
          </div>
        </div>
      ))}
    </div>
  )
}

// ==================== ANOMALIES SUB-VIEW ====================

function AnomaliesView({ isConnected }: { isConnected: boolean }) {
  const { data, isLoading, isError } = useQuery({
    queryKey: ['world-anomalies'],
    queryFn: () => getTemporalAnomalies({ min_severity: 'medium' }),
    refetchInterval: isConnected ? false : 180000,
  })

  if (isLoading) return <div className="text-center text-muted-foreground py-8">Loading anomaly data...</div>
  if (isError) return <div className="text-center text-red-400 py-8">Failed to load anomaly data</div>

  return (
    <div className="space-y-2">
      {(data?.anomalies || []).length === 0 && (
        <div className="text-center text-muted-foreground py-8">No significant anomalies detected</div>
      )}
      {(data?.anomalies || []).map((a, i) => (
        <div key={i} className="p-3 rounded-lg bg-card border border-border">
          <div className="flex items-center justify-between mb-1">
            <div className="flex items-center gap-2">
              <Zap className={cn('w-4 h-4', a.severity === 'critical' ? 'text-red-400' : a.severity === 'high' ? 'text-orange-400' : 'text-yellow-400')} />
              <span className="text-sm font-medium">{formatCountry(a.country)} — {a.signal_type.replace(/_/g, ' ')}</span>
            </div>
            <Badge variant="outline" className={cn('text-[10px] font-mono uppercase', a.severity === 'critical' ? 'bg-red-500/20 text-red-400' : a.severity === 'high' ? 'bg-orange-500/20 text-orange-400' : 'bg-yellow-500/20 text-yellow-400')}>
              {a.severity}
            </Badge>
          </div>
          <p className="text-xs text-muted-foreground mb-1">{a.description}</p>
          <div className="flex items-center gap-3 text-[10px] text-muted-foreground font-mono">
            <span>z={a.z_score.toFixed(1)}</span>
            <span>current={a.current_value}</span>
            <span>baseline={a.baseline_mean.toFixed(1)} ± {a.baseline_std.toFixed(1)}</span>
          </div>
        </div>
      ))}
    </div>
  )
}

// ==================== MAIN COMPONENT ====================

const SUB_NAV: { id: WorldSubView; label: string; icon: React.ElementType }[] = [
  { id: 'map', label: 'Map', icon: MapIcon },
  { id: 'signals', label: 'Signals', icon: Radio },
]

export default function EventsPanel({
  isConnected = true,
  eventsOnly = false,
}: {
  isConnected?: boolean
  eventsOnly?: boolean
}) {
  const [subView, setSubView] = useState<WorldSubView>(eventsOnly ? 'signals' : 'map')

  if (eventsOnly) {
    return (
      <div className="h-full min-h-0 flex flex-col overflow-hidden">
        <div className="flex-1 min-h-0 p-4">
          <ErrorBoundary fallback={<div className="rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-500">This events view failed to render.</div>}>
            <SignalsView isConnected={isConnected} />
          </ErrorBoundary>
        </div>
      </div>
    )
  }

  return (
    <div className="h-full min-h-0 flex flex-col overflow-hidden">
      {/* Sub-navigation */}
      <div className="flex items-center gap-1 px-4 py-2 border-b border-border bg-card/50 overflow-x-auto shrink-0">
        {SUB_NAV.map((item) => (
          <Button
            key={item.id}
            variant={subView === item.id ? 'default' : 'ghost'}
            size="sm"
            onClick={() => setSubView(item.id)}
            className="h-7 text-xs gap-1 shrink-0"
          >
            <item.icon className="w-3 h-3" />
            {item.label}
          </Button>
        ))}
      </div>

      {/* Content */}
      {subView === 'map' ? (
        <div className="flex-1 min-h-0 relative overflow-hidden">
          <ErrorBoundary fallback={<div className="m-4 rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-500">Map view crashed.</div>}>
            <Suspense fallback={<div className="h-full w-full" />}>
              <WorldMap isConnected={isConnected} />
            </Suspense>
          </ErrorBoundary>
        </div>
      ) : (
        <div className={cn('flex-1 p-4', subView === 'signals' ? 'min-h-0' : 'overflow-y-auto')}>
          <ErrorBoundary fallback={<div className="rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-500">This events view failed to render.</div>}>
            {subView === 'signals' && <SignalsView isConnected={isConnected} />}
            {subView === 'countries' && <CountriesView isConnected={isConnected} />}
            {subView === 'tensions' && <TensionsView isConnected={isConnected} />}
            {subView === 'convergences' && <ConvergencesView isConnected={isConnected} />}
            {subView === 'anomalies' && <AnomaliesView isConnected={isConnected} />}
          </ErrorBoundary>
        </div>
      )}
    </div>
  )
}
