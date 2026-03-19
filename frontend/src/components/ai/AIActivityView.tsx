import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  RefreshCw,
  AlertCircle,
  CheckCircle,
  Zap,
  Activity,
  BarChart3,
  ChevronDown,
  ChevronRight,
  Target,
  Clock,
  DollarSign,
  Filter,
  Bot,
} from 'lucide-react'
import { cn } from '../../lib/utils'
import { Card, CardContent, CardHeader, CardTitle } from '../ui/card'
import { Button } from '../ui/button'
import { Badge } from '../ui/badge'
import { ScrollArea } from '../ui/scroll-area'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../ui/select'
import {
  listLLMUsageLog,
  getJudgmentHistory,
  getAgreementStats,
  type LLMUsageLogEntry,
} from '../../services/api'

// ── Shared metric components (from AIToolsView) ──

type MetricTone = 'good' | 'bad' | 'warn' | 'neutral' | 'info'

const TONE_CLASSES: Record<MetricTone, string> = {
  good: 'border-emerald-300 bg-emerald-100 text-emerald-900 dark:border-emerald-500/30 dark:bg-emerald-500/10 dark:text-emerald-200',
  bad: 'border-red-300 bg-red-100 text-red-900 dark:border-red-500/30 dark:bg-red-500/10 dark:text-red-200',
  warn: 'border-amber-300 bg-amber-100 text-amber-900 dark:border-amber-500/30 dark:bg-amber-500/10 dark:text-amber-200',
  neutral: 'border-border/70 bg-background/35 text-foreground',
  info: 'border-purple-300 bg-purple-100 text-purple-900 dark:border-purple-500/30 dark:bg-purple-500/10 dark:text-purple-200',
}

function MetricTile({ label, value, icon, tone }: { label: string; value: string; icon: React.ReactNode; tone: MetricTone }) {
  return (
    <Card className={cn('border', TONE_CLASSES[tone])}>
      <CardContent className="p-3">
        <div className="flex items-start justify-between gap-2">
          <p className="text-[10px] uppercase tracking-wide opacity-85">{label}</p>
          <span className="opacity-80">{icon}</span>
        </div>
        <p className="mt-2 font-data text-lg font-semibold">{value}</p>
      </CardContent>
    </Card>
  )
}

function ScoreBadge({ label, value }: { label: string; value: number }) {
  const color =
    value >= 0.7
      ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20'
      : value >= 0.4
        ? 'bg-amber-500/10 text-amber-400 border-amber-500/20'
        : 'bg-red-500/10 text-red-400 border-red-500/20'
  return (
    <Badge variant="outline" className={cn('rounded text-[10px]', color)}>
      {label}: {typeof value === 'number' ? value.toFixed(2) : 'N/A'}
    </Badge>
  )
}

// ── Judgments Panel (from AIToolsView) ──

function JudgmentsPanel() {
  const { data: history, isLoading } = useQuery({
    queryKey: ['ai-judgment-history'],
    queryFn: async () => {
      const { data } = await getJudgmentHistory({ limit: 50 })
      return data
    },
  })

  const { data: agreementStats } = useQuery({
    queryKey: ['ai-agreement-stats'],
    queryFn: async () => {
      const { data } = await getAgreementStats()
      return data
    },
  })

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-8">
        <RefreshCw className="w-6 h-6 animate-spin text-cyan-400" />
      </div>
    )
  }

  const agRate = agreementStats?.agreement_rate ?? 0
  const avgScore = agreementStats?.avg_score ?? 0

  return (
    <div className="space-y-4">
      {agreementStats && (
        <Card className="overflow-hidden border-border/60 bg-card/80">
          <div className="h-0.5 bg-cyan-400" />
          <CardHeader className="pb-3">
            <CardTitle className="flex items-center gap-2 text-base font-semibold">
              <Activity className="h-4 w-4 text-cyan-400" />
              ML vs LLM Agreement
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3 card-stagger">
              <MetricTile label="Total Judged" value={String(agreementStats.total_judged ?? 0)} icon={<BarChart3 className="h-3.5 w-3.5" />} tone="info" />
              <MetricTile label="Agreement Rate" value={`${(agRate * 100).toFixed(1)}%`} icon={<Target className="h-3.5 w-3.5" />} tone={agRate >= 0.7 ? 'good' : agRate >= 0.4 ? 'warn' : 'bad'} />
              <MetricTile label="ML Overrides" value={String(agreementStats.ml_overrides ?? 0)} icon={<Zap className="h-3.5 w-3.5" />} tone="neutral" />
              <MetricTile label="Avg Score" value={avgScore.toFixed(2)} icon={<Activity className="h-3.5 w-3.5" />} tone={avgScore >= 0.7 ? 'good' : avgScore >= 0.4 ? 'warn' : 'neutral'} />
            </div>
          </CardContent>
        </Card>
      )}

      <Card className="overflow-hidden border-border/60 bg-card/80">
        <div className="h-0.5 bg-emerald-400" />
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-base font-semibold">
            <Target className="h-4 w-4 text-emerald-400" />
            Recent Judgments
          </CardTitle>
        </CardHeader>
        <CardContent>
          {!history || history.length === 0 ? (
            <div className="text-center py-8">
              <Bot className="w-10 h-10 text-muted-foreground/30 mx-auto mb-3" />
              <p className="text-xs text-muted-foreground">No opportunity judgments yet. AI will judge opportunities during scans when enabled.</p>
            </div>
          ) : (
            <ScrollArea className="h-[300px] pr-3">
              <div className="space-y-2">
                {(Array.isArray(history) ? history : []).map((j: any, i: number) => (
                  <div
                    key={j.opportunity_id || i}
                    className="rounded-lg border border-border/55 bg-background/30 p-3 hover:border-border/80 transition-colors"
                  >
                    <div className="flex items-center justify-between gap-3">
                      <div className="flex-1 min-w-0">
                        <p className="text-sm font-medium truncate">{j.opportunity_id}</p>
                        <div className="flex items-center gap-2 mt-1">
                          <Badge variant="outline" className="text-[9px] h-4 px-1.5 bg-muted/30 border-border/40">
                            {j.strategy_type ?? 'unknown'}
                          </Badge>
                          <Badge
                            variant="outline"
                            className={cn(
                              'text-[9px] h-4 px-1.5',
                              j.recommendation === 'take' || j.recommendation === 'buy'
                                ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20'
                                : j.recommendation === 'skip' || j.recommendation === 'avoid'
                                  ? 'bg-red-500/10 text-red-400 border-red-500/20'
                                  : 'bg-amber-500/10 text-amber-400 border-amber-500/20'
                            )}
                          >
                            {j.recommendation}
                          </Badge>
                        </div>
                      </div>
                      <div className="flex items-center gap-1.5 ml-4 shrink-0">
                        <ScoreBadge label="Score" value={j.overall_score} />
                        <ScoreBadge label="Profit" value={j.profit_viability} />
                        <ScoreBadge label="Safety" value={j.resolution_safety} />
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </ScrollArea>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

// ── Usage Log Section ──

const PAGE_SIZE = 50

function formatTime(iso: string) {
  try {
    const d = new Date(iso)
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }) +
      ' ' + d.toLocaleDateString([], { month: 'short', day: 'numeric' })
  } catch {
    return iso
  }
}

function formatCost(usd: number) {
  if (usd === 0) return '$0'
  if (usd < 0.001) return '<$0.001'
  return `$${usd.toFixed(4)}`
}

function formatLatency(ms: number | null) {
  if (ms === null || ms === undefined) return '-'
  if (ms < 1000) return `${ms}ms`
  return `${(ms / 1000).toFixed(1)}s`
}

function UsageLogSection() {
  const [purposeFilter, setPurposeFilter] = useState<string>('__all__')
  const [providerFilter, setProviderFilter] = useState<string>('__all__')
  const [successFilter, setSuccessFilter] = useState<string>('__all__')
  const [offset, setOffset] = useState(0)
  const [expandedRow, setExpandedRow] = useState<string | null>(null)

  const queryParams = {
    limit: PAGE_SIZE,
    offset,
    purpose: purposeFilter !== '__all__' ? purposeFilter : undefined,
    provider: providerFilter !== '__all__' ? providerFilter : undefined,
    success: successFilter === '__all__' ? undefined : successFilter === 'true',
  }

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['llm-usage-log', queryParams],
    queryFn: () => listLLMUsageLog(queryParams),
    refetchInterval: 30_000,
  })

  const entries = data?.entries ?? []
  const total = data?.total ?? 0

  // Compute summary stats from visible entries
  const totalCalls = total
  const totalTokens = entries.reduce((s, e) => s + (e.input_tokens ?? 0) + (e.output_tokens ?? 0), 0)
  const totalCost = entries.reduce((s, e) => s + (e.cost_usd ?? 0), 0)
  const errorCount = entries.filter(e => !e.success).length
  const errorRate = entries.length > 0 ? ((errorCount / entries.length) * 100).toFixed(1) : '0'

  // Collect unique values for filters
  const purposes = [...new Set(entries.map(e => e.purpose).filter(Boolean))] as string[]
  const providers = [...new Set(entries.map(e => e.provider).filter(Boolean))] as string[]

  const handleFilterChange = () => {
    setOffset(0)
  }

  const hasPrev = offset > 0
  const hasNext = offset + PAGE_SIZE < total

  return (
    <div className="space-y-4">
      {/* Filter bar */}
      <Card className="overflow-hidden border-border/60 bg-card/80">
        <div className="h-0.5 bg-blue-400" />
        <CardContent className="p-3">
          <div className="flex items-center gap-3 flex-wrap">
            <div className="flex items-center gap-1.5">
              <Filter className="h-3.5 w-3.5 text-muted-foreground" />
              <span className="text-[10px] uppercase tracking-wide text-muted-foreground">Filters</span>
            </div>

            <Select
              value={purposeFilter}
              onValueChange={(val) => { setPurposeFilter(val); handleFilterChange() }}
            >
              <SelectTrigger className="h-7 w-[140px] text-xs bg-muted/60 border-border">
                <SelectValue placeholder="Purpose" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="__all__">All Purposes</SelectItem>
                {purposes.map(p => (
                  <SelectItem key={p} value={p}>{p}</SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Select
              value={providerFilter}
              onValueChange={(val) => { setProviderFilter(val); handleFilterChange() }}
            >
              <SelectTrigger className="h-7 w-[140px] text-xs bg-muted/60 border-border">
                <SelectValue placeholder="Provider" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="__all__">All Providers</SelectItem>
                {providers.map(p => (
                  <SelectItem key={p} value={p}>{p}</SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Select
              value={successFilter}
              onValueChange={(val) => { setSuccessFilter(val); handleFilterChange() }}
            >
              <SelectTrigger className="h-7 w-[120px] text-xs bg-muted/60 border-border">
                <SelectValue placeholder="Status" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="__all__">All Status</SelectItem>
                <SelectItem value="true">Success</SelectItem>
                <SelectItem value="false">Error</SelectItem>
              </SelectContent>
            </Select>

            <Button
              variant="outline"
              size="sm"
              onClick={() => refetch()}
              className="h-7 gap-1.5 text-xs ml-auto"
            >
              <RefreshCw className="h-3 w-3" />
              Refresh
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Summary stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 card-stagger">
        <MetricTile label="Total Calls" value={String(totalCalls)} icon={<Activity className="h-3.5 w-3.5" />} tone="info" />
        <MetricTile label="Page Tokens" value={totalTokens.toLocaleString()} icon={<Zap className="h-3.5 w-3.5" />} tone="neutral" />
        <MetricTile label="Page Cost" value={formatCost(totalCost)} icon={<DollarSign className="h-3.5 w-3.5" />} tone="neutral" />
        <MetricTile
          label="Page Error Rate"
          value={`${errorRate}%`}
          icon={<AlertCircle className="h-3.5 w-3.5" />}
          tone={Number(errorRate) >= 20 ? 'bad' : Number(errorRate) >= 5 ? 'warn' : 'good'}
        />
      </div>

      {/* Log table */}
      <Card className="overflow-hidden border-border/60 bg-card/80">
        <div className="h-0.5 bg-blue-400" />
        <CardHeader className="pb-2">
          <CardTitle className="flex items-center gap-2 text-base font-semibold">
            <Clock className="h-4 w-4 text-blue-400" />
            LLM Call Log
            <Badge variant="outline" className="text-[10px] ml-2">{total} total</Badge>
          </CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          {isLoading ? (
            <div className="flex items-center justify-center py-8">
              <RefreshCw className="w-6 h-6 animate-spin text-blue-400" />
            </div>
          ) : entries.length === 0 ? (
            <div className="text-center py-8">
              <Bot className="w-10 h-10 text-muted-foreground/30 mx-auto mb-3" />
              <p className="text-xs text-muted-foreground">No LLM calls recorded yet.</p>
            </div>
          ) : (
            <ScrollArea className="h-[420px]">
              {/* Table header */}
              <div className="sticky top-0 z-10 grid grid-cols-[100px_90px_80px_1fr_90px_70px_70px_60px] gap-2 px-4 py-2 text-[10px] uppercase tracking-wide text-muted-foreground bg-card border-b border-border/40">
                <span>Time</span>
                <span>Purpose</span>
                <span>Provider</span>
                <span>Model</span>
                <span>Tokens</span>
                <span>Cost</span>
                <span>Latency</span>
                <span className="text-center">Status</span>
              </div>

              {/* Rows */}
              <div className="divide-y divide-border/30">
                {entries.map((entry: LLMUsageLogEntry) => {
                  const isExpanded = expandedRow === entry.id
                  const isError = !entry.success

                  return (
                    <div key={entry.id}>
                      <button
                        onClick={() => setExpandedRow(isExpanded ? null : entry.id)}
                        className={cn(
                          'w-full grid grid-cols-[100px_90px_80px_1fr_90px_70px_70px_60px] gap-2 px-4 py-2 text-left text-xs transition-colors',
                          isError
                            ? 'bg-red-500/5 hover:bg-red-500/10'
                            : 'hover:bg-muted/40'
                        )}
                      >
                        <span className="text-muted-foreground font-data truncate">{formatTime(entry.requested_at)}</span>
                        <span className="truncate">
                          {entry.purpose ? (
                            <Badge variant="outline" className="text-[9px] h-4 px-1.5 bg-muted/30 border-border/40">
                              {entry.purpose}
                            </Badge>
                          ) : (
                            <span className="text-muted-foreground">-</span>
                          )}
                        </span>
                        <span className="truncate text-muted-foreground">{entry.provider}</span>
                        <span className="truncate font-data">{entry.model}</span>
                        <span className="font-data text-muted-foreground">
                          {entry.input_tokens}/{entry.output_tokens}
                        </span>
                        <span className="font-data">{formatCost(entry.cost_usd)}</span>
                        <span className="font-data text-muted-foreground">{formatLatency(entry.latency_ms)}</span>
                        <span className="flex justify-center">
                          {entry.success ? (
                            <CheckCircle className="w-3.5 h-3.5 text-emerald-400" />
                          ) : (
                            <AlertCircle className="w-3.5 h-3.5 text-red-400" />
                          )}
                        </span>
                      </button>

                      {/* Expanded details */}
                      {isExpanded && (
                        <div className={cn(
                          'px-4 py-3 text-xs border-t',
                          isError ? 'bg-red-500/5 border-red-500/20' : 'bg-muted/20 border-border/40'
                        )}>
                          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                            <div>
                              <p className="text-[10px] uppercase tracking-wide text-muted-foreground mb-0.5">ID</p>
                              <p className="font-data text-foreground/80 break-all">{entry.id}</p>
                            </div>
                            <div>
                              <p className="text-[10px] uppercase tracking-wide text-muted-foreground mb-0.5">Session</p>
                              <p className="font-data text-foreground/80 break-all">{entry.session_id ?? '-'}</p>
                            </div>
                            <div>
                              <p className="text-[10px] uppercase tracking-wide text-muted-foreground mb-0.5">Input Tokens</p>
                              <p className="font-data">{entry.input_tokens.toLocaleString()}</p>
                            </div>
                            <div>
                              <p className="text-[10px] uppercase tracking-wide text-muted-foreground mb-0.5">Output Tokens</p>
                              <p className="font-data">{entry.output_tokens.toLocaleString()}</p>
                            </div>
                          </div>
                          {isError && entry.error && (
                            <div className="mt-3 p-2.5 rounded-lg bg-red-500/10 border border-red-500/20">
                              <p className="text-[10px] uppercase tracking-wide text-red-400 mb-1">Error</p>
                              <p className="text-red-300 font-mono text-[11px] whitespace-pre-wrap">{entry.error}</p>
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  )
                })}
              </div>
            </ScrollArea>
          )}

          {/* Pagination */}
          {total > PAGE_SIZE && (
            <div className="flex items-center justify-between px-4 py-3 border-t border-border/40">
              <p className="text-xs text-muted-foreground">
                Showing {offset + 1}-{Math.min(offset + PAGE_SIZE, total)} of {total}
              </p>
              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  disabled={!hasPrev}
                  onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
                  className="h-7 text-xs"
                >
                  Previous
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  disabled={!hasNext}
                  onClick={() => setOffset(offset + PAGE_SIZE)}
                  className="h-7 text-xs"
                >
                  Next
                </Button>
              </div>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

// ── Main Activity View ──

export default function AIActivityView() {
  const [judgmentsOpen, setJudgmentsOpen] = useState(true)

  return (
    <div className="space-y-4">
      {/* Collapsible Judgments section */}
      <button
        onClick={() => setJudgmentsOpen(!judgmentsOpen)}
        className="flex items-center gap-2 text-sm font-medium text-foreground hover:text-foreground/80 transition-colors w-full"
      >
        {judgmentsOpen ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}
        <Target className="w-4 h-4 text-cyan-400" />
        AI Judgments
      </button>
      {judgmentsOpen && <JudgmentsPanel />}

      {/* Usage Log */}
      <UsageLogSection />
    </div>
  )
}
