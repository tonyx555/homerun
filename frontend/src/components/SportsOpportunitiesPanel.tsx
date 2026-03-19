import { useState, useMemo, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  Trophy,
  TrendingDown,
  Target,
  Clock,
  ChevronLeft,
  ChevronRight,
  Zap,
  Shield,
} from 'lucide-react'
import { cn } from '../lib/utils'
import { getOpportunities, Opportunity } from '../services/api'
import { useWebSocket } from '../hooks/useWebSocket'
import { Button } from './ui/button'
import { Separator } from './ui/separator'
import OpportunityTable from './OpportunityTable'
import OpportunityTerminal from './OpportunityTerminal'
import OpportunityEmptyState from './OpportunityEmptyState'
import OpportunityCard from './OpportunityCard'
import BuyButton from './BuyButton'

// ─── Constants ────────────────────────────────────────────

const ITEMS_PER_PAGE = 20
const SPORTS_OVERREACTION_FADER_STRATEGY = 'sports_overreaction_fader'

const MARKET_TYPE_LABELS: Record<string, string> = {
  moneyline: 'Moneyline',
  spread: 'Spread',
  total: 'Total',
  over_under: 'Over/Under',
  player_prop: 'Player Prop',
}

const MARKET_TYPE_COLORS: Record<string, string> = {
  moneyline: 'bg-blue-500/15 text-blue-400 border-blue-500/25',
  spread: 'bg-purple-500/15 text-purple-400 border-purple-500/25',
  total: 'bg-amber-500/15 text-amber-400 border-amber-500/25',
  over_under: 'bg-amber-500/15 text-amber-400 border-amber-500/25',
  player_prop: 'bg-pink-500/15 text-pink-400 border-pink-500/25',
}

// ─── Helpers ──────────────────────────────────────────────

function formatPrice(price: number): string {
  return `${(price * 100).toFixed(1)}\u00A2`
}

function formatPct(pct: number): string {
  const sign = pct >= 0 ? '+' : ''
  return `${sign}${pct.toFixed(1)}%`
}

function formatGameTime(isoStr: string | null | undefined): string {
  if (!isoStr) return ''
  try {
    const d = new Date(isoStr)
    if (isNaN(d.getTime())) return ''
    const now = new Date()
    const diffMs = d.getTime() - now.getTime()
    if (diffMs < 0) return 'In Progress'
    if (diffMs < 3600_000) return `${Math.ceil(diffMs / 60_000)}m`
    if (diffMs < 86400_000) {
      return d.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' })
    }
    return d.toLocaleDateString([], { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' })
  } catch {
    return ''
  }
}

function timeAgo(dateStr: string): string {
  const diff = Date.now() - new Date(dateStr).getTime()
  if (diff < 60_000) return 'just now'
  if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`
  if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`
  return `${Math.floor(diff / 86_400_000)}d ago`
}

function extractSportFromQuestion(question: string): string {
  const q = question.toLowerCase()
  if (q.includes('nba') || q.includes('basketball')) return 'NBA'
  if (q.includes('nfl') || q.includes('football')) return 'NFL'
  if (q.includes('mlb') || q.includes('baseball')) return 'MLB'
  if (q.includes('nhl') || q.includes('hockey')) return 'NHL'
  if (q.includes('soccer') || q.includes('fifa') || q.includes('premier league')) return 'Soccer'
  if (q.includes('ufc') || q.includes('mma')) return 'UFC'
  if (q.includes('tennis') || q.includes('atp') || q.includes('wta')) return 'Tennis'
  if (q.includes('f1') || q.includes('formula')) return 'F1'
  if (q.includes('golf') || q.includes('pga')) return 'Golf'
  if (q.includes('boxing')) return 'Boxing'
  return 'Sports'
}

// ─── Sport Game Card ──────────────────────────────────────

function SportsGameCard({
  opportunity,
}: {
  opportunity: Opportunity
}) {
  const ctx = (opportunity.strategy_context || {}) as Record<string, unknown>
  const entryPrice = Number(ctx.entry_price || 0)
  const baselinePrice = Number(ctx.baseline_price || 0)
  const movePct = Number(ctx.move_pct || 0)
  const reversionTarget = Number(ctx.reversion_target || 0)
  const reversionEdge = Number(ctx.reversion_edge || 0)
  const fadeOutcome = String(ctx.fade_outcome || 'YES')
  const isFavoriteDropping = Boolean(ctx.is_favorite_dropping)
  const hoursToResolution = Number(ctx.hours_to_resolution || 0)
  const gameStartTime = ctx.game_start_time as string | null | undefined
  const sportsMarketType = String(ctx.sports_market_type || '').toLowerCase()
  const line = ctx.line as number | null | undefined
  const confidence = opportunity.confidence ?? 0
  const riskScore = opportunity.risk_score ?? 0

  const market = opportunity.markets?.[0]
  const question = market?.question || opportunity.title
  const sport = extractSportFromQuestion(question)
  const gameTimeStr = formatGameTime(gameStartTime)

  const marketTypeLabel = MARKET_TYPE_LABELS[sportsMarketType] || (sportsMarketType ? sportsMarketType.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase()) : null)
  const marketTypeColor = MARKET_TYPE_COLORS[sportsMarketType] || 'bg-zinc-500/15 text-zinc-400 border-zinc-500/25'

  const moveBarWidth = Math.min(Math.abs(movePct) * 2.5, 100)

  return (
    <div className="group relative rounded-xl border border-border/50 bg-card/60 backdrop-blur-sm overflow-hidden transition-all hover:border-emerald-500/30 hover:shadow-lg hover:shadow-emerald-500/5">
      {/* Accent bar */}
      <div className="absolute top-0 left-0 right-0 h-[2px] bg-gradient-to-r from-emerald-500 via-emerald-400 to-transparent" />

      <div className="p-4 space-y-3">
        {/* Header: Sport + Market Type + Game Time */}
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2 min-w-0">
            <span className="inline-flex items-center gap-1 rounded-md bg-emerald-500/15 text-emerald-400 border border-emerald-500/25 px-2 py-0.5 text-[10px] font-semibold tracking-wider uppercase">
              <Trophy className="w-3 h-3" />
              {sport}
            </span>
            {marketTypeLabel && (
              <span className={cn('rounded-md border px-1.5 py-0.5 text-[10px] font-medium', marketTypeColor)}>
                {marketTypeLabel}
                {line != null && ` ${line > 0 ? '+' : ''}${line}`}
              </span>
            )}
          </div>
          <div className="flex items-center gap-2 shrink-0">
            {gameTimeStr && (
              <span className={cn(
                'inline-flex items-center gap-1 text-[10px] font-medium',
                gameTimeStr === 'In Progress' ? 'text-red-400' : 'text-muted-foreground',
              )}>
                <Clock className="w-3 h-3" />
                {gameTimeStr === 'In Progress' ? (
                  <span className="flex items-center gap-1">
                    <span className="w-1.5 h-1.5 rounded-full bg-red-400 animate-pulse" />
                    LIVE
                  </span>
                ) : gameTimeStr}
              </span>
            )}
            <span className="text-[10px] text-muted-foreground/60">
              {timeAgo(opportunity.detected_at)}
            </span>
          </div>
        </div>

        {/* Question */}
        <p className="text-sm font-medium text-foreground/90 leading-snug line-clamp-2">
          {question}
        </p>

        {/* Price Movement Visualization */}
        <div className="rounded-lg bg-muted/30 border border-border/30 p-3 space-y-2">
          <div className="flex items-center justify-between text-xs">
            <span className="text-muted-foreground">
              {fadeOutcome} price movement
            </span>
            <span className={cn(
              'font-mono font-semibold',
              movePct < 0 ? 'text-red-400' : 'text-green-400',
            )}>
              {formatPct(movePct)}
            </span>
          </div>

          {/* Price bar */}
          <div className="flex items-center gap-2">
            <span className="text-[11px] font-mono text-muted-foreground w-12 text-right">
              {formatPrice(baselinePrice)}
            </span>
            <div className="flex-1 relative h-2 rounded-full bg-muted/50 overflow-hidden">
              <div
                className="absolute inset-y-0 left-0 rounded-full bg-gradient-to-r from-red-500 to-red-400"
                style={{ width: `${moveBarWidth}%` }}
              />
            </div>
            <span className="text-[11px] font-mono font-semibold text-foreground w-12">
              {formatPrice(entryPrice)}
            </span>
          </div>

          {/* Reversion target */}
          <div className="flex items-center justify-between text-[11px]">
            <div className="flex items-center gap-1.5 text-muted-foreground">
              <Target className="w-3 h-3 text-emerald-400" />
              <span>Reversion target</span>
            </div>
            <div className="flex items-center gap-1 font-mono">
              <span className="text-foreground">{formatPrice(reversionTarget)}</span>
              <span className="text-emerald-400">
                (+{(reversionEdge * 100).toFixed(1)}{'\u00A2'} edge)
              </span>
            </div>
          </div>
        </div>

        {/* Metrics Row */}
        <div className="grid grid-cols-4 gap-2">
          <div className="text-center">
            <div className="text-[10px] text-muted-foreground mb-0.5">Confidence</div>
            <div className={cn(
              'text-sm font-semibold font-mono',
              confidence >= 0.7 ? 'text-green-400' : confidence >= 0.5 ? 'text-yellow-400' : 'text-muted-foreground',
            )}>
              {(confidence * 100).toFixed(0)}%
            </div>
          </div>
          <div className="text-center">
            <div className="text-[10px] text-muted-foreground mb-0.5">Risk</div>
            <div className={cn(
              'text-sm font-semibold font-mono',
              riskScore <= 0.4 ? 'text-green-400' : riskScore <= 0.65 ? 'text-yellow-400' : 'text-red-400',
            )}>
              {riskScore.toFixed(2)}
            </div>
          </div>
          <div className="text-center">
            <div className="text-[10px] text-muted-foreground mb-0.5">ROI</div>
            <div className={cn(
              'text-sm font-semibold font-mono',
              opportunity.roi_percent > 0 ? 'text-green-400' : 'text-red-400',
            )}>
              {opportunity.roi_percent.toFixed(1)}%
            </div>
          </div>
          <div className="text-center">
            <div className="text-[10px] text-muted-foreground mb-0.5">Resolves</div>
            <div className="text-sm font-semibold font-mono text-foreground/80">
              {hoursToResolution < 1 ? `${(hoursToResolution * 60).toFixed(0)}m` : `${hoursToResolution.toFixed(1)}h`}
            </div>
          </div>
        </div>

        {/* Tags */}
        <div className="flex items-center gap-1.5 flex-wrap">
          {isFavoriteDropping && (
            <span className="inline-flex items-center gap-1 rounded-md bg-yellow-500/10 text-yellow-400 border border-yellow-500/20 px-1.5 py-0.5 text-[10px] font-medium">
              <Zap className="w-2.5 h-2.5" />
              Surprise Move
            </span>
          )}
          <span className="inline-flex items-center gap-1 rounded-md bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 px-1.5 py-0.5 text-[10px] font-medium">
            <TrendingDown className="w-2.5 h-2.5" />
            Fade {fadeOutcome}
          </span>
          <span className="inline-flex items-center gap-1 rounded-md bg-zinc-500/10 text-zinc-400 border border-zinc-500/20 px-1.5 py-0.5 text-[10px] font-medium">
            <Shield className="w-2.5 h-2.5" />
            Mean Reversion
          </span>
        </div>

        {/* Buy Button */}
        <BuyButton opportunity={opportunity} />
      </div>
    </div>
  )
}

// ─── Main Panel ───────────────────────────────────────────

interface SportsOpportunitiesPanelProps {
  onOpenCopilot?: (opportunity: Opportunity) => void
  viewMode?: 'card' | 'list' | 'terminal'
  showSettingsButton?: boolean
  onAnalyzeTargetsChange?: (targets: { visibleIds: string[]; allIds: string[] }) => void
}

export default function SportsOpportunitiesPanel({
  onOpenCopilot,
  viewMode = 'card',
  onAnalyzeTargetsChange,
}: SportsOpportunitiesPanelProps) {
  const { isConnected } = useWebSocket('/ws')
  const [currentPage, setCurrentPage] = useState(0)

  const { data: oppData, isLoading } = useQuery({
    queryKey: ['sports-opportunities', currentPage],
    queryFn: () =>
      getOpportunities({
        category: 'sports',
        limit: ITEMS_PER_PAGE,
        offset: currentPage * ITEMS_PER_PAGE,
      }),
    refetchInterval: isConnected ? false : 15000,
  })

  const opportunities = oppData?.opportunities || []
  const total = oppData?.total || 0
  const totalPages = Math.max(1, Math.ceil(total / ITEMS_PER_PAGE))

  const visibleIds = useMemo(
    () => opportunities.map(o => o.id),
    [opportunities],
  )

  useEffect(() => {
    onAnalyzeTargetsChange?.({ visibleIds, allIds: visibleIds })
  }, [visibleIds, onAnalyzeTargetsChange])

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="flex items-center gap-2 text-muted-foreground">
          <Trophy className="w-5 h-5 animate-pulse text-emerald-400" />
          <span className="text-sm">Loading sports opportunities...</span>
        </div>
      </div>
    )
  }

  if (opportunities.length === 0) {
    return (
      <OpportunityEmptyState
        title="No sports opportunities"
        description="Sports strategies are active and monitoring markets. New opportunities will appear here as they are detected."
      />
    )
  }

  return (
    <>
      {/* Header */}
      <div className="mb-4 rounded-xl border border-border/40 bg-card/40 p-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Trophy className="w-4 h-4 text-emerald-400" />
            <span className="text-xs font-medium text-foreground/80">Sports Opportunities</span>
            <span className="inline-flex items-center justify-center rounded-full bg-emerald-500/15 text-emerald-400 text-[10px] font-medium min-w-[20px] h-4 px-1.5">
              {total}
            </span>
          </div>
          <div className="flex items-center gap-2">
            {isConnected && (
              <span className="flex items-center gap-1 text-[10px] text-emerald-400">
                <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />
                Live
              </span>
            )}
          </div>
        </div>
      </div>

      {/* Content */}
      {viewMode === 'terminal' ? (
        <OpportunityTerminal
          opportunities={opportunities}
          onOpenCopilot={onOpenCopilot}
          isConnected={isConnected}
          totalCount={total}
        />
      ) : viewMode === 'list' ? (
        <OpportunityTable
          opportunities={opportunities}
          onOpenCopilot={onOpenCopilot}
        />
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-3 card-stagger">
          {opportunities.map((opp) => (
            opp.strategy === SPORTS_OVERREACTION_FADER_STRATEGY ? (
              <SportsGameCard
                key={opp.stable_id || opp.id}
                opportunity={opp}
              />
            ) : (
              <OpportunityCard
                key={opp.stable_id || opp.id}
                opportunity={opp}
                onOpenCopilot={onOpenCopilot}
              />
            )
          ))}
        </div>
      )}

      {/* Pagination */}
      {total > ITEMS_PER_PAGE && (
        <div className="mt-5">
          <Separator />
          <div className="flex items-center justify-between pt-4">
            <div className="text-xs text-muted-foreground">
              {currentPage * ITEMS_PER_PAGE + 1} - {Math.min((currentPage + 1) * ITEMS_PER_PAGE, total)} of {total}
            </div>
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                className="h-7 text-xs"
                onClick={() => setCurrentPage(p => Math.max(0, p - 1))}
                disabled={currentPage === 0}
              >
                <ChevronLeft className="w-3.5 h-3.5" />
                Prev
              </Button>
              <span className="px-2.5 py-1 bg-card rounded-lg text-xs border border-border font-mono">
                {currentPage + 1}/{totalPages}
              </span>
              <Button
                variant="outline"
                size="sm"
                className="h-7 text-xs"
                onClick={() => setCurrentPage(p => p + 1)}
                disabled={currentPage >= totalPages - 1}
              >
                Next
                <ChevronRight className="w-3.5 h-3.5" />
              </Button>
            </div>
          </div>
        </div>
      )}
    </>
  )
}
