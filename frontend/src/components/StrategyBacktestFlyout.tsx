import { useMemo, useState, type ComponentType } from 'react'
import { useMutation } from '@tanstack/react-query'
import {
  Activity,
  AlertTriangle,
  BarChart3,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Clock,
  Database,
  FlaskConical,
  Gauge,
  LineChart,
  Loader2,
  Play,
  Settings2,
  ShieldCheck,
  Sparkles,
  Target,
  TrendingDown,
  TrendingUp,
  XCircle,
  Zap,
} from 'lucide-react'
import { Badge } from './ui/badge'
import { Button } from './ui/button'
import { Input } from './ui/input'
import { Label } from './ui/label'
import { ScrollArea } from './ui/scroll-area'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './ui/select'
import { Sheet, SheetContent, SheetTitle } from './ui/sheet'
import { Switch } from './ui/switch'
import { cn } from '../lib/utils'
import { normalizeUtcTimestampsInPlace } from '../lib/timestamps'
import axios from 'axios'

const api = axios.create({ baseURL: '/api', timeout: 120000 })

type BacktestMode = 'detect' | 'evaluate' | 'exit'
type Tone = 'good' | 'warn' | 'bad' | 'neutral'

interface MetricCI {
  value: number
  ci_low: number | null
  ci_high: number | null
}

interface ExecutionBacktestResult {
  success: boolean
  strategy_slug: string
  strategy_name: string
  class_name: string
  initial_capital_usd: number
  start_iso: string
  end_iso: string
  n_intents: number
  n_snapshots: number
  final_equity_usd: number
  total_return_pct: number
  annualized_return_pct: number
  sharpe: MetricCI
  sortino: MetricCI
  calmar: MetricCI
  max_drawdown_pct: number
  max_drawdown_usd: number
  drawdown_duration_seconds: number
  hit_rate: MetricCI
  profit_factor: MetricCI
  expectancy_usd: MetricCI
  avg_win_usd: number
  avg_loss_usd: number
  trade_count: number
  fees_paid_usd: number
  fees_per_fill_usd: number
  fees_resolution_usd: number
  total_fills: number
  rejected_orders: number
  cancelled_orders: number
  closed_position_count: number
  open_position_count: number
  correlation_pairs: Array<{ token_a: string; token_b: string; correlation: number }>
  fills_sample: Array<{
    order_id: string
    token_id: string
    side: string
    price: number
    size: number
    fee_usd: number
    occurred_at: string
    fill_index: number
    is_maker: boolean
  }>
  equity_curve_sample: Array<{ at: string; equity_usd: number }>
  load_time_ms: number
  data_fetch_time_ms: number
  run_time_ms: number
  total_time_ms: number
  validation_errors: string[]
  validation_warnings: string[]
  runtime_error: string | null
  runtime_traceback: string | null
}

interface ExecutionRunSettings {
  lookbackHours: string
  initialCapitalUsd: string
  submitP50Ms: string
  submitP95Ms: string
  cancelP50Ms: string
  cancelP95Ms: string
  seed: string
  tokenIds: string  // comma-separated, blank → auto
}

interface QualityFilter {
  filter_name: string
  passed: boolean
  reason: string
  threshold: number | string | null
  actual_value: number | string | null
}

interface QualityReportData {
  opportunity_id: string
  passed: boolean
  rejection_reasons: string[]
  filters: QualityFilter[]
}

interface DecisionCheck {
  check_key?: string
  check_label?: string
  passed?: boolean
  score?: number | null
}

interface BacktestResult {
  success: boolean
  strategy_slug: string
  strategy_name: string
  class_name: string
  load_time_ms: number
  data_fetch_time_ms: number
  total_time_ms: number
  validation_errors: string[]
  validation_warnings: string[]
  runtime_error: string | null
  runtime_traceback: string | null

  num_events?: number
  num_markets?: number
  num_prices?: number
  data_source?: string
  replay_mode?: string
  replay_steps?: number
  replay_markets?: number
  replay_window_hours?: number
  replay_timeframe?: string
  opportunities?: Array<Record<string, any>>
  num_opportunities?: number
  quality_reports?: QualityReportData[]
  detect_time_ms?: number

  num_signals?: number
  decisions?: Array<Record<string, any>>
  selected?: number
  skipped?: number
  blocked?: number
  evaluate_time_ms?: number

  num_positions?: number
  exit_decisions?: Array<Record<string, any>>
  would_close?: number
  would_reduce?: number
  would_hold?: number
  errors?: number
  exit_time_ms?: number
}

interface OutcomeRow {
  id: string
  primary: string
  secondary?: string
  action: string
  tone: Tone
  headlineMetric: string
  subMetrics: string[]
  reason?: string
  detail: Record<string, any>
}

interface SummaryCard {
  label: string
  value: string
  subtitle: string
  tone: Tone
  icon: ComponentType<{ className?: string }>
}

interface BacktestRunSettings {
  detect: {
    useOhlcReplay: boolean
    replayLookbackHours: string
    replayTimeframe: string
    replayMaxMarkets: string
    replayMaxSteps: string
    maxOpportunities: string
  }
  evaluate: {
    maxSignals: string
  }
  exit: {
    maxPositions: string
  }
}

function toNumber(value: unknown): number {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : 0
}

function fmt(n: number, decimals = 0): string {
  return toNumber(n).toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  })
}

function fmtMoney(n: number): string {
  return `$${fmt(n, 2)}`
}

function fmtSignedPct(n: number): string {
  const value = toNumber(n)
  return `${value > 0 ? '+' : ''}${fmt(value, 2)}%`
}

function fmtPrice(n: number): string {
  return `$${fmt(n, 3)}`
}

function fmtMinutes(n: number): string {
  const value = toNumber(n)
  if (value < 60) {
    return `${fmt(value, 0)}m`
  }
  return `${fmt(value / 60, 1)}h`
}

function fmtMs(ms: number): string {
  const value = toNumber(ms)
  if (value < 1000) {
    return `${Math.round(value)}ms`
  }
  return `${(value / 1000).toFixed(1)}s`
}

function CollapsibleCard({
  title,
  subtitle,
  icon: Icon,
  iconClass,
  defaultOpen = false,
  actions,
  children,
}: {
  title: string
  subtitle?: string
  icon?: ComponentType<{ className?: string }>
  iconClass?: string
  defaultOpen?: boolean
  actions?: React.ReactNode
  children: React.ReactNode
}) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <div className="rounded-lg border border-border/40 bg-card/30 overflow-hidden">
      {/* Header is a flex row with the toggle as one button and actions
          as a sibling — nesting <button> inside <button> is invalid HTML
          and triggers a React warning. */}
      <div className="flex items-center gap-2 px-3 py-2 hover:bg-card/50 transition-colors">
        <button
          type="button"
          className="flex items-center gap-2 flex-1 min-w-0 text-left cursor-pointer"
          onClick={() => setOpen(!open)}
          aria-expanded={open}
        >
          {open ? (
            <ChevronDown className="w-3.5 h-3.5 text-muted-foreground shrink-0" />
          ) : (
            <ChevronRight className="w-3.5 h-3.5 text-muted-foreground shrink-0" />
          )}
          {Icon && <Icon className={cn('w-3.5 h-3.5 shrink-0', iconClass || 'text-muted-foreground')} />}
          <div className="min-w-0 flex-1">
            <p className="text-[11px] font-medium leading-tight">{title}</p>
            {subtitle && <p className="text-[10px] text-muted-foreground leading-tight mt-0.5">{subtitle}</p>}
          </div>
        </button>
        {actions}
      </div>
      {open && <div className="px-3 pb-3 border-t border-border/20">{children}</div>}
    </div>
  )
}

function fmtCi(metric: MetricCI | undefined, decimals = 2): string {
  if (!metric) return '—'
  return fmt(toNumber(metric.value), decimals)
}

function fmtCiBand(metric: MetricCI | undefined, decimals = 2): string | null {
  if (!metric || metric.ci_low === null || metric.ci_high === null) return null
  return `[${fmt(metric.ci_low, decimals)}, ${fmt(metric.ci_high, decimals)}]`
}

function ciToneFromValue(value: number, target = 0): Tone {
  if (!Number.isFinite(value) || value === 0) return 'neutral'
  return value > target ? 'good' : 'bad'
}

function defaultExecutionSettings(): ExecutionRunSettings {
  return {
    lookbackHours: '24',
    initialCapitalUsd: '1000',
    submitP50Ms: '350',
    submitP95Ms: '900',
    cancelP50Ms: '200',
    cancelP95Ms: '600',
    seed: '42',
    tokenIds: '',
  }
}

function buildExecutionPayload(
  sourceCode: string,
  slug: string,
  config: Record<string, any> | undefined,
  s: ExecutionRunSettings
) {
  const tokens = s.tokenIds
    .split(',')
    .map((t) => t.trim())
    .filter(Boolean)
  const num = (v: string, fallback: number) => {
    const n = Number(v)
    return Number.isFinite(n) ? n : fallback
  }
  return {
    source_code: sourceCode,
    slug,
    config: config || {},
    token_ids: tokens.length ? tokens : null,
    lookback_hours: num(s.lookbackHours, 24),
    initial_capital_usd: num(s.initialCapitalUsd, 1000),
    submit_latency_p50_ms: num(s.submitP50Ms, 350),
    submit_latency_p95_ms: num(s.submitP95Ms, 900),
    cancel_latency_p50_ms: num(s.cancelP50Ms, 200),
    cancel_latency_p95_ms: num(s.cancelP95Ms, 600),
    seed: num(s.seed, 42),
  }
}

function percent(part: number, total: number): number {
  if (total <= 0) {
    return 0
  }
  return (part / total) * 100
}

function mean(values: number[]): number {
  if (values.length === 0) {
    return 0
  }
  return values.reduce((sum, value) => sum + value, 0) / values.length
}

function actionTone(action: string): Tone {
  const value = action.toLowerCase()
  if (value === 'selected' || value === 'close' || value === 'quality-pass' || value === 'pass') {
    return 'good'
  }
  if (value === 'blocked' || value === 'error' || value === 'quality-fail' || value === 'fail') {
    return 'bad'
  }
  if (value === 'reduce') {
    return 'warn'
  }
  return 'neutral'
}

function toneClasses(tone: Tone): string {
  if (tone === 'good') {
    return 'border-emerald-500/30 bg-emerald-500/5 text-emerald-300'
  }
  if (tone === 'warn') {
    return 'border-amber-500/30 bg-amber-500/5 text-amber-300'
  }
  if (tone === 'bad') {
    return 'border-red-500/30 bg-red-500/5 text-red-300'
  }
  return 'border-border/40 bg-card/30 text-foreground'
}

function phaseLabel(mode: BacktestMode): string {
  if (mode === 'detect') {
    return 'Detection'
  }
  if (mode === 'evaluate') {
    return 'Evaluation'
  }
  return 'Exit Logic'
}

function phaseDurationMs(mode: BacktestMode, result: BacktestResult): number {
  if (mode === 'detect') {
    return toNumber(result.detect_time_ms)
  }
  if (mode === 'evaluate') {
    return toNumber(result.evaluate_time_ms)
  }
  return toNumber(result.exit_time_ms)
}

function runSampleSize(mode: BacktestMode, result: BacktestResult): number {
  if (mode === 'detect') {
    return toNumber(result.num_opportunities)
  }
  if (mode === 'evaluate') {
    return toNumber(result.num_signals)
  }
  return toNumber(result.num_positions)
}

function completenessScore(mode: BacktestMode, result: BacktestResult): number {
  if (mode === 'detect') {
    const opportunities = result.opportunities || []
    if (opportunities.length === 0) {
      return 0
    }
    let present = 0
    let total = 0
    opportunities.forEach((opp) => {
      total += 3
      if (Number.isFinite(Number(opp.roi_percent ?? opp.roi))) {
        present += 1
      }
      if (Number.isFinite(Number(opp.total_cost))) {
        present += 1
      }
      if (Number.isFinite(Number(opp.risk_score))) {
        present += 1
      }
    })
    return Math.round(percent(present, total))
  }

  if (mode === 'evaluate') {
    const decisions = result.decisions || []
    if (decisions.length === 0) {
      return 0
    }
    let present = 0
    let total = 0
    decisions.forEach((decision) => {
      total += 3
      if (String(decision.decision || '').trim()) {
        present += 1
      }
      if (String(decision.reason || decision.strategy_reason || '').trim()) {
        present += 1
      }
      if (Array.isArray(decision.checks)) {
        present += 1
      }
    })
    return Math.round(percent(present, total))
  }

  const exitDecisions = result.exit_decisions || []
  if (exitDecisions.length === 0) {
    return 0
  }
  let present = 0
  let total = 0
  exitDecisions.forEach((decision) => {
    total += 4
    if (String(decision.action || '').trim()) {
      present += 1
    }
    if (Number.isFinite(Number(decision.entry_price))) {
      present += 1
    }
    if (Number.isFinite(Number(decision.current_price))) {
      present += 1
    }
    if (Number.isFinite(Number(decision.pnl_pct))) {
      present += 1
    }
  })
  return Math.round(percent(present, total))
}

function buildOutcomeRows(mode: BacktestMode, result: BacktestResult | null): OutcomeRow[] {
  if (!result) {
    return []
  }

  if (mode === 'detect') {
    const opportunities = result.opportunities || []
    const qualityReports = result.quality_reports || []
    const qualityById = new Map<string, QualityReportData>()
    qualityReports.forEach((report) => {
      const key = String(report.opportunity_id || '').trim()
      if (key) {
        qualityById.set(key, report)
      }
    })

    return opportunities.map((opp, index) => {
      const oppId = String(opp.id || opp.stable_id || `opp-${index}`)
      const quality = qualityById.get(oppId) || qualityReports[index]
      const roi = toNumber(opp.roi_percent ?? opp.roi)
      const cost = toNumber(opp.total_cost)
      const risk = toNumber(opp.risk_score)
      const markets = Array.isArray(opp.markets) ? opp.markets.length : 0
      const legs = Array.isArray(opp.positions_to_take) ? opp.positions_to_take.length : 0
      const qualityAction = quality ? (quality.passed ? 'quality-pass' : 'quality-fail') : 'candidate'

      return {
        id: oppId,
        primary: String(opp.title || `Opportunity ${index + 1}`),
        secondary: String(opp.strategy || result.strategy_name || '').trim() || undefined,
        action: qualityAction,
        tone: actionTone(qualityAction),
        headlineMetric: `${fmtSignedPct(roi)} ROI`,
        subMetrics: [
          `Cost ${fmtMoney(cost)}`,
          `Risk ${fmt(risk, 2)}`,
          `Markets ${fmt(markets)}`,
          `Legs ${fmt(legs)}`,
        ],
        reason:
          quality && !quality.passed
            ? (quality.rejection_reasons || []).join(' | ')
            : String(opp.description || ''),
        detail: {
          opportunity: opp,
          quality_report: quality || null,
        },
      }
    })
  }

  if (mode === 'evaluate') {
    const decisions = result.decisions || []
    return decisions.map((decision, index) => {
      const checks = Array.isArray(decision.checks) ? (decision.checks as DecisionCheck[]) : []
      const passedChecks = checks.filter((check) => Boolean(check.passed)).length
      const action = String(decision.decision || 'skipped').toLowerCase()
      return {
        id: String(decision.signal_id || `signal-${index}`),
        primary: String(decision.source || decision.strategy_type || `Signal ${index + 1}`),
        secondary: String(decision.signal_id || '').trim() || undefined,
        action,
        tone: actionTone(action),
        headlineMetric: `Size ${fmtMoney(toNumber(decision.size_usd))}`,
        subMetrics: [
          `Checks ${passedChecks}/${checks.length}`,
          `Strategy ${String(decision.strategy_decision || 'n/a')}`,
        ],
        reason: String(decision.reason || decision.strategy_reason || ''),
        detail: {
          decision,
        },
      }
    })
  }

  const exitDecisions = result.exit_decisions || []
  return exitDecisions.map((decision, index) => {
    const action = String(decision.action || 'hold').toLowerCase()
    const reduceFraction = toNumber(decision.reduce_fraction)
    const subMetrics = [
      `Entry ${fmtPrice(toNumber(decision.entry_price))}`,
      `Now ${fmtPrice(toNumber(decision.current_price))}`,
      `Age ${fmtMinutes(toNumber(decision.age_minutes))}`,
      `Notional ${fmtMoney(toNumber(decision.notional_usd))}`,
    ]
    if (action === 'reduce' && reduceFraction > 0) {
      subMetrics.push(`Reduce ${fmt(reduceFraction * 100, 0)}%`)
    }

    return {
      id: String(decision.position_id || `position-${index}`),
      primary: String(decision.market_question || decision.market_id || `Position ${index + 1}`),
      secondary: String(decision.position_id || '').trim() || undefined,
      action,
      tone: actionTone(action),
      headlineMetric: `${fmtSignedPct(toNumber(decision.pnl_pct))} PnL`,
      subMetrics,
      reason: String(decision.reason || ''),
      detail: {
        decision,
      },
    }
  })
}

function buildSummaryCards(mode: BacktestMode, result: BacktestResult, dataCompleteness: number): SummaryCard[] {
  if (mode === 'detect') {
    const opportunities = result.opportunities || []
    const qualityReports = result.quality_reports || []
    const passCount = qualityReports.filter((report) => report.passed).length
    const roiValues = opportunities.map((opp) => toNumber(opp.roi_percent ?? opp.roi))
    const riskValues = opportunities.map((opp) => toNumber(opp.risk_score))

    return [
      {
        label: 'Opportunities',
        value: fmt(toNumber(result.num_opportunities)),
        subtitle: 'Detected opportunities',
        tone: toNumber(result.num_opportunities) > 0 ? 'good' : 'neutral',
        icon: Target,
      },
      {
        label: 'Quality Pass',
        value: qualityReports.length > 0 ? `${fmt(percent(passCount, qualityReports.length), 1)}%` : 'n/a',
        subtitle: `${fmt(passCount)}/${fmt(qualityReports.length)} passed`,
        tone: qualityReports.length > 0 && passCount > 0 ? 'good' : 'warn',
        icon: ShieldCheck,
      },
      {
        label: 'Average ROI',
        value: `${fmt(mean(roiValues), 2)}%`,
        subtitle: 'Across detected opportunities',
        tone: mean(roiValues) > 0 ? 'good' : 'neutral',
        icon: TrendingUp,
      },
      {
        label: 'Average Risk',
        value: fmt(mean(riskValues), 2),
        subtitle: 'Normalized risk score',
        tone: mean(riskValues) > 0.7 ? 'bad' : mean(riskValues) > 0.45 ? 'warn' : 'good',
        icon: Gauge,
      },
      {
        label: 'Replay Coverage',
        value: `${fmt(toNumber(result.replay_steps))} steps`,
        subtitle: `${fmt(toNumber(result.replay_markets))} markets`,
        tone: toNumber(result.replay_steps) > 0 ? 'good' : 'neutral',
        icon: BarChart3,
      },
      {
        label: 'Completeness',
        value: `${fmt(dataCompleteness)}%`,
        subtitle: 'Core fields populated',
        tone: dataCompleteness >= 90 ? 'good' : dataCompleteness >= 70 ? 'warn' : 'bad',
        icon: Database,
      },
    ]
  }

  if (mode === 'evaluate') {
    const decisions = result.decisions || []
    const checks = decisions.flatMap((decision) =>
      Array.isArray(decision.checks) ? (decision.checks as DecisionCheck[]) : []
    )
    const passedChecks = checks.filter((check) => Boolean(check.passed)).length
    const sizeValues = decisions
      .map((decision) => toNumber(decision.size_usd))
      .filter((value) => value > 0)
    const decisionErrors = decisions.filter(
      (decision) => String(decision.decision || '').toLowerCase() === 'error'
    ).length

    return [
      {
        label: 'Signals Tested',
        value: fmt(toNumber(result.num_signals)),
        subtitle: 'Recent emissions evaluated',
        tone: toNumber(result.num_signals) > 0 ? 'good' : 'neutral',
        icon: Target,
      },
      {
        label: 'Selected Rate',
        value: `${fmt(percent(toNumber(result.selected), toNumber(result.num_signals)), 1)}%`,
        subtitle: `${fmt(toNumber(result.selected))} selected`,
        tone: toNumber(result.selected) > 0 ? 'good' : 'neutral',
        icon: CheckCircle2,
      },
      {
        label: 'Blocked Rate',
        value: `${fmt(percent(toNumber(result.blocked), toNumber(result.num_signals)), 1)}%`,
        subtitle: `${fmt(toNumber(result.blocked))} blocked`,
        tone: toNumber(result.blocked) > 0 ? 'warn' : 'good',
        icon: AlertTriangle,
      },
      {
        label: 'Avg Size',
        value: sizeValues.length > 0 ? fmtMoney(mean(sizeValues)) : 'n/a',
        subtitle: 'Suggested position size',
        tone: sizeValues.length > 0 ? 'good' : 'neutral',
        icon: Gauge,
      },
      {
        label: 'Check Pass Rate',
        value: checks.length > 0 ? `${fmt(percent(passedChecks, checks.length), 1)}%` : 'n/a',
        subtitle: `${fmt(passedChecks)}/${fmt(checks.length)} checks passed`,
        tone: checks.length > 0 && passedChecks > 0 ? 'good' : 'warn',
        icon: ShieldCheck,
      },
      {
        label: 'Completeness',
        value: `${fmt(dataCompleteness)}%`,
        subtitle: `${fmt(decisionErrors)} decision errors`,
        tone: dataCompleteness >= 90 ? 'good' : dataCompleteness >= 70 ? 'warn' : 'bad',
        icon: Database,
      },
    ]
  }

  const exits = result.exit_decisions || []
  const pnlValues = exits
    .filter((decision) => String(decision.action || '').toLowerCase() !== 'error')
    .map((decision) => toNumber(decision.pnl_pct))
  const totalNotional = exits.reduce((sum, decision) => sum + toNumber(decision.notional_usd), 0)
  const actionable = toNumber(result.would_close) + toNumber(result.would_reduce)

  return [
    {
      label: 'Positions Tested',
      value: fmt(toNumber(result.num_positions)),
      subtitle: 'Open inventory sampled',
      tone: toNumber(result.num_positions) > 0 ? 'good' : 'neutral',
      icon: Target,
    },
    {
      label: 'Exit Rate',
      value: `${fmt(percent(actionable, toNumber(result.num_positions)), 1)}%`,
      subtitle: `${fmt(actionable)} actionable exits`,
      tone: actionable > 0 ? 'good' : 'neutral',
      icon: TrendingUp,
    },
    {
      label: 'Close / Reduce',
      value: `${fmt(toNumber(result.would_close))}/${fmt(toNumber(result.would_reduce))}`,
      subtitle: 'Action split',
      tone: toNumber(result.would_close) > 0 || toNumber(result.would_reduce) > 0 ? 'good' : 'neutral',
      icon: CheckCircle2,
    },
    {
      label: 'Hold',
      value: fmt(toNumber(result.would_hold)),
      subtitle: 'Held positions',
      tone: toNumber(result.would_hold) > 0 ? 'warn' : 'good',
      icon: Gauge,
    },
    {
      label: 'Average PnL',
      value: `${fmt(mean(pnlValues), 2)}%`,
      subtitle: 'Across non-error exits',
      tone: mean(pnlValues) > 0 ? 'good' : 'neutral',
      icon: BarChart3,
    },
    {
      label: 'Completeness',
      value: `${fmt(dataCompleteness)}%`,
      subtitle: `${fmtMoney(totalNotional)} notional`,
      tone: dataCompleteness >= 90 ? 'good' : dataCompleteness >= 70 ? 'warn' : 'bad',
      icon: Database,
    },
  ]
}

function SummaryMetricCard({ card }: { card: SummaryCard }) {
  return (
    <div className={cn('rounded-md border px-2.5 py-2', toneClasses(card.tone))}>
      <div className="flex items-center gap-1.5 mb-1">
        <card.icon className="w-3 h-3 opacity-70" />
        <span className="text-[9px] uppercase tracking-wider opacity-75">{card.label}</span>
      </div>
      <div className="text-sm font-mono font-semibold">{card.value}</div>
      <p className="text-[9px] opacity-70 mt-0.5">{card.subtitle}</p>
    </div>
  )
}

function OutcomeCard({
  row,
  expanded,
  onToggle,
}: {
  row: OutcomeRow
  expanded: boolean
  onToggle: () => void
}) {
  return (
    <div className="border border-border/40 rounded-lg overflow-hidden">
      <button
        className="w-full text-left px-3 py-2 hover:bg-card/40 transition-colors"
        onClick={onToggle}
      >
        <div className="flex items-start gap-2">
          {expanded ? (
            <ChevronDown className="w-3 h-3 text-muted-foreground shrink-0 mt-0.5" />
          ) : (
            <ChevronRight className="w-3 h-3 text-muted-foreground shrink-0 mt-0.5" />
          )}
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2">
              <span className="text-[11px] font-medium truncate">{row.primary}</span>
              <Badge
                variant="outline"
                className={cn(
                  'text-[8px] h-3.5 uppercase',
                  row.tone === 'good'
                    ? 'border-emerald-500/30 text-emerald-400'
                    : row.tone === 'warn'
                      ? 'border-amber-500/30 text-amber-400'
                      : row.tone === 'bad'
                        ? 'border-red-500/30 text-red-400'
                        : 'border-border/40 text-muted-foreground'
                )}
              >
                {row.action}
              </Badge>
              <span className="ml-auto text-[10px] font-mono font-medium">{row.headlineMetric}</span>
            </div>
            {row.secondary && (
              <p className="text-[10px] text-muted-foreground mt-0.5 truncate">{row.secondary}</p>
            )}
            <div className="mt-1 flex flex-wrap gap-1.5">
              {row.subMetrics.map((metric, index) => (
                <span
                  key={`${row.id}-${index}`}
                  className="text-[9px] px-1.5 py-0.5 rounded bg-muted/40 text-muted-foreground font-mono"
                >
                  {metric}
                </span>
              ))}
            </div>
            {row.reason && <p className="text-[10px] text-muted-foreground mt-1.5 leading-relaxed">{row.reason}</p>}
          </div>
        </div>
      </button>
      {expanded && (
        <div className="px-3 pb-3 border-t border-border/20">
          <pre className="text-[10px] font-mono bg-black/20 rounded p-2 mt-2 overflow-x-auto whitespace-pre">
            {JSON.stringify(row.detail, null, 2)}
          </pre>
        </div>
      )}
    </div>
  )
}

const MODE_ENDPOINTS: Record<BacktestMode, string> = {
  detect: '/validation/code-backtest',
  evaluate: '/validation/code-backtest/evaluate',
  exit: '/validation/code-backtest/exit',
}

const MODE_LABELS: Record<BacktestMode, { label: string; desc: string; running: string }> = {
  detect: {
    label: 'Detect',
    desc: 'Find opportunities in current and replayed market states',
    running: 'Running detection...',
  },
  evaluate: {
    label: 'Evaluate',
    desc: 'Score recent signals through strategy and platform gates',
    running: 'Evaluating signals...',
  },
  exit: {
    label: 'Exit',
    desc: 'Run should_exit() against live open position inventory',
    running: 'Testing exit logic...',
  },
}

const DETECT_REPLAY_OPTIONS = {
  use_ohlc_replay: true,
  replay_lookback_hours: 24,
  replay_timeframe: '30m',
  replay_max_markets: 80,
  replay_max_steps: 72,
  max_opportunities: 100,
}

const BACKTEST_MODES: BacktestMode[] = ['detect', 'evaluate', 'exit']
const REPLAY_TIMEFRAME_OPTIONS = ['5m', '15m', '30m', '1h', '4h']

function createDefaultBacktestSettings(): BacktestRunSettings {
  return {
    detect: {
      useOhlcReplay: DETECT_REPLAY_OPTIONS.use_ohlc_replay,
      replayLookbackHours: String(DETECT_REPLAY_OPTIONS.replay_lookback_hours),
      replayTimeframe: DETECT_REPLAY_OPTIONS.replay_timeframe,
      replayMaxMarkets: String(DETECT_REPLAY_OPTIONS.replay_max_markets),
      replayMaxSteps: String(DETECT_REPLAY_OPTIONS.replay_max_steps),
      maxOpportunities: String(DETECT_REPLAY_OPTIONS.max_opportunities),
    },
    evaluate: {
      maxSignals: '50',
    },
    exit: {
      maxPositions: '50',
    },
  }
}

function normalizeIntegerSetting(value: string, fallback: number, min: number, max: number): number {
  const parsed = Number.parseInt(String(value || '').trim(), 10)
  if (!Number.isFinite(parsed)) {
    return fallback
  }
  return Math.min(max, Math.max(min, parsed))
}

function buildBacktestPayload(
  mode: BacktestMode,
  sourceCode: string,
  slug: string,
  config: Record<string, any> | undefined,
  settings: BacktestRunSettings
): Record<string, any> {
  return {
    source_code: sourceCode,
    slug,
    config: config || {},
    use_ohlc_replay: mode === 'detect' ? settings.detect.useOhlcReplay : false,
    replay_lookback_hours: normalizeIntegerSetting(settings.detect.replayLookbackHours, 24, 1, 720),
    replay_timeframe: settings.detect.replayTimeframe || DETECT_REPLAY_OPTIONS.replay_timeframe,
    replay_max_markets: normalizeIntegerSetting(settings.detect.replayMaxMarkets, 80, 1, 300),
    replay_max_steps: normalizeIntegerSetting(settings.detect.replayMaxSteps, 72, 1, 500),
    max_opportunities: normalizeIntegerSetting(settings.detect.maxOpportunities, 100, 1, 500),
    max_signals: normalizeIntegerSetting(settings.evaluate.maxSignals, 50, 1, 500),
    max_positions: normalizeIntegerSetting(settings.exit.maxPositions, 50, 1, 500),
  }
}

interface IntegratedBacktestScore {
  overall: number
  readiness: number
  passedPhases: number
  phaseScores: Record<BacktestMode, number>
  phaseCompleteness: Record<BacktestMode, number>
  phaseSamples: Record<BacktestMode, number>
  phasePassed: Record<BacktestMode, boolean>
}

function scoreTone(score: number): Tone {
  if (score >= 85) {
    return 'good'
  }
  if (score >= 65) {
    return 'warn'
  }
  return 'bad'
}

function createRequestFailureResult(mode: BacktestMode, slug: string, error: unknown): BacktestResult {
  const detail =
    axios.isAxiosError(error)
      ? String(error.response?.data?.detail || error.message || 'Request failed')
      : String(error)
  return {
    success: false,
    strategy_slug: slug,
    strategy_name: slug,
    class_name: '',
    load_time_ms: 0,
    data_fetch_time_ms: 0,
    total_time_ms: 0,
    validation_errors: [],
    validation_warnings: [],
    runtime_error: `${phaseLabel(mode)} request failed: ${detail}`,
    runtime_traceback: null,
  }
}

function buildIntegratedScore(results: Partial<Record<BacktestMode, BacktestResult>>): IntegratedBacktestScore {
  const phaseScores: Record<BacktestMode, number> = {
    detect: 0,
    evaluate: 0,
    exit: 0,
  }
  const phaseCompleteness: Record<BacktestMode, number> = {
    detect: 0,
    evaluate: 0,
    exit: 0,
  }
  const phaseSamples: Record<BacktestMode, number> = {
    detect: 0,
    evaluate: 0,
    exit: 0,
  }
  const phasePassed: Record<BacktestMode, boolean> = {
    detect: false,
    evaluate: false,
    exit: false,
  }

  BACKTEST_MODES.forEach((mode) => {
    const result = results[mode]
    if (!result) {
      return
    }

    const completeness = completenessScore(mode, result)
    const samples = runSampleSize(mode, result)
    const runHealthy =
      result.success &&
      !result.runtime_error &&
      Array.isArray(result.validation_errors) &&
      result.validation_errors.length === 0
    const phaseScore = Math.round(completeness * 0.6 + (samples > 0 ? 100 : 0) * 0.2 + (runHealthy ? 100 : 0) * 0.2)

    phaseCompleteness[mode] = completeness
    phaseSamples[mode] = samples
    phaseScores[mode] = phaseScore
    phasePassed[mode] = runHealthy && samples > 0 && completeness >= 70
  })

  const passedPhases = BACKTEST_MODES.filter((mode) => phasePassed[mode]).length
  const readiness = Math.round(percent(passedPhases, BACKTEST_MODES.length))
  const averagePhaseScore = mean(BACKTEST_MODES.map((mode) => phaseScores[mode]))
  const overall = Math.round(averagePhaseScore * (readiness / 100))

  return {
    overall,
    readiness,
    passedPhases,
    phaseScores,
    phaseCompleteness,
    phaseSamples,
    phasePassed,
  }
}

/**
 * The full backtest suite UI body — extracted so it can be mounted both
 * as a flyout (Sheet) from the strategy editor and inline inside the
 * Research subview.
 */
export function BacktestSuitePanel({
  sourceCode,
  slug,
  config,
  variant,
}: {
  sourceCode: string
  slug: string
  config?: Record<string, any>
  variant: 'opportunity' | 'trader'
}) {
  const [phaseResults, setPhaseResults] = useState<Partial<Record<BacktestMode, BacktestResult>>>({})
  const [expandedRowId, setExpandedRowId] = useState<string | null>(null)
  const [runningPhase, setRunningPhase] = useState<BacktestMode | null>(null)
  const [runSettings, setRunSettings] = useState<BacktestRunSettings>(() => createDefaultBacktestSettings())
  const [executionSettings, setExecutionSettings] = useState<ExecutionRunSettings>(() => defaultExecutionSettings())
  const [executionResult, setExecutionResult] = useState<ExecutionBacktestResult | null>(null)
  const executionMutation = useMutation({
    mutationFn: async () => {
      const payload = buildExecutionPayload(sourceCode, slug, config, executionSettings)
      const { data } = await api.post('/validation/code-backtest/execution', payload)
      normalizeUtcTimestampsInPlace(data)
      return data as ExecutionBacktestResult
    },
    onSuccess: (data) => setExecutionResult(data),
    onError: () => setExecutionResult(null),
  })

  const backtestMutation = useMutation({
    mutationFn: async () => {
      const suite: Record<BacktestMode, BacktestResult> = {
        detect: createRequestFailureResult('detect', slug, 'Not run'),
        evaluate: createRequestFailureResult('evaluate', slug, 'Not run'),
        exit: createRequestFailureResult('exit', slug, 'Not run'),
      }

      for (const candidate of BACKTEST_MODES) {
        setRunningPhase(candidate)
        const payload = buildBacktestPayload(candidate, sourceCode, slug, config, runSettings)
        try {
          const { data } = await api.post(MODE_ENDPOINTS[candidate], payload)
          normalizeUtcTimestampsInPlace(data)
          suite[candidate] = data as BacktestResult
        } catch (error) {
          suite[candidate] = createRequestFailureResult(candidate, slug, error)
        }
      }

      return suite
    },
    onMutate: () => {
      setExpandedRowId(null)
      setPhaseResults({})
      setRunningPhase('detect')
    },
    onSuccess: (data) => {
      setExpandedRowId(null)
      setPhaseResults(data)
      setRunningPhase(null)
    },
    onError: () => {
      setExpandedRowId(null)
      setPhaseResults({})
      setRunningPhase(null)
    },
  })

  const integratedScore = useMemo(() => buildIntegratedScore(phaseResults), [phaseResults])
  const overallTone = scoreTone(integratedScore.overall)
  const hasSuiteResults = useMemo(
    () => BACKTEST_MODES.some((candidate) => Boolean(phaseResults[candidate])),
    [phaseResults]
  )
  const totalRuntimeMs = useMemo(
    () => BACKTEST_MODES.reduce((sum, candidate) => sum + toNumber(phaseResults[candidate]?.total_time_ms), 0),
    [phaseResults]
  )

  const phaseViews = useMemo(
    () =>
      BACKTEST_MODES.map((candidate) => {
        const phaseResult = phaseResults[candidate] || null
        const rows = buildOutcomeRows(candidate, phaseResult)
        const actionCounts = (() => {
          const counts = new Map<string, number>()
          rows.forEach((row) => {
            counts.set(row.action, (counts.get(row.action) || 0) + 1)
          })
          return Array.from(counts.entries()).sort((a, b) => b[1] - a[1])
        })()
        const dataCompleteness = phaseResult ? completenessScore(candidate, phaseResult) : 0
        const summaryCards = phaseResult ? buildSummaryCards(candidate, phaseResult, dataCompleteness) : []
        const phaseMs = phaseResult ? phaseDurationMs(candidate, phaseResult) : 0
        const sampleSize = phaseResult ? runSampleSize(candidate, phaseResult) : 0
        const hasWarnings = Boolean(phaseResult && phaseResult.validation_warnings.length > 0)
        return {
          mode: candidate,
          result: phaseResult,
          rows,
          actionCounts,
          dataCompleteness,
          summaryCards,
          phaseMs,
          sampleSize,
          hasWarnings,
        }
      }),
    [phaseResults]
  )

  const runningLabel = runningPhase ? MODE_LABELS[runningPhase].running : 'Running full suite...'

  return (
    <div className="h-full min-h-0 flex flex-col">
      {/* Compact sticky header: title + integrated score + run button.
          Everything else lives inside the ScrollArea below so the
          user can scroll Run Settings, Suite Results, and Execution
          Realism freely. */}
      <div className="border-b border-border px-4 py-2.5 shrink-0">
        <div className="flex items-center gap-3">
          <FlaskConical className="w-4 h-4 text-fuchsia-400 shrink-0" />
          <div className="min-w-0 flex-1">
            <h2 className="text-sm font-semibold leading-tight flex items-center gap-2 m-0">
              Strategy Backtest
              <Badge variant="outline" className="text-[9px] h-4 font-normal">Integrated</Badge>
            </h2>
                <p className="text-[10px] leading-tight mt-0.5 text-muted-foreground">
                  Detect · Evaluate · Exit suite + L2 execution replay
                </p>
              </div>
              {hasSuiteResults && (
                <div
                  className={cn(
                    'shrink-0 rounded-md border px-2 py-1 flex items-center gap-1.5',
                    overallTone === 'good'
                      ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-300'
                      : overallTone === 'warn'
                        ? 'border-amber-500/40 bg-amber-500/10 text-amber-300'
                        : 'border-red-500/40 bg-red-500/10 text-red-300'
                  )}
                >
                  <span className="text-[9px] uppercase tracking-wider opacity-70">Score</span>
                  <span className="text-base font-bold font-mono leading-none">
                    {fmt(integratedScore.overall)}
                  </span>
                  <span className="text-[9px] opacity-60">/100</span>
                </div>
              )}
              <Button
                size="sm"
                className="shrink-0 h-8 gap-1.5 px-3 text-xs bg-fuchsia-600 hover:bg-fuchsia-500 text-white"
                onClick={() => {
                  // Run both suites in parallel — Detect/Evaluate/Exit
                  // logic suite + L2 execution replay. They use different
                  // data planes so they don't interfere; running them
                  // together gives one unified set of results.
                  backtestMutation.mutate()
                  executionMutation.mutate()
                }}
                disabled={
                  (backtestMutation.isPending || executionMutation.isPending)
                  || !sourceCode.trim()
                }
              >
                {(backtestMutation.isPending || executionMutation.isPending) ? (
                  <>
                    <Loader2 className="w-3.5 h-3.5 animate-spin" />
                    Running...
                  </>
                ) : (
                  <>
                    <Play className="w-3.5 h-3.5" />
                    Run All Backtests
                  </>
                )}
              </Button>
            </div>
          </div>

          <ScrollArea className="flex-1 min-h-0 px-4 py-3">
            {/* Collapsible Run Settings — closed by default once a run completes,
                so results can take the majority of the height. */}
            <CollapsibleCard
              title="Run Settings"
              subtitle="Tune replay depth and per-phase sample caps before launching the suite."
              icon={Settings2}
              iconClass="text-cyan-400"
              defaultOpen={false}
              actions={
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  className="h-6 px-2 text-[10px]"
                  onClick={(e) => { e.stopPropagation(); setRunSettings(createDefaultBacktestSettings()) }}
                  disabled={backtestMutation.isPending}
                >
                  Reset defaults
                </Button>
              }
            >
              <div className="space-y-3 pt-1">
                <div className="hidden">{/* keep prior content semantics: */}</div>

              <div className="grid gap-3 xl:grid-cols-3">
                <div className="rounded-md border border-border/40 bg-background/40 p-3 space-y-3">
                  <div className="flex items-center gap-2">
                    <BarChart3 className="w-3.5 h-3.5 text-cyan-400" />
                    <div>
                      <p className="text-[11px] font-medium">Detect</p>
                      <p className="text-[10px] text-muted-foreground">Replay window and collection limits.</p>
                    </div>
                  </div>

                  <div className="rounded-md border border-border/30 bg-background/70 px-2.5 py-2">
                    <div className="flex items-center justify-between gap-3">
                      <div>
                        <Label htmlFor="backtest-use-ohlc-replay" className="text-[11px]">
                          Use OHLC replay
                        </Label>
                        <p className="mt-1 text-[10px] text-muted-foreground">
                          Replays historical snapshots only when live detect returns nothing.
                        </p>
                      </div>
                      <Switch
                        id="backtest-use-ohlc-replay"
                        checked={runSettings.detect.useOhlcReplay}
                        onCheckedChange={(checked) =>
                          setRunSettings((current) => ({
                            ...current,
                            detect: {
                              ...current.detect,
                              useOhlcReplay: checked,
                            },
                          }))
                        }
                        disabled={backtestMutation.isPending}
                      />
                    </div>
                  </div>

                  <div className="grid grid-cols-2 gap-2">
                    <div className="space-y-1">
                      <Label htmlFor="backtest-replay-lookback" className="text-[10px] uppercase tracking-wide text-muted-foreground">
                        Replay Hours
                      </Label>
                      <Input
                        id="backtest-replay-lookback"
                        type="number"
                        min={1}
                        max={720}
                        step={1}
                        className="h-8 text-xs font-mono"
                        value={runSettings.detect.replayLookbackHours}
                        onChange={(event) =>
                          setRunSettings((current) => ({
                            ...current,
                            detect: {
                              ...current.detect,
                              replayLookbackHours: event.target.value,
                            },
                          }))
                        }
                        disabled={backtestMutation.isPending || !runSettings.detect.useOhlcReplay}
                      />
                    </div>
                    <div className="space-y-1">
                      <Label className="text-[10px] uppercase tracking-wide text-muted-foreground">Replay Cadence</Label>
                      <Select
                        value={runSettings.detect.replayTimeframe}
                        onValueChange={(value) =>
                          setRunSettings((current) => ({
                            ...current,
                            detect: {
                              ...current.detect,
                              replayTimeframe: value,
                            },
                          }))
                        }
                        disabled={backtestMutation.isPending || !runSettings.detect.useOhlcReplay}
                      >
                        <SelectTrigger className="h-8 text-xs font-mono">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {REPLAY_TIMEFRAME_OPTIONS.map((timeframe) => (
                            <SelectItem key={timeframe} value={timeframe}>
                              {timeframe}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-1">
                      <Label htmlFor="backtest-replay-markets" className="text-[10px] uppercase tracking-wide text-muted-foreground">
                        Replay Markets
                      </Label>
                      <Input
                        id="backtest-replay-markets"
                        type="number"
                        min={1}
                        max={300}
                        step={1}
                        className="h-8 text-xs font-mono"
                        value={runSettings.detect.replayMaxMarkets}
                        onChange={(event) =>
                          setRunSettings((current) => ({
                            ...current,
                            detect: {
                              ...current.detect,
                              replayMaxMarkets: event.target.value,
                            },
                          }))
                        }
                        disabled={backtestMutation.isPending || !runSettings.detect.useOhlcReplay}
                      />
                    </div>
                    <div className="space-y-1">
                      <Label htmlFor="backtest-replay-steps" className="text-[10px] uppercase tracking-wide text-muted-foreground">
                        Replay Steps
                      </Label>
                      <Input
                        id="backtest-replay-steps"
                        type="number"
                        min={1}
                        max={500}
                        step={1}
                        className="h-8 text-xs font-mono"
                        value={runSettings.detect.replayMaxSteps}
                        onChange={(event) =>
                          setRunSettings((current) => ({
                            ...current,
                            detect: {
                              ...current.detect,
                              replayMaxSteps: event.target.value,
                            },
                          }))
                        }
                        disabled={backtestMutation.isPending || !runSettings.detect.useOhlcReplay}
                      />
                    </div>
                  </div>

                  <div className="space-y-1">
                    <Label htmlFor="backtest-max-opportunities" className="text-[10px] uppercase tracking-wide text-muted-foreground">
                      Max Opportunities Returned
                    </Label>
                    <Input
                      id="backtest-max-opportunities"
                      type="number"
                      min={1}
                      max={500}
                      step={1}
                      className="h-8 text-xs font-mono"
                      value={runSettings.detect.maxOpportunities}
                      onChange={(event) =>
                        setRunSettings((current) => ({
                          ...current,
                          detect: {
                            ...current.detect,
                            maxOpportunities: event.target.value,
                          },
                        }))
                      }
                      disabled={backtestMutation.isPending}
                    />
                  </div>
                </div>

                <div className="rounded-md border border-border/40 bg-background/40 p-3 space-y-3">
                  <div className="flex items-center gap-2">
                    <ShieldCheck className="w-3.5 h-3.5 text-emerald-400" />
                    <div>
                      <p className="text-[11px] font-medium">Evaluate</p>
                      <p className="text-[10px] text-muted-foreground">How many recent signals to score.</p>
                    </div>
                  </div>

                  <div className="space-y-1">
                    <Label htmlFor="backtest-max-signals" className="text-[10px] uppercase tracking-wide text-muted-foreground">
                      Max Signals
                    </Label>
                    <Input
                      id="backtest-max-signals"
                      type="number"
                      min={1}
                      max={500}
                      step={1}
                      className="h-8 text-xs font-mono"
                      value={runSettings.evaluate.maxSignals}
                      onChange={(event) =>
                        setRunSettings((current) => ({
                          ...current,
                          evaluate: {
                            ...current.evaluate,
                            maxSignals: event.target.value,
                          },
                        }))
                      }
                      disabled={backtestMutation.isPending}
                    />
                  </div>

                  <p className="text-[10px] text-muted-foreground">
                    The evaluator scores the newest qualifying signal emissions first, capped at this count.
                  </p>
                </div>

                <div className="rounded-md border border-border/40 bg-background/40 p-3 space-y-3">
                  <div className="flex items-center gap-2">
                    <Gauge className="w-3.5 h-3.5 text-amber-400" />
                    <div>
                      <p className="text-[11px] font-medium">Exit</p>
                      <p className="text-[10px] text-muted-foreground">How much open inventory to inspect.</p>
                    </div>
                  </div>

                  <div className="space-y-1">
                    <Label htmlFor="backtest-max-positions" className="text-[10px] uppercase tracking-wide text-muted-foreground">
                      Max Open Positions
                    </Label>
                    <Input
                      id="backtest-max-positions"
                      type="number"
                      min={1}
                      max={500}
                      step={1}
                      className="h-8 text-xs font-mono"
                      value={runSettings.exit.maxPositions}
                      onChange={(event) =>
                        setRunSettings((current) => ({
                          ...current,
                          exit: {
                            ...current.exit,
                            maxPositions: event.target.value,
                          },
                        }))
                      }
                      disabled={backtestMutation.isPending}
                    />
                  </div>

                  <p className="text-[10px] text-muted-foreground">
                    Exit backtests walk the most recent open positions first, up to this cap.
                  </p>
                </div>
              </div>
              </div>
            </CollapsibleCard>

            {/* Phase status strip — always visible, compact */}
            <div className="grid grid-cols-3 gap-2 mt-3">
              {BACKTEST_MODES.map((candidate) => {
                const passed = integratedScore.phasePassed[candidate]
                const score = integratedScore.phaseScores[candidate]
                const samples = integratedScore.phaseSamples[candidate]
                const data = integratedScore.phaseCompleteness[candidate]
                const isRunning = runningPhase === candidate && backtestMutation.isPending
                const tone: Tone = isRunning
                  ? 'neutral'
                  : !hasSuiteResults
                    ? 'neutral'
                    : passed
                      ? 'good'
                      : score > 30
                        ? 'warn'
                        : 'bad'
                const Icon = candidate === 'detect' ? BarChart3 : candidate === 'evaluate' ? ShieldCheck : Target
                return (
                  <div
                    key={candidate}
                    className={cn(
                      'rounded-md border p-2 space-y-1 transition-colors',
                      toneClasses(tone),
                      isRunning && 'ring-1 ring-cyan-400/50'
                    )}
                  >
                    <div className="flex items-center gap-1.5">
                      {isRunning ? (
                        <Loader2 className="w-3 h-3 animate-spin opacity-80" />
                      ) : (
                        <Icon className="w-3 h-3 opacity-80" />
                      )}
                      <span className="text-[10px] uppercase tracking-wider font-medium opacity-80">
                        {MODE_LABELS[candidate].label}
                      </span>
                      {hasSuiteResults && (
                        <span className="ml-auto text-[10px] font-mono font-semibold">
                          {fmt(score)}
                        </span>
                      )}
                    </div>
                    {hasSuiteResults ? (
                      <>
                        <div className="h-1 rounded bg-black/20 overflow-hidden">
                          <div
                            className={cn(
                              'h-full',
                              tone === 'good' ? 'bg-emerald-500/80' : tone === 'warn' ? 'bg-amber-500/80' : 'bg-red-500/80'
                            )}
                            style={{ width: `${Math.max(0, Math.min(100, score))}%` }}
                          />
                        </div>
                        <div className="flex items-center justify-between text-[9px] font-mono opacity-70">
                          <span>{fmt(samples)} samples</span>
                          <span>{fmt(data)}% data</span>
                        </div>
                      </>
                    ) : (
                      <p className="text-[10px] opacity-60 leading-tight line-clamp-2">
                        {isRunning ? MODE_LABELS[candidate].running : MODE_LABELS[candidate].desc}
                      </p>
                    )}
                  </div>
                )
              })}
            </div>

            <div className="mt-3">
            {backtestMutation.isError && (
              <div className="rounded-lg border border-red-500/30 bg-red-500/5 p-3 mb-3">
                <div className="flex items-center gap-2 text-red-400 text-xs font-medium mb-1">
                  <XCircle className="w-3.5 h-3.5" />
                  Request Failed
                </div>
                <p className="text-[11px] text-red-300/80">
                  Failed to run backtest. Check that the backend is running.
                </p>
              </div>
            )}

            {!hasSuiteResults && !backtestMutation.isPending && !backtestMutation.isError && (
              <div className="text-center py-12 space-y-2">
                <FlaskConical className="w-8 h-8 mx-auto text-muted-foreground/30" />
                <p className="text-xs text-muted-foreground">
                  Run a full backtest suite to benchmark this strategy across{' '}
                  {variant === 'opportunity'
                    ? 'live+replay detection, evaluation, and exits'
                    : 'signal gating and open-position exits'}
                  .
                </p>
                <p className="text-[10px] text-muted-foreground/60">
                  The score only passes when all three phases succeed with usable output.
                </p>
              </div>
            )}

            {backtestMutation.isPending && (
              <div className="text-center py-12 space-y-3">
                <Loader2 className="w-8 h-8 mx-auto text-primary animate-spin" />
                <p className="text-xs text-muted-foreground">{runningLabel}</p>
                <p className="text-[10px] text-muted-foreground/60">
                  Running Detect, Evaluate, and Exit in sequence.
                </p>
              </div>
            )}

            {hasSuiteResults && !backtestMutation.isPending && (
              <div className="space-y-3">
                <div
                  className={cn(
                    'rounded-lg border p-3 space-y-3',
                    overallTone === 'good'
                      ? 'border-emerald-500/30 bg-emerald-500/5'
                      : overallTone === 'warn'
                        ? 'border-amber-500/30 bg-amber-500/5'
                        : 'border-red-500/30 bg-red-500/5'
                  )}
                >
                  <div className="flex items-start gap-3">
                    <div
                      className={cn(
                        'shrink-0 rounded-lg border-2 px-3 py-2 flex flex-col items-center justify-center min-w-[80px]',
                        overallTone === 'good'
                          ? 'border-emerald-500/40 bg-emerald-500/10'
                          : overallTone === 'warn'
                            ? 'border-amber-500/40 bg-amber-500/10'
                            : 'border-red-500/40 bg-red-500/10'
                      )}
                    >
                      <span className="text-2xl font-bold font-mono leading-none">
                        {fmt(integratedScore.overall)}
                      </span>
                      <span className="text-[9px] uppercase tracking-wider opacity-70 mt-0.5">/ 100</span>
                    </div>
                    <div className="flex-1 min-w-0 space-y-1">
                      <div className="flex items-center gap-2 text-xs font-semibold">
                        {integratedScore.passedPhases === BACKTEST_MODES.length ? (
                          <>
                            <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400" />
                            <span className="text-emerald-300">All phases passing</span>
                          </>
                        ) : integratedScore.passedPhases > 0 ? (
                          <>
                            <AlertTriangle className="w-3.5 h-3.5 text-amber-400" />
                            <span className="text-amber-300">
                              {integratedScore.passedPhases} of {BACKTEST_MODES.length} phases passing
                            </span>
                          </>
                        ) : (
                          <>
                            <XCircle className="w-3.5 h-3.5 text-red-400" />
                            <span className="text-red-300">Suite failed</span>
                          </>
                        )}
                      </div>
                      <p className="text-[11px] text-muted-foreground leading-snug">
                        Combined fitness across detection, evaluation, and exit phases. Requires
                        successful run, non-zero samples, and ≥70% data completeness per phase.
                      </p>
                      <div className="h-1.5 rounded bg-black/20 overflow-hidden">
                        <div
                          className={cn(
                            'h-full transition-all duration-500',
                            integratedScore.overall >= 85
                              ? 'bg-emerald-500/80'
                              : integratedScore.overall >= 65
                                ? 'bg-amber-500/80'
                                : 'bg-red-500/80'
                          )}
                          style={{ width: `${Math.min(100, Math.max(0, integratedScore.overall))}%` }}
                        />
                      </div>
                    </div>
                  </div>

                  <div className="grid grid-cols-3 gap-2">
                    <div className={cn('rounded-md border p-2 space-y-0.5', toneClasses(integratedScore.readiness >= 80 ? 'good' : integratedScore.readiness >= 50 ? 'warn' : 'bad'))}>
                      <div className="flex items-center gap-1">
                        <Gauge className="w-3 h-3 opacity-70" />
                        <span className="text-[10px] uppercase tracking-wider opacity-70">Readiness</span>
                      </div>
                      <p className="text-base font-bold font-mono leading-tight">{fmt(integratedScore.readiness)}%</p>
                    </div>
                    <div className={cn('rounded-md border p-2 space-y-0.5', toneClasses(integratedScore.passedPhases === BACKTEST_MODES.length ? 'good' : integratedScore.passedPhases > 0 ? 'warn' : 'bad'))}>
                      <div className="flex items-center gap-1">
                        <CheckCircle2 className="w-3 h-3 opacity-70" />
                        <span className="text-[10px] uppercase tracking-wider opacity-70">Phase Passes</span>
                      </div>
                      <p className="text-base font-bold font-mono leading-tight">
                        {fmt(integratedScore.passedPhases)}<span className="text-xs opacity-60">/{fmt(BACKTEST_MODES.length)}</span>
                      </p>
                    </div>
                    <div className="rounded-md border border-border/40 bg-card/30 p-2 space-y-0.5">
                      <div className="flex items-center gap-1">
                        <Clock className="w-3 h-3 opacity-70" />
                        <span className="text-[10px] uppercase tracking-wider opacity-70">Runtime</span>
                      </div>
                      <p className="text-base font-bold font-mono leading-tight">{fmtMs(totalRuntimeMs)}</p>
                    </div>
                  </div>

                  <div className="grid grid-cols-3 gap-2">
                    {BACKTEST_MODES.map((candidate) => {
                      const passed = integratedScore.phasePassed[candidate]
                      const score = integratedScore.phaseScores[candidate]
                      const samples = integratedScore.phaseSamples[candidate]
                      const data = integratedScore.phaseCompleteness[candidate]
                      const PhaseIcon = candidate === 'detect' ? BarChart3 : candidate === 'evaluate' ? ShieldCheck : Target
                      const tone: Tone = passed ? 'good' : score > 30 ? 'warn' : 'bad'
                      return (
                        <div key={candidate} className={cn('rounded-md border p-2 space-y-1', toneClasses(tone))}>
                          <div className="flex items-center gap-1.5">
                            <PhaseIcon className="w-3 h-3 opacity-80" />
                            <span className="text-[10px] uppercase tracking-wider font-medium">
                              {MODE_LABELS[candidate].label}
                            </span>
                            <span className="ml-auto text-[11px] font-mono font-semibold">{fmt(score)}/100</span>
                          </div>
                          <div className="h-1 rounded bg-black/20 overflow-hidden">
                            <div
                              className={cn(
                                'h-full',
                                tone === 'good' ? 'bg-emerald-500/80' : tone === 'warn' ? 'bg-amber-500/80' : 'bg-red-500/80'
                              )}
                              style={{ width: `${Math.max(0, Math.min(100, score))}%` }}
                            />
                          </div>
                          <div className="flex items-center justify-between text-[9px] font-mono opacity-70">
                            <span>{fmt(samples)} samples</span>
                            <span>{fmt(data)}% data</span>
                          </div>
                        </div>
                      )
                    })}
                  </div>
                </div>

                {phaseViews.map((view) => (
                  <div
                    key={view.mode}
                    className={cn(
                      'rounded-lg border p-3 space-y-3',
                      view.result && view.result.success
                        ? 'border-border/40 bg-card/30'
                        : 'border-red-500/20 bg-red-500/5'
                    )}
                  >
                    <div className="flex items-center gap-2">
                      <Badge variant="outline" className="text-[9px] h-4 font-medium uppercase tracking-wide">
                        {phaseLabel(view.mode)}
                      </Badge>
                      <span className="text-[10px] text-muted-foreground">{MODE_LABELS[view.mode].desc}</span>
                    </div>

                    {view.result && (
                      <>
                        <div
                          className={cn(
                            'rounded-lg border p-3',
                            view.result.success
                              ? 'border-emerald-500/30 bg-emerald-500/5'
                              : 'border-red-500/30 bg-red-500/5'
                          )}
                        >
                          <div
                            className={cn(
                              'flex items-center gap-2 text-xs font-medium',
                              view.result.success ? 'text-emerald-400' : 'text-red-400'
                            )}
                          >
                            {view.result.success ? (
                              <CheckCircle2 className="w-3.5 h-3.5" />
                            ) : (
                              <XCircle className="w-3.5 h-3.5" />
                            )}
                            {view.result.success ? `${phaseLabel(view.mode)} completed` : `${phaseLabel(view.mode)} failed`}
                            <span className="ml-auto text-[10px] font-mono opacity-70">{fmtMs(view.result.total_time_ms)}</span>
                          </div>
                          <div className="mt-2 grid grid-cols-2 gap-2 text-[10px]">
                            <div className="rounded border border-border/30 bg-black/10 px-2 py-1.5">
                              <span className="text-muted-foreground">Sample Size</span>
                              <p className="font-mono font-medium">{fmt(view.sampleSize)}</p>
                            </div>
                            <div className="rounded border border-border/30 bg-black/10 px-2 py-1.5">
                              <span className="text-muted-foreground">Data Completeness</span>
                              <p className="font-mono font-medium">{fmt(view.dataCompleteness)}%</p>
                            </div>
                          </div>
                          <div className="mt-2 h-1.5 rounded bg-muted/40 overflow-hidden">
                            <div
                              className={cn(
                                'h-full',
                                view.dataCompleteness >= 90
                                  ? 'bg-emerald-500/80'
                                  : view.dataCompleteness >= 70
                                    ? 'bg-amber-500/80'
                                    : 'bg-red-500/80'
                              )}
                              style={{ width: `${Math.min(100, Math.max(0, view.dataCompleteness))}%` }}
                            />
                          </div>
                        </div>

                        <div className="grid grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-2">
                          {view.summaryCards.map((card) => (
                            <SummaryMetricCard key={`${view.mode}-${card.label}`} card={card} />
                          ))}
                        </div>

                        <div className="rounded-lg border border-border/30 p-3 space-y-1.5">
                          <div className="flex items-center gap-2 text-xs font-medium mb-1.5">
                            <Clock className="w-3.5 h-3.5 text-muted-foreground" />
                            Timing Breakdown
                          </div>
                          <div className="flex items-center justify-between text-[11px]">
                            <span className="text-muted-foreground">Code Load</span>
                            <span className="font-mono">{fmtMs(view.result.load_time_ms)}</span>
                          </div>
                          <div className="flex items-center justify-between text-[11px]">
                            <span className="text-muted-foreground">Data Fetch</span>
                            <span className="font-mono">{fmtMs(view.result.data_fetch_time_ms)}</span>
                          </div>
                          <div className="flex items-center justify-between text-[11px]">
                            <span className="text-muted-foreground">{phaseLabel(view.mode)}</span>
                            <span className="font-mono">{fmtMs(view.phaseMs)}</span>
                          </div>
                          <div className="flex items-center justify-between text-[11px] border-t border-border/20 pt-1.5 mt-1.5">
                            <span className="font-medium">Total</span>
                            <span className="font-mono font-medium">{fmtMs(view.result.total_time_ms)}</span>
                          </div>
                        </div>

                        <div className="rounded-lg border border-border/30 p-3 space-y-2">
                          <div className="flex items-center gap-2 text-xs font-medium">
                            <Database className="w-3.5 h-3.5 text-blue-400" />
                            Outcome Stream
                            <span className="ml-auto text-[10px] text-muted-foreground font-mono">{fmt(view.rows.length)} rows</span>
                          </div>

                          {view.actionCounts.length > 0 && (
                            <div className="flex flex-wrap gap-1.5">
                              {view.actionCounts.map(([action, count]) => (
                                <Badge key={`${view.mode}-${action}`} variant="outline" className="text-[9px] h-4 font-mono">
                                  {action}: {fmt(count)}
                                </Badge>
                              ))}
                            </div>
                          )}

                          {view.rows.length === 0 ? (
                            <div className="text-center py-6 space-y-2">
                              <Target className="w-6 h-6 mx-auto text-muted-foreground/30" />
                              <p className="text-xs text-muted-foreground">No outcomes produced for this run.</p>
                            </div>
                          ) : (
                            <div className="space-y-1.5">
                              {view.rows.map((row) => {
                                const rowKey = `${view.mode}:${row.id}`
                                return (
                                  <OutcomeCard
                                    key={rowKey}
                                    row={row}
                                    expanded={expandedRowId === rowKey}
                                    onToggle={() => setExpandedRowId(expandedRowId === rowKey ? null : rowKey)}
                                  />
                                )
                              })}
                            </div>
                          )}
                        </div>

                        {(view.result.validation_errors.length > 0 || view.hasWarnings || view.result.runtime_error) && (
                          <div className="rounded-lg border border-border/30 p-3 space-y-2">
                            <div className="flex items-center gap-2 text-xs font-medium">
                              <AlertTriangle className="w-3.5 h-3.5 text-amber-400" />
                              Diagnostics
                            </div>

                            {view.result.validation_errors.length > 0 && (
                              <div className="rounded border border-red-500/30 bg-red-500/5 p-2 space-y-1">
                                <p className="text-[10px] uppercase tracking-wider text-red-400 font-medium">
                                  Validation Errors
                                </p>
                                {view.result.validation_errors.map((error, index) => (
                                  <p key={index} className="text-[11px] text-red-300/80 font-mono">
                                    {error}
                                  </p>
                                ))}
                              </div>
                            )}

                            {view.hasWarnings && (
                              <div className="rounded border border-amber-500/30 bg-amber-500/5 p-2 space-y-1">
                                <p className="text-[10px] uppercase tracking-wider text-amber-400 font-medium">Warnings</p>
                                {view.result.validation_warnings.map((warning, index) => (
                                  <p key={index} className="text-[11px] text-amber-200/80">
                                    {warning}
                                  </p>
                                ))}
                              </div>
                            )}

                            {view.result.runtime_error && (
                              <div className="rounded border border-red-500/30 bg-red-500/5 p-2 space-y-1">
                                <p className="text-[10px] uppercase tracking-wider text-red-400 font-medium">
                                  Runtime Error
                                </p>
                                <p className="text-[11px] text-red-300/80 font-mono">{view.result.runtime_error}</p>
                                {view.result.runtime_traceback && (
                                  <pre className="text-[10px] text-red-300/60 font-mono bg-black/20 rounded p-2 overflow-x-auto whitespace-pre max-h-48 overflow-y-auto">
                                    {view.result.runtime_traceback}
                                  </pre>
                                )}
                              </div>
                            )}
                          </div>
                        )}
                      </>
                    )}
                  </div>
                ))}
              </div>
            )}

            </div>

            <ExecutionRealismSection
              isPending={executionMutation.isPending}
              result={executionResult}
              settings={executionSettings}
              onChange={setExecutionSettings}
              onRun={() => executionMutation.mutate()}
              onReset={() => setExecutionSettings(defaultExecutionSettings())}
              error={executionMutation.error as Error | null}
            />
      </ScrollArea>
    </div>
  )
}

// ────────────────────────────────────────────────────────────────────────
// Default flyout export — thin Sheet wrapper around BacktestSuitePanel.
// ────────────────────────────────────────────────────────────────────────

export default function StrategyBacktestFlyout({
  open,
  onOpenChange,
  sourceCode,
  slug,
  config,
  variant,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  sourceCode: string
  slug: string
  config?: Record<string, any>
  variant: 'opportunity' | 'trader'
}) {
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side="right" className="w-[98vw] sm:max-w-4xl xl:max-w-6xl p-0">
        {/* Required by Radix for a11y — visually hidden so the inline
            BacktestSuitePanel can render its own header. */}
        <SheetTitle className="sr-only">Strategy Backtest</SheetTitle>
        <BacktestSuitePanel
          sourceCode={sourceCode}
          slug={slug}
          config={config}
          variant={variant}
        />
      </SheetContent>
    </Sheet>
  )
}

// ────────────────────────────────────────────────────────────────────────
// Execution Realism — drives services.backtest engine
// ────────────────────────────────────────────────────────────────────────

function ExecutionRealismSection({
  isPending,
  result,
  settings,
  onChange,
  onReset,
  error,
}: {
  isPending: boolean
  result: ExecutionBacktestResult | null
  settings: ExecutionRunSettings
  onChange: (next: ExecutionRunSettings) => void
  // ``onRun`` was removed when the master Run All button at the top of
  // the flyout took over driving this section's mutation.
  onRun?: () => void
  onReset: () => void
  error: Error | null
}) {
  const updateField = (key: keyof ExecutionRunSettings, value: string) =>
    onChange({ ...settings, [key]: value })

  const headlineCards: Array<{
    label: string
    value: string
    band: string | null
    tone: Tone
    icon: ComponentType<{ className?: string }>
    sub?: string
  }> = result
    ? [
        {
          label: 'Total Return',
          value: fmtSignedPct(result.total_return_pct),
          band: null,
          tone: ciToneFromValue(result.total_return_pct),
          icon: TrendingUp,
          sub: `${fmtMoney(result.final_equity_usd)} final equity`,
        },
        {
          label: 'Sharpe (annualized)',
          value: fmtCi(result.sharpe),
          band: fmtCiBand(result.sharpe),
          tone: ciToneFromValue(result.sharpe.value, 1.0),
          icon: Sparkles,
          sub: 'bootstrap 95% CI',
        },
        {
          label: 'Max Drawdown',
          value: `${fmt(result.max_drawdown_pct, 2)}%`,
          band: null,
          tone: result.max_drawdown_pct > 20 ? 'bad' : result.max_drawdown_pct > 10 ? 'warn' : 'neutral',
          icon: TrendingDown,
          sub: `${fmtMoney(result.max_drawdown_usd)} dd`,
        },
        {
          label: 'Hit Rate',
          value: `${fmt(result.hit_rate.value * 100, 1)}%`,
          band: result.hit_rate.ci_low !== null && result.hit_rate.ci_high !== null
            ? `[${fmt(result.hit_rate.ci_low * 100, 1)}%, ${fmt(result.hit_rate.ci_high * 100, 1)}%]`
            : null,
          tone: ciToneFromValue(result.hit_rate.value, 0.5),
          icon: Target,
          sub: `${fmt(result.trade_count)} trades`,
        },
      ]
    : []

  const equityPath = useMemo(() => {
    if (!result || result.equity_curve_sample.length < 2) return null
    const points = result.equity_curve_sample.map((p) => p.equity_usd)
    const minV = Math.min(...points)
    const maxV = Math.max(...points)
    const range = Math.max(1e-9, maxV - minV)
    const w = 600
    const h = 80
    const stepX = points.length > 1 ? w / (points.length - 1) : 0
    const path = points
      .map((v, i) => {
        const x = i * stepX
        const y = h - ((v - minV) / range) * (h - 4) - 2
        return `${i === 0 ? 'M' : 'L'} ${x.toFixed(1)} ${y.toFixed(1)}`
      })
      .join(' ')
    return { path, w, h, minV, maxV }
  }, [result])

  return (
    <div className="px-4 py-3">
      <div className="rounded-lg border border-fuchsia-500/30 bg-fuchsia-500/5 p-3 space-y-3">
        {/* Slim section header — the master "Run All Backtests" button at
            the top of the flyout drives this section's mutation. We keep
            a small Reset button local to this card. */}
        <div className="flex items-center gap-2">
          <Zap className="w-3.5 h-3.5 text-fuchsia-400 shrink-0" />
          <h3 className="text-[11px] font-semibold">Execution Realism</h3>
          <Badge variant="outline" className="text-[9px] h-4 font-normal text-fuchsia-300 border-fuchsia-500/40">
            L2 Replay · bootstrap CI
          </Badge>
          {isPending && <Loader2 className="w-3 h-3 animate-spin text-fuchsia-300" />}
          <Button
            type="button"
            variant="ghost"
            size="sm"
            className="ml-auto h-6 px-2 text-[10px]"
            onClick={onReset}
            disabled={isPending}
          >
            Reset settings
          </Button>
        </div>

        {/* Compact settings grid — same row as the title to keep it tight. */}
        <div className="grid grid-cols-3 md:grid-cols-4 lg:grid-cols-8 gap-2">
          <SettingsField label="Lookback (h)" value={settings.lookbackHours} onChange={(v) => updateField('lookbackHours', v)} />
          <SettingsField label="Capital ($)" value={settings.initialCapitalUsd} onChange={(v) => updateField('initialCapitalUsd', v)} />
          <SettingsField label="Submit p50 ms" value={settings.submitP50Ms} onChange={(v) => updateField('submitP50Ms', v)} />
          <SettingsField label="Submit p95 ms" value={settings.submitP95Ms} onChange={(v) => updateField('submitP95Ms', v)} />
          <SettingsField label="Cancel p50 ms" value={settings.cancelP50Ms} onChange={(v) => updateField('cancelP50Ms', v)} />
          <SettingsField label="Cancel p95 ms" value={settings.cancelP95Ms} onChange={(v) => updateField('cancelP95Ms', v)} />
          <SettingsField label="Seed" value={settings.seed} onChange={(v) => updateField('seed', v)} />
          <SettingsField label="Token IDs (csv)" value={settings.tokenIds} onChange={(v) => updateField('tokenIds', v)} />
        </div>

        {error && (
          <div className="rounded border border-red-500/30 bg-red-500/5 p-2 text-[11px] text-red-300">
            <AlertTriangle className="inline w-3.5 h-3.5 mr-1 align-text-bottom" />
            {error.message || 'Execution backtest failed.'}
          </div>
        )}

        {result && !result.success && (result.runtime_error || result.validation_errors.length > 0) && (
          <div className="rounded border border-red-500/30 bg-red-500/5 p-2 space-y-1">
            <p className="text-[10px] uppercase tracking-wider text-red-400 font-medium">Backtest failed</p>
            {result.runtime_error && (
              <p className="text-[11px] text-red-300/80 font-mono">{result.runtime_error}</p>
            )}
            {result.validation_errors.map((e, i) => (
              <p key={i} className="text-[11px] text-red-300/80 font-mono">{e}</p>
            ))}
          </div>
        )}

        {result && result.success && (
          <div className="space-y-3">
            {/* Headline metric cards */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
              {headlineCards.map((c) => {
                const Icon = c.icon
                return (
                  <div
                    key={c.label}
                    className={cn('rounded-md border p-2.5 space-y-1', toneClasses(c.tone))}
                  >
                    <div className="flex items-center justify-between">
                      <span className="text-[10px] uppercase tracking-wider opacity-70">{c.label}</span>
                      <Icon className="w-3.5 h-3.5 opacity-70" />
                    </div>
                    <p className="text-base font-bold font-mono leading-tight">{c.value}</p>
                    {c.band && (
                      <p className="text-[10px] font-mono opacity-60">{c.band}</p>
                    )}
                    {c.sub && <p className="text-[10px] opacity-60">{c.sub}</p>}
                  </div>
                )
              })}
            </div>

            {/* Risk-adjusted second row */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
              <RiskCard label="Sortino" metric={result.sortino} target={1.0} />
              <RiskCard label="Calmar" metric={result.calmar} target={1.0} />
              <RiskCard label="Profit Factor" metric={result.profit_factor} target={1.0} />
              <RiskCard label="Expectancy" metric={result.expectancy_usd} target={0} prefix="$" />
            </div>

            {/* Equity curve */}
            {equityPath && (
              <div className="rounded-md border border-border/40 bg-card/30 p-3">
                <div className="flex items-center justify-between mb-1.5">
                  <div className="flex items-center gap-2">
                    <LineChart className="w-3.5 h-3.5 text-cyan-400" />
                    <span className="text-[11px] font-medium">Equity Curve</span>
                  </div>
                  <span className="text-[10px] text-muted-foreground font-mono">
                    {fmtMoney(equityPath.minV)} → {fmtMoney(equityPath.maxV)}
                  </span>
                </div>
                <svg viewBox={`0 0 ${equityPath.w} ${equityPath.h}`} className="w-full h-20">
                  <path d={equityPath.path} fill="none" stroke="rgb(34 211 238)" strokeWidth="1.5" />
                </svg>
                <p className="text-[10px] text-muted-foreground mt-1">
                  {result.equity_curve_sample.length} samples · annualized {fmtSignedPct(result.annualized_return_pct)}
                </p>
              </div>
            )}

            {/* Execution detail */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-2 text-[11px]">
              <DetailStat label="Snapshots" value={fmt(result.n_snapshots)} icon={Database} />
              <DetailStat label="Intents" value={fmt(result.n_intents)} icon={Activity} />
              <DetailStat label="Fills" value={fmt(result.total_fills)} icon={Zap} />
              <DetailStat label="Rejected" value={fmt(result.rejected_orders)} icon={XCircle} tone={result.rejected_orders > 0 ? 'warn' : 'neutral'} />
              <DetailStat label="Cancelled" value={fmt(result.cancelled_orders)} icon={Clock} />
              <DetailStat label="Closed Pos" value={fmt(result.closed_position_count)} icon={CheckCircle2} />
              <DetailStat label="Open Pos" value={fmt(result.open_position_count)} icon={ShieldCheck} />
              <DetailStat label="Total Fees" value={fmtMoney(result.fees_paid_usd)} icon={Gauge} />
            </div>

            {/* Fee breakdown */}
            <div className="rounded-md border border-border/40 bg-card/30 p-3 space-y-1.5">
              <div className="flex items-center gap-2">
                <Gauge className="w-3.5 h-3.5 text-amber-400" />
                <span className="text-[11px] font-medium">Fee Breakdown</span>
                <span className="ml-auto text-[10px] text-muted-foreground font-mono">
                  {result.initial_capital_usd > 0
                    ? `${fmt((result.fees_paid_usd / result.initial_capital_usd) * 100, 2)}% of capital`
                    : ''}
                </span>
              </div>
              <div className="grid grid-cols-2 gap-2">
                <div className="rounded border border-border/30 bg-background/40 p-2">
                  <div className="flex items-center justify-between mb-0.5">
                    <span className="text-[10px] uppercase tracking-wider text-muted-foreground">Per-fill (gas + bps)</span>
                    <Zap className="w-3 h-3 text-cyan-400/70" />
                  </div>
                  <p className="text-sm font-mono font-semibold">{fmtMoney(result.fees_per_fill_usd)}</p>
                  <p className="text-[10px] text-muted-foreground">
                    {result.total_fills > 0
                      ? `${fmtMoney(result.fees_per_fill_usd / result.total_fills)} avg per fill`
                      : '—'}
                  </p>
                </div>
                <div className="rounded border border-border/30 bg-background/40 p-2">
                  <div className="flex items-center justify-between mb-0.5">
                    <span className="text-[10px] uppercase tracking-wider text-muted-foreground">Resolution (2% on wins)</span>
                    <Target className="w-3 h-3 text-emerald-400/70" />
                  </div>
                  <p className="text-sm font-mono font-semibold">{fmtMoney(result.fees_resolution_usd)}</p>
                  <p className="text-[10px] text-muted-foreground">
                    Polymarket charges 2% on winning proceeds at settlement
                  </p>
                </div>
              </div>
            </div>

            {/* Correlation matrix (if any) */}
            {result.correlation_pairs.length > 0 && (
              <div className="rounded-md border border-border/40 bg-card/30 p-3 space-y-1.5">
                <div className="flex items-center gap-2">
                  <BarChart3 className="w-3.5 h-3.5 text-violet-400" />
                  <span className="text-[11px] font-medium">Realized Correlations</span>
                </div>
                <div className="grid grid-cols-2 gap-1.5">
                  {result.correlation_pairs.slice(0, 12).map((p, i) => (
                    <div key={i} className="flex items-center justify-between text-[10px] font-mono px-2 py-1 rounded bg-background/60">
                      <span className="truncate text-muted-foreground">
                        {p.token_a.slice(0, 8)}↔{p.token_b.slice(0, 8)}
                      </span>
                      <span
                        className={cn(
                          'font-bold',
                          p.correlation > 0.5 ? 'text-emerald-400' : p.correlation < -0.5 ? 'text-red-400' : 'text-muted-foreground'
                        )}
                      >
                        {p.correlation >= 0 ? '+' : ''}
                        {fmt(p.correlation, 2)}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Fills sample table */}
            {result.fills_sample.length > 0 && (
              <div className="rounded-md border border-border/40 bg-card/30 p-3 space-y-1.5">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <Activity className="w-3.5 h-3.5 text-emerald-400" />
                    <span className="text-[11px] font-medium">Fills Sample</span>
                  </div>
                  <span className="text-[10px] text-muted-foreground font-mono">
                    {fmt(result.fills_sample.length)} of {fmt(result.total_fills)}
                  </span>
                </div>
                <div className="max-h-48 overflow-y-auto">
                  <table className="w-full text-[10px] font-mono">
                    <thead className="sticky top-0 bg-card/95">
                      <tr className="border-b border-border/30 text-muted-foreground">
                        <th className="text-left py-1 px-1.5">Time</th>
                        <th className="text-left py-1 px-1.5">Side</th>
                        <th className="text-right py-1 px-1.5">Size</th>
                        <th className="text-right py-1 px-1.5">Price</th>
                        <th className="text-left py-1 px-1.5">Type</th>
                      </tr>
                    </thead>
                    <tbody>
                      {result.fills_sample.slice(0, 100).map((f, i) => (
                        <tr key={i} className="border-b border-border/10 hover:bg-card/60">
                          <td className="py-0.5 px-1.5 text-muted-foreground">
                            {f.occurred_at.split('T')[1]?.slice(0, 8) || '—'}
                          </td>
                          <td
                            className={cn(
                              'py-0.5 px-1.5 font-medium',
                              f.side === 'BUY' ? 'text-emerald-400' : 'text-red-400'
                            )}
                          >
                            {f.side}
                          </td>
                          <td className="text-right py-0.5 px-1.5">{fmt(f.size, 2)}</td>
                          <td className="text-right py-0.5 px-1.5">{fmt(f.price, 4)}</td>
                          <td className="py-0.5 px-1.5 text-muted-foreground">
                            {f.is_maker ? 'maker' : 'taker'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Timing footer */}
            <div className="flex items-center justify-between text-[10px] text-muted-foreground font-mono pt-1 border-t border-border/20">
              <span>
                {result.start_iso.slice(0, 19)} → {result.end_iso.slice(0, 19)}
              </span>
              <span>
                load {fmtMs(result.load_time_ms)} · data {fmtMs(result.data_fetch_time_ms)} · run{' '}
                {fmtMs(result.run_time_ms)} · total {fmtMs(result.total_time_ms)}
              </span>
            </div>

            {result.validation_warnings.length > 0 && (
              <div className="rounded border border-amber-500/30 bg-amber-500/5 p-2 space-y-1">
                <p className="text-[10px] uppercase tracking-wider text-amber-400 font-medium">Warnings</p>
                {result.validation_warnings.map((w, i) => (
                  <p key={i} className="text-[11px] text-amber-200/80">{w}</p>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

function SettingsField({
  label,
  value,
  onChange,
}: {
  label: string
  value: string
  onChange: (v: string) => void
}) {
  return (
    <div className="space-y-1">
      <Label className="text-[10px] text-muted-foreground">{label}</Label>
      <Input
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="h-7 text-[11px] font-mono"
      />
    </div>
  )
}

function RiskCard({
  label,
  metric,
  target,
  prefix,
}: {
  label: string
  metric: MetricCI
  target: number
  prefix?: string
}) {
  const tone = ciToneFromValue(metric.value, target)
  const band = fmtCiBand(metric, 2)
  return (
    <div className={cn('rounded-md border p-2.5 space-y-0.5', toneClasses(tone))}>
      <span className="text-[10px] uppercase tracking-wider opacity-70">{label}</span>
      <p className="text-sm font-bold font-mono leading-tight">
        {prefix || ''}
        {fmtCi(metric, 2)}
      </p>
      {band && <p className="text-[10px] font-mono opacity-60">{band}</p>}
    </div>
  )
}

function DetailStat({
  label,
  value,
  icon: Icon,
  tone = 'neutral',
}: {
  label: string
  value: string
  icon: ComponentType<{ className?: string }>
  tone?: Tone
}) {
  return (
    <div className={cn('rounded-md border p-2 flex items-center gap-2', toneClasses(tone))}>
      <Icon className="w-3.5 h-3.5 opacity-70 shrink-0" />
      <div className="min-w-0 flex-1">
        <p className="text-[10px] uppercase tracking-wider opacity-70 leading-none mb-1">{label}</p>
        <p className="text-[12px] font-mono font-semibold leading-none">{value}</p>
      </div>
    </div>
  )
}
