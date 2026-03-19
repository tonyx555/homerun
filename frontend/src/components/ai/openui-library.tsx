/**
 * OpenUI component library for generative AI tool results.
 *
 * When the chat agent calls tools (resolution analysis, sentiment, market data, etc.),
 * the LLM can emit OpenUI Lang markup that renders as rich interactive cards
 * inside chat messages — tables, score gauges, sentiment bars, market cards, etc.
 *
 * Usage:
 *   import { homerunLibrary } from './openui-library'
 *   <Renderer library={homerunLibrary} text={openUILangText} streaming={isStreaming} />
 */

import { defineComponent, createLibrary, type ComponentRenderProps } from '@openuidev/react-lang'
import { z } from 'zod'
import { cn } from '../../lib/utils'

// ---------------------------------------------------------------------------
// Stack — root layout container
// ---------------------------------------------------------------------------
const Stack = defineComponent({
  name: 'Stack',
  description: 'Vertical stack of child components with gap spacing',
  props: z.object({
    gap: z.string().optional().describe('Tailwind gap class, e.g. "gap-3"'),
    children: z.array(z.any()),
  }),
  component: ({ props, renderNode }: ComponentRenderProps<{ gap?: string; children: unknown[] }>) => (
    <div className={cn('flex flex-col', props.gap || 'gap-3')}>
      {props.children.map((child, i) => <div key={i}>{renderNode(child)}</div>)}
    </div>
  ),
})

// ---------------------------------------------------------------------------
// Card — generic card container
// ---------------------------------------------------------------------------
const Card = defineComponent({
  name: 'Card',
  description: 'A card container with optional title, subtitle, and accent color',
  props: z.object({
    title: z.string(),
    subtitle: z.string().optional(),
    accent: z.string().optional().describe('Color: purple, cyan, emerald, amber, red'),
    children: z.array(z.any()),
  }),
  component: ({ props, renderNode }: ComponentRenderProps<{ title: string; subtitle?: string; accent?: string; children: unknown[] }>) => {
    const accentColors: Record<string, string> = {
      purple: 'border-purple-500/30 bg-purple-500/5',
      cyan: 'border-cyan-500/30 bg-cyan-500/5',
      emerald: 'border-emerald-500/30 bg-emerald-500/5',
      amber: 'border-amber-500/30 bg-amber-500/5',
      red: 'border-red-500/30 bg-red-500/5',
    }
    const cls = accentColors[props.accent || 'purple'] || accentColors.purple
    return (
      <div className={cn('rounded-lg border p-4', cls)}>
        <div className="mb-2">
          <h3 className="text-sm font-semibold text-white">{props.title}</h3>
          {props.subtitle && <p className="text-xs text-white/50">{props.subtitle}</p>}
        </div>
        <div className="flex flex-col gap-2">
          {props.children.map((child, i) => <div key={i}>{renderNode(child)}</div>)}
        </div>
      </div>
    )
  },
})

// ---------------------------------------------------------------------------
// ScoreGauge — circular or bar score indicator
// ---------------------------------------------------------------------------
const ScoreGauge = defineComponent({
  name: 'ScoreGauge',
  description: 'A score indicator showing a value 0-1 with label and color coding',
  props: z.object({
    label: z.string(),
    value: z.number().describe('Score from 0 to 1'),
    format: z.string().optional().describe('"percent" or "decimal"'),
  }),
  component: ({ props }: ComponentRenderProps<{ label: string; value: number; format?: string }>) => {
    const pct = Math.round(props.value * 100)
    const color =
      props.value >= 0.7 ? 'bg-emerald-500' : props.value >= 0.4 ? 'bg-amber-500' : 'bg-red-500'
    const textColor =
      props.value >= 0.7 ? 'text-emerald-400' : props.value >= 0.4 ? 'text-amber-400' : 'text-red-400'
    const display = props.format === 'decimal' ? props.value.toFixed(2) : `${pct}%`

    return (
      <div className="flex items-center gap-3">
        <div className="flex-1">
          <div className="flex items-center justify-between mb-1">
            <span className="text-xs text-white/60">{props.label}</span>
            <span className={cn('text-xs font-mono font-bold', textColor)}>{display}</span>
          </div>
          <div className="h-1.5 rounded-full bg-white/10 overflow-hidden">
            <div
              className={cn('h-full rounded-full transition-all', color)}
              style={{ width: `${pct}%` }}
            />
          </div>
        </div>
      </div>
    )
  },
})

// ---------------------------------------------------------------------------
// Badge — inline status badge
// ---------------------------------------------------------------------------
const Badge = defineComponent({
  name: 'Badge',
  description: 'Inline badge/tag with text and color variant',
  props: z.object({
    text: z.string(),
    variant: z.string().optional().describe('"success", "warning", "danger", "info", "neutral"'),
  }),
  component: ({ props }: ComponentRenderProps<{ text: string; variant?: string }>) => {
    const variants: Record<string, string> = {
      success: 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30',
      warning: 'bg-amber-500/20 text-amber-400 border-amber-500/30',
      danger: 'bg-red-500/20 text-red-400 border-red-500/30',
      info: 'bg-cyan-500/20 text-cyan-400 border-cyan-500/30',
      neutral: 'bg-white/10 text-white/60 border-white/20',
    }
    const cls = variants[props.variant || 'neutral'] || variants.neutral
    return (
      <span className={cn('inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border', cls)}>
        {props.text}
      </span>
    )
  },
})

// ---------------------------------------------------------------------------
// KeyValue — label:value pair row
// ---------------------------------------------------------------------------
const KeyValue = defineComponent({
  name: 'KeyValue',
  description: 'A key-value display row with label and value',
  props: z.object({
    label: z.string(),
    value: z.string(),
    mono: z.boolean().optional().describe('Use monospace font for value'),
  }),
  component: ({ props }: ComponentRenderProps<{ label: string; value: string; mono?: boolean }>) => (
    <div className="flex items-center justify-between py-1">
      <span className="text-xs text-white/50">{props.label}</span>
      <span className={cn('text-xs text-white/90', props.mono && 'font-mono')}>{props.value}</span>
    </div>
  ),
})

// ---------------------------------------------------------------------------
// Table — data table with headers and rows
// ---------------------------------------------------------------------------
const DataTable = defineComponent({
  name: 'DataTable',
  description: 'Data table with column headers and rows of values',
  props: z.object({
    headers: z.array(z.string()),
    rows: z.array(z.array(z.string())),
  }),
  component: ({ props }: ComponentRenderProps<{ headers: string[]; rows: string[][] }>) => (
    <div className="overflow-x-auto rounded border border-white/10">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-white/10 bg-white/5">
            {props.headers.map((h: string, i: number) => (
              <th key={i} className="px-3 py-1.5 text-left font-medium text-white/60">{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {props.rows.map((row: string[], ri: number) => (
            <tr key={ri} className="border-b border-white/5 last:border-0">
              {row.map((cell: string, ci: number) => (
                <td key={ci} className="px-3 py-1.5 text-white/80">{cell}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  ),
})

// ---------------------------------------------------------------------------
// SentimentBar — sentiment visualization from -1 to +1
// ---------------------------------------------------------------------------
const SentimentBar = defineComponent({
  name: 'SentimentBar',
  description: 'Sentiment visualization bar from -1 (bearish) to +1 (bullish)',
  props: z.object({
    label: z.string(),
    score: z.number().describe('Sentiment score from -1 to 1'),
    confidence: z.number().optional().describe('Confidence 0-1'),
  }),
  component: ({ props }: ComponentRenderProps<{ label: string; score: number; confidence?: number }>) => {
    const pct = ((props.score + 1) / 2) * 100
    const sentimentLabel = props.score > 0.3 ? 'Bullish' : props.score < -0.3 ? 'Bearish' : 'Neutral'
    const sentimentColor =
      props.score > 0.3 ? 'text-emerald-400' : props.score < -0.3 ? 'text-red-400' : 'text-amber-400'

    return (
      <div>
        <div className="flex items-center justify-between mb-1">
          <span className="text-xs text-white/60">{props.label}</span>
          <span className={cn('text-xs font-bold', sentimentColor)}>
            {sentimentLabel} ({props.score > 0 ? '+' : ''}{props.score.toFixed(2)})
          </span>
        </div>
        <div className="relative h-2 rounded-full bg-white/10 overflow-hidden">
          <div className="absolute inset-0 flex">
            <div className="w-1/2 bg-red-500/20" />
            <div className="w-1/2 bg-emerald-500/20" />
          </div>
          <div
            className="absolute top-0 h-full w-1 bg-white rounded-full"
            style={{ left: `${pct}%`, transform: 'translateX(-50%)' }}
          />
        </div>
        {props.confidence !== undefined && (
          <p className="text-[10px] text-white/40 mt-0.5">
            Confidence: {Math.round(props.confidence * 100)}%
          </p>
        )}
      </div>
    )
  },
})

// ---------------------------------------------------------------------------
// MarketCard — prediction market summary
// ---------------------------------------------------------------------------
const MarketCard = defineComponent({
  name: 'MarketCard',
  description: 'Prediction market summary card with question, prices, volume',
  props: z.object({
    question: z.string(),
    yesPrice: z.number(),
    noPrice: z.number(),
    volume: z.string().optional(),
    liquidity: z.string().optional(),
    platform: z.string().optional(),
  }),
  component: ({ props }: ComponentRenderProps<{ question: string; yesPrice: number; noPrice: number; volume?: string; liquidity?: string; platform?: string }>) => (
    <div className="rounded-lg border border-purple-500/20 bg-purple-500/5 p-3">
      <p className="text-xs font-medium text-white mb-2">{props.question}</p>
      <div className="flex gap-4 mb-2">
        <div className="flex items-center gap-1.5">
          <div className="w-2 h-2 rounded-full bg-emerald-500" />
          <span className="text-xs text-white/60">YES</span>
          <span className="text-xs font-mono font-bold text-emerald-400">
            {(props.yesPrice * 100).toFixed(0)}¢
          </span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-2 h-2 rounded-full bg-red-500" />
          <span className="text-xs text-white/60">NO</span>
          <span className="text-xs font-mono font-bold text-red-400">
            {(props.noPrice * 100).toFixed(0)}¢
          </span>
        </div>
      </div>
      <div className="flex gap-3 text-[10px] text-white/40">
        {props.volume && <span>Vol: {props.volume}</span>}
        {props.liquidity && <span>Liq: {props.liquidity}</span>}
        {props.platform && <span>{props.platform}</span>}
      </div>
    </div>
  ),
})

// ---------------------------------------------------------------------------
// Recommendation — action recommendation with reasoning
// ---------------------------------------------------------------------------
const Recommendation = defineComponent({
  name: 'Recommendation',
  description: 'Action recommendation block with verdict and reasoning',
  props: z.object({
    verdict: z.string().describe('"strong_execute", "execute", "review", "skip", "strong_skip", "safe", "caution", "avoid"'),
    reasoning: z.string(),
  }),
  component: ({ props }: ComponentRenderProps<{ verdict: string; reasoning: string }>) => {
    const v = props.verdict.toLowerCase()
    const colors: Record<string, { bg: string; border: string; text: string; icon: string }> = {
      strong_execute: { bg: 'bg-emerald-500/10', border: 'border-emerald-500/30', text: 'text-emerald-400', icon: '++' },
      execute: { bg: 'bg-emerald-500/10', border: 'border-emerald-500/30', text: 'text-emerald-400', icon: '+' },
      safe: { bg: 'bg-emerald-500/10', border: 'border-emerald-500/30', text: 'text-emerald-400', icon: 'OK' },
      review: { bg: 'bg-amber-500/10', border: 'border-amber-500/30', text: 'text-amber-400', icon: '!' },
      caution: { bg: 'bg-amber-500/10', border: 'border-amber-500/30', text: 'text-amber-400', icon: '!' },
      skip: { bg: 'bg-red-500/10', border: 'border-red-500/30', text: 'text-red-400', icon: '-' },
      strong_skip: { bg: 'bg-red-500/10', border: 'border-red-500/30', text: 'text-red-400', icon: '--' },
      avoid: { bg: 'bg-red-500/10', border: 'border-red-500/30', text: 'text-red-400', icon: 'X' },
    }
    const c = colors[v] || colors.review

    return (
      <div className={cn('rounded-lg border p-3', c.bg, c.border)}>
        <div className="flex items-center gap-2 mb-1">
          <span className={cn('text-xs font-mono font-bold px-1.5 py-0.5 rounded', c.bg, c.text)}>{c.icon}</span>
          <span className={cn('text-sm font-bold uppercase', c.text)}>
            {props.verdict.replace(/_/g, ' ')}
          </span>
        </div>
        <p className="text-xs text-white/70 leading-relaxed">{props.reasoning}</p>
      </div>
    )
  },
})

// ---------------------------------------------------------------------------
// BulletList — list of text items
// ---------------------------------------------------------------------------
const BulletList = defineComponent({
  name: 'BulletList',
  description: 'A list of bullet point text items',
  props: z.object({
    items: z.array(z.string()),
    ordered: z.boolean().optional(),
  }),
  component: ({ props }: ComponentRenderProps<{ items: string[]; ordered?: boolean }>) => {
    const Tag = props.ordered ? 'ol' : 'ul'
    return (
      <Tag className={cn('text-xs text-white/70 space-y-1 pl-4', props.ordered ? 'list-decimal' : 'list-disc')}>
        {props.items.map((item: string, i: number) => (
          <li key={i}>{item}</li>
        ))}
      </Tag>
    )
  },
})

// ---------------------------------------------------------------------------
// TextBlock — styled text content
// ---------------------------------------------------------------------------
const TextBlock = defineComponent({
  name: 'TextBlock',
  description: 'A block of text content with optional styling',
  props: z.object({
    text: z.string(),
    variant: z.string().optional().describe('"body", "heading", "caption", "code"'),
  }),
  component: ({ props }: ComponentRenderProps<{ text: string; variant?: string }>) => {
    const styles: Record<string, string> = {
      body: 'text-xs text-white/70 leading-relaxed',
      heading: 'text-sm font-semibold text-white',
      caption: 'text-[10px] text-white/40',
      code: 'text-xs font-mono text-cyan-300 bg-white/5 px-2 py-1 rounded',
    }
    return <p className={styles[props.variant || 'body'] || styles.body}>{props.text}</p>
  },
})

// ---------------------------------------------------------------------------
// Library export
// ---------------------------------------------------------------------------
export const homerunLibrary = createLibrary({
  components: [
    Stack,
    Card,
    ScoreGauge,
    Badge,
    KeyValue,
    DataTable,
    SentimentBar,
    MarketCard,
    Recommendation,
    BulletList,
    TextBlock,
  ],
})
