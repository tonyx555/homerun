import { useEffect, useState } from 'react'
import { Bot, Layers, Puzzle, Search, X } from 'lucide-react'
import { cn } from '../lib/utils'
import { Input } from './ui/input'
import OpportunityStrategiesManager from './OpportunityStrategiesManager'
import TraderStrategiesManager from './TraderStrategiesManager'

interface NavigateStrategiesDetail {
  subtab?: 'opportunity' | 'autotrader'
  sourceFilter?: string
}

export default function StrategiesPanel() {
  const [initialSourceFilter, setInitialSourceFilter] = useState<string | undefined>(undefined)
  const [searchQuery, setSearchQuery] = useState('')
  /** Which section to scroll to / highlight when navigated via event */
  const [focusSection, setFocusSection] = useState<'detection' | 'execution' | null>(null)

  useEffect(() => {
    const handler = (event: Event) => {
      const detail = (event as CustomEvent<'opportunity' | 'autotrader' | NavigateStrategiesDetail>).detail
      // Support both legacy string format and new object format
      if (typeof detail === 'string') {
        if (detail === 'opportunity') setFocusSection('detection')
        if (detail === 'autotrader') setFocusSection('execution')
      } else if (detail && typeof detail === 'object') {
        const subtab = detail.subtab || 'opportunity'
        setFocusSection(subtab === 'autotrader' ? 'execution' : 'detection')
        if (detail.sourceFilter) {
          setInitialSourceFilter(detail.sourceFilter)
        }
      }
    }
    window.addEventListener('navigate-strategies-subtab', handler as EventListener)
    return () => window.removeEventListener('navigate-strategies-subtab', handler as EventListener)
  }, [])

  // Auto-scroll to the focused section
  useEffect(() => {
    if (!focusSection) return
    const elementId = focusSection === 'detection' ? 'strategies-detection-section' : 'strategies-execution-section'
    const el = document.getElementById(elementId)
    if (el) {
      el.scrollIntoView({ behavior: 'smooth', block: 'start' })
    }
    // Clear after scroll
    const timeout = setTimeout(() => setFocusSection(null), 1500)
    return () => clearTimeout(timeout)
  }, [focusSection])

  return (
    <div className="h-full min-h-0 flex flex-col">
      {/* ── Unified header ── */}
      <div className="shrink-0 pb-3 space-y-3">
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <Layers className="w-4 h-4 text-violet-400" />
            <h2 className="text-sm font-semibold">All Strategies</h2>
          </div>
          {/* Search bar */}
          <div className="relative flex-1 max-w-xs">
            <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-muted-foreground pointer-events-none" />
            <Input
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="Filter strategies..."
              className="h-7 pl-8 pr-7 text-xs"
            />
            {searchQuery && (
              <button
                type="button"
                onClick={() => setSearchQuery('')}
                className="absolute right-2 top-1/2 -translate-y-1/2 p-0.5 hover:bg-muted/50 rounded"
              >
                <X className="w-3 h-3 text-muted-foreground" />
              </button>
            )}
          </div>
        </div>

        {/* Pipeline info cards */}
        <div className="grid gap-2 md:grid-cols-2">
          <div className="rounded-md border border-amber-500/30 bg-amber-500/5 px-3 py-2">
            <div className="flex items-center gap-1.5">
              <Puzzle className="w-3 h-3 text-amber-400" />
              <p className="text-[11px] font-semibold text-amber-300">Detection Strategies</p>
            </div>
            <p className="text-[11px] text-muted-foreground leading-relaxed mt-1">
              <code className="text-[10px] bg-muted/30 px-1 rounded">detect()</code> scans market data and surfaces tradeable opportunities.
            </p>
          </div>
          <div className="rounded-md border border-cyan-500/30 bg-cyan-500/5 px-3 py-2">
            <div className="flex items-center gap-1.5">
              <Bot className="w-3 h-3 text-cyan-400" />
              <p className="text-[11px] font-semibold text-cyan-300">Execution Strategies</p>
            </div>
            <p className="text-[11px] text-muted-foreground leading-relaxed mt-1">
              <code className="text-[10px] bg-muted/30 px-1 rounded">evaluate()</code> decides whether to trade a detected opportunity.
            </p>
          </div>
        </div>
      </div>

      {/* ── Stacked strategy managers ── */}
      <div className="flex-1 min-h-0 overflow-y-auto space-y-4">
        {/* Detection Strategies section */}
        <div
          id="strategies-detection-section"
          className={cn(
            'min-h-0 transition-all duration-300',
            focusSection === 'detection' && 'ring-1 ring-amber-500/30 rounded-lg'
          )}
        >
          <div className="flex items-center gap-2 px-1 pb-2">
            <div className="w-1 h-4 rounded-full bg-amber-500" />
            <Puzzle className="w-3.5 h-3.5 text-amber-400" />
            <span className="text-xs font-semibold text-amber-300 uppercase tracking-wider">Detection Strategies</span>
            <div className="flex-1 h-px bg-amber-500/15" />
          </div>
          <div className="h-[calc(50vh-80px)] min-h-[400px]">
            <OpportunityStrategiesManager initialSourceFilter={initialSourceFilter} searchQuery={searchQuery} />
          </div>
        </div>

        {/* Execution Strategies section */}
        <div
          id="strategies-execution-section"
          className={cn(
            'min-h-0 transition-all duration-300',
            focusSection === 'execution' && 'ring-1 ring-cyan-500/30 rounded-lg'
          )}
        >
          <div className="flex items-center gap-2 px-1 pb-2">
            <div className="w-1 h-4 rounded-full bg-cyan-500" />
            <Bot className="w-3.5 h-3.5 text-cyan-400" />
            <span className="text-xs font-semibold text-cyan-300 uppercase tracking-wider">Execution Strategies</span>
            <div className="flex-1 h-px bg-cyan-500/15" />
          </div>
          <div className="h-[calc(50vh-80px)] min-h-[400px]">
            <TraderStrategiesManager searchQuery={searchQuery} />
          </div>
        </div>
      </div>
    </div>
  )
}
