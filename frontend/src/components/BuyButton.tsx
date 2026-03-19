import { useState, useRef, useEffect } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ShoppingCart, ChevronDown, Check, AlertTriangle, Loader2, Bot } from 'lucide-react'
import { cn } from '../lib/utils'
import { getTraders, traderManualBuy, Opportunity, Trader } from '../services/api'
import { Button } from './ui/button'

interface BuyButtonProps {
  opportunity: Opportunity
  className?: string
}

export default function BuyButton({ opportunity, className }: BuyButtonProps) {
  const [open, setOpen] = useState(false)
  const [result, setResult] = useState<{ success: boolean; message: string } | null>(null)
  const ref = useRef<HTMLDivElement>(null)
  const queryClient = useQueryClient()

  const { data: traders = [] } = useQuery({
    queryKey: ['traders-for-buy'],
    queryFn: () => getTraders(),
    staleTime: 30_000,
  })

  const enabledTraders = traders.filter((t: Trader) => t.is_enabled)

  const mutation = useMutation({
    mutationFn: (traderId: string) => {
      const positions = opportunity.positions_to_take.map(pos => ({
        token_id: pos.token_id || '',
        side: pos.action || 'BUY',
        price: pos.price,
        market_id: pos.market_id || '',
        market_question: pos.market || '',
        outcome: pos.outcome || '',
      }))
      return traderManualBuy(traderId, {
        positions,
        size_usd: opportunity.total_cost > 0 ? opportunity.total_cost : 10,
        opportunity_id: opportunity.id,
      })
    },
    onSuccess: (data) => {
      setResult({ success: data.status !== 'partial_failure', message: data.message })
      setOpen(false)
      queryClient.invalidateQueries({ queryKey: ['trader-orders'] })
      queryClient.invalidateQueries({ queryKey: ['trading-positions'] })
      setTimeout(() => setResult(null), 4000)
    },
    onError: (error: any) => {
      setResult({
        success: false,
        message: error?.response?.data?.detail || error?.message || 'Buy failed',
      })
      setOpen(false)
      setTimeout(() => setResult(null), 4000)
    },
  })

  useEffect(() => {
    if (!open) return
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [open])

  const hasPositions = opportunity.positions_to_take && opportunity.positions_to_take.length > 0

  return (
    <div ref={ref} className={cn('relative', className)}>
      {result && (
        <div className={cn(
          'absolute bottom-full mb-1 left-0 right-0 rounded-md px-2 py-1 text-[10px] z-50 flex items-center gap-1',
          result.success
            ? 'bg-green-500/20 text-green-400 border border-green-500/30'
            : 'bg-red-500/20 text-red-400 border border-red-500/30'
        )}>
          {result.success ? <Check className="w-3 h-3" /> : <AlertTriangle className="w-3 h-3" />}
          <span className="truncate">{result.message}</span>
        </div>
      )}

      <Button
        onClick={(e) => { e.stopPropagation(); setOpen(!open) }}
        disabled={!hasPositions || mutation.isPending}
        size="sm"
        className={cn(
          'w-full h-8 text-xs font-medium',
          'bg-gradient-to-r from-emerald-500 to-green-500 hover:from-emerald-600 hover:to-green-600',
          'shadow-lg shadow-green-500/20 text-white',
        )}
      >
        {mutation.isPending ? (
          <Loader2 className="w-3 h-3 mr-1.5 animate-spin" />
        ) : (
          <ShoppingCart className="w-3 h-3 mr-1.5" />
        )}
        Buy
        <ChevronDown className={cn('w-3 h-3 ml-1 transition-transform', open && 'rotate-180')} />
      </Button>

      {open && (
        <div className="absolute top-full mt-1 left-0 right-0 z-[110] bg-popover border border-border rounded-lg shadow-xl shadow-black/30 overflow-hidden">
          <div className="px-2 py-1.5 border-b border-border">
            <p className="text-[10px] text-muted-foreground font-medium">Select a bot</p>
          </div>
          <div className="max-h-48 overflow-y-auto">
            {enabledTraders.length === 0 ? (
              <div className="px-3 py-4 text-center">
                <Bot className="w-5 h-5 mx-auto mb-1 text-muted-foreground/50" />
                <p className="text-xs text-muted-foreground">No enabled bots</p>
              </div>
            ) : (
              enabledTraders.map((trader: Trader) => (
                <button
                  key={trader.id}
                  onClick={(e) => {
                    e.stopPropagation()
                    if (trader.mode === 'live') {
                      if (!confirm(`Send LIVE buy order via "${trader.name}" with REAL MONEY?`)) return
                    }
                    mutation.mutate(trader.id)
                  }}
                  className="w-full px-3 py-2 text-left hover:bg-accent/50 transition-colors flex items-center gap-2"
                >
                  <div className={cn(
                    'w-1.5 h-1.5 rounded-full flex-shrink-0',
                    trader.mode === 'live' ? 'bg-green-400' : 'bg-amber-400'
                  )} />
                  <div className="flex-1 min-w-0">
                    <p className="text-xs font-medium text-foreground truncate">{trader.name}</p>
                    <p className="text-[10px] text-muted-foreground">
                      {trader.mode === 'live' ? 'Live' : 'Shadow'}
                    </p>
                  </div>
                </button>
              ))
            )}
          </div>
        </div>
      )}
    </div>
  )
}
