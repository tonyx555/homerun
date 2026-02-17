import { useCallback, useEffect, useRef } from 'react'
import { QueryClient, QueryKey } from '@tanstack/react-query'

type WSMessage = {
  type?: string
  data?: Record<string, any>
} | null | undefined

type RealtimeContext = {
  activeTab?: string
  opportunitiesView?: string
}

const INVALIDATION_DEBOUNCE_MS = 120

export function useRealtimeInvalidation(
  lastMessage: WSMessage,
  queryClient: QueryClient,
  setScannerActivity: (activity: string) => void,
  context: RealtimeContext = {},
) {
  const pendingInvalidationsRef = useRef<Map<string, QueryKey>>(new Map())
  const flushTimerRef = useRef<number | null>(null)

  const flushInvalidations = useCallback(() => {
    if (flushTimerRef.current != null) {
      window.clearTimeout(flushTimerRef.current)
      flushTimerRef.current = null
    }
    if (pendingInvalidationsRef.current.size < 1) return

    const queryKeys = Array.from(pendingInvalidationsRef.current.values())
    pendingInvalidationsRef.current.clear()
    for (const queryKey of queryKeys) {
      queryClient.invalidateQueries({ queryKey })
    }
  }, [queryClient])

  const queueInvalidations = useCallback(
    (queryKeys: QueryKey[], options?: { immediate?: boolean }) => {
      const immediate = Boolean(options?.immediate)
      for (const queryKey of queryKeys) {
        pendingInvalidationsRef.current.set(JSON.stringify(queryKey), queryKey)
      }

      if (immediate) {
        flushInvalidations()
        return
      }

      if (flushTimerRef.current != null) {
        return
      }
      flushTimerRef.current = window.setTimeout(() => {
        flushInvalidations()
      }, INVALIDATION_DEBOUNCE_MS)
    },
    [flushInvalidations],
  )

  useEffect(() => {
    return () => {
      if (flushTimerRef.current != null) {
        window.clearTimeout(flushTimerRef.current)
      }
    }
  }, [])

  useEffect(() => {
    const messageType = lastMessage?.type
    if (!messageType) return

    const activeTab = String(context.activeTab || '')
    const opportunitiesView = String(context.opportunitiesView || '')
    const viewingOpportunities = activeTab === 'opportunities'
    const viewingArbitrage = viewingOpportunities && opportunitiesView === 'arbitrage'
    const viewingNews = viewingOpportunities && opportunitiesView === 'news'
    const viewingWeather = viewingOpportunities && opportunitiesView === 'weather'
    const viewingWorld = viewingOpportunities && opportunitiesView === 'world'
    const viewingTrading = activeTab === 'trading'

    if (messageType === 'scanner_status' && lastMessage.data) {
      queryClient.setQueryData(['scanner-status'], lastMessage.data)
    }
    if (messageType === 'scanner_activity') {
      setScannerActivity(lastMessage.data?.activity || 'Idle')
    }

    if (messageType === 'opportunities_update' || messageType === 'init') {
      queueInvalidations(
        [
          ...(viewingArbitrage ? ([['opportunities']] as QueryKey[]) : []),
          ['opportunity-counts'],
          ['scanner-status'],
        ],
      )
    }
    if (messageType === 'opportunity_events') {
      queueInvalidations(
        [
          ...(viewingArbitrage ? ([['opportunities']] as QueryKey[]) : []),
          ['opportunity-counts'],
        ],
      )
    }
    if (messageType === 'wallet_trade') {
      queueInvalidations([
        ['copy-trades'],
        ['copy-trading-status'],
      ])
    }
    if (
      messageType === 'copy_trade_detected'
      || messageType === 'copy_trade_executed'
    ) {
      queueInvalidations([
        ['copy-trades'],
        ['copy-configs'],
        ['copy-trading-status'],
      ])
    }
    if (messageType === 'tracked_trader_signal') {
      queueInvalidations([
        ['tracked-trader-opportunities'],
        ['discovery-confluence'],
        ['discovery-active-signal-count'],
      ])
    }
    if (messageType === 'tracked_trader_pool_update') {
      queueInvalidations([
        ['discovery-pool-stats'],
        ['discovery-leaderboard'],
      ])
    }
    if (
      messageType === 'news_update'
      || messageType === 'news_workflow_update'
      || messageType === 'news_workflow_status'
    ) {
      queueInvalidations([
        ['news-workflow-status'],
        ['news-workflow-findings-count'],
        ...(viewingNews
          ? ([
              ['news-articles'],
              ['news-matches'],
              ['news-edges'],
              ['news-feed-status'],
              ['news-workflow-findings'],
              ['news-workflow-intents'],
            ] as QueryKey[])
          : []),
      ])
    }
    if (messageType === 'weather_update' || messageType === 'weather_status') {
      queueInvalidations([
        ['weather-workflow-status'],
        ...(viewingWeather
          ? ([
              ['weather-workflow-opportunities'],
              ['weather-workflow-intents'],
              ['weather-workflow-performance'],
            ] as QueryKey[])
          : []),
      ])
    }
    if (messageType === 'world_intelligence_update' || messageType === 'world_intelligence_status') {
      queueInvalidations([
        ['world-intelligence-status'],
        ...(viewingWorld
          ? ([
              ['world-signals'],
              ['world-instability'],
              ['world-tensions'],
              ['world-convergences'],
              ['world-anomalies'],
              ['world-regions'],
              ['world-intelligence-summary'],
              ['world-intelligence-sources'],
            ] as QueryKey[])
          : []),
      ])
    }
    if (messageType === 'worker_status_update') {
      queueInvalidations([
        ['workers-status'],
        ['scanner-status'],
        ['news-workflow-status'],
        ['weather-workflow-status'],
      ])
    }
    if (messageType === 'signals_update') {
      queueInvalidations([
        ['signals'],
        ['signals-stats'],
      ])
    }
    if (messageType === 'trader_orchestrator_status') {
      queueInvalidations([
        ['trader-orchestrator-overview'],
        ['trader-orchestrator-status'],
        ...(viewingTrading ? ([['traders-list']] as QueryKey[]) : []),
      ])
    }
    if (messageType === 'trader_decision') {
      queueInvalidations([
        ['trader-orchestrator-overview'],
        ...(viewingTrading
          ? ([
              ['traders-decisions'],
              ['trader-decisions-all'],
              ['trader-events-all'],
            ] as QueryKey[])
          : []),
      ])
    }
    if (messageType === 'trader_order') {
      queueInvalidations([
        ['trader-orchestrator-overview'],
        ...(viewingTrading
          ? ([
              ['traders-orders'],
              ['traders-decisions'],
              ['trader-orders-all'],
              ['trader-decisions-all'],
              ['trader-events-all'],
            ] as QueryKey[])
          : []),
      ])
    }
    if (messageType === 'trader_event') {
      queueInvalidations(
        viewingTrading
          ? [
              ['traders-events'],
              ['trader-events-all'],
            ]
          : [],
      )
    }
  }, [context.activeTab, context.opportunitiesView, lastMessage, queryClient, queueInvalidations, setScannerActivity])
}
