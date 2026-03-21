import { useState, useRef, useCallback, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient, type UseMutationResult } from '@tanstack/react-query'
import {
  Play,
  Square,
  Check,
  X,
  FlaskConical,
  Settings2,
  Loader2,
  TrendingUp,
  Code2,
  SlidersHorizontal,
  FlaskRound,
} from 'lucide-react'
import { cn } from '../lib/utils'
import { Badge } from './ui/badge'
import { Button } from './ui/button'
import { Input } from './ui/input'
import { Switch } from './ui/switch'
import { Tabs, TabsContent, TabsList, TabsTrigger } from './ui/tabs'
import { Label } from './ui/label'
import StrategyConfigForm from './StrategyConfigForm'
import {
  getAutoresearchStatus,
  getAutoresearchHistory,
  getAutoresearchSettings,
  updateAutoresearchSettings,
  stopAutoresearchExperiment,
  streamAutoresearchExperiment,
  createAbExperimentFromAutoresearch,
  getPlugins,
  type ChatStreamEvent,
  type Trader,
} from '../services/api'

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface StrategyParamSection {
  sectionKey: string
  sourceLabel: string
  strategyLabel: string
  fieldKeys: string[]
  values: Record<string, unknown>
  groups: { key: string; label: string; fields: unknown[] }[]
}

interface RevertSnapshot {
  traderId: string
  capturedAt: string
}

interface AutoresearchViewProps {
  trader: Trader
  dynamicStrategyParamSections: StrategyParamSection[]
  tuneParamSectionTab: string
  setTuneParamSectionTab: (tab: string) => void
  tuneDraftDirty: boolean
  setTuneDraftDirty: (dirty: boolean) => void
  applyTraderDraftSettings: (trader: Trader, options?: Record<string, boolean>) => void
  applyDynamicStrategyFormValues: (keys: string[], values: Record<string, unknown>) => void
  saveTuneParametersMutation: UseMutationResult<unknown, unknown, void, unknown>
  revertTuneParametersMutation: UseMutationResult<unknown, unknown, void, unknown>
  tuneRevertSnapshot: RevertSnapshot | null
  tuneSaveError: string | null
  tuneRevertError: string | null
  formatTimestamp: (iso: string) => string
}

interface StreamIteration {
  iteration: number
  decision: 'kept' | 'reverted' | 'pending'
  new_score: number
  score_delta: number
  reasoning: string
  changed_params: Record<string, unknown> | null
  duration_seconds: number
  source_diff?: string | null
  source_diff_lines?: number
  validation_passed?: boolean | null
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function AutoresearchView({
  trader,
  dynamicStrategyParamSections,
  tuneParamSectionTab,
  setTuneParamSectionTab,
  tuneDraftDirty,
  setTuneDraftDirty,
  applyTraderDraftSettings,
  applyDynamicStrategyFormValues,
  saveTuneParametersMutation,
  revertTuneParametersMutation,
  tuneRevertSnapshot,
  tuneSaveError,
  tuneRevertError,
  formatTimestamp,
}: AutoresearchViewProps) {
  const queryClient = useQueryClient()
  const abortRef = useRef<AbortController | null>(null)

  // Top-level subtab: "parameters" or "autoresearch"
  const [topTab, setTopTab] = useState<'parameters' | 'autoresearch'>('parameters')

  // Autoresearch inner mode: "params" or "code"
  const [arMode, setArMode] = useState<'params' | 'code'>('params')

  // Streaming state
  const [isStreaming, setIsStreaming] = useState(false)
  const [streamIterations, setStreamIterations] = useState<StreamIteration[]>([])
  const [streamPhase, setStreamPhase] = useState<string>('')
  const [streamError, setStreamError] = useState<string>('')

  // Settings panel
  const [showSettings, setShowSettings] = useState(false)

  // Code mode state
  const [selectedStrategyKey, setSelectedStrategyKey] = useState<string>('')
  const [expandedIteration, setExpandedIteration] = useState<number | null>(null)
  const [abCreating, setAbCreating] = useState(false)
  const [doneData, setDoneData] = useState<Record<string, unknown> | null>(null)

  // Strategy list to resolve strategy_key → strategy ID
  const { data: strategies } = useQuery({
    queryKey: ['strategy-plugins'],
    queryFn: getPlugins,
    staleTime: 60000,
  })

  // Derive trader's strategies from source_configs
  const traderStrategies = (trader.source_configs ?? [])
    .map(sc => sc.strategy_key)
    .filter((v, i, arr) => v && arr.indexOf(v) === i)  // unique, non-empty

  // Auto-select if trader has exactly one strategy
  useEffect(() => {
    if (traderStrategies.length === 1 && !selectedStrategyKey) {
      setSelectedStrategyKey(traderStrategies[0])
    }
  }, [traderStrategies, selectedStrategyKey])

  // Resolve strategy_key → strategy ID via the plugins list
  const selectedStrategyId = (strategies ?? []).find(
    s => s.slug === selectedStrategyKey || s.name === selectedStrategyKey
  )?.id ?? ''

  // Queries
  const { data: status } = useQuery({
    queryKey: ['autoresearch-status', trader.id],
    queryFn: () => getAutoresearchStatus(trader.id),
    refetchInterval: isStreaming ? false : 10000,
  })

  const { data: historyData } = useQuery({
    queryKey: ['autoresearch-history', trader.id],
    queryFn: () => getAutoresearchHistory(trader.id, 30),
    refetchInterval: isStreaming ? false : 15000,
  })

  const { data: settings } = useQuery({
    queryKey: ['autoresearch-settings'],
    queryFn: getAutoresearchSettings,
  })

  const settingsMutation = useMutation({
    mutationFn: updateAutoresearchSettings,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['autoresearch-settings'] }),
  })

  // Display iterations
  const dbIterations = historyData?.iterations ?? []
  const displayIterations = isStreaming
    ? streamIterations
    : dbIterations.map(i => ({
        iteration: i.iteration_number,
        decision: i.decision,
        new_score: i.new_score,
        score_delta: i.score_delta,
        reasoning: i.reasoning,
        changed_params: i.changed_params,
        duration_seconds: i.duration_seconds,
        source_diff: i.source_diff,
        validation_passed: i.validation_result?.valid as boolean | undefined,
      } as StreamIteration))

  // Derived
  const experimentStatus = isStreaming ? 'running' : (status?.status ?? 'idle')
  const bestScore = isStreaming
    ? (streamIterations.length > 0 ? Math.max(...streamIterations.filter(i => i.decision === 'kept').map(i => i.new_score), status?.baseline_score ?? 0) : status?.baseline_score ?? 0)
    : (status?.best_score ?? 0)
  const baselineScore = status?.baseline_score ?? 0
  const iterationCount = isStreaming ? streamIterations.length : (status?.iteration_count ?? 0)
  const keptCount = isStreaming ? streamIterations.filter(i => i.decision === 'kept').length : (status?.kept_count ?? 0)
  const revertedCount = isStreaming ? streamIterations.filter(i => i.decision === 'reverted').length : (status?.reverted_count ?? 0)

  // Start experiment
  const handleStart = useCallback(() => {
    setIsStreaming(true)
    setStreamIterations([])
    setStreamPhase('starting')
    setStreamError('')
    setDoneData(null)
    const controller = new AbortController()
    abortRef.current = controller

    streamAutoresearchExperiment(
      trader.id,
      (event: ChatStreamEvent) => {
        const { event: type, data } = event
        switch (type) {
          case 'experiment_start':
            setStreamPhase('running')
            break
          case 'iteration_start':
            setStreamPhase(`iteration ${data.iteration}/${data.total}`)
            break
          case 'proposal':
            setStreamPhase('evaluating proposal...')
            break
          case 'decision':
            setStreamIterations(prev => [
              {
                iteration: data.iteration as number,
                decision: data.decision as 'kept' | 'reverted',
                new_score: data.new_score as number,
                score_delta: data.score_delta as number,
                reasoning: (data.reasoning as string) || '',
                changed_params: (data.changed_params as Record<string, unknown>) || null,
                duration_seconds: data.duration_seconds as number,
                source_diff_lines: (data.source_diff_lines as number) || 0,
                validation_passed: data.validation_passed as boolean | null,
              },
              ...prev,
            ])
            setStreamPhase(`iteration ${data.iteration} — ${data.decision}`)
            break
          case 'error':
            setStreamError(String(data.error || 'Unknown error'))
            break
          case 'done':
            setStreamPhase('completed')
            setDoneData(data)
            break
        }
      },
      () => {
        setIsStreaming(false)
        abortRef.current = null
        queryClient.invalidateQueries({ queryKey: ['autoresearch-status', trader.id] })
        queryClient.invalidateQueries({ queryKey: ['autoresearch-history', trader.id] })
      },
      (error) => {
        setIsStreaming(false)
        abortRef.current = null
        setStreamError(error)
      },
      controller.signal,
      arMode === 'code' ? { mode: 'code', strategy_id: selectedStrategyId || undefined } : { mode: 'params' },
    )
  }, [trader.id, queryClient, arMode, selectedStrategyId, selectedStrategyKey])

  const handleStop = useCallback(async () => {
    abortRef.current?.abort()
    setIsStreaming(false)
    try { await stopAutoresearchExperiment(trader.id) } catch { /* ignore */ }
    queryClient.invalidateQueries({ queryKey: ['autoresearch-status', trader.id] })
    queryClient.invalidateQueries({ queryKey: ['autoresearch-history', trader.id] })
  }, [trader.id, queryClient])

  useEffect(() => { return () => { abortRef.current?.abort() } }, [])

  const statusColor = {
    running: 'bg-emerald-500',
    paused: 'bg-amber-500',
    completed: 'bg-blue-500',
    failed: 'bg-red-500',
    idle: 'bg-muted-foreground',
  }[experimentStatus] ?? 'bg-muted-foreground'

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  return (
    <div className="h-full min-h-0 overflow-hidden px-1">
      <div className="h-full min-h-0 flex flex-col rounded-md border border-border/50 bg-muted/10">
        {/* ===== Top-level subtabs: Parameters | Autoresearch ===== */}
        <div className="shrink-0 flex items-center gap-0.5 border-b border-border/50 px-2 pt-1">
          <button
            onClick={() => setTopTab('parameters')}
            className={cn(
              'flex items-center gap-1.5 px-3 py-1.5 text-[11px] font-medium border-b-2 -mb-px transition-colors',
              topTab === 'parameters'
                ? 'border-cyan-500 text-foreground'
                : 'border-transparent text-muted-foreground hover:text-foreground'
            )}
          >
            <SlidersHorizontal className="w-3 h-3" /> Parameters
          </button>
          <button
            onClick={() => setTopTab('autoresearch')}
            className={cn(
              'flex items-center gap-1.5 px-3 py-1.5 text-[11px] font-medium border-b-2 -mb-px transition-colors',
              topTab === 'autoresearch'
                ? 'border-purple-500 text-foreground'
                : 'border-transparent text-muted-foreground hover:text-foreground'
            )}
          >
            <FlaskConical className="w-3 h-3" /> Autoresearch
          </button>
        </div>

        {/* ===== Parameters subtab ===== */}
        {topTab === 'parameters' && (
          <div className="flex-1 min-h-0 overflow-hidden p-2.5">
            <div className="flex h-full min-h-0 flex-col gap-2">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="flex items-center gap-1.5">
                  <p className="text-[11px] font-medium">Parameter Workspace</p>
                  <Badge variant="outline" className="h-4 px-1.5 text-[9px] font-mono">
                    {dynamicStrategyParamSections.reduce((sum, s) => sum + s.fieldKeys.length, 0)} fields
                  </Badge>
                  {tuneDraftDirty && (
                    <span className="rounded bg-amber-500/15 px-1.5 py-0.5 text-[9px] font-semibold text-amber-500">UNSAVED</span>
                  )}
                </div>
                <div className="flex flex-wrap items-center gap-1.5">
                  <Button type="button" size="sm" variant="outline" className="h-6 px-2 text-[10px]"
                    onClick={() => { applyTraderDraftSettings(trader); setTuneDraftDirty(false) }}>
                    Discard Edits
                  </Button>
                  <Button type="button" size="sm" variant="outline" className="h-6 px-2 text-[10px]"
                    onClick={() => revertTuneParametersMutation.mutate()}
                    disabled={revertTuneParametersMutation.isPending || !tuneRevertSnapshot || tuneRevertSnapshot.traderId !== trader.id}>
                    {revertTuneParametersMutation.isPending && <Loader2 className="mr-1 h-3 w-3 animate-spin" />}
                    Revert Last Applied
                  </Button>
                  <Button type="button" size="sm" className="h-6 px-2 text-[10px]"
                    onClick={() => saveTuneParametersMutation.mutate()}
                    disabled={saveTuneParametersMutation.isPending || !tuneDraftDirty}>
                    {saveTuneParametersMutation.isPending && <Loader2 className="mr-1 h-3 w-3 animate-spin" />}
                    Save Parameters
                  </Button>
                </div>
              </div>
              {tuneRevertSnapshot && tuneRevertSnapshot.traderId === trader.id && (
                <p className="text-[10px] text-muted-foreground/80">Revert snapshot captured at {formatTimestamp(tuneRevertSnapshot.capturedAt)}.</p>
              )}
              {tuneSaveError && <p className="text-[10px] text-red-500">{tuneSaveError}</p>}
              {tuneRevertError && <p className="text-[10px] text-red-500">{tuneRevertError}</p>}
              {dynamicStrategyParamSections.length === 0 ? (
                <p className="text-[10px] text-muted-foreground/80">No dynamic parameter fields available for this bot.</p>
              ) : (
                <Tabs value={tuneParamSectionTab} onValueChange={setTuneParamSectionTab}
                  className="flex min-h-0 flex-1 flex-col overflow-hidden">
                  <div className="shrink-0 overflow-x-auto pb-1">
                    <TabsList className="h-auto w-max min-w-full justify-start gap-1 rounded-md border border-border/50 bg-background/60 p-1">
                      {dynamicStrategyParamSections.map((section) => (
                        <TabsTrigger key={section.sectionKey} value={section.sectionKey} className="h-6 gap-1 px-2 text-[10px]">
                          <span className="max-w-[220px] truncate">{section.sourceLabel} &middot; {section.strategyLabel}</span>
                          <span className="text-[9px] text-muted-foreground">{section.fieldKeys.length}</span>
                        </TabsTrigger>
                      ))}
                    </TabsList>
                  </div>
                  {dynamicStrategyParamSections.map((section) => (
                    <TabsContent key={section.sectionKey} value={section.sectionKey} className="mt-0 flex min-h-0 flex-1 flex-col overflow-hidden">
                      {section.groups.length === 0 ? (
                        <p className="text-[10px] text-muted-foreground/80">No grouped parameter fields.</p>
                      ) : (
                        <Tabs defaultValue={section.groups[0].key} className="flex min-h-0 flex-1 flex-col overflow-hidden">
                          <div className="shrink-0 overflow-x-auto pb-1">
                            <TabsList className="h-auto w-max min-w-full justify-start gap-1 rounded-md border border-border/50 bg-background/50 p-1">
                              {section.groups.map((group) => (
                                <TabsTrigger key={`${section.sectionKey}:${group.key}`} value={group.key} className="h-6 gap-1 px-2 text-[10px]">
                                  <span>{group.label}</span>
                                  <span className="text-[9px] text-muted-foreground">{group.fields.length}</span>
                                </TabsTrigger>
                              ))}
                            </TabsList>
                          </div>
                          {section.groups.map((group) => (
                            <TabsContent key={`${section.sectionKey}:panel:${group.key}`} value={group.key}
                              className="mt-0 min-h-0 flex-1 overflow-auto rounded-md border border-border/50 bg-background/65 p-2">
                              <StrategyConfigForm
                                schema={{ param_fields: group.fields as any[] }}
                                values={section.values}
                                onChange={(nextValues: Record<string, unknown>) => applyDynamicStrategyFormValues(section.fieldKeys, nextValues)}
                              />
                            </TabsContent>
                          ))}
                        </Tabs>
                      )}
                    </TabsContent>
                  ))}
                </Tabs>
              )}
            </div>
          </div>
        )}

        {/* ===== Autoresearch subtab ===== */}
        {topTab === 'autoresearch' && (
          <div className="flex-1 min-h-0 overflow-hidden flex flex-col">
            {/* Inner mode tabs: Params | Code */}
            <div className="shrink-0 flex items-center justify-between gap-2 border-b border-border/40 px-3 py-1.5">
              <div className="flex items-center gap-1 rounded-md border border-border/60 bg-background p-0.5">
                <button
                  onClick={() => setArMode('params')}
                  className={cn(
                    'flex items-center gap-1 rounded px-2.5 py-1 text-[10px] font-medium transition-colors',
                    arMode === 'params' ? 'bg-cyan-500/20 text-cyan-400' : 'text-muted-foreground hover:text-foreground'
                  )}
                >
                  <SlidersHorizontal className="w-3 h-3" /> Parameters
                </button>
                <button
                  onClick={() => setArMode('code')}
                  className={cn(
                    'flex items-center gap-1 rounded px-2.5 py-1 text-[10px] font-medium transition-colors',
                    arMode === 'code' ? 'bg-purple-500/20 text-purple-400' : 'text-muted-foreground hover:text-foreground'
                  )}
                >
                  <Code2 className="w-3 h-3" /> Code
                </button>
              </div>

              {/* Strategy from trader's source_configs (code mode only) */}
              {arMode === 'code' && traderStrategies.length > 0 && (
                traderStrategies.length === 1 ? (
                  <Badge variant="outline" className="h-5 px-2 text-[10px] font-mono">
                    {traderStrategies[0]}
                  </Badge>
                ) : (
                  <select
                    value={selectedStrategyKey}
                    onChange={(e) => setSelectedStrategyKey(e.target.value)}
                    className="h-7 max-w-[220px] rounded-md border border-border/60 bg-background px-2 text-[10px]"
                  >
                    <option value="">Select strategy...</option>
                    {traderStrategies.map(key => (
                      <option key={key} value={key}>{key}</option>
                    ))}
                  </select>
                )
              )}
              {arMode === 'code' && traderStrategies.length === 0 && (
                <span className="text-[10px] text-amber-400">No strategies configured on this trader</span>
              )}

              {/* Status + controls */}
              <div className="flex items-center gap-2">
                <div className={cn('w-2 h-2 rounded-full', statusColor)} />
                <Badge variant="outline" className="h-4 px-1.5 text-[9px] font-mono uppercase">
                  {experimentStatus}
                </Badge>
                <div className="flex items-center gap-1.5 text-[9px] text-muted-foreground">
                  <span>{iterationCount}i</span>
                  <span className="text-emerald-500">{keptCount}K</span>
                  <span className="text-red-400">{revertedCount}R</span>
                </div>
                {bestScore > baselineScore && baselineScore > 0 && (
                  <span className="flex items-center gap-0.5 text-[9px] text-emerald-500">
                    <TrendingUp className="w-3 h-3" />+{(bestScore - baselineScore).toFixed(2)}
                  </span>
                )}
                <button onClick={() => setShowSettings(s => !s)}
                  className="rounded p-1 text-muted-foreground hover:bg-muted/60 hover:text-foreground transition-colors">
                  <Settings2 className="w-3.5 h-3.5" />
                </button>
                {isStreaming ? (
                  <Button size="sm" variant="destructive" className="h-6 px-2 text-[10px]" onClick={handleStop}>
                    <Square className="w-3 h-3 mr-1" /> Stop
                  </Button>
                ) : (
                  <Button size="sm" className="h-6 px-2 text-[10px]" onClick={handleStart}
                    disabled={arMode === 'code' && !selectedStrategyKey}>
                    <Play className="w-3 h-3 mr-1" /> Start
                  </Button>
                )}
              </div>
            </div>

            {/* Settings collapsible */}
            {showSettings && (
              <div className="shrink-0 border-b border-border/40 px-3 py-2 space-y-2">
                <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
                  <div>
                    <Label className="text-[10px] text-muted-foreground">Model Override</Label>
                    <Input value={settings?.model ?? ''} onChange={(e) => settingsMutation.mutate({ model: e.target.value || null })}
                      placeholder="app default" className="mt-0.5 h-7 text-xs font-mono" />
                  </div>
                  <div>
                    <Label className="text-[10px] text-muted-foreground">Max Iterations</Label>
                    <Input type="number" min={1} max={500} value={settings?.max_iterations ?? 50}
                      onChange={(e) => settingsMutation.mutate({ max_iterations: parseInt(e.target.value) || 50 })}
                      className="mt-0.5 h-7 text-xs font-mono" />
                  </div>
                  <div>
                    <Label className="text-[10px] text-muted-foreground">Temperature</Label>
                    <Input type="number" min={0} max={2} step={0.1} value={settings?.temperature ?? 0.2}
                      onChange={(e) => settingsMutation.mutate({ temperature: parseFloat(e.target.value) || 0.2 })}
                      className="mt-0.5 h-7 text-xs font-mono" />
                  </div>
                  <div className="flex items-end gap-2 pb-0.5">
                    <label className="flex items-center gap-1.5 cursor-pointer">
                      <Switch checked={settings?.auto_apply ?? true}
                        onCheckedChange={(v) => settingsMutation.mutate({ auto_apply: v })} />
                      <span className="text-[10px]">Auto-apply kept changes</span>
                    </label>
                  </div>
                </div>
                {arMode === 'params' && (
                  <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
                    <div>
                      <Label className="text-[10px] text-muted-foreground">Walk-Forward Windows</Label>
                      <Input type="number" min={1} max={20} value={settings?.walk_forward_windows ?? 5}
                        onChange={(e) => settingsMutation.mutate({ walk_forward_windows: parseInt(e.target.value) || 5 })}
                        className="mt-0.5 h-7 text-xs font-mono" />
                    </div>
                    <div>
                      <Label className="text-[10px] text-muted-foreground">Train Ratio</Label>
                      <Input type="number" min={0.5} max={0.9} step={0.05} value={settings?.train_ratio ?? 0.7}
                        onChange={(e) => settingsMutation.mutate({ train_ratio: parseFloat(e.target.value) || 0.7 })}
                        className="mt-0.5 h-7 text-xs font-mono" />
                    </div>
                  </div>
                )}
                <div>
                  <Label className="text-[10px] text-muted-foreground">Mandate / Constraints</Label>
                  <textarea value={settings?.mandate ?? ''}
                    onChange={(e) => settingsMutation.mutate({ mandate: e.target.value || null })}
                    placeholder="e.g. Keep max drawdown below 15%, focus on win rate..."
                    className="mt-0.5 min-h-[40px] max-h-[64px] w-full rounded-md border border-border/60 bg-background px-2 py-1 text-xs leading-relaxed resize-y" />
                </div>
              </div>
            )}

            {streamError && <p className="shrink-0 px-3 py-1 text-[10px] text-red-500">{streamError}</p>}
            {isStreaming && <p className="shrink-0 px-3 py-0.5 text-[9px] text-muted-foreground">{streamPhase}</p>}

            {/* Iteration log */}
            <div className="flex-1 min-h-0 overflow-auto px-2 py-1">
              {displayIterations.length === 0 ? (
                <div className="flex items-center justify-center h-full text-muted-foreground">
                  <div className="text-center space-y-1">
                    <FlaskConical className="w-8 h-8 mx-auto opacity-20" />
                    <p className="text-[10px]">
                      {isStreaming ? 'Waiting for first iteration...' : (
                        arMode === 'code'
                          ? 'Select a strategy and click Start to begin code evolution.'
                          : 'Click Start to begin parameter optimization.'
                      )}
                    </p>
                  </div>
                </div>
              ) : (
                <div className="space-y-1">
                  <table className="w-full text-[10px]">
                    <thead className="sticky top-0 bg-background/90 backdrop-blur-sm">
                      <tr className="text-left text-muted-foreground">
                        <th className="px-1.5 py-1 font-medium w-8">#</th>
                        <th className="px-1.5 py-1 font-medium w-16">Score</th>
                        <th className="px-1.5 py-1 font-medium w-14">&Delta;</th>
                        <th className="px-1.5 py-1 font-medium w-10"></th>
                        {arMode === 'code' && <th className="px-1.5 py-1 font-medium w-10">Valid</th>}
                        <th className="px-1.5 py-1 font-medium">{arMode === 'code' ? 'Changes' : 'Params Changed'}</th>
                        <th className="px-1.5 py-1 font-medium w-12">Time</th>
                      </tr>
                    </thead>
                    <tbody>
                      {displayIterations.map((iter, idx) => (
                        <tr key={idx}
                          className={cn('border-t border-border/30 hover:bg-muted/30', iter.decision === 'kept' && 'bg-emerald-500/5')}
                          title={iter.reasoning}>
                          <td className="px-1.5 py-1 font-mono text-muted-foreground">{iter.iteration}</td>
                          <td className="px-1.5 py-1 font-mono">{iter.new_score.toFixed(2)}</td>
                          <td className={cn('px-1.5 py-1 font-mono',
                            iter.score_delta > 0 ? 'text-emerald-500' : iter.score_delta < 0 ? 'text-red-400' : 'text-muted-foreground')}>
                            {iter.score_delta > 0 ? '+' : ''}{iter.score_delta.toFixed(3)}
                          </td>
                          <td className="px-1.5 py-1">
                            {iter.decision === 'kept' ? <Check className="w-3.5 h-3.5 text-emerald-500" />
                              : iter.decision === 'reverted' ? <X className="w-3.5 h-3.5 text-red-400" />
                              : <Loader2 className="w-3.5 h-3.5 animate-spin text-muted-foreground" />}
                          </td>
                          {arMode === 'code' && (
                            <td className="px-1.5 py-1">
                              {iter.validation_passed === true ? <Check className="w-3 h-3 text-emerald-500" />
                                : iter.validation_passed === false ? <X className="w-3 h-3 text-red-400" />
                                : <span className="text-muted-foreground">-</span>}
                            </td>
                          )}
                          <td className="px-1.5 py-1 text-muted-foreground truncate max-w-[200px]">
                            {arMode === 'code' ? (
                              iter.source_diff_lines ? (
                                <button onClick={() => setExpandedIteration(expandedIteration === iter.iteration ? null : iter.iteration)}
                                  className="text-[10px] text-purple-400 hover:underline">
                                  {iter.source_diff_lines} lines
                                </button>
                              ) : <span className="italic">none</span>
                            ) : (
                              iter.changed_params ? Object.keys(iter.changed_params).join(', ') : <span className="italic">none</span>
                            )}
                          </td>
                          <td className="px-1.5 py-1 font-mono text-muted-foreground">{iter.duration_seconds.toFixed(0)}s</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>

                  {/* Expanded diff viewer */}
                  {arMode === 'code' && expandedIteration !== null &&
                    Boolean(displayIterations.find(i => i.iteration === expandedIteration)?.source_diff) && (
                    <div className="rounded border border-purple-500/30 bg-background/90 p-2">
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-[9px] text-muted-foreground">Diff for iteration {expandedIteration}</span>
                        <button onClick={() => setExpandedIteration(null)} className="text-[9px] text-muted-foreground hover:text-foreground">close</button>
                      </div>
                      <pre className="text-[9px] font-mono overflow-auto max-h-[150px] leading-tight">
                        {(displayIterations.find(i => i.iteration === expandedIteration)?.source_diff ?? '').split('\n').map((line: string, i: number) => (
                          <div key={i} className={cn(
                            line.startsWith('+') && !line.startsWith('+++') ? 'text-emerald-400' :
                            line.startsWith('-') && !line.startsWith('---') ? 'text-red-400' :
                            'text-muted-foreground'
                          )}>{line}</div>
                        ))}
                      </pre>
                    </div>
                  )}

                  {/* A/B experiment button */}
                  {arMode === 'code' && !isStreaming && Boolean(doneData?.can_create_ab_experiment) && (
                    <div className="flex items-center gap-2 rounded border border-purple-500/30 bg-purple-500/5 px-2 py-1.5">
                      <FlaskRound className="w-3.5 h-3.5 text-purple-400" />
                      <span className="text-[10px] text-muted-foreground flex-1">
                        Best version v{String(doneData!.best_version)} improved by +{String(doneData!.improvement)}
                      </span>
                      <Button size="sm" variant="outline" className="h-6 px-2 text-[10px]" disabled={abCreating}
                        onClick={async () => {
                          setAbCreating(true)
                          try { await createAbExperimentFromAutoresearch(String(doneData!.experiment_id)); setDoneData(null) } catch { /* */ }
                          setAbCreating(false)
                        }}>
                        {abCreating && <Loader2 className="w-3 h-3 mr-1 animate-spin" />}
                        Create A/B Experiment
                      </Button>
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
