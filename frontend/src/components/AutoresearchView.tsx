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
  // Parameter workspace pass-through
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

// ---------------------------------------------------------------------------
// Iteration log row
// ---------------------------------------------------------------------------

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

  // Streaming state
  const [isStreaming, setIsStreaming] = useState(false)
  const [streamIterations, setStreamIterations] = useState<StreamIteration[]>([])
  const [streamPhase, setStreamPhase] = useState<string>('')
  const [streamError, setStreamError] = useState<string>('')

  // Settings panel
  const [showSettings, setShowSettings] = useState(false)

  // Code mode state
  const [selectedStrategyId, setSelectedStrategyId] = useState<string>('')
  const [expandedIteration, setExpandedIteration] = useState<number | null>(null)
  const [abCreating, setAbCreating] = useState(false)
  const [doneData, setDoneData] = useState<Record<string, unknown> | null>(null)

  // Strategy list for code mode
  const { data: strategies } = useQuery({
    queryKey: ['strategy-plugins'],
    queryFn: getPlugins,
    staleTime: 60000,
  })

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

  // Combine streamed iterations with DB history
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

  // Derive status fields
  const currentMode = settings?.mode ?? 'params'
  const experimentStatus = isStreaming ? 'running' : (status?.status ?? 'idle')
  const bestScore = isStreaming
    ? (streamIterations.length > 0 ? Math.max(...streamIterations.filter(i => i.decision === 'kept').map(i => i.new_score), status?.baseline_score ?? 0) : status?.baseline_score ?? 0)
    : (status?.best_score ?? 0)
  const baselineScore = status?.baseline_score ?? 0
  const iterationCount = isStreaming ? streamIterations.length : (status?.iteration_count ?? 0)
  const keptCount = isStreaming
    ? streamIterations.filter(i => i.decision === 'kept').length
    : (status?.kept_count ?? 0)
  const revertedCount = isStreaming
    ? streamIterations.filter(i => i.decision === 'reverted').length
    : (status?.reverted_count ?? 0)

  // Start experiment
  const handleStart = useCallback(() => {
    setIsStreaming(true)
    setStreamIterations([])
    setStreamPhase('starting')
    setStreamError('')
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
            setStreamPhase(`evaluating proposal...`)
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
      currentMode === 'code' ? { mode: 'code', strategy_id: selectedStrategyId || undefined } : undefined,
    )
  }, [trader.id, queryClient, currentMode, selectedStrategyId])

  const handleStop = useCallback(async () => {
    abortRef.current?.abort()
    setIsStreaming(false)
    try {
      await stopAutoresearchExperiment(trader.id)
    } catch { /* ignore */ }
    queryClient.invalidateQueries({ queryKey: ['autoresearch-status', trader.id] })
    queryClient.invalidateQueries({ queryKey: ['autoresearch-history', trader.id] })
  }, [trader.id, queryClient])

  // Cleanup on unmount
  useEffect(() => {
    return () => { abortRef.current?.abort() }
  }, [])

  const statusColor = {
    running: 'bg-emerald-500',
    paused: 'bg-amber-500',
    completed: 'bg-blue-500',
    failed: 'bg-red-500',
    idle: 'bg-muted-foreground',
  }[experimentStatus] ?? 'bg-muted-foreground'

  return (
    <div className="h-full min-h-0 overflow-hidden px-1">
      <div className="h-full min-h-0 rounded-md border border-border/50 bg-muted/10 p-2">
        <div
          className="grid h-full min-h-0 gap-2"
          style={{ gridTemplateRows: 'minmax(0, 40%) minmax(0, 60%)' }}
        >
          {/* ===== TOP: Autoresearch Loop ===== */}
          <div className="min-h-0 overflow-hidden rounded-md border border-cyan-500/30 bg-cyan-500/5 p-2.5">
            <div className="flex h-full min-h-0 flex-col gap-2">
              {/* Status bar */}
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="flex items-center gap-1.5">
                  <FlaskConical className="h-3.5 w-3.5 text-cyan-500" />
                  <p className="text-[11px] font-medium">Autoresearch</p>
                  <div className={cn('w-2 h-2 rounded-full', statusColor)} />
                  <Badge variant="outline" className="h-4 px-1.5 text-[9px] font-mono uppercase">
                    {experimentStatus}
                  </Badge>
                  {isStreaming && (
                    <span className="text-[9px] text-muted-foreground">{streamPhase}</span>
                  )}
                </div>
                <div className="flex items-center gap-2">
                  {/* Quick metrics */}
                  <div className="flex items-center gap-3 text-[10px] text-muted-foreground">
                    <span>Iter: <span className="font-mono text-foreground">{iterationCount}</span></span>
                    <span>
                      Best: <span className="font-mono text-foreground">{bestScore.toFixed(2)}</span>
                    </span>
                    {bestScore > baselineScore && baselineScore > 0 && (
                      <span className="flex items-center gap-0.5 text-emerald-500">
                        <TrendingUp className="w-3 h-3" />
                        +{(bestScore - baselineScore).toFixed(2)}
                      </span>
                    )}
                    <span className="text-[9px]">
                      <span className="text-emerald-500">{keptCount}K</span>
                      {' / '}
                      <span className="text-red-400">{revertedCount}R</span>
                    </span>
                  </div>
                  {/* Controls */}
                  <button
                    onClick={() => setShowSettings(s => !s)}
                    className="rounded p-1 text-muted-foreground hover:bg-muted/60 hover:text-foreground transition-colors"
                  >
                    <Settings2 className="w-3.5 h-3.5" />
                  </button>
                  {isStreaming ? (
                    <Button size="sm" variant="destructive" className="h-6 px-2 text-[10px]" onClick={handleStop}>
                      <Square className="w-3 h-3 mr-1" /> Stop
                    </Button>
                  ) : (
                    <Button
                      size="sm"
                      className="h-6 px-2 text-[10px]"
                      onClick={handleStart}
                      disabled={currentMode === 'code' && !selectedStrategyId}
                    >
                      <Play className="w-3 h-3 mr-1" /> Start
                    </Button>
                  )}
                </div>
              </div>

              {/* Settings collapsible */}
              {showSettings && (
                <div className="rounded-md border border-border/60 bg-background/80 p-2 space-y-2">
                  {/* Mode selector + strategy picker */}
                  <div className="flex items-center gap-3">
                    <div className="flex items-center gap-1 rounded-md border border-border/60 bg-background p-0.5">
                      <button
                        onClick={() => settingsMutation.mutate({ mode: 'params' })}
                        className={cn(
                          'flex items-center gap-1 rounded px-2 py-1 text-[10px] font-medium transition-colors',
                          currentMode === 'params' ? 'bg-cyan-500/20 text-cyan-400' : 'text-muted-foreground hover:text-foreground'
                        )}
                      >
                        <SlidersHorizontal className="w-3 h-3" /> Parameters
                      </button>
                      <button
                        onClick={() => settingsMutation.mutate({ mode: 'code' })}
                        className={cn(
                          'flex items-center gap-1 rounded px-2 py-1 text-[10px] font-medium transition-colors',
                          currentMode === 'code' ? 'bg-purple-500/20 text-purple-400' : 'text-muted-foreground hover:text-foreground'
                        )}
                      >
                        <Code2 className="w-3 h-3" /> Code
                      </button>
                    </div>
                    {currentMode === 'code' && (
                      <div className="flex-1">
                        <select
                          value={selectedStrategyId}
                          onChange={(e) => setSelectedStrategyId(e.target.value)}
                          className="h-7 w-full rounded-md border border-border/60 bg-background px-2 text-xs"
                        >
                          <option value="">Select strategy to evolve...</option>
                          {(strategies ?? []).filter(s => s.enabled && s.source_code).map(s => (
                            <option key={s.id} value={s.id}>{s.name || s.slug} ({s.source_key})</option>
                          ))}
                        </select>
                      </div>
                    )}
                  </div>
                  <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
                    <div>
                      <Label className="text-[10px] text-muted-foreground">Model Override</Label>
                      <Input
                        value={settings?.model ?? ''}
                        onChange={(e) => settingsMutation.mutate({ model: e.target.value || null })}
                        placeholder="app default"
                        className="mt-0.5 h-7 text-xs font-mono"
                      />
                    </div>
                    <div>
                      <Label className="text-[10px] text-muted-foreground">Max Iterations</Label>
                      <Input
                        type="number"
                        min={1}
                        max={500}
                        value={settings?.max_iterations ?? 50}
                        onChange={(e) => settingsMutation.mutate({ max_iterations: parseInt(e.target.value) || 50 })}
                        className="mt-0.5 h-7 text-xs font-mono"
                      />
                    </div>
                    <div>
                      <Label className="text-[10px] text-muted-foreground">Walk-Forward Windows</Label>
                      <Input
                        type="number"
                        min={1}
                        max={20}
                        value={settings?.walk_forward_windows ?? 5}
                        onChange={(e) => settingsMutation.mutate({ walk_forward_windows: parseInt(e.target.value) || 5 })}
                        className="mt-0.5 h-7 text-xs font-mono"
                      />
                    </div>
                    <div>
                      <Label className="text-[10px] text-muted-foreground">Train Ratio</Label>
                      <Input
                        type="number"
                        min={0.5}
                        max={0.9}
                        step={0.05}
                        value={settings?.train_ratio ?? 0.7}
                        onChange={(e) => settingsMutation.mutate({ train_ratio: parseFloat(e.target.value) || 0.7 })}
                        className="mt-0.5 h-7 text-xs font-mono"
                      />
                    </div>
                  </div>
                  <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
                    <div>
                      <Label className="text-[10px] text-muted-foreground">Temperature</Label>
                      <Input
                        type="number"
                        min={0}
                        max={2}
                        step={0.1}
                        value={settings?.temperature ?? 0.2}
                        onChange={(e) => settingsMutation.mutate({ temperature: parseFloat(e.target.value) || 0.2 })}
                        className="mt-0.5 h-7 text-xs font-mono"
                      />
                    </div>
                    <div className="flex items-end gap-2 pb-0.5">
                      <label className="flex items-center gap-1.5 cursor-pointer">
                        <Switch
                          checked={settings?.auto_apply ?? true}
                          onCheckedChange={(v) => settingsMutation.mutate({ auto_apply: v })}
                        />
                        <span className="text-[10px]">Auto-apply kept params</span>
                      </label>
                    </div>
                    <div className="sm:col-span-2">
                      <Label className="text-[10px] text-muted-foreground">Mandate / Constraints</Label>
                      <textarea
                        value={settings?.mandate ?? ''}
                        onChange={(e) => settingsMutation.mutate({ mandate: e.target.value || null })}
                        placeholder="e.g. Keep max drawdown below 15%, focus on win rate over profit..."
                        className="mt-0.5 min-h-[48px] max-h-[72px] w-full rounded-md border border-border/60 bg-background px-2 py-1 text-xs leading-relaxed resize-y"
                      />
                    </div>
                  </div>
                </div>
              )}

              {streamError && (
                <p className="text-[10px] text-red-500">{streamError}</p>
              )}

              {/* Iteration log */}
              <div className="flex-1 min-h-0 overflow-auto rounded-md border border-border/50 bg-background/60">
                {displayIterations.length === 0 ? (
                  <div className="flex items-center justify-center h-full text-muted-foreground">
                    <p className="text-[10px]">
                      {isStreaming ? 'Waiting for first iteration...' : 'No iterations yet. Click Start to begin.'}
                    </p>
                  </div>
                ) : (
                  <table className="w-full text-[10px]">
                    <thead className="sticky top-0 bg-background/90 backdrop-blur-sm">
                      <tr className="text-left text-muted-foreground">
                        <th className="px-1.5 py-1 font-medium w-8">#</th>
                        <th className="px-1.5 py-1 font-medium w-16">Score</th>
                        <th className="px-1.5 py-1 font-medium w-14">&Delta;</th>
                        <th className="px-1.5 py-1 font-medium w-10"></th>
                        <th className="px-1.5 py-1 font-medium">{currentMode === 'code' ? 'Changes' : 'Params Changed'}</th>
                        {currentMode === 'code' && <th className="px-1.5 py-1 font-medium w-10">Valid</th>}
                        <th className="px-1.5 py-1 font-medium w-12">Time</th>
                      </tr>
                    </thead>
                    <tbody>
                      {displayIterations.map((iter, idx) => (
                        <tr
                          key={idx}
                          className={cn(
                            'border-t border-border/30 hover:bg-muted/30',
                            iter.decision === 'kept' && 'bg-emerald-500/5',
                          )}
                          title={iter.reasoning}
                        >
                          <td className="px-1.5 py-1 font-mono text-muted-foreground">{iter.iteration}</td>
                          <td className="px-1.5 py-1 font-mono">{iter.new_score.toFixed(2)}</td>
                          <td className={cn(
                            'px-1.5 py-1 font-mono',
                            iter.score_delta > 0 ? 'text-emerald-500' : iter.score_delta < 0 ? 'text-red-400' : 'text-muted-foreground'
                          )}>
                            {iter.score_delta > 0 ? '+' : ''}{iter.score_delta.toFixed(3)}
                          </td>
                          <td className="px-1.5 py-1">
                            {iter.decision === 'kept' ? (
                              <Check className="w-3.5 h-3.5 text-emerald-500" />
                            ) : iter.decision === 'reverted' ? (
                              <X className="w-3.5 h-3.5 text-red-400" />
                            ) : (
                              <Loader2 className="w-3.5 h-3.5 animate-spin text-muted-foreground" />
                            )}
                          </td>
                          <td className="px-1.5 py-1 text-muted-foreground truncate max-w-[200px]">
                            {currentMode === 'code' ? (
                              iter.source_diff_lines ? (
                                <button
                                  onClick={() => setExpandedIteration(expandedIteration === iter.iteration ? null : iter.iteration)}
                                  className="text-[10px] text-purple-400 hover:underline"
                                >
                                  {iter.source_diff_lines} lines changed
                                </button>
                              ) : <span className="italic">none</span>
                            ) : (
                              iter.changed_params
                                ? Object.keys(iter.changed_params).join(', ')
                                : <span className="italic">none</span>
                            )}
                          </td>
                          {currentMode === 'code' && (
                            <td className="px-1.5 py-1">
                              {iter.validation_passed === true ? (
                                <Check className="w-3 h-3 text-emerald-500" />
                              ) : iter.validation_passed === false ? (
                                <X className="w-3 h-3 text-red-400" />
                              ) : (
                                <span className="text-muted-foreground">-</span>
                              )}
                            </td>
                          )}
                          <td className="px-1.5 py-1 font-mono text-muted-foreground">
                            {iter.duration_seconds.toFixed(0)}s
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}

                {/* Expanded diff viewer */}
                {currentMode === 'code' && expandedIteration !== null &&
                  Boolean(displayIterations.find(i => i.iteration === expandedIteration)?.source_diff) && (
                  <div className="mt-1 rounded border border-purple-500/30 bg-background/90 p-2">
                    <div className="flex items-center justify-between mb-1">
                      <span className="text-[9px] text-muted-foreground">Diff for iteration {expandedIteration}</span>
                      <button onClick={() => setExpandedIteration(null)} className="text-[9px] text-muted-foreground hover:text-foreground">close</button>
                    </div>
                    <pre className="text-[9px] font-mono overflow-auto max-h-[120px] leading-tight">
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

                {/* A/B experiment button for completed code experiments */}
                {currentMode === 'code' && !isStreaming && Boolean(doneData?.can_create_ab_experiment) && (
                  <div className="mt-1 flex items-center gap-2 rounded border border-purple-500/30 bg-purple-500/5 px-2 py-1.5">
                    <FlaskRound className="w-3.5 h-3.5 text-purple-400" />
                    <span className="text-[10px] text-muted-foreground flex-1">
                      Best version v{String(doneData!.best_version)} improved score by +{String(doneData!.improvement)}
                    </span>
                    <Button
                      size="sm"
                      variant="outline"
                      className="h-6 px-2 text-[10px]"
                      disabled={abCreating}
                      onClick={async () => {
                        setAbCreating(true)
                        try {
                          await createAbExperimentFromAutoresearch(String(doneData!.experiment_id))
                          setDoneData(null)
                        } catch { /* ignore */ }
                        setAbCreating(false)
                      }}
                    >
                      {abCreating ? <Loader2 className="w-3 h-3 mr-1 animate-spin" /> : null}
                      Create A/B Experiment
                    </Button>
                  </div>
                )}
              </div>
            </div>
          </div>

          {/* ===== BOTTOM: Parameter Workspace (unchanged) ===== */}
          <div className="min-h-0 rounded-md border border-border/60 bg-background/70 p-2.5">
            <div className="flex h-full min-h-0 flex-col gap-2">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="flex items-center gap-1.5">
                  <p className="text-[11px] font-medium">Parameter Workspace</p>
                  <Badge variant="outline" className="h-4 px-1.5 text-[9px] font-mono">
                    {dynamicStrategyParamSections.reduce((sum, section) => sum + section.fieldKeys.length, 0)} fields
                  </Badge>
                  {tuneDraftDirty ? (
                    <span className="rounded bg-amber-500/15 px-1.5 py-0.5 text-[9px] font-semibold text-amber-500">
                      UNSAVED
                    </span>
                  ) : null}
                </div>
                <div className="flex flex-wrap items-center gap-1.5">
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    className="h-6 px-2 text-[10px]"
                    onClick={() => {
                      applyTraderDraftSettings(trader)
                      setTuneDraftDirty(false)
                    }}
                  >
                    Discard Edits
                  </Button>
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    className="h-6 px-2 text-[10px]"
                    onClick={() => revertTuneParametersMutation.mutate()}
                    disabled={
                      revertTuneParametersMutation.isPending
                      || !tuneRevertSnapshot
                      || tuneRevertSnapshot.traderId !== trader.id
                    }
                  >
                    {revertTuneParametersMutation.isPending ? (
                      <Loader2 className="mr-1 h-3 w-3 animate-spin" />
                    ) : null}
                    Revert Last Applied
                  </Button>
                  <Button
                    type="button"
                    size="sm"
                    className="h-6 px-2 text-[10px]"
                    onClick={() => saveTuneParametersMutation.mutate()}
                    disabled={saveTuneParametersMutation.isPending || !tuneDraftDirty}
                  >
                    {saveTuneParametersMutation.isPending ? (
                      <Loader2 className="mr-1 h-3 w-3 animate-spin" />
                    ) : null}
                    Save Parameters
                  </Button>
                </div>
              </div>
              <p className="text-[10px] text-muted-foreground/80">
                Review and edit grouped strategy controls before committing.
              </p>
              {tuneRevertSnapshot && tuneRevertSnapshot.traderId === trader.id ? (
                <p className="text-[10px] text-muted-foreground/80">
                  Revert snapshot captured at {formatTimestamp(tuneRevertSnapshot.capturedAt)}.
                </p>
              ) : null}
              {tuneSaveError ? <p className="text-[10px] text-red-500">{tuneSaveError}</p> : null}
              {tuneRevertError ? <p className="text-[10px] text-red-500">{tuneRevertError}</p> : null}
              {dynamicStrategyParamSections.length === 0 ? (
                <p className="text-[10px] text-muted-foreground/80">No dynamic parameter fields available for this bot.</p>
              ) : (
                <Tabs
                  value={tuneParamSectionTab}
                  onValueChange={setTuneParamSectionTab}
                  className="flex min-h-0 flex-1 flex-col overflow-hidden"
                >
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
                        <p className="text-[10px] text-muted-foreground/80">No grouped parameter fields are available for this strategy.</p>
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
                            <TabsContent
                              key={`${section.sectionKey}:panel:${group.key}`}
                              value={group.key}
                              className="mt-0 min-h-0 flex-1 overflow-auto rounded-md border border-border/50 bg-background/65 p-2"
                            >
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
        </div>
      </div>
    </div>
  )
}
