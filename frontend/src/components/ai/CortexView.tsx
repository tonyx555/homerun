import { useState, useRef, useCallback } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Brain,
  Play,
  Square,
  Clock,
  Zap,
  BookOpen,
  Settings2,
  Trash2,
  Plus,
  ChevronRight,
  CheckCircle2,
  XCircle,
  AlertTriangle,
  Eye,
  Pencil,
  Power,
  Shield,
  RefreshCw,
} from 'lucide-react'
import { cn } from '../../lib/utils'
import { Card, CardContent, CardHeader, CardTitle } from '../ui/card'
import { Button } from '../ui/button'
import { Badge } from '../ui/badge'
import { Input } from '../ui/input'
import {
  getCortexStatus,
  listCortexRuns,
  getCortexRun,
  getCortexSettings,
  updateCortexSettings,
  listCortexMemories,
  createCortexMemory,
  updateCortexMemory,
  deleteCortexMemory,
  streamCortexRun,
  type CortexRunDetail,
  type CortexSettings,
  type CortexStatus,
  type ChatStreamEvent,
  getLLMModels,
} from '../../services/api'

type Panel = 'runs' | 'memory' | 'settings'

export default function CortexView() {
  const queryClient = useQueryClient()
  const [panel, setPanel] = useState<Panel>('runs')
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null)
  const [isStreaming, setIsStreaming] = useState(false)
  const [streamEvents, setStreamEvents] = useState<ChatStreamEvent[]>([])
  const abortRef = useRef<AbortController | null>(null)

  const { data: status, isLoading: statusLoading } = useQuery({
    queryKey: ['cortex-status'],
    queryFn: getCortexStatus,
    refetchInterval: 15000,
  })

  const handleTriggerRun = useCallback(() => {
    setIsStreaming(true)
    setStreamEvents([])
    setSelectedRunId(null)
    const controller = new AbortController()
    abortRef.current = controller

    streamCortexRun(
      (event) => setStreamEvents(prev => [...prev, event]),
      () => {
        setIsStreaming(false)
        abortRef.current = null
        queryClient.invalidateQueries({ queryKey: ['cortex-status'] })
        queryClient.invalidateQueries({ queryKey: ['cortex-runs'] })
      },
      () => {
        setIsStreaming(false)
        abortRef.current = null
      },
      controller.signal,
    )
  }, [queryClient])

  const handleStopRun = useCallback(() => {
    abortRef.current?.abort()
    setIsStreaming(false)
  }, [])

  return (
    <div className="flex flex-col gap-4">
      {/* Status Bar */}
      <StatusBar status={status} loading={statusLoading} />

      {/* Panel Tabs */}
      <div className="flex gap-2">
        <Button
          variant={panel === 'runs' ? 'default' : 'outline'}
          size="sm"
          onClick={() => setPanel('runs')}
          className={cn(panel === 'runs' && 'bg-orange-500/20 text-orange-300 border-orange-500/30')}
        >
          <Zap className="w-3.5 h-3.5 mr-1.5" /> Runs
        </Button>
        <Button
          variant={panel === 'memory' ? 'default' : 'outline'}
          size="sm"
          onClick={() => setPanel('memory')}
          className={cn(panel === 'memory' && 'bg-purple-500/20 text-purple-300 border-purple-500/30')}
        >
          <BookOpen className="w-3.5 h-3.5 mr-1.5" /> Memory ({status?.memory_count ?? 0})
        </Button>
        <Button
          variant={panel === 'settings' ? 'default' : 'outline'}
          size="sm"
          onClick={() => setPanel('settings')}
          className={cn(panel === 'settings' && 'bg-blue-500/20 text-blue-300 border-blue-500/30')}
        >
          <Settings2 className="w-3.5 h-3.5 mr-1.5" /> Settings
        </Button>
        <div className="flex-1" />
        {isStreaming ? (
          <Button size="sm" variant="destructive" onClick={handleStopRun}>
            <Square className="w-3.5 h-3.5 mr-1.5" /> Stop
          </Button>
        ) : (
          <Button
            size="sm"
            onClick={handleTriggerRun}
            disabled={!status?.enabled}
            className="bg-orange-600 hover:bg-orange-700"
          >
            <Play className="w-3.5 h-3.5 mr-1.5" /> Trigger Run
          </Button>
        )}
      </div>

      {/* Main Content */}
      <div className="min-h-[400px]">
        {panel === 'runs' && (
          <RunsPanel
            selectedRunId={selectedRunId}
            onSelectRun={setSelectedRunId}
            isStreaming={isStreaming}
            streamEvents={streamEvents}
          />
        )}
        {panel === 'memory' && <MemoryPanel />}
        {panel === 'settings' && <SettingsPanel />}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Status Bar
// ---------------------------------------------------------------------------

function StatusBar({ status, loading }: { status?: CortexStatus; loading: boolean }) {
  if (loading || !status) {
    return (
      <div className="flex items-center gap-3 px-4 py-2 rounded-lg bg-white/5 border border-white/10">
        <RefreshCw className="w-4 h-4 animate-spin text-muted-foreground" />
        <span className="text-sm text-muted-foreground">Loading Cortex status...</span>
      </div>
    )
  }

  const lastRun = status.last_run
  const timeSince = lastRun?.finished_at
    ? formatTimeSince(lastRun.finished_at)
    : 'Never'

  return (
    <div className="flex items-center gap-4 px-4 py-2.5 rounded-lg bg-white/5 border border-white/10">
      <div className="flex items-center gap-2">
        <Brain className="w-5 h-5 text-orange-400" />
        <span className="font-semibold text-orange-300">Cortex</span>
      </div>
      <Badge variant={status.enabled ? 'default' : 'outline'} className={cn(
        'text-xs',
        status.enabled ? 'bg-green-500/20 text-green-300 border-green-500/30' : 'text-muted-foreground'
      )}>
        {status.enabled ? 'Enabled' : 'Disabled'}
      </Badge>
      {status.write_actions_enabled && (
        <Badge className="text-xs bg-amber-500/20 text-amber-300 border-amber-500/30">
          Write Actions ON
        </Badge>
      )}
      <div className="text-xs text-muted-foreground flex items-center gap-1">
        <Clock className="w-3 h-3" /> Last run: {timeSince}
      </div>
      <div className="text-xs text-muted-foreground">
        {status.total_runs} total runs
      </div>
      <div className="text-xs text-muted-foreground">
        {status.memory_count} memories
      </div>
      {lastRun && (
        <Badge variant="outline" className={cn(
          'text-xs ml-auto',
          lastRun.status === 'completed' ? 'text-green-400 border-green-500/30' :
          lastRun.status === 'error' ? 'text-red-400 border-red-500/30' : 'text-yellow-400'
        )}>
          {lastRun.status === 'completed' ? <CheckCircle2 className="w-3 h-3 mr-1" /> :
           lastRun.status === 'error' ? <XCircle className="w-3 h-3 mr-1" /> :
           <AlertTriangle className="w-3 h-3 mr-1" />}
          {lastRun.actions_count} actions, {lastRun.learnings_count} learnings
        </Badge>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Runs Panel
// ---------------------------------------------------------------------------

function RunsPanel({
  selectedRunId,
  onSelectRun,
  isStreaming,
  streamEvents,
}: {
  selectedRunId: string | null
  onSelectRun: (id: string | null) => void
  isStreaming: boolean
  streamEvents: ChatStreamEvent[]
}) {
  const { data: runsData } = useQuery({
    queryKey: ['cortex-runs'],
    queryFn: () => listCortexRuns({ limit: 50 }),
    refetchInterval: 10000,
  })

  const { data: runDetail } = useQuery({
    queryKey: ['cortex-run', selectedRunId],
    queryFn: () => getCortexRun(selectedRunId!),
    enabled: !!selectedRunId,
  })

  const runs = runsData?.runs ?? []

  return (
    <div className="flex gap-4">
      {/* Run List */}
      <div className="w-64 shrink-0 overflow-y-auto space-y-1 pr-2">
        {runs.map((run) => (
          <button
            key={run.id}
            onClick={() => onSelectRun(run.id)}
            className={cn(
              'w-full text-left px-3 py-2 rounded-md text-sm transition-colors',
              selectedRunId === run.id
                ? 'bg-orange-500/20 text-orange-200'
                : 'hover:bg-white/5 text-muted-foreground'
            )}
          >
            <div className="flex items-center gap-2">
              {run.status === 'completed' ? (
                <CheckCircle2 className="w-3.5 h-3.5 text-green-400 shrink-0" />
              ) : run.status === 'error' ? (
                <XCircle className="w-3.5 h-3.5 text-red-400 shrink-0" />
              ) : (
                <RefreshCw className="w-3.5 h-3.5 text-yellow-400 animate-spin shrink-0" />
              )}
              <span className="truncate text-xs">
                {run.started_at ? new Date(run.started_at).toLocaleTimeString() : 'Unknown'}
              </span>
              <Badge variant="outline" className="text-[10px] ml-auto shrink-0">
                {run.trigger}
              </Badge>
            </div>
            <div className="text-[11px] text-muted-foreground mt-0.5 flex gap-2">
              <span>{run.actions_count} actions</span>
              <span>{run.learnings_count} learnings</span>
            </div>
          </button>
        ))}
        {runs.length === 0 && !isStreaming && (
          <div className="text-sm text-muted-foreground text-center py-8">No runs yet</div>
        )}
      </div>

      {/* Run Detail / Stream View */}
      <div className="flex-1 overflow-y-auto">
        {isStreaming ? (
          <StreamView events={streamEvents} />
        ) : runDetail ? (
          <RunDetailView run={runDetail} />
        ) : (
          <div className="flex items-center justify-center h-full text-muted-foreground text-sm">
            Select a run or trigger one
          </div>
        )}
      </div>
    </div>
  )
}

function StreamView({ events }: { events: ChatStreamEvent[] }) {
  return (
    <div className="space-y-2">
      <div className="flex items-center gap-2 text-orange-400 text-sm font-medium mb-3">
        <Brain className="w-4 h-4 animate-pulse" /> Live Run
      </div>
      {events.map((evt, i) => (
        <EventCard key={i} event={evt} />
      ))}
      {events.length === 0 && (
        <div className="text-sm text-muted-foreground animate-pulse">Starting Cortex cycle...</div>
      )}
    </div>
  )
}

function RunDetailView({ run }: { run: CortexRunDetail }) {
  return (
    <div className="space-y-3">
      {/* Header */}
      <div className="flex items-center gap-3">
        <Badge className={cn(
          'text-xs',
          run.status === 'completed' ? 'bg-green-500/20 text-green-300' :
          run.status === 'error' ? 'bg-red-500/20 text-red-300' : 'bg-yellow-500/20 text-yellow-300'
        )}>
          {run.status}
        </Badge>
        <span className="text-xs text-muted-foreground">
          {run.started_at && new Date(run.started_at).toLocaleString()}
        </span>
        {run.model_used && (
          <Badge variant="outline" className="text-[10px]">{run.model_used}</Badge>
        )}
        {run.tokens_used && (
          <span className="text-[11px] text-muted-foreground">{run.tokens_used.toLocaleString()} tokens</span>
        )}
      </div>

      {/* Summary */}
      {run.summary && (
        <Card className="border-white/10 bg-white/5">
          <CardContent className="p-3">
            <div className="text-sm whitespace-pre-wrap">{run.summary}</div>
          </CardContent>
        </Card>
      )}

      {/* Actions */}
      {run.actions_taken.length > 0 && (
        <div>
          <h4 className="text-xs font-medium text-muted-foreground mb-2 uppercase tracking-wider">
            Actions ({run.actions_taken.length})
          </h4>
          <div className="space-y-1.5">
            {run.actions_taken.map((action, i) => (
              <ActionCard key={i} action={action} />
            ))}
          </div>
        </div>
      )}

      {/* Learnings */}
      {run.learnings_saved.length > 0 && (
        <div>
          <h4 className="text-xs font-medium text-muted-foreground mb-2 uppercase tracking-wider">
            Learnings Saved ({run.learnings_saved.length})
          </h4>
          <div className="space-y-1">
            {run.learnings_saved.map((l, i) => (
              <div key={i} className="px-3 py-2 rounded bg-purple-500/10 border border-purple-500/20 text-sm">
                <Badge variant="outline" className="text-[10px] mr-2">{l.category}</Badge>
                {l.content}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Thinking Log (collapsible) */}
      {run.thinking_log && <ThinkingLog log={run.thinking_log} />}
    </div>
  )
}

function EventCard({ event }: { event: ChatStreamEvent }) {
  const { event: type, data } = event
  if (type === 'thinking') {
    return (
      <div className="px-3 py-1.5 rounded bg-blue-500/10 border border-blue-500/15 text-sm text-blue-200">
        <Eye className="w-3 h-3 inline mr-1.5 opacity-60" />
        {String(data?.content || '')}
      </div>
    )
  }
  if (type === 'tool_start') {
    return (
      <div className="px-3 py-1.5 rounded bg-cyan-500/10 border border-cyan-500/15 text-sm">
        <Zap className="w-3 h-3 inline mr-1.5 text-cyan-400" />
        <span className="font-mono text-cyan-300">{String(data?.tool || '')}</span>
        <span className="text-muted-foreground ml-2 text-xs">
          {JSON.stringify(data?.input || {}).slice(0, 100)}
        </span>
      </div>
    )
  }
  if (type === 'tool_end') {
    const output = data?.output as Record<string, unknown> | undefined
    const preview = JSON.stringify(output || {}).slice(0, 150)
    return (
      <div className="px-3 py-1.5 rounded bg-green-500/10 border border-green-500/15 text-xs text-green-300">
        <CheckCircle2 className="w-3 h-3 inline mr-1.5" />
        {preview}
      </div>
    )
  }
  if (type === 'tool_error') {
    return (
      <div className="px-3 py-1.5 rounded bg-red-500/10 border border-red-500/15 text-xs text-red-300">
        <XCircle className="w-3 h-3 inline mr-1.5" />
        {String(data?.error || 'Error')}
      </div>
    )
  }
  if (type === 'done') {
    const result = data?.result as Record<string, unknown> | undefined
    return (
      <div className="px-3 py-2 rounded bg-orange-500/10 border border-orange-500/20 text-sm">
        <Brain className="w-3.5 h-3.5 inline mr-1.5 text-orange-400" />
        <span className="font-medium text-orange-300">Cycle Complete</span>
        {result?.answer ? (
          <div className="mt-2 text-sm whitespace-pre-wrap">{String(result.answer)}</div>
        ) : null}
      </div>
    )
  }
  return null
}

function ActionCard({ action }: { action: { tool: string; input: Record<string, unknown>; output: Record<string, unknown> | null } }) {
  const isWrite = ['cortex_pause_trader', 'cortex_enable_strategy', 'cortex_update_risk_clamps', 'update_strategy_config'].includes(action.tool)

  return (
    <div className={cn(
      'px-3 py-2 rounded border text-sm',
      isWrite ? 'bg-amber-500/10 border-amber-500/20' : 'bg-white/5 border-white/10'
    )}>
      <div className="flex items-center gap-2">
        {isWrite ? <Shield className="w-3.5 h-3.5 text-amber-400" /> : <Eye className="w-3.5 h-3.5 text-cyan-400" />}
        <span className="font-mono text-xs">{action.tool}</span>
        {action.input?.reason ? (
          <span className="text-xs text-muted-foreground ml-auto">{String(action.input.reason)}</span>
        ) : null}
      </div>
    </div>
  )
}

function ThinkingLog({ log }: { log: string }) {
  const [expanded, setExpanded] = useState(false)
  return (
    <div>
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-1 text-xs text-muted-foreground hover:text-white transition-colors"
      >
        <ChevronRight className={cn('w-3 h-3 transition-transform', expanded && 'rotate-90')} />
        Thinking Log
      </button>
      {expanded && (
        <pre className="mt-2 p-3 rounded bg-black/30 border border-white/10 text-xs text-muted-foreground whitespace-pre-wrap max-h-64 overflow-y-auto">
          {log}
        </pre>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Memory Panel
// ---------------------------------------------------------------------------

function MemoryPanel() {
  const queryClient = useQueryClient()
  const [categoryFilter, setCategoryFilter] = useState<string>('')
  const [showExpired, setShowExpired] = useState(false)
  const [editingId, setEditingId] = useState<string | null>(null)
  const [editContent, setEditContent] = useState('')
  const [editImportance, setEditImportance] = useState(0.5)
  const [newContent, setNewContent] = useState('')
  const [newCategory, setNewCategory] = useState('observation')
  const [showAdd, setShowAdd] = useState(false)

  const { data } = useQuery({
    queryKey: ['cortex-memory', categoryFilter, showExpired],
    queryFn: () => listCortexMemories({
      limit: 100,
      category: categoryFilter || undefined,
      include_expired: showExpired,
    }),
    refetchInterval: 30000,
  })

  const deleteMutation = useMutation({
    mutationFn: deleteCortexMemory,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['cortex-memory'] }),
  })

  const updateMutation = useMutation({
    mutationFn: ({ id, updates }: { id: string; updates: Parameters<typeof updateCortexMemory>[1] }) =>
      updateCortexMemory(id, updates),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['cortex-memory'] })
      setEditingId(null)
    },
  })

  const createMutation = useMutation({
    mutationFn: createCortexMemory,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['cortex-memory'] })
      setNewContent('')
      setShowAdd(false)
    },
  })

  const memories = data?.memories ?? []
  const categories = ['observation', 'lesson', 'rule', 'preference']
  const categoryColors: Record<string, string> = {
    observation: 'bg-blue-500/20 text-blue-300 border-blue-500/30',
    lesson: 'bg-green-500/20 text-green-300 border-green-500/30',
    rule: 'bg-amber-500/20 text-amber-300 border-amber-500/30',
    preference: 'bg-purple-500/20 text-purple-300 border-purple-500/30',
  }

  return (
    <div className="space-y-3 h-full overflow-y-auto">
      {/* Filters */}
      <div className="flex items-center gap-2">
        <select
          value={categoryFilter}
          onChange={(e) => setCategoryFilter(e.target.value)}
          className="px-2 py-1 rounded bg-white/5 border border-white/10 text-sm"
        >
          <option value="">All Categories</option>
          {categories.map(c => <option key={c} value={c}>{c}</option>)}
        </select>
        <label className="flex items-center gap-1.5 text-xs text-muted-foreground">
          <input
            type="checkbox"
            checked={showExpired}
            onChange={(e) => setShowExpired(e.target.checked)}
            className="rounded"
          />
          Show expired
        </label>
        <div className="flex-1" />
        <Button size="sm" variant="outline" onClick={() => setShowAdd(!showAdd)}>
          <Plus className="w-3.5 h-3.5 mr-1" /> Add Memory
        </Button>
      </div>

      {/* Add Form */}
      {showAdd && (
        <Card className="border-white/10 bg-white/5">
          <CardContent className="p-3 space-y-2">
            <textarea
              value={newContent}
              onChange={(e) => setNewContent(e.target.value)}
              placeholder="Memory content..."
              className="w-full px-3 py-2 rounded bg-black/20 border border-white/10 text-sm min-h-[60px]"
            />
            <div className="flex gap-2 items-center">
              <select
                value={newCategory}
                onChange={(e) => setNewCategory(e.target.value)}
                className="px-2 py-1 rounded bg-white/5 border border-white/10 text-sm"
              >
                {categories.map(c => <option key={c} value={c}>{c}</option>)}
              </select>
              <Button
                size="sm"
                onClick={() => createMutation.mutate({ content: newContent, category: newCategory })}
                disabled={!newContent.trim() || createMutation.isPending}
              >
                Save
              </Button>
              <Button size="sm" variant="ghost" onClick={() => setShowAdd(false)}>Cancel</Button>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Memory List */}
      <div className="space-y-1.5">
        {memories.map((m) => (
          <div
            key={m.id}
            className={cn(
              'px-3 py-2 rounded border text-sm',
              m.expired ? 'opacity-40 bg-white/3 border-white/5' : 'bg-white/5 border-white/10'
            )}
          >
            {editingId === m.id ? (
              <div className="space-y-2">
                <textarea
                  value={editContent}
                  onChange={(e) => setEditContent(e.target.value)}
                  className="w-full px-2 py-1 rounded bg-black/20 border border-white/10 text-sm min-h-[50px]"
                />
                <div className="flex gap-2 items-center">
                  <label className="text-xs text-muted-foreground">Importance:</label>
                  <Input
                    type="number"
                    min={0} max={1} step={0.1}
                    value={editImportance}
                    onChange={(e) => setEditImportance(Number(e.target.value))}
                    className="w-20 h-7 text-xs"
                  />
                  <Button size="sm" onClick={() => updateMutation.mutate({
                    id: m.id,
                    updates: { content: editContent, importance: editImportance },
                  })}>
                    Save
                  </Button>
                  <Button size="sm" variant="ghost" onClick={() => setEditingId(null)}>Cancel</Button>
                </div>
              </div>
            ) : (
              <>
                <div className="flex items-start gap-2">
                  <Badge variant="outline" className={cn('text-[10px] shrink-0 mt-0.5', categoryColors[m.category])}>
                    {m.category}
                  </Badge>
                  <span className="flex-1">{m.content}</span>
                  <div className="flex items-center gap-1 shrink-0">
                    <span className="text-[10px] text-muted-foreground">{m.importance.toFixed(1)}</span>
                    <Button
                      size="sm"
                      variant="ghost"
                      className="h-6 w-6 p-0"
                      onClick={() => { setEditingId(m.id); setEditContent(m.content); setEditImportance(m.importance) }}
                    >
                      <Pencil className="w-3 h-3" />
                    </Button>
                    <Button
                      size="sm"
                      variant="ghost"
                      className="h-6 w-6 p-0 text-red-400 hover:text-red-300"
                      onClick={() => deleteMutation.mutate(m.id)}
                    >
                      <Trash2 className="w-3 h-3" />
                    </Button>
                  </div>
                </div>
                <div className="text-[10px] text-muted-foreground mt-1">
                  Accessed {m.access_count}x | {m.updated_at && new Date(m.updated_at).toLocaleDateString()}
                </div>
              </>
            )}
          </div>
        ))}
        {memories.length === 0 && (
          <div className="text-sm text-muted-foreground text-center py-8">
            No memories yet. Cortex will learn over time.
          </div>
        )}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Settings Panel
// ---------------------------------------------------------------------------

function SettingsPanel() {
  const queryClient = useQueryClient()
  const { data: settings, isLoading } = useQuery({
    queryKey: ['cortex-settings'],
    queryFn: getCortexSettings,
  })

  const { data: modelsData } = useQuery({
    queryKey: ['llm-models'],
    queryFn: () => getLLMModels(),
  })

  const [form, setForm] = useState<Partial<CortexSettings>>({})
  const [dirty, setDirty] = useState(false)

  // Sync form from settings
  const currentSettings = { ...settings, ...form }

  const updateField = <K extends keyof CortexSettings>(key: K, value: CortexSettings[K]) => {
    setForm(prev => ({ ...prev, [key]: value }))
    setDirty(true)
  }

  const saveMutation = useMutation({
    mutationFn: () => updateCortexSettings(form),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['cortex-settings'] })
      queryClient.invalidateQueries({ queryKey: ['cortex-status'] })
      setForm({})
      setDirty(false)
    },
  })

  if (isLoading) return <div className="text-sm text-muted-foreground">Loading settings...</div>

  // Flatten all models from all providers
  const allModels: { id: string; name: string }[] = []
  if (modelsData?.models) {
    for (const providerModels of Object.values(modelsData.models)) {
      for (const m of providerModels as Array<{ id: string; name: string }>) {
        allModels.push(m)
      }
    }
  }

  return (
    <div className="space-y-4 max-w-2xl">
      <Card className="border-white/10 bg-white/5">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm flex items-center gap-2">
            <Power className="w-4 h-4 text-orange-400" /> Core
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <ToggleRow
            label="Enable Cortex"
            description="Start the autonomous fleet management agent"
            checked={currentSettings.enabled ?? false}
            onChange={(v) => updateField('enabled', v)}
          />
          <ToggleRow
            label="Write Actions"
            description="Allow Cortex to pause traders, enable/disable strategies, adjust risk clamps"
            checked={currentSettings.write_actions_enabled ?? false}
            onChange={(v) => updateField('write_actions_enabled', v)}
          />
          <ToggleRow
            label="Telegram Notifications"
            description="Send Telegram message when Cortex takes write actions"
            checked={currentSettings.notify_telegram ?? false}
            onChange={(v) => updateField('notify_telegram', v)}
          />
        </CardContent>
      </Card>

      <Card className="border-white/10 bg-white/5">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm flex items-center gap-2">
            <Settings2 className="w-4 h-4 text-blue-400" /> Configuration
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="text-xs text-muted-foreground block mb-1">Model</label>
              <select
                value={currentSettings.model ?? ''}
                onChange={(e) => updateField('model', e.target.value || null)}
                className="w-full px-2 py-1.5 rounded bg-black/20 border border-white/10 text-sm"
              >
                <option value="">Default (AI Settings)</option>
                {allModels.map(m => <option key={m.id} value={m.id}>{m.name || m.id}</option>)}
              </select>
            </div>
            <div>
              <label className="text-xs text-muted-foreground block mb-1">Run Interval (seconds)</label>
              <Input
                type="number"
                min={30} max={3600}
                value={currentSettings.interval_seconds ?? 300}
                onChange={(e) => updateField('interval_seconds', Number(e.target.value))}
                className="h-8 text-sm"
              />
            </div>
            <div>
              <label className="text-xs text-muted-foreground block mb-1">Max Iterations</label>
              <Input
                type="number"
                min={1} max={50}
                value={currentSettings.max_iterations ?? 15}
                onChange={(e) => updateField('max_iterations', Number(e.target.value))}
                className="h-8 text-sm"
              />
            </div>
            <div>
              <label className="text-xs text-muted-foreground block mb-1">Temperature</label>
              <Input
                type="number"
                min={0} max={2} step={0.05}
                value={currentSettings.temperature ?? 0.1}
                onChange={(e) => updateField('temperature', Number(e.target.value))}
                className="h-8 text-sm"
              />
            </div>
            <div>
              <label className="text-xs text-muted-foreground block mb-1">Memory Limit (per run)</label>
              <Input
                type="number"
                min={1} max={100}
                value={currentSettings.memory_limit ?? 20}
                onChange={(e) => updateField('memory_limit', Number(e.target.value))}
                className="h-8 text-sm"
              />
            </div>
          </div>
        </CardContent>
      </Card>

      <Card className="border-white/10 bg-white/5">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm flex items-center gap-2">
            <Brain className="w-4 h-4 text-purple-400" /> Mandate (System Prompt)
          </CardTitle>
        </CardHeader>
        <CardContent>
          <textarea
            value={currentSettings.mandate ?? ''}
            onChange={(e) => updateField('mandate', e.target.value || null)}
            placeholder="Leave empty for default mandate. Customize to change Cortex's priorities and behavior..."
            className="w-full px-3 py-2 rounded bg-black/20 border border-white/10 text-sm min-h-[120px] font-mono"
          />
        </CardContent>
      </Card>

      {dirty && (
        <div className="flex items-center gap-3">
          <Button onClick={() => saveMutation.mutate()} disabled={saveMutation.isPending} className="bg-orange-600 hover:bg-orange-700">
            {saveMutation.isPending ? <RefreshCw className="w-3.5 h-3.5 mr-1.5 animate-spin" /> : null}
            Save Settings
          </Button>
          <Button variant="ghost" onClick={() => { setForm({}); setDirty(false) }}>Discard</Button>
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Shared Components
// ---------------------------------------------------------------------------

function ToggleRow({
  label,
  description,
  checked,
  onChange,
}: {
  label: string
  description: string
  checked: boolean
  onChange: (v: boolean) => void
}) {
  return (
    <div className="flex items-center justify-between">
      <div>
        <div className="text-sm font-medium">{label}</div>
        <div className="text-xs text-muted-foreground">{description}</div>
      </div>
      <button
        onClick={() => onChange(!checked)}
        className={cn(
          'relative w-10 h-5 rounded-full transition-colors',
          checked ? 'bg-orange-500' : 'bg-white/10'
        )}
      >
        <div className={cn(
          'absolute top-0.5 w-4 h-4 rounded-full bg-white transition-transform',
          checked ? 'translate-x-5' : 'translate-x-0.5'
        )} />
      </button>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

function formatTimeSince(iso: string): string {
  const now = Date.now()
  const then = new Date(iso).getTime()
  const seconds = Math.floor((now - then) / 1000)
  if (seconds < 60) return `${seconds}s ago`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  return `${Math.floor(hours / 24)}d ago`
}
