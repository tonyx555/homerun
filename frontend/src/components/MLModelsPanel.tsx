import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Archive,
  Brain,
  ChevronDown,
  ChevronRight,
  CircleDot,
  Database,
  Loader2,
  Play,
  Trash2,
  TrendingUp,
  Zap,
} from 'lucide-react'
import { Badge } from './ui/badge'
import { Button } from './ui/button'
import { Input } from './ui/input'
import { Label } from './ui/label'
import { ScrollArea } from './ui/scroll-area'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './ui/select'
import { Switch } from './ui/switch'
import { cn } from '../lib/utils'
import {
  getMLRecorderConfig,
  updateMLRecorderConfig,
  getMLDataStats,
  pruneMLData,
  startMLTraining,
  getMLTrainingJobs,
  getMLTrainingJob,
  getMLModels,
  promoteMLModel,
  archiveMLModel,
  deleteMLModel,
  type MLRecorderStats,
  type MLTrainingJob,
  type MLTrainedModel,
  type MLDataStats,
} from '../services/api'

// ==================== Sub-components ====================

function StatCard({ label, value, sub }: { label: string; value: string | number; sub?: string }) {
  return (
    <div className="rounded-lg border border-border/50 bg-card/50 p-3">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="text-lg font-semibold mt-0.5">{value}</div>
      {sub && <div className="text-xs text-muted-foreground mt-0.5">{sub}</div>}
    </div>
  )
}

function MetricBadge({ value, label, good }: { value: number | null; label: string; good?: number }) {
  if (value == null) return null
  const pct = (value * 100).toFixed(1)
  const isGood = good != null && value >= good
  return (
    <span
      className={cn(
        'inline-flex items-center gap-1 rounded-md px-2 py-0.5 text-xs font-medium',
        isGood ? 'bg-emerald-500/10 text-emerald-400' : 'bg-amber-500/10 text-amber-400'
      )}
    >
      {label}: {pct}%
    </span>
  )
}

function FeatureImportanceChart({ importance }: { importance: Record<string, number> | null }) {
  if (!importance) return null
  const sorted = Object.entries(importance)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
  const maxVal = Math.max(...sorted.map(([, v]) => v), 0.001)
  return (
    <div className="space-y-1.5">
      <div className="text-xs font-medium text-muted-foreground mb-2">Feature Importance (top 10)</div>
      {sorted.map(([name, val]) => (
        <div key={name} className="flex items-center gap-2 text-xs">
          <span className="w-32 truncate text-muted-foreground text-right">{name}</span>
          <div className="flex-1 h-3 bg-muted/30 rounded-full overflow-hidden">
            <div
              className="h-full bg-blue-500/60 rounded-full"
              style={{ width: `${(val / maxVal) * 100}%` }}
            />
          </div>
          <span className="w-10 text-right text-muted-foreground">{(val * 100).toFixed(0)}%</span>
        </div>
      ))}
    </div>
  )
}

// ==================== Data Recording Tab ====================

function DataRecordingTab() {
  const queryClient = useQueryClient()

  const { data: recorderStats, isLoading } = useQuery<MLRecorderStats>({
    queryKey: ['ml-recorder-config'],
    queryFn: getMLRecorderConfig,
    refetchInterval: 10000,
  })

  const { data: dataStats } = useQuery<MLDataStats>({
    queryKey: ['ml-data-stats'],
    queryFn: getMLDataStats,
    refetchInterval: 30000,
  })

  const toggleMutation = useMutation({
    mutationFn: (recording: boolean) => updateMLRecorderConfig({ is_recording: recording }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['ml-recorder-config'] }),
  })

  const updateMutation = useMutation({
    mutationFn: (config: Parameters<typeof updateMLRecorderConfig>[0]) => updateMLRecorderConfig(config),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['ml-recorder-config'] }),
  })

  const pruneMutation = useMutation({
    mutationFn: () => pruneMLData(undefined, true),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['ml-data-stats'] })
      queryClient.invalidateQueries({ queryKey: ['ml-recorder-config'] })
    },
  })

  const config = recorderStats?.config
  const isRecording = config?.is_recording ?? false

  const [interval, setInterval_] = useState('')
  const [retention, setRetention] = useState('')

  useEffect(() => {
    if (config) {
      setInterval_(String(config.interval_seconds))
      setRetention(String(config.retention_days))
    }
  }, [config])

  if (isLoading) {
    return (
      <div className="flex items-center justify-center p-8">
        <Loader2 className="w-5 h-5 animate-spin text-muted-foreground" />
      </div>
    )
  }

  return (
    <div className="space-y-6 p-4">
      {/* Recording toggle */}
      <div className="flex items-center justify-between rounded-lg border border-border/50 bg-card/30 p-4">
        <div className="flex items-center gap-3">
          <div className={cn('w-2.5 h-2.5 rounded-full', isRecording ? 'bg-red-500 animate-pulse' : 'bg-muted')}>
          </div>
          <div>
            <div className="font-medium">Data Recording</div>
            <div className="text-xs text-muted-foreground">
              {isRecording ? 'Recording live market snapshots' : 'Recording paused'}
            </div>
          </div>
        </div>
        <Switch
          checked={isRecording}
          onCheckedChange={(checked) => toggleMutation.mutate(checked)}
          disabled={toggleMutation.isPending}
        />
      </div>

      {/* Stats grid */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <StatCard
          label="Total Snapshots"
          value={recorderStats?.total_snapshots?.toLocaleString() ?? '0'}
        />
        <StatCard
          label="Oldest"
          value={
            recorderStats?.oldest_snapshot
              ? new Date(recorderStats.oldest_snapshot).toLocaleDateString()
              : '—'
          }
        />
        <StatCard
          label="Newest"
          value={
            recorderStats?.newest_snapshot
              ? new Date(recorderStats.newest_snapshot).toLocaleString()
              : '—'
          }
        />
        <StatCard
          label="Est. Size"
          value={`~${Math.round(((recorderStats?.total_snapshots ?? 0) * 200) / 1024 / 1024)} MB`}
          sub="~200 bytes/row"
        />
      </div>

      {/* Per-asset breakdown */}
      {dataStats?.groups && dataStats.groups.length > 0 && (
        <div>
          <div className="text-xs font-medium text-muted-foreground mb-2">Snapshots by Asset/Timeframe</div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
            {dataStats.groups.map((g) => (
              <div
                key={`${g.asset}-${g.timeframe}`}
                className="flex items-center justify-between rounded-md border border-border/30 px-3 py-1.5 text-xs"
              >
                <span className="font-medium uppercase">
                  {g.asset}/{g.timeframe}
                </span>
                <span className="text-muted-foreground">{g.count.toLocaleString()}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Config */}
      <div className="space-y-3">
        <div className="text-xs font-medium text-muted-foreground">Recording Settings</div>
        <div className="grid grid-cols-2 gap-3">
          <div>
            <Label className="text-xs">Interval (seconds)</Label>
            <div className="flex gap-2 mt-1">
              <Input
                className="h-8 text-xs"
                type="number"
                min={5}
                max={3600}
                value={interval}
                onChange={(e) => setInterval_(e.target.value)}
              />
              <Button
                size="sm"
                variant="outline"
                className="h-8 text-xs"
                onClick={() => updateMutation.mutate({ interval_seconds: Number(interval) })}
                disabled={updateMutation.isPending}
              >
                Set
              </Button>
            </div>
          </div>
          <div>
            <Label className="text-xs">Retention (days)</Label>
            <div className="flex gap-2 mt-1">
              <Input
                className="h-8 text-xs"
                type="number"
                min={1}
                max={365}
                value={retention}
                onChange={(e) => setRetention(e.target.value)}
              />
              <Button
                size="sm"
                variant="outline"
                className="h-8 text-xs"
                onClick={() => updateMutation.mutate({ retention_days: Number(retention) })}
                disabled={updateMutation.isPending}
              >
                Set
              </Button>
            </div>
          </div>
        </div>
        <div className="flex gap-2 mt-2">
          <Button
            size="sm"
            variant="outline"
            className="text-xs"
            onClick={() => pruneMutation.mutate()}
            disabled={pruneMutation.isPending}
          >
            {pruneMutation.isPending ? <Loader2 className="w-3 h-3 animate-spin mr-1" /> : <Trash2 className="w-3 h-3 mr-1" />}
            Prune Old Data
          </Button>
        </div>
      </div>
    </div>
  )
}

// ==================== Training Tab ====================

function TrainingTab() {
  const queryClient = useQueryClient()
  const [modelType, setModelType] = useState('xgboost')
  const [daysLookback, setDaysLookback] = useState('30')
  const [pollingJobId, setPollingJobId] = useState<string | null>(null)

  const { data: jobs = [] } = useQuery<MLTrainingJob[]>({
    queryKey: ['ml-training-jobs'],
    queryFn: () => getMLTrainingJobs(),
    refetchInterval: 5000,
  })

  // Poll active job
  const { data: activeJob } = useQuery({
    queryKey: ['ml-training-job', pollingJobId],
    queryFn: () => (pollingJobId ? getMLTrainingJob(pollingJobId) : null),
    enabled: !!pollingJobId,
    refetchInterval: 2000,
  })

  useEffect(() => {
    if (activeJob && (activeJob.status === 'completed' || activeJob.status === 'failed')) {
      setPollingJobId(null)
      queryClient.invalidateQueries({ queryKey: ['ml-training-jobs'] })
      queryClient.invalidateQueries({ queryKey: ['ml-models'] })
    }
  }, [activeJob, queryClient])

  const trainMutation = useMutation({
    mutationFn: () =>
      startMLTraining({
        model_type: modelType,
        days_lookback: Number(daysLookback),
      }),
    onSuccess: (result) => {
      setPollingJobId(result.job_id)
      queryClient.invalidateQueries({ queryKey: ['ml-training-jobs'] })
    },
  })

  const runningJob = activeJob?.status === 'running' ? activeJob : jobs.find((j) => j.status === 'running')

  return (
    <div className="space-y-6 p-4">
      {/* Train controls */}
      <div className="rounded-lg border border-border/50 bg-card/30 p-4 space-y-3">
        <div className="flex items-center gap-2">
          <Brain className="w-4 h-4 text-blue-400" />
          <span className="font-medium text-sm">Train New Model</span>
        </div>
        <div className="grid grid-cols-3 gap-3">
          <div>
            <Label className="text-xs">Model Type</Label>
            <Select value={modelType} onValueChange={setModelType}>
              <SelectTrigger className="h-8 text-xs mt-1">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="xgboost">XGBoost</SelectItem>
                <SelectItem value="lightgbm">LightGBM</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div>
            <Label className="text-xs">Lookback (days)</Label>
            <Input
              className="h-8 text-xs mt-1"
              type="number"
              min={1}
              max={365}
              value={daysLookback}
              onChange={(e) => setDaysLookback(e.target.value)}
            />
          </div>
          <div className="flex items-end">
            <Button
              size="sm"
              className="w-full text-xs"
              onClick={() => trainMutation.mutate()}
              disabled={trainMutation.isPending || !!runningJob}
            >
              {runningJob ? (
                <Loader2 className="w-3 h-3 animate-spin mr-1" />
              ) : (
                <Play className="w-3 h-3 mr-1" />
              )}
              {runningJob ? 'Training...' : 'Train Now'}
            </Button>
          </div>
        </div>

        {/* Active job progress */}
        {runningJob && (
          <div className="rounded-md bg-blue-500/5 border border-blue-500/20 p-3 space-y-2">
            <div className="flex justify-between text-xs">
              <span className="text-blue-400">{runningJob.message || 'Training...'}</span>
              <span className="text-muted-foreground">{Math.round((runningJob.progress || 0) * 100)}%</span>
            </div>
            <div className="w-full h-1.5 bg-muted/30 rounded-full overflow-hidden">
              <div
                className="h-full bg-blue-500 rounded-full transition-all duration-500"
                style={{ width: `${(runningJob.progress || 0) * 100}%` }}
              />
            </div>
          </div>
        )}
      </div>

      {/* Job history */}
      <div>
        <div className="text-xs font-medium text-muted-foreground mb-2">Training History</div>
        <div className="space-y-2">
          {jobs.length === 0 && (
            <div className="text-xs text-muted-foreground text-center py-4">No training jobs yet</div>
          )}
          {jobs.slice(0, 10).map((job) => (
            <div
              key={job.id}
              className="flex items-center justify-between rounded-md border border-border/30 px-3 py-2 text-xs"
            >
              <div className="flex items-center gap-2">
                <Badge
                  variant="outline"
                  className={cn(
                    'text-[10px]',
                    job.status === 'completed' && 'border-emerald-500/30 text-emerald-400',
                    job.status === 'running' && 'border-blue-500/30 text-blue-400',
                    job.status === 'failed' && 'border-red-500/30 text-red-400',
                    job.status === 'queued' && 'border-amber-500/30 text-amber-400'
                  )}
                >
                  {job.status}
                </Badge>
                <span className="font-medium uppercase">{job.model_type}</span>
              </div>
              <div className="flex items-center gap-3">
                {job.result_summary && (
                  <>
                    <MetricBadge value={job.result_summary.test_accuracy} label="Acc" good={0.6} />
                    <MetricBadge value={job.result_summary.test_auc} label="AUC" good={0.65} />
                  </>
                )}
                <span className="text-muted-foreground">
                  {job.created_at ? new Date(job.created_at).toLocaleString() : ''}
                </span>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

// ==================== Models Tab ====================

function ModelsTab() {
  const queryClient = useQueryClient()
  const [expandedModel, setExpandedModel] = useState<string | null>(null)

  const { data: models = [] } = useQuery<MLTrainedModel[]>({
    queryKey: ['ml-models'],
    queryFn: () => getMLModels(),
    refetchInterval: 10000,
  })

  const promoteMutation = useMutation({
    mutationFn: promoteMLModel,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['ml-models'] }),
  })

  const archiveMutation = useMutation({
    mutationFn: archiveMLModel,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['ml-models'] }),
  })

  const deleteMutation = useMutation({
    mutationFn: deleteMLModel,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['ml-models'] }),
  })

  return (
    <div className="space-y-4 p-4">
      {models.length === 0 && (
        <div className="text-center py-8 text-sm text-muted-foreground">
          No trained models yet. Record some data and train a model to get started.
        </div>
      )}
      {models.map((model) => {
        const expanded = expandedModel === model.id
        return (
          <div key={model.id} className="rounded-lg border border-border/50 bg-card/30 overflow-hidden">
            {/* Header */}
            <div
              className="flex items-center justify-between px-4 py-3 cursor-pointer hover:bg-muted/10"
              onClick={() => setExpandedModel(expanded ? null : model.id)}
            >
              <div className="flex items-center gap-3">
                {expanded ? (
                  <ChevronDown className="w-3.5 h-3.5 text-muted-foreground" />
                ) : (
                  <ChevronRight className="w-3.5 h-3.5 text-muted-foreground" />
                )}
                <Badge
                  variant="outline"
                  className={cn(
                    'text-[10px]',
                    model.status === 'active' && 'border-emerald-500/30 text-emerald-400 bg-emerald-500/5',
                    model.status === 'trained' && 'border-blue-500/30 text-blue-400',
                    model.status === 'archived' && 'border-muted text-muted-foreground'
                  )}
                >
                  {model.status === 'active' && <CircleDot className="w-2.5 h-2.5 mr-1" />}
                  {model.status}
                </Badge>
                <span className="font-medium text-sm">{model.name}</span>
                <span className="text-xs text-muted-foreground">v{model.version}</span>
              </div>
              <div className="flex items-center gap-2">
                <MetricBadge value={model.test_accuracy} label="Acc" good={0.6} />
                <MetricBadge value={model.test_auc} label="AUC" good={0.65} />
                <span className="text-xs text-muted-foreground ml-2">
                  {model.train_samples + model.test_samples} samples
                </span>
              </div>
            </div>

            {/* Expanded details */}
            {expanded && (
              <div className="border-t border-border/30 px-4 py-4 space-y-4">
                {/* Metrics row */}
                <div className="grid grid-cols-4 gap-3">
                  <StatCard
                    label="Train Accuracy"
                    value={model.train_accuracy != null ? `${(model.train_accuracy * 100).toFixed(1)}%` : '—'}
                  />
                  <StatCard
                    label="Test Accuracy"
                    value={model.test_accuracy != null ? `${(model.test_accuracy * 100).toFixed(1)}%` : '—'}
                  />
                  <StatCard
                    label="Test AUC"
                    value={model.test_auc != null ? model.test_auc.toFixed(3) : '—'}
                  />
                  <StatCard
                    label="Date Range"
                    value={
                      model.training_date_range?.start
                        ? `${new Date(model.training_date_range.start).toLocaleDateString()} → ${new Date(model.training_date_range.end!).toLocaleDateString()}`
                        : '—'
                    }
                  />
                </div>

                {/* Feature importance */}
                <FeatureImportanceChart importance={model.feature_importance} />

                {/* Walk-forward results */}
                {model.walkforward_results && model.walkforward_results.length > 0 && (
                  <div>
                    <div className="text-xs font-medium text-muted-foreground mb-2">Walk-Forward Validation</div>
                    <div className="grid grid-cols-5 gap-1.5 text-[10px]">
                      <div className="font-medium text-muted-foreground">Fold</div>
                      <div className="font-medium text-muted-foreground">Train</div>
                      <div className="font-medium text-muted-foreground">Test</div>
                      <div className="font-medium text-muted-foreground">AUC</div>
                      <div className="font-medium text-muted-foreground">Asset</div>
                      {model.walkforward_results
                        .filter((f: any) => !f.error)
                        .slice(0, 10)
                        .map((fold: any, i: number) => (
                          <>
                            <div key={`f-${i}`}>{fold.fold}</div>
                            <div key={`ta-${i}`}>{fold.train_accuracy != null ? `${(fold.train_accuracy * 100).toFixed(1)}%` : '—'}</div>
                            <div key={`te-${i}`}>{fold.test_accuracy != null ? `${(fold.test_accuracy * 100).toFixed(1)}%` : '—'}</div>
                            <div key={`au-${i}`}>{fold.test_auc != null ? fold.test_auc.toFixed(3) : '—'}</div>
                            <div key={`as-${i}`} className="uppercase">{fold.asset || '—'}/{fold.timeframe || '—'}</div>
                          </>
                        ))}
                    </div>
                  </div>
                )}

                {/* Actions */}
                <div className="flex gap-2 pt-2 border-t border-border/30">
                  {model.status !== 'active' && (
                    <Button
                      size="sm"
                      variant="outline"
                      className="text-xs"
                      onClick={(e) => {
                        e.stopPropagation()
                        promoteMutation.mutate(model.id)
                      }}
                      disabled={promoteMutation.isPending}
                    >
                      <Zap className="w-3 h-3 mr-1" />
                      Promote to Active
                    </Button>
                  )}
                  {model.status === 'active' && (
                    <Button
                      size="sm"
                      variant="outline"
                      className="text-xs"
                      onClick={(e) => {
                        e.stopPropagation()
                        archiveMutation.mutate(model.id)
                      }}
                      disabled={archiveMutation.isPending}
                    >
                      <Archive className="w-3 h-3 mr-1" />
                      Archive
                    </Button>
                  )}
                  {model.status !== 'active' && (
                    <Button
                      size="sm"
                      variant="outline"
                      className="text-xs text-red-400 hover:text-red-300"
                      onClick={(e) => {
                        e.stopPropagation()
                        if (confirm('Delete this model permanently?')) {
                          deleteMutation.mutate(model.id)
                        }
                      }}
                      disabled={deleteMutation.isPending}
                    >
                      <Trash2 className="w-3 h-3 mr-1" />
                      Delete
                    </Button>
                  )}
                </div>
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}

// ==================== Main component ====================

type TabId = 'data' | 'training' | 'models'

const TABS: { id: TabId; label: string; icon: typeof Database }[] = [
  { id: 'data', label: 'Data Recording', icon: Database },
  { id: 'training', label: 'Training', icon: Brain },
  { id: 'models', label: 'Models', icon: TrendingUp },
]

export default function MLModelsPanel() {
  const [activeTab, setActiveTab] = useState<TabId>('data')

  return (
    <div className="h-full flex flex-col min-h-0">
      {/* Tab bar */}
      <div className="flex items-center gap-1 px-4 pt-3 pb-2 border-b border-border/50">
        {TABS.map((tab) => {
          const Icon = tab.icon
          const active = activeTab === tab.id
          return (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={cn(
                'flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-colors',
                active
                  ? 'bg-accent text-accent-foreground'
                  : 'text-muted-foreground hover:text-foreground hover:bg-muted/30'
              )}
            >
              <Icon className="w-3.5 h-3.5" />
              {tab.label}
            </button>
          )
        })}
      </div>

      {/* Tab content */}
      <ScrollArea className="flex-1 min-h-0">
        {activeTab === 'data' && <DataRecordingTab />}
        {activeTab === 'training' && <TrainingTab />}
        {activeTab === 'models' && <ModelsTab />}
      </ScrollArea>
    </div>
  )
}
