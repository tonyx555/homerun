import { useState } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import {
  RefreshCw,
  AlertCircle,
  CheckCircle,
  Zap,
  FileText,
  Activity,
  BarChart3,
  ChevronDown,
  ChevronRight,
  Layers,
  Clock,
  Shield,
  DollarSign,
  BookOpen,
  Bot,
} from 'lucide-react'
import { cn } from '../../lib/utils'
import { Card, CardContent, CardHeader, CardTitle } from '../ui/card'
import { Button } from '../ui/button'
import { Badge } from '../ui/badge'
import { ScrollArea } from '../ui/scroll-area'
import {
  getAIStatus,
  getAIUsage,
  listSkills,
  executeSkill,
  getResearchSessions,
  getResearchSession,
} from '../../services/api'

type MetricTone = 'good' | 'bad' | 'warn' | 'neutral' | 'info'

const TONE_CLASSES: Record<MetricTone, string> = {
  good: 'border-emerald-300 bg-emerald-100 text-emerald-900 dark:border-emerald-500/30 dark:bg-emerald-500/10 dark:text-emerald-200',
  bad: 'border-red-300 bg-red-100 text-red-900 dark:border-red-500/30 dark:bg-red-500/10 dark:text-red-200',
  warn: 'border-amber-300 bg-amber-100 text-amber-900 dark:border-amber-500/30 dark:bg-amber-500/10 dark:text-amber-200',
  neutral: 'border-border/70 bg-background/35 text-foreground',
  info: 'border-purple-300 bg-purple-100 text-purple-900 dark:border-purple-500/30 dark:bg-purple-500/10 dark:text-purple-200',
}

function formatNumber(num: number): string {
  if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`
  if (num >= 1_000) return `${(num / 1_000).toFixed(1)}K`
  return num.toString()
}

function MetricTile({ label, value, icon, tone }: { label: string; value: string; icon: React.ReactNode; tone: MetricTone }) {
  return (
    <Card className={cn('border', TONE_CLASSES[tone])}>
      <CardContent className="p-3">
        <div className="flex items-start justify-between gap-2">
          <p className="text-[10px] uppercase tracking-wide opacity-85">{label}</p>
          <span className="opacity-80">{icon}</span>
        </div>
        <p className="mt-2 font-data text-lg font-semibold">{value}</p>
      </CardContent>
    </Card>
  )
}

function ErrorBanner({ message }: { message: string }) {
  return (
    <div className="p-3 bg-red-500/10 border border-red-500/20 rounded-lg">
      <div className="flex items-center gap-2">
        <AlertCircle className="w-4 h-4 text-red-400 flex-shrink-0" />
        <p className="text-sm text-red-400">{message}</p>
      </div>
    </div>
  )
}

export default function AISystemView() {
  return (
    <div className="space-y-4">
      <UsageBlock />
      <SessionsBlock />
      <SkillsBlock />
    </div>
  )
}

function UsageBlock() {
  const { data: status, isLoading: statusLoading, error: statusError } = useQuery({
    queryKey: ['ai-status'],
    queryFn: async () => {
      const { data } = await getAIStatus()
      return data
    },
    refetchInterval: 30000,
  })
  const { data: usageFallback, isLoading: usageLoading, error: usageError } = useQuery({
    queryKey: ['ai-usage'],
    queryFn: async () => {
      const { data } = await getAIUsage()
      return data
    },
    refetchInterval: 30000,
    enabled: !!status?.enabled && status?.usage == null,
  })
  const usage = status?.usage ?? usageFallback
  const isLoading = statusLoading || (!!status?.enabled && usage == null && usageLoading)

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-8">
        <RefreshCw className="w-6 h-6 animate-spin text-purple-400" />
      </div>
    )
  }

  if (statusError || usageError) {
    return (
      <Card className="overflow-hidden border-border/60 bg-card/80">
        <CardContent className="p-5">
          <div className="flex items-center gap-3 text-muted-foreground">
            <AlertCircle className="w-4 h-4" />
            <span className="text-sm">Unable to fetch usage stats.</span>
          </div>
        </CardContent>
      </Card>
    )
  }

  if (!usage) {
    return (
      <Card className="overflow-hidden border-border/60 bg-card/80">
        <CardContent className="p-5">
          <div className="text-center py-8">
            <Bot className="w-10 h-10 text-muted-foreground/30 mx-auto mb-3" />
            <p className="text-xs text-muted-foreground">No usage data available yet.</p>
          </div>
        </CardContent>
      </Card>
    )
  }

  const cost = usage.estimated_cost ?? usage.total_cost_usd ?? 0
  const failedReq = usage.failed_requests ?? usage.error_count ?? 0

  return (
    <Card className="overflow-hidden border-border/60 bg-card/80">
      <div className="h-0.5 bg-amber-400" />
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center justify-between gap-3 text-base font-semibold">
          <span className="flex items-center gap-2">
            <BarChart3 className="h-4 w-4 text-amber-400" />
            Usage Telemetry
          </span>
          {usage.active_model && (
            <Badge variant="outline" className="text-xs font-mono border-purple-500/25 bg-purple-500/10 text-purple-300">
              {usage.active_model}
            </Badge>
          )}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 card-stagger">
          <MetricTile label="Requests" value={String(usage.total_requests ?? 0)} icon={<Zap className="h-3.5 w-3.5" />} tone="info" />
          <MetricTile label="Input Tokens" value={formatNumber(usage.total_input_tokens ?? 0)} icon={<FileText className="h-3.5 w-3.5" />} tone="info" />
          <MetricTile label="Output Tokens" value={formatNumber(usage.total_output_tokens ?? 0)} icon={<FileText className="h-3.5 w-3.5" />} tone="info" />
          <MetricTile label="Est. Cost" value={`$${cost.toFixed(4)}`} icon={<DollarSign className="h-3.5 w-3.5" />} tone={cost > 5 ? 'warn' : 'neutral'} />
          <MetricTile label="Avg Latency" value={`${(usage.avg_latency_ms ?? 0).toFixed(0)}ms`} icon={<Clock className="h-3.5 w-3.5" />} tone={(usage.avg_latency_ms ?? 0) > 5000 ? 'warn' : 'good'} />
          <MetricTile label="Total Tokens" value={formatNumber(usage.total_tokens ?? 0)} icon={<Activity className="h-3.5 w-3.5" />} tone="neutral" />
          <MetricTile label="Successful" value={String(usage.successful_requests ?? usage.total_requests ?? 0)} icon={<CheckCircle className="h-3.5 w-3.5" />} tone="good" />
          <MetricTile label="Failed" value={String(failedReq)} icon={<AlertCircle className="h-3.5 w-3.5" />} tone={failedReq > 0 ? 'bad' : 'good'} />
        </div>

        {usage.spend_limit_usd != null && (
          <div className="rounded-lg border border-border/60 bg-muted/30 p-4">
            <div className="flex items-center justify-between mb-2">
              <div className="flex items-center gap-2">
                <Shield className="w-3.5 h-3.5 text-blue-400" />
                <span className="text-[10px] uppercase tracking-wide text-muted-foreground">Monthly Spend Limit</span>
              </div>
              <span className="text-xs font-semibold font-data">
                ${cost.toFixed(2)} / ${usage.spend_limit_usd.toFixed(2)}
              </span>
            </div>
            <div className="w-full bg-background rounded-full h-2 border border-border">
              <div
                className={cn(
                  'h-full rounded-full transition-all',
                  (cost / usage.spend_limit_usd) >= 0.9
                    ? 'bg-red-500'
                    : (cost / usage.spend_limit_usd) >= 0.7
                      ? 'bg-amber-500'
                      : 'bg-emerald-500'
                )}
                style={{ width: `${Math.min(100, (cost / usage.spend_limit_usd) * 100)}%` }}
              />
            </div>
            <div className="flex items-center justify-between mt-1.5">
              <span className="text-[10px] text-muted-foreground font-data">
                ${(usage.spend_remaining_usd ?? 0).toFixed(2)} remaining
              </span>
              <span className="text-[10px] text-muted-foreground">
                {usage.month_start ? `Since ${new Date(usage.month_start).toLocaleDateString()}` : ''}
              </span>
            </div>
          </div>
        )}

        {usage.by_model && typeof usage.by_model === 'object' && Object.keys(usage.by_model).length > 0 && (
          <div>
            <p className="text-[10px] uppercase tracking-wide text-muted-foreground mb-2">By Model</p>
            <div className="space-y-1.5">
              {Object.entries(usage.by_model).map(([model, stats]: [string, any]) => {
                const isActive = usage.active_model && model === usage.active_model
                return (
                  <div
                    key={model}
                    className={cn(
                      'flex items-center justify-between p-3 rounded-lg border transition-colors',
                      isActive
                        ? 'bg-purple-500/5 border-purple-500/20 shadow-sm shadow-purple-500/5'
                        : 'bg-background/30 border-border/55 hover:border-border/80'
                    )}
                  >
                    <div className="flex items-center gap-2">
                      <p className="text-xs font-medium font-mono">{model}</p>
                      {isActive && (
                        <Badge variant="outline" className="text-[9px] h-4 px-1.5 border-purple-500/20 bg-purple-500/10 text-purple-400">
                          active
                        </Badge>
                      )}
                    </div>
                    <div className="flex items-center gap-3 text-[11px] text-muted-foreground font-data">
                      <span>{stats.requests ?? 0} req</span>
                      <span>{formatNumber(stats.tokens ?? 0)} tok</span>
                      <span className="font-medium text-foreground/80">${(stats.cost ?? 0).toFixed(4)}</span>
                    </div>
                  </div>
                )
              })}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  )
}

function SessionsBlock() {
  const [selectedSessionId, setSelectedSessionId] = useState<string | null>(null)
  const [sessionTypeFilter, setSessionTypeFilter] = useState('')
  const [expanded, setExpanded] = useState(true)

  const { data: sessions, isLoading } = useQuery({
    queryKey: ['ai-sessions', sessionTypeFilter],
    queryFn: async () => {
      const { data } = await getResearchSessions({
        session_type: sessionTypeFilter || undefined,
        limit: 50,
      })
      return data
    },
  })

  const { data: sessionDetail, isLoading: detailLoading } = useQuery({
    queryKey: ['ai-session-detail', selectedSessionId],
    queryFn: async () => {
      if (!selectedSessionId) return null
      const { data } = await getResearchSession(selectedSessionId)
      return data
    },
    enabled: !!selectedSessionId,
  })

  return (
    <Card className="overflow-hidden border-border/60 bg-card/80">
      <div className="h-0.5 bg-indigo-400" />
      <CardHeader className="pb-1">
        <button
          onClick={() => setExpanded(!expanded)}
          className="flex items-center justify-between w-full"
        >
          <CardTitle className="flex items-center gap-2 text-base font-semibold">
            <BookOpen className="h-4 w-4 text-indigo-400" />
            Research Sessions
          </CardTitle>
          {expanded ? <ChevronDown className="w-4 h-4 text-muted-foreground" /> : <ChevronRight className="w-4 h-4 text-muted-foreground" />}
        </button>
      </CardHeader>

      {expanded && (
        <CardContent className="pt-3">
          {isLoading ? (
            <div className="flex items-center justify-center py-8">
              <RefreshCw className="w-6 h-6 animate-spin text-indigo-400" />
            </div>
          ) : (
            <>
              <div className="mb-3">
                <select
                  value={sessionTypeFilter}
                  onChange={e => setSessionTypeFilter(e.target.value)}
                  className="bg-muted/60 border border-border rounded-lg px-3 py-1.5 text-xs h-8"
                >
                  <option value="">All Types</option>
                  <option value="resolution_analysis">Resolution Analysis</option>
                  <option value="opportunity_judgment">Opportunity Judgment</option>
                  <option value="market_analysis">Market Analysis</option>
                  <option value="news_sentiment">News Sentiment</option>
                </select>
              </div>

              {!sessions || (Array.isArray(sessions) && sessions.length === 0) ? (
                <div className="text-center py-8">
                  <Bot className="w-10 h-10 text-muted-foreground/30 mx-auto mb-3" />
                  <p className="text-xs text-muted-foreground">No research sessions found.</p>
                </div>
              ) : (
                <ScrollArea className="h-72">
                  <div className="space-y-1.5 pr-3">
                    {(Array.isArray(sessions) ? sessions : []).map((s: any) => {
                      const id = s.session_id || s.id
                      const isSelected = selectedSessionId === id
                      return (
                        <div key={id}>
                          <button
                            onClick={() => setSelectedSessionId(isSelected ? null : id)}
                            className={cn(
                              'w-full text-left p-2.5 rounded-lg border transition-colors',
                              isSelected
                                ? 'bg-indigo-500/10 border-indigo-500/30'
                                : 'bg-background/30 border-border/55 hover:border-border/80'
                            )}
                          >
                            <div className="flex items-center justify-between">
                              <div className="flex-1 min-w-0">
                                <div className="flex items-center gap-2">
                                  <p className="text-xs font-medium truncate">{s.session_type || 'Unknown'}</p>
                                  <Badge variant="outline" className="text-[9px] h-4 px-1.5 bg-indigo-500/10 text-indigo-400 border-indigo-500/20">
                                    {s.session_type?.split('_')[0] || 'session'}
                                  </Badge>
                                </div>
                                <p className="text-[10px] text-muted-foreground truncate mt-0.5 font-mono">{id}</p>
                              </div>
                              <div className="flex items-center gap-2 ml-3">
                                <span className="text-[10px] text-muted-foreground whitespace-nowrap font-data">
                                  {s.created_at ? new Date(s.created_at).toLocaleString() : ''}
                                </span>
                                {isSelected ? <ChevronDown className="w-3 h-3 text-muted-foreground" /> : <ChevronRight className="w-3 h-3 text-muted-foreground" />}
                              </div>
                            </div>
                          </button>
                          {isSelected && (
                            <div className="mt-1 ml-3 border-l-2 border-indigo-500/20 pl-3">
                              {detailLoading ? (
                                <div className="py-3"><RefreshCw className="w-4 h-4 animate-spin text-indigo-400" /></div>
                              ) : sessionDetail ? (
                                <ScrollArea className="h-48">
                                  <pre className="text-[11px] text-muted-foreground whitespace-pre-wrap py-2 font-mono">
                                    {JSON.stringify(sessionDetail, null, 2)}
                                  </pre>
                                </ScrollArea>
                              ) : (
                                <p className="text-xs text-muted-foreground py-2">Session not found.</p>
                              )}
                            </div>
                          )}
                        </div>
                      )
                    })}
                  </div>
                </ScrollArea>
              )}
            </>
          )}
        </CardContent>
      )}
    </Card>
  )
}

function SkillsBlock() {
  const [selectedSkill, setSelectedSkill] = useState<string | null>(null)
  const [skillContext, setSkillContext] = useState('')
  const [expanded, setExpanded] = useState(true)

  const { data: skills, isLoading } = useQuery({
    queryKey: ['ai-skills'],
    queryFn: async () => {
      const { data } = await listSkills()
      return data
    },
  })

  const executeMutation = useMutation({
    mutationFn: async () => {
      if (!selectedSkill) throw new Error('No skill selected')
      let ctx = {}
      try {
        ctx = skillContext ? JSON.parse(skillContext) : {}
      } catch {
        throw new Error('Invalid JSON context')
      }
      const { data } = await executeSkill({
        skill_name: selectedSkill,
        context: ctx,
      })
      return data
    },
  })

  return (
    <Card className="overflow-hidden border-border/60 bg-card/80">
      <div className="h-0.5 bg-emerald-400" />
      <CardHeader className="pb-1">
        <button
          onClick={() => setExpanded(!expanded)}
          className="flex items-center justify-between w-full"
        >
          <CardTitle className="flex items-center gap-2 text-base font-semibold">
            <Layers className="h-4 w-4 text-emerald-400" />
            AI Skills
          </CardTitle>
          {expanded ? <ChevronDown className="w-4 h-4 text-muted-foreground" /> : <ChevronRight className="w-4 h-4 text-muted-foreground" />}
        </button>
      </CardHeader>

      {expanded && (
        <CardContent className="pt-3">
          {isLoading ? (
            <div className="flex items-center justify-center py-8">
              <RefreshCw className="w-6 h-6 animate-spin text-emerald-400" />
            </div>
          ) : !skills || (Array.isArray(skills) && skills.length === 0) ? (
            <div className="text-center py-8">
              <Bot className="w-10 h-10 text-muted-foreground/30 mx-auto mb-3" />
              <p className="text-xs text-muted-foreground">No AI skills available.</p>
            </div>
          ) : (
            <div className="space-y-1.5">
              {(Array.isArray(skills) ? skills : []).map((skill: any) => (
                <div key={skill.name}>
                  <button
                    onClick={() => setSelectedSkill(skill.name === selectedSkill ? null : skill.name)}
                    className={cn(
                      'w-full text-left p-3 rounded-lg border cursor-pointer transition-colors',
                      selectedSkill === skill.name
                        ? 'bg-emerald-500/10 border-emerald-500/30'
                        : 'bg-background/30 border-border/55 hover:border-border/80'
                    )}
                  >
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <div className={cn(
                          'flex items-center justify-center w-7 h-7 rounded-md shrink-0',
                          selectedSkill === skill.name ? 'bg-emerald-500/15' : 'bg-muted/40'
                        )}>
                          <Zap className={cn('w-3.5 h-3.5', selectedSkill === skill.name ? 'text-emerald-400' : 'text-muted-foreground')} />
                        </div>
                        <div>
                          <p className="text-xs font-medium">{skill.name}</p>
                          <p className="text-[10px] text-muted-foreground">{skill.description || 'No description'}</p>
                        </div>
                      </div>
                      {selectedSkill === skill.name ? (
                        <ChevronDown className="w-3 h-3 text-muted-foreground" />
                      ) : (
                        <ChevronRight className="w-3 h-3 text-muted-foreground" />
                      )}
                    </div>
                  </button>

                  {selectedSkill === skill.name && (
                    <div className="mt-2 ml-3 pl-3 border-l-2 border-emerald-500/20 space-y-2">
                      <div>
                        <label className="block text-[10px] uppercase tracking-wide text-muted-foreground mb-1.5">Context (JSON)</label>
                        <textarea
                          value={skillContext}
                          onChange={e => setSkillContext(e.target.value)}
                          placeholder='{"market_id": "...", "question": "..."}'
                          rows={3}
                          className="w-full bg-muted/60 border border-border rounded-lg px-3 py-2 text-xs font-mono focus:outline-none focus:border-emerald-500 resize-none"
                        />
                      </div>
                      <Button
                        onClick={() => executeMutation.mutate()}
                        disabled={executeMutation.isPending}
                        size="sm"
                        className={cn(
                          'h-auto gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium transition-colors',
                          executeMutation.isPending
                            ? 'bg-muted text-muted-foreground cursor-not-allowed'
                            : 'bg-emerald-500 hover:bg-emerald-600 text-white'
                        )}
                      >
                        {executeMutation.isPending ? <RefreshCw className="w-3 h-3 animate-spin" /> : <Zap className="w-3 h-3" />}
                        Execute
                      </Button>

                      {executeMutation.data && (
                        <div className="rounded-lg border border-emerald-500/20 bg-emerald-500/5 p-3">
                          <pre className="text-[11px] text-muted-foreground whitespace-pre-wrap overflow-auto max-h-48 font-mono">
                            {typeof executeMutation.data === 'string'
                              ? executeMutation.data
                              : JSON.stringify(executeMutation.data, null, 2)}
                          </pre>
                        </div>
                      )}

                      {executeMutation.error && (
                        <ErrorBanner message={(executeMutation.error as Error).message} />
                      )}
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </CardContent>
      )}
    </Card>
  )
}
