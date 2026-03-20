import { useState, useEffect, useCallback } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Eye,
  EyeOff,
  RefreshCw,
  CheckCircle,
  AlertCircle,
  XCircle,
  DollarSign,
  Cpu,
  Cloud,
  Sparkles,
  Bot,
  Zap,
  Globe,
  Server,
} from 'lucide-react'
import { cn } from '../../lib/utils'
import { Card, CardContent, CardHeader, CardTitle } from '../ui/card'
import { Button } from '../ui/button'
import { Badge } from '../ui/badge'
import { Input } from '../ui/input'
import { Label } from '../ui/label'
import { Separator } from '../ui/separator'
import {
  getSettings,
  updateSettings,
  getLLMModels,
  refreshLLMModels,
  testLLMConnection,
  type LLMModelOption,
} from '../../services/api'

// ---------------------------------------------------------------------------
// Types & constants
// ---------------------------------------------------------------------------

interface ProviderConfig {
  id: string
  name: string
  icon: React.ElementType
  keyField: string
  keyPlaceholder: string
  hasBaseUrl?: boolean
  baseUrlField?: string
  baseUrlPlaceholder?: string
  baseUrlHint?: string
  isLocal?: boolean
}

const PROVIDERS: ProviderConfig[] = [
  {
    id: 'openai',
    name: 'OpenAI',
    icon: Sparkles,
    keyField: 'openai_api_key',
    keyPlaceholder: 'sk-...',
  },
  {
    id: 'anthropic',
    name: 'Anthropic',
    icon: Bot,
    keyField: 'anthropic_api_key',
    keyPlaceholder: 'sk-ant-...',
  },
  {
    id: 'google',
    name: 'Google (Gemini)',
    icon: Globe,
    keyField: 'google_api_key',
    keyPlaceholder: 'AIza...',
  },
  {
    id: 'xai',
    name: 'xAI (Grok)',
    icon: Zap,
    keyField: 'xai_api_key',
    keyPlaceholder: 'xai-...',
  },
  {
    id: 'deepseek',
    name: 'DeepSeek',
    icon: Cloud,
    keyField: 'deepseek_api_key',
    keyPlaceholder: 'sk-...',
  },
  {
    id: 'ollama',
    name: 'Ollama (Local)',
    icon: Server,
    keyField: 'ollama_api_key',
    keyPlaceholder: 'Optional',
    hasBaseUrl: true,
    baseUrlField: 'ollama_base_url',
    baseUrlPlaceholder: 'http://localhost:11434',
    baseUrlHint: 'Uses the OpenAI-compatible endpoint at /v1. Leave blank for default.',
    isLocal: true,
  },
  {
    id: 'lmstudio',
    name: 'LM Studio (Local)',
    icon: Cpu,
    keyField: 'lmstudio_api_key',
    keyPlaceholder: 'Optional',
    hasBaseUrl: true,
    baseUrlField: 'lmstudio_base_url',
    baseUrlPlaceholder: 'http://localhost:1234/v1',
    baseUrlHint: 'OpenAI-compatible server URL. Leave blank for default.',
    isLocal: true,
  },
]

type ProviderKeyMap = Record<string, string>
type ProviderBaseUrlMap = Record<string, string>
type ProviderTestStatus = Record<string, { status: 'idle' | 'testing' | 'success' | 'error'; message?: string }>

// ---------------------------------------------------------------------------
// Secret input (inline)
// ---------------------------------------------------------------------------

function SecretInput({
  label,
  value,
  placeholder,
  onChange,
  showSecret,
  onToggle,
}: {
  label: string
  value: string
  placeholder: string
  onChange: (v: string) => void
  showSecret: boolean
  onToggle: () => void
}) {
  return (
    <div>
      <Label className="text-xs text-muted-foreground">{label}</Label>
      <div className="relative mt-1">
        <Input
          type={showSecret ? 'text' : 'password'}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder={placeholder}
          className="pr-10 font-mono text-sm"
        />
        <Button
          type="button"
          variant="ghost"
          size="icon"
          className="absolute right-0 top-0 h-full px-3"
          onClick={onToggle}
        >
          {showSecret ? <EyeOff className="w-3.5 h-3.5" /> : <Eye className="w-3.5 h-3.5" />}
        </Button>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export default function AIProvidersView() {
  const queryClient = useQueryClient()

  // ---- data fetching ----
  const { data: settings } = useQuery({
    queryKey: ['settings'],
    queryFn: getSettings,
  })

  const [availableModels, setAvailableModels] = useState<Record<string, LLMModelOption[]>>({})
  const [isRefreshingModels, setIsRefreshingModels] = useState(false)

  // ---- form state ----
  const [primaryProvider, setPrimaryProvider] = useState('none')
  const [primaryModel, setPrimaryModel] = useState('')
  const [maxMonthlySpend, setMaxMonthlySpend] = useState(50)
  const [keys, setKeys] = useState<ProviderKeyMap>({})
  const [baseUrls, setBaseUrls] = useState<ProviderBaseUrlMap>({})
  const [showSecrets, setShowSecrets] = useState<Record<string, boolean>>({})
  const [testStatuses, setTestStatuses] = useState<ProviderTestStatus>({})
  const [saveMessage, setSaveMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null)

  // ---- sync from loaded settings ----
  useEffect(() => {
    if (!settings) return
    const llm = settings.llm
    setPrimaryProvider(llm?.provider || 'none')
    setPrimaryModel(llm?.model || '')
    setMaxMonthlySpend(llm?.max_monthly_spend ?? 50)
    setBaseUrls({
      ollama_base_url: llm?.ollama_base_url || '',
      lmstudio_base_url: llm?.lmstudio_base_url || '',
    })
    // Keys start blank (they are secrets; the server returns masked or null)
    setKeys({})
  }, [settings])

  // ---- load models ----
  useEffect(() => {
    getLLMModels()
      .then((res) => {
        if (res.models) setAvailableModels(res.models)
      })
      .catch(() => {})
  }, [])

  const handleRefreshModels = useCallback(async () => {
    setIsRefreshingModels(true)
    try {
      const provider = primaryProvider !== 'none' ? primaryProvider : undefined
      const res = await refreshLLMModels(provider)
      if (res.models) setAvailableModels(res.models)
    } catch {
      // ignore
    } finally {
      setIsRefreshingModels(false)
    }
  }, [primaryProvider])

  const modelsForProvider = availableModels[primaryProvider] || []

  // ---- mutations ----
  const saveMutation = useMutation({
    mutationFn: updateSettings,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['settings'] })
      queryClient.invalidateQueries({ queryKey: ['ai-status'] })
      queryClient.invalidateQueries({ queryKey: ['ai-usage'] })
      setSaveMessage({ type: 'success', text: 'Settings saved successfully' })
      setTimeout(() => setSaveMessage(null), 3000)
    },
    onError: (error: any) => {
      setSaveMessage({ type: 'error', text: error.message || 'Failed to save settings' })
      setTimeout(() => setSaveMessage(null), 5000)
    },
  })

  // ---- helpers ----
  const toggleSecret = (id: string) => setShowSecrets((p) => ({ ...p, [id]: !p[id] }))

  const setKey = (field: string, value: string) => setKeys((p) => ({ ...p, [field]: value }))
  const setBaseUrl = (field: string, value: string) => setBaseUrls((p) => ({ ...p, [field]: value }))

  const getConnectionStatus = (provider: ProviderConfig): 'connected' | 'not_configured' | 'error' => {
    const test = testStatuses[provider.id]
    if (test?.status === 'success') return 'connected'
    if (test?.status === 'error') return 'error'
    // Fall back to checking if a key exists on the server (masked value present)
    const serverKey = settings?.llm?.[provider.keyField as keyof typeof settings.llm]
    if (serverKey) return 'connected'
    return 'not_configured'
  }

  const handleTestProvider = async (providerId: string) => {
    setTestStatuses((p) => ({ ...p, [providerId]: { status: 'testing' } }))
    try {
      const res = await testLLMConnection(providerId)
      setTestStatuses((p) => ({
        ...p,
        [providerId]: {
          status: res.status === 'success' ? 'success' : 'error',
          message: res.message,
        },
      }))
    } catch (err: any) {
      setTestStatuses((p) => ({
        ...p,
        [providerId]: { status: 'error', message: err.message || 'Connection failed' },
      }))
    }
  }

  const handleSaveProvider = (provider: ProviderConfig) => {
    const llmUpdate: Record<string, unknown> = {}
    const keyVal = keys[provider.keyField]
    if (keyVal) llmUpdate[provider.keyField] = keyVal
    if (provider.hasBaseUrl && provider.baseUrlField) {
      llmUpdate[provider.baseUrlField] = baseUrls[provider.baseUrlField] || null
    }
    if (Object.keys(llmUpdate).length === 0) {
      setSaveMessage({ type: 'error', text: 'No changes to save for this provider' })
      setTimeout(() => setSaveMessage(null), 3000)
      return
    }
    saveMutation.mutate({ llm: llmUpdate as any })
  }

  const handleSaveGlobal = () => {
    saveMutation.mutate({
      llm: {
        provider: primaryProvider,
        model: primaryModel || '',
        max_monthly_spend: maxMonthlySpend,
      },
    })
  }

  // ---- render ----
  return (
    <div className="space-y-5">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-base font-semibold tracking-tight">Providers</h3>
          <p className="text-xs text-muted-foreground mt-0.5">
            Configure API keys for LLM providers. Multiple providers can be configured simultaneously.
          </p>
        </div>
        {saveMessage && (
          <Badge
            variant="outline"
            className={cn(
              'text-xs',
              saveMessage.type === 'success'
                ? 'bg-green-500/10 text-green-400 border-green-500/30'
                : 'bg-red-500/10 text-red-400 border-red-500/30',
            )}
          >
            {saveMessage.text}
          </Badge>
        )}
      </div>

      {/* Global controls — primary provider, model, spend limit */}
      <Card className="border-border/40 bg-card/50">
        <CardHeader className="pb-3 pt-4 px-4">
          <CardTitle className="text-sm font-medium flex items-center gap-2">
            <Cpu className="w-4 h-4 text-purple-400" />
            Primary Configuration
          </CardTitle>
        </CardHeader>
        <CardContent className="px-4 pb-4 space-y-4">
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
            {/* Primary provider */}
            <div>
              <Label className="text-xs text-muted-foreground">Primary Provider</Label>
              <select
                value={primaryProvider}
                onChange={(e) => {
                  setPrimaryProvider(e.target.value)
                  setPrimaryModel('')
                }}
                className="w-full bg-muted border border-border rounded-lg px-3 py-2 text-sm mt-1"
              >
                <option value="none">None (Disabled)</option>
                {PROVIDERS.map((p) => (
                  <option key={p.id} value={p.id}>
                    {p.name}
                  </option>
                ))}
              </select>
            </div>

            {/* Primary model */}
            <div>
              <Label className="text-xs text-muted-foreground">Model</Label>
              <div className="flex gap-1.5 mt-1">
                <select
                  value={primaryModel}
                  onChange={(e) => setPrimaryModel(e.target.value)}
                  className="flex-1 bg-muted border border-border rounded-lg px-3 py-2 text-sm"
                  disabled={primaryProvider === 'none'}
                >
                  <option value="">Select a model...</option>
                  {modelsForProvider.map((m) => (
                    <option key={m.id} value={m.id}>
                      {m.name}
                    </option>
                  ))}
                  {primaryModel && !modelsForProvider.find((m) => m.id === primaryModel) && (
                    <option value={primaryModel}>{primaryModel} (current)</option>
                  )}
                </select>
                <Button
                  variant="secondary"
                  size="icon"
                  onClick={handleRefreshModels}
                  disabled={isRefreshingModels || primaryProvider === 'none'}
                  title="Refresh models from provider API"
                >
                  <RefreshCw className={cn('w-4 h-4', isRefreshingModels && 'animate-spin')} />
                </Button>
              </div>
              <p className="text-[11px] text-muted-foreground/70 mt-1">
                {modelsForProvider.length > 0
                  ? `${modelsForProvider.length} models available`
                  : primaryProvider !== 'none'
                    ? 'Click refresh to fetch models'
                    : 'Select a provider first'}
              </p>
            </div>

            {/* Spend limit */}
            <div>
              <Label className="text-xs text-muted-foreground">Monthly Spend Limit</Label>
              <div className="flex items-center gap-2 mt-1">
                <DollarSign className="w-4 h-4 text-muted-foreground flex-shrink-0" />
                <Input
                  type="number"
                  min={0}
                  step={5}
                  value={maxMonthlySpend}
                  onChange={(e) => setMaxMonthlySpend(parseFloat(e.target.value) || 0)}
                  className="w-full text-sm"
                />
              </div>
              <p className="text-[11px] text-muted-foreground/70 mt-1">
                Requests blocked when limit is reached. 0 = unlimited.
              </p>
            </div>
          </div>

          <div className="flex justify-end">
            <Button size="sm" onClick={handleSaveGlobal} disabled={saveMutation.isPending}>
              Save Primary Settings
            </Button>
          </div>
        </CardContent>
      </Card>

      <Separator className="opacity-30" />

      {/* Provider cards grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
        {PROVIDERS.map((provider) => {
          const Icon = provider.icon
          const connectionStatus = getConnectionStatus(provider)
          const testStatus = testStatuses[provider.id]

          return (
            <Card
              key={provider.id}
              className={cn(
                'border-border/40 bg-card/50 transition-colors',
                primaryProvider === provider.id && 'border-purple-500/40 bg-purple-500/5',
              )}
            >
              <CardHeader className="pb-2 pt-3 px-4">
                <div className="flex items-center justify-between">
                  <CardTitle className="text-sm font-medium flex items-center gap-2">
                    <Icon className="w-4 h-4 text-cyan-400" />
                    {provider.name}
                  </CardTitle>
                  <StatusBadge status={connectionStatus} />
                </div>
              </CardHeader>

              <CardContent className="px-4 pb-4 space-y-3">
                {/* Base URL (Ollama / LM Studio) */}
                {provider.hasBaseUrl && provider.baseUrlField && (
                  <div>
                    <Label className="text-xs text-muted-foreground">Base URL</Label>
                    <Input
                      type="text"
                      value={baseUrls[provider.baseUrlField] || ''}
                      onChange={(e) => setBaseUrl(provider.baseUrlField!, e.target.value)}
                      placeholder={provider.baseUrlPlaceholder}
                      className="mt-1 text-sm font-mono"
                    />
                    {provider.baseUrlHint && (
                      <p className="text-[11px] text-muted-foreground/70 mt-1">{provider.baseUrlHint}</p>
                    )}
                  </div>
                )}

                {/* API Key */}
                <SecretInput
                  label={provider.isLocal ? 'API Key (Optional)' : 'API Key'}
                  value={keys[provider.keyField] || ''}
                  placeholder={
                    (settings?.llm?.[provider.keyField as keyof typeof settings.llm] as string) ||
                    provider.keyPlaceholder
                  }
                  onChange={(v) => setKey(provider.keyField, v)}
                  showSecret={!!showSecrets[provider.id]}
                  onToggle={() => toggleSecret(provider.id)}
                />

                {/* Test result message */}
                {testStatus?.message && (
                  <p
                    className={cn(
                      'text-[11px]',
                      testStatus.status === 'success' ? 'text-green-400' : 'text-red-400',
                    )}
                  >
                    {testStatus.message}
                  </p>
                )}

                {/* Actions */}
                <div className="flex items-center gap-2 pt-1">
                  <Button
                    size="sm"
                    variant="secondary"
                    onClick={() => handleTestProvider(provider.id)}
                    disabled={testStatus?.status === 'testing'}
                    className="text-xs"
                  >
                    {testStatus?.status === 'testing' ? (
                      <RefreshCw className="w-3 h-3 mr-1.5 animate-spin" />
                    ) : (
                      <Zap className="w-3 h-3 mr-1.5" />
                    )}
                    Test
                  </Button>
                  <Button
                    size="sm"
                    onClick={() => handleSaveProvider(provider)}
                    disabled={saveMutation.isPending}
                    className="text-xs"
                  >
                    Save
                  </Button>
                </div>
              </CardContent>
            </Card>
          )
        })}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Status badge sub-component
// ---------------------------------------------------------------------------

function StatusBadge({ status }: { status: 'connected' | 'not_configured' | 'error' }) {
  switch (status) {
    case 'connected':
      return (
        <Badge variant="outline" className="text-[10px] bg-emerald-500/10 text-emerald-400 border-emerald-500/30 gap-1">
          <CheckCircle className="w-3 h-3" />
          Connected
        </Badge>
      )
    case 'error':
      return (
        <Badge variant="outline" className="text-[10px] bg-red-500/10 text-red-400 border-red-500/30 gap-1">
          <XCircle className="w-3 h-3" />
          Error
        </Badge>
      )
    default:
      return (
        <Badge variant="outline" className="text-[10px] bg-muted text-muted-foreground border-border/50 gap-1">
          <AlertCircle className="w-3 h-3" />
          Not configured
        </Badge>
      )
  }
}
