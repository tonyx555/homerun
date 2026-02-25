import { Input } from './ui/input'
import { Label } from './ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './ui/select'

interface TwitterSourceFormProps {
  config: Record<string, any>
  onChange: (config: Record<string, any>) => void
}

const POLL_INTERVAL_OPTIONS = [
  { value: '5', label: '5 minutes' },
  { value: '15', label: '15 minutes' },
  { value: '30', label: '30 minutes' },
  { value: '60', label: '1 hour' },
  { value: '240', label: '4 hours' },
  { value: '1440', label: '24 hours' },
]

export default function TwitterSourceForm({ config, onChange }: TwitterSourceFormProps) {
  const update = (key: string, value: unknown) => {
    onChange({ ...config, [key]: value })
  }

  const handles = String(config.handles || '')
  const keywords = String(config.keywords || '')
  const bearerToken = String(config.bearer_token || '')
  const nitterInstance = String(config.nitter_instance || 'nitter.privacydev.net')
  const pollInterval = String(config.poll_interval_minutes || '15')
  const limit = String(config.limit || '50')

  return (
    <div className="space-y-3">
      <div>
        <Label className="text-[11px] text-muted-foreground">
          Accounts <span className="text-muted-foreground/60 font-normal">(comma-separated @handles)</span>
        </Label>
        <textarea
          value={handles}
          onChange={(e) => update('handles', e.target.value)}
          className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-xs font-mono ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
          rows={2}
          placeholder="@polyaborist, @KalshiWatch, @ElectionBettingOdds, @PolymarketWhale"
          spellCheck={false}
        />
        <p className="text-[10px] text-muted-foreground mt-1">Twitter/X accounts to monitor for prediction-market-relevant posts</p>
      </div>

      <div>
        <Label className="text-[11px] text-muted-foreground">
          Keywords <span className="text-muted-foreground/60 font-normal">(comma-separated search terms)</span>
        </Label>
        <textarea
          value={keywords}
          onChange={(e) => update('keywords', e.target.value)}
          className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-xs font-mono ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
          rows={2}
          placeholder="polymarket, kalshi, prediction market, election odds"
          spellCheck={false}
        />
        <p className="text-[10px] text-muted-foreground mt-1">Search terms for finding relevant tweets (requires X API bearer token)</p>
      </div>

      <div>
        <Label className="text-[11px] text-muted-foreground">
          X API Bearer Token <span className="text-muted-foreground/60 font-normal">(optional)</span>
        </Label>
        <Input
          type="password"
          value={bearerToken}
          onChange={(e) => update('bearer_token', e.target.value)}
          className="mt-1 h-8 text-xs font-mono"
          placeholder="AAAA..."
          autoComplete="off"
        />
        <p className="text-[10px] text-muted-foreground mt-1">
          If set, uses X API v2 for real-time search. Without it, falls back to Nitter RSS (accounts only, no keyword search).
        </p>
      </div>

      <div>
        <Label className="text-[11px] text-muted-foreground">Nitter Instance</Label>
        <Input
          value={nitterInstance}
          onChange={(e) => update('nitter_instance', e.target.value)}
          className="mt-1 h-8 text-xs font-mono"
          placeholder="nitter.privacydev.net"
        />
        <p className="text-[10px] text-muted-foreground mt-1">Fallback RSS source when no bearer token is configured</p>
      </div>

      <div className="grid gap-3 grid-cols-2">
        <div>
          <Label className="text-[11px] text-muted-foreground">Poll Interval</Label>
          <Select value={pollInterval} onValueChange={(val) => update('poll_interval_minutes', parseInt(val, 10))}>
            <SelectTrigger className="mt-1 h-8 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {POLL_INTERVAL_OPTIONS.map((opt) => (
                <SelectItem key={opt.value} value={opt.value}>
                  {opt.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div>
          <Label className="text-[11px] text-muted-foreground">Max Tweets</Label>
          <Input
            type="number"
            value={limit}
            onChange={(e) => update('limit', parseInt(e.target.value, 10) || 50)}
            className="mt-1 h-8 text-xs font-mono"
            min={1}
            max={200}
            placeholder="50"
          />
        </div>
      </div>
    </div>
  )
}
