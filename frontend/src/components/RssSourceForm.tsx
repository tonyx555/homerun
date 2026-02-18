import { Input } from './ui/input'
import { Label } from './ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './ui/select'

interface RssSourceFormProps {
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

export default function RssSourceForm({ config, onChange }: RssSourceFormProps) {
  const update = (key: string, value: unknown) => {
    onChange({ ...config, [key]: value })
  }

  const feedUrl = String(config.feed_url || '')
  const pollInterval = String(config.poll_interval_minutes || '15')
  const categoryFilter = String(config.category_filter || '')

  return (
    <div className="space-y-3">
      <div>
        <Label className="text-[11px] text-muted-foreground">
          Feed URL <span className="text-red-400">*</span>
        </Label>
        <Input
          type="url"
          value={feedUrl}
          onChange={(e) => update('feed_url', e.target.value)}
          className="mt-1 h-8 text-xs font-mono"
          placeholder="https://example.com/feed.xml"
          required
        />
        {feedUrl && !/^https?:\/\/.+/.test(feedUrl) && (
          <p className="text-[10px] text-red-400 mt-1">Must be a valid HTTP or HTTPS URL</p>
        )}
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
          <Label className="text-[11px] text-muted-foreground">Category Filter</Label>
          <Input
            value={categoryFilter}
            onChange={(e) => update('category_filter', e.target.value || undefined)}
            className="mt-1 h-8 text-xs"
            placeholder="e.g. technology, finance"
          />
        </div>
      </div>
    </div>
  )
}
