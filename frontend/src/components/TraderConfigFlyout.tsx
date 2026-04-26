import { memo } from 'react'
import { AlertTriangle, Clock3, Sparkles, Zap } from 'lucide-react'
import type { Trader, TraderLatencyClass } from '../services/apiTraders'
import { Badge } from './ui/badge'
import { Button } from './ui/button'
import { Input } from './ui/input'
import { Label } from './ui/label'
import { ScrollArea } from './ui/scroll-area'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './ui/select'
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from './ui/sheet'
import { Switch } from './ui/switch'
import {
  FlyoutSection,
  TRADING_SCHEDULE_DAYS,
  TRADING_SCHEDULE_DAY_LABEL,
  TRADING_SCHEDULE_WEEKDAYS,
  TRADING_SCHEDULE_WEEKENDS,
  type StrategyCatalogOption,
  type StrategyOptionDetail,
  type TradingScheduleDay,
  type TradingScheduleDraft,
  normalizeStrategyKey,
  normalizeVersionList,
} from './tradingPanelFlyoutShared'

type DeleteAction = 'block' | 'disable' | 'force_delete' | 'transfer_delete'

type Mutation<TInput = void> = {
  isPending: boolean
  mutate: (input: TInput) => void
}

export type TraderConfigFlyoutProps = {
  open: boolean
  onOpenChange: (open: boolean) => void
  mode: 'create' | 'edit'
  busy: boolean
  saveError: string | null

  // Bot Profile
  draftMode: 'shadow' | 'live'
  draftName: string
  setDraftName: (value: string) => void
  draftDescription: string
  setDraftDescription: (value: string) => void
  draftLatencyClass: TraderLatencyClass
  setDraftLatencyClass: (value: TraderLatencyClass) => void

  // Copy Settings
  draftCopyFromMode: 'shadow' | 'live'
  setDraftCopyFromMode: (value: 'shadow' | 'live') => void
  draftCopyFromTraderId: string
  copySourceTraders: Trader[]
  applyCreateCopyFromSelection: (value: string) => void

  // Strategy
  draftStrategyKey: string
  setDraftStrategy: (value: string) => void
  setDraftStrategyVersionFromValue: (value: string) => void
  allStrategyOptions: StrategyCatalogOption[]
  draftStrategyOption: StrategyCatalogOption | null
  effectiveDraftSourceKey: string
  effectiveDraftStrategyDetail: StrategyOptionDetail | null
  effectiveDraftStrategyVersion: number | null

  // Trading Schedule
  tradingScheduleDraft: TradingScheduleDraft
  setDraftTradingSchedule: (
    patch:
      | Partial<TradingScheduleDraft>
      | ((current: TradingScheduleDraft) => Partial<TradingScheduleDraft>)
  ) => void
  toggleTradingScheduleDay: (day: TradingScheduleDay) => void

  // Delete (edit mode only)
  selectedTrader: Trader | null
  selectedTraderDeleteExposureSummary: string
  selectedTraderHasLiveDeleteExposure: boolean
  selectedTraderHasAnyDeleteExposure: boolean
  selectedTraderOpenLivePositions: number
  selectedTraderOpenShadowPositions: number
  selectedTraderOpenLiveOrders: number
  selectedTraderOpenShadowOrders: number
  traders: Trader[]
  deleteAction: DeleteAction
  setDeleteAction: (value: DeleteAction) => void
  deleteForceConfirm: boolean
  setDeleteForceConfirm: (value: boolean) => void
  deleteTransferTargetId: string | null
  setDeleteTransferTargetId: (value: string | null) => void
  deleteTraderMutation: Mutation<{
    traderId: string
    action: DeleteAction
    transferToTraderId?: string
  }>

  // Save / Create
  createTraderMutation: Mutation<void>
  saveTraderMutation: Mutation<string>
}

function TraderConfigFlyoutImpl(props: TraderConfigFlyoutProps) {
  const {
    open,
    onOpenChange,
    mode,
    busy,
    saveError,
    draftMode,
    draftName,
    setDraftName,
    draftDescription,
    setDraftDescription,
    draftLatencyClass,
    setDraftLatencyClass,
    draftCopyFromMode,
    setDraftCopyFromMode,
    draftCopyFromTraderId,
    copySourceTraders,
    applyCreateCopyFromSelection,
    draftStrategyKey,
    setDraftStrategy,
    setDraftStrategyVersionFromValue,
    allStrategyOptions,
    draftStrategyOption,
    effectiveDraftSourceKey,
    effectiveDraftStrategyDetail,
    effectiveDraftStrategyVersion,
    tradingScheduleDraft,
    setDraftTradingSchedule,
    toggleTradingScheduleDay,
    selectedTrader,
    selectedTraderDeleteExposureSummary,
    selectedTraderHasLiveDeleteExposure,
    selectedTraderHasAnyDeleteExposure,
    selectedTraderOpenLivePositions,
    selectedTraderOpenShadowPositions,
    selectedTraderOpenLiveOrders,
    selectedTraderOpenShadowOrders,
    traders,
    deleteAction,
    setDeleteAction,
    deleteForceConfirm,
    setDeleteForceConfirm,
    deleteTransferTargetId,
    setDeleteTransferTargetId,
    deleteTraderMutation,
    createTraderMutation,
    saveTraderMutation,
  } = props

  const detail = effectiveDraftStrategyDetail
  const sourceKey = effectiveDraftSourceKey
  const sourceLabel = draftStrategyOption?.sourceLabel || sourceKey.toUpperCase()
  const latestVersion = detail?.latestVersion ?? detail?.version ?? null
  const selectedVersion = effectiveDraftStrategyVersion
  const selectedVersionToken = selectedVersion == null ? 'latest' : `v${selectedVersion}`
  const availableVersions = (() => {
    const rows = normalizeVersionList(detail?.versions || [])
    if (latestVersion != null && !rows.includes(latestVersion)) rows.unshift(latestVersion)
    if (selectedVersion != null && !rows.includes(selectedVersion)) rows.unshift(selectedVersion)
    return rows
  })()

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side="right" className="w-full sm:max-w-3xl p-0">
        <div className="h-full min-h-0 flex flex-col">
          <div className="border-b border-border px-4 py-3">
            <SheetHeader className="space-y-1 text-left">
              <SheetTitle className="text-base">
                {mode === 'create' ? 'Create Auto Bot' : 'Edit Auto Bot'}
              </SheetTitle>
              <SheetDescription>
                {mode === 'create'
                  ? 'Configure a new bot profile with explicit strategy, source, risk, and schedule controls.'
                  : 'Update strategy, source, risk, and schedule settings for this bot.'}
              </SheetDescription>
            </SheetHeader>
          </div>

          <ScrollArea className="flex-1 min-h-0 px-4 py-3">
            <div className="space-y-3 pb-2">
              <FlyoutSection
                title="Bot Profile"
                icon={Sparkles}
                subtitle="Name this bot. The strategy and source are configured below."
              >
                <div className="rounded-md border border-border/60 bg-muted/15 px-3 py-2">
                  <div className="flex items-center justify-between gap-2">
                    <p className="text-[11px] uppercase tracking-wider text-muted-foreground">Bot Mode</p>
                    <Badge className="h-5 px-1.5 text-[10px]" variant={draftMode === 'live' ? 'destructive' : 'outline'}>
                      {draftMode.toUpperCase()}
                    </Badge>
                  </div>
                  <p className="mt-1 text-[10px] text-muted-foreground/75">
                    This bot is scoped to {draftMode === 'live' ? 'live' : 'sandbox'} execution only.
                  </p>
                </div>

                <div>
                  <Label>Name</Label>
                  <Input value={draftName} onChange={(event) => setDraftName(event.target.value)} className="mt-1" />
                </div>

                <div>
                  <Label>Description</Label>
                  <Input value={draftDescription} onChange={(event) => setDraftDescription(event.target.value)} className="mt-1" />
                </div>

                <div>
                  <Label>Latency Class</Label>
                  <Select
                    value={draftLatencyClass}
                    onValueChange={(value: string) => setDraftLatencyClass((value === 'fast' || value === 'slow') ? (value as TraderLatencyClass) : 'normal')}
                  >
                    <SelectTrigger className="mt-1">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="fast">Fast — event-driven, sub-second single-leg</SelectItem>
                      <SelectItem value="normal">Normal — shared orchestrator loop (default)</SelectItem>
                      <SelectItem value="slow">Slow — relaxed budgets for multi-leg / recon-heavy strategies</SelectItem>
                    </SelectContent>
                  </Select>
                  <p className="mt-1 text-[10px] text-muted-foreground/75 leading-tight">
                    Fast-tier bots run in the ``fast_trader_runtime`` with an isolated DB pool and sub-second budgets.
                    They MUST be single-leg strategies (one market per signal).
                    Pick Normal unless you explicitly need sub-second latency.
                  </p>
                </div>

                {mode === 'create' ? (
                  <div>
                    <Label>Copy Settings From Existing Bot (Optional)</Label>
                    <div className="mt-1 flex items-center gap-1">
                      <Button
                        type="button"
                        size="sm"
                        variant={draftCopyFromMode === 'shadow' ? 'default' : 'outline'}
                        className="h-6 px-2 text-[10px]"
                        onClick={() => setDraftCopyFromMode('shadow')}
                      >
                        Sandbox Bots
                      </Button>
                      <Button
                        type="button"
                        size="sm"
                        variant={draftCopyFromMode === 'live' ? 'default' : 'outline'}
                        className="h-6 px-2 text-[10px]"
                        onClick={() => setDraftCopyFromMode('live')}
                      >
                        Live Bots
                      </Button>
                    </div>
                    <Select
                      value={draftCopyFromTraderId || '__none__'}
                      onValueChange={applyCreateCopyFromSelection}
                    >
                      <SelectTrigger className="mt-1">
                        <SelectValue placeholder="Start from scratch" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="__none__">Start from scratch</SelectItem>
                        {copySourceTraders.map((trader) => (
                          <SelectItem key={trader.id} value={trader.id}>
                            {trader.name}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    {copySourceTraders.length === 0 ? (
                      <p className="mt-1 text-[10px] text-muted-foreground/75 leading-tight">
                        No {draftCopyFromMode === 'live' ? 'live' : 'sandbox'} bots available to copy from.
                      </p>
                    ) : null}
                    <p className="mt-1 text-[10px] text-muted-foreground/75 leading-tight">
                      Creates a new bot with copied source config, strategy params, interval, risk limits, and schedule settings
                      from the selected bot. Trades, decisions, orders, and events are never copied.
                    </p>
                  </div>
                ) : null}
              </FlyoutSection>

              <FlyoutSection
                title="Strategy"
                icon={Zap}
                subtitle="Pick the strategy this bot will run. The signal source is derived from the strategy."
              >
                <div>
                  <Label>Strategy</Label>
                  <Select
                    value={normalizeStrategyKey(draftStrategyKey)}
                    onValueChange={setDraftStrategy}
                  >
                    <SelectTrigger className="mt-1">
                      <SelectValue placeholder="Choose a strategy" />
                    </SelectTrigger>
                    <SelectContent>
                      {allStrategyOptions.map((option) => (
                        <SelectItem key={option.key} value={option.key}>
                          <span>{option.label}</span>
                          <span className="ml-2 text-muted-foreground/70 text-[10px]">
                            {option.sourceLabel}
                          </span>
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <div className="mt-1.5 flex items-center gap-1.5 text-[10px] text-muted-foreground/80">
                    <span>Source:</span>
                    <Badge
                      variant="outline"
                      className="h-4 px-1.5 text-[9px] font-mono border-emerald-500/30 text-emerald-300 bg-emerald-500/10"
                    >
                      {sourceLabel || 'AUTO'}
                    </Badge>
                    <span className="text-muted-foreground/60">auto-derived from strategy</span>
                  </div>
                </div>

                {detail ? (
                  <div className="mt-3">
                    <Label>Version</Label>
                    <div className="mt-1 flex min-w-0 items-center gap-1.5">
                      <Badge
                        variant="outline"
                        className="h-5 min-w-0 flex-1 truncate px-1.5 text-[10px] font-mono border-emerald-500/30 text-emerald-300 bg-emerald-500/10"
                      >
                        {latestVersion != null ? `Latest v${latestVersion}` : 'Latest'}
                      </Badge>
                      <Select
                        value={selectedVersionToken}
                        onValueChange={setDraftStrategyVersionFromValue}
                      >
                        <SelectTrigger className="h-7 w-[160px] shrink-0 text-[10px] font-mono">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="latest">
                            {latestVersion != null ? `latest (v${latestVersion})` : 'latest'}
                          </SelectItem>
                          {availableVersions.map((version) => (
                            <SelectItem key={`v${version}`} value={`v${version}`}>
                              {`v${version}`}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                ) : null}
              </FlyoutSection>

              <FlyoutSection
                title="Trading Schedule"
                icon={Clock3}
                iconClassName="text-cyan-500"
                count={tradingScheduleDraft.enabled ? 'active' : 'always-on'}
                subtitle="UTC-only bot gate by days, time window, and optional date bounds."
              >
                <div className="rounded-md border border-border p-3 space-y-3">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div>
                      <p className="text-sm font-medium">Enable Schedule Gate</p>
                      <p className="text-[10px] text-muted-foreground">
                        When off, this bot can trade any time.
                      </p>
                    </div>
                    <Switch
                      checked={tradingScheduleDraft.enabled}
                      onCheckedChange={(checked) => setDraftTradingSchedule({ enabled: checked })}
                    />
                  </div>

                  <div className="grid grid-cols-2 gap-1.5 md:grid-cols-6">
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      className="h-6 px-2 text-[11px]"
                      onClick={() => setDraftTradingSchedule({ enabled: false })}
                    >
                      Always On
                    </Button>
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      className="h-6 px-2 text-[11px]"
                      onClick={() =>
                        setDraftTradingSchedule({
                          enabled: true,
                          days: [...TRADING_SCHEDULE_DAYS],
                          startTimeUtc: '00:00',
                          endTimeUtc: '23:59',
                        })
                      }
                    >
                      24/7
                    </Button>
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      className="h-6 px-2 text-[11px]"
                      onClick={() =>
                        setDraftTradingSchedule({
                          enabled: true,
                          days: [...TRADING_SCHEDULE_WEEKDAYS],
                          startTimeUtc: '00:00',
                          endTimeUtc: '23:59',
                        })
                      }
                    >
                      Weekdays
                    </Button>
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      className="h-6 px-2 text-[11px]"
                      onClick={() =>
                        setDraftTradingSchedule({
                          enabled: true,
                          days: [...TRADING_SCHEDULE_WEEKENDS],
                          startTimeUtc: '00:00',
                          endTimeUtc: '23:59',
                        })
                      }
                    >
                      Weekends
                    </Button>
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      className="h-6 px-2 text-[11px]"
                      onClick={() =>
                        setDraftTradingSchedule({
                          enabled: true,
                          startTimeUtc: '00:00',
                          endTimeUtc: '23:59',
                        })
                      }
                    >
                      Reset Window
                    </Button>
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      className="h-6 px-2 text-[11px]"
                      onClick={() =>
                        setDraftTradingSchedule({
                          startDateUtc: '',
                          endDateUtc: '',
                          endAtUtc: '',
                        })
                      }
                    >
                      Clear Bounds
                    </Button>
                  </div>

                  <div>
                    <Label className="text-[11px] text-muted-foreground">Days (UTC)</Label>
                    <div className="mt-1 flex flex-wrap gap-1.5">
                      {TRADING_SCHEDULE_DAYS.map((day) => {
                        const selected = tradingScheduleDraft.days.includes(day)
                        return (
                          <Button
                            key={day}
                            type="button"
                            size="sm"
                            variant={selected ? 'default' : 'outline'}
                            className="h-6 px-2 text-[11px]"
                            onClick={() => toggleTradingScheduleDay(day)}
                            disabled={!tradingScheduleDraft.enabled}
                          >
                            {TRADING_SCHEDULE_DAY_LABEL[day]}
                          </Button>
                        )
                      })}
                    </div>
                  </div>

                  <div className="grid gap-2 md:grid-cols-2">
                    <div>
                      <Label className="text-[11px] text-muted-foreground">Start Time (UTC)</Label>
                      <Input
                        type="time"
                        value={tradingScheduleDraft.startTimeUtc}
                        onChange={(event) => setDraftTradingSchedule({ startTimeUtc: event.target.value })}
                        className="mt-1 h-8 text-xs font-mono"
                        disabled={!tradingScheduleDraft.enabled}
                      />
                    </div>
                    <div>
                      <Label className="text-[11px] text-muted-foreground">End Time (UTC)</Label>
                      <Input
                        type="time"
                        value={tradingScheduleDraft.endTimeUtc}
                        onChange={(event) => setDraftTradingSchedule({ endTimeUtc: event.target.value })}
                        className="mt-1 h-8 text-xs font-mono"
                        disabled={!tradingScheduleDraft.enabled}
                      />
                    </div>
                    <div>
                      <Label className="text-[11px] text-muted-foreground">Start Date (UTC)</Label>
                      <Input
                        type="date"
                        value={tradingScheduleDraft.startDateUtc}
                        onChange={(event) => setDraftTradingSchedule({ startDateUtc: event.target.value })}
                        className="mt-1 h-8 text-xs font-mono"
                        disabled={!tradingScheduleDraft.enabled}
                      />
                    </div>
                    <div>
                      <Label className="text-[11px] text-muted-foreground">End Date (UTC)</Label>
                      <Input
                        type="date"
                        value={tradingScheduleDraft.endDateUtc}
                        onChange={(event) => setDraftTradingSchedule({ endDateUtc: event.target.value })}
                        className="mt-1 h-8 text-xs font-mono"
                        disabled={!tradingScheduleDraft.enabled}
                      />
                    </div>
                  </div>

                  <div>
                    <Label className="text-[11px] text-muted-foreground">Hard End Timestamp (UTC ISO, optional)</Label>
                    <Input
                      value={tradingScheduleDraft.endAtUtc}
                      onChange={(event) => setDraftTradingSchedule({ endAtUtc: event.target.value })}
                      placeholder="2026-02-28T23:59:59Z"
                      className="mt-1 h-8 text-xs font-mono"
                      disabled={!tradingScheduleDraft.enabled}
                    />
                  </div>
                </div>
              </FlyoutSection>

              {mode === 'edit' && selectedTrader ? (
                <FlyoutSection
                  title="Delete / Disable Bot"
                  icon={AlertTriangle}
                  iconClassName="text-red-500"
                  tone="danger"
                  count={selectedTraderDeleteExposureSummary || 'No open exposure'}
                  defaultOpen={false}
                >
                  <p className="text-xs text-muted-foreground">
                    Open live positions: {selectedTraderOpenLivePositions} • Open shadow positions: {selectedTraderOpenShadowPositions}
                  </p>
                  <p className="text-xs text-muted-foreground">
                    Open live orders: {selectedTraderOpenLiveOrders} • Open shadow orders: {selectedTraderOpenShadowOrders}
                  </p>
                  <Select
                    value={deleteAction}
                    onValueChange={(value) => {
                      setDeleteAction(value as DeleteAction)
                      setDeleteForceConfirm(false)
                      if (value !== 'transfer_delete') setDeleteTransferTargetId(null)
                    }}
                  >
                    <SelectTrigger className="h-8">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="disable">Disable (Recommended)</SelectItem>
                      <SelectItem value="transfer_delete">Delete &amp; Transfer Trades</SelectItem>
                      <SelectItem value="block">Delete (No Open Positions)</SelectItem>
                      <SelectItem value="force_delete">Force Delete (Override Live Checks)</SelectItem>
                    </SelectContent>
                  </Select>
                  {deleteAction === 'transfer_delete' ? (
                    <div className="space-y-2 rounded-md border border-amber-500/40 bg-amber-500/10 p-3">
                      <p className="text-xs font-medium text-amber-700 dark:text-amber-100">
                        Transfer open trades to another bot before deleting
                      </p>
                      <p className="text-[11px] text-amber-700/90 dark:text-amber-100/90">
                        All open positions and active orders from {selectedTrader.name} will be reassigned to the selected bot, then {selectedTrader.name} will be permanently deleted.
                      </p>
                      <Select
                        value={deleteTransferTargetId || ''}
                        onValueChange={(value) => setDeleteTransferTargetId(value || null)}
                      >
                        <SelectTrigger className="h-8">
                          <SelectValue placeholder="Select target bot..." />
                        </SelectTrigger>
                        <SelectContent>
                          {traders
                            .filter((t) => t.id !== selectedTrader.id)
                            .map((t) => (
                              <SelectItem key={t.id} value={t.id}>
                                {t.name}
                              </SelectItem>
                            ))}
                        </SelectContent>
                      </Select>
                    </div>
                  ) : null}
                  {deleteAction === 'force_delete' ? (
                    <div className="space-y-2 rounded-md border border-red-500/40 bg-red-500/10 p-3">
                      <p className="text-xs font-medium text-red-700 dark:text-red-100">
                        {selectedTraderHasLiveDeleteExposure ? 'Force delete with live exposure' : 'Confirm force delete'}
                      </p>
                      <p className="text-[11px] text-red-700/90 dark:text-red-100/90">
                        {selectedTraderHasLiveDeleteExposure
                          ? `This permanently deletes ${selectedTrader.name} while live exposure is still open. Only continue if those positions or orders were already flattened outside Homerun.`
                          : `This permanently deletes ${selectedTrader.name} and bypasses the normal exposure safety check.`}
                      </p>
                      {selectedTraderHasAnyDeleteExposure ? (
                        <p className="text-[11px] text-red-700/90 dark:text-red-100/90">
                          Current exposure: {selectedTraderDeleteExposureSummary}.
                        </p>
                      ) : null}
                      <div className="flex items-center justify-between gap-3 rounded-md border border-red-500/30 bg-background/70 px-3 py-2">
                        <div className="space-y-0.5">
                          <Label className="text-xs text-foreground">Confirm permanent deletion</Label>
                          <p className="text-[11px] text-muted-foreground">
                            {selectedTraderHasLiveDeleteExposure
                              ? 'I understand this can orphan live exposure if it is still open.'
                              : 'I understand this permanently deletes the bot.'}
                          </p>
                        </div>
                        <Switch checked={deleteForceConfirm} onCheckedChange={setDeleteForceConfirm} />
                      </div>
                    </div>
                  ) : null}
                  <Button
                    variant="destructive"
                    className="h-8 text-xs"
                    disabled={
                      deleteTraderMutation.isPending ||
                      (deleteAction === 'force_delete' && !deleteForceConfirm) ||
                      (deleteAction === 'transfer_delete' && !deleteTransferTargetId)
                    }
                    onClick={() => deleteTraderMutation.mutate({
                      traderId: selectedTrader.id,
                      action: deleteAction,
                      ...(deleteAction === 'transfer_delete' && deleteTransferTargetId
                        ? { transferToTraderId: deleteTransferTargetId }
                        : {}),
                    })}
                  >
                    {deleteTraderMutation.isPending
                      ? 'Processing...'
                      : deleteAction === 'disable'
                        ? 'Disable Bot'
                        : deleteAction === 'transfer_delete'
                          ? 'Delete & Transfer'
                          : deleteAction === 'force_delete'
                            ? 'Force Delete Bot'
                            : 'Delete Bot'}
                  </Button>
                </FlyoutSection>
              ) : null}
            </div>
          </ScrollArea>

          <div className="border-t border-border px-4 py-3 flex flex-wrap items-center justify-end gap-2">
            {saveError ? (
              <div className="mr-auto text-xs text-red-500 max-w-[65%] break-words leading-tight" title={saveError}>
                {saveError}
              </div>
            ) : null}
            <Button variant="outline" onClick={() => onOpenChange(false)} disabled={busy}>
              Close
            </Button>
            <Button
              onClick={() => {
                if (mode === 'create') {
                  createTraderMutation.mutate()
                  return
                }
                if (selectedTrader) {
                  saveTraderMutation.mutate(selectedTrader.id)
                }
              }}
              disabled={
                busy ||
                !draftName.trim() ||
                (mode === 'create' && !draftCopyFromTraderId && !effectiveDraftSourceKey)
              }
            >
              {mode === 'create' ? 'Create Bot' : 'Save Bot'}
            </Button>
          </div>
        </div>
      </SheetContent>
    </Sheet>
  )
}

export const TraderConfigFlyout = memo(TraderConfigFlyoutImpl)
