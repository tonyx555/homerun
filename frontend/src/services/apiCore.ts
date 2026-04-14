import { api, getStrategyManagerItems, unwrapApiData, unwrapStrategyManagerPayload } from './apiClient'
import type { MLMarketRuntimePayload } from './apiMachineLearning'

// ==================== TYPES ====================

export interface AIAnalysis {
  overall_score: number
  profit_viability: number
  resolution_safety: number
  execution_feasibility: number
  market_efficiency: number
  recommendation: string
  reasoning: string | null
  risk_factors: string[]
  judged_at: string | null
  resolution_analyses: Array<{
    market_id: string
    clarity_score: number
    risk_score: number
    confidence: number
    recommendation: string
    summary: string
    ambiguities: string[]
    edge_cases: string[]
  }>
}

export interface Opportunity {
  id: string
  stable_id: string
  strategy: string
  strategy_subtype?: string | null
  title: string
  description: string
  total_cost: number
  expected_payout: number
  gross_profit: number
  fee: number
  net_profit: number
  roi_percent: number
  risk_score: number
  risk_factors: string[]
  confidence?: number
  markets: Market[]
  polymarket_url?: string | null
  kalshi_url?: string | null
  event_id?: string
  event_slug?: string
  event_title?: string
  category?: string
  min_liquidity: number
  volume?: number
  max_position_size: number
  detected_at: string
  first_detected_at?: string | null
  last_detected_at?: string | null
  last_priced_at?: string | null
  last_seen_at?: string | null
  resolution_date?: string
  positions_to_take: Position[]
  strategy_context?: Record<string, unknown> | null
  revision?: number | null
  ai_analysis: AIAnalysis | null
}

export interface Market {
  id: string
  condition_id?: string
  conditionId?: string
  slug?: string
  event_slug?: string
  event_ticker?: string
  eventTicker?: string
  url?: string
  market_url?: string
  question: string
  yes_price: number
  no_price: number
  current_yes_price?: number | null
  current_no_price?: number | null
  price_updated_at?: string | null
  price_age_seconds?: number | null
  is_price_fresh?: boolean
  outcome_labels?: string[]
  outcomes?: unknown[]
  outcome_prices?: number[]
  tokens?: Array<{
    token_id?: string
    outcome?: string
    name?: string
    label?: string
    price?: number | null
  }>
  liquidity: number
  volume?: number
  platform?: string  // "polymarket" | "kalshi"
  weather?: WeatherMarketDetails
  price_history?: Array<Record<string, unknown> | unknown[]>
}

export interface WeatherForecastSource {
  source_id: string
  provider: string
  model: string
  value_c: number | null
  value_f: number | null
  probability: number | null
  weight: number | null
  target_time: string | null
}

export interface WeatherMarketDetails {
  location?: string
  metric?: string
  operator?: string
  threshold_c?: number | null
  threshold_c_low?: number | null
  threshold_c_high?: number | null
  raw_threshold?: number | null
  raw_threshold_low?: number | null
  raw_threshold_high?: number | null
  raw_unit?: string | null
  target_time?: string | null
  gfs_probability?: number | null
  ecmwf_probability?: number | null
  gfs_value?: number | null
  ecmwf_value?: number | null
  forecast_sources?: WeatherForecastSource[]
  source_weights?: Record<string, number> | null
  source_count?: number | null
  source_spread_c?: number | null
  source_spread_f?: number | null
  consensus_probability?: number | null
  consensus_temp_c?: number | null
  consensus_temp_f?: number | null
  market_probability?: number | null
  market_implied_temp_c?: number | null
  market_implied_temp_f?: number | null
  agreement?: number | null
  model_confidence?: number | null
}

export interface Position {
  action: string
  outcome: string
  market: string
  price: number
  token_id?: string
  platform?: string  // "polymarket" | "kalshi"
  ticker?: string    // Kalshi market ticker
  market_id?: string
}

export interface ScannerStatus {
  running: boolean
  enabled: boolean
  interval_seconds: number
  last_scan: string | null
  last_fast_scan?: string | null
  last_heavy_scan?: string | null
  opportunities_count: number
  current_activity?: string | null
  lane_watchdogs?: Record<string, unknown> | null
  strategies: Strategy[]
}

export interface Strategy {
  type: string
  name: string
  description: string
  is_plugin?: boolean
  is_system?: boolean
  plugin_id?: string
  plugin_slug?: string
  source_key?: string
  enabled?: boolean
  status?: string  // For plugins: loaded, error, unloaded
  error_message?: string | null
  domain?: 'event_markets' | 'crypto' | string
  timeframe?: string
  sources?: string[]
  validation_status?: 'active' | 'demoted' | 'unknown' | string
  validation_sample_size?: number
}

export interface Wallet {
  address: string
  label: string
  username?: string
  positions: any[]
  recent_trades: any[]
}

export interface SimulationAccount {
  id: string
  name: string
  initial_capital: number
  current_capital: number
  total_pnl: number
  roi_percent: number
  total_trades: number
  winning_trades: number
  losing_trades: number
  win_rate: number
  open_positions: number
  unrealized_pnl: number
  book_value: number
  market_value: number
  created_at: string | null
}

export interface TradingPosition {
  token_id: string
  market_id: string
  market_slug?: string
  event_slug?: string
  market_question: string
  outcome: string
  size: number
  average_cost: number
  current_price: number
  unrealized_pnl: number
  redeemable?: boolean
  counts_as_open?: boolean
  end_date?: string | null
}

export interface SimulationPosition {
  id: string
  market_id: string
  market_slug?: string
  event_slug?: string
  market_question: string
  token_id?: string | null
  side: string
  quantity: number
  entry_price: number
  entry_cost: number
  current_price: number | null
  unrealized_pnl: number
  opened_at: string
  resolution_date: string | null
  status: string
  take_profit_price: number | null
  stop_loss_price: number | null
}

export interface EquityPoint {
  date: string
  equity: number
  pnl: number
  cumulative_pnl: number
  trade_count: number
  trade_id?: string
  status?: string
}

export interface EquityHistorySummary {
  total_trades: number
  winning_trades: number
  losing_trades: number
  open_trades: number
  total_invested: number
  total_returned: number
  realized_pnl: number
  unrealized_pnl: number
  total_pnl: number
  book_value: number
  market_value: number
  max_drawdown: number
  max_drawdown_pct: number
  profit_factor: number
  best_trade: number
  worst_trade: number
  avg_win: number
  avg_loss: number
  win_rate: number
  roi_percent: number
}

export interface EquityHistoryResponse {
  account_id: string
  initial_capital: number
  current_capital: number
  equity_points: EquityPoint[]
  summary: EquityHistorySummary
}

export interface SimulationTrade {
  id: string
  opportunity_id: string
  strategy_type: string
  total_cost: number
  expected_profit: number
  slippage: number
  status: string
  actual_payout?: number
  actual_pnl?: number
  fees_paid: number
  executed_at: string
  resolved_at?: string
  copied_from?: string
}

export interface WalletAnalysis {
  wallet: string
  stats: {
    total_trades: number
    win_rate: number
    total_pnl: number
    avg_roi: number
    max_roi: number
    avg_hold_time_hours?: number
    trade_frequency_per_day?: number
    markets_traded?: number
  }
  strategies_detected: string[]
  anomaly_score: number
  anomalies: Anomaly[]
  is_profitable_pattern: boolean
  recommendation: string
}

export interface Anomaly {
  type: string
  severity: string
  score: number
  description: string
  evidence: Record<string, any>
}

export interface WalletTrade {
  id: string
  market: string
  market_slug: string
  market_title: string
  event_slug: string
  outcome: string
  side: string
  size: number
  price: number
  cost: number
  timestamp: string
  transaction_hash: string
}

export interface WalletPosition {
  market: string
  title: string
  market_slug: string
  event_slug?: string
  outcome: string
  size: number
  avg_price: number
  current_price: number
  cost_basis: number
  current_value: number
  unrealized_pnl: number
  roi_percent: number
}

export interface WalletSummary {
  wallet: string
  summary: {
    total_trades: number
    buys: number
    sells: number
    open_positions: number
    total_invested: number
    total_returned: number
    position_value: number
    realized_pnl: number
    unrealized_pnl: number
    total_pnl: number
    roi_percent: number
  }
}

// ==================== OPPORTUNITIES ====================

export interface OpportunitiesResponse {
  opportunities: Opportunity[]
  total: number
}

export interface OpportunityIdsResponse {
  total: number
  offset: number
  limit: number
  ids: string[]
}

export const getOpportunities = async (params?: {
  source?: 'markets' | 'traders' | 'all'
  min_profit?: number
  max_risk?: number
  strategy?: string
  sub_strategy?: string
  min_liquidity?: number
  search?: string
  category?: string
  sort_by?: string
  sort_dir?: string
  exclude_strategy?: string
  limit?: number
  offset?: number
  include_price_history?: boolean
  block_for_history_backfill?: boolean
}): Promise<OpportunitiesResponse> => {
  const response = await api.get('/opportunities', { params })
  const payload = unwrapApiData(response.data)
  const opportunities = Array.isArray(payload)
    ? payload
    : (payload && typeof payload === 'object' && Array.isArray((payload as { opportunities?: unknown }).opportunities)
      ? (payload as { opportunities: Opportunity[] }).opportunities
      : [])
  const headerTotal = parseInt(response.headers['x-total-count'] || '', 10)
  const payloadTotal = payload && typeof payload === 'object'
    ? Number((payload as { total?: unknown }).total)
    : Number.NaN
  const total = Number.isFinite(headerTotal)
    ? headerTotal
    : (Number.isFinite(payloadTotal) ? payloadTotal : opportunities.length)
  return {
    opportunities,
    total
  }
}

export const getOpportunityIds = async (params?: {
  source?: 'markets' | 'traders' | 'all'
  min_profit?: number
  max_risk?: number
  strategy?: string
  sub_strategy?: string
  min_liquidity?: number
  search?: string
  category?: string
  sort_by?: string
  sort_dir?: string
  exclude_strategy?: string
  limit?: number
  offset?: number
}): Promise<OpportunityIdsResponse> => {
  const { data } = await api.get('/opportunities/ids', { params })
  return unwrapApiData(data)
}

// ==================== CRYPTO MARKETS (independent infrastructure) ====================

export interface CryptoMarketUpcoming {
  id: string
  slug: string
  event_title: string
  start_time: string | null
  end_time: string | null
  up_price: number | null
  down_price: number | null
  best_bid: number | null
  best_ask: number | null
  liquidity: number
  volume: number
}

export interface CryptoMarket {
  id: string
  condition_id: string
  slug: string
  question: string
  asset: string
  timeframe: string
  start_time: string | null
  end_time: string | null
  seconds_left: number | null
  is_live: boolean
  is_current: boolean
  up_price: number | null
  down_price: number | null
  best_bid: number | null
  best_ask: number | null
  spread: number | null
  combined: number | null
  liquidity: number
  volume: number
  volume_24h: number
  series_volume_24h: number
  series_liquidity: number
  last_trade_price: number | null
  clob_token_ids: string[]
  fees_enabled: boolean
  event_slug: string
  event_title: string
  strategy_sdk?: string | null
  strategy_key?: string | null
  strategy?: string | null
  upcoming_markets: CryptoMarketUpcoming[]
  // Attached by API
  oracle_price: number | null
  oracle_source: string | null
  oracle_updated_at_ms: number | null
  oracle_age_seconds: number | null
  oracle_prices_by_source?: Record<string, {
    source: string
    price: number | null
    updated_at_ms: number | null
    age_seconds: number | null
  }>
  price_to_beat: number | null
  oracle_history: { t: number; p: number }[]
  machine_learning?: MLMarketRuntimePayload | null
}

export const getCryptoMarkets = async (params?: { viewer_active?: boolean }): Promise<CryptoMarket[]> => {
  const { data } = await api.get('/crypto/markets', { params })
  return unwrapApiData(data)
}

// ==================== OPPORTUNITY COUNTS ====================

export interface OpportunityCounts {
  strategies: Record<string, number>
  categories: Record<string, number>
  sub_strategies?: Record<string, number>
}

export const getOpportunityCounts = async (params?: {
  source?: 'markets' | 'traders' | 'all'
  min_profit?: number
  max_risk?: number
  min_liquidity?: number
  search?: string
  strategy?: string
  sub_strategy?: string
  category?: string
}): Promise<OpportunityCounts> => {
  const { data } = await api.get('/opportunities/counts', { params })
  return unwrapApiData(data)
}

export const searchPolymarketOpportunities = async (params: {
  q: string
  limit?: number
}): Promise<OpportunitiesResponse> => {
  const response = await api.get('/opportunities/search-polymarket', { params, timeout: 60_000 })
  const total = parseInt(response.headers['x-total-count'] || '0', 10)
  return {
    opportunities: response.data,
    total
  }
}

export const evaluateSearchResults = async (conditionIds: string[]): Promise<{ status: string; count: number; message: string }> => {
  const { data } = await api.post('/opportunities/search-polymarket/evaluate', { condition_ids: conditionIds })
  return unwrapApiData(data)
}

export const triggerScan = async () => {
  const { data } = await api.post('/scan')
  return unwrapApiData(data)
}

export const clearOpportunities = async () => {
  const { data } = await api.delete('/opportunities')
  return unwrapApiData(data)
}

// ==================== SCANNER ====================

export const getScannerStatus = async (): Promise<ScannerStatus> => {
  const { data } = await api.get('/scanner/status')
  return unwrapApiData(data)
}

export const startScanner = async (): Promise<ScannerStatus> => {
  const { data } = await api.post('/scanner/start')
  return unwrapApiData(data)
}

export const pauseScanner = async (): Promise<ScannerStatus> => {
  const { data } = await api.post('/scanner/pause')
  return unwrapApiData(data)
}

export const setScannerInterval = async (intervalSeconds: number): Promise<ScannerStatus> => {
  const { data } = await api.post('/scanner/interval', null, {
    params: { interval_seconds: intervalSeconds }
  })
  return unwrapApiData(data)
}

export const getStrategies = async (): Promise<Strategy[]> => {
  const { data } = await api.get('/strategies')
  return unwrapApiData(data)
}

// ==================== PLUGINS ====================

export interface PluginRuntime {
  slug: string
  class_name: string
  name: string
  description: string
  loaded_at: string
  source_hash: string
  run_count: number
  error_count: number
  total_opportunities: number
  last_run: string | null
  last_error: string | null
}

export interface StrategyPlugin {
  id: string
  slug: string
  source_key: string
  name: string
  description: string | null
  source_code: string
  class_name: string | null
  is_system: boolean
  enabled: boolean
  status: 'unloaded' | 'loaded' | 'error'
  error_message: string | null
  config: Record<string, unknown>
  config_schema: { param_fields: { key: string; label: string; type: string; min?: number; max?: number; options?: string[] }[] } | null
  version: number
  sort_order: number
  created_at: string | null
  updated_at: string | null
  runtime: PluginRuntime | null
}

export interface PluginValidation {
  valid: boolean
  class_name: string | null
  strategy_name: string | null
  strategy_description: string | null
  errors: string[]
  warnings: string[]
}

export const getPlugins = async (): Promise<StrategyPlugin[]> => {
  const { data } = await api.get('/strategy-manager')
  return getStrategyManagerItems(data)
}

export const createPlugin = async (plugin: {
  slug: string
  source_key?: string
  source_code: string
  config?: Record<string, unknown>
  name?: string
  description?: string
  enabled?: boolean
}): Promise<StrategyPlugin> => {
  const { data } = await api.post('/strategy-manager', plugin)
  return unwrapApiData(data)
}

export const updatePlugin = async (
  id: string,
  updates: Partial<{
    slug: string
    source_code: string
    config: Record<string, unknown>
    enabled: boolean
    source_key: string
    name: string
    description: string
    unlock_system: boolean
  }>
): Promise<StrategyPlugin> => {
  const { data } = await api.put(`/strategy-manager/${id}`, updates)
  return unwrapApiData(data)
}

export const deletePlugin = async (id: string): Promise<void> => {
  await api.delete(`/strategy-manager/${id}`)
}

export const validatePlugin = async (source_code: string): Promise<PluginValidation> => {
  const { data } = await api.post('/strategy-manager/validate', { source_code })
  return unwrapApiData(data)
}

export const getPluginTemplate = async (): Promise<{
  template: string
  instructions: string
  available_imports: string[]
}> => {
  const { data } = await api.get('/strategy-manager/template')
  return unwrapApiData(data)
}

export const reloadPlugin = async (id: string): Promise<{
  status: string
  message: string
  runtime: PluginRuntime | null
}> => {
  const { data } = await api.post(`/strategy-manager/${id}/reload`)
  return unwrapApiData(data)
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const getPluginDocs = async (): Promise<Record<string, any>> => {
  const { data } = await api.get('/strategy-manager/docs')
  if (Array.isArray(data)) {
    return {}
  }
  return (data?.items && !Array.isArray(data.items) ? data.items : data) ?? {}
}

export type OpportunityStrategyDefinition = StrategyPlugin

export const getOpportunityStrategies = async (): Promise<OpportunityStrategyDefinition[]> => {
  return getPlugins()
}

export const createOpportunityStrategy = async (payload: {
  slug: string
  source_key: string
  source_code: string
  config?: Record<string, unknown>
  name?: string
  description?: string
  enabled?: boolean
}): Promise<OpportunityStrategyDefinition> => {
  return createPlugin(payload)
}

export const updateOpportunityStrategy = async (
  id: string,
  payload: Partial<{
    slug: string
    source_code: string
    config: Record<string, unknown>
    enabled: boolean
    source_key: string
    name: string
    description: string
  }>
): Promise<OpportunityStrategyDefinition> => {
  return updatePlugin(id, payload)
}

export const validateOpportunityStrategy = async (source_code: string): Promise<PluginValidation> => {
  return validatePlugin(source_code)
}

export const reloadOpportunityStrategy = async (id: string): Promise<{
  status: string
  message: string
  runtime: PluginRuntime | null
}> => {
  return reloadPlugin(id)
}

export const deleteOpportunityStrategy = async (id: string): Promise<void> => {
  return deletePlugin(id)
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const getTraderStrategyDocs = async (): Promise<Record<string, any>> => {
  const { data } = await api.get('/strategy-manager/docs')
  const payload = unwrapStrategyManagerPayload(data)
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return {}
  }
  return payload
}

// ==================== WALLETS ====================

export const getWallets = async (): Promise<Wallet[]> => {
  const { data } = await api.get('/wallets')
  return unwrapApiData(data)
}

export const addWallet = async (address: string, label?: string): Promise<{ status: string; address: string; label: string | null }> => {
  const { data } = await api.post('/wallets', null, {
    params: { address, label }
  })
  return unwrapApiData(data)
}

export const removeWallet = async (address: string): Promise<{ status: string; message: string }> => {
  const { data } = await api.delete(`/wallets/${address}`)
  return unwrapApiData(data)
}

// Recent trades from tracked wallets
export interface RecentTradeFromWallet {
  id?: string
  market?: string
  market_title?: string
  market_slug?: string
  event_slug?: string
  outcome?: string
  side?: string
  size?: number
  price?: number
  cost?: number
  timestamp?: string
  timestamp_iso?: string
  time?: string
  match_time?: string
  created_at?: string
  transaction_hash?: string
  asset_id?: string
  wallet_address: string
  wallet_label: string
  wallet_username?: string
}

export interface RecentTradesResponse {
  trades: RecentTradeFromWallet[]
  total: number
  tracked_wallets: number
  hours_window: number
}

export const getRecentTradesFromWallets = async (params?: {
  limit?: number
  hours?: number
}): Promise<RecentTradesResponse> => {
  const { data } = await api.get('/wallets/recent-trades/all', { params })
  return unwrapApiData(data)
}

export const getWalletPositions = async (address: string): Promise<{ address: string; positions: WalletPosition[] }> => {
  const { data } = await api.get(`/wallets/${address}/positions`)
  return unwrapApiData(data)
}

export const getWalletTrades = async (address: string, limit = 100): Promise<{ address: string; trades: WalletTrade[] }> => {
  const { data } = await api.get(`/wallets/${address}/trades`, {
    params: { limit }
  })
  return unwrapApiData(data)
}

// ==================== MARKETS ====================

export const getMarkets = async (params?: {
  active?: boolean
  limit?: number
  offset?: number
}): Promise<Market[]> => {
  const { data } = await api.get('/markets', { params })
  return unwrapApiData(data)
}

export const getEvents = async (params?: {
  closed?: boolean
  limit?: number
  offset?: number
}): Promise<Record<string, any>[]> => {
  const { data } = await api.get('/events', { params })
  return unwrapApiData(data)
}

// ==================== SIMULATION ====================

export const createSimulationAccount = async (params: {
  name: string
  initial_capital?: number
  max_position_pct?: number
  max_positions?: number
}): Promise<{ account_id: string; name: string; initial_capital: number; message: string }> => {
  const { data } = await api.post('/simulation/accounts', params, { timeout: 20_000 })
  return unwrapApiData(data)
}

export const getSimulationAccounts = async (): Promise<SimulationAccount[]> => {
  const { data } = await api.get('/simulation/accounts')
  return unwrapApiData(data)
}

export const getSimulationAccount = async (accountId: string): Promise<SimulationAccount> => {
  const { data } = await api.get(`/simulation/accounts/${accountId}`)
  return unwrapApiData(data)
}

export const deleteSimulationAccount = async (accountId: string): Promise<{ message: string; account_id: string }> => {
  const { data } = await api.delete(`/simulation/accounts/${accountId}`)
  return unwrapApiData(data)
}

export const getAccountPositions = async (accountId: string): Promise<SimulationPosition[]> => {
  const { data } = await api.get(`/simulation/accounts/${accountId}/positions`)
  return unwrapApiData(data)
}

export const getAccountTrades = async (accountId: string, limit = 50): Promise<SimulationTrade[]> => {
  const { data } = await api.get(`/simulation/accounts/${accountId}/trades`, { params: { limit } })
  return unwrapApiData(data)
}

export const executeOpportunity = async (
  accountId: string,
  opportunityId: string,
  positionSize?: number,
  takeProfitPrice?: number,
  stopLossPrice?: number
): Promise<{ trade_id: string; status: string; total_cost: number; expected_profit: number; slippage: number; message: string }> => {
  const { data } = await api.post(`/simulation/accounts/${accountId}/execute`, {
    opportunity_id: opportunityId,
    position_size: positionSize,
    take_profit_price: takeProfitPrice,
    stop_loss_price: stopLossPrice
  })
  return unwrapApiData(data)
}

export const getAccountPerformance = async (accountId: string): Promise<Record<string, any>> => {
  const { data } = await api.get(`/simulation/accounts/${accountId}/performance`)
  return unwrapApiData(data)
}

export const getAccountEquityHistory = async (accountId: string): Promise<EquityHistoryResponse> => {
  const { data } = await api.get(`/simulation/accounts/${accountId}/equity-history`)
  return unwrapApiData(data)
}

// ==================== ANOMALY DETECTION ====================

export const analyzeWallet = async (address: string): Promise<WalletAnalysis> => {
  const { data } = await api.get(`/anomaly/analyze/${address}`)
  return unwrapApiData(data)
}

export const findProfitableWallets = async (params?: {
  min_trades?: number
  min_win_rate?: number
  min_pnl?: number
  max_anomaly_score?: number
}): Promise<{ count: number; wallets: WalletAnalysis[] }> => {
  const { data } = await api.post('/anomaly/find-profitable', params || {})
  return unwrapApiData(data)
}

export const getAnomalies = async (params?: {
  severity?: string
  anomaly_type?: string
  limit?: number
}): Promise<{ count: number; anomalies: Anomaly[] }> => {
  const { data } = await api.get('/anomaly/anomalies', { params })
  return unwrapApiData(data)
}

export const quickCheckWallet = async (address: string): Promise<{ wallet: string; is_suspicious: boolean; anomaly_score: number; critical_anomalies: number; win_rate: number; total_pnl: number; verdict: string; summary: string }> => {
  const { data } = await api.get(`/anomaly/check/${address}`)
  return unwrapApiData(data)
}

export const getWalletTradesAnalysis = async (address: string, limit = 100): Promise<{ wallet: string; total: number; trades: WalletTrade[] }> => {
  const { data } = await api.get(`/anomaly/wallet/${address}/trades`, { params: { limit } })
  return unwrapApiData(data)
}

export const getWalletPositionsAnalysis = async (address: string): Promise<{ wallet: string; total_positions: number; total_value: number; total_unrealized_pnl: number; positions: WalletPosition[] }> => {
  const { data } = await api.get(`/anomaly/wallet/${address}/positions`)
  return unwrapApiData(data)
}

export const getWalletSummary = async (address: string): Promise<WalletSummary> => {
  const { data } = await api.get(`/anomaly/wallet/${address}/summary`)
  return unwrapApiData(data)
}
