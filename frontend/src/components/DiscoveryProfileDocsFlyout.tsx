import { useState } from 'react'
import {
  BookOpen,
  ChevronDown,
  ChevronRight,
  Code2,
  Zap,
  Copy,
  Check,
  Rocket,
  Shield,
  Database,
  Users,
  Package,
  Target,
  Layers,
} from 'lucide-react'
import { ScrollArea } from './ui/scroll-area'
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from './ui/sheet'
import { cn } from '../lib/utils'

// ==================== TYPES ====================

interface Props {
  open: boolean
  onOpenChange: (open: boolean) => void
}

// ==================== CODE BLOCK ====================

function CodeBlock({ code, className }: { code: string; className?: string }) {
  const [copied, setCopied] = useState(false)
  return (
    <div className={cn('relative group', className)}>
      <pre className="bg-[#1e1e2e] border border-border/30 rounded-md p-3 text-[11px] leading-relaxed font-mono text-gray-300 overflow-x-auto whitespace-pre">
        {code}
      </pre>
      <button
        className="absolute top-1.5 right-1.5 p-1 rounded bg-white/5 hover:bg-white/10 opacity-0 group-hover:opacity-100 transition-opacity"
        onClick={() => {
          navigator.clipboard.writeText(code)
          setCopied(true)
          setTimeout(() => setCopied(false), 1500)
        }}
      >
        {copied ? <Check className="w-3 h-3 text-emerald-400" /> : <Copy className="w-3 h-3 text-gray-400" />}
      </button>
    </div>
  )
}

// ==================== COLLAPSIBLE SECTION ====================

function Section({
  title,
  icon: Icon,
  iconColor,
  defaultOpen = false,
  children,
}: {
  title: string
  icon: React.ComponentType<{ className?: string }>
  iconColor?: string
  defaultOpen?: boolean
  children: React.ReactNode
}) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <div className="border border-border/30 rounded-lg overflow-hidden">
      <button
        className="w-full flex items-center gap-2 px-3 py-2 text-xs font-medium hover:bg-card/50 transition-colors"
        onClick={() => setOpen(!open)}
      >
        {open ? <ChevronDown className="w-3 h-3 text-muted-foreground" /> : <ChevronRight className="w-3 h-3 text-muted-foreground" />}
        <Icon className={cn('w-3.5 h-3.5', iconColor || 'text-muted-foreground')} />
        {title}
      </button>
      {open && <div className="px-3 pb-3 space-y-2 border-t border-border/20">{children}</div>}
    </div>
  )
}

// ==================== FIELD TABLE ====================

function FieldTable({ fields }: { fields: [string, string][] }) {
  return (
    <div className="space-y-0.5">
      {fields.map(([key, desc]) => (
        <div key={key} className="flex gap-2 text-[11px] py-0.5">
          <code className="text-amber-400 font-mono shrink-0">{key}</code>
          <span className="text-muted-foreground">{desc}</span>
        </div>
      ))}
    </div>
  )
}

// ==================== MAIN COMPONENT ====================

export default function DiscoveryProfileDocsFlyout({ open, onOpenChange }: Props) {
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side="right" className="w-[640px] sm:max-w-[640px] p-0 border-l border-border/50">
        <SheetHeader className="px-4 py-3 border-b border-border/30">
          <div className="flex items-center gap-2">
            <BookOpen className="w-4 h-4 text-purple-400" />
            <SheetTitle className="text-sm">Discovery Profile SDK</SheetTitle>
          </div>
          <SheetDescription className="text-[11px] text-muted-foreground">
            BaseDiscoveryProfile API reference and examples
          </SheetDescription>
        </SheetHeader>

        <ScrollArea className="h-[calc(100vh-80px)]">
          <div className="p-4 space-y-2">

            {/* ==================== 1. QUICK START ==================== */}
            <Section title="Quick Start" icon={Rocket} iconColor="text-emerald-400" defaultOpen>
              <p className="text-[11px] text-muted-foreground pt-1">
                Create a custom discovery profile in three steps. Profiles control how wallets are scored and which wallets enter your copy-trade pool.
              </p>

              <div className="space-y-2 pt-1">
                <div className="flex items-start gap-2">
                  <span className="text-[10px] font-bold text-emerald-400 mt-0.5 shrink-0">1.</span>
                  <div className="space-y-1">
                    <p className="text-[11px] text-foreground font-medium">Extend BaseDiscoveryProfile</p>
                    <CodeBlock code={`from services.discovery_profile_sdk import BaseDiscoveryProfile

class MyProfile(BaseDiscoveryProfile):
    name = "My Custom Profile"
    description = "Scores wallets with custom logic"
    default_config = {"min_trades": 10, "boost_recent": True}`} />
                  </div>
                </div>

                <div className="flex items-start gap-2">
                  <span className="text-[10px] font-bold text-emerald-400 mt-0.5 shrink-0">2.</span>
                  <div className="space-y-1">
                    <p className="text-[11px] text-foreground font-medium">Override score_wallet() and/or select_pool()</p>
                    <CodeBlock code={`def score_wallet(self, wallet, trades, rolling):
    base = self._calculate_rank_score(wallet, trades)
    # Boost wallets with recent high win rate
    if rolling.get("rolling_win_rate", 0) > 0.65:
        base["rank_score"] = min(1.0, base["rank_score"] * 1.2)
    return base`} />
                  </div>
                </div>

                <div className="flex items-start gap-2">
                  <span className="text-[10px] font-bold text-emerald-400 mt-0.5 shrink-0">3.</span>
                  <div className="space-y-1">
                    <p className="text-[11px] text-foreground font-medium">Save and set as active profile</p>
                    <p className="text-[11px] text-muted-foreground">
                      Save your file and select it as the active profile in the Discovery settings panel. It will be hot-loaded automatically.
                    </p>
                  </div>
                </div>
              </div>
            </Section>

            {/* ==================== 2. BASE INTERFACE ==================== */}
            <Section title="BaseDiscoveryProfile Interface" icon={Code2} iconColor="text-cyan-400">
              <p className="text-[11px] text-muted-foreground pt-1">
                The base class your profile extends. Override any method to customize behavior.
              </p>

              <CodeBlock code={`class BaseDiscoveryProfile:
    name: str = "Unnamed Profile"
    description: str = ""
    default_config: dict = {}`} className="mt-1" />

              <div className="space-y-3 pt-2">
                <div>
                  <p className="text-[11px] font-medium text-foreground mb-1">Class Attributes</p>
                  <FieldTable fields={[
                    ['name', 'Display name for the profile (shown in UI)'],
                    ['description', 'Short description of the profile behavior'],
                    ['default_config', 'Dict of default configuration values, overridden by user config'],
                  ]} />
                </div>

                <div className="border-t border-border/20 pt-2">
                  <p className="text-[11px] font-medium text-foreground mb-1">score_wallet(wallet, trades, rolling) → dict</p>
                  <p className="text-[11px] text-muted-foreground mb-1">
                    Score an individual wallet. Called once per wallet per scoring cycle.
                  </p>
                  <CodeBlock code={`# Return type:
{
    "rank_score": float,       # 0.0-1.0, overall composite score
    "quality_score": float,    # 0.0-1.0, trade quality metric
    "insider_score": float,    # 0.0-1.0, insider detection score
    "tags": list[str],         # e.g. ["sniper", "whale", "consistent"]
    "recommendation": str      # "strong_follow" | "follow" | "watch" | "avoid"
}`} />
                </div>

                <div className="border-t border-border/20 pt-2">
                  <p className="text-[11px] font-medium text-foreground mb-1">select_pool(scored_wallets, current_pool) → dict</p>
                  <p className="text-[11px] text-muted-foreground mb-1">
                    Select which wallets enter the copy-trade pool. Called after all wallets are scored.
                  </p>
                  <CodeBlock code={`# scored_wallets: list of (wallet, scores) tuples
# current_pool: set of addresses currently in pool
#
# Return type:
{
    "members": list[str],    # addresses to include in new pool
    "removals": list[str],   # addresses explicitly removed
    "pool_size": int         # final pool size
}`} />
                </div>

                <div className="border-t border-border/20 pt-2">
                  <p className="text-[11px] font-medium text-foreground mb-1">configure(config) → None</p>
                  <p className="text-[11px] text-muted-foreground mb-1">
                    Called when user changes profile config in the UI. Merges with default_config.
                  </p>
                  <CodeBlock code={`def configure(self, config):
    # self.config is already merged with default_config
    # Override to validate or transform config values
    if self.config.get("min_trades", 0) < 1:
        self.config["min_trades"] = 1`} />
                </div>
              </div>
            </Section>

            {/* ==================== 3. WALLET DATA CONTRACT ==================== */}
            <Section title="Wallet Data Contract" icon={Database} iconColor="text-amber-400">
              <p className="text-[11px] text-muted-foreground pt-1">
                Fields available on the <code className="text-amber-400 font-mono">wallet</code> dict passed to score_wallet().
              </p>

              <div className="space-y-3 pt-1">
                <div>
                  <p className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider mb-1">Identity</p>
                  <FieldTable fields={[
                    ['address', 'str - Wallet public address'],
                    ['username', 'str | None - Display name if known'],
                    ['cluster_id', 'str | None - Cluster group identifier'],
                    ['tags', 'list[str] - Existing classification tags'],
                    ['is_bot', 'bool - Whether wallet is flagged as a bot'],
                  ]} />
                </div>

                <div>
                  <p className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider mb-1">Performance</p>
                  <FieldTable fields={[
                    ['total_trades', 'int - Lifetime trade count'],
                    ['wins', 'int - Winning trade count'],
                    ['losses', 'int - Losing trade count'],
                    ['win_rate', 'float - Win rate (0.0-1.0)'],
                    ['total_pnl', 'float - Lifetime PnL in USD'],
                    ['realized_pnl', 'float - Realized PnL in USD'],
                    ['unrealized_pnl', 'float - Unrealized PnL in USD'],
                    ['avg_roi', 'float - Average return on investment'],
                  ]} />
                </div>

                <div>
                  <p className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider mb-1">Risk Metrics</p>
                  <FieldTable fields={[
                    ['sharpe_ratio', 'float - Risk-adjusted return (Sharpe)'],
                    ['sortino_ratio', 'float - Downside risk-adjusted return'],
                    ['max_drawdown', 'float - Maximum drawdown (0.0-1.0)'],
                    ['profit_factor', 'float - Gross profit / gross loss'],
                    ['calmar_ratio', 'float - Annual return / max drawdown'],
                  ]} />
                </div>

                <div>
                  <p className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider mb-1">Rolling Window</p>
                  <FieldTable fields={[
                    ['rolling_pnl', 'float - PnL over rolling window'],
                    ['rolling_roi', 'float - ROI over rolling window'],
                    ['rolling_win_rate', 'float - Win rate over rolling window'],
                    ['rolling_trade_count', 'int - Trades in rolling window'],
                    ['trades_1h', 'int - Trade count last 1 hour'],
                    ['trades_24h', 'int - Trade count last 24 hours'],
                    ['last_trade_at', 'str - ISO timestamp of last trade'],
                  ]} />
                </div>

                <div>
                  <p className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider mb-1">Detection</p>
                  <FieldTable fields={[
                    ['anomaly_score', 'float - Anomaly detection score (0.0-1.0)'],
                    ['insider_score', 'float - Insider likelihood score (0.0-1.0)'],
                  ]} />
                </div>

                <div className="border-t border-border/20 pt-2">
                  <p className="text-[11px] font-medium text-foreground mb-1">trades list format</p>
                  <p className="text-[11px] text-muted-foreground mb-1">
                    Each item in the <code className="text-amber-400 font-mono">trades</code> list:
                  </p>
                  <CodeBlock code={`{
    "size": float,          # Position size in USD
    "price": float,         # Entry price
    "side": str,            # "long" | "short"
    "market": str,          # Market identifier (e.g. "SOL-PERP")
    "outcome": str,         # "win" | "loss" | "open"
    "timestamp": str        # ISO 8601 timestamp
}`} />
                </div>

                <div className="border-t border-border/20 pt-2">
                  <p className="text-[11px] font-medium text-foreground mb-1">rolling dict format</p>
                  <p className="text-[11px] text-muted-foreground mb-1">
                    The <code className="text-amber-400 font-mono">rolling</code> dict contains pre-computed rolling window metrics:
                  </p>
                  <CodeBlock code={`{
    "rolling_pnl": float,          # PnL over the window period
    "rolling_roi": float,          # ROI over the window period
    "rolling_win_rate": float,     # Win rate over the window period
    "rolling_trade_count": int,    # Trade count in the window
    "rolling_sharpe": float,       # Sharpe ratio over the window
    "rolling_max_drawdown": float  # Max drawdown over the window
}`} />
                </div>
              </div>
            </Section>

            {/* ==================== 4. SCORING COMPONENTS ==================== */}
            <Section title="Scoring Components" icon={Target} iconColor="text-orange-400">
              <p className="text-[11px] text-muted-foreground pt-1">
                The default <code className="text-amber-400 font-mono">rank_score</code> is a weighted composite of the following components.
                Override <code className="text-amber-400 font-mono">_calculate_rank_score()</code> to change weights or add components.
              </p>

              <div className="mt-2 border border-border/20 rounded-md overflow-hidden">
                <table className="w-full text-[11px]">
                  <thead>
                    <tr className="border-b border-border/20 bg-card/30">
                      <th className="text-left px-2 py-1.5 font-medium text-muted-foreground">Component</th>
                      <th className="text-right px-2 py-1.5 font-medium text-muted-foreground">Weight</th>
                      <th className="text-left px-2 py-1.5 font-medium text-muted-foreground">Description</th>
                    </tr>
                  </thead>
                  <tbody>
                    {[
                      ['Timing Skill', '20.0%', 'Composite of entry/exit timing accuracy and market timing'],
                      ['Sharpe Ratio', '19.5%', 'Risk-adjusted return normalized to 0-1 range'],
                      ['Profit Factor', '16.25%', 'Gross profit / gross loss, capped and normalized'],
                      ['Execution Quality', '15.0%', 'Slippage, fill rate, and order efficiency'],
                      ['Win Rate', '13.0%', 'Confidence-adjusted win rate score'],
                      ['PnL', '9.75%', 'Normalized total PnL with logarithmic scaling'],
                      ['Consistency', '6.5%', 'Variance in returns, lower variance = higher score'],
                    ].map(([name, weight, desc]) => (
                      <tr key={name} className="border-b border-border/10 last:border-0">
                        <td className="px-2 py-1.5">
                          <code className="text-cyan-400 font-mono">{name}</code>
                        </td>
                        <td className="px-2 py-1.5 text-right font-mono text-emerald-400">{weight}</td>
                        <td className="px-2 py-1.5 text-muted-foreground">{desc}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <CodeBlock code={`# Default rank_score calculation (simplified)
rank_score = (
    timing_skill    * 0.200 +
    sharpe_norm     * 0.195 +
    profit_factor   * 0.1625 +
    execution_qual  * 0.150 +
    win_rate_score  * 0.130 +
    pnl_norm        * 0.0975 +
    consistency     * 0.065
)`} className="mt-2" />
            </Section>

            {/* ==================== 5. POOL SELECTION ==================== */}
            <Section title="Pool Selection" icon={Users} iconColor="text-blue-400">
              <p className="text-[11px] text-muted-foreground pt-1">
                The default <code className="text-amber-400 font-mono">_default_pool_selection()</code> applies eligibility gates, tiered ranking, churn guards, and diversity constraints.
              </p>

              <div className="space-y-3 pt-2">
                <div>
                  <p className="text-[11px] font-medium text-foreground mb-1">Eligibility Gates</p>
                  <p className="text-[11px] text-muted-foreground mb-1">Wallets must pass all gates to be considered:</p>
                  <FieldTable fields={[
                    ['min_trades', 'Minimum lifetime trade count (default: 10)'],
                    ['min_win_rate', 'Minimum win rate (default: 0.52)'],
                    ['min_sharpe', 'Minimum Sharpe ratio (default: 0.5)'],
                    ['min_profit_factor', 'Minimum profit factor (default: 1.1)'],
                    ['max_anomaly', 'Maximum anomaly score (default: 0.7)'],
                  ]} />
                </div>

                <div className="border-t border-border/20 pt-2">
                  <p className="text-[11px] font-medium text-foreground mb-1">Tier System</p>
                  <p className="text-[11px] text-muted-foreground mb-1">
                    Eligible wallets are ranked by rank_score and split into tiers:
                  </p>
                  <FieldTable fields={[
                    ['core', 'Top 70% of pool slots - highest rank_score wallets, stable members'],
                    ['rising', 'Bottom 30% of pool slots - next best wallets, rotated more freely'],
                  ]} />
                </div>

                <div className="border-t border-border/20 pt-2">
                  <p className="text-[11px] font-medium text-foreground mb-1">Churn Guard</p>
                  <p className="text-[11px] text-muted-foreground mb-1">
                    Limits pool turnover to prevent excessive rotation:
                  </p>
                  <FieldTable fields={[
                    ['max_hourly_replacement_rate', 'Max fraction of pool replaced per hour (default: 0.15)'],
                    ['replacement_score_cutoff', 'New wallet must beat removed wallet by this margin (default: 0.05)'],
                  ]} />
                </div>

                <div className="border-t border-border/20 pt-2">
                  <p className="text-[11px] font-medium text-foreground mb-1">Cluster Diversity</p>
                  <p className="text-[11px] text-muted-foreground mb-1">
                    Prevents any single cluster from dominating the pool:
                  </p>
                  <FieldTable fields={[
                    ['max_cluster_share', 'Max fraction of pool from one cluster (default: 0.25)'],
                  ]} />
                </div>

                <div className="border-t border-border/20 pt-2">
                  <p className="text-[11px] font-medium text-foreground mb-1">Target Pool Size</p>
                  <p className="text-[11px] text-muted-foreground">
                    Configurable via <code className="text-amber-400 font-mono">pool_size</code> in config (default: 50).
                    The pool will fill up to this size from eligible wallets ranked by score.
                  </p>
                </div>
              </div>
            </Section>

            {/* ==================== 6. INSIDER DETECTION ==================== */}
            <Section title="Insider Detection" icon={Shield} iconColor="text-red-400">
              <p className="text-[11px] text-muted-foreground pt-1">
                The insider score is computed from 10 weighted behavioral signals. Override{' '}
                <code className="text-amber-400 font-mono">_compute_insider_score()</code> to customize.
              </p>

              <div className="mt-2 border border-border/20 rounded-md overflow-hidden">
                <table className="w-full text-[11px]">
                  <thead>
                    <tr className="border-b border-border/20 bg-card/30">
                      <th className="text-left px-2 py-1.5 font-medium text-muted-foreground">Signal</th>
                      <th className="text-right px-2 py-1.5 font-medium text-muted-foreground">Weight</th>
                    </tr>
                  </thead>
                  <tbody>
                    {[
                      ['Early entry frequency', '0.18'],
                      ['Pre-announcement trading', '0.16'],
                      ['Abnormal win rate on new tokens', '0.14'],
                      ['Size concentration before events', '0.12'],
                      ['Correlated timing with deployers', '0.10'],
                      ['Unusual PnL consistency', '0.08'],
                      ['Low-latency trade clustering', '0.07'],
                      ['Cross-wallet fund flows', '0.06'],
                      ['Social signal correlation', '0.05'],
                      ['Anomalous volume timing', '0.04'],
                    ].map(([signal, weight]) => (
                      <tr key={signal} className="border-b border-border/10 last:border-0">
                        <td className="px-2 py-1.5 text-muted-foreground">{signal}</td>
                        <td className="px-2 py-1.5 text-right font-mono text-red-400">{weight}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div className="mt-2">
                <p className="text-[11px] font-medium text-foreground mb-1">Classification Thresholds</p>
                <FieldTable fields={[
                  ['insider_score >= 0.8', '"confirmed_insider" - Excluded from pool by default'],
                  ['insider_score >= 0.6', '"likely_insider" - Flagged, reduced pool priority'],
                  ['insider_score >= 0.4', '"suspected_insider" - Monitored, tagged for review'],
                  ['insider_score < 0.4', '"clean" - No insider indicators'],
                ]} />
              </div>
            </Section>

            {/* ==================== 7. HELPER METHODS ==================== */}
            <Section title="Available Helper Methods" icon={Zap} iconColor="text-yellow-400">
              <p className="text-[11px] text-muted-foreground pt-1">
                All <code className="text-amber-400 font-mono">self._</code> methods available on the base class. Call these from your overrides.
              </p>

              <div className="space-y-3 pt-2">
                <div>
                  <p className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider mb-1">Trade Analysis</p>
                  <FieldTable fields={[
                    ['_calculate_trade_stats(trades)', 'Returns dict with win_rate, avg_size, avg_pnl, total_volume, etc.'],
                    ['_calculate_risk_adjusted_metrics(trades)', 'Returns sharpe, sortino, max_drawdown, profit_factor, calmar'],
                    ['_calculate_rolling_windows(trades, windows)', 'Computes metrics over multiple time windows (1h, 6h, 24h, 7d)'],
                  ]} />
                </div>

                <div className="border-t border-border/20 pt-2">
                  <p className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider mb-1">Scoring</p>
                  <FieldTable fields={[
                    ['_calculate_rank_score(wallet, trades)', 'Full weighted composite score, returns dict with rank_score and components'],
                    ['_compute_timing_skill(trades)', 'Entry/exit timing accuracy (0.0-1.0)'],
                    ['_compute_execution_quality(trades)', 'Slippage and fill efficiency (0.0-1.0)'],
                    ['_score_quality(wallet, trades)', 'Trade quality metric (0.0-1.0)'],
                    ['_score_activity(wallet)', 'Activity recency and frequency (0.0-1.0)'],
                    ['_score_stability(wallet, trades)', 'Return consistency and drawdown control (0.0-1.0)'],
                  ]} />
                </div>

                <div className="border-t border-border/20 pt-2">
                  <p className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider mb-1">Classification</p>
                  <FieldTable fields={[
                    ['_classify_wallet(wallet, scores)', 'Returns tags and recommendation based on scores'],
                    ['_detect_strategies(trades)', 'Identifies trading patterns (scalper, swing, sniper, etc.)'],
                  ]} />
                </div>

                <div className="border-t border-border/20 pt-2">
                  <p className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider mb-1">Pool Selection</p>
                  <FieldTable fields={[
                    ['_default_pool_selection(scored_wallets, current_pool)', 'Full default pool selection with gates, tiers, churn guard'],
                  ]} />
                </div>

                <div className="border-t border-border/20 pt-2">
                  <p className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider mb-1">Insider Detection</p>
                  <FieldTable fields={[
                    ['_compute_insider_score(wallet, trades)', 'Weighted insider signal score (0.0-1.0)'],
                    ['_classify_insider(insider_score)', 'Returns classification string based on thresholds'],
                  ]} />
                </div>

                <div className="border-t border-border/20 pt-2">
                  <p className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider mb-1">Utilities</p>
                  <FieldTable fields={[
                    ['_std_dev(values)', 'Standard deviation of a list of numbers'],
                    ['_parse_timestamp(ts)', 'Parses ISO timestamp to datetime object'],
                    ['_to_float(value, default=0.0)', 'Safe float conversion with fallback'],
                    ['_confidence_adjusted_win_rate(wins, losses)', 'Bayesian-adjusted win rate accounting for sample size'],
                  ]} />
                </div>
              </div>
            </Section>

            {/* ==================== 8. AVAILABLE IMPORTS ==================== */}
            <Section title="Available Imports" icon={Package} iconColor="text-violet-400">
              <p className="text-[11px] text-muted-foreground pt-1">
                These modules are available in the profile sandbox environment.
              </p>

              <CodeBlock code={`# SDK
from services.discovery_profile_sdk import BaseDiscoveryProfile

# Standard library
import math
import statistics
import json
import re
import hashlib
import time
import datetime
from datetime import datetime, timedelta, timezone
from collections import defaultdict, Counter
from typing import Any, Dict, List, Optional, Tuple

# Third-party (pre-installed)
import numpy as np`} className="mt-1" />

              <p className="text-[11px] text-muted-foreground mt-2">
                Network access, file I/O, and subprocess calls are not permitted in the sandbox.
              </p>
            </Section>

            {/* ==================== 9. CODE EXAMPLES ==================== */}
            <Section title="Code Examples" icon={Layers} iconColor="text-pink-400">
              <div className="space-y-4 pt-1">

                <div>
                  <p className="text-[11px] font-medium text-foreground mb-1">
                    Example 1: Custom Scoring with Boosted Recent Activity
                  </p>
                  <p className="text-[11px] text-muted-foreground mb-1">
                    Boosts rank_score for wallets with strong rolling performance.
                  </p>
                  <CodeBlock code={`from services.discovery_profile_sdk import BaseDiscoveryProfile

class RecentActivityProfile(BaseDiscoveryProfile):
    name = "Recent Activity Boost"
    description = "Favors wallets with strong recent performance"
    default_config = {
        "rolling_boost": 1.3,
        "rolling_win_threshold": 0.60,
        "rolling_trade_min": 5,
        "min_trades": 10,
    }

    def score_wallet(self, wallet, trades, rolling):
        base = self._calculate_rank_score(wallet, trades)
        quality = self._score_quality(wallet, trades)
        insider = self._compute_insider_score(wallet, trades)
        tags_info = self._classify_wallet(wallet, base)

        rolling_count = rolling.get("rolling_trade_count", 0)
        rolling_wr = rolling.get("rolling_win_rate", 0)

        if (rolling_count >= self.config["rolling_trade_min"]
                and rolling_wr >= self.config["rolling_win_threshold"]):
            boost = self.config["rolling_boost"]
            base["rank_score"] = min(1.0, base["rank_score"] * boost)
            tags_info["tags"].append("hot_streak")

        return {
            "rank_score": base["rank_score"],
            "quality_score": quality,
            "insider_score": insider,
            "tags": tags_info["tags"],
            "recommendation": tags_info["recommendation"],
        }`} />
                </div>

                <div className="border-t border-border/20 pt-3">
                  <p className="text-[11px] font-medium text-foreground mb-1">
                    Example 2: Strict Pool with Higher Thresholds
                  </p>
                  <p className="text-[11px] text-muted-foreground mb-1">
                    Tighter eligibility gates and smaller pool size for conservative copy-trading.
                  </p>
                  <CodeBlock code={`from services.discovery_profile_sdk import BaseDiscoveryProfile

class StrictPoolProfile(BaseDiscoveryProfile):
    name = "Strict Pool"
    description = "High-bar pool with tight eligibility"
    default_config = {
        "pool_size": 20,
        "min_trades": 50,
        "min_win_rate": 0.60,
        "min_sharpe": 1.5,
        "min_profit_factor": 1.8,
        "max_anomaly": 0.4,
        "max_cluster_share": 0.15,
        "max_hourly_replacement_rate": 0.10,
        "replacement_score_cutoff": 0.08,
    }

    def select_pool(self, scored_wallets, current_pool):
        # Use default selection but with our strict config
        result = self._default_pool_selection(
            scored_wallets, current_pool
        )

        # Extra filter: remove any wallet with insider suspicion
        clean_members = [
            addr for addr, scores in scored_wallets
            if addr in result["members"]
            and scores.get("insider_score", 0) < 0.35
        ]

        return {
            "members": clean_members,
            "removals": [
                a for a in result["members"]
                if a not in clean_members
            ],
            "pool_size": len(clean_members),
        }`} />
                </div>

                <div className="border-t border-border/20 pt-3">
                  <p className="text-[11px] font-medium text-foreground mb-1">
                    Example 3: Insider-Weighted Scoring
                  </p>
                  <p className="text-[11px] text-muted-foreground mb-1">
                    Penalizes rank_score based on insider likelihood, avoids wallets with suspicious patterns.
                  </p>
                  <CodeBlock code={`from services.discovery_profile_sdk import BaseDiscoveryProfile

class InsiderWeightedProfile(BaseDiscoveryProfile):
    name = "Insider-Weighted"
    description = "Deprioritizes wallets with insider signals"
    default_config = {
        "insider_penalty_factor": 0.5,
        "insider_hard_cutoff": 0.7,
        "min_trades": 15,
    }

    def score_wallet(self, wallet, trades, rolling):
        base = self._calculate_rank_score(wallet, trades)
        quality = self._score_quality(wallet, trades)
        insider = self._compute_insider_score(wallet, trades)
        classification = self._classify_insider(insider)
        tags_info = self._classify_wallet(wallet, base)

        rank = base["rank_score"]

        # Hard cutoff for high insider scores
        if insider >= self.config["insider_hard_cutoff"]:
            rank = 0.0
            tags_info["tags"].append("insider_blocked")
            tags_info["recommendation"] = "avoid"
        elif insider >= 0.4:
            # Proportional penalty
            penalty = insider * self.config["insider_penalty_factor"]
            rank = max(0.0, rank - penalty)
            tags_info["tags"].append(f"insider_{classification}")

        return {
            "rank_score": rank,
            "quality_score": quality,
            "insider_score": insider,
            "tags": tags_info["tags"],
            "recommendation": tags_info["recommendation"],
        }`} />
                </div>
              </div>
            </Section>

          </div>
        </ScrollArea>
      </SheetContent>
    </Sheet>
  )
}
