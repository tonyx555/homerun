"""
Discovery Profile Catalog
==========================

System-seeded discovery profiles containing the complete business logic
for wallet scoring and pool selection.  Each profile's ``source_code``
is runnable Python that extends ``BaseDiscoveryProfile`` from the SDK.

Two fixed profiles are maintained:

1. **discovery_scoring** -- All wallet scoring logic (trade stats,
   risk-adjusted metrics, rolling windows, rank score, timing skill,
   execution quality, classification, strategy detection, insider scoring).

2. **pool_selection** -- All pool selection logic (quality scoring,
   activity scoring, stability scoring, selection scoring, eligibility
   gates, tier classification, cluster diversity cap, churn guard).
"""

from __future__ import annotations

from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.exc import OperationalError, ProgrammingError
from models.database import (
    AsyncSessionLocal,
    DiscoveryProfile,
    DiscoveryProfileTombstone,
    DiscoveryProfileVersion,
)
from utils.logger import get_logger
from utils.utcnow import utcnow

logger = get_logger(__name__)


# ======================================================================
# Profile 1: Discovery Scoring
# ======================================================================

_DISCOVERY_SCORING_SOURCE_CODE = '''\
from services.discovery_profile_sdk import BaseDiscoveryProfile

import math
import statistics
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ROLLING_WINDOWS = {
    "1d": timedelta(days=1),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
    "90d": timedelta(days=90),
}

MIN_TRADES_FOR_ANALYSIS = 5
MIN_TRADES_FOR_RISK_METRICS = 3

_WIN_RATE_PRIOR_POSITIONS = 20.0
_WIN_RATE_PRIOR_MEAN = 0.5

# Insider metric weights (simplified 10-component model)
INSIDER_METRIC_WEIGHTS = {
    "win_rate_component": 0.10,
    "timing_alpha_component": 0.16,
    "roi_component": 0.08,
    "brier_component": 0.12,
    "entry_resolution_edge_component": 0.10,
    "position_concentration_component": 0.07,
    "pre_news_timing_component": 0.12,
    "market_selection_edge_component": 0.08,
    "drawdown_behavior_component": 0.05,
    "cluster_correlation_component": 0.08,
    "funding_overlap_proxy_component": 0.04,
}


def _clamp(value, lo, hi):
    if value < lo:
        return lo
    if value > hi:
        return hi
    return value


class DiscoveryScoringProfile(BaseDiscoveryProfile):
    """System profile containing all wallet scoring business logic.

    Computes trade stats, risk-adjusted metrics, rolling windows,
    timing skill, execution quality, classification, strategy detection,
    insider scoring, and the composite rank score.
    """

    name = "Discovery Scoring"
    description = (
        "Complete wallet scoring pipeline: trade stats, risk metrics, "
        "timing skill, execution quality, classification, insider scoring, "
        "and composite rank."
    )

    default_config = {
        # Rank score component weights (must sum to ~1.0)
        "rank_sharpe_weight": 0.195,
        "rank_pf_weight": 0.1625,
        "rank_wr_weight": 0.13,
        "rank_pnl_weight": 0.0975,
        "rank_consistency_weight": 0.065,
        "rank_timing_weight": 0.20,
        "rank_execution_weight": 0.15,
        # Sharpe normalization divisor
        "sharpe_norm_divisor": 5.0,
        # Profit factor normalization divisor
        "pf_norm_divisor": 10.0,
        # PnL log normalization divisor
        "pnl_log_norm_divisor": 5.0,
        # Timing skill window weights
        "timing_5m_weight": 0.35,
        "timing_30m_weight": 0.30,
        "timing_4h_weight": 0.20,
        "timing_24h_weight": 0.15,
        # Execution quality: slippage BPS ceiling for 0 quality
        "execution_slippage_ceiling_bps": 100.0,
        # Bot detection thresholds
        "bot_trades_per_day": 20.0,
        "bot_total_trades": 500,
        # Classification thresholds
        "high_win_rate_threshold": 0.70,
        "high_win_rate_min_positions": 20,
        "whale_pnl_threshold": 10000.0,
        "profitable_pnl_threshold": 1000.0,
        "risk_adjusted_alpha_sharpe": 2.0,
        "strong_edge_pf": 3.0,
        "consistent_min_wr": 0.55,
        "consistent_min_positions": 50,
        # Anomaly detection
        "suspicious_wr_threshold": 0.95,
        "suspicious_wr_min_positions": 20,
        "elevated_anomaly_wr": 0.85,
        "elevated_anomaly_min_positions": 50,
        # Copy candidate thresholds
        "copy_min_wr": 0.60,
        "copy_min_positions": 20,
        "monitor_min_positions": 10,
        # Insider scoring thresholds
        "insider_flagged_threshold": 0.72,
        "insider_flagged_confidence": 0.60,
        "insider_flagged_sample": 25,
        "insider_watch_threshold": 0.60,
        "insider_watch_confidence": 0.50,
        "insider_watch_sample": 15,
    }

    # ==================================================================
    # Public entry point
    # ==================================================================

    def score_wallet(self, wallet, trades, rolling):
        """Run the full scoring pipeline for a single wallet.

        Args:
            wallet: dict with pre-computed fields (address, total_trades,
                    wins, losses, win_rate, total_pnl, sharpe_ratio, etc.)
            trades: raw trade list for rolling windows and strategy detection
            rolling: dict with rolling_pnl, rolling_roi, etc.

        Returns:
            dict with rank_score, tags, recommendation, strategies, insider,
            and all intermediate metrics.
        """
        positions = wallet.get("positions") or []
        closed_positions = wallet.get("closed_positions") or []

        # -- Trade statistics --
        stats = self._calculate_trade_stats(trades, positions)
        market_rois = stats.get("_market_rois", [])
        market_pnls = stats.get("_market_pnls", [])
        days_active = stats.get("days_active", 1)
        total_pnl = stats.get("total_pnl", 0.0)
        total_invested = stats.get("total_invested", 0.0)

        # -- Risk-adjusted metrics --
        risk_metrics = self._calculate_risk_adjusted_metrics(
            market_rois, market_pnls, days_active, total_pnl, total_invested,
        )

        # -- Rolling windows --
        rolling_out = self._calculate_rolling_windows(trades, utcnow())

        # -- Timing skill --
        timing = self._compute_timing_skill(closed_positions)

        # -- Execution quality --
        execution = self._compute_execution_quality(closed_positions)

        # -- Classification --
        merged_stats = {**stats, **risk_metrics, **timing, **execution}
        classification = self._classify_wallet(stats, risk_metrics)

        # -- Strategy detection --
        strategies = self._detect_strategies(trades)

        # -- Rank score --
        rank_input = {
            "sharpe_ratio": risk_metrics.get("sharpe_ratio"),
            "profit_factor": risk_metrics.get("profit_factor"),
            "win_rate": stats.get("win_rate", 0.0),
            "win_rate_score": self._confidence_adjusted_win_rate(
                stats.get("wins", 0), stats.get("losses", 0),
            ),
            "total_pnl": stats.get("total_pnl", 0.0),
            "max_drawdown": risk_metrics.get("max_drawdown"),
            "timing_skill_composite": timing.get("timing_skill_composite", 0.5),
            "execution_quality_score": execution.get("execution_quality_score", 0.5),
        }
        rank_score = self._calculate_rank_score(rank_input)

        # -- Insider scoring --
        insider = self._compute_insider_score(wallet)

        return {
            **merged_stats,
            "rank_score": rank_score,
            "win_rate_score": rank_input["win_rate_score"],
            "strategies": strategies,
            "rolling": rolling_out if rolling_out else rolling,
            **classification,
            "insider": insider,
        }

    # ==================================================================
    # 1. Trade Statistics
    # ==================================================================

    def _calculate_trade_stats(self, trades, positions=None):
        """Group trades by market, compute PnL, win/loss counts."""
        positions = positions or []

        if not trades and not positions:
            return self._empty_stats()

        market_positions = {}
        markets = set()
        total_invested = 0.0
        total_returned = 0.0

        for trade in trades:
            size = float(trade.get("size", 0) or trade.get("amount", 0) or 0)
            price = float(trade.get("price", 0) or 0)
            side = (trade.get("side", "") or "").upper()
            market_id = trade.get("market", trade.get("condition_id", trade.get("asset", "")))
            outcome = trade.get("outcome", trade.get("outcome_index", ""))

            if market_id:
                markets.add(market_id)

            if market_id not in market_positions:
                market_positions[market_id] = {"buys": [], "sells": []}

            cost = size * price

            if side == "BUY":
                total_invested += cost
                market_positions[market_id]["buys"].append({
                    "size": size, "price": price, "cost": cost,
                    "outcome": outcome,
                    "timestamp": trade.get("timestamp", trade.get("created_at", "")),
                })
            elif side == "SELL":
                total_returned += cost
                market_positions[market_id]["sells"].append({
                    "size": size, "price": price, "cost": cost,
                    "outcome": outcome,
                    "timestamp": trade.get("timestamp", trade.get("created_at", "")),
                })

        # Realized PnL from completed trades
        realized_pnl = total_returned - total_invested

        # Unrealized PnL from open positions
        unrealized_pnl = 0.0
        for pos in positions:
            sz = float(pos.get("size", 0) or 0)
            avg_price = float(pos.get("avgPrice", pos.get("avg_price", 0)) or 0)
            current_price = float(
                pos.get("currentPrice", pos.get("curPrice", pos.get("price", 0))) or 0
            )
            unrealized_pnl += sz * (current_price - avg_price)

        total_pnl = realized_pnl + unrealized_pnl

        # Per-market win/loss and ROI series
        wins = 0
        losses = 0
        market_rois = []
        market_pnls = []

        for _mid, pos_data in market_positions.items():
            buy_cost = sum(b["cost"] for b in pos_data["buys"])
            sell_revenue = sum(s["cost"] for s in pos_data["sells"])

            if buy_cost > 0 and sell_revenue > 0:
                m_pnl = sell_revenue - buy_cost
                m_roi = (m_pnl / buy_cost) * 100 if buy_cost > 0 else 0.0
                market_rois.append(m_roi)
                market_pnls.append(m_pnl)

                if m_pnl > 0:
                    wins += 1
                elif m_pnl < 0:
                    losses += 1

        # Fallback when no closed positions detected
        if wins == 0 and losses == 0 and total_invested > 0:
            if total_returned > total_invested * 1.02:
                wins = max(1, len(markets) // 2)
                losses = len(markets) - wins
            elif total_returned < total_invested * 0.98:
                losses = max(1, len(markets) // 2)
                wins = len(markets) - losses

        total_trades = len(trades)
        closed_markets = wins + losses
        win_rate = wins / closed_markets if closed_markets > 0 else 0.0
        days_active = self._compute_days_active(trades)

        avg_roi = (
            sum(market_rois) / len(market_rois)
            if market_rois
            else (total_pnl / total_invested * 100 if total_invested > 0 else 0.0)
        )
        max_roi = max(market_rois) if market_rois else avg_roi
        min_roi = min(market_rois) if market_rois else avg_roi
        roi_std = self._std_dev(market_rois) if len(market_rois) > 1 else 0.0
        avg_position_size = total_invested / total_trades if total_trades > 0 else 0.0

        return {
            "total_trades": total_trades,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "total_pnl": total_pnl,
            "realized_pnl": realized_pnl,
            "unrealized_pnl": unrealized_pnl,
            "total_invested": total_invested,
            "total_returned": total_returned,
            "avg_roi": avg_roi,
            "max_roi": max_roi,
            "min_roi": min_roi,
            "roi_std": roi_std,
            "unique_markets": len(markets),
            "open_positions": len(positions),
            "days_active": days_active,
            "avg_hold_time_hours": 0.0,
            "trades_per_day": total_trades / max(days_active, 1),
            "avg_position_size": avg_position_size,
            "_market_rois": market_rois,
            "_market_pnls": market_pnls,
        }

    def _empty_stats(self):
        return {
            "total_trades": 0, "wins": 0, "losses": 0,
            "resolved_positions": 0, "win_rate": 0.0,
            "win_rate_score": 0.5, "win_rate_confidence": 0.0,
            "total_pnl": 0.0, "realized_pnl": 0.0, "unrealized_pnl": 0.0,
            "total_invested": 0.0, "total_returned": 0.0,
            "avg_roi": 0.0, "max_roi": 0.0, "min_roi": 0.0, "roi_std": 0.0,
            "unique_markets": 0, "open_positions": 0, "days_active": 0,
            "avg_hold_time_hours": 0.0, "trades_per_day": 0.0,
            "avg_position_size": 0.0, "_market_rois": [], "_market_pnls": [],
        }

    # ==================================================================
    # 2. Risk-Adjusted Metrics
    # ==================================================================

    def _calculate_risk_adjusted_metrics(
        self, market_rois, market_pnls, days_active, total_pnl=0.0, total_invested=0.0,
    ):
        """Compute Sharpe, Sortino, max drawdown, profit factor, Calmar."""
        result = {
            "sharpe_ratio": None,
            "sortino_ratio": None,
            "max_drawdown": None,
            "profit_factor": None,
            "calmar_ratio": None,
        }

        if len(market_rois) < MIN_TRADES_FOR_RISK_METRICS:
            return result

        # -- Sharpe Ratio --
        mean_return = sum(market_rois) / len(market_rois)
        std_return = self._std_dev(market_rois)

        if std_return > 0:
            result["sharpe_ratio"] = mean_return / std_return
        else:
            result["sharpe_ratio"] = float("inf") if mean_return > 0 else 0.0

        # -- Sortino Ratio --
        negative_returns = [r for r in market_rois if r < 0]
        if negative_returns:
            downside_variance = sum(r ** 2 for r in negative_returns) / len(negative_returns)
            downside_deviation = math.sqrt(downside_variance)
            if downside_deviation > 0:
                result["sortino_ratio"] = mean_return / downside_deviation
            else:
                result["sortino_ratio"] = float("inf") if mean_return > 0 else 0.0
        else:
            result["sortino_ratio"] = float("inf") if mean_return > 0 else 0.0

        # -- Max Drawdown --
        # Largest peak-to-trough decline in cumulative PnL
        if market_pnls:
            cumulative = 0.0
            peak = 0.0
            max_dd = 0.0
            for pnl in market_pnls:
                cumulative += pnl
                if cumulative > peak:
                    peak = cumulative
                drawdown = peak - cumulative
                if drawdown > max_dd:
                    max_dd = drawdown

            if peak > 0:
                result["max_drawdown"] = max_dd / peak
            elif max_dd > 0:
                base = total_invested if total_invested > 0 else abs(sum(market_pnls))
                result["max_drawdown"] = max_dd / base if base > 0 else 0.0
            else:
                result["max_drawdown"] = 0.0

        # -- Profit Factor --
        gross_profit = sum(p for p in market_pnls if p > 0)
        gross_loss = sum(p for p in market_pnls if p < 0)

        if gross_loss < 0:
            result["profit_factor"] = gross_profit / abs(gross_loss)
        elif gross_profit > 0:
            result["profit_factor"] = float("inf")
        else:
            result["profit_factor"] = 0.0

        # -- Calmar Ratio --
        days = max(days_active, 1)
        if total_invested > 0 and days > 0:
            annualized_return = (total_pnl / total_invested) * (365.0 / days)
            max_dd_val = result.get("max_drawdown") or 0.0
            if max_dd_val > 0:
                result["calmar_ratio"] = annualized_return / max_dd_val
            else:
                result["calmar_ratio"] = None
        else:
            result["calmar_ratio"] = None

        return result

    # ==================================================================
    # 3. Rolling Time Windows (1d / 7d / 30d / 90d)
    # ==================================================================

    def _calculate_rolling_windows(self, trades, current_time):
        """Compute metrics over rolling 1d/7d/30d/90d windows."""
        rolling_pnl = {}
        rolling_roi = {}
        rolling_win_rate = {}
        rolling_trade_count = {}
        rolling_sharpe = {}

        for label, delta in ROLLING_WINDOWS.items():
            cutoff = current_time - delta
            window_trades = self._filter_trades_after(trades, cutoff)

            if not window_trades:
                rolling_pnl[label] = 0.0
                rolling_roi[label] = 0.0
                rolling_win_rate[label] = 0.0
                rolling_trade_count[label] = 0
                rolling_sharpe[label] = None
                continue

            stats = self._calculate_trade_stats(window_trades)
            rois = stats.get("_market_rois", [])

            rolling_pnl[label] = stats["total_pnl"]
            rolling_roi[label] = stats["avg_roi"]
            rolling_win_rate[label] = stats["win_rate"]
            rolling_trade_count[label] = stats["total_trades"]

            if len(rois) >= 2:
                mean_r = sum(rois) / len(rois)
                std_r = self._std_dev(rois)
                rolling_sharpe[label] = (mean_r / std_r) if std_r > 0 else None
            else:
                rolling_sharpe[label] = None

        return {
            "rolling_pnl": rolling_pnl,
            "rolling_roi": rolling_roi,
            "rolling_win_rate": rolling_win_rate,
            "rolling_trade_count": rolling_trade_count,
            "rolling_sharpe": rolling_sharpe,
        }

    # ==================================================================
    # 4. Composite Rank Score
    # ==================================================================

    def _calculate_rank_score(self, metrics):
        """Weighted composite: Sharpe, PF, WR, PnL, consistency, timing, execution.

        Weights are read from self.config so the form editor can tune them
        without modifying source code.
        """
        cfg = self.config

        # -- Normalize Sharpe --
        sharpe = metrics.get("sharpe_ratio")
        sharpe_divisor = float(cfg.get("sharpe_norm_divisor", 5.0))
        if sharpe is None or not math.isfinite(sharpe):
            sharpe_norm = 0.5
        else:
            sharpe_norm = max(0.0, min(sharpe / sharpe_divisor, 1.0))

        # -- Normalize Profit Factor --
        pf = metrics.get("profit_factor")
        pf_divisor = float(cfg.get("pf_norm_divisor", 10.0))
        if pf is None or not math.isfinite(pf):
            pf_norm = 0.5
        else:
            pf_norm = max(0.0, min(pf / pf_divisor, 1.0))

        # -- Normalize Win Rate --
        win_rate = metrics.get("win_rate_score", metrics.get("win_rate", 0.0))
        wr_norm = max(0.0, min(win_rate, 1.0))

        # -- Normalize PnL (log scale) --
        total_pnl = metrics.get("total_pnl", 0.0)
        pnl_divisor = float(cfg.get("pnl_log_norm_divisor", 5.0))
        if total_pnl > 0:
            pnl_norm = max(0.0, min(math.log10(total_pnl + 1) / pnl_divisor, 1.0))
        else:
            pnl_norm = 0.0

        # -- Normalize Consistency (inverse drawdown) --
        max_dd = metrics.get("max_drawdown")
        if max_dd is not None and math.isfinite(max_dd):
            consistency_norm = max(0.0, 1.0 - min(max_dd, 1.0))
        else:
            consistency_norm = 0.5

        # -- Timing skill --
        timing_skill = metrics.get("timing_skill_composite", 0.5)
        timing_norm = max(0.0, min(timing_skill, 1.0))

        # -- Execution quality --
        execution_quality = metrics.get("execution_quality_score", 0.5)
        execution_norm = max(0.0, min(execution_quality, 1.0))

        # -- Weighted composite --
        rank_score = (
            float(cfg.get("rank_sharpe_weight", 0.195)) * sharpe_norm
            + float(cfg.get("rank_pf_weight", 0.1625)) * pf_norm
            + float(cfg.get("rank_wr_weight", 0.13)) * wr_norm
            + float(cfg.get("rank_pnl_weight", 0.0975)) * pnl_norm
            + float(cfg.get("rank_consistency_weight", 0.065)) * consistency_norm
            + float(cfg.get("rank_timing_weight", 0.20)) * timing_norm
            + float(cfg.get("rank_execution_weight", 0.15)) * execution_norm
        )

        return max(0.0, min(rank_score, 1.0))

    # ==================================================================
    # 4b. Timing Skill (4-window favorable movement)
    # ==================================================================

    def _compute_timing_skill(self, positions):
        """Measure how often price moves favorably within 5m/30m/4h/24h windows."""
        cfg = self.config
        WINDOWS = [
            ("timing_5m", 5),
            ("timing_30m", 30),
            ("timing_4h", 240),
            ("timing_24h", 1440),
        ]
        WEIGHTS = {
            "timing_5m": float(cfg.get("timing_5m_weight", 0.35)),
            "timing_30m": float(cfg.get("timing_30m_weight", 0.30)),
            "timing_4h": float(cfg.get("timing_4h_weight", 0.20)),
            "timing_24h": float(cfg.get("timing_24h_weight", 0.15)),
        }
        DEFAULT = {
            "timing_5m": 0.5, "timing_30m": 0.5,
            "timing_4h": 0.5, "timing_24h": 0.5,
            "timing_skill_composite": 0.5,
        }

        closed = [
            p for p in positions
            if isinstance(p, dict) and self._to_float(p.get("realizedPnl")) != 0.0
        ]
        if len(closed) < MIN_TRADES_FOR_ANALYSIS:
            return DEFAULT

        favorable_counts = {key: 0 for key, _ in WINDOWS}
        total_counted = {key: 0 for key, _ in WINDOWS}

        for pos in closed:
            entry_price = self._to_float(pos.get("avgPrice", pos.get("avg_price", 0)))
            exit_price = self._to_float(pos.get("exitPrice", pos.get("exit_price", 0)))
            realized_pnl = self._to_float(pos.get("realizedPnl", 0))
            initial_value = self._to_float(pos.get("initialValue", 0))
            size = self._to_float(pos.get("size", 0))

            if entry_price <= 0:
                continue

            # Infer exit price from PnL if missing
            if exit_price <= 0 and initial_value > 0 and size > 0:
                exit_price = (initial_value + realized_pnl) / size
            if exit_price <= 0:
                continue

            is_buy = realized_pnl > 0 if exit_price > entry_price else realized_pnl <= 0
            price_delta = exit_price - entry_price

            # Compute hold duration in minutes
            hold_minutes = 0.0
            start_ts = self._parse_timestamp(
                pos.get("startTimestamp", pos.get("createdAt", pos.get("created_at")))
            )
            end_ts = self._parse_timestamp(
                pos.get("endTimestamp", pos.get("closedAt", pos.get("closed_at")))
            )
            if start_ts and end_ts and end_ts > start_ts:
                hold_minutes = (end_ts - start_ts).total_seconds() / 60.0
            if hold_minutes <= 0:
                hold_dur_hours = self._to_float(
                    pos.get("holdDuration", pos.get("hold_duration_hours", 0))
                )
                hold_minutes = hold_dur_hours * 60.0
            if hold_minutes <= 0:
                hold_minutes = 1440.0  # default 1 day

            for key, window_minutes in WINDOWS:
                if hold_minutes <= 0:
                    continue

                if hold_minutes <= window_minutes:
                    interpolated_price = exit_price
                else:
                    fraction = window_minutes / hold_minutes
                    interpolated_price = entry_price + price_delta * fraction

                movement = interpolated_price - entry_price
                favorable = movement > 0 if is_buy else movement < 0

                total_counted[key] += 1
                if favorable:
                    favorable_counts[key] += 1

        result = {}
        for key, _ in WINDOWS:
            if total_counted[key] > 0:
                result[key] = favorable_counts[key] / total_counted[key]
            else:
                result[key] = 0.5

        composite = sum(result[key] * WEIGHTS[key] for key in WEIGHTS)
        result["timing_skill_composite"] = composite
        return result

    # ==================================================================
    # 4c. Execution Quality (slippage BPS)
    # ==================================================================

    def _compute_execution_quality(self, positions):
        """Measure execution quality via average slippage in basis points."""
        ceiling = float(self.config.get("execution_slippage_ceiling_bps", 100.0))
        DEFAULT = {"avg_slippage_bps": 0.0, "median_slippage_bps": 0.0, "execution_quality_score": 0.5}

        slippages = []
        for pos in positions:
            if not isinstance(pos, dict):
                continue

            fill_price = self._to_float(pos.get("avgPrice", pos.get("avg_price", 0)))
            if fill_price <= 0:
                continue

            # Determine mid price from multiple sources
            mid_price = 0.0
            yes_price = self._to_float(pos.get("yesPrice", pos.get("yes_price", 0)))
            no_price = self._to_float(pos.get("noPrice", pos.get("no_price", 0)))
            if yes_price > 0 and no_price > 0:
                outcome = str(pos.get("outcome", pos.get("outcome_index", ""))).strip().upper()
                mid_price = no_price if outcome in ("NO", "1") else yes_price
            elif yes_price > 0:
                mid_price = yes_price
            elif no_price > 0:
                mid_price = no_price

            if mid_price <= 0:
                bid = self._to_float(pos.get("bestBid", pos.get("bid", 0)))
                ask = self._to_float(pos.get("bestAsk", pos.get("ask", 0)))
                if bid > 0 and ask > 0:
                    mid_price = (bid + ask) / 2.0
                elif bid > 0:
                    mid_price = bid
                elif ask > 0:
                    mid_price = ask

            if mid_price <= 0:
                initial_value = self._to_float(pos.get("initialValue", 0))
                sz = self._to_float(pos.get("size", 0))
                if initial_value > 0 and sz > 0:
                    mid_price = initial_value / sz

            if mid_price <= 0:
                continue

            slippage_bps = abs(fill_price - mid_price) / mid_price * 10000.0
            slippages.append(slippage_bps)

        if len(slippages) < MIN_TRADES_FOR_ANALYSIS:
            return DEFAULT

        avg_slippage = sum(slippages) / len(slippages)
        median_slippage = statistics.median(slippages)
        execution_quality_score = 1.0 - min(1.0, avg_slippage / ceiling)

        return {
            "avg_slippage_bps": avg_slippage,
            "median_slippage_bps": median_slippage,
            "execution_quality_score": max(0.0, min(1.0, execution_quality_score)),
        }

    # ==================================================================
    # 5. Classification (bot detection, tags, recommendation)
    # ==================================================================

    def _classify_wallet(self, stats, risk_metrics):
        """Classify wallet: anomaly score, bot flag, profitability, tags, recommendation."""
        cfg = self.config
        tags = []
        anomaly_score = 0.0

        win_rate = stats.get("win_rate", 0.0)
        wins = stats.get("wins", 0)
        losses = stats.get("losses", 0)
        resolved_positions = int(wins or 0) + int(losses or 0)
        adjusted_win_rate = self._confidence_adjusted_win_rate(wins, losses)
        total_pnl = stats.get("total_pnl", 0.0)
        total_trades = stats.get("total_trades", 0)
        trades_per_day = stats.get("trades_per_day", 0.0)
        sharpe = risk_metrics.get("sharpe_ratio")
        profit_factor = risk_metrics.get("profit_factor")

        # -- Bot detection --
        is_bot = False
        if trades_per_day > float(cfg.get("bot_trades_per_day", 20.0)):
            is_bot = True
            tags.append("bot")
        if total_trades > int(cfg.get("bot_total_trades", 500)):
            is_bot = True
            if "bot" not in tags:
                tags.append("bot")

        # -- Profitability --
        is_profitable = total_pnl > 0 and win_rate > 0.5

        # -- Performance tags --
        if adjusted_win_rate >= float(cfg.get("high_win_rate_threshold", 0.70)) and resolved_positions >= int(cfg.get("high_win_rate_min_positions", 20)):
            tags.append("high_win_rate")
        if total_pnl >= float(cfg.get("whale_pnl_threshold", 10000.0)):
            tags.append("whale")
        elif total_pnl >= float(cfg.get("profitable_pnl_threshold", 1000.0)):
            tags.append("profitable")
        if sharpe is not None and math.isfinite(sharpe) and sharpe >= float(cfg.get("risk_adjusted_alpha_sharpe", 2.0)):
            tags.append("risk_adjusted_alpha")
        if profit_factor is not None and math.isfinite(profit_factor) and profit_factor >= float(cfg.get("strong_edge_pf", 3.0)):
            tags.append("strong_edge")
        if adjusted_win_rate >= float(cfg.get("consistent_min_wr", 0.55)) and total_pnl > 0 and resolved_positions >= int(cfg.get("consistent_min_positions", 50)):
            tags.append("consistent")

        # -- Anomaly score --
        if adjusted_win_rate >= float(cfg.get("suspicious_wr_threshold", 0.95)) and resolved_positions >= int(cfg.get("suspicious_wr_min_positions", 20)):
            anomaly_score = max(anomaly_score, 0.8)
            tags.append("suspicious_win_rate")
        if adjusted_win_rate >= float(cfg.get("elevated_anomaly_wr", 0.85)) and resolved_positions >= int(cfg.get("elevated_anomaly_min_positions", 50)):
            anomaly_score = max(anomaly_score, 0.5)

        # -- Recommendation --
        if anomaly_score >= 0.7:
            recommendation = "avoid"
        elif is_profitable and adjusted_win_rate >= float(cfg.get("copy_min_wr", 0.60)) and resolved_positions >= int(cfg.get("copy_min_positions", 20)):
            recommendation = "copy_candidate"
        elif is_profitable and resolved_positions >= int(cfg.get("monitor_min_positions", 10)):
            recommendation = "monitor"
        elif total_trades < MIN_TRADES_FOR_ANALYSIS:
            recommendation = "unanalyzed"
        else:
            recommendation = "monitor"

        return {
            "anomaly_score": anomaly_score,
            "is_bot": is_bot,
            "is_profitable": is_profitable,
            "recommendation": recommendation,
            "tags": tags,
        }

    # ==================================================================
    # 6. Strategy Detection
    # ==================================================================

    def _detect_strategies(self, trades):
        """Detect trading strategies: negrisk, arb, automated, scalping."""
        strategies = set()

        market_groups = {}
        for trade in trades:
            market_id = trade.get("market", trade.get("condition_id", ""))
            if market_id:
                market_groups.setdefault(market_id, []).append(trade)

        # NegRisk date sweep: 3+ trades in a single market
        for _mid, mtrades in market_groups.items():
            if len(mtrades) >= 3:
                strategies.add("negrisk_date_sweep")

        # Basic arbitrage: same market, opposite outcomes
        for mtrades in market_groups.values():
            sides = set(t.get("outcome", t.get("side", "")) for t in mtrades)
            if "Yes" in sides and "No" in sides:
                strategies.add("basic_arbitrage")
            if "YES" in sides and "NO" in sides:
                strategies.add("basic_arbitrage")

        # Automated / high-frequency
        if len(trades) > 100:
            strategies.add("automated_trading")

        # Scalping (many trades per market)
        for mtrades in market_groups.values():
            if len(mtrades) >= 6:
                strategies.add("scalping")
                break

        return list(strategies)

    # ==================================================================
    # 7. Insider Scoring (simplified 10-component model)
    # ==================================================================

    def _compute_insider_score(self, wallet):
        """Score insider likelihood using available metric components.

        Uses a weighted model over 10 signal components.  Missing
        components are skipped and the score is adjusted for coverage.
        """
        cfg = self.config
        components = wallet.get("insider_components") or {}

        wins = int(wallet.get("wins") or 0)
        losses = int(wallet.get("losses") or 0)
        resolved_sample = wins + losses

        # Backfill simple components from wallet-level metrics
        if "win_rate_component" not in components and resolved_sample >= 15:
            components["win_rate_component"] = _clamp(
                float(wallet.get("win_rate") or 0.0), 0.0, 1.0,
            )

        if "roi_component" not in components and resolved_sample >= 15:
            avg_roi = float(wallet.get("avg_roi") or 0.0)
            components["roi_component"] = _clamp((avg_roi + 20.0) / 80.0, 0.0, 1.0)

        if "drawdown_behavior_component" not in components:
            max_drawdown = float(wallet.get("max_drawdown") or 0.0)
            dd_inv = _clamp(1.0 - _clamp(max_drawdown, 0.0, 1.0), 0.0, 1.0)
            timing = components.get("timing_alpha_component")
            if timing is None:
                components["drawdown_behavior_component"] = dd_inv
            else:
                components["drawdown_behavior_component"] = _clamp(
                    (0.60 * dd_inv) + (0.40 * _clamp(timing, 0.0, 1.0)), 0.0, 1.0,
                )

        # Weighted sum over available components
        weighted_sum = 0.0
        available_weight_sum = 0.0
        for metric_key, weight in INSIDER_METRIC_WEIGHTS.items():
            value = components.get(metric_key)
            if value is None:
                continue
            weighted_sum += weight * _clamp(float(value), 0.0, 1.0)
            available_weight_sum += weight

        sample_factor = _clamp(resolved_sample / 40.0, 0.0, 1.0)
        insider_confidence = available_weight_sum * sample_factor

        base_score = (weighted_sum / available_weight_sum) if available_weight_sum > 0 else 0.0
        confidence_penalty = _clamp(insider_confidence / 0.75, 0.0, 1.0)
        insider_score = _clamp(base_score * confidence_penalty, 0.0, 1.0)

        classification = self._classify_insider(insider_score, insider_confidence, resolved_sample)

        return {
            "insider_score": round(insider_score, 6),
            "insider_confidence": round(insider_confidence, 6),
            "sample_size": resolved_sample,
            "classification": classification,
            "components": {
                key: (_clamp(float(v), 0.0, 1.0) if isinstance(v, (int, float)) else None)
                for key, v in components.items()
            },
        }

    def _classify_insider(self, score, confidence, sample_size):
        """Classify insider status based on score, confidence, sample size."""
        cfg = self.config
        if (
            score >= float(cfg.get("insider_flagged_threshold", 0.72))
            and confidence >= float(cfg.get("insider_flagged_confidence", 0.60))
            and sample_size >= int(cfg.get("insider_flagged_sample", 25))
        ):
            return "flagged_insider"
        if (
            score >= float(cfg.get("insider_watch_threshold", 0.60))
            and confidence >= float(cfg.get("insider_watch_confidence", 0.50))
            and sample_size >= int(cfg.get("insider_watch_sample", 15))
        ):
            return "watch_insider"
        return "none"
'''

_DISCOVERY_SCORING_CONFIG: dict = {
    "rank_sharpe_weight": 0.195,
    "rank_pf_weight": 0.1625,
    "rank_wr_weight": 0.13,
    "rank_pnl_weight": 0.0975,
    "rank_consistency_weight": 0.065,
    "rank_timing_weight": 0.20,
    "rank_execution_weight": 0.15,
    "sharpe_norm_divisor": 5.0,
    "pf_norm_divisor": 10.0,
    "pnl_log_norm_divisor": 5.0,
    "timing_5m_weight": 0.35,
    "timing_30m_weight": 0.30,
    "timing_4h_weight": 0.20,
    "timing_24h_weight": 0.15,
    "execution_slippage_ceiling_bps": 100.0,
    "bot_trades_per_day": 20.0,
    "bot_total_trades": 500,
    "high_win_rate_threshold": 0.70,
    "high_win_rate_min_positions": 20,
    "whale_pnl_threshold": 10000.0,
    "profitable_pnl_threshold": 1000.0,
    "risk_adjusted_alpha_sharpe": 2.0,
    "strong_edge_pf": 3.0,
    "consistent_min_wr": 0.55,
    "consistent_min_positions": 50,
    "suspicious_wr_threshold": 0.95,
    "suspicious_wr_min_positions": 20,
    "elevated_anomaly_wr": 0.85,
    "elevated_anomaly_min_positions": 50,
    "copy_min_wr": 0.60,
    "copy_min_positions": 20,
    "monitor_min_positions": 10,
    "insider_flagged_threshold": 0.72,
    "insider_flagged_confidence": 0.60,
    "insider_flagged_sample": 25,
    "insider_watch_threshold": 0.60,
    "insider_watch_confidence": 0.50,
    "insider_watch_sample": 15,
}

_DISCOVERY_SCORING_CONFIG_SCHEMA: dict = {
    "param_fields": [
        # Rank score weights
        {"key": "rank_sharpe_weight", "label": "Sharpe Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "rank_pf_weight", "label": "Profit Factor Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "rank_wr_weight", "label": "Win Rate Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "rank_pnl_weight", "label": "PnL Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "rank_consistency_weight", "label": "Consistency Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "rank_timing_weight", "label": "Timing Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "rank_execution_weight", "label": "Execution Weight", "type": "number", "min": 0.0, "max": 1.0},
        # Normalization
        {"key": "sharpe_norm_divisor", "label": "Sharpe Normalization Divisor", "type": "number", "min": 0.1, "max": 20.0},
        {"key": "pf_norm_divisor", "label": "Profit Factor Normalization Divisor", "type": "number", "min": 0.1, "max": 50.0},
        {"key": "pnl_log_norm_divisor", "label": "PnL Log Normalization Divisor", "type": "number", "min": 0.1, "max": 20.0},
        # Timing weights
        {"key": "timing_5m_weight", "label": "5min Timing Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "timing_30m_weight", "label": "30min Timing Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "timing_4h_weight", "label": "4h Timing Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "timing_24h_weight", "label": "24h Timing Weight", "type": "number", "min": 0.0, "max": 1.0},
        # Execution
        {"key": "execution_slippage_ceiling_bps", "label": "Execution Slippage Ceiling (BPS)", "type": "number", "min": 1.0, "max": 1000.0},
        # Bot detection
        {"key": "bot_trades_per_day", "label": "Bot: Trades/Day Threshold", "type": "number", "min": 1.0, "max": 500.0},
        {"key": "bot_total_trades", "label": "Bot: Total Trades Threshold", "type": "integer", "min": 10, "max": 10000},
        # Classification
        {"key": "high_win_rate_threshold", "label": "High Win Rate Threshold", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "high_win_rate_min_positions", "label": "High Win Rate Min Positions", "type": "integer", "min": 1, "max": 1000},
        {"key": "whale_pnl_threshold", "label": "Whale PnL Threshold", "type": "number", "min": 0.0, "max": 1000000.0},
        {"key": "profitable_pnl_threshold", "label": "Profitable PnL Threshold", "type": "number", "min": 0.0, "max": 100000.0},
        {"key": "risk_adjusted_alpha_sharpe", "label": "Risk-Adjusted Alpha Min Sharpe", "type": "number", "min": 0.0, "max": 10.0},
        {"key": "strong_edge_pf", "label": "Strong Edge Min Profit Factor", "type": "number", "min": 0.0, "max": 20.0},
        {"key": "consistent_min_wr", "label": "Consistent Min Win Rate", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "consistent_min_positions", "label": "Consistent Min Positions", "type": "integer", "min": 1, "max": 10000},
        # Anomaly
        {"key": "suspicious_wr_threshold", "label": "Suspicious Win Rate Threshold", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "suspicious_wr_min_positions", "label": "Suspicious WR Min Positions", "type": "integer", "min": 1, "max": 1000},
        {"key": "elevated_anomaly_wr", "label": "Elevated Anomaly Win Rate", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "elevated_anomaly_min_positions", "label": "Elevated Anomaly Min Positions", "type": "integer", "min": 1, "max": 10000},
        # Copy candidate
        {"key": "copy_min_wr", "label": "Copy Candidate Min Win Rate", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "copy_min_positions", "label": "Copy Candidate Min Positions", "type": "integer", "min": 1, "max": 1000},
        {"key": "monitor_min_positions", "label": "Monitor Min Positions", "type": "integer", "min": 1, "max": 1000},
        # Insider thresholds
        {"key": "insider_flagged_threshold", "label": "Insider Flagged Score Threshold", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "insider_flagged_confidence", "label": "Insider Flagged Confidence", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "insider_flagged_sample", "label": "Insider Flagged Min Sample", "type": "integer", "min": 1, "max": 500},
        {"key": "insider_watch_threshold", "label": "Insider Watch Score Threshold", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "insider_watch_confidence", "label": "Insider Watch Confidence", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "insider_watch_sample", "label": "Insider Watch Min Sample", "type": "integer", "min": 1, "max": 500},
    ]
}


# ======================================================================
# Profile 2: Pool Selection
# ======================================================================

_POOL_SELECTION_SOURCE_CODE = '''\
from services.discovery_profile_sdk import BaseDiscoveryProfile

import math
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Optional


def _clamp(value, lo, hi):
    if value < lo:
        return lo
    if value > hi:
        return hi
    return value


class PoolSelectionProfile(BaseDiscoveryProfile):
    """System profile containing all pool selection business logic.

    Scores wallet quality/activity/stability, computes selection scores,
    applies eligibility gates and tier classification, enforces cluster
    diversity caps, and runs the churn guard.
    """

    name = "Pool Selection"
    description = (
        "Complete pool selection pipeline: quality/activity/stability scoring, "
        "eligibility gates, tier classification, cluster diversity, and churn guard."
    )

    default_config = {
        # Pool sizing
        "target_pool_size": 500,
        "min_pool_size": 400,
        "max_pool_size": 600,
        "active_window_hours": 168,
        # Quality score weights
        "quality_rank_weight": 0.35,
        "quality_wr_weight": 0.25,
        "quality_sharpe_weight": 0.15,
        "quality_pf_weight": 0.15,
        "quality_pnl_weight": 0.10,
        "quality_sharpe_norm_divisor": 3.0,
        "quality_pf_norm_divisor": 5.0,
        "quality_pnl_tanh_divisor": 25000.0,
        "quality_copy_candidate_boost": 0.08,
        "quality_monitor_boost": 0.02,
        # Activity score weights
        "activity_flow_1h_weight": 0.50,
        "activity_flow_24h_weight": 0.30,
        "activity_recency_weight": 0.20,
        "activity_flow_1h_divisor": 6.0,
        "activity_flow_24h_divisor": 40.0,
        # Stability score parameters
        "stability_roi_penalty_divisor": 50.0,
        "stability_roi_penalty_scale": 0.25,
        "stability_anomaly_penalty_scale": 0.35,
        "stability_cluster_penalty": 0.10,
        "stability_profitable_bonus": 0.15,
        # Composite weights
        "composite_quality_weight": 0.45,
        "composite_activity_weight": 0.35,
        "composite_stability_weight": 0.20,
        # Selection score weights
        "selection_composite_weight": 0.62,
        "selection_rank_weight": 0.16,
        "selection_insider_weight": 0.08,
        "selection_source_confidence_weight": 0.06,
        "selection_diversity_weight": 0.05,
        "selection_momentum_weight": 0.03,
        # Eligibility gates
        "min_eligible_trades": 25,
        "max_eligible_anomaly": 0.5,
        # Core tier thresholds
        "core_min_win_rate": 0.60,
        "core_min_sharpe": 0.5,
        "core_min_profit_factor": 1.2,
        # Rising tier thresholds
        "rising_min_win_rate": 0.55,
        # Cluster diversity
        "max_cluster_share": 0.08,
        # Churn guard
        "max_hourly_replacement_rate": 0.15,
        "replacement_score_cutoff": 0.05,
    }

    # ==================================================================
    # Public entry point
    # ==================================================================

    def select_pool(self, scored_wallets, current_pool):
        """Select wallets for the active pool.

        Args:
            scored_wallets: list of wallet dicts (from discovery scoring)
            current_pool: list of addresses currently in the pool

        Returns:
            dict with 'pool' (list of addresses) and 'churn_rate'.
        """
        cfg = self.config
        target_size = int(cfg.get("target_pool_size", 500))
        max_pool_size = int(cfg.get("max_pool_size", 600))
        now = utcnow()

        # -- Phase 1: Score all wallets --
        wallet_scores = {}
        selection_scores = {}
        eligible_addresses = set()
        tier_hints = {}
        cluster_ids = {}

        for wallet in scored_wallets:
            address = wallet.get("address", "").lower()
            if not address:
                continue

            quality = self._score_quality(wallet)
            activity = self._score_activity(wallet, now)
            stability = self._score_stability(wallet)

            composite = _clamp(
                float(cfg.get("composite_quality_weight", 0.45)) * quality
                + float(cfg.get("composite_activity_weight", 0.35)) * activity
                + float(cfg.get("composite_stability_weight", 0.20)) * stability,
                0.0, 1.0,
            )

            insider = _clamp(float(wallet.get("insider_score") or 0.0), 0.0, 1.0)
            source_confidence = self._score_source_confidence(wallet)
            unique_markets_24h = int(wallet.get("unique_markets_24h") or 0)
            trades_1h = int(wallet.get("trades_1h") or 0)
            trades_24h = int(wallet.get("trades_24h") or 0)
            diversity_score = _clamp(unique_markets_24h / 14.0, 0.0, 1.0)
            momentum = _clamp(
                0.6 * (trades_1h / 4.0) + 0.4 * (trades_24h / 24.0),
                0.0, 1.0,
            )

            selection = self._score_selection(
                composite=composite,
                rank_score=float(wallet.get("rank_score") or 0.0),
                insider_score=insider,
                source_confidence=source_confidence,
                diversity_score=diversity_score,
                momentum_score=momentum,
            )

            wallet_scores[address] = {
                "quality": quality,
                "activity": activity,
                "stability": stability,
                "composite": composite,
            }
            selection_scores[address] = selection
            cluster_ids[address] = str(wallet.get("cluster_id") or "").strip().lower()

            # -- Eligibility check --
            blockers = self._eligibility_blockers(wallet)
            if not blockers:
                core_ok, rising_ok = self._tier_eligibility(wallet, now)
                if core_ok:
                    tier_hints[address] = "core"
                    eligible_addresses.add(address)
                elif rising_ok:
                    tier_hints[address] = "rising"
                    eligible_addresses.add(address)
                else:
                    tier_hints[address] = "blocked"
            else:
                tier_hints[address] = "blocked"

        # -- Phase 2: Rank and select --
        ranked = sorted(eligible_addresses, key=lambda a: selection_scores.get(a, 0.0), reverse=True)

        # Apply cluster diversity cap
        diversified = self._rank_with_cluster_diversity(ranked, cluster_ids, target_size)

        # Build desired pool: core first, then rising, then backfill
        core = [a for a in diversified if tier_hints.get(a) == "core"]
        rising = [a for a in diversified if tier_hints.get(a) == "rising"]

        desired = []
        seen = set()
        core_target = max(1, int(target_size * 0.70))

        # Core wallets first
        for a in core:
            if a not in seen and len(desired) < core_target:
                desired.append(a)
                seen.add(a)

        # Rising wallets fill remaining
        for a in rising:
            if a not in seen and len(desired) < target_size:
                desired.append(a)
                seen.add(a)

        # Any remaining diversified wallets
        for a in diversified:
            if a not in seen and len(desired) < target_size:
                desired.append(a)
                seen.add(a)

        # -- Phase 3: Churn guard --
        final_pool, churn_rate = self._apply_churn_guard(
            desired=desired,
            current=current_pool,
            scores=selection_scores,
        )

        return {
            "pool": final_pool,
            "churn_rate": churn_rate,
            "wallet_scores": wallet_scores,
            "selection_scores": selection_scores,
            "tier_hints": tier_hints,
        }

    # ==================================================================
    # Quality Score
    # ==================================================================

    def _score_quality(self, wallet):
        """35% rank + 25% WR + 15% Sharpe + 15% PF + 10% PnL + recommendation boost."""
        cfg = self.config

        rank = _clamp(float(wallet.get("rank_score") or 0.0), 0.0, 1.0)
        win = _clamp(float(wallet.get("win_rate") or 0.0), 0.0, 1.0)

        sharpe = wallet.get("sharpe_ratio")
        sharpe_div = float(cfg.get("quality_sharpe_norm_divisor", 3.0))
        sharpe_norm = 0.0 if sharpe is None or not math.isfinite(sharpe) else _clamp(sharpe / sharpe_div, 0.0, 1.0)

        pf = wallet.get("profit_factor")
        pf_div = float(cfg.get("quality_pf_norm_divisor", 5.0))
        pf_norm = 0.0 if pf is None or not math.isfinite(pf) else _clamp(pf / pf_div, 0.0, 1.0)

        pnl = float(wallet.get("total_pnl") or 0.0)
        pnl_div = float(cfg.get("quality_pnl_tanh_divisor", 25000.0))
        pnl_norm = _clamp((math.tanh(pnl / pnl_div) + 1.0) / 2.0, 0.0, 1.0)

        recommendation = str(wallet.get("recommendation") or "").strip().lower()
        recommendation_boost = 0.0
        if recommendation == "copy_candidate":
            recommendation_boost = float(cfg.get("quality_copy_candidate_boost", 0.08))
        elif recommendation == "monitor":
            recommendation_boost = float(cfg.get("quality_monitor_boost", 0.02))

        raw = _clamp(
            float(cfg.get("quality_rank_weight", 0.35)) * rank
            + float(cfg.get("quality_wr_weight", 0.25)) * win
            + float(cfg.get("quality_sharpe_weight", 0.15)) * sharpe_norm
            + float(cfg.get("quality_pf_weight", 0.15)) * pf_norm
            + float(cfg.get("quality_pnl_weight", 0.10)) * pnl_norm
            + recommendation_boost,
            0.0, 1.0,
        )

        return _clamp(raw, 0.0, 1.0)

    # ==================================================================
    # Activity Score
    # ==================================================================

    def _score_activity(self, wallet, now):
        """50% flow_1h + 30% flow_24h + 20% recency."""
        cfg = self.config

        trades_1h = int(wallet.get("trades_1h") or 0)
        trades_24h = int(wallet.get("trades_24h") or 0)
        last_trade_at = wallet.get("last_trade_at")
        active_window = float(cfg.get("active_window_hours", 168))

        flow_1h = _clamp(trades_1h / float(cfg.get("activity_flow_1h_divisor", 6.0)), 0.0, 1.0)
        flow_24h = _clamp(trades_24h / float(cfg.get("activity_flow_24h_divisor", 40.0)), 0.0, 1.0)

        if last_trade_at is None:
            recency = 0.0
        else:
            if isinstance(last_trade_at, str):
                try:
                    last_trade_at = datetime.fromisoformat(last_trade_at.replace("Z", "+00:00")).replace(tzinfo=None)
                except (ValueError, TypeError):
                    last_trade_at = None
            if last_trade_at is not None:
                age_hours = max((now - last_trade_at).total_seconds() / 3600.0, 0.0)
                recency = _clamp(1.0 - (age_hours / active_window), 0.0, 1.0)
            else:
                recency = 0.0

        return _clamp(
            float(cfg.get("activity_flow_1h_weight", 0.50)) * flow_1h
            + float(cfg.get("activity_flow_24h_weight", 0.30)) * flow_24h
            + float(cfg.get("activity_recency_weight", 0.20)) * recency,
            0.0, 1.0,
        )

    # ==================================================================
    # Stability Score
    # ==================================================================

    def _score_stability(self, wallet):
        """consistency - drawdown - anomaly - cluster_penalty + profitable_bonus."""
        cfg = self.config

        drawdown = wallet.get("max_drawdown")
        consistency = 0.5 if drawdown is None else _clamp(1.0 - min(drawdown, 1.0), 0.0, 1.0)

        roi_std = float(wallet.get("roi_std") or 0.0)
        roi_penalty = _clamp(
            abs(roi_std) / float(cfg.get("stability_roi_penalty_divisor", 50.0)),
            0.0, 1.0,
        ) * float(cfg.get("stability_roi_penalty_scale", 0.25))

        anomaly = _clamp(float(wallet.get("anomaly_score") or 0.0), 0.0, 1.0)
        anomaly_penalty = anomaly * float(cfg.get("stability_anomaly_penalty_scale", 0.35))

        cluster_penalty = float(cfg.get("stability_cluster_penalty", 0.10)) if wallet.get("cluster_id") else 0.0
        profitable_bonus = float(cfg.get("stability_profitable_bonus", 0.15)) if wallet.get("is_profitable") else 0.0

        return _clamp(
            consistency - roi_penalty - anomaly_penalty - cluster_penalty + profitable_bonus,
            0.0, 1.0,
        )

    # ==================================================================
    # Selection Score
    # ==================================================================

    def _score_selection(self, *, composite, rank_score, insider_score,
                         source_confidence, diversity_score, momentum_score):
        """62% composite + 16% rank + 8% insider + 6% source + 5% diversity + 3% momentum."""
        cfg = self.config
        return _clamp(
            float(cfg.get("selection_composite_weight", 0.62)) * _clamp(composite, 0.0, 1.0)
            + float(cfg.get("selection_rank_weight", 0.16)) * _clamp(rank_score, 0.0, 1.0)
            + float(cfg.get("selection_insider_weight", 0.08)) * _clamp(insider_score, 0.0, 1.0)
            + float(cfg.get("selection_source_confidence_weight", 0.06)) * _clamp(source_confidence, 0.0, 1.0)
            + float(cfg.get("selection_diversity_weight", 0.05)) * _clamp(diversity_score, 0.0, 1.0)
            + float(cfg.get("selection_momentum_weight", 0.03)) * _clamp(momentum_score, 0.0, 1.0),
            0.0, 1.0,
        )

    # ==================================================================
    # Source Confidence
    # ==================================================================

    def _score_source_confidence(self, wallet):
        """Score based on how many discovery sources confirmed the wallet."""
        source_flags = wallet.get("source_flags") or {}
        if not isinstance(source_flags, dict):
            return 0.0

        score = 0.0
        if source_flags.get("leaderboard"):
            score += 0.35
        if source_flags.get("leaderboard_pnl"):
            score += 0.15
        if source_flags.get("leaderboard_vol"):
            score += 0.10
        if source_flags.get("wallet_trades"):
            score += 0.15
        if source_flags.get("market_trades"):
            score += 0.10
        if source_flags.get("activity"):
            score += 0.10
        if source_flags.get("holders"):
            score += 0.05
        return _clamp(score, 0.0, 1.0)

    # ==================================================================
    # Eligibility Gates
    # ==================================================================

    def _eligibility_blockers(self, wallet):
        """Check hard eligibility gates. Returns list of blocker dicts."""
        cfg = self.config
        blockers = []

        recommendation = str(wallet.get("recommendation") or "").strip().lower()
        total_trades = int(wallet.get("total_trades") or 0)
        total_pnl = float(wallet.get("total_pnl") or 0.0)
        anomaly = float(wallet.get("anomaly_score") or 0.0)
        min_trades = int(cfg.get("min_eligible_trades", 25))
        max_anomaly = float(cfg.get("max_eligible_anomaly", 0.5))

        if wallet.get("last_analyzed_at") is None:
            blockers.append({"code": "not_analyzed", "label": "Analysis missing"})
        if recommendation not in {"copy_candidate", "monitor"}:
            blockers.append({"code": "recommendation_blocked", "label": "Recommendation blocked"})
        if total_trades < min_trades:
            blockers.append({"code": "insufficient_trades", "label": f"Need {min_trades}+ trades"})
        if anomaly > max_anomaly:
            blockers.append({"code": "anomaly_too_high", "label": f"Anomaly > {max_anomaly}"})
        if total_pnl <= 0:
            blockers.append({"code": "non_positive_pnl", "label": "Non-positive PnL"})

        return blockers

    # ==================================================================
    # Tier Classification
    # ==================================================================

    def _tier_eligibility(self, wallet, now):
        """Classify as core or rising. Returns (core_eligible, rising_eligible)."""
        cfg = self.config

        win_rate = float(wallet.get("win_rate") or 0.0)
        total_trades = int(wallet.get("total_trades") or 0)
        total_pnl = float(wallet.get("total_pnl") or 0.0)
        recommendation = str(wallet.get("recommendation") or "").strip().lower()
        min_trades = int(cfg.get("min_eligible_trades", 25))
        active_window = float(cfg.get("active_window_hours", 168))

        sharpe = wallet.get("sharpe_ratio")
        profit_factor = wallet.get("profit_factor")
        core_sharpe_ok = (
            sharpe is not None and math.isfinite(sharpe)
            and sharpe >= float(cfg.get("core_min_sharpe", 0.5))
        )
        core_pf_ok = (
            profit_factor is not None and math.isfinite(profit_factor)
            and profit_factor >= float(cfg.get("core_min_profit_factor", 1.2))
        )

        # Core: copy_candidate recommendation OR strong metrics across the board
        core_eligible = recommendation == "copy_candidate" or (
            win_rate >= float(cfg.get("core_min_win_rate", 0.60))
            and core_sharpe_ok
            and core_pf_ok
        )

        # Rising: active recently, minimum win rate, positive PnL, enough trades
        last_trade_at = wallet.get("last_trade_at")
        is_active_recent = False
        if last_trade_at is not None:
            if isinstance(last_trade_at, str):
                try:
                    last_trade_at = datetime.fromisoformat(
                        last_trade_at.replace("Z", "+00:00")
                    ).replace(tzinfo=None)
                except (ValueError, TypeError):
                    last_trade_at = None
            if last_trade_at is not None:
                cutoff = now - timedelta(hours=active_window)
                is_active_recent = last_trade_at >= cutoff

        rising_eligible = (
            is_active_recent
            and win_rate >= float(cfg.get("rising_min_win_rate", 0.55))
            and total_pnl > 0
            and total_trades >= min_trades
        )

        return core_eligible, rising_eligible

    # ==================================================================
    # Cluster Diversity Cap
    # ==================================================================

    def _rank_with_cluster_diversity(self, ranked_addresses, cluster_ids, target_size):
        """Enforce max_cluster_share to prevent concentration."""
        cfg = self.config
        max_share = float(cfg.get("max_cluster_share", 0.08))
        cap = max(3, int(target_size * max_share))

        selected = []
        deferred = []
        cluster_counts = defaultdict(int)

        for address in ranked_addresses:
            cluster = cluster_ids.get(address, "")
            if not cluster:
                selected.append(address)
                continue
            if cluster_counts[cluster] < cap:
                selected.append(address)
                cluster_counts[cluster] += 1
            else:
                deferred.append(address)

        # Backfill if needed
        if len(selected) < target_size:
            for address in deferred:
                selected.append(address)
                if len(selected) >= target_size:
                    break

        return selected

    # ==================================================================
    # Churn Guard
    # ==================================================================

    def _apply_churn_guard(self, desired, current, scores):
        """Limit hourly pool turnover to max_hourly_replacement_rate.

        Returns (final_pool, churn_rate).
        """
        cfg = self.config
        target_size = int(cfg.get("target_pool_size", 500))
        max_pool_size = int(cfg.get("max_pool_size", 600))
        max_replacement_rate = float(cfg.get("max_hourly_replacement_rate", 0.15))
        score_cutoff = float(cfg.get("replacement_score_cutoff", 0.05))

        desired = desired[:target_size]
        current = current[:max_pool_size]

        # If no existing pool, initialize directly
        if not current:
            final = desired[:target_size]
            return final[:max_pool_size], 0.0

        max_replacements = max(1, int(target_size * max_replacement_rate))
        pool_set = set(current)

        # Trim oversized pool
        if len(pool_set) > target_size:
            keep = sorted(list(pool_set), key=lambda a: scores.get(a, 0.0), reverse=True)[:target_size]
            pool_set = set(keep)

        desired_set = set(desired)
        additions = sorted(
            [a for a in desired if a not in pool_set],
            key=lambda a: scores.get(a, 0.0),
            reverse=True,
        )
        removals = sorted(
            [a for a in pool_set if a not in desired_set],
            key=lambda a: scores.get(a, 0.0),
        )

        replacements = 0
        for address in additions:
            if len(pool_set) < target_size:
                pool_set.add(address)
                replacements += 1
                continue

            if not removals:
                break

            outgoing = removals[0]
            incoming_score = scores.get(address, 0.0)
            outgoing_score = scores.get(outgoing, 0.0)

            can_replace = replacements < max_replacements or (
                incoming_score >= outgoing_score + score_cutoff
            )
            if not can_replace:
                continue

            pool_set.discard(outgoing)
            pool_set.add(address)
            removals.pop(0)
            replacements += 1

        # Trim stale members when desired set is smaller
        if len(pool_set) > len(desired):
            trim_candidates = sorted(
                [a for a in pool_set if a not in desired_set],
                key=lambda a: scores.get(a, 0.0),
            )
            for address in trim_candidates:
                if replacements >= max_replacements:
                    break
                pool_set.discard(address)
                replacements += 1
                if len(pool_set) <= len(desired):
                    break

        # Hard upper bound
        if len(pool_set) > max_pool_size:
            ordered = sorted(list(pool_set), key=lambda a: scores.get(a, 0.0), reverse=True)
            pool_set = set(ordered[:max_pool_size])

        final = sorted(list(pool_set), key=lambda a: scores.get(a, 0.0), reverse=True)
        churn_rate = replacements / max(len(current), 1)
        return final, churn_rate
'''

_POOL_SELECTION_CONFIG: dict = {
    "target_pool_size": 500,
    "min_pool_size": 400,
    "max_pool_size": 600,
    "active_window_hours": 168,
    "quality_rank_weight": 0.35,
    "quality_wr_weight": 0.25,
    "quality_sharpe_weight": 0.15,
    "quality_pf_weight": 0.15,
    "quality_pnl_weight": 0.10,
    "quality_sharpe_norm_divisor": 3.0,
    "quality_pf_norm_divisor": 5.0,
    "quality_pnl_tanh_divisor": 25000.0,
    "quality_copy_candidate_boost": 0.08,
    "quality_monitor_boost": 0.02,
    "activity_flow_1h_weight": 0.50,
    "activity_flow_24h_weight": 0.30,
    "activity_recency_weight": 0.20,
    "activity_flow_1h_divisor": 6.0,
    "activity_flow_24h_divisor": 40.0,
    "stability_roi_penalty_divisor": 50.0,
    "stability_roi_penalty_scale": 0.25,
    "stability_anomaly_penalty_scale": 0.35,
    "stability_cluster_penalty": 0.10,
    "stability_profitable_bonus": 0.15,
    "composite_quality_weight": 0.45,
    "composite_activity_weight": 0.35,
    "composite_stability_weight": 0.20,
    "selection_composite_weight": 0.62,
    "selection_rank_weight": 0.16,
    "selection_insider_weight": 0.08,
    "selection_source_confidence_weight": 0.06,
    "selection_diversity_weight": 0.05,
    "selection_momentum_weight": 0.03,
    "min_eligible_trades": 25,
    "max_eligible_anomaly": 0.5,
    "core_min_win_rate": 0.60,
    "core_min_sharpe": 0.5,
    "core_min_profit_factor": 1.2,
    "rising_min_win_rate": 0.55,
    "max_cluster_share": 0.08,
    "max_hourly_replacement_rate": 0.15,
    "replacement_score_cutoff": 0.05,
}

_POOL_SELECTION_CONFIG_SCHEMA: dict = {
    "param_fields": [
        # Pool sizing
        {"key": "target_pool_size", "label": "Pool Target Size", "type": "integer", "min": 10, "max": 10000},
        {"key": "min_pool_size", "label": "Pool Min Size", "type": "integer", "min": 0, "max": 10000},
        {"key": "max_pool_size", "label": "Pool Max Size", "type": "integer", "min": 1, "max": 10000},
        {"key": "active_window_hours", "label": "Active Window (hours)", "type": "integer", "min": 1, "max": 720},
        # Quality score weights
        {"key": "quality_rank_weight", "label": "Quality: Rank Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "quality_wr_weight", "label": "Quality: Win Rate Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "quality_sharpe_weight", "label": "Quality: Sharpe Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "quality_pf_weight", "label": "Quality: Profit Factor Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "quality_pnl_weight", "label": "Quality: PnL Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "quality_sharpe_norm_divisor", "label": "Quality: Sharpe Norm Divisor", "type": "number", "min": 0.1, "max": 20.0},
        {"key": "quality_pf_norm_divisor", "label": "Quality: PF Norm Divisor", "type": "number", "min": 0.1, "max": 50.0},
        {"key": "quality_pnl_tanh_divisor", "label": "Quality: PnL Tanh Divisor", "type": "number", "min": 100.0, "max": 1000000.0},
        {"key": "quality_copy_candidate_boost", "label": "Quality: Copy Candidate Boost", "type": "number", "min": 0.0, "max": 0.5},
        {"key": "quality_monitor_boost", "label": "Quality: Monitor Boost", "type": "number", "min": 0.0, "max": 0.5},
        # Activity score weights
        {"key": "activity_flow_1h_weight", "label": "Activity: 1h Flow Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "activity_flow_24h_weight", "label": "Activity: 24h Flow Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "activity_recency_weight", "label": "Activity: Recency Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "activity_flow_1h_divisor", "label": "Activity: 1h Flow Divisor", "type": "number", "min": 0.1, "max": 100.0},
        {"key": "activity_flow_24h_divisor", "label": "Activity: 24h Flow Divisor", "type": "number", "min": 0.1, "max": 1000.0},
        # Stability parameters
        {"key": "stability_roi_penalty_divisor", "label": "Stability: ROI Penalty Divisor", "type": "number", "min": 0.1, "max": 200.0},
        {"key": "stability_roi_penalty_scale", "label": "Stability: ROI Penalty Scale", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "stability_anomaly_penalty_scale", "label": "Stability: Anomaly Penalty Scale", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "stability_cluster_penalty", "label": "Stability: Cluster Penalty", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "stability_profitable_bonus", "label": "Stability: Profitable Bonus", "type": "number", "min": 0.0, "max": 1.0},
        # Composite weights
        {"key": "composite_quality_weight", "label": "Composite: Quality Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "composite_activity_weight", "label": "Composite: Activity Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "composite_stability_weight", "label": "Composite: Stability Weight", "type": "number", "min": 0.0, "max": 1.0},
        # Selection score weights
        {"key": "selection_composite_weight", "label": "Selection: Composite Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "selection_rank_weight", "label": "Selection: Rank Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "selection_insider_weight", "label": "Selection: Insider Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "selection_source_confidence_weight", "label": "Selection: Source Confidence Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "selection_diversity_weight", "label": "Selection: Diversity Weight", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "selection_momentum_weight", "label": "Selection: Momentum Weight", "type": "number", "min": 0.0, "max": 1.0},
        # Eligibility gates
        {"key": "min_eligible_trades", "label": "Min Eligible Trades", "type": "integer", "min": 1, "max": 100000},
        {"key": "max_eligible_anomaly", "label": "Max Eligible Anomaly", "type": "number", "min": 0.0, "max": 5.0},
        # Core tier
        {"key": "core_min_win_rate", "label": "Core: Min Win Rate", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "core_min_sharpe", "label": "Core: Min Sharpe", "type": "number", "min": -10.0, "max": 20.0},
        {"key": "core_min_profit_factor", "label": "Core: Min Profit Factor", "type": "number", "min": 0.0, "max": 20.0},
        # Rising tier
        {"key": "rising_min_win_rate", "label": "Rising: Min Win Rate", "type": "number", "min": 0.0, "max": 1.0},
        # Cluster diversity
        {"key": "max_cluster_share", "label": "Max Cluster Share", "type": "number", "min": 0.01, "max": 1.0},
        # Churn guard
        {"key": "max_hourly_replacement_rate", "label": "Max Hourly Replacement Rate", "type": "number", "min": 0.0, "max": 1.0},
        {"key": "replacement_score_cutoff", "label": "Replacement Score Cutoff", "type": "number", "min": 0.0, "max": 1.0},
    ]
}


# ======================================================================
# System profiles to seed
# ======================================================================

SYSTEM_PROFILES: list[dict] = [
    {
        "slug": "discovery_scoring",
        "name": "Discovery Scoring",
        "description": (
            "Complete wallet scoring pipeline: trade stats, risk metrics, "
            "timing skill, execution quality, classification, insider scoring, "
            "and composite rank."
        ),
        "profile_kind": "python",
        "is_system": True,
        "source_code": _DISCOVERY_SCORING_SOURCE_CODE,
        "config": dict(_DISCOVERY_SCORING_CONFIG),
        "config_schema": dict(_DISCOVERY_SCORING_CONFIG_SCHEMA),
    },
    {
        "slug": "pool_selection",
        "name": "Pool Selection",
        "description": (
            "Complete pool selection pipeline: quality/activity/stability scoring, "
            "eligibility gates, tier classification, cluster diversity, and churn guard."
        ),
        "profile_kind": "python",
        "is_system": True,
        "source_code": _POOL_SELECTION_SOURCE_CODE,
        "config": dict(_POOL_SELECTION_CONFIG),
        "config_schema": dict(_POOL_SELECTION_CONFIG_SCHEMA),
    },
]


# ======================================================================
# Seed function
# ======================================================================

async def seed_discovery_profiles() -> int:
    """Seed or update the two system discovery profiles.

    Checks tombstones to skip profiles the user intentionally deleted.
    Creates version snapshots on every source_code change.
    Returns the number of profiles created or updated.
    """
    async with AsyncSessionLocal() as session:
        seed_by_slug = {p["slug"]: p for p in SYSTEM_PROFILES}

        # Check tombstones -- if user deleted a system profile, don't recreate it
        try:
            tombstoned_slugs = set(
                (
                    await session.execute(
                        select(DiscoveryProfileTombstone.slug).where(
                            DiscoveryProfileTombstone.slug.in_(list(seed_by_slug.keys()))
                        )
                    )
                )
                .scalars()
                .all()
            )
        except (ProgrammingError, OperationalError):
            tombstoned_slugs = set()

        existing_rows = (
            await session.execute(
                select(DiscoveryProfile).where(
                    DiscoveryProfile.slug.in_(list(seed_by_slug.keys()))
                )
            )
        ).scalars().all()
        existing_by_slug: dict[str, DiscoveryProfile] = {
            str(row.slug): row for row in existing_rows
        }

        now = utcnow()
        created = 0
        updated = 0

        for idx, (slug, profile_def) in enumerate(seed_by_slug.items()):
            if slug in tombstoned_slugs:
                continue

            existing = existing_by_slug.get(slug)
            if existing is None:
                # -- Create new profile --
                profile_id = uuid4().hex
                profile = DiscoveryProfile(
                    id=profile_id,
                    slug=profile_def["slug"],
                    name=profile_def["name"],
                    description=profile_def["description"],
                    source_code=profile_def["source_code"],
                    class_name=None,
                    is_system=True,
                    enabled=True,
                    is_active=False,
                    status="unloaded",
                    error_message=None,
                    config=dict(profile_def["config"]),
                    config_schema=dict(profile_def["config_schema"]),
                    profile_kind=profile_def["profile_kind"],
                    version=1,
                    sort_order=idx,
                    created_at=now,
                    updated_at=now,
                )
                session.add(profile)

                version_row = DiscoveryProfileVersion(
                    id=uuid4().hex,
                    profile_id=profile_id,
                    profile_slug=profile_def["slug"],
                    version=1,
                    is_latest=True,
                    name=profile_def["name"],
                    description=profile_def["description"],
                    source_code=profile_def["source_code"],
                    class_name=None,
                    config=dict(profile_def["config"]),
                    config_schema=dict(profile_def["config_schema"]),
                    profile_kind=profile_def["profile_kind"],
                    enabled=True,
                    is_system=True,
                    sort_order=idx,
                    parent_version=None,
                    created_by="system",
                    reason="Initial system seed",
                    created_at=now,
                )
                session.add(version_row)
                created += 1
            else:
                # -- Update existing profile if source_code changed --
                if existing.source_code != profile_def["source_code"]:
                    existing.source_code = profile_def["source_code"]
                    existing.version = (existing.version or 1) + 1
                    existing.updated_at = now

                    # Mark previous latest version as non-latest
                    prev_latest = (
                        await session.execute(
                            select(DiscoveryProfileVersion).where(
                                DiscoveryProfileVersion.profile_id == existing.id,
                                DiscoveryProfileVersion.is_latest.is_(True),
                            )
                        )
                    ).scalar_one_or_none()
                    if prev_latest is not None:
                        prev_latest.is_latest = False

                    version_row = DiscoveryProfileVersion(
                        id=uuid4().hex,
                        profile_id=existing.id,
                        profile_slug=existing.slug,
                        version=existing.version,
                        is_latest=True,
                        name=existing.name,
                        description=existing.description,
                        source_code=profile_def["source_code"],
                        class_name=existing.class_name,
                        config=dict(existing.config or {}),
                        config_schema=dict(existing.config_schema or {}),
                        profile_kind=existing.profile_kind or "python",
                        enabled=bool(existing.enabled),
                        is_system=True,
                        sort_order=existing.sort_order or 0,
                        parent_version=(existing.version - 1) if existing.version and existing.version > 1 else None,
                        created_by="system",
                        reason="System profile source_code updated",
                        created_at=now,
                    )
                    session.add(version_row)
                    updated += 1

        if created == 0 and updated == 0:
            return 0

        await session.commit()
        logger.info("Discovery profile seeding complete", created=created, updated=updated)
        return created + updated
