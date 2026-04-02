"""
Discovery Profile SDK
=====================

Base class for user-editable wallet discovery scoring and pool selection.
Users extend BaseDiscoveryProfile and override score_wallet() and/or
select_pool() to customize discovery behavior.
"""

from __future__ import annotations

import math
import statistics
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Optional

from utils.utcnow import utcnow


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ROLLING_WINDOWS: dict[str, timedelta] = {
    "1d": timedelta(days=1),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
    "90d": timedelta(days=90),
}

MIN_TRADES_FOR_ANALYSIS = 5
MIN_TRADES_FOR_RISK_METRICS = 3

_WIN_RATE_PRIOR_POSITIONS = 20.0
_WIN_RATE_PRIOR_MEAN = 0.5

METRICS_SOURCE_VERSION = "accuracy_v2_closed_positions"

# Pool eligibility defaults (mirrors StrategySDK.POOL_ELIGIBILITY_DEFAULTS)
POOL_ELIGIBILITY_DEFAULTS: dict[str, Any] = {
    "target_pool_size": 500,
    "min_pool_size": 400,
    "max_pool_size": 600,
    "active_window_hours": 168,
    "inactive_rising_retention_hours": 720,
    "selection_score_quality_target_floor": 0.35,
    "max_hourly_replacement_rate": 0.15,
    "replacement_score_cutoff": 0.05,
    "max_cluster_share": 0.08,
    "high_conviction_threshold": 0.72,
    "insider_priority_threshold": 0.62,
    "min_eligible_trades": 25,
    "max_eligible_anomaly": 0.5,
    "core_min_win_rate": 0.60,
    "core_min_sharpe": 0.5,
    "core_min_profit_factor": 1.2,
    "rising_min_win_rate": 0.50,
}

# Rank score component weights
RANK_WEIGHT_SHARPE = 0.195
RANK_WEIGHT_PROFIT_FACTOR = 0.1625
RANK_WEIGHT_WIN_RATE = 0.13
RANK_WEIGHT_PNL = 0.0975
RANK_WEIGHT_CONSISTENCY = 0.065
RANK_WEIGHT_TIMING = 0.20
RANK_WEIGHT_EXECUTION = 0.15

# Timing skill windows (label, minutes) and weights
TIMING_WINDOWS = [
    ("timing_5m", 5),
    ("timing_30m", 30),
    ("timing_4h", 240),
    ("timing_24h", 1440),
]
TIMING_WEIGHTS = {
    "timing_5m": 0.35,
    "timing_30m": 0.30,
    "timing_4h": 0.20,
    "timing_24h": 0.15,
}

# Insider metric weights and thresholds
INSIDER_METRIC_WEIGHTS: dict[str, float] = {
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

INSIDER_FLAGGED_THRESHOLD = 0.72
INSIDER_WATCH_THRESHOLD = 0.60
INSIDER_FLAGGED_CONFIDENCE = 0.60
INSIDER_WATCH_CONFIDENCE = 0.50
INSIDER_FLAGGED_SAMPLE = 25
INSIDER_WATCH_SAMPLE = 15


def _clamp(value: float, lo: float, hi: float) -> float:
    if value < lo:
        return lo
    if value > hi:
        return hi
    return value


class BaseDiscoveryProfile:
    """Base class for wallet discovery profiles.

    Users extend this and override ``score_wallet`` and/or ``select_pool``
    to customize discovery behavior.  All helper methods are available as
    instance methods prefixed with ``_`` so overrides can reuse them freely
    via ``self._calculate_rank_score(metrics)``, etc.
    """

    name: str = "Default Profile"
    description: str = ""
    default_config: dict = {}

    def __init__(self) -> None:
        self.config: dict = {}

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    def configure(self, config: dict) -> None:
        merged = dict(self.default_config)
        if config:
            merged.update(config)
        self.config = merged

    # ------------------------------------------------------------------
    # Public entry points (override these)
    # ------------------------------------------------------------------

    def score_wallet(self, wallet: dict, trades: list[dict], rolling: dict) -> dict:
        """Score a single wallet.

        Default implementation runs the full analysis pipeline:
        trade stats -> risk metrics -> timing -> execution -> classify -> rank.

        Returns a dict with all computed metrics and scores.
        """
        positions = wallet.get("positions") or []
        stats = self._calculate_trade_stats(trades, positions=positions)
        market_rois = stats.get("_market_rois", [])
        market_pnls = stats.get("_market_pnls", [])
        days_active = stats.get("days_active", 1)
        total_pnl = stats.get("total_pnl", 0.0)
        total_invested = stats.get("total_invested", 0.0)

        risk_metrics = self._calculate_risk_adjusted_metrics(
            market_rois, market_pnls, days_active, total_pnl, total_invested,
        )

        closed_positions = wallet.get("closed_positions") or []
        timing = self._compute_timing_skill(closed_positions)
        execution = self._compute_execution_quality(closed_positions)

        merged_metrics = {
            **stats,
            **risk_metrics,
            **timing,
            **execution,
        }

        rank_score = self._calculate_rank_score(merged_metrics)
        classification = self._classify_wallet(stats, risk_metrics)
        strategies = self._detect_strategies(trades)

        win_rate_score = self._confidence_adjusted_win_rate(
            stats.get("wins", 0), stats.get("losses", 0),
        )

        return {
            **merged_metrics,
            "rank_score": rank_score,
            "win_rate_score": win_rate_score,
            "strategies": strategies,
            "rolling": rolling,
            **classification,
        }

    def select_pool(
        self,
        scored_wallets: list[dict],
        current_pool: list[str],
    ) -> dict:
        """Select wallets for the active pool.

        Default implementation delegates to ``_default_pool_selection``.
        Returns a dict with ``pool`` (list of addresses) and ``churn_rate``.
        """
        return self._default_pool_selection(scored_wallets, current_pool, self.config)

    # ------------------------------------------------------------------
    # Trade statistics (from wallet_discovery.py)
    # ------------------------------------------------------------------

    def _calculate_trade_stats(
        self,
        trades: list[dict],
        positions: list[dict] | None = None,
    ) -> dict:
        positions = positions or []

        if not trades and not positions:
            return self._empty_stats()

        market_positions: dict[str, dict] = {}
        markets: set[str] = set()
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
                market_positions[market_id]["buys"].append(
                    {
                        "size": size,
                        "price": price,
                        "cost": cost,
                        "outcome": outcome,
                        "timestamp": trade.get("timestamp", trade.get("created_at", "")),
                    }
                )
            elif side == "SELL":
                total_returned += cost
                market_positions[market_id]["sells"].append(
                    {
                        "size": size,
                        "price": price,
                        "cost": cost,
                        "outcome": outcome,
                        "timestamp": trade.get("timestamp", trade.get("created_at", "")),
                    }
                )

        realized_pnl = total_returned - total_invested

        unrealized_pnl = 0.0
        for pos in positions:
            size = float(pos.get("size", 0) or 0)
            avg_price = float(pos.get("avgPrice", pos.get("avg_price", 0)) or 0)
            current_price = float(
                pos.get("currentPrice", pos.get("curPrice", pos.get("price", 0))) or 0
            )
            unrealized_pnl += size * (current_price - avg_price)

        total_pnl = realized_pnl + unrealized_pnl

        wins = 0
        losses = 0
        market_rois: list[float] = []
        market_pnls: list[float] = []

        for _market_id, pos_data in market_positions.items():
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

    def _empty_stats(self) -> dict:
        return {
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "resolved_positions": 0,
            "win_rate": 0.0,
            "win_rate_score": 0.5,
            "win_rate_confidence": 0.0,
            "total_pnl": 0.0,
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "total_invested": 0.0,
            "total_returned": 0.0,
            "avg_roi": 0.0,
            "max_roi": 0.0,
            "min_roi": 0.0,
            "roi_std": 0.0,
            "unique_markets": 0,
            "open_positions": 0,
            "days_active": 0,
            "avg_hold_time_hours": 0.0,
            "trades_per_day": 0.0,
            "avg_position_size": 0.0,
            "_market_rois": [],
            "_market_pnls": [],
        }

    # ------------------------------------------------------------------
    # Risk-adjusted metrics (from wallet_discovery.py)
    # ------------------------------------------------------------------

    def _calculate_risk_adjusted_metrics(
        self,
        market_rois: list[float],
        market_pnls: list[float],
        days_active: int,
        total_pnl: float = 0.0,
        total_invested: float = 0.0,
    ) -> dict:
        result: dict = {
            "sharpe_ratio": None,
            "sortino_ratio": None,
            "max_drawdown": None,
            "profit_factor": None,
            "calmar_ratio": None,
        }

        if len(market_rois) < MIN_TRADES_FOR_RISK_METRICS:
            return result

        mean_return = sum(market_rois) / len(market_rois)
        std_return = self._std_dev(market_rois)

        if std_return > 0:
            result["sharpe_ratio"] = mean_return / std_return
        else:
            result["sharpe_ratio"] = float("inf") if mean_return > 0 else 0.0

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

        gross_profit = sum(p for p in market_pnls if p > 0)
        gross_loss = sum(p for p in market_pnls if p < 0)

        if gross_loss < 0:
            result["profit_factor"] = gross_profit / abs(gross_loss)
        elif gross_profit > 0:
            result["profit_factor"] = float("inf")
        else:
            result["profit_factor"] = 0.0

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

    # ------------------------------------------------------------------
    # Rolling time windows (from wallet_discovery.py)
    # ------------------------------------------------------------------

    def _calculate_rolling_windows(
        self,
        trades: list[dict],
        current_time: datetime,
    ) -> dict:
        rolling_pnl: dict[str, float] = {}
        rolling_roi: dict[str, float] = {}
        rolling_win_rate: dict[str, float] = {}
        rolling_trade_count: dict[str, int] = {}
        rolling_sharpe: dict[str, float | None] = {}

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

    # ------------------------------------------------------------------
    # Composite rank score (from wallet_discovery.py)
    # ------------------------------------------------------------------

    def _calculate_rank_score(self, metrics: dict) -> float:
        sharpe = metrics.get("sharpe_ratio")
        if sharpe is None or not math.isfinite(sharpe):
            sharpe_norm = 0.5
        else:
            sharpe_norm = max(0.0, min(sharpe / 5.0, 1.0))

        pf = metrics.get("profit_factor")
        if pf is None or not math.isfinite(pf):
            pf_norm = 0.5
        else:
            pf_norm = max(0.0, min(pf / 10.0, 1.0))

        win_rate = metrics.get("win_rate_score", metrics.get("win_rate", 0.0))
        wr_norm = max(0.0, min(win_rate, 1.0))

        total_pnl = metrics.get("total_pnl", 0.0)
        if total_pnl > 0:
            pnl_norm = max(0.0, min(math.log10(total_pnl + 1) / 5.0, 1.0))
        else:
            pnl_norm = 0.0

        max_dd = metrics.get("max_drawdown")
        if max_dd is not None and math.isfinite(max_dd):
            consistency_norm = max(0.0, 1.0 - min(max_dd, 1.0))
        else:
            consistency_norm = 0.5

        timing_skill = metrics.get("timing_skill_composite", 0.5)
        timing_norm = max(0.0, min(timing_skill, 1.0))

        execution_quality = metrics.get("execution_quality_score", 0.5)
        execution_norm = max(0.0, min(execution_quality, 1.0))

        rank_score = (
            RANK_WEIGHT_SHARPE * sharpe_norm
            + RANK_WEIGHT_PROFIT_FACTOR * pf_norm
            + RANK_WEIGHT_WIN_RATE * wr_norm
            + RANK_WEIGHT_PNL * pnl_norm
            + RANK_WEIGHT_CONSISTENCY * consistency_norm
            + RANK_WEIGHT_TIMING * timing_norm
            + RANK_WEIGHT_EXECUTION * execution_norm
        )

        return max(0.0, min(rank_score, 1.0))

    # ------------------------------------------------------------------
    # Timing skill (from wallet_discovery.py)
    # ------------------------------------------------------------------

    def _compute_timing_skill(self, positions: list[dict]) -> dict:
        default = {
            "timing_5m": 0.5,
            "timing_30m": 0.5,
            "timing_4h": 0.5,
            "timing_24h": 0.5,
            "timing_skill_composite": 0.5,
        }

        closed = [
            p for p in positions
            if isinstance(p, dict) and self._to_float(p.get("realizedPnl")) != 0.0
        ]
        if len(closed) < MIN_TRADES_FOR_ANALYSIS:
            return default

        favorable_counts: dict[str, int] = {key: 0 for key, _ in TIMING_WINDOWS}
        total_counted: dict[str, int] = {key: 0 for key, _ in TIMING_WINDOWS}

        for pos in closed:
            entry_price = self._to_float(pos.get("avgPrice", pos.get("avg_price", 0)))
            exit_price = self._to_float(pos.get("exitPrice", pos.get("exit_price", 0)))
            realized_pnl = self._to_float(pos.get("realizedPnl", 0))
            initial_value = self._to_float(pos.get("initialValue", 0))
            size = self._to_float(pos.get("size", 0))

            if entry_price <= 0:
                continue

            if exit_price <= 0 and initial_value > 0 and size > 0:
                exit_price = (initial_value + realized_pnl) / size
            if exit_price <= 0:
                continue

            is_buy = realized_pnl > 0 if exit_price > entry_price else realized_pnl <= 0
            price_delta = exit_price - entry_price

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
                hold_duration_hours = self._to_float(
                    pos.get("holdDuration", pos.get("hold_duration_hours", 0))
                )
                hold_minutes = hold_duration_hours * 60.0
            if hold_minutes <= 0:
                hold_minutes = 1440.0

            for key, window_minutes in TIMING_WINDOWS:
                if hold_minutes <= 0:
                    continue

                if hold_minutes <= window_minutes:
                    interpolated_price = exit_price
                else:
                    fraction = window_minutes / hold_minutes
                    interpolated_price = entry_price + price_delta * fraction

                movement = interpolated_price - entry_price
                if is_buy:
                    favorable = movement > 0
                else:
                    favorable = movement < 0

                total_counted[key] += 1
                if favorable:
                    favorable_counts[key] += 1

        result = {}
        for key, _ in TIMING_WINDOWS:
            if total_counted[key] > 0:
                result[key] = favorable_counts[key] / total_counted[key]
            else:
                result[key] = 0.5

        composite = sum(result[key] * TIMING_WEIGHTS[key] for key in TIMING_WEIGHTS)
        result["timing_skill_composite"] = composite
        return result

    # ------------------------------------------------------------------
    # Execution quality (from wallet_discovery.py)
    # ------------------------------------------------------------------

    def _compute_execution_quality(self, positions: list[dict]) -> dict:
        default = {
            "avg_slippage_bps": 0.0,
            "median_slippage_bps": 0.0,
            "execution_quality_score": 0.5,
        }

        slippages: list[float] = []
        for pos in positions:
            if not isinstance(pos, dict):
                continue

            fill_price = self._to_float(pos.get("avgPrice", pos.get("avg_price", 0)))
            if fill_price <= 0:
                continue

            mid_price = 0.0
            yes_price = self._to_float(pos.get("yesPrice", pos.get("yes_price", 0)))
            no_price = self._to_float(pos.get("noPrice", pos.get("no_price", 0)))
            if yes_price > 0 and no_price > 0:
                outcome = str(
                    pos.get("outcome", pos.get("outcome_index", ""))
                ).strip().upper()
                if outcome in ("NO", "1"):
                    mid_price = no_price
                else:
                    mid_price = yes_price
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
                size = self._to_float(pos.get("size", 0))
                if initial_value > 0 and size > 0:
                    mid_price = initial_value / size

            if mid_price <= 0:
                continue

            slippage_bps = abs(fill_price - mid_price) / mid_price * 10000.0
            slippages.append(slippage_bps)

        if len(slippages) < MIN_TRADES_FOR_ANALYSIS:
            return default

        avg_slippage = sum(slippages) / len(slippages)
        median_slippage = statistics.median(slippages)
        execution_quality_score = 1.0 - min(1.0, avg_slippage / 100.0)

        return {
            "avg_slippage_bps": avg_slippage,
            "median_slippage_bps": median_slippage,
            "execution_quality_score": max(0.0, min(1.0, execution_quality_score)),
        }

    # ------------------------------------------------------------------
    # Classification and strategy detection (from wallet_discovery.py)
    # ------------------------------------------------------------------

    def _classify_wallet(self, stats: dict, risk_metrics: dict) -> dict:
        tags: list[str] = []
        anomaly_score = 0.0

        win_rate = stats.get("win_rate", 0.0)
        wins = stats.get("wins", 0)
        losses = stats.get("losses", 0)
        resolved_positions = max(0, int(wins or 0) + int(losses or 0))
        adjusted_win_rate = self._confidence_adjusted_win_rate(wins, losses)
        total_pnl = stats.get("total_pnl", 0.0)
        total_trades = stats.get("total_trades", 0)
        trades_per_day = stats.get("trades_per_day", 0.0)
        sharpe = risk_metrics.get("sharpe_ratio")
        profit_factor = risk_metrics.get("profit_factor")

        is_bot = False
        if trades_per_day > 20:
            is_bot = True
            tags.append("bot")
        if total_trades > 500:
            is_bot = True
            if "bot" not in tags:
                tags.append("bot")

        is_profitable = total_pnl > 0 and win_rate > 0.5

        if adjusted_win_rate >= 0.7 and resolved_positions >= 20:
            tags.append("high_win_rate")
        if total_pnl >= 10000:
            tags.append("whale")
        elif total_pnl >= 1000:
            tags.append("profitable")
        if sharpe is not None and math.isfinite(sharpe) and sharpe >= 2.0:
            tags.append("risk_adjusted_alpha")
        if profit_factor is not None and math.isfinite(profit_factor) and profit_factor >= 3.0:
            tags.append("strong_edge")
        if adjusted_win_rate >= 0.55 and total_pnl > 0 and resolved_positions >= 50:
            tags.append("consistent")

        if adjusted_win_rate >= 0.95 and resolved_positions >= 20:
            anomaly_score = max(anomaly_score, 0.8)
            tags.append("suspicious_win_rate")
        if adjusted_win_rate >= 0.85 and resolved_positions >= 50:
            anomaly_score = max(anomaly_score, 0.5)

        if anomaly_score >= 0.7:
            recommendation = "avoid"
        elif is_profitable and adjusted_win_rate >= 0.6 and resolved_positions >= 20:
            recommendation = "copy_candidate"
        elif is_profitable and resolved_positions >= 10:
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

    def _detect_strategies(self, trades: list[dict]) -> list[str]:
        strategies: set[str] = set()

        market_groups: dict[str, list[dict]] = {}
        for trade in trades:
            market_id = trade.get("market", trade.get("condition_id", ""))
            if market_id:
                market_groups.setdefault(market_id, []).append(trade)

        for _mid, mtrades in market_groups.items():
            if len(mtrades) >= 3:
                strategies.add("negrisk_date_sweep")

        for mtrades in market_groups.values():
            sides = set(t.get("outcome", t.get("side", "")) for t in mtrades)
            if "Yes" in sides and "No" in sides:
                strategies.add("basic_arbitrage")
            if "YES" in sides and "NO" in sides:
                strategies.add("basic_arbitrage")

        if len(trades) > 100:
            strategies.add("automated_trading")

        for mtrades in market_groups.values():
            if len(mtrades) >= 6:
                strategies.add("scalping")
                break

        return list(strategies)

    # ------------------------------------------------------------------
    # Pool quality/activity/stability scoring (from smart_wallet_pool.py)
    # ------------------------------------------------------------------

    def _score_quality(self, wallet: dict) -> float:
        rank = _clamp(float(wallet.get("rank_score") or 0.0), 0.0, 1.0)
        win = _clamp(float(wallet.get("win_rate") or 0.0), 0.0, 1.0)

        sharpe = wallet.get("sharpe_ratio")
        if sharpe is None or not math.isfinite(sharpe):
            sharpe_norm = 0.0
        else:
            sharpe_norm = _clamp(sharpe / 3.0, 0.0, 1.0)

        pf = wallet.get("profit_factor")
        if pf is None or not math.isfinite(pf):
            pf_norm = 0.0
        else:
            pf_norm = _clamp(pf / 5.0, 0.0, 1.0)

        pnl = float(wallet.get("total_pnl") or 0.0)
        pnl_norm = _clamp((math.tanh(pnl / 25000.0) + 1.0) / 2.0, 0.0, 1.0)

        recommendation = str(wallet.get("recommendation") or "").strip().lower()
        recommendation_boost = 0.0
        if recommendation == "copy_candidate":
            recommendation_boost = 0.08
        elif recommendation == "monitor":
            recommendation_boost = 0.02

        raw = _clamp(
            0.35 * rank + 0.25 * win + 0.15 * sharpe_norm + 0.15 * pf_norm + 0.10 * pnl_norm + recommendation_boost,
            0.0,
            1.0,
        )

        source_version = str(wallet.get("metrics_source_version") or "").strip()
        if wallet.get("last_analyzed_at") is None or source_version != METRICS_SOURCE_VERSION:
            raw *= 0.80

        return _clamp(raw, 0.0, 1.0)

    def _score_activity(self, wallet: dict, now: datetime) -> float:
        trades_1h = int(wallet.get("trades_1h") or 0)
        trades_24h = int(wallet.get("trades_24h") or 0)
        last_trade_at = wallet.get("last_trade_at")
        active_window_hours = self.config.get(
            "active_window_hours",
            POOL_ELIGIBILITY_DEFAULTS["active_window_hours"],
        )

        flow_1h = _clamp(trades_1h / 6.0, 0.0, 1.0)
        flow_24h = _clamp(trades_24h / 40.0, 0.0, 1.0)

        if last_trade_at is None:
            recency = 0.0
        else:
            if isinstance(last_trade_at, str):
                last_trade_at = self._parse_timestamp(last_trade_at)
            if last_trade_at is None:
                recency = 0.0
            else:
                age_hours = max((now - last_trade_at).total_seconds() / 3600.0, 0.0)
                recency = _clamp(1.0 - (age_hours / active_window_hours), 0.0, 1.0)

        base_score = _clamp(0.50 * flow_1h + 0.30 * flow_24h + 0.20 * recency, 0.0, 1.0)

        source_version = str(wallet.get("metrics_source_version") or "").strip()
        if wallet.get("last_analyzed_at") is None or source_version != METRICS_SOURCE_VERSION:
            base_score *= 0.80

        return _clamp(base_score, 0.0, 1.0)

    def _score_stability(self, wallet: dict) -> float:
        drawdown = wallet.get("max_drawdown")
        if drawdown is None:
            consistency = 0.5
        else:
            consistency = _clamp(1.0 - min(drawdown, 1.0), 0.0, 1.0)

        roi_std = float(wallet.get("roi_std") or 0.0)
        roi_penalty = _clamp(abs(roi_std) / 50.0, 0.0, 1.0) * 0.25

        anomaly = _clamp(float(wallet.get("anomaly_score") or 0.0), 0.0, 1.0)
        anomaly_penalty = anomaly * 0.35

        cluster_penalty = 0.10 if wallet.get("cluster_id") else 0.0
        profitable_bonus = 0.15 if wallet.get("is_profitable") else 0.0

        return _clamp(
            consistency - roi_penalty - anomaly_penalty - cluster_penalty + profitable_bonus,
            0.0,
            1.0,
        )

    # ------------------------------------------------------------------
    # Default pool selection (from smart_wallet_pool.py)
    # ------------------------------------------------------------------

    def _default_pool_selection(
        self,
        scored_wallets: list[dict],
        current_pool: list[str],
        config: dict,
    ) -> dict:
        now = utcnow()
        target_pool_size = int(config.get(
            "target_pool_size",
            POOL_ELIGIBILITY_DEFAULTS["target_pool_size"],
        ))
        max_pool_size = int(config.get(
            "max_pool_size",
            POOL_ELIGIBILITY_DEFAULTS["max_pool_size"],
        ))
        min_eligible_trades = int(config.get(
            "min_eligible_trades",
            POOL_ELIGIBILITY_DEFAULTS["min_eligible_trades"],
        ))
        max_eligible_anomaly = float(config.get(
            "max_eligible_anomaly",
            POOL_ELIGIBILITY_DEFAULTS["max_eligible_anomaly"],
        ))
        core_min_win_rate = float(config.get(
            "core_min_win_rate",
            POOL_ELIGIBILITY_DEFAULTS["core_min_win_rate"],
        ))
        core_min_sharpe = float(config.get(
            "core_min_sharpe",
            POOL_ELIGIBILITY_DEFAULTS["core_min_sharpe"],
        ))
        core_min_profit_factor = float(config.get(
            "core_min_profit_factor",
            POOL_ELIGIBILITY_DEFAULTS["core_min_profit_factor"],
        ))
        rising_min_win_rate = float(config.get(
            "rising_min_win_rate",
            POOL_ELIGIBILITY_DEFAULTS["rising_min_win_rate"],
        ))
        max_hourly_replacement_rate = float(config.get(
            "max_hourly_replacement_rate",
            POOL_ELIGIBILITY_DEFAULTS["max_hourly_replacement_rate"],
        ))
        replacement_score_cutoff = float(config.get(
            "replacement_score_cutoff",
            POOL_ELIGIBILITY_DEFAULTS["replacement_score_cutoff"],
        ))
        max_cluster_share = float(config.get(
            "max_cluster_share",
            POOL_ELIGIBILITY_DEFAULTS["max_cluster_share"],
        ))

        # Score each wallet
        selection_scores: dict[str, float] = {}
        tier_assignments: dict[str, str] = {}
        eligible_addresses: set[str] = set()

        for wallet in scored_wallets:
            address = wallet.get("address", "")
            if not address:
                continue

            quality = self._score_quality(wallet)
            activity = self._score_activity(wallet, now)
            stability = self._score_stability(wallet)
            composite = _clamp(0.45 * quality + 0.35 * activity + 0.20 * stability, 0.0, 1.0)
            selection_scores[address] = composite

            total_trades = int(wallet.get("total_trades") or 0)
            anomaly_score = float(wallet.get("anomaly_score") or 0.0)
            win_rate = float(wallet.get("win_rate") or 0.0)
            sharpe = wallet.get("sharpe_ratio")
            pf = wallet.get("profit_factor")
            is_profitable = bool(wallet.get("is_profitable"))

            # Eligibility gates
            if total_trades < min_eligible_trades:
                tier_assignments[address] = "blocked"
                continue
            if anomaly_score > max_eligible_anomaly:
                tier_assignments[address] = "blocked"
                continue

            # Core tier
            sharpe_val = sharpe if sharpe is not None and math.isfinite(sharpe) else 0.0
            pf_val = pf if pf is not None and math.isfinite(pf) else 0.0
            if (
                win_rate >= core_min_win_rate
                and sharpe_val >= core_min_sharpe
                and pf_val >= core_min_profit_factor
                and is_profitable
            ):
                tier_assignments[address] = "core"
                eligible_addresses.add(address)
                continue

            # Rising tier
            if win_rate >= rising_min_win_rate:
                tier_assignments[address] = "rising"
                eligible_addresses.add(address)
                continue

            tier_assignments[address] = "blocked"

        # Rank eligible wallets by score
        eligible_ranked = sorted(
            [w for w in scored_wallets if w.get("address", "") in eligible_addresses],
            key=lambda w: selection_scores.get(w.get("address", ""), 0.0),
            reverse=True,
        )

        # Cluster diversity cap
        cluster_cap = max(3, int(max(target_pool_size, 1) * max_cluster_share))
        cluster_counts: dict[str, int] = defaultdict(int)
        diversified: list[str] = []
        for wallet in eligible_ranked:
            address = wallet.get("address", "")
            cluster_id = wallet.get("cluster_id")
            if cluster_id:
                if cluster_counts[cluster_id] >= cluster_cap:
                    continue
                cluster_counts[cluster_id] += 1
            diversified.append(address)
            if len(diversified) >= target_pool_size:
                break

        # Churn guard
        final_pool, churn_rate = self._apply_churn_guard(
            desired=diversified,
            current=current_pool,
            scores=selection_scores,
            target_pool_size=target_pool_size,
            max_pool_size=max_pool_size,
            max_hourly_replacement_rate=max_hourly_replacement_rate,
            replacement_score_cutoff=replacement_score_cutoff,
        )

        return {
            "pool": final_pool,
            "churn_rate": churn_rate,
            "selection_scores": selection_scores,
            "tier_assignments": tier_assignments,
        }

    def _apply_churn_guard(
        self,
        desired: list[str],
        current: list[str],
        scores: dict[str, float],
        target_pool_size: int = 500,
        max_pool_size: int = 600,
        max_hourly_replacement_rate: float = 0.15,
        replacement_score_cutoff: float = 0.05,
    ) -> tuple[list[str], float]:
        desired = desired[:target_pool_size]
        current = current[:max_pool_size]

        if not current:
            initialized = desired[:target_pool_size]
            return initialized[:max_pool_size], 0.0

        max_replacements = max(1, int(target_pool_size * max_hourly_replacement_rate))
        pool_set = set(current)

        if len(pool_set) > target_pool_size:
            keep = sorted(list(pool_set), key=lambda a: scores.get(a, 0.0), reverse=True)[
                :target_pool_size
            ]
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
            if len(pool_set) < target_pool_size:
                pool_set.add(address)
                replacements += 1
                continue

            if not removals:
                break

            outgoing = removals[0]
            incoming_score = scores.get(address, 0.0)
            outgoing_score = scores.get(outgoing, 0.0)

            can_replace = replacements < max_replacements or (
                incoming_score >= outgoing_score + replacement_score_cutoff
            )
            if not can_replace:
                continue

            pool_set.discard(outgoing)
            pool_set.add(address)
            removals.pop(0)
            replacements += 1

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

        if len(pool_set) > max_pool_size:
            ordered = sorted(
                list(pool_set), key=lambda a: scores.get(a, 0.0), reverse=True
            )
            pool_set = set(ordered[:max_pool_size])

        final = sorted(list(pool_set), key=lambda a: scores.get(a, 0.0), reverse=True)
        churn_rate = replacements / max(len(current), 1)
        return final, churn_rate

    # ------------------------------------------------------------------
    # Insider scoring (from insider_detector.py)
    # ------------------------------------------------------------------

    def _compute_insider_score(self, wallet: dict) -> dict:
        """Simplified insider scoring using the 10-component model.

        Expects a wallet dict with pre-computed metric fields.  For
        components that require trade-level event data (timing_alpha,
        pre_news_timing, etc.) pass them pre-computed in the wallet dict
        as ``insider_components``.
        """
        components: dict[str, Optional[float]] = wallet.get("insider_components") or {}

        wins = int(wallet.get("wins") or 0)
        losses = int(wallet.get("losses") or 0)
        resolved_sample = wins + losses

        # Backfill simple components from wallet-level metrics if not
        # already present in the components dict.
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
                    (0.60 * dd_inv) + (0.40 * _clamp(timing, 0.0, 1.0)),
                    0.0,
                    1.0,
                )

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

    @staticmethod
    def _classify_insider(score: float, confidence: float, sample_size: int) -> str:
        if (
            score >= INSIDER_FLAGGED_THRESHOLD
            and confidence >= INSIDER_FLAGGED_CONFIDENCE
            and sample_size >= INSIDER_FLAGGED_SAMPLE
        ):
            return "flagged_insider"
        if (
            score >= INSIDER_WATCH_THRESHOLD
            and confidence >= INSIDER_WATCH_CONFIDENCE
            and sample_size >= INSIDER_WATCH_SAMPLE
        ):
            return "watch_insider"
        return "none"

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _std_dev(values: list[float]) -> float:
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
        return math.sqrt(variance)

    @staticmethod
    def _parse_timestamp(raw: Any) -> Optional[datetime]:
        if raw is None:
            return None
        if isinstance(raw, datetime):
            return raw
        if isinstance(raw, (int, float)):
            try:
                return datetime.utcfromtimestamp(raw)
            except (ValueError, OSError):
                return None
        if isinstance(raw, str):
            try:
                return datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
            except (ValueError, TypeError):
                return None
        return None

    @staticmethod
    def _confidence_adjusted_win_rate(wins: Any, losses: Any) -> float:
        try:
            wins_value = max(0.0, float(wins or 0))
        except Exception:
            wins_value = 0.0
        try:
            losses_value = max(0.0, float(losses or 0))
        except Exception:
            losses_value = 0.0
        total = wins_value + losses_value
        if total <= 0:
            return _WIN_RATE_PRIOR_MEAN
        return max(
            0.0,
            min(
                (wins_value + (_WIN_RATE_PRIOR_MEAN * _WIN_RATE_PRIOR_POSITIONS))
                / (total + _WIN_RATE_PRIOR_POSITIONS),
                1.0,
            ),
        )

    @staticmethod
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return default

    def _compute_days_active(self, trades: list[dict]) -> int:
        if not trades:
            return 0

        first_raw = trades[-1].get("timestamp", trades[-1].get("created_at"))
        last_raw = trades[0].get("timestamp", trades[0].get("created_at"))

        first = self._parse_timestamp(first_raw)
        last = self._parse_timestamp(last_raw)

        if first and last:
            return max((last - first).days, 1)
        return 30

    def _filter_trades_after(self, trades: list[dict], cutoff: datetime) -> list[dict]:
        filtered: list[dict] = []
        for trade in trades:
            raw = trade.get(
                "timestamp",
                trade.get("created_at", trade.get("createdAt")),
            )
            ts = self._parse_timestamp(raw)
            if ts is None:
                filtered.append(trade)
                continue
            if ts >= cutoff:
                filtered.append(trade)
        return filtered
