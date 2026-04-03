from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import Any

from models import Market, Opportunity
from services.data_events import DataEvent, EventType
from services.strategy_sdk import StrategySDK
from services.strategies.base import BaseStrategy, DecisionCheck, ExitDecision, StrategyDecision, _trader_size_limits
from services.strategies.crypto_strategy_utils import build_binary_crypto_market, parse_datetime_utc
from utils.converters import clamp, safe_float, to_float
from utils.logger import get_logger
from utils.signal_helpers import signal_payload

logger = get_logger(__name__)
def _normalize_timeframe(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"5m", "5min", "5-minute", "5minutes"}:
        return "5m"
    return text


def _as_float(value: Any) -> float | None:
    parsed = safe_float(value, None)
    return float(parsed) if parsed is not None else None


def _now_ts() -> float:
    return time.time()


def _trade_price(trade: Any) -> float | None:
    if isinstance(trade, dict):
        return _as_float(trade.get("price"))
    return _as_float(getattr(trade, "price", None))


def _trade_timestamp_ms(trade: Any) -> float | None:
    if isinstance(trade, dict):
        raw = trade.get("timestamp") or trade.get("created_at") or trade.get("time")
    else:
        raw = getattr(trade, "timestamp", None)
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            raw = raw.replace(tzinfo=timezone.utc)
        return raw.timestamp() * 1000.0
    if isinstance(raw, (int, float)):
        raw_f = float(raw)
        return raw_f if raw_f > 10_000_000_000 else raw_f * 1000.0
    text = str(raw or "").strip()
    if not text:
        return None
    parsed_dt = parse_datetime_utc(text)
    if parsed_dt is not None:
        return parsed_dt.timestamp() * 1000.0
    parsed = safe_float(text, None)
    if parsed is None:
        return None
    return float(parsed) if parsed > 10_000_000_000 else float(parsed) * 1000.0


def _token_quotes(token_id: str) -> dict[str, float | None]:
    snapshots = StrategySDK.get_price_history(token_id, max_snapshots=1)
    if not snapshots:
        return {"mid": None, "bid": None, "ask": None}
    latest = snapshots[0] if isinstance(snapshots[0], dict) else {}
    return {
        "mid": _as_float(latest.get("mid")),
        "bid": _as_float(latest.get("bid")),
        "ask": _as_float(latest.get("ask")),
    }


def _recent_trade_stats(token_id: str, *, breakout_price: float, final_seconds: float) -> dict[str, Any]:
    trades = StrategySDK.get_recent_trades(token_id, max_trades=25)
    now_ms = _now_ts() * 1000.0
    last_trade = None
    breakout_seen = False
    for trade in trades:
        price = _trade_price(trade)
        if price is None:
            continue
        if last_trade is None:
            last_trade = price
        if price < breakout_price:
            continue
        ts_ms = _trade_timestamp_ms(trade)
        if ts_ms is None:
            breakout_seen = True
            continue
        if now_ms - ts_ms <= max(1.0, final_seconds) * 1000.0:
            breakout_seen = True
    return {
        "last_trade_price": last_trade,
        "breakout_seen": breakout_seen,
    }


def _sum_visible_depth_usd(levels: Any) -> float | None:
    if not isinstance(levels, list):
        return None
    total = 0.0
    seen = False
    for level in levels:
        if not isinstance(level, dict):
            continue
        price = _as_float(level.get("price"))
        size = _as_float(level.get("size"))
        if price is None or size is None or price <= 0.0 or size <= 0.0:
            continue
        total += price * size
        seen = True
    return total if seen else None


def _rolling_drop(observations: list[dict[str, Any]], length: int) -> float | None:
    if len(observations) < length:
        return None
    newest = observations[-1].get("gap_usd")
    oldest = observations[-length].get("gap_usd")
    if newest is None or oldest is None:
        return None
    return float(oldest) - float(newest)


def _strictly_falling(observations: list[dict[str, Any]], length: int) -> bool:
    if len(observations) < length:
        return False
    window = observations[-length:]
    gaps = [item.get("gap_usd") for item in window]
    if any(gap is None for gap in gaps):
        return False
    return all(float(gaps[idx]) > float(gaps[idx + 1]) for idx in range(len(gaps) - 1))


def _strictly_rising(observations: list[dict[str, Any]], length: int) -> bool:
    if len(observations) < length:
        return False
    window = observations[-length:]
    gaps = [item.get("gap_usd") for item in window]
    if any(gap is None for gap in gaps):
        return False
    return all(float(gaps[idx]) < float(gaps[idx + 1]) for idx in range(len(gaps) - 1))


class BTC5mThresholdFlipStrategy(BaseStrategy):
    strategy_type = "btc_5m_threshold_flip"
    name = "BTC 5m Threshold Flip"
    description = "Buys late NO against overconfident BTC 5m YES pricing as the oracle compresses toward the threshold."
    mispricing_type = "within_market"
    source_key = "crypto"
    market_categories = ["crypto"]
    subscriptions = [EventType.CRYPTO_UPDATE]

    default_config = {
        "asset": "BTC",
        "timeframe": "5m",
        "entry_mode": "two_stage",
        "max_oracle_age_seconds": 2.0,
        "entry_window_start_seconds": 60.0,
        "entry_window_end_seconds": 2.0,
        "probe_window_end_seconds": 5.0,
        "liquidity_floor_usd": 2500.0,
        "max_no_spread": 0.03,
        "max_gap_usd": 40.0,
        "preferred_gap_usd": 20.0,
        "arm_yes_mid_min": 0.75,
        "preferred_yes_mid_min": 0.75,
        "weak_yes_mid_min": 0.85,
        "preferred_no_ask_max": 0.25,
        "weak_no_ask_max": 0.20,
        "compression_drop_usd": 5.0,
        "strong_compression_drop_usd": 8.0,
        "breakout_price": 0.50,
        "breakout_final_seconds": 10.0,
        "breakout_gap_cap_usd": 5.0,
        "max_depth_fraction": 0.05,
        "probe_fraction": 0.25,
        "confirm_fraction": 0.75,
        "max_average_entry_price": 0.42,
        "max_confirmation_no_ask": 0.58,
        "min_flow_imbalance": 0.0,
        "max_recent_move_zscore": 99.0,
        "history_seconds": 15.0,
        "max_observations": 24,
    }

    def __init__(self) -> None:
        super().__init__()
        self.state.setdefault("detect_markets", {})
        self.state.setdefault("exit_positions", {})

    @staticmethod
    def _market_key(row: dict[str, Any]) -> str:
        return str(row.get("condition_id") or row.get("id") or row.get("slug") or "").strip()

    @staticmethod
    def _row_market(row: dict[str, Any]) -> Market | None:
        return build_binary_crypto_market(row)

    @staticmethod
    def _seconds_left(row: dict[str, Any]) -> float | None:
        direct = _as_float(row.get("seconds_left"))
        if direct is not None and direct >= 0.0:
            return direct
        end_time = parse_datetime_utc(row.get("end_time"))
        if end_time is None:
            return None
        return max(0.0, (end_time - datetime.now(timezone.utc)).total_seconds())

    def _cleanup_detect_state(self, history_seconds: float, max_observations: int) -> None:
        markets_state = self.state.get("detect_markets")
        if not isinstance(markets_state, dict):
            self.state["detect_markets"] = {}
            return
        cutoff = _now_ts() - max(10.0, history_seconds * 4.0)
        stale_keys: list[str] = []
        for market_key, state in markets_state.items():
            if not isinstance(state, dict):
                stale_keys.append(market_key)
                continue
            observations = state.get("observations") if isinstance(state.get("observations"), list) else []
            observations = [obs for obs in observations if isinstance(obs, dict) and float(obs.get("ts", 0.0) or 0.0) >= cutoff]
            if max_observations > 0:
                observations = observations[-max_observations:]
            state["observations"] = observations
            if not observations and bool(state.get("closed", False)):
                stale_keys.append(market_key)
        for market_key in stale_keys:
            markets_state.pop(market_key, None)

    def _quotes_and_tape(self, typed_market: Market, row: dict[str, Any]) -> dict[str, Any]:
        token_ids = list(typed_market.clob_token_ids or [])
        yes_token = token_ids[0] if len(token_ids) > 0 else None
        no_token = token_ids[1] if len(token_ids) > 1 else None

        yes_quotes = _token_quotes(yes_token) if yes_token else {"mid": None, "bid": None, "ask": None}
        no_quotes = _token_quotes(no_token) if no_token else {"mid": None, "bid": None, "ask": None}

        yes_mid = yes_quotes["mid"] if yes_quotes["mid"] is not None else _as_float(row.get("up_price"))
        no_mid = no_quotes["mid"] if no_quotes["mid"] is not None else _as_float(row.get("down_price"))
        yes_bid = yes_quotes["bid"]
        yes_ask = yes_quotes["ask"]
        no_bid = no_quotes["bid"]
        no_ask = no_quotes["ask"]

        if yes_bid is None and yes_mid is not None:
            yes_bid = yes_mid
        if yes_ask is None and yes_mid is not None:
            yes_ask = yes_mid
        if no_bid is None and no_mid is not None:
            no_bid = no_mid
        if no_ask is None and no_mid is not None:
            no_ask = no_mid

        strategy_cfg = dict(getattr(self, "config", {}) or {})
        no_tape = _recent_trade_stats(
            no_token,
            breakout_price=to_float(strategy_cfg.get("breakout_price", self.default_config["breakout_price"]), 0.50),
            final_seconds=to_float(
                strategy_cfg.get("breakout_final_seconds", self.default_config["breakout_final_seconds"]),
                10.0,
            ),
        ) if no_token else {"last_trade_price": None, "breakout_seen": False}

        no_levels = StrategySDK.get_book_levels(typed_market, side="NO", max_levels=8)
        visible_depth_usd = _sum_visible_depth_usd(no_levels)
        if visible_depth_usd is None:
            depth_summary = StrategySDK.get_order_book_depth(typed_market, side="NO", size_usd=100.0) or {}
            visible_depth_usd = _as_float(depth_summary.get("available_liquidity"))

        no_spread = None
        if no_bid is not None and no_ask is not None and no_ask >= no_bid:
            no_spread = no_ask - no_bid
        elif row.get("spread") is not None:
            no_spread = clamp(float(_as_float(row.get("spread")) or 0.0), 0.0, 1.0)

        return {
            "yes_mid": yes_mid,
            "no_mid": no_mid,
            "yes_best_bid": yes_bid,
            "yes_best_ask": yes_ask,
            "no_best_bid": no_bid,
            "no_best_ask": no_ask,
            "no_last_trade": no_tape.get("last_trade_price"),
            "no_breakout_trade": bool(no_tape.get("breakout_seen")),
            "visible_no_depth_usd": visible_depth_usd,
            "no_spread": no_spread,
        }

    def _record_observation(self, market_key: str, observation: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
        markets_state = self.state.setdefault("detect_markets", {})
        state = markets_state.get(market_key)
        if not isinstance(state, dict):
            state = {
                "observations": [],
                "probe_emitted": False,
                "confirm_emitted": False,
                "closed": False,
                "probe_reference_price": None,
                "campaign_key": None,
            }
            markets_state[market_key] = state
        observations = state.get("observations") if isinstance(state.get("observations"), list) else []
        observations.append(observation)
        history_seconds = max(5.0, to_float(cfg.get("history_seconds", 15.0), 15.0))
        cutoff = observation["ts"] - history_seconds
        observations = [item for item in observations if float(item.get("ts", 0.0) or 0.0) >= cutoff]
        max_observations = max(3, int(to_float(cfg.get("max_observations", 24), 24.0)))
        observations = observations[-max_observations:]
        state["observations"] = observations
        state["closed"] = bool(observation.get("seconds_left") is not None and observation.get("seconds_left") <= 0.0)
        return state

    @staticmethod
    def _current_window_ok(row: dict[str, Any], seconds_left: float | None) -> bool:
        if isinstance(row.get("is_current"), bool):
            return bool(row.get("is_current"))
        if isinstance(row.get("is_live"), bool):
            return bool(row.get("is_live"))
        return seconds_left is not None and seconds_left > 0.0

    def _compression_state(self, observations: list[dict[str, Any]], cfg: dict[str, Any]) -> dict[str, bool]:
        compression_drop_usd = max(0.0, to_float(cfg.get("compression_drop_usd", 5.0), 5.0))
        strong_drop_usd = max(compression_drop_usd, to_float(cfg.get("strong_compression_drop_usd", 8.0), 8.0))
        falling_3 = _strictly_falling(observations, 3)
        drop_3 = _rolling_drop(observations, 3)
        compression = falling_3 or (drop_3 is not None and drop_3 >= compression_drop_usd)
        strong_compression = falling_3 and drop_3 is not None and drop_3 >= strong_drop_usd
        below_two = False
        if len(observations) >= 2:
            last_two = observations[-2:]
            below_two = all(
                item.get("oracle_price") is not None
                and item.get("price_to_beat") is not None
                and float(item["oracle_price"]) <= float(item["price_to_beat"])
                for item in last_two
            )
        upward_three = _strictly_rising(observations, 3)
        return {
            "compression": compression,
            "strong_compression": strong_compression,
            "below_two": below_two,
            "upward_three": upward_three,
        }

    def _build_opportunity(
        self,
        *,
        typed_market: Market,
        row: dict[str, Any],
        observation: dict[str, Any],
        stage: str,
        stage_reason: str,
        stage_fraction: float,
        state: dict[str, Any],
        compression_state: dict[str, bool],
    ) -> Opportunity | None:
        no_price = observation.get("no_best_ask") or observation.get("no_mid")
        if no_price is None or float(no_price) <= 0.0:
            return None
        token_ids = list(typed_market.clob_token_ids or [])
        no_token = token_ids[1] if len(token_ids) > 1 else None
        yes_token = token_ids[0] if len(token_ids) > 0 else None
        confidence = 0.68 if stage == "probe" else 0.82
        if observation.get("gap_usd") is not None:
            confidence += clamp((40.0 - float(observation["gap_usd"])) / 100.0, 0.0, 0.10)
        if compression_state.get("strong_compression"):
            confidence += 0.05
        if observation.get("no_breakout"):
            confidence += 0.05
        confidence = clamp(confidence, 0.50, 0.96)
        edge_percent = max(1.0, (confidence - float(no_price)) * 100.0)
        risk_score = clamp(0.62 - ((confidence - float(no_price)) * 0.65), 0.10, 0.82)

        opp = self.create_opportunity(
            title=f"BTC 5m Threshold Flip {stage.title()} NO",
            description=(
                f"{stage_reason}; gap=${float(observation.get('gap_usd') or 0.0):.2f}, "
                f"YES={float(observation.get('yes_mid') or 0.0):.3f}, NO ask={float(no_price):.3f}"
            ),
            total_cost=float(no_price),
            expected_payout=1.0,
            markets=[typed_market],
            positions=[
                {
                    "action": "BUY",
                    "outcome": "NO",
                    "price": float(no_price),
                    "token_id": no_token,
                    "price_policy": "taker_limit",
                    "time_in_force": "IOC",
                }
            ],
            is_guaranteed=False,
            skip_fee_model=True,
            custom_roi_percent=edge_percent,
            custom_risk_score=risk_score,
            confidence=confidence,
            min_liquidity_hard=max(100.0, float(observation.get("liquidity") or 0.0)),
            min_position_size=1.0,
        )
        if opp is None:
            return None
        opp.stable_id = f"{opp.stable_id}_{stage}"
        opp.id = f"{opp.id}_{stage}"
        opp.risk_factors = [
            f"Stage={stage}",
            f"Gap=${float(observation.get('gap_usd') or 0.0):.2f}",
            f"Compression={compression_state.get('compression', False)}",
        ]
        opp.strategy_context = {
            "source_key": "crypto",
            "strategy_slug": self.strategy_type,
            "strategy_origin": "crypto_worker",
            "stage": stage,
            "stage_reason": stage_reason,
            "stage_fraction": float(stage_fraction),
            "campaign_key": state.get("campaign_key"),
            "confirmed_stage_triggered": bool(stage == "confirm" or state.get("confirm_emitted")),
            "asset": observation.get("asset"),
            "timeframe": observation.get("timeframe"),
            "seconds_left": observation.get("seconds_left"),
            "oracle_price": observation.get("oracle_price"),
            "price_to_beat": observation.get("price_to_beat"),
            "oracle_age_seconds": observation.get("oracle_age_seconds"),
            "gap_usd": observation.get("gap_usd"),
            "gap_abs_usd": observation.get("gap_abs_usd"),
            "gap_velocity_1s": observation.get("gap_velocity_1s"),
            "gap_drop_3": observation.get("gap_drop_3"),
            "compression": bool(compression_state.get("compression")),
            "strong_compression": bool(compression_state.get("strong_compression")),
            "oracle_below_two": bool(compression_state.get("below_two")),
            "yes_mid": observation.get("yes_mid"),
            "no_mid": observation.get("no_mid"),
            "yes_token_id": yes_token,
            "no_token_id": no_token,
            "selected_token_id": no_token,
            "yes_best_bid": observation.get("yes_best_bid"),
            "yes_best_ask": observation.get("yes_best_ask"),
            "no_best_bid": observation.get("no_best_bid"),
            "no_best_ask": observation.get("no_best_ask"),
            "no_last_trade": observation.get("no_last_trade"),
            "no_breakout": bool(observation.get("no_breakout")),
            "flow_imbalance": observation.get("flow_imbalance"),
            "recent_move_zscore": observation.get("recent_move_zscore"),
            "liquidity": observation.get("liquidity"),
            "no_spread": observation.get("no_spread"),
            "visible_no_depth_usd": observation.get("visible_no_depth_usd"),
            "probe_reference_price": state.get("probe_reference_price"),
            "end_time": row.get("end_time"),
        }
        return opp

    def _log_candidate(self, row: dict[str, Any], observation: dict[str, Any], stage: str, outcome: str) -> None:
        logger.info(
            "btc_5m_threshold_flip_candidate",
            market=self._market_key(row),
            seconds_left=observation.get("seconds_left"),
            oracle_price=observation.get("oracle_price"),
            price_to_beat=observation.get("price_to_beat"),
            gap_usd=observation.get("gap_usd"),
            yes_mid=observation.get("yes_mid"),
            no_mid=observation.get("no_mid"),
            stage=stage,
            outcome=outcome,
        )

    def _detect_from_row(self, row: dict[str, Any], cfg: dict[str, Any]) -> Opportunity | None:
        entry_mode = str(cfg.get("entry_mode", "two_stage") or "two_stage").strip().lower()
        if entry_mode not in {"one_stage", "two_stage"}:
            entry_mode = "two_stage"
        asset = str(row.get("asset") or "").strip().upper()
        timeframe = _normalize_timeframe(row.get("timeframe"))
        if asset != str(cfg.get("asset", "BTC") or "BTC").strip().upper():
            return None
        if timeframe != str(cfg.get("timeframe", "5m") or "5m").strip().lower():
            return None

        typed_market = self._row_market(row)
        if typed_market is None:
            return None

        seconds_left = self._seconds_left(row)
        current_window_ok = self._current_window_ok(row, seconds_left)
        oracle_price = _as_float(row.get("oracle_price"))
        price_to_beat = _as_float(row.get("price_to_beat"))
        oracle_age_seconds = _as_float(row.get("oracle_age_seconds"))
        liquidity = max(0.0, float(_as_float(row.get("liquidity")) or 0.0))
        flow_imbalance = _as_float(row.get("flow_imbalance") if row.get("flow_imbalance") is not None else row.get("orderflow_imbalance"))
        recent_move_zscore = _as_float(row.get("recent_move_zscore"))
        quotes = self._quotes_and_tape(typed_market, row)
        yes_mid = quotes["yes_mid"]
        no_mid = quotes["no_mid"]
        no_best_ask = quotes["no_best_ask"]
        no_best_bid = quotes["no_best_bid"]
        no_last_trade = quotes["no_last_trade"]
        no_breakout = bool(
            (no_best_bid is not None and no_best_bid >= to_float(cfg.get("breakout_price", 0.50), 0.50))
            or (no_last_trade is not None and no_last_trade >= to_float(cfg.get("breakout_price", 0.50), 0.50))
            or quotes["no_breakout_trade"]
        )
        gap_usd = None
        if oracle_price is not None and price_to_beat is not None:
            gap_usd = oracle_price - price_to_beat
        observation = {
            "ts": _now_ts(),
            "asset": asset,
            "timeframe": timeframe,
            "seconds_left": seconds_left,
            "oracle_price": oracle_price,
            "price_to_beat": price_to_beat,
            "oracle_age_seconds": oracle_age_seconds,
            "gap_usd": gap_usd,
            "gap_abs_usd": abs(gap_usd) if gap_usd is not None else None,
            "yes_mid": yes_mid,
            "no_mid": no_mid,
            "yes_best_bid": quotes["yes_best_bid"],
            "yes_best_ask": quotes["yes_best_ask"],
            "no_best_bid": no_best_bid,
            "no_best_ask": no_best_ask,
            "no_last_trade": no_last_trade,
            "no_breakout": no_breakout,
            "flow_imbalance": flow_imbalance,
            "recent_move_zscore": recent_move_zscore,
            "liquidity": liquidity,
            "no_spread": quotes["no_spread"],
            "visible_no_depth_usd": quotes["visible_no_depth_usd"],
        }

        market_key = self._market_key(row)
        state = self._record_observation(market_key, observation, cfg)
        observations = state.get("observations") if isinstance(state.get("observations"), list) else []
        if len(observations) >= 2:
            prev_gap = observations[-2].get("gap_usd")
            if prev_gap is not None and gap_usd is not None:
                observation["gap_velocity_1s"] = float(gap_usd) - float(prev_gap)
        observation["gap_drop_3"] = _rolling_drop(observations, 3)
        compression_state = self._compression_state(observations, cfg)

        self._log_candidate(row, observation, "watch", "watch")

        if not current_window_ok:
            return None
        if price_to_beat is None or price_to_beat <= 0.0:
            return None
        if oracle_price is None or oracle_age_seconds is None:
            return None
        if oracle_age_seconds > max(0.1, to_float(cfg.get("max_oracle_age_seconds", 2.0), 2.0)):
            return None
        if seconds_left is None:
            return None
        start_seconds = max(1.0, to_float(cfg.get("entry_window_start_seconds", 60.0), 60.0))
        end_seconds = max(0.0, to_float(cfg.get("entry_window_end_seconds", 2.0), 2.0))
        if not (end_seconds <= seconds_left <= start_seconds):
            return None
        if liquidity < max(0.0, to_float(cfg.get("liquidity_floor_usd", 2500.0), 2500.0)):
            return None
        no_spread = _as_float(observation.get("no_spread"))
        if no_spread is None or no_spread > clamp(to_float(cfg.get("max_no_spread", 0.03), 0.03), 0.0, 0.25):
            return None
        if gap_usd is None or gap_usd <= 0.0 or gap_usd > max(0.0, to_float(cfg.get("max_gap_usd", 40.0), 40.0)):
            return None
        if yes_mid is None or no_mid is None:
            return None
        if yes_mid < to_float(cfg.get("arm_yes_mid_min", 0.75), 0.75):
            return None
        min_flow_imbalance = max(0.0, to_float(cfg.get("min_flow_imbalance", 0.0), 0.0))
        if min_flow_imbalance > 0.0 and flow_imbalance is not None and abs(flow_imbalance) < min_flow_imbalance:
            return None
        max_recent_move_zscore = max(0.0, to_float(cfg.get("max_recent_move_zscore", 99.0), 99.0))
        if recent_move_zscore is not None and recent_move_zscore > max_recent_move_zscore:
            return None

        arm_ok = bool(compression_state.get("compression"))
        if not arm_ok:
            return None

        if not state.get("campaign_key"):
            state["campaign_key"] = f"{market_key}:{int(observation['ts'])}"

        probe_window_end = max(end_seconds, to_float(cfg.get("probe_window_end_seconds", 5.0), 5.0))
        preferred_probe = (
            gap_usd <= to_float(cfg.get("preferred_gap_usd", 20.0), 20.0)
            and yes_mid >= to_float(cfg.get("preferred_yes_mid_min", 0.75), 0.75)
            and no_best_ask is not None
            and no_best_ask <= to_float(cfg.get("preferred_no_ask_max", 0.25), 0.25)
            and bool(compression_state.get("compression"))
        )
        weak_probe = (
            gap_usd > to_float(cfg.get("preferred_gap_usd", 20.0), 20.0)
            and gap_usd <= to_float(cfg.get("max_gap_usd", 40.0), 40.0)
            and yes_mid >= to_float(cfg.get("weak_yes_mid_min", 0.85), 0.85)
            and no_best_ask is not None
            and no_best_ask <= to_float(cfg.get("weak_no_ask_max", 0.20), 0.20)
            and bool(compression_state.get("strong_compression"))
        )

        breakout_final_seconds = max(1.0, to_float(cfg.get("breakout_final_seconds", 10.0), 10.0))
        confirm_gap_cap = max(0.0, to_float(cfg.get("breakout_gap_cap_usd", 5.0), 5.0))
        confirm_on_breakout = seconds_left <= breakout_final_seconds and gap_usd <= confirm_gap_cap and no_breakout
        confirm_on_threshold = bool(compression_state.get("below_two"))

        if entry_mode == "one_stage" and not state.get("confirm_emitted"):
            if preferred_probe or weak_probe or confirm_on_threshold or confirm_on_breakout:
                state["probe_emitted"] = True
                state["confirm_emitted"] = True
                state["probe_reference_price"] = float(no_best_ask or no_mid or 0.0) or None
                if preferred_probe:
                    stage_reason = "single-stage preferred entry"
                elif weak_probe:
                    stage_reason = "single-stage weak entry"
                elif confirm_on_threshold:
                    stage_reason = "single-stage threshold confirmation"
                else:
                    stage_reason = "single-stage late breakout confirmation"
                self._log_candidate(row, observation, "single", "candidate")
                return self._build_opportunity(
                    typed_market=typed_market,
                    row=row,
                    observation=observation,
                    stage="single",
                    stage_reason=stage_reason,
                    stage_fraction=1.0,
                    state=state,
                    compression_state=compression_state,
                )

        if not state.get("probe_emitted") and probe_window_end <= seconds_left <= start_seconds:
            if preferred_probe or weak_probe:
                state["probe_emitted"] = True
                state["probe_reference_price"] = float(no_best_ask or no_mid or 0.0) or None
                stage_reason = "preferred probe" if preferred_probe else "weak probe"
                self._log_candidate(row, observation, "probe", "candidate")
                return self._build_opportunity(
                    typed_market=typed_market,
                    row=row,
                    observation=observation,
                    stage="probe",
                    stage_reason=stage_reason,
                    stage_fraction=clamp(to_float(cfg.get("probe_fraction", 0.25), 0.25), 0.01, 1.0),
                    state=state,
                    compression_state=compression_state,
                )

        if state.get("probe_emitted") and not state.get("confirm_emitted") and (confirm_on_threshold or confirm_on_breakout):
            state["confirm_emitted"] = True
            self._log_candidate(row, observation, "confirm", "candidate")
            return self._build_opportunity(
                typed_market=typed_market,
                row=row,
                observation=observation,
                stage="confirm",
                stage_reason=("threshold confirmation" if confirm_on_threshold else "late breakout confirmation"),
                stage_fraction=clamp(to_float(cfg.get("confirm_fraction", 0.75), 0.75), 0.01, 1.0),
                state=state,
                compression_state=compression_state,
            )

        return None

    def _rejection_reason(self, row: dict[str, Any], cfg: dict[str, Any]) -> str:
        entry_mode = str(cfg.get("entry_mode", "two_stage") or "two_stage").strip().lower()
        if entry_mode not in {"one_stage", "two_stage"}:
            entry_mode = "two_stage"
        asset = str(row.get("asset") or "").strip().upper()
        timeframe = _normalize_timeframe(row.get("timeframe"))
        if asset != str(cfg.get("asset", "BTC") or "BTC").strip().upper():
            return "asset_mismatch"
        if timeframe != str(cfg.get("timeframe", "5m") or "5m").strip().lower():
            return "timeframe_mismatch"

        typed_market = self._row_market(row)
        if typed_market is None:
            return "invalid_market"

        seconds_left = self._seconds_left(row)
        current_window_ok = self._current_window_ok(row, seconds_left)
        oracle_price = _as_float(row.get("oracle_price"))
        price_to_beat = _as_float(row.get("price_to_beat"))
        oracle_age_seconds = _as_float(row.get("oracle_age_seconds"))
        liquidity = max(0.0, float(_as_float(row.get("liquidity")) or 0.0))
        flow_imbalance = _as_float(
            row.get("flow_imbalance") if row.get("flow_imbalance") is not None else row.get("orderflow_imbalance")
        )
        recent_move_zscore = _as_float(row.get("recent_move_zscore"))
        quotes = self._quotes_and_tape(typed_market, row)
        yes_mid = quotes["yes_mid"]
        no_mid = quotes["no_mid"]
        no_best_ask = quotes["no_best_ask"]
        no_best_bid = quotes["no_best_bid"]
        no_last_trade = quotes["no_last_trade"]
        no_breakout = bool(
            (no_best_bid is not None and no_best_bid >= to_float(cfg.get("breakout_price", 0.50), 0.50))
            or (no_last_trade is not None and no_last_trade >= to_float(cfg.get("breakout_price", 0.50), 0.50))
            or quotes["no_breakout_trade"]
        )
        gap_usd = None
        if oracle_price is not None and price_to_beat is not None:
            gap_usd = oracle_price - price_to_beat

        observation = {
            "ts": _now_ts(),
            "asset": asset,
            "timeframe": timeframe,
            "seconds_left": seconds_left,
            "oracle_price": oracle_price,
            "price_to_beat": price_to_beat,
            "oracle_age_seconds": oracle_age_seconds,
            "gap_usd": gap_usd,
            "gap_abs_usd": abs(gap_usd) if gap_usd is not None else None,
            "yes_mid": yes_mid,
            "no_mid": no_mid,
            "yes_best_bid": quotes["yes_best_bid"],
            "yes_best_ask": quotes["yes_best_ask"],
            "no_best_bid": no_best_bid,
            "no_best_ask": no_best_ask,
            "no_last_trade": no_last_trade,
            "no_breakout": no_breakout,
            "flow_imbalance": flow_imbalance,
            "recent_move_zscore": recent_move_zscore,
            "liquidity": liquidity,
            "no_spread": quotes["no_spread"],
            "visible_no_depth_usd": quotes["visible_no_depth_usd"],
        }

        market_key = self._market_key(row)
        markets_state = self.state.get("markets") if isinstance(self.state.get("markets"), dict) else {}
        state = markets_state.get(market_key) if isinstance(markets_state, dict) else {}
        if not isinstance(state, dict):
            state = {}
        observations = state.get("observations") if isinstance(state.get("observations"), list) else []
        observations = [dict(item) for item in observations if isinstance(item, dict)]
        history_seconds = max(5.0, to_float(cfg.get("history_seconds", 15.0), 15.0))
        cutoff = observation["ts"] - history_seconds
        observations = [item for item in observations if float(item.get("ts", 0.0) or 0.0) >= cutoff]
        max_observations = max(3, int(to_float(cfg.get("max_observations", 24), 24.0)))
        observations = (observations + [observation])[-max_observations:]
        compression_state = self._compression_state(observations, cfg)

        if not current_window_ok:
            return "not_current_window"
        if price_to_beat is None or price_to_beat <= 0.0:
            return "missing_reference"
        if oracle_price is None or oracle_age_seconds is None:
            return "oracle_missing"
        if oracle_age_seconds > max(0.1, to_float(cfg.get("max_oracle_age_seconds", 2.0), 2.0)):
            return "oracle_stale"
        if seconds_left is None:
            return "missing_seconds_left"
        start_seconds = max(1.0, to_float(cfg.get("entry_window_start_seconds", 60.0), 60.0))
        end_seconds = max(0.0, to_float(cfg.get("entry_window_end_seconds", 2.0), 2.0))
        if not (end_seconds <= seconds_left <= start_seconds):
            return "outside_entry_window"
        if liquidity < max(0.0, to_float(cfg.get("liquidity_floor_usd", 2500.0), 2500.0)):
            return "low_liquidity"
        no_spread = _as_float(observation.get("no_spread"))
        if no_spread is None:
            return "missing_no_spread"
        if no_spread > clamp(to_float(cfg.get("max_no_spread", 0.03), 0.03), 0.0, 0.25):
            return "no_spread_too_wide"
        if gap_usd is None:
            return "missing_gap"
        if gap_usd <= 0.0:
            return "gap_not_compressing"
        if gap_usd > max(0.0, to_float(cfg.get("max_gap_usd", 40.0), 40.0)):
            return "gap_too_large"
        if yes_mid is None or no_mid is None:
            return "missing_mid_prices"
        if yes_mid < to_float(cfg.get("arm_yes_mid_min", 0.75), 0.75):
            return "yes_mid_too_low"
        min_flow_imbalance = max(0.0, to_float(cfg.get("min_flow_imbalance", 0.0), 0.0))
        if min_flow_imbalance > 0.0 and flow_imbalance is not None and abs(flow_imbalance) < min_flow_imbalance:
            return "flow_imbalance_too_small"
        max_recent_move_zscore = max(0.0, to_float(cfg.get("max_recent_move_zscore", 99.0), 99.0))
        if recent_move_zscore is not None and recent_move_zscore > max_recent_move_zscore:
            return "recent_move_too_large"
        if not compression_state.get("compression"):
            return "compression_not_ready"

        probe_window_end = max(end_seconds, to_float(cfg.get("probe_window_end_seconds", 5.0), 5.0))
        preferred_probe = (
            gap_usd <= to_float(cfg.get("preferred_gap_usd", 20.0), 20.0)
            and yes_mid >= to_float(cfg.get("preferred_yes_mid_min", 0.75), 0.75)
            and no_best_ask is not None
            and no_best_ask <= to_float(cfg.get("preferred_no_ask_max", 0.25), 0.25)
            and bool(compression_state.get("compression"))
        )
        weak_probe = (
            gap_usd > to_float(cfg.get("preferred_gap_usd", 20.0), 20.0)
            and gap_usd <= to_float(cfg.get("max_gap_usd", 40.0), 40.0)
            and yes_mid >= to_float(cfg.get("weak_yes_mid_min", 0.85), 0.85)
            and no_best_ask is not None
            and no_best_ask <= to_float(cfg.get("weak_no_ask_max", 0.20), 0.20)
            and bool(compression_state.get("strong_compression"))
        )
        breakout_final_seconds = max(1.0, to_float(cfg.get("breakout_final_seconds", 10.0), 10.0))
        confirm_gap_cap = max(0.0, to_float(cfg.get("breakout_gap_cap_usd", 5.0), 5.0))
        confirm_on_breakout = seconds_left <= breakout_final_seconds and gap_usd <= confirm_gap_cap and no_breakout
        confirm_on_threshold = bool(compression_state.get("below_two"))

        if entry_mode == "one_stage":
            if preferred_probe or weak_probe or confirm_on_threshold or confirm_on_breakout:
                return "awaiting_state_refresh"
            return "stage_conditions_not_met"
        if not state.get("probe_emitted"):
            if not (probe_window_end <= seconds_left <= start_seconds):
                return "outside_probe_window"
            if preferred_probe or weak_probe:
                return "awaiting_state_refresh"
            return "probe_not_ready"
        if state.get("confirm_emitted"):
            return "campaign_completed"
        if confirm_on_threshold or confirm_on_breakout:
            return "awaiting_state_refresh"
        return "confirm_not_ready"

    def detect(self, events: list, markets: list, prices: dict[str, dict]) -> list[Opportunity]:
        return []

    async def on_event(self, event: DataEvent) -> list[Opportunity]:
        if event.event_type != EventType.CRYPTO_UPDATE:
            return []
        rows = event.payload.get("markets") if isinstance(event.payload, dict) else None
        if not isinstance(rows, list) or not rows:
            return []
        cfg = dict(self.default_config)
        cfg.update(getattr(self, "config", {}) or {})
        self._cleanup_detect_state(
            max(5.0, to_float(cfg.get("history_seconds", 15.0), 15.0)),
            max(3, int(to_float(cfg.get("max_observations", 24), 24.0))),
        )
        out: list[Opportunity] = []
        rejection_counts: dict[str, int] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            opp = self._detect_from_row(row, cfg)
            if opp is not None:
                out.append(opp)
                continue
            reason = self._rejection_reason(row, cfg)
            rejection_counts[reason] = rejection_counts.get(reason, 0) + 1
        top_rejections = sorted(rejection_counts.items(), key=lambda item: item[1], reverse=True)[:4]
        message_parts = [f"Scanned {len(rows)} markets, {len(out)} signals"]
        if top_rejections:
            message_parts.append(
                "rejected: " + ", ".join(f"{count} {reason}" for reason, count in top_rejections)
            )
        self._filter_diagnostics = {
            "strategy_key": self.strategy_type,
            "scanned_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "markets_scanned": len(rows),
            "signals_emitted": len(out),
            "rejections": rejection_counts,
            "message": " \u2014 ".join(message_parts),
            "summary": {f"rejected_{reason}": count for reason, count in rejection_counts.items()},
        }
        return out

    def _evaluate_quotes(self, signal: Any, context: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
        payload = signal_payload(signal)
        strategy_context = payload.get("strategy_context") if isinstance(payload.get("strategy_context"), dict) else {}
        live_market = context.get("live_market")
        if not isinstance(live_market, dict):
            live_market = payload.get("live_market") if isinstance(payload.get("live_market"), dict) else {}
        token_id = str(
            payload.get("selected_token_id")
            or strategy_context.get("selected_token_id")
            or strategy_context.get("no_token_id")
            or live_market.get("selected_token_id")
            or live_market.get("no_token_id")
            or ""
        ).strip()
        no_quotes = _token_quotes(token_id) if token_id else {"mid": None, "bid": None, "ask": None}
        no_trade = _recent_trade_stats(
            token_id,
            breakout_price=to_float(cfg.get("breakout_price", 0.50), 0.50),
            final_seconds=to_float(cfg.get("breakout_final_seconds", 10.0), 10.0),
        ) if token_id else {"last_trade_price": None, "breakout_seen": False}
        no_bid = no_quotes["bid"]
        no_ask = no_quotes["ask"]
        no_mid = no_quotes["mid"]
        if no_mid is None:
            no_mid = _as_float(live_market.get("live_no_price"))
        if no_bid is None and no_mid is not None:
            no_bid = no_mid
        if no_ask is None and no_mid is not None:
            no_ask = no_mid
        yes_mid = _as_float(live_market.get("live_yes_price"))
        if yes_mid is None:
            yes_mid = _as_float(strategy_context.get("yes_mid"))
        visible_depth_usd = None
        market_hint = payload.get("markets")
        if isinstance(market_hint, list) and market_hint:
            first_market = market_hint[0] if isinstance(market_hint[0], dict) else {}
            typed_market = self._row_market(first_market)
            if typed_market is not None:
                levels = StrategySDK.get_book_levels(typed_market, side="NO", max_levels=8)
                visible_depth_usd = _sum_visible_depth_usd(levels)
                if visible_depth_usd is None:
                    depth_summary = StrategySDK.get_order_book_depth(typed_market, side="NO", size_usd=100.0) or {}
                    visible_depth_usd = _as_float(depth_summary.get("available_liquidity"))
        no_spread = None
        if no_bid is not None and no_ask is not None and no_ask >= no_bid:
            no_spread = no_ask - no_bid
        else:
            no_spread = _as_float(strategy_context.get("no_spread"))
        return {
            "yes_mid": yes_mid,
            "no_mid": no_mid,
            "no_best_bid": no_bid,
            "no_best_ask": no_ask,
            "no_last_trade": no_trade.get("last_trade_price"),
            "no_breakout": bool(
                (no_bid is not None and no_bid >= to_float(cfg.get("breakout_price", 0.50), 0.50))
                or (no_trade.get("last_trade_price") is not None and no_trade.get("last_trade_price") >= to_float(cfg.get("breakout_price", 0.50), 0.50))
                or no_trade.get("breakout_seen")
            ),
            "visible_no_depth_usd": visible_depth_usd,
            "no_spread": no_spread,
        }

    def evaluate(self, signal: Any, context: dict[str, Any]) -> StrategyDecision:
        params = context.get("params") or {}
        cfg = dict(self.default_config)
        cfg.update(getattr(self, "config", {}) or {})
        cfg.update(params)

        payload = signal_payload(signal)
        strategy_context = payload.get("strategy_context") if isinstance(payload.get("strategy_context"), dict) else {}
        live_market = context.get("live_market")
        if not isinstance(live_market, dict):
            live_market = payload.get("live_market") if isinstance(payload.get("live_market"), dict) else {}

        stage = str(strategy_context.get("stage") or payload.get("stage") or "probe").strip().lower()
        seconds_left = _as_float(live_market.get("seconds_left"))
        oracle_price = _as_float(live_market.get("oracle_price"))
        price_to_beat = _as_float(live_market.get("price_to_beat"))
        oracle_age_seconds = _as_float(live_market.get("oracle_age_seconds"))
        liquidity = max(0.0, float(_as_float(live_market.get("liquidity_usd")) or _as_float(live_market.get("liquidity")) or strategy_context.get("liquidity") or 0.0))
        flow_imbalance = _as_float(
            live_market.get("flow_imbalance")
            if live_market.get("flow_imbalance") is not None
            else strategy_context.get("flow_imbalance")
        )
        recent_move_zscore = _as_float(
            live_market.get("recent_move_zscore")
            if live_market.get("recent_move_zscore") is not None
            else strategy_context.get("recent_move_zscore")
        )
        quotes = self._evaluate_quotes(signal, context, cfg)
        yes_mid = quotes["yes_mid"]
        no_mid = quotes["no_mid"]
        no_best_bid = quotes["no_best_bid"]
        no_best_ask = quotes["no_best_ask"]
        no_last_trade = quotes["no_last_trade"]
        no_spread = quotes["no_spread"]
        gap_usd = oracle_price - price_to_beat if oracle_price is not None and price_to_beat is not None else None
        gap_drop_3 = _as_float(strategy_context.get("gap_drop_3"))
        compression = bool(strategy_context.get("compression"))
        strong_compression = bool(strategy_context.get("strong_compression"))
        oracle_below_two = bool(strategy_context.get("oracle_below_two")) or bool(gap_usd is not None and gap_usd <= 0.0 and stage == "confirm")
        no_breakout = bool(quotes["no_breakout"])

        max_oracle_age_seconds = max(0.1, to_float(cfg.get("max_oracle_age_seconds", 2.0), 2.0))
        entry_window_start = max(1.0, to_float(cfg.get("entry_window_start_seconds", 60.0), 60.0))
        entry_window_end = max(0.0, to_float(cfg.get("entry_window_end_seconds", 2.0), 2.0))
        probe_window_end = max(entry_window_end, to_float(cfg.get("probe_window_end_seconds", 5.0), 5.0))
        checks = [
            DecisionCheck("source", "Crypto source", str(getattr(signal, "source", "") or "").strip().lower() == "crypto", detail="Requires source=crypto."),
            DecisionCheck("price_to_beat", "Price to beat present", price_to_beat is not None and price_to_beat > 0.0, score=price_to_beat),
            DecisionCheck("oracle_price", "Live oracle present", oracle_price is not None, score=oracle_price),
            DecisionCheck("oracle_fresh", "Oracle freshness", oracle_age_seconds is not None and oracle_age_seconds <= max_oracle_age_seconds, score=oracle_age_seconds, detail=f"max={max_oracle_age_seconds:.1f}s"),
            DecisionCheck("window", "Entry window", seconds_left is not None and entry_window_end <= seconds_left <= entry_window_start, score=seconds_left, detail=f"[{entry_window_end:.0f}, {entry_window_start:.0f}]s"),
            DecisionCheck("liquidity", "Liquidity floor", liquidity >= max(0.0, to_float(cfg.get("liquidity_floor_usd", 2500.0), 2500.0)), score=liquidity, detail=f"min=${to_float(cfg.get('liquidity_floor_usd', 2500.0), 2500.0):.0f}"),
            DecisionCheck("quotes", "Live quote availability", no_best_ask is not None and no_best_bid is not None and yes_mid is not None, detail="Requires live NO bid/ask and YES mid."),
            DecisionCheck("spread", "NO spread cap", no_spread is not None and no_spread <= clamp(to_float(cfg.get("max_no_spread", 0.03), 0.03), 0.0, 0.25), score=no_spread, detail=f"max={to_float(cfg.get('max_no_spread', 0.03), 0.03):.3f}"),
            DecisionCheck("gap", "Gap range", gap_usd is not None and 0.0 < gap_usd <= to_float(cfg.get("max_gap_usd", 40.0), 40.0), score=gap_usd, detail=f"max=${to_float(cfg.get('max_gap_usd', 40.0), 40.0):.0f}"),
            DecisionCheck("yes_mid", "Late YES richness", yes_mid is not None and yes_mid >= to_float(cfg.get("arm_yes_mid_min", 0.75), 0.75), score=yes_mid, detail=f"min={to_float(cfg.get('arm_yes_mid_min', 0.75), 0.75):.2f}"),
        ]
        min_flow_imbalance = max(0.0, to_float(cfg.get("min_flow_imbalance", 0.0), 0.0))
        if min_flow_imbalance > 0.0:
            checks.append(
                DecisionCheck(
                    "flow_imbalance",
                    "Flow imbalance floor",
                    flow_imbalance is not None and abs(flow_imbalance) >= min_flow_imbalance,
                    score=flow_imbalance,
                    detail=f"min=|{min_flow_imbalance:.2f}|",
                )
            )
        max_recent_move_zscore = max(0.0, to_float(cfg.get("max_recent_move_zscore", 99.0), 99.0))
        checks.append(
            DecisionCheck(
                "recent_move",
                "Recent move cap",
                recent_move_zscore is None or recent_move_zscore <= max_recent_move_zscore,
                score=recent_move_zscore,
                detail=f"max={max_recent_move_zscore:.2f}",
            )
        )

        if stage == "probe":
            preferred_probe = (
                gap_usd is not None
                and gap_usd <= to_float(cfg.get("preferred_gap_usd", 20.0), 20.0)
                and yes_mid is not None
                and yes_mid >= to_float(cfg.get("preferred_yes_mid_min", 0.75), 0.75)
                and no_best_ask is not None
                and no_best_ask <= to_float(cfg.get("preferred_no_ask_max", 0.25), 0.25)
                and compression
                and seconds_left is not None
                and probe_window_end <= seconds_left <= entry_window_start
            )
            weak_probe = (
                gap_usd is not None
                and gap_usd > to_float(cfg.get("preferred_gap_usd", 20.0), 20.0)
                and gap_usd <= to_float(cfg.get("max_gap_usd", 40.0), 40.0)
                and yes_mid is not None
                and yes_mid >= to_float(cfg.get("weak_yes_mid_min", 0.85), 0.85)
                and no_best_ask is not None
                and no_best_ask <= to_float(cfg.get("weak_no_ask_max", 0.20), 0.20)
                and strong_compression
                and seconds_left is not None
                and probe_window_end <= seconds_left <= entry_window_start
            )
            checks.extend(
                [
                    DecisionCheck("compression", "Compression armed", compression, score=gap_drop_3, detail=f"drop3={gap_drop_3}"),
                    DecisionCheck("probe_trigger", "Probe trigger", preferred_probe or weak_probe, detail="Requires preferred or weak probe setup."),
                ]
            )
        elif stage == "single":
            preferred_probe = (
                gap_usd is not None
                and gap_usd <= to_float(cfg.get("preferred_gap_usd", 20.0), 20.0)
                and yes_mid is not None
                and yes_mid >= to_float(cfg.get("preferred_yes_mid_min", 0.75), 0.75)
                and no_best_ask is not None
                and no_best_ask <= to_float(cfg.get("preferred_no_ask_max", 0.25), 0.25)
                and compression
            )
            weak_probe = (
                gap_usd is not None
                and gap_usd > to_float(cfg.get("preferred_gap_usd", 20.0), 20.0)
                and gap_usd <= to_float(cfg.get("max_gap_usd", 40.0), 40.0)
                and yes_mid is not None
                and yes_mid >= to_float(cfg.get("weak_yes_mid_min", 0.85), 0.85)
                and no_best_ask is not None
                and no_best_ask <= to_float(cfg.get("weak_no_ask_max", 0.20), 0.20)
                and strong_compression
            )
            confirm_gap_cap = max(0.0, to_float(cfg.get("breakout_gap_cap_usd", 5.0), 5.0))
            breakout_final_seconds = max(1.0, to_float(cfg.get("breakout_final_seconds", 10.0), 10.0))
            confirm_now = oracle_below_two or (
                seconds_left is not None and seconds_left <= breakout_final_seconds and gap_usd is not None and gap_usd <= confirm_gap_cap and no_breakout
            )
            checks.extend(
                [
                    DecisionCheck("compression", "Compression armed", compression or strong_compression, score=gap_drop_3, detail=f"drop3={gap_drop_3}"),
                    DecisionCheck("single_trigger", "Single-stage trigger", preferred_probe or weak_probe or confirm_now, detail="Requires probe-style or confirmation-style entry."),
                ]
            )
        else:
            confirm_gap_cap = max(0.0, to_float(cfg.get("breakout_gap_cap_usd", 5.0), 5.0))
            breakout_final_seconds = max(1.0, to_float(cfg.get("breakout_final_seconds", 10.0), 10.0))
            confirm_now = oracle_below_two or (
                seconds_left is not None and seconds_left <= breakout_final_seconds and gap_usd is not None and gap_usd <= confirm_gap_cap and no_breakout
            )
            probe_reference_price = _as_float(strategy_context.get("probe_reference_price"))
            max_avg_entry_price = clamp(to_float(cfg.get("max_average_entry_price", 0.42), 0.42), 0.05, 0.95)
            probe_fraction = clamp(to_float(cfg.get("probe_fraction", 0.25), 0.25), 0.01, 0.99)
            confirm_fraction = clamp(to_float(cfg.get("confirm_fraction", 0.75), 0.75), 0.01, 0.99)
            average_entry_price = None
            if probe_reference_price is not None and no_best_ask is not None:
                average_entry_price = (probe_reference_price * probe_fraction) + (no_best_ask * confirm_fraction)
            checks.extend(
                [
                    DecisionCheck("confirm_trigger", "Confirmation trigger", confirm_now, score=gap_usd, detail="Requires 2-tick threshold break or late NO breakout."),
                    DecisionCheck("confirm_price", "Confirmation chase cap", no_best_ask is not None and no_best_ask <= clamp(to_float(cfg.get("max_confirmation_no_ask", 0.58), 0.58), 0.05, 0.99), score=no_best_ask, detail=f"max={to_float(cfg.get('max_confirmation_no_ask', 0.58), 0.58):.2f}"),
                    DecisionCheck("average_price", "Average price cap", average_entry_price is None or average_entry_price <= max_avg_entry_price, score=average_entry_price, detail=f"max={max_avg_entry_price:.2f}"),
                ]
            )

        score = (
            max(0.0, float((40.0 - float(gap_usd or 40.0)) if gap_usd is not None else 0.0)) * 0.8
            + float(yes_mid or 0.0) * 20.0
            + max(0.0, float((0.50 - float(no_best_ask or 0.50)) * 40.0))
            + (12.0 if stage == "confirm" else 0.0)
            + (6.0 if no_breakout else 0.0)
        )

        if not all(check.passed for check in checks):
            return StrategyDecision(
                decision="skipped",
                reason="BTC 5m threshold flip filters not met",
                score=score,
                checks=checks,
                payload={
                    "stage": stage,
                    "gap_usd": gap_usd,
                    "yes_mid": yes_mid,
                    "no_best_ask": no_best_ask,
                    "no_breakout": no_breakout,
                },
            )

        trader_context = context.get("trader")
        risk_limits = trader_context.get("risk_limits") if isinstance(trader_context, dict) and isinstance(trader_context.get("risk_limits"), dict) else {}
        base_target, max_size = _trader_size_limits(context)
        stage_fraction = clamp(to_float(strategy_context.get("stage_fraction"), 0.25 if stage == "probe" else 0.75), 0.01, 1.0)
        requested_size = max(1.0, min(max_size, base_target * stage_fraction))
        risk_cap = _as_float(risk_limits.get("max_trade_notional_usd"))
        if risk_cap is not None and risk_cap > 0.0:
            requested_size = min(requested_size, risk_cap * stage_fraction)
        depth_cap = None
        visible_depth_usd = _as_float(quotes.get("visible_no_depth_usd"))
        if visible_depth_usd is not None and visible_depth_usd > 0.0:
            depth_cap = visible_depth_usd * clamp(to_float(cfg.get("max_depth_fraction", 0.05), 0.05), 0.001, 1.0)
            requested_size = min(requested_size, depth_cap)
        requested_size = max(1.0, requested_size)

        return StrategyDecision(
            decision="selected",
            reason=f"BTC 5m threshold flip {stage} selected",
            score=score,
            size_usd=requested_size,
            checks=checks,
            payload={
                "stage": stage,
                "gap_usd": gap_usd,
                "yes_mid": yes_mid,
                "no_mid": no_mid,
                "no_best_bid": no_best_bid,
                "no_best_ask": no_best_ask,
                "no_last_trade": no_last_trade,
                "no_breakout": no_breakout,
                "requested_size_usd": requested_size,
                "visible_no_depth_usd": visible_depth_usd,
                "depth_cap_usd": depth_cap,
            },
        )

    def should_exit(self, position: Any, market_state: dict) -> ExitDecision:
        if market_state.get("is_resolved"):
            return self.default_exit_check(position, market_state)

        strategy_context = getattr(position, "strategy_context", None) or {}
        if not isinstance(strategy_context, dict):
            strategy_context = {}
        stage = str(strategy_context.get("stage") or "probe").strip().lower()
        if stage not in {"probe", "single"}:
            return ExitDecision("hold", "Confirmation position holds through resolution")
        if market_state.get("confirmed_stage_triggered"):
            return ExitDecision("hold", "Confirmation stage triggered; hold probe through resolution")

        market_key = str(strategy_context.get("campaign_key") or getattr(position, "market_id", "") or "probe").strip()
        exit_state = self.state.setdefault("exit_positions", {})
        state = exit_state.get(market_key)
        if not isinstance(state, dict):
            state = {"oracle_gaps": []}
            exit_state[market_key] = state

        oracle_price = _as_float(market_state.get("oracle_price"))
        price_to_beat = _as_float(market_state.get("price_to_beat"))
        gap_usd = oracle_price - price_to_beat if oracle_price is not None and price_to_beat is not None else None
        if gap_usd is not None:
            gaps = state.get("oracle_gaps") if isinstance(state.get("oracle_gaps"), list) else []
            gaps.append({"ts": _now_ts(), "gap_usd": gap_usd})
            gaps = gaps[-6:]
            state["oracle_gaps"] = gaps

        seconds_left = _as_float(market_state.get("seconds_left"))
        current_price = _as_float(market_state.get("current_price"))

        if gap_usd is not None and gap_usd >= 15.0:
            return ExitDecision("close", "Probe abort: oracle reclaimed +15", close_price=current_price)
        gaps = state.get("oracle_gaps") if isinstance(state.get("oracle_gaps"), list) else []
        if _strictly_rising(gaps, 3):
            return ExitDecision("close", "Probe abort: oracle compression reversed", close_price=current_price)
        if seconds_left is not None and seconds_left <= 5.0:
            if gap_usd is not None and gap_usd > 0.0:
                return ExitDecision("close", "Probe abort: endpoint reached above threshold", close_price=current_price)
            if current_price is not None and current_price < 0.10:
                return ExitDecision("close", "Probe abort: NO never lifted", close_price=current_price)

        return ExitDecision("hold", "Probe remains valid")
