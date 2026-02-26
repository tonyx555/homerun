from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from models import Event, Market, Opportunity
from services.strategies.base import BaseStrategy, DecisionCheck, ExitDecision, StrategyDecision
from services.strategy_sdk import StrategySDK
from utils.converters import safe_float, to_confidence


def _to_utc(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


class TradersCopyTradeStrategy(BaseStrategy):
    strategy_type = "traders_copy_trade"
    name = "Traders Copy Trade"
    description = "Mirror tracked wallet trades in real time with explicit scope, sizing, and execution controls"
    source_key = "traders"
    worker_affinity = "traders"
    allow_deduplication = False
    accepted_signal_strategy_types = ["traders_copy_trade"]
    default_config = StrategySDK.traders_copy_trade_defaults()

    def configure(self, config: dict) -> None:
        self.config = StrategySDK.validate_traders_copy_trade_config(config)

    def detect(self, events: list[Event], markets: list[Market], prices: dict[str, dict]) -> list[Opportunity]:
        return []

    async def detect_async(self, events: list[Event], markets: list[Market], prices: dict[str, dict]) -> list[Opportunity]:
        return []

    def evaluate(self, signal: Any, context: dict[str, Any]) -> StrategyDecision:
        params = StrategySDK.validate_traders_copy_trade_config(context.get("params") or {})
        payload = signal.payload_json if isinstance(getattr(signal, "payload_json", None), dict) else {}
        strategy_context = payload.get("strategy_context") if isinstance(payload.get("strategy_context"), dict) else {}
        copy_event = strategy_context.get("copy_event") if isinstance(strategy_context.get("copy_event"), dict) else {}
        source_trade = payload.get("source_trade") if isinstance(payload.get("source_trade"), dict) else {}

        source = str(getattr(signal, "source", "") or "").strip().lower()
        signal_strategy = str(getattr(signal, "strategy_type", "") or "").strip().lower()
        side = str(copy_event.get("side") or source_trade.get("side") or "").strip().upper()
        token_id = str(payload.get("selected_token_id") or payload.get("token_id") or "").strip()
        entry_price = safe_float(getattr(signal, "entry_price", None), safe_float(copy_event.get("price"), 0.0))
        confidence = to_confidence(getattr(signal, "confidence", copy_event.get("confidence")), 0.0)
        source_notional = safe_float(source_trade.get("source_notional_usd"), 0.0)
        if source_notional <= 0.0:
            source_size = safe_float(copy_event.get("size"), 0.0)
            source_notional = max(0.0, source_size * max(0.0, entry_price))

        detected_at = _to_utc(copy_event.get("detected_at") or source_trade.get("detected_at"))
        age_seconds = 0.0
        if detected_at is not None:
            age_seconds = max(0.0, (datetime.now(timezone.utc) - detected_at).total_seconds())

        copy_buys = bool(params.get("copy_buys", True))
        copy_sells = bool(params.get("copy_sells", True))
        copy_delay_seconds = max(0.0, safe_float(params.get("copy_delay_seconds"), 0.0))

        checks = [
            DecisionCheck("source", "Source is traders", source == "traders", detail="requires source=traders"),
            DecisionCheck(
                "strategy_type",
                "Signal strategy matches",
                signal_strategy == self.strategy_type,
                detail=f"signal={signal_strategy or 'unknown'}",
            ),
            DecisionCheck("token", "Token id present", bool(token_id), detail="selected_token_id or token_id required"),
            DecisionCheck(
                "confidence",
                "Confidence threshold",
                confidence >= safe_float(params.get("min_confidence"), 0.45),
                score=confidence,
                detail=f"min={safe_float(params.get('min_confidence'), 0.45):.2f}",
            ),
            DecisionCheck(
                "entry_price",
                "Entry price ceiling",
                entry_price <= safe_float(params.get("max_entry_price"), 0.98),
                score=entry_price,
                detail=f"max={safe_float(params.get('max_entry_price'), 0.98):.3f}",
            ),
            DecisionCheck(
                "min_notional",
                "Source notional floor",
                source_notional >= safe_float(params.get("min_source_notional_usd"), 10.0),
                score=source_notional,
                detail=f"min={safe_float(params.get('min_source_notional_usd'), 10.0):.2f}",
            ),
            DecisionCheck(
                "max_age",
                "Signal freshness",
                age_seconds <= max(1.0, safe_float(params.get("max_signal_age_seconds"), 900.0)),
                score=age_seconds,
                detail=f"max={max(1.0, safe_float(params.get('max_signal_age_seconds'), 900.0)):.0f}s",
            ),
            DecisionCheck(
                "copy_delay",
                "Copy delay elapsed",
                age_seconds >= copy_delay_seconds,
                score=age_seconds,
                detail=f"delay={copy_delay_seconds:.0f}s",
            ),
        ]

        if side == "BUY":
            checks.append(DecisionCheck("copy_side", "BUY side enabled", copy_buys, detail="copy_buys=true required"))
        elif side == "SELL":
            checks.append(DecisionCheck("copy_side", "SELL side enabled", copy_sells, detail="copy_sells=true required"))
        else:
            checks.append(DecisionCheck("copy_side", "Trade side supported", False, detail=f"side={side or 'unknown'}"))

        failed = [check for check in checks if not check.passed]
        if failed:
            reason = ", ".join(check.key for check in failed)
            return StrategyDecision(
                decision="skipped",
                reason=f"copy_trade_gate_failed:{reason}",
                score=max(0.0, confidence * 100.0),
                checks=checks,
            )

        max_position_size = safe_float(params.get("max_position_size"), 1000.0)
        base_size = safe_float(params.get("base_size_usd"), 25.0)
        max_size = safe_float(params.get("max_size_usd"), max(base_size, max_position_size))
        proportional = bool(params.get("proportional_sizing", True))
        proportional_multiplier = safe_float(params.get("proportional_multiplier"), 1.0)

        if proportional and source_notional > 0.0:
            target_size = source_notional * max(0.01, proportional_multiplier)
        else:
            target_size = source_notional if source_notional > 0.0 else base_size

        target_size = max(1.0, min(target_size, max_position_size, max_size))
        score = (confidence * 70.0) + min(30.0, source_notional / 100.0)
        return StrategyDecision(
            decision="selected",
            reason="copy_trade_signal_selected",
            score=score,
            size_usd=target_size,
            checks=checks,
        )

    def should_exit(self, position: Any, market_state: dict[str, Any]) -> ExitDecision:
        return self.default_exit_check(position, market_state)
