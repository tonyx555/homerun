from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from models import Event, Market, Opportunity
from services.strategies.base import BaseStrategy, DecisionCheck, ExitDecision, StrategyDecision
from services.strategy_sdk import StrategySDK
from utils.converters import safe_float, to_confidence

_MAX_LIVE_COPY_SIGNAL_AGE_SECONDS = 5.0

TRADERS_COPY_TRADE_DEFAULTS: dict[str, Any] = {
    "min_confidence": 0.45,
    "min_source_notional_usd": 10.0,
    "max_entry_price": 0.98,
    "max_signal_age_seconds": 5,
    "min_live_liquidity_usd": 150.0,
    "max_adverse_entry_drift_pct": 2.0,
    "copy_delay_seconds": 0,
    "copy_buys": True,
    "copy_sells": True,
    "max_position_size": 1000.0,
    "proportional_sizing": True,
    "proportional_multiplier": 1.0,
    "base_size_usd": 25.0,
    "max_size_usd": 1500.0,
    "traders_scope": {
        "modes": ["tracked", "pool"],
        "individual_wallets": [],
        "group_ids": [],
    },
}

TRADERS_COPY_TRADE_CONFIG_SCHEMA: dict[str, Any] = {
    "param_fields": [
        {"key": "min_confidence", "label": "Min Confidence", "type": "number", "min": 0, "max": 1},
        {"key": "min_source_notional_usd", "label": "Min Source Notional (USD)", "type": "number", "min": 0},
        {"key": "max_entry_price", "label": "Max Entry Price", "type": "number", "min": 0, "max": 1},
        {"key": "max_signal_age_seconds", "label": "Max Signal Age (sec)", "type": "integer", "min": 1, "max": 5},
        {"key": "min_live_liquidity_usd", "label": "Min Live Liquidity (USD)", "type": "number", "min": 0},
        {
            "key": "max_adverse_entry_drift_pct",
            "label": "Max Adverse Entry Drift (%)",
            "type": "number",
            "min": 0,
            "max": 100,
        },
        {"key": "copy_delay_seconds", "label": "Copy Delay (sec)", "type": "integer", "min": 0, "max": 300},
        {"key": "copy_buys", "label": "Copy Buys", "type": "boolean"},
        {"key": "copy_sells", "label": "Copy Sells", "type": "boolean"},
        {"key": "max_position_size", "label": "Max Position Size (USD)", "type": "number", "min": 1, "max": 1000000},
        {"key": "proportional_sizing", "label": "Proportional Sizing", "type": "boolean"},
        {"key": "proportional_multiplier", "label": "Proportional Multiplier", "type": "number", "min": 0.01, "max": 100},
        {"key": "base_size_usd", "label": "Base Size (USD)", "type": "number", "min": 1, "max": 10000},
        {"key": "max_size_usd", "label": "Max Size (USD)", "type": "number", "min": 1, "max": 50000},
        {
            "key": "traders_scope",
            "label": "Wallet Scope",
            "type": "object",
            "properties": [
                {
                    "key": "modes",
                    "label": "Modes",
                    "type": "array[string]",
                    "options": ["tracked", "pool", "individual", "group"],
                    "required": True,
                },
                {"key": "individual_wallets", "label": "Individual Wallets", "type": "array[string]"},
                {"key": "group_ids", "label": "Group IDs", "type": "array[string]"},
            ],
        },
    ]
}


def traders_copy_trade_defaults() -> dict[str, Any]:
    return dict(TRADERS_COPY_TRADE_DEFAULTS)


def traders_copy_trade_config_schema() -> dict[str, Any]:
    return dict(TRADERS_COPY_TRADE_CONFIG_SCHEMA)


def _coerce_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _coerce_float(value: Any, default: float, lo: float, hi: float) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = default
    if not math.isfinite(parsed):
        parsed = default
    return max(lo, min(hi, parsed))


def _coerce_int(value: Any, default: int, lo: int, hi: int) -> int:
    try:
        parsed = int(float(value))
    except Exception:
        parsed = default
    return max(lo, min(hi, parsed))


def validate_traders_copy_trade_config(config: Any) -> dict[str, Any]:
    cfg = traders_copy_trade_defaults()
    raw = config if isinstance(config, dict) else {}
    for key in (
        "min_confidence",
        "min_source_notional_usd",
        "max_entry_price",
        "max_signal_age_seconds",
        "min_live_liquidity_usd",
        "max_adverse_entry_drift_pct",
        "copy_delay_seconds",
        "copy_buys",
        "copy_sells",
        "max_position_size",
        "proportional_sizing",
        "proportional_multiplier",
        "base_size_usd",
        "max_size_usd",
        "traders_scope",
        "max_opportunities",
        "retention_max_opportunities",
        "retention_max_age_minutes",
        "retention_window",
        "retention_period",
        "retention_duration",
        "opportunity_ttl_minutes",
        "opportunity_ttl",
    ):
        if key in raw:
            cfg[key] = raw[key]

    cfg["min_confidence"] = _coerce_float(cfg.get("min_confidence"), 0.45, 0.0, 1.0)
    cfg["min_source_notional_usd"] = _coerce_float(cfg.get("min_source_notional_usd"), 10.0, 0.0, 1_000_000.0)
    cfg["max_entry_price"] = _coerce_float(cfg.get("max_entry_price"), 0.98, 0.0, 1.0)
    cfg["max_signal_age_seconds"] = _coerce_int(cfg.get("max_signal_age_seconds"), 5, 1, 5)
    cfg["min_live_liquidity_usd"] = _coerce_float(cfg.get("min_live_liquidity_usd"), 150.0, 0.0, 1_000_000_000.0)
    cfg["max_adverse_entry_drift_pct"] = _coerce_float(cfg.get("max_adverse_entry_drift_pct"), 2.0, 0.0, 100.0)
    cfg["copy_delay_seconds"] = _coerce_int(cfg.get("copy_delay_seconds"), 0, 0, 300)
    cfg["copy_buys"] = _coerce_bool(cfg.get("copy_buys"), True)
    cfg["copy_sells"] = _coerce_bool(cfg.get("copy_sells"), True)
    cfg["max_position_size"] = _coerce_float(cfg.get("max_position_size"), 1000.0, 1.0, 1_000_000.0)
    cfg["proportional_sizing"] = _coerce_bool(cfg.get("proportional_sizing"), True)
    cfg["proportional_multiplier"] = _coerce_float(cfg.get("proportional_multiplier"), 1.0, 0.01, 100.0)
    cfg["base_size_usd"] = _coerce_float(cfg.get("base_size_usd"), 25.0, 1.0, 10_000.0)
    cfg["max_size_usd"] = _coerce_float(cfg.get("max_size_usd"), max(1.0, float(cfg["base_size_usd"])), 1.0, 50_000.0)
    if cfg["max_size_usd"] < cfg["base_size_usd"]:
        cfg["max_size_usd"] = cfg["base_size_usd"]
    cfg["traders_scope"] = StrategySDK.validate_trader_scope_config(cfg.get("traders_scope"))
    return StrategySDK.normalize_strategy_retention_config(cfg)


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
    default_config = traders_copy_trade_defaults()

    def configure(self, config: dict) -> None:
        self.config = validate_traders_copy_trade_config(config)

    def detect(self, events: list[Event], markets: list[Market], prices: dict[str, dict]) -> list[Opportunity]:
        return []

    async def detect_async(self, events: list[Event], markets: list[Market], prices: dict[str, dict]) -> list[Opportunity]:
        return []

    def evaluate(self, signal: Any, context: dict[str, Any]) -> StrategyDecision:
        context_payload = context if isinstance(context, dict) else {}
        params = validate_traders_copy_trade_config(context_payload.get("params") or {})
        payload = signal.payload_json if isinstance(getattr(signal, "payload_json", None), dict) else {}
        strategy_context = payload.get("strategy_context") if isinstance(payload.get("strategy_context"), dict) else {}
        copy_event = strategy_context.get("copy_event") if isinstance(strategy_context.get("copy_event"), dict) else {}
        source_trade = payload.get("source_trade") if isinstance(payload.get("source_trade"), dict) else {}
        live_market = context_payload.get("live_market") if isinstance(context_payload.get("live_market"), dict) else {}
        runtime_scope_context = (
            context_payload.get("traders_scope_context")
            if isinstance(context_payload.get("traders_scope_context"), dict)
            else None
        )

        source = str(getattr(signal, "source", "") or "").strip().lower()
        signal_strategy = str(getattr(signal, "strategy_type", "") or "").strip().lower()
        accepted_strategy_types = {
            str(item or "").strip().lower()
            for item in self.accepted_signal_strategy_types
            if str(item or "").strip()
        }
        accepted_strategy_types.add(self.strategy_type)
        side = str(copy_event.get("side") or source_trade.get("side") or "").strip().upper()
        source_tx_hash = str(copy_event.get("tx_hash") or source_trade.get("tx_hash") or "").strip()
        token_id = str(payload.get("selected_token_id") or payload.get("token_id") or "").strip()
        signal_entry_price = safe_float(
            getattr(signal, "entry_price", None),
            safe_float(copy_event.get("price"), safe_float(source_trade.get("price"), 0.0)),
        )
        live_entry_price = safe_float(live_market.get("live_selected_price"), None)
        entry_price = signal_entry_price
        entry_price_source = "signal"
        if live_entry_price is not None and live_entry_price > 0.0:
            entry_price = live_entry_price
            entry_price_source = "live_market"
        confidence = to_confidence(getattr(signal, "confidence", copy_event.get("confidence")), 0.0)
        source_notional = safe_float(source_trade.get("source_notional_usd"), 0.0)
        if source_notional <= 0.0:
            source_size = safe_float(copy_event.get("size"), 0.0)
            sizing_price = signal_entry_price if signal_entry_price > 0.0 else entry_price
            source_notional = max(0.0, source_size * max(0.0, sizing_price))

        detected_at = _to_utc(
            copy_event.get("detected_at")
            or source_trade.get("detected_at")
            or copy_event.get("timestamp")
            or source_trade.get("timestamp")
        )
        requested_max_signal_age_seconds = max(
            1.0,
            safe_float(params.get("max_signal_age_seconds"), _MAX_LIVE_COPY_SIGNAL_AGE_SECONDS),
        )
        max_signal_age_seconds = min(
            _MAX_LIVE_COPY_SIGNAL_AGE_SECONDS,
            requested_max_signal_age_seconds,
        )
        age_seconds = max_signal_age_seconds + 1.0
        if detected_at is not None:
            age_seconds = max(0.0, (datetime.now(timezone.utc) - detected_at).total_seconds())

        scope_passed = True
        scope_payload: dict[str, Any] = {}
        scope_detail = "runtime scope context unavailable"
        if runtime_scope_context is not None:
            scope_passed, scope_payload = StrategySDK.match_trader_signal_scope(signal, runtime_scope_context)
            matched_modes = scope_payload.get("matched_modes") if isinstance(scope_payload, dict) else []
            if isinstance(matched_modes, list):
                matched_label = ", ".join(str(mode or "") for mode in matched_modes if str(mode or "").strip())
            else:
                matched_label = ""
            scope_detail = f"matched={matched_label or 'none'}"
        else:
            explicit_scope = StrategySDK.validate_trader_scope_config(params.get("traders_scope"))
            modes = {
                str(mode or "").strip().lower()
                for mode in (explicit_scope.get("modes") or [])
                if str(mode or "").strip()
            }
            individual_wallets = {
                StrategySDK.normalize_trader_wallet(wallet)
                for wallet in (explicit_scope.get("individual_wallets") or [])
                if StrategySDK.normalize_trader_wallet(wallet)
            }
            signal_wallets = StrategySDK.extract_trader_signal_wallets(signal)
            if "individual" in modes and individual_wallets:
                matched_wallets = sorted(signal_wallets.intersection(individual_wallets))
                scope_passed = bool(matched_wallets)
                scope_payload = {
                    "signal_wallets": sorted(signal_wallets),
                    "selected_modes": sorted(modes),
                    "matched_modes": (["individual"] if matched_wallets else []),
                    "matched_wallets": matched_wallets,
                }
                scope_detail = (
                    f"individual_wallets_match={len(matched_wallets)}"
                    if matched_wallets
                    else "individual mode selected but signal wallet mismatch"
                )

        copy_buys = bool(params.get("copy_buys", True))
        copy_sells = bool(params.get("copy_sells", True))
        copy_delay_seconds = max(0.0, safe_float(params.get("copy_delay_seconds"), 0.0))
        min_live_liquidity_usd = max(0.0, safe_float(params.get("min_live_liquidity_usd"), 150.0))
        live_liquidity = safe_float(live_market.get("liquidity_usd"), None)
        liquidity_passed = live_liquidity is None or live_liquidity >= min_live_liquidity_usd
        max_adverse_entry_drift_pct = max(0.0, safe_float(params.get("max_adverse_entry_drift_pct"), 2.0))
        entry_drift_pct = safe_float(live_market.get("entry_price_delta_pct"), None)
        adverse_entry_drift_pct = None
        if entry_drift_pct is not None:
            if side == "BUY":
                adverse_entry_drift_pct = max(0.0, entry_drift_pct)
            elif side == "SELL":
                adverse_entry_drift_pct = max(0.0, -entry_drift_pct)
            else:
                adverse_entry_drift_pct = abs(entry_drift_pct)
        drift_passed = adverse_entry_drift_pct is None or adverse_entry_drift_pct <= max_adverse_entry_drift_pct

        checks = [
            DecisionCheck("source", "Source is traders", source == "traders", detail="requires source=traders"),
            DecisionCheck(
                "strategy_type",
                "Signal strategy matches",
                signal_strategy in accepted_strategy_types,
                detail=(
                    f"signal={signal_strategy or 'unknown'} "
                    f"accepted={','.join(sorted(accepted_strategy_types)) or 'none'}"
                ),
            ),
            DecisionCheck(
                "traders_scope",
                "Signal wallet in selected scope",
                scope_passed,
                detail=scope_detail,
                payload=scope_payload,
            ),
            DecisionCheck(
                "source_trade",
                "Source trade tx hash present",
                bool(source_tx_hash),
                detail="copy_event.tx_hash or source_trade.tx_hash required",
            ),
            DecisionCheck("token", "Token id present", bool(token_id), detail="selected_token_id or token_id required"),
            DecisionCheck(
                "entry_price_available",
                "Entry price available",
                entry_price > 0.0,
                score=entry_price,
                detail=f"source={entry_price_source}",
            ),
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
                detail=(
                    f"max={safe_float(params.get('max_entry_price'), 0.98):.3f}"
                    f" source={entry_price_source}"
                ),
            ),
            DecisionCheck(
                "min_notional",
                "Source notional floor",
                source_notional >= safe_float(params.get("min_source_notional_usd"), 10.0),
                score=source_notional,
                detail=f"min={safe_float(params.get('min_source_notional_usd'), 10.0):.2f}",
            ),
            DecisionCheck(
                "live_liquidity",
                "Live liquidity floor",
                liquidity_passed,
                score=live_liquidity,
                detail=(
                    f"min={min_live_liquidity_usd:.2f}, current={live_liquidity:.2f}"
                    if live_liquidity is not None
                    else "live liquidity unavailable"
                ),
            ),
            DecisionCheck(
                "entry_drift",
                "Adverse entry drift limit",
                drift_passed,
                score=adverse_entry_drift_pct,
                detail=(
                    f"max={max_adverse_entry_drift_pct:.2f}%, adverse={adverse_entry_drift_pct:.2f}%"
                    if adverse_entry_drift_pct is not None
                    else "drift unavailable"
                ),
            ),
            DecisionCheck(
                "signal_timestamp",
                "Signal timestamp available",
                detected_at is not None,
                detail="copy_event.detected_at or copy_event.timestamp required",
            ),
            DecisionCheck(
                "max_age",
                "Signal freshness",
                age_seconds <= max_signal_age_seconds,
                score=age_seconds,
                detail=(
                    f"max={max_signal_age_seconds:.0f}s "
                    f"requested={requested_max_signal_age_seconds:.0f}s"
                ),
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
