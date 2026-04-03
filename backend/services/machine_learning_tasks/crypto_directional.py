from __future__ import annotations

import math
from datetime import datetime
from typing import Any


def _to_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


class CryptoDirectionalTask:
    task_key = "crypto_directional"
    label = "Crypto Directional"
    description = "Directional probability for live crypto markets using Homerun market features."
    allowed_assets = ("btc", "eth", "sol", "xrp")
    allowed_timeframes = ("5m", "15m", "1h", "4h")
    default_lookback = 5

    @staticmethod
    def normalize_asset(value: Any) -> str:
        return str(value or "").strip().lower()

    @staticmethod
    def normalize_timeframe(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in {"5m", "5min", "5-minute", "5minutes"}:
            return "5m"
        if text in {"15m", "15min", "15-minute", "15minutes"}:
            return "15m"
        if text in {"1h", "1hr", "1hour", "60m"}:
            return "1h"
        if text in {"4h", "4hr", "4hour", "240m"}:
            return "4h"
        return text

    def normalize_assets(self, values: list[str] | None) -> list[str]:
        normalized: list[str] = []
        source = list(values or self.allowed_assets)
        for value in source:
            asset = self.normalize_asset(value)
            if asset in self.allowed_assets and asset not in normalized:
                normalized.append(asset)
        return normalized or list(self.allowed_assets)

    def normalize_timeframes(self, values: list[str] | None) -> list[str]:
        normalized: list[str] = []
        source = list(values or self.allowed_timeframes)
        for value in source:
            timeframe = self.normalize_timeframe(value)
            if timeframe in self.allowed_timeframes and timeframe not in normalized:
                normalized.append(timeframe)
        return normalized or list(self.allowed_timeframes)

    def default_feature_names(self, *, lookback: int | None = None) -> list[str]:
        resolved_lookback = max(1, int(lookback or self.default_lookback))
        feature_names = [
            "price",
            "spread",
            "combined",
            "liquidity_log",
            "volume_24h_log",
            "seconds_left_norm",
            "oracle_distance",
            "ptb_distance",
        ]
        for lag in range(1, resolved_lookback + 1):
            feature_names.append(f"return_{lag}")
        feature_names.append("spread_change_1")
        return feature_names

    def scope_matches(
        self,
        market: dict[str, Any],
        *,
        assets: list[str],
        timeframes: list[str],
    ) -> bool:
        asset = self.normalize_asset(market.get("asset"))
        timeframe = self.normalize_timeframe(market.get("timeframe"))
        return asset in set(assets) and timeframe in set(timeframes)

    def build_snapshot_record(self, market: dict[str, Any], *, recorded_at: datetime) -> dict[str, Any] | None:
        asset = self.normalize_asset(market.get("asset"))
        timeframe = self.normalize_timeframe(market.get("timeframe"))
        if asset not in self.allowed_assets or timeframe not in self.allowed_timeframes:
            return None

        up_price = _to_float(market.get("up_price"))
        down_price = _to_float(market.get("down_price"))
        mid_price = _to_float(market.get("mid_price"))
        if mid_price is None:
            if up_price is not None and down_price is not None:
                mid_price = (up_price + (1.0 - down_price)) / 2.0
            elif up_price is not None:
                mid_price = up_price
            elif down_price is not None:
                mid_price = 1.0 - down_price
        if mid_price is None or mid_price <= 0.0:
            return None

        return {
            "task_key": self.task_key,
            "asset": asset,
            "timeframe": timeframe,
            "timestamp": recorded_at,
            "mid_price": round(mid_price, 6),
            "up_price": up_price,
            "down_price": down_price,
            "best_bid": _to_float(market.get("best_bid")),
            "best_ask": _to_float(market.get("best_ask")),
            "spread": _to_float(market.get("spread")),
            "combined": _to_float(market.get("combined")),
            "liquidity": _to_float(market.get("liquidity")),
            "volume": _to_float(market.get("volume")),
            "volume_24h": _to_float(market.get("volume_24h")),
            "oracle_price": _to_float(market.get("oracle_price")),
            "price_to_beat": _to_float(market.get("price_to_beat")),
            "seconds_left": (
                int(float(market.get("seconds_left")))
                if _to_float(market.get("seconds_left")) is not None
                else None
            ),
            "is_live": bool(market.get("is_live")) if market.get("is_live") is not None else None,
        }

    def _current_price(self, row: dict[str, Any]) -> float | None:
        up_price = _to_float(row.get("up_price"))
        if up_price is not None and up_price > 0.0:
            return up_price
        mid_price = _to_float(row.get("mid_price"))
        if mid_price is not None and mid_price > 0.0:
            return mid_price
        down_price = _to_float(row.get("down_price"))
        if down_price is not None and 0.0 <= down_price < 1.0:
            return 1.0 - down_price
        return None

    def _feature_row(
        self,
        rows: list[dict[str, Any]],
        *,
        index: int,
        feature_names: list[str],
    ) -> list[float] | None:
        current = rows[index]
        current_price = self._current_price(current)
        if current_price is None or current_price <= 0.0:
            return None

        current_spread = max(0.0, _to_float(current.get("spread")) or 0.0)
        combined = _to_float(current.get("combined"))
        if combined is None:
            up_price = _to_float(current.get("up_price"))
            down_price = _to_float(current.get("down_price"))
            combined = (up_price + down_price) if up_price is not None and down_price is not None else 1.0

        liquidity = max(1.0, _to_float(current.get("liquidity")) or 1.0)
        volume_24h = max(1.0, _to_float(current.get("volume_24h")) or _to_float(current.get("volume")) or 1.0)
        seconds_left = _to_float(current.get("seconds_left")) or 900.0
        oracle_price = _to_float(current.get("oracle_price")) or 0.0
        price_to_beat = _to_float(current.get("price_to_beat")) or 0.0

        prior_spread = max(0.0, _to_float(rows[index - 1].get("spread")) or 0.0) if index > 0 else current_spread

        values_by_name: dict[str, float] = {
            "price": current_price,
            "mid_price": current_price,
            "spread": current_spread,
            "combined": combined,
            "liquidity_log": math.log(liquidity),
            "volume_24h_log": math.log(volume_24h),
            "seconds_left_norm": min(1.0, seconds_left / 3600.0),
            "oracle_distance": (
                (current_price - oracle_price) / max(abs(oracle_price), 1.0)
                if oracle_price > 0.0
                else 0.0
            ),
            "ptb_distance": (
                (oracle_price - price_to_beat) / max(abs(price_to_beat), 1.0)
                if price_to_beat > 0.0
                else 0.0
            ),
            "spread_change_1": current_spread - prior_spread,
        }

        for feature_name in feature_names:
            if not feature_name.startswith("return_"):
                continue
            try:
                lag = int(feature_name.split("_", 1)[1])
            except (TypeError, ValueError):
                continue
            if index - lag < 0:
                previous_price = current_price
            else:
                previous_price = self._current_price(rows[index - lag]) or current_price
            values_by_name[feature_name] = (current_price - previous_price) / max(abs(previous_price), 1e-6)

        return [float(values_by_name.get(name, 0.0)) for name in feature_names]

    def feature_vector_from_market(
        self,
        market: dict[str, Any],
        *,
        feature_names: list[str],
    ):
        import numpy as np

        synthetic_rows = []
        history_tail = market.get("history_tail") if isinstance(market.get("history_tail"), list) else []
        for snapshot in history_tail:
            if not isinstance(snapshot, dict):
                continue
            synthetic_rows.append(
                {
                    "mid_price": _to_float(snapshot.get("mid")),
                    "spread": (
                        (_to_float(snapshot.get("ask")) or 0.0) - (_to_float(snapshot.get("bid")) or 0.0)
                        if _to_float(snapshot.get("ask")) is not None and _to_float(snapshot.get("bid")) is not None
                        else _to_float(market.get("spread"))
                    ),
                    "combined": _to_float(market.get("combined")),
                    "liquidity": _to_float(market.get("liquidity")),
                    "volume": _to_float(market.get("volume")),
                    "volume_24h": _to_float(market.get("volume_24h")),
                    "seconds_left": _to_float(market.get("seconds_left")),
                    "oracle_price": _to_float(market.get("oracle_price")),
                    "price_to_beat": _to_float(market.get("price_to_beat")),
                    "up_price": None,
                    "down_price": None,
                }
            )

        synthetic_rows.append(
            {
                "mid_price": _to_float(market.get("mid_price")),
                "up_price": _to_float(market.get("up_price")),
                "down_price": _to_float(market.get("down_price")),
                "spread": _to_float(market.get("spread")),
                "combined": _to_float(market.get("combined")),
                "liquidity": _to_float(market.get("liquidity")),
                "volume": _to_float(market.get("volume")),
                "volume_24h": _to_float(market.get("volume_24h")),
                "seconds_left": _to_float(market.get("seconds_left")),
                "oracle_price": _to_float(market.get("oracle_price")),
                "price_to_beat": _to_float(market.get("price_to_beat")),
            }
        )

        if not synthetic_rows:
            return None
        row = self._feature_row(synthetic_rows, index=len(synthetic_rows) - 1, feature_names=feature_names)
        if row is None:
            return None
        return np.asarray(row, dtype=np.float64)

    def build_training_dataset(
        self,
        rows: list[dict[str, Any]],
        *,
        feature_names: list[str] | None = None,
    ) -> tuple[Any, Any, list[str], list[dict[str, Any]]]:
        import numpy as np

        resolved_feature_names = list(feature_names or self.default_feature_names())
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            asset = self.normalize_asset(row.get("asset"))
            timeframe = self.normalize_timeframe(row.get("timeframe"))
            if asset not in self.allowed_assets or timeframe not in self.allowed_timeframes:
                continue
            grouped.setdefault((asset, timeframe), []).append(dict(row))

        x_rows: list[list[float]] = []
        y_rows: list[int] = []
        meta_rows: list[dict[str, Any]] = []
        for (asset, timeframe), group_rows in grouped.items():
            sorted_rows = sorted(group_rows, key=lambda row: row.get("timestamp") or datetime.min)
            if len(sorted_rows) < 3:
                continue
            for index in range(1, len(sorted_rows) - 1):
                feature_row = self._feature_row(sorted_rows, index=index, feature_names=resolved_feature_names)
                if feature_row is None:
                    continue
                current_price = self._current_price(sorted_rows[index])
                next_price = self._current_price(sorted_rows[index + 1])
                if current_price is None or next_price is None:
                    continue
                x_rows.append(feature_row)
                y_rows.append(1 if next_price > current_price else 0)
                meta_rows.append(
                    {
                        "task_key": self.task_key,
                        "asset": asset,
                        "timeframe": timeframe,
                        "timestamp": sorted_rows[index].get("timestamp"),
                        "current_price": current_price,
                        "next_price": next_price,
                    }
                )

        if not x_rows:
            return np.empty((0, len(resolved_feature_names)), dtype=np.float64), np.empty((0,), dtype=np.int32), resolved_feature_names, []
        return (
            np.asarray(x_rows, dtype=np.float64),
            np.asarray(y_rows, dtype=np.int32),
            resolved_feature_names,
            meta_rows,
        )
