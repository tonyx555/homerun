#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

np: Any | None = None


HF_DATASET_API = "https://huggingface.co/api/datasets/{dataset_id}"
HF_DATASET_RESOLVE = "https://huggingface.co/datasets/{dataset_id}/resolve/main/{filename}"
SUPPORTED_ASSETS = ("btc", "eth", "sol", "xrp")
DEFAULT_DATASET_IDS = ("trentmkelly/polymarket_crypto_derivatives",)
DEFAULT_TIMEFRAMES = (5, 15, 60, 240)
DEFAULT_EPOCHS = 600
DEFAULT_LEARNING_RATE = 0.07
DEFAULT_L2 = 0.0007
DEFAULT_MIN_TRAIN_ROWS = 20
REQUEST_USER_AGENT = "homerun-ml-pipeline/1.0"
FILE_PATTERN = re.compile(
    r"^(?P<prefix>[a-z0-9]+)_market(?P<market_id>\d+)_(?P<date>\d{4}-\d{2}-\d{2})_(?P<time>\d{2}-\d{2}-\d{2})(?:_ext)?\.ndjson$"
)


@dataclass(frozen=True)
class SourceFile:
    dataset_id: str
    filename: str
    asset: str
    market_id: str
    start_ts_ms: int


@dataclass(frozen=True)
class LocalSourceFile:
    path: Path
    dataset_id: str
    filename: str
    asset: str
    market_id: str
    start_ts_ms: int


@dataclass
class MinuteBar:
    asset: str
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    spread: float
    quote_imbalance: float
    bid_depth: float
    ask_depth: float
    trade_count: int
    trade_volume: float
    signed_trade_volume: float


@dataclass
class TimeframeBar:
    asset: str
    timeframe_minutes: int
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    spread_mean: float
    imbalance_mean: float
    bid_depth_mean: float
    ask_depth_mean: float
    trade_count: int
    trade_volume: float
    signed_trade_volume: float
    minute_coverage: int


class MinuteAccumulator:
    def __init__(self) -> None:
        self.open_price: float | None = None
        self.high_price: float | None = None
        self.low_price: float | None = None
        self.close_price: float | None = None
        self.close_ts: int = -1
        self.spread_sum: float = 0.0
        self.spread_count: int = 0
        self.spread_last: float = 0.0
        self.imbalance_sum: float = 0.0
        self.imbalance_count: int = 0
        self.imbalance_last: float = 0.0
        self.bid_depth_sum: float = 0.0
        self.ask_depth_sum: float = 0.0
        self.depth_count: int = 0
        self.trade_count: int = 0
        self.trade_buy_count: int = 0
        self.trade_sell_count: int = 0
        self.trade_volume_sum: float = 0.0
        self.signed_trade_volume_sum: float = 0.0

    def update(self, payload: dict[str, Any], event_ts: int) -> None:
        event_type = _coerce_int(payload.get("type"), default=0)
        best_bid = _coerce_float(payload.get("best_bid"))
        best_ask = _coerce_float(payload.get("best_ask"))
        spread = _coerce_float(payload.get("spread"))
        bid_size_total = _coerce_float(payload.get("bid_size_total"))
        ask_size_total = _coerce_float(payload.get("ask_size_total"))

        reference_price = _extract_reference_price(payload, event_type, best_bid, best_ask)
        if reference_price is not None:
            self._update_ohlc(reference_price, event_ts)

        if spread is not None and spread >= 0.0:
            self.spread_sum += spread
            self.spread_count += 1
            self.spread_last = spread

        if bid_size_total is not None and ask_size_total is not None:
            bid_depth = max(0.0, bid_size_total)
            ask_depth = max(0.0, ask_size_total)
            self.bid_depth_sum += bid_depth
            self.ask_depth_sum += ask_depth
            self.depth_count += 1
            total_depth = bid_depth + ask_depth
            if total_depth > 0.0:
                imbalance = (bid_depth - ask_depth) / total_depth
                self.imbalance_sum += imbalance
                self.imbalance_count += 1
                self.imbalance_last = imbalance

        if event_type == 2:
            trade_size = _coerce_float(payload.get("size"))
            if trade_size is None:
                return
            trade_size = max(0.0, trade_size)
            trade_side = _coerce_float(payload.get("side"), default=0.0)
            trade_sign = 1.0 if trade_side > 0.0 else -1.0
            self.trade_count += 1
            if trade_sign > 0.0:
                self.trade_buy_count += 1
            else:
                self.trade_sell_count += 1
            self.trade_volume_sum += trade_size
            self.signed_trade_volume_sum += trade_size * trade_sign

    def _update_ohlc(self, price: float, event_ts: int) -> None:
        if self.open_price is None:
            self.open_price = price
            self.high_price = price
            self.low_price = price
            self.close_price = price
            self.close_ts = event_ts
            return
        assert self.high_price is not None
        assert self.low_price is not None
        self.high_price = max(self.high_price, price)
        self.low_price = min(self.low_price, price)
        if event_ts >= self.close_ts:
            self.close_price = price
            self.close_ts = event_ts

    def to_bar(
        self,
        *,
        asset: str,
        ts_ms: int,
        prev_close: float | None,
        prev_spread: float,
        prev_imbalance: float,
        prev_bid_depth: float,
        prev_ask_depth: float,
    ) -> MinuteBar | None:
        open_price = self.open_price
        high_price = self.high_price
        low_price = self.low_price
        close_price = self.close_price

        if open_price is None or high_price is None or low_price is None or close_price is None:
            if prev_close is None:
                return None
            open_price = prev_close
            high_price = prev_close
            low_price = prev_close
            close_price = prev_close

        spread = self.spread_sum / self.spread_count if self.spread_count > 0 else prev_spread
        quote_imbalance = self.imbalance_sum / self.imbalance_count if self.imbalance_count > 0 else prev_imbalance
        bid_depth = self.bid_depth_sum / self.depth_count if self.depth_count > 0 else prev_bid_depth
        ask_depth = self.ask_depth_sum / self.depth_count if self.depth_count > 0 else prev_ask_depth

        return MinuteBar(
            asset=asset,
            ts_ms=ts_ms,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            spread=spread,
            quote_imbalance=quote_imbalance,
            bid_depth=bid_depth,
            ask_depth=ask_depth,
            trade_count=self.trade_count,
            trade_volume=self.trade_volume_sum,
            signed_trade_volume=self.signed_trade_volume_sum,
        )


class TimeframeAccumulator:
    def __init__(self, *, asset: str, timeframe_minutes: int, ts_ms: int) -> None:
        self.asset = asset
        self.timeframe_minutes = timeframe_minutes
        self.ts_ms = ts_ms
        self.open: float | None = None
        self.high: float | None = None
        self.low: float | None = None
        self.close: float | None = None
        self.spread_sum = 0.0
        self.imbalance_sum = 0.0
        self.bid_depth_sum = 0.0
        self.ask_depth_sum = 0.0
        self.minute_coverage = 0
        self.trade_count = 0
        self.trade_volume = 0.0
        self.signed_trade_volume = 0.0

    def update(self, minute_bar: MinuteBar) -> None:
        if self.open is None:
            self.open = minute_bar.open
            self.high = minute_bar.high
            self.low = minute_bar.low
            self.close = minute_bar.close
        else:
            assert self.high is not None
            assert self.low is not None
            self.high = max(self.high, minute_bar.high)
            self.low = min(self.low, minute_bar.low)
            self.close = minute_bar.close

        self.spread_sum += minute_bar.spread
        self.imbalance_sum += minute_bar.quote_imbalance
        self.bid_depth_sum += minute_bar.bid_depth
        self.ask_depth_sum += minute_bar.ask_depth
        self.minute_coverage += 1
        self.trade_count += minute_bar.trade_count
        self.trade_volume += minute_bar.trade_volume
        self.signed_trade_volume += minute_bar.signed_trade_volume

    def finalize(self) -> TimeframeBar:
        assert self.open is not None
        assert self.high is not None
        assert self.low is not None
        assert self.close is not None
        coverage = max(1, self.minute_coverage)
        return TimeframeBar(
            asset=self.asset,
            timeframe_minutes=self.timeframe_minutes,
            ts_ms=self.ts_ms,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            spread_mean=self.spread_sum / coverage,
            imbalance_mean=self.imbalance_sum / coverage,
            bid_depth_mean=self.bid_depth_sum / coverage,
            ask_depth_mean=self.ask_depth_sum / coverage,
            trade_count=self.trade_count,
            trade_volume=self.trade_volume,
            signed_trade_volume=self.signed_trade_volume,
            minute_coverage=self.minute_coverage,
        )


def _safe_dataset_dir_name(dataset_id: str) -> str:
    return dataset_id.replace("/", "__")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_numpy() -> Any:
    global np
    if np is not None:
        return np
    try:
        import numpy as _np
    except ImportError as exc:  # pragma: no cover - runtime guard
        raise SystemExit("numpy is required. Install with: python -m pip install -r backend/requirements.txt") from exc
    np = _np
    return np


def _parse_csv_str_list(raw_value: str, *, lowercase: bool) -> list[str]:
    values = [part.strip() for part in raw_value.split(",")]
    if lowercase:
        values = [value.lower() for value in values]
    return [value for value in values if value]


def _parse_csv_int_list(raw_value: str) -> list[int]:
    out: list[int] = []
    for part in raw_value.split(","):
        item = part.strip()
        if not item:
            continue
        out.append(int(item))
    return out


def _coerce_float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(parsed) or math.isinf(parsed):
        return default
    return parsed


def _coerce_int(value: Any, default: int | None = None) -> int | None:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _extract_reference_price(
    payload: dict[str, Any],
    event_type: int,
    best_bid: float | None,
    best_ask: float | None,
) -> float | None:
    if event_type == 2:
        trade_price = _coerce_float(payload.get("price"))
        if trade_price is not None and 0.0 <= trade_price <= 1.0:
            return trade_price
    if best_bid is not None and best_ask is not None:
        mid = (best_bid + best_ask) / 2.0
        if 0.0 <= mid <= 1.0:
            return mid
    if best_bid is not None and 0.0 <= best_bid <= 1.0:
        return best_bid
    if best_ask is not None and 0.0 <= best_ask <= 1.0:
        return best_ask
    return None


def _parse_source_filename(filename: str) -> tuple[str | None, str | None, int | None]:
    match = FILE_PATTERN.match(filename)
    if not match:
        return None, None, None

    prefix = str(match.group("prefix") or "").lower()
    market_id = str(match.group("market_id") or "")
    date_component = str(match.group("date") or "")
    time_component = str(match.group("time") or "")

    asset: str | None
    if prefix.endswith("15m") and len(prefix) > 3:
        asset = prefix[:-3]
    elif prefix in SUPPORTED_ASSETS:
        asset = prefix
    else:
        asset = None

    start_ts_ms: int | None
    try:
        dt = datetime.strptime(f"{date_component} {time_component}", "%Y-%m-%d %H-%M-%S").replace(tzinfo=timezone.utc)
        start_ts_ms = int(dt.timestamp() * 1000)
    except ValueError:
        start_ts_ms = None

    return asset, market_id or None, start_ts_ms


def _http_json(url: str, *, timeout_seconds: int, retry_attempts: int) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(retry_attempts):
        req = Request(url=url, headers={"User-Agent": REQUEST_USER_AGENT, "Accept": "application/json"})
        try:
            with urlopen(req, timeout=timeout_seconds) as response:
                payload = response.read()
            parsed = json.loads(payload.decode("utf-8"))
            if not isinstance(parsed, dict):
                raise RuntimeError(f"Expected JSON object from {url}")
            return parsed
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, RuntimeError) as exc:
            last_error = exc
            if attempt >= retry_attempts - 1:
                break
            time.sleep(min(5.0, 0.8 * (2**attempt)))
    assert last_error is not None
    raise last_error


def discover_remote_files(
    *,
    dataset_ids: list[str],
    assets: list[str],
    max_files_per_asset: int,
    max_files_total: int,
    timeout_seconds: int,
    retry_attempts: int,
) -> list[SourceFile]:
    selected_assets = {asset.lower() for asset in assets}
    discovered: list[SourceFile] = []

    for dataset_id in dataset_ids:
        payload = _http_json(
            HF_DATASET_API.format(dataset_id=dataset_id),
            timeout_seconds=timeout_seconds,
            retry_attempts=retry_attempts,
        )
        siblings = payload.get("siblings")
        if not isinstance(siblings, list):
            continue
        for sibling in siblings:
            if not isinstance(sibling, dict):
                continue
            filename = sibling.get("rfilename")
            if not isinstance(filename, str) or not filename.endswith(".ndjson"):
                continue
            asset, market_id, start_ts_ms = _parse_source_filename(filename)
            if asset is None or market_id is None or start_ts_ms is None:
                continue
            if asset not in selected_assets:
                continue
            discovered.append(
                SourceFile(
                    dataset_id=dataset_id,
                    filename=filename,
                    asset=asset,
                    market_id=market_id,
                    start_ts_ms=start_ts_ms,
                )
            )

    discovered.sort(key=lambda row: (row.asset, row.start_ts_ms, row.market_id, row.dataset_id, row.filename))

    if max_files_per_asset > 0:
        capped: list[SourceFile] = []
        count_by_asset: dict[str, int] = {}
        for source_file in discovered:
            current_count = count_by_asset.get(source_file.asset, 0)
            if current_count >= max_files_per_asset:
                continue
            capped.append(source_file)
            count_by_asset[source_file.asset] = current_count + 1
        discovered = capped

    if max_files_total > 0:
        discovered = discovered[:max_files_total]

    return discovered


def _download_single_file(
    *,
    source_file: SourceFile,
    raw_dir: Path,
    timeout_seconds: int,
    retry_attempts: int,
    overwrite: bool,
) -> tuple[str, SourceFile, int]:
    dataset_dir = raw_dir / _safe_dataset_dir_name(source_file.dataset_id)
    dataset_dir.mkdir(parents=True, exist_ok=True)
    target_path = dataset_dir / source_file.filename

    if target_path.exists() and target_path.stat().st_size > 0 and not overwrite:
        return "skipped", source_file, int(target_path.stat().st_size)

    url = HF_DATASET_RESOLVE.format(
        dataset_id=source_file.dataset_id,
        filename=quote(source_file.filename),
    )

    last_error: Exception | None = None
    for attempt in range(retry_attempts):
        req = Request(url=url, headers={"User-Agent": REQUEST_USER_AGENT, "Accept": "application/x-ndjson"})
        part_path = target_path.with_suffix(target_path.suffix + ".part")
        try:
            with urlopen(req, timeout=timeout_seconds) as response:
                with part_path.open("wb") as output:
                    bytes_written = 0
                    while True:
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        output.write(chunk)
                        bytes_written += len(chunk)
            part_path.replace(target_path)
            return "downloaded", source_file, bytes_written
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            last_error = exc
            if part_path.exists():
                part_path.unlink(missing_ok=True)
            if attempt >= retry_attempts - 1:
                break
            time.sleep(min(5.0, 0.8 * (2**attempt)))

    assert last_error is not None
    raise RuntimeError(f"Failed downloading {source_file.dataset_id}/{source_file.filename}: {last_error}")


def download_files(
    *,
    files: list[SourceFile],
    raw_dir: Path,
    workers: int,
    timeout_seconds: int,
    retry_attempts: int,
    overwrite: bool,
    quiet: bool,
) -> dict[str, Any]:
    if not files:
        return {
            "requested_files": 0,
            "downloaded_files": 0,
            "skipped_files": 0,
            "downloaded_bytes": 0,
            "assets": {},
        }

    requested = len(files)
    downloaded = 0
    skipped = 0
    downloaded_bytes = 0
    by_asset_requested: dict[str, int] = {}
    by_asset_downloaded: dict[str, int] = {}
    by_asset_skipped: dict[str, int] = {}

    for item in files:
        by_asset_requested[item.asset] = by_asset_requested.get(item.asset, 0) + 1

    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {
            executor.submit(
                _download_single_file,
                source_file=file_item,
                raw_dir=raw_dir,
                timeout_seconds=timeout_seconds,
                retry_attempts=retry_attempts,
                overwrite=overwrite,
            ): file_item
            for file_item in files
        }

        for future in as_completed(futures):
            status, source_file, size_bytes = future.result()
            if status == "downloaded":
                downloaded += 1
                downloaded_bytes += size_bytes
                by_asset_downloaded[source_file.asset] = by_asset_downloaded.get(source_file.asset, 0) + 1
            else:
                skipped += 1
                by_asset_skipped[source_file.asset] = by_asset_skipped.get(source_file.asset, 0) + 1

    if not quiet:
        print(
            f"download summary: requested={requested} downloaded={downloaded} skipped={skipped} bytes={downloaded_bytes}"
        )

    return {
        "requested_files": requested,
        "downloaded_files": downloaded,
        "skipped_files": skipped,
        "downloaded_bytes": downloaded_bytes,
        "assets": {
            asset: {
                "requested": by_asset_requested.get(asset, 0),
                "downloaded": by_asset_downloaded.get(asset, 0),
                "skipped": by_asset_skipped.get(asset, 0),
            }
            for asset in sorted(by_asset_requested)
        },
    }


def select_local_files(
    *,
    raw_dir: Path,
    assets: list[str],
    max_files_per_asset: int,
    max_files_total: int,
    dataset_ids: list[str],
) -> list[LocalSourceFile]:
    selected_assets = {asset.lower() for asset in assets}
    allowed_datasets = {_safe_dataset_dir_name(item) for item in dataset_ids}
    discovered: list[LocalSourceFile] = []

    for path in raw_dir.rglob("*.ndjson"):
        if not path.is_file():
            continue
        if path.parent.name not in allowed_datasets:
            continue
        asset, market_id, start_ts_ms = _parse_source_filename(path.name)
        if asset is None or market_id is None or start_ts_ms is None:
            continue
        if asset not in selected_assets:
            continue
        dataset_dir_name = path.parent.name
        dataset_id = dataset_dir_name.replace("__", "/")
        discovered.append(
            LocalSourceFile(
                path=path,
                dataset_id=dataset_id,
                filename=path.name,
                asset=asset,
                market_id=market_id,
                start_ts_ms=start_ts_ms,
            )
        )

    discovered.sort(key=lambda row: (row.asset, row.start_ts_ms, row.market_id, str(row.path)))

    if max_files_per_asset > 0:
        capped: list[LocalSourceFile] = []
        count_by_asset: dict[str, int] = {}
        for local_file in discovered:
            current_count = count_by_asset.get(local_file.asset, 0)
            if current_count >= max_files_per_asset:
                continue
            capped.append(local_file)
            count_by_asset[local_file.asset] = current_count + 1
        discovered = capped

    if max_files_total > 0:
        discovered = discovered[:max_files_total]

    return discovered


def process_local_ndjson(
    *,
    local_files: list[LocalSourceFile],
    assets: list[str],
    quiet: bool,
) -> tuple[dict[str, list[MinuteBar]], dict[str, Any]]:
    minute_maps: dict[str, dict[int, MinuteAccumulator]] = {asset: {} for asset in assets}
    processed_files = 0
    processed_lines = 0
    ignored_lines = 0
    bad_json_lines = 0

    for local_file in local_files:
        processed_files += 1
        if not quiet:
            print(f"processing file {processed_files}/{len(local_files)}: {local_file.path}")
        with local_file.path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                processed_lines += 1
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    bad_json_lines += 1
                    continue
                if not isinstance(payload, dict):
                    ignored_lines += 1
                    continue
                outcome_up = _coerce_int(payload.get("outcome_up"), default=0)
                if outcome_up != 1:
                    ignored_lines += 1
                    continue
                ts_ms = _coerce_int(payload.get("ts"))
                if ts_ms is None:
                    ts_ms = _coerce_int(payload.get("message_ts"))
                if ts_ms is None or ts_ms <= 0:
                    ignored_lines += 1
                    continue
                minute_ts = ts_ms - (ts_ms % 60000)
                bucket = minute_maps[local_file.asset]
                acc = bucket.get(minute_ts)
                if acc is None:
                    acc = MinuteAccumulator()
                    bucket[minute_ts] = acc
                acc.update(payload, ts_ms)

    minute_bars: dict[str, list[MinuteBar]] = {}
    for asset in assets:
        minute_bars[asset] = _finalize_minute_series(asset=asset, minute_map=minute_maps.get(asset, {}))

    summary = {
        "processed_files": processed_files,
        "processed_lines": processed_lines,
        "ignored_lines": ignored_lines,
        "bad_json_lines": bad_json_lines,
        "minute_rows": {asset: len(rows) for asset, rows in minute_bars.items()},
    }
    return minute_bars, summary


def _finalize_minute_series(*, asset: str, minute_map: dict[int, MinuteAccumulator]) -> list[MinuteBar]:
    if not minute_map:
        return []

    minute_keys = sorted(minute_map)
    first_minute = minute_keys[0]
    last_minute = minute_keys[-1]
    out: list[MinuteBar] = []

    prev_close: float | None = None
    prev_spread = 0.0
    prev_imbalance = 0.0
    prev_bid_depth = 0.0
    prev_ask_depth = 0.0

    for minute_ts in range(first_minute, last_minute + 1, 60000):
        acc = minute_map.get(minute_ts)
        if acc is None:
            if prev_close is None:
                continue
            bar = MinuteBar(
                asset=asset,
                ts_ms=minute_ts,
                open=prev_close,
                high=prev_close,
                low=prev_close,
                close=prev_close,
                spread=prev_spread,
                quote_imbalance=prev_imbalance,
                bid_depth=prev_bid_depth,
                ask_depth=prev_ask_depth,
                trade_count=0,
                trade_volume=0.0,
                signed_trade_volume=0.0,
            )
        else:
            bar = acc.to_bar(
                asset=asset,
                ts_ms=minute_ts,
                prev_close=prev_close,
                prev_spread=prev_spread,
                prev_imbalance=prev_imbalance,
                prev_bid_depth=prev_bid_depth,
                prev_ask_depth=prev_ask_depth,
            )
            if bar is None:
                continue

        prev_close = bar.close
        prev_spread = bar.spread
        prev_imbalance = bar.quote_imbalance
        prev_bid_depth = bar.bid_depth
        prev_ask_depth = bar.ask_depth
        out.append(bar)

    return out


def aggregate_timeframe(minute_bars: list[MinuteBar], timeframe_minutes: int) -> list[TimeframeBar]:
    if not minute_bars:
        return []

    bucket_ms = timeframe_minutes * 60_000
    out: list[TimeframeBar] = []
    current: TimeframeAccumulator | None = None

    for minute_bar in minute_bars:
        bucket_ts = minute_bar.ts_ms - (minute_bar.ts_ms % bucket_ms)
        if current is None or bucket_ts != current.ts_ms:
            if current is not None:
                out.append(current.finalize())
            current = TimeframeAccumulator(asset=minute_bar.asset, timeframe_minutes=timeframe_minutes, ts_ms=bucket_ts)
        current.update(minute_bar)

    if current is not None:
        out.append(current.finalize())

    return out


def write_minute_csv(output_path: Path, bars_by_asset: dict[str, list[MinuteBar]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "asset",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "spread",
        "quote_imbalance",
        "bid_depth",
        "ask_depth",
        "trade_count",
        "trade_volume",
        "signed_trade_volume",
    ]
    with gzip.open(output_path, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for asset in sorted(bars_by_asset):
            for row in bars_by_asset[asset]:
                writer.writerow(
                    {
                        "asset": row.asset,
                        "timestamp": _timestamp_to_iso(row.ts_ms),
                        "open": f"{row.open:.10f}",
                        "high": f"{row.high:.10f}",
                        "low": f"{row.low:.10f}",
                        "close": f"{row.close:.10f}",
                        "spread": f"{row.spread:.10f}",
                        "quote_imbalance": f"{row.quote_imbalance:.10f}",
                        "bid_depth": f"{row.bid_depth:.10f}",
                        "ask_depth": f"{row.ask_depth:.10f}",
                        "trade_count": row.trade_count,
                        "trade_volume": f"{row.trade_volume:.10f}",
                        "signed_trade_volume": f"{row.signed_trade_volume:.10f}",
                    }
                )


def write_timeframe_csv(output_path: Path, bars_by_asset: dict[str, list[TimeframeBar]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "asset",
        "timeframe_minutes",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "spread_mean",
        "imbalance_mean",
        "bid_depth_mean",
        "ask_depth_mean",
        "trade_count",
        "trade_volume",
        "signed_trade_volume",
        "minute_coverage",
    ]
    with gzip.open(output_path, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for asset in sorted(bars_by_asset):
            for row in bars_by_asset[asset]:
                writer.writerow(
                    {
                        "asset": row.asset,
                        "timeframe_minutes": row.timeframe_minutes,
                        "timestamp": _timestamp_to_iso(row.ts_ms),
                        "open": f"{row.open:.10f}",
                        "high": f"{row.high:.10f}",
                        "low": f"{row.low:.10f}",
                        "close": f"{row.close:.10f}",
                        "spread_mean": f"{row.spread_mean:.10f}",
                        "imbalance_mean": f"{row.imbalance_mean:.10f}",
                        "bid_depth_mean": f"{row.bid_depth_mean:.10f}",
                        "ask_depth_mean": f"{row.ask_depth_mean:.10f}",
                        "trade_count": row.trade_count,
                        "trade_volume": f"{row.trade_volume:.10f}",
                        "signed_trade_volume": f"{row.signed_trade_volume:.10f}",
                        "minute_coverage": row.minute_coverage,
                    }
                )


def _timestamp_to_iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


def build_feature_matrix(
    *,
    timeframe_bars_by_asset: dict[str, list[TimeframeBar]],
    assets: list[str],
) -> tuple[np.ndarray, np.ndarray, list[str], dict[str, int], list[int]]:
    np_mod = _require_numpy()
    feature_names = [
        "ret_1",
        "ret_3",
        "ret_6",
        "range_6",
        "volume_zscore",
        "spread_ma3",
        "imbalance_ma3",
        "trade_intensity",
        "signed_volume_ratio_6",
        "depth_ratio",
        "close_position",
        "hour_sin",
        "hour_cos",
    ] + [f"asset_{asset}" for asset in assets]

    rows: list[tuple[int, list[float], float, str]] = []
    row_counts_by_asset: dict[str, int] = {asset: 0 for asset in assets}

    for asset in assets:
        bars = timeframe_bars_by_asset.get(asset, [])
        if len(bars) < 22:
            continue

        closes = [bar.close for bar in bars]
        highs = [bar.high for bar in bars]
        lows = [bar.low for bar in bars]
        spreads = [bar.spread_mean for bar in bars]
        imbalances = [bar.imbalance_mean for bar in bars]
        trade_counts = [bar.trade_count for bar in bars]
        trade_volumes = [bar.trade_volume for bar in bars]
        signed_volumes = [bar.signed_trade_volume for bar in bars]

        for i in range(20, len(bars) - 1):
            close_i = closes[i]
            close_prev_1 = closes[i - 1]
            close_prev_3 = closes[i - 3]
            close_prev_6 = closes[i - 6]

            ret_1 = _safe_ratio(close_i, close_prev_1) - 1.0
            ret_3 = _safe_ratio(close_i, close_prev_3) - 1.0
            ret_6 = _safe_ratio(close_i, close_prev_6) - 1.0

            high_window = max(highs[i - 5 : i + 1])
            low_window = min(lows[i - 5 : i + 1])
            range_6 = (high_window - low_window) / max(1e-9, close_i)

            volume_window = [math.log1p(max(0.0, value)) for value in trade_volumes[i - 19 : i + 1]]
            volume_center = volume_window[-1]
            volume_mean = float(np_mod.mean(volume_window))
            volume_std = float(np_mod.std(volume_window))
            volume_zscore = (volume_center - volume_mean) / max(1e-9, volume_std)

            spread_ma3 = float(np_mod.mean(spreads[i - 2 : i + 1]))
            imbalance_ma3 = float(np_mod.mean(imbalances[i - 2 : i + 1]))

            trade_intensity = trade_counts[i] / max(1.0, float(bars[i].timeframe_minutes))

            signed_window = signed_volumes[i - 5 : i + 1]
            signed_abs_sum = sum(abs(value) for value in signed_window)
            signed_sum = sum(signed_window)
            signed_volume_ratio_6 = signed_sum / max(1e-9, signed_abs_sum)

            depth_ratio = (bars[i].bid_depth_mean + 1.0) / (bars[i].ask_depth_mean + 1.0)

            close_position = (close_i - lows[i]) / max(1e-9, highs[i] - lows[i])

            dt = datetime.fromtimestamp(bars[i].ts_ms / 1000.0, tz=timezone.utc)
            minute_of_day = dt.hour * 60 + dt.minute
            angle = (2.0 * math.pi * minute_of_day) / 1440.0
            hour_sin = math.sin(angle)
            hour_cos = math.cos(angle)

            values = [
                ret_1,
                ret_3,
                ret_6,
                range_6,
                volume_zscore,
                spread_ma3,
                imbalance_ma3,
                trade_intensity,
                signed_volume_ratio_6,
                depth_ratio,
                close_position,
                hour_sin,
                hour_cos,
            ]
            values.extend(1.0 if asset_name == asset else 0.0 for asset_name in assets)

            target = 1.0 if closes[i + 1] > close_i else 0.0
            rows.append((bars[i].ts_ms, values, target, asset))
            row_counts_by_asset[asset] += 1

    rows.sort(key=lambda item: item[0])

    if not rows:
        return (
            np_mod.zeros((0, len(feature_names)), dtype=np_mod.float64),
            np_mod.zeros((0,), dtype=np_mod.float64),
            feature_names,
            row_counts_by_asset,
            [],
        )

    timestamps = [row[0] for row in rows]
    x = np_mod.asarray([row[1] for row in rows], dtype=np_mod.float64)
    y = np_mod.asarray([row[2] for row in rows], dtype=np_mod.float64)
    return x, y, feature_names, row_counts_by_asset, timestamps


def _safe_ratio(numerator: float, denominator: float) -> float:
    if abs(denominator) < 1e-9:
        return 1.0
    return numerator / denominator


def train_logistic_model(
    *,
    x_train: np.ndarray,
    y_train: np.ndarray,
    epochs: int,
    learning_rate: float,
    l2_penalty: float,
) -> tuple[np.ndarray, float, float]:
    np_mod = _require_numpy()
    n_rows, n_features = x_train.shape
    weights = np_mod.zeros(n_features, dtype=np_mod.float64)
    bias = 0.0

    positive = float(np_mod.sum(y_train))
    negative = float(n_rows - positive)
    if positive <= 0.0 or negative <= 0.0:
        raise RuntimeError("Training labels contain only one class")

    pos_weight = n_rows / (2.0 * positive)
    neg_weight = n_rows / (2.0 * negative)
    sample_weights = np_mod.where(y_train > 0.5, pos_weight, neg_weight)

    final_loss = float("nan")
    for _ in range(max(1, epochs)):
        logits = np_mod.clip(x_train @ weights + bias, -50.0, 50.0)
        probs = 1.0 / (1.0 + np_mod.exp(-logits))

        residual = (probs - y_train) * sample_weights
        grad_w = (x_train.T @ residual) / n_rows + l2_penalty * weights
        grad_b = float(np_mod.mean(residual))

        weights -= learning_rate * grad_w
        bias -= learning_rate * grad_b

        probs_safe = np_mod.clip(probs, 1e-9, 1.0 - 1e-9)
        ce = -(y_train * np_mod.log(probs_safe) + (1.0 - y_train) * np_mod.log(1.0 - probs_safe))
        final_loss = float(
            np_mod.average(ce, weights=sample_weights) + 0.5 * l2_penalty * float(np_mod.dot(weights, weights))
        )

    return weights, bias, final_loss


def predict_proba(x: np.ndarray, weights: np.ndarray, bias: float) -> np.ndarray:
    np_mod = _require_numpy()
    logits = np_mod.clip(x @ weights + bias, -50.0, 50.0)
    return 1.0 / (1.0 + np_mod.exp(-logits))


def evaluate_binary(y_true: np.ndarray, y_prob: np.ndarray, *, threshold: float = 0.5) -> dict[str, float | None]:
    np_mod = _require_numpy()
    if y_true.size == 0:
        return {
            "rows": 0,
            "accuracy": None,
            "precision": None,
            "recall": None,
            "f1": None,
            "log_loss": None,
            "brier": None,
            "auc": None,
            "positive_rate": None,
        }

    y_pred = (y_prob >= threshold).astype(np_mod.float64)
    tp = float(np_mod.sum((y_true == 1.0) & (y_pred == 1.0)))
    tn = float(np_mod.sum((y_true == 0.0) & (y_pred == 0.0)))
    fp = float(np_mod.sum((y_true == 0.0) & (y_pred == 1.0)))
    fn = float(np_mod.sum((y_true == 1.0) & (y_pred == 0.0)))

    precision = tp / (tp + fp) if (tp + fp) > 0 else None
    recall = tp / (tp + fn) if (tp + fn) > 0 else None
    if precision is not None and recall is not None and (precision + recall) > 0:
        f1 = 2.0 * precision * recall / (precision + recall)
    else:
        f1 = None

    probs_safe = np_mod.clip(y_prob, 1e-9, 1.0 - 1.0e-9)
    log_loss = float(-np_mod.mean(y_true * np_mod.log(probs_safe) + (1.0 - y_true) * np_mod.log(1.0 - probs_safe)))
    brier = float(np_mod.mean((y_prob - y_true) ** 2))

    auc = _compute_auc(y_true, y_prob)

    return {
        "rows": int(y_true.size),
        "accuracy": float((tp + tn) / y_true.size),
        "precision": float(precision) if precision is not None else None,
        "recall": float(recall) if recall is not None else None,
        "f1": float(f1) if f1 is not None else None,
        "log_loss": log_loss,
        "brier": brier,
        "auc": auc,
        "positive_rate": float(np_mod.mean(y_true)),
    }


def _compute_auc(y_true: np.ndarray, y_prob: np.ndarray) -> float | None:
    np_mod = _require_numpy()
    positives = int(np_mod.sum(y_true == 1.0))
    negatives = int(np_mod.sum(y_true == 0.0))
    if positives == 0 or negatives == 0:
        return None

    order = np_mod.argsort(y_prob)
    ranks = np_mod.empty_like(order, dtype=np_mod.float64)
    ranks[order] = np_mod.arange(1, len(order) + 1, dtype=np_mod.float64)
    sum_ranks_positive = float(np_mod.sum(ranks[y_true == 1.0]))
    auc = (sum_ranks_positive - positives * (positives + 1) / 2.0) / (positives * negatives)
    return float(auc)


def train_timeframe(
    *,
    timeframe_minutes: int,
    bars_by_asset: dict[str, list[TimeframeBar]],
    assets: list[str],
    train_ratio: float,
    min_train_rows: int,
    epochs: int,
    learning_rate: float,
    l2_penalty: float,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    np_mod = _require_numpy()
    x, y, feature_names, row_counts_by_asset, timestamps = build_feature_matrix(
        timeframe_bars_by_asset=bars_by_asset,
        assets=assets,
    )

    total_rows = int(x.shape[0])
    summary = {
        "timeframe_minutes": timeframe_minutes,
        "total_rows": total_rows,
        "rows_by_asset": row_counts_by_asset,
        "status": "skipped",
        "reason": "not_enough_rows",
    }

    if total_rows < min_train_rows:
        summary["reason"] = f"not_enough_rows(min={min_train_rows})"
        return None, summary

    split_idx = int(total_rows * train_ratio)
    split_idx = max(1, min(total_rows - 1, split_idx))

    x_train = x[:split_idx]
    y_train = y[:split_idx]
    x_test = x[split_idx:]
    y_test = y[split_idx:]

    if int(np_mod.sum(y_train)) == 0 or int(np_mod.sum(y_train)) == len(y_train):
        summary["reason"] = "train_split_single_class"
        return None, summary

    mean = np_mod.mean(x_train, axis=0)
    std = np_mod.std(x_train, axis=0)
    std = np_mod.where(std < 1e-9, 1.0, std)

    x_train_scaled = (x_train - mean) / std
    x_test_scaled = (x_test - mean) / std

    weights, bias, train_objective = train_logistic_model(
        x_train=x_train_scaled,
        y_train=y_train,
        epochs=epochs,
        learning_rate=learning_rate,
        l2_penalty=l2_penalty,
    )

    train_prob = predict_proba(x_train_scaled, weights, bias)
    test_prob = predict_proba(x_test_scaled, weights, bias)

    train_metrics = evaluate_binary(y_train, train_prob)
    test_metrics = evaluate_binary(y_test, test_prob)

    model_payload = {
        "timeframe_minutes": timeframe_minutes,
        "feature_names": feature_names,
        "scale_mean": mean.tolist(),
        "scale_std": std.tolist(),
        "weights": weights.tolist(),
        "bias": float(bias),
        "train_metrics": train_metrics,
        "test_metrics": test_metrics,
        "train_rows": int(y_train.size),
        "test_rows": int(y_test.size),
        "train_start": _timestamp_to_iso(timestamps[0]),
        "train_end": _timestamp_to_iso(timestamps[split_idx - 1]),
        "test_start": _timestamp_to_iso(timestamps[split_idx]),
        "test_end": _timestamp_to_iso(timestamps[-1]),
        "train_objective": float(train_objective),
    }

    summary.update(
        {
            "status": "trained",
            "reason": "ok",
            "train_rows": int(y_train.size),
            "test_rows": int(y_test.size),
            "train_accuracy": train_metrics.get("accuracy"),
            "test_accuracy": test_metrics.get("accuracy"),
            "test_auc": test_metrics.get("auc"),
        }
    )
    return model_payload, summary


def timeframe_label(minutes: int) -> str:
    if minutes == 60:
        return "1h"
    if minutes == 240:
        return "4h"
    return f"{minutes}m"


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Download Polymarket crypto NDJSON from Hugging Face, process into 5m/15m/1h/4h bars, "
            "and train standalone ML model bundle for offline strategy research."
        )
    )
    parser.add_argument("--mode", choices=["run", "download", "build"], default="run")
    parser.add_argument("--dataset-ids", default=",".join(DEFAULT_DATASET_IDS))
    parser.add_argument("--assets", default=",".join(SUPPORTED_ASSETS))
    parser.add_argument("--timeframes", default=",".join(str(item) for item in DEFAULT_TIMEFRAMES))
    parser.add_argument("--raw-dir", default="data/polymarket_crypto/raw")
    parser.add_argument("--output-dir", default="output/ml/polymarket_crypto")
    parser.add_argument("--max-files-per-asset", type=int, default=0)
    parser.add_argument("--max-files-total", type=int, default=0)
    parser.add_argument("--download-workers", type=int, default=8)
    parser.add_argument("--request-timeout-seconds", type=int, default=45)
    parser.add_argument("--retry-attempts", type=int, default=4)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--train-ratio", type=float, default=0.8)
    parser.add_argument("--min-train-rows", type=int, default=DEFAULT_MIN_TRAIN_ROWS)
    parser.add_argument("--epochs", type=int, default=DEFAULT_EPOCHS)
    parser.add_argument("--learning-rate", type=float, default=DEFAULT_LEARNING_RATE)
    parser.add_argument("--l2-penalty", type=float, default=DEFAULT_L2)
    parser.add_argument("--seed", type=int, default=13)
    parser.add_argument("--quiet", action="store_true")
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()

    dataset_ids = _parse_csv_str_list(args.dataset_ids, lowercase=False)
    assets = _parse_csv_str_list(args.assets, lowercase=True)
    timeframes = sorted(set(_parse_csv_int_list(args.timeframes)))

    if not dataset_ids:
        raise SystemExit("--dataset-ids cannot be empty")
    if not assets:
        raise SystemExit("--assets cannot be empty")
    unsupported = [asset for asset in assets if asset not in SUPPORTED_ASSETS]
    if unsupported:
        raise SystemExit(f"Unsupported asset(s): {','.join(unsupported)}. Supported: {','.join(SUPPORTED_ASSETS)}")
    if any(item <= 0 for item in timeframes):
        raise SystemExit("--timeframes must contain positive integers")

    train_ratio = float(args.train_ratio)
    if not (0.05 <= train_ratio <= 0.95):
        raise SystemExit("--train-ratio must be between 0.05 and 0.95")

    random.seed(args.seed)
    if args.mode in {"run", "build"}:
        np_mod = _require_numpy()
        np_mod.random.seed(args.seed)

    raw_dir = Path(args.raw_dir)
    output_dir = Path(args.output_dir)
    processed_dir = output_dir / "processed"

    run_summary: dict[str, Any] = {
        "created_at": _utc_now_iso(),
        "mode": args.mode,
        "dataset_ids": dataset_ids,
        "assets": assets,
        "timeframes": timeframes,
        "raw_dir": str(raw_dir),
        "output_dir": str(output_dir),
    }

    if args.mode in {"run", "download"}:
        if not args.quiet:
            print("discovering remote files...")
        remote_files = discover_remote_files(
            dataset_ids=dataset_ids,
            assets=assets,
            max_files_per_asset=max(0, args.max_files_per_asset),
            max_files_total=max(0, args.max_files_total),
            timeout_seconds=max(5, args.request_timeout_seconds),
            retry_attempts=max(1, args.retry_attempts),
        )
        if not remote_files:
            raise SystemExit("No remote NDJSON files matched the requested filters")

        download_summary = download_files(
            files=remote_files,
            raw_dir=raw_dir,
            workers=max(1, args.download_workers),
            timeout_seconds=max(5, args.request_timeout_seconds),
            retry_attempts=max(1, args.retry_attempts),
            overwrite=bool(args.overwrite),
            quiet=bool(args.quiet),
        )
        run_summary["download"] = download_summary

        manifest_path = output_dir / "download_manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with manifest_path.open("w", encoding="utf-8") as handle:
            json.dump(
                {
                    "created_at": _utc_now_iso(),
                    "dataset_ids": dataset_ids,
                    "assets": assets,
                    "files": [
                        {
                            "dataset_id": row.dataset_id,
                            "filename": row.filename,
                            "asset": row.asset,
                            "market_id": row.market_id,
                            "start_ts_ms": row.start_ts_ms,
                            "start_time_utc": _timestamp_to_iso(row.start_ts_ms),
                        }
                        for row in remote_files
                    ],
                },
                handle,
                indent=2,
                sort_keys=True,
            )

        if args.mode == "download":
            summary_path = output_dir / "run_summary.json"
            with summary_path.open("w", encoding="utf-8") as handle:
                json.dump(run_summary, handle, indent=2, sort_keys=True)
            if not args.quiet:
                print(f"wrote summary: {summary_path}")
            return

    if not args.quiet:
        print("loading local NDJSON files...")

    local_files = select_local_files(
        raw_dir=raw_dir,
        assets=assets,
        max_files_per_asset=max(0, args.max_files_per_asset),
        max_files_total=max(0, args.max_files_total),
        dataset_ids=dataset_ids,
    )
    if not local_files:
        raise SystemExit("No local NDJSON files found. Run with --mode run or --mode download first.")

    minute_bars, process_summary = process_local_ndjson(local_files=local_files, assets=assets, quiet=bool(args.quiet))
    run_summary["process"] = process_summary
    run_summary["local_file_count"] = len(local_files)

    minute_csv_path = processed_dir / "minute_bars.csv.gz"
    write_minute_csv(minute_csv_path, minute_bars)

    timeframe_bars: dict[int, dict[str, list[TimeframeBar]]] = {}
    timeframe_outputs: dict[str, str] = {}
    for timeframe in timeframes:
        by_asset: dict[str, list[TimeframeBar]] = {}
        for asset in assets:
            by_asset[asset] = aggregate_timeframe(minute_bars.get(asset, []), timeframe)
        timeframe_bars[timeframe] = by_asset
        tf_csv_path = processed_dir / f"bars_{timeframe_label(timeframe)}.csv.gz"
        write_timeframe_csv(tf_csv_path, by_asset)
        timeframe_outputs[str(timeframe)] = str(tf_csv_path)

    run_summary["processed_outputs"] = {
        "minute_csv": str(minute_csv_path),
        "timeframe_csv": timeframe_outputs,
    }

    model_bundle: dict[str, Any] = {
        "artifact_version": "1.0",
        "created_at": _utc_now_iso(),
        "datasets": dataset_ids,
        "assets": assets,
        "timeframes": timeframes,
        "models": {},
    }

    train_summaries: list[dict[str, Any]] = []
    for timeframe in timeframes:
        model_payload, train_summary = train_timeframe(
            timeframe_minutes=timeframe,
            bars_by_asset=timeframe_bars[timeframe],
            assets=assets,
            train_ratio=train_ratio,
            min_train_rows=max(2, args.min_train_rows),
            epochs=max(1, args.epochs),
            learning_rate=max(1e-6, args.learning_rate),
            l2_penalty=max(0.0, args.l2_penalty),
        )
        train_summaries.append(train_summary)
        if model_payload is not None:
            model_bundle["models"][timeframe_label(timeframe)] = model_payload

    run_summary["train"] = train_summaries

    bundle_path = output_dir / "polymarket_crypto_ml_bundle.json"
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    with bundle_path.open("w", encoding="utf-8") as handle:
        json.dump(model_bundle, handle, indent=2, sort_keys=True)

    run_summary["model_bundle_path"] = str(bundle_path)
    run_summary["trained_model_count"] = len(model_bundle["models"])

    summary_path = output_dir / "run_summary.json"
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(run_summary, handle, indent=2, sort_keys=True)

    if not args.quiet:
        print(f"wrote minute bars: {minute_csv_path}")
        for timeframe in timeframes:
            print(f"wrote {timeframe_label(timeframe)} bars: {timeframe_outputs[str(timeframe)]}")
        print(f"wrote model bundle: {bundle_path}")
        print(f"wrote run summary: {summary_path}")


if __name__ == "__main__":
    main()
