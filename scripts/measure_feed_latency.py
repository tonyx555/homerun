"""One-shot harness: measure realtime feed latencies on homerun's data sources.

Independent of any strategy. Connects to:
  - Polymarket RTDS WS: wss://ws-live-data.polymarket.com
    topics: crypto_prices_chainlink (Chainlink resolution oracle),
            crypto_prices (Binance via Polymarket relay)
  - Binance combined stream WS: btcusdt/ethusdt/solusdt/xrpusdt @ bookTicker

Records every received tick with (source, asset, price, msg_ts_ms, recv_ts_ms)
and prints aggregate stats every 30s. Runs for DURATION_SECONDS, then dumps
raw ticks to scripts/feed_latency_ticks.jsonl.

Run:
  backend/venv/Scripts/python.exe scripts/measure_feed_latency.py
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import defaultdict
from dataclasses import dataclass

import websockets


RTDS_URL = "wss://ws-live-data.polymarket.com"
BINANCE_URL = (
    "wss://stream.binance.com:9443/stream?"
    "streams=btcusdt@bookTicker/ethusdt@bookTicker/"
    "solusdt@bookTicker/xrpusdt@bookTicker"
)

CHAINLINK_TOPIC = "crypto_prices_chainlink"
BINANCE_TOPIC = "crypto_prices"

DURATION_SECONDS = 300

_SYMBOL_MAP = {
    "btc/usd": "BTC", "eth/usd": "ETH", "sol/usd": "SOL", "xrp/usd": "XRP",
    "btcusdt": "BTC", "ethusdt": "ETH", "solusdt": "SOL", "xrpusdt": "XRP",
}


@dataclass
class Tick:
    source: str
    asset: str
    price: float
    msg_ts_ms: int
    recv_ts_ms: int


ticks: list[Tick] = []


def record(t: Tick) -> None:
    ticks.append(t)


def percentile(xs, p):
    if not xs:
        return 0
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100.0
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    if f == c:
        return xs[f]
    return xs[f] + (xs[c] - xs[f]) * (k - f)


# ---------------------------------------------------------------------------
# RTDS (Polymarket) — Chainlink + Binance topics
# ---------------------------------------------------------------------------

async def rtds_loop(stop_at_ms: int) -> None:
    while time.time() * 1000 < stop_at_ms:
        try:
            async with websockets.connect(
                RTDS_URL, ping_interval=20, ping_timeout=10, close_timeout=5
            ) as ws:
                sub = json.dumps({
                    "action": "subscribe",
                    "subscriptions": [
                        {"topic": CHAINLINK_TOPIC, "type": "update", "filters": ""},
                        {"topic": BINANCE_TOPIC, "type": "update"},
                    ],
                })
                await ws.send(sub)
                print(f"[rtds] connected, subscribed to {CHAINLINK_TOPIC} + {BINANCE_TOPIC}")
                async for raw in ws:
                    if time.time() * 1000 >= stop_at_ms:
                        return
                    handle_rtds(raw)
        except Exception as e:
            print(f"[rtds] error: {e}; reconnecting in 0.5s")
            await asyncio.sleep(0.5)


def handle_rtds(raw) -> None:
    recv_ms = int(time.time() * 1000)
    try:
        data = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
    except Exception:
        return
    topic = data.get("topic")
    if topic not in (CHAINLINK_TOPIC, BINANCE_TOPIC):
        return
    payload = data.get("payload")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            return

    rows: list[dict] = []
    if isinstance(payload, dict):
        dpts = payload.get("data")
        if isinstance(dpts, list):
            base_sym = payload.get("symbol") or payload.get("pair") or payload.get("ticker")
            for r in dpts:
                if not isinstance(r, dict):
                    continue
                if base_sym and not (r.get("symbol") or r.get("pair") or r.get("ticker")):
                    nr = dict(r)
                    nr["symbol"] = base_sym
                    rows.append(nr)
                else:
                    rows.append(r)
        else:
            rows.append(payload)
    elif isinstance(payload, list):
        rows.extend([r for r in payload if isinstance(r, dict)])

    src = "chainlink_rtds" if topic == CHAINLINK_TOPIC else "binance_rtds"
    for row in rows:
        sym = str(row.get("symbol") or row.get("pair") or row.get("ticker") or "").lower()
        asset = _SYMBOL_MAP.get(sym)
        if not asset:
            for k, v in _SYMBOL_MAP.items():
                if k in sym:
                    asset = v
                    break
        if not asset:
            continue
        try:
            price = float(row.get("value") or row.get("price") or row.get("current") or 0)
        except (TypeError, ValueError):
            continue
        if price <= 0:
            continue
        ts_val = row.get("timestamp") or row.get("updatedAt")
        msg_ms = None
        if ts_val is not None:
            try:
                ts_f = float(ts_val)
                msg_ms = int(ts_f * 1000) if ts_f < 1e12 else int(ts_f)
            except (TypeError, ValueError):
                pass
        if msg_ms is None:
            msg_ms = recv_ms
        record(Tick(src, asset, price, msg_ms, recv_ms))


# ---------------------------------------------------------------------------
# Binance direct WS (bookTicker — no exchange-side timestamp on this stream)
# ---------------------------------------------------------------------------

async def binance_loop(stop_at_ms: int) -> None:
    while time.time() * 1000 < stop_at_ms:
        try:
            async with websockets.connect(
                BINANCE_URL, ping_interval=20, ping_timeout=10, close_timeout=5
            ) as ws:
                print("[binance] connected to combined bookTicker stream")
                async for raw in ws:
                    if time.time() * 1000 >= stop_at_ms:
                        return
                    handle_binance(raw)
        except Exception as e:
            print(f"[binance] error: {e}; reconnecting in 0.5s")
            await asyncio.sleep(0.5)


def handle_binance(raw) -> None:
    recv_ms = int(time.time() * 1000)
    try:
        data = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
    except Exception:
        return
    ticker = data.get("data") if isinstance(data, dict) and "data" in data else data
    if not isinstance(ticker, dict):
        return
    sym = str(ticker.get("s") or "").lower()
    asset = _SYMBOL_MAP.get(sym)
    if not asset:
        return
    try:
        bid = float(ticker["b"])
        ask = float(ticker["a"])
    except (KeyError, TypeError, ValueError):
        return
    if bid <= 0 or ask <= 0:
        return
    mid = (bid + ask) / 2.0
    record(Tick("binance_direct", asset, mid, recv_ms, recv_ms))


# ---------------------------------------------------------------------------
# Reporter
# ---------------------------------------------------------------------------

def report_stats(label: str = "") -> None:
    by = defaultdict(list)
    for t in ticks:
        by[(t.source, t.asset)].append(t)
    header = f"\n--- {time.strftime('%H:%M:%S')} {label} | total ticks: {len(ticks)} ---"
    print(header)
    print(f"  {'source':18s} {'asset':5s}  {'n':>6s}  {'rate/s':>7s}  {'lat_p50':>7s} {'lat_p95':>7s} {'lat_p99':>7s} {'lat_max':>7s}  {'gap_p50':>7s} {'gap_p95':>7s}")
    for (src, asset), ts in sorted(by.items()):
        if not ts:
            continue
        if src.endswith("_rtds"):
            latencies = [t.recv_ts_ms - t.msg_ts_ms for t in ts if t.msg_ts_ms != t.recv_ts_ms]
        else:
            latencies = []
        recvs = sorted(t.recv_ts_ms for t in ts)
        gaps = [recvs[i] - recvs[i-1] for i in range(1, len(recvs))]
        elapsed_s = (recvs[-1] - recvs[0]) / 1000.0 if len(recvs) > 1 else 1.0
        rate = len(ts) / max(1.0, elapsed_s)
        lat_p50 = percentile(latencies, 50) if latencies else float("nan")
        lat_p95 = percentile(latencies, 95) if latencies else float("nan")
        lat_p99 = percentile(latencies, 99) if latencies else float("nan")
        lat_max = max(latencies) if latencies else float("nan")
        gap_p50 = percentile(gaps, 50) if gaps else float("nan")
        gap_p95 = percentile(gaps, 95) if gaps else float("nan")
        print(
            f"  {src:18s} {asset:5s}  {len(ts):6d}  {rate:7.2f}  "
            f"{lat_p50:7.0f} {lat_p95:7.0f} {lat_p99:7.0f} {lat_max:7.0f}  "
            f"{gap_p50:7.0f} {gap_p95:7.0f}"
        )


async def reporter(stop_at_ms: int) -> None:
    next_at = time.time() + 30
    while time.time() * 1000 < stop_at_ms:
        await asyncio.sleep(1)
        if time.time() >= next_at:
            report_stats(label=f"+{int(time.time() - start_wall):d}s")
            next_at = time.time() + 30


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

start_wall = time.time()


async def main() -> None:
    global start_wall
    start_wall = time.time()
    stop_at_ms = int(start_wall * 1000) + DURATION_SECONDS * 1000
    print(f"Measuring feed latency for {DURATION_SECONDS}s")
    print(f"  RTDS:    {RTDS_URL}")
    print(f"  Binance: {BINANCE_URL[:80]}...")
    print()

    tasks = [
        asyncio.create_task(rtds_loop(stop_at_ms)),
        asyncio.create_task(binance_loop(stop_at_ms)),
        asyncio.create_task(reporter(stop_at_ms)),
    ]
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    print("\n=== FINAL REPORT ===")
    report_stats(label="FINAL")

    out_path = "scripts/feed_latency_ticks.jsonl"
    with open(out_path, "w") as f:
        for t in ticks:
            f.write(json.dumps({
                "source": t.source,
                "asset": t.asset,
                "price": t.price,
                "msg_ts_ms": t.msg_ts_ms,
                "recv_ts_ms": t.recv_ts_ms,
            }) + "\n")
    print(f"\nRaw ticks dumped to {out_path} ({len(ticks)} rows)")


if __name__ == "__main__":
    asyncio.run(main())
