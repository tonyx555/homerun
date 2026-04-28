"""Measure Polymarket CLOB orderbook WebSocket latency on a live 5m BTC Up/Down market.

Subscribes to YES + NO token IDs of an active 5m BTC market via the same WS
endpoint and protocol that production uses (ws_feeds.py). Records every
inbound message with (event_type, asset_id, recv_ts_ms, server_ts_ms when
present) and reports interarrival + latency stats.

Run:
  backend/venv/Scripts/python.exe scripts/measure_clob_orderbook_latency.py
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field

import httpx
import websockets


CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_EVENTS = "https://gamma-api.polymarket.com/events"

DURATION_SECONDS = 300


@dataclass
class Event:
    event_type: str
    asset_id: str
    recv_ts_ms: int
    server_ts_ms: int  # 0 when message has no server timestamp
    raw_size: int
    sample: dict = field(default_factory=dict)


events: list[Event] = []


def percentile(xs, p):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100.0
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    if f == c:
        return xs[f]
    return xs[f] + (xs[c] - xs[f]) * (k - f)


async def discover_markets() -> list[tuple[str, list[str]]]:
    """Return [(title, [yes, no]), ...] for all active short-window crypto markets."""
    out: list[tuple[str, list[str]]] = []
    async with httpx.AsyncClient(timeout=15.0) as c:
        r = await c.get(GAMMA_EVENTS, params={
            "closed": "false",
            "active": "true",
            "limit": 100,
            "order": "startDate",
            "ascending": "false",
            "tag": "Crypto",
        })
        r.raise_for_status()
        data = r.json()
    for e in data:
        title = (e.get("title") or "")
        slug = (e.get("slug") or "").lower()
        is_short = any(k in slug for k in ["5m", "15m", "30m", "1h", "hourly"]) or any(
            k in title.lower() for k in ["5 min", "15 min", "30 min", "hour"]
        )
        if not is_short:
            continue
        for m in e.get("markets") or []:
            tids_raw = m.get("clobTokenIds")
            if not tids_raw:
                continue
            try:
                tids = json.loads(tids_raw) if isinstance(tids_raw, str) else tids_raw
            except json.JSONDecodeError:
                continue
            if isinstance(tids, list) and len(tids) >= 2:
                out.append((title, [str(tids[0]), str(tids[1])]))
    if not out:
        raise RuntimeError("No active short-window crypto markets found via Gamma")
    return out


def handle_message(raw: str | bytes) -> None:
    recv_ms = int(time.time() * 1000)
    text = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
    if not text.strip() or text.strip().lower() == "pong":
        return
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return
    items = data if isinstance(data, list) else [data]
    for item in items:
        if not isinstance(item, dict):
            continue
        ev_type = str(item.get("event_type") or item.get("type") or "?")
        asset_id = str(item.get("asset_id") or item.get("assetId") or item.get("token_id") or "?")
        ts_raw = item.get("timestamp") or item.get("ts") or item.get("hash_timestamp") or 0
        try:
            server_ms = int(ts_raw)
            if 0 < server_ms < 1e12:
                server_ms *= 1000
        except (TypeError, ValueError):
            server_ms = 0
        events.append(Event(ev_type, asset_id, recv_ms, server_ms, len(text)))


async def ws_loop(token_ids: list[str], stop_at_ms: int) -> None:
    while time.time() * 1000 < stop_at_ms:
        try:
            async with websockets.connect(
                CLOB_WS_URL,
                ping_interval=None,
                ping_timeout=None,
                open_timeout=10,
                close_timeout=5,
                max_size=2**22,
            ) as ws:
                sub = json.dumps({"type": "market", "assets_ids": token_ids})
                await ws.send(sub)
                print(f"[clob] connected, subscribed to {len(token_ids)} token IDs")

                # Heartbeat task — server expects periodic pings to keep alive
                async def hb():
                    while True:
                        await asyncio.sleep(10)
                        try:
                            await ws.send("PING")
                        except Exception:
                            return

                hb_task = asyncio.create_task(hb())
                try:
                    async for raw in ws:
                        if time.time() * 1000 >= stop_at_ms:
                            return
                        handle_message(raw)
                finally:
                    hb_task.cancel()
        except Exception as e:
            print(f"[clob] error: {e}; reconnecting in 0.5s")
            await asyncio.sleep(0.5)


def report() -> None:
    by_type = defaultdict(list)
    by_asset = defaultdict(list)
    for ev in events:
        by_type[ev.event_type].append(ev)
        by_asset[ev.asset_id].append(ev)

    print(f"\n=== CLOB orderbook WS report  (n={len(events)} events) ===")
    if events:
        elapsed_s = (events[-1].recv_ts_ms - events[0].recv_ts_ms) / 1000.0
        print(f"  Elapsed: {elapsed_s:.1f}s  Overall rate: {len(events)/max(1,elapsed_s):.2f}/s")
        print(f"  Mean message size: {sum(e.raw_size for e in events)/len(events):.0f} bytes")
    print()

    print(f"  {'event_type':18s}  {'n':>6s}  {'rate/s':>7s}  {'gap_p50':>7s} {'gap_p95':>7s} {'gap_p99':>7s} {'gap_max':>7s}  {'lat_p50':>7s} {'lat_p95':>7s}")
    for et, evs in sorted(by_type.items()):
        recvs = sorted(e.recv_ts_ms for e in evs)
        gaps = [recvs[i] - recvs[i-1] for i in range(1, len(recvs))]
        elapsed_s = (recvs[-1] - recvs[0]) / 1000.0 if len(recvs) > 1 else 1.0
        rate = len(evs) / max(1.0, elapsed_s)
        lats = [e.recv_ts_ms - e.server_ts_ms for e in evs if e.server_ts_ms > 0]
        gp50 = percentile(gaps, 50) if gaps else float("nan")
        gp95 = percentile(gaps, 95) if gaps else float("nan")
        gp99 = percentile(gaps, 99) if gaps else float("nan")
        gmax = max(gaps) if gaps else 0
        lp50 = percentile(lats, 50) if lats else float("nan")
        lp95 = percentile(lats, 95) if lats else float("nan")
        print(
            f"  {et:18s}  {len(evs):6d}  {rate:7.2f}  "
            f"{gp50:7.0f} {gp95:7.0f} {gp99:7.0f} {gmax:7.0f}  "
            f"{lp50:7.0f} {lp95:7.0f}"
        )

    print()
    # Top 10 most active assets (excluding initial book snapshots)
    asset_activity = []
    for aid, evs in by_asset.items():
        non_book = [e for e in evs if e.event_type != "book"]
        if non_book:
            asset_activity.append((aid, len(evs), non_book))
    asset_activity.sort(key=lambda x: -len(x[2]))
    print(f"Top {min(10, len(asset_activity))} most-active assets (by non-book events):")
    for aid, total, non_book in asset_activity[:10]:
        ets = defaultdict(int)
        for e in non_book:
            ets[e.event_type] += 1
        breakdown = ", ".join(f"{k}={v}" for k, v in sorted(ets.items()))
        print(f"  {aid[:14]}...  non_book={len(non_book)}  ({breakdown})")
    print(f"\n  ({len(asset_activity)} of {len(by_asset)} assets had any non-book activity during the window)")

    # First few raw events as samples
    print("\nFirst 3 events (truncated):")
    for ev in events[:3]:
        print(f"  recv={ev.recv_ts_ms} server={ev.server_ts_ms} type={ev.event_type} asset={ev.asset_id[:14]}... size={ev.raw_size}B")


async def main() -> None:
    print("Discovering active short-window crypto markets...")
    markets = await discover_markets()
    print(f"  Found {len(markets)} markets:")
    for title, _ in markets[:8]:
        print(f"    - {title}")
    if len(markets) > 8:
        print(f"    ... and {len(markets) - 8} more")
    all_tids: list[str] = []
    for _, tids in markets:
        all_tids.extend(tids)
    # Dedupe while preserving order
    seen: set[str] = set()
    unique_tids = []
    for t in all_tids:
        if t not in seen:
            seen.add(t)
            unique_tids.append(t)
    print(f"  Total unique token IDs: {len(unique_tids)}")
    print()

    stop_at_ms = int(time.time() * 1000) + DURATION_SECONDS * 1000
    print(f"Measuring CLOB WS for {DURATION_SECONDS}s")
    print()

    task = asyncio.create_task(ws_loop(unique_tids, stop_at_ms))
    try:
        # Periodic intermediate reports
        next_print = time.time() + 30
        while time.time() * 1000 < stop_at_ms:
            await asyncio.sleep(2)
            if time.time() >= next_print:
                print(f"  [+{int(time.time() - (stop_at_ms/1000) + DURATION_SECONDS)}s] events so far: {len(events)}")
                next_print = time.time() + 30
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    report()

    out_path = "scripts/clob_latency_events.jsonl"
    with open(out_path, "w") as f:
        for ev in events:
            f.write(json.dumps({
                "event_type": ev.event_type,
                "asset_id": ev.asset_id,
                "recv_ts_ms": ev.recv_ts_ms,
                "server_ts_ms": ev.server_ts_ms,
                "raw_size": ev.raw_size,
            }) + "\n")
    print(f"\nRaw events dumped to {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
