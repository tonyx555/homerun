import asyncio
import json
import sys
from pathlib import Path

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services.wallet_rtds_feed import WalletRTDSFeed
from services.wallet_ws_monitor import WalletTradeEvent


@pytest.mark.asyncio
async def test_rtds_filters_untracked_wallets():
    tracked = {"0xtracked"}
    feed = WalletRTDSFeed(get_tracked_wallets=lambda: tracked)
    received: list[WalletTradeEvent] = []

    async def cb(event: WalletTradeEvent) -> None:
        received.append(event)

    feed.add_callback(cb)

    # Untracked wallet — no emit.
    untracked_msg = json.dumps({
        "topic": "activity",
        "type": "trades",
        "payload": {
            "proxyWallet": "0xstranger",
            "asset": "tok",
            "side": "BUY",
            "price": 0.42,
            "size": 100,
            "timestamp": 1_730_000_000,
            "transactionHash": "0xabc",
        },
    })
    await feed._handle_message(untracked_msg)
    assert received == []
    assert feed.stats["events_filtered_no_match"] == 1

    # Tracked wallet — emit a preliminary event.
    tracked_msg = json.dumps({
        "topic": "activity",
        "type": "trades",
        "payload": {
            "proxyWallet": "0xTracked",  # mixed-case — should normalize
            "asset": "tok",
            "side": "SELL",
            "price": 0.55,
            "size": 100,
            "timestamp": 1_730_000_001,
            "transactionHash": "0xfeed",
        },
    })
    await feed._handle_message(tracked_msg)
    assert len(received) == 1
    event = received[0]
    assert event.wallet_address == "0xtracked"
    assert event.token_id == "tok"
    assert event.side == "SELL"
    assert event.price == pytest.approx(0.55)
    assert event.confirmed is False
    assert event.source == "rtds"
    assert event.tx_hash == "0xfeed"
    assert feed.stats["events_emitted"] == 1


@pytest.mark.asyncio
async def test_rtds_dedupes_replays_by_deterministic_id():
    feed = WalletRTDSFeed(get_tracked_wallets=lambda: {"0xw"})
    received: list[WalletTradeEvent] = []

    async def cb(event: WalletTradeEvent) -> None:
        received.append(event)

    feed.add_callback(cb)

    msg = json.dumps({
        "topic": "activity",
        "type": "trades",
        "payload": {
            "proxyWallet": "0xw",
            "asset": "tok",
            "side": "BUY",
            "price": 0.42,
            "size": 100,
            "timestamp": 1_730_000_000,
            "transactionHash": "0xabc",
        },
    })

    # First delivery emits.
    await feed._handle_message(msg)
    # Replay (same tx_hash, same fields) is dropped — not a new event.
    await feed._handle_message(msg)
    await feed._handle_message(msg)

    assert len(received) == 1
    assert feed.stats["events_emitted"] == 1
    assert feed.stats["events_filtered_dedup"] == 2


@pytest.mark.asyncio
async def test_rtds_ignores_non_activity_topics():
    feed = WalletRTDSFeed(get_tracked_wallets=lambda: {"0xw"})
    received: list[WalletTradeEvent] = []

    async def cb(event: WalletTradeEvent) -> None:
        received.append(event)

    feed.add_callback(cb)

    # Heartbeats and other topics shouldn't produce events.
    await feed._handle_message("pong")
    await feed._handle_message("")
    await feed._handle_message(json.dumps({"topic": "crypto_prices", "type": "update"}))
    await feed._handle_message(json.dumps({"topic": "activity", "type": "fills"}))
    assert received == []
