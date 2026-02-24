from __future__ import annotations

import json
import sys
import time
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services.chainlink_feed import BINANCE_TOPIC, CHAINLINK_TOPIC, ChainlinkFeed


def test_handle_message_parses_snapshot_rows_with_parent_symbol():
    feed = ChainlinkFeed()
    # Use timestamps close to "now" so history entries survive the 25-minute
    # pruning window inside _handle_message.
    now_s = int(time.time())
    ts0 = now_s - 10
    ts1 = now_s - 9
    msg = {
        "topic": CHAINLINK_TOPIC,
        "payload": {
            "symbol": "btc/usd",
            "data": [
                {"value": 67500.10, "timestamp": ts0},
                {"value": 67501.25, "timestamp": ts1},
            ],
        },
    }

    feed._handle_message(json.dumps(msg))

    latest = feed.get_price("BTC")
    assert latest is not None
    assert latest.price == 67501.25
    assert latest.updated_at_ms == ts1 * 1000
    assert latest.source == "chainlink"

    history = list(feed._history.get("BTC") or [])
    assert history == [(ts0 * 1000, 67500.10), (ts1 * 1000, 67501.25)]


def test_handle_message_parses_payload_list_and_respects_source_priority():
    feed = ChainlinkFeed()
    # Use timestamps close to "now" so history entries survive the 25-minute
    # pruning window inside _handle_message.
    now_s = int(time.time())
    ts_binance = now_s - 10
    ts_chainlink = now_s - 9

    binance_msg = {
        "topic": BINANCE_TOPIC,
        "payload": [
            {
                "symbol": "ethusdt",
                "price": "1940.50",
                "timestamp": ts_binance,
            }
        ],
    }
    chainlink_msg = {
        "topic": CHAINLINK_TOPIC,
        "payload": {
            "symbol": "eth/usd",
            "value": 1939.80,
            "timestamp": ts_chainlink,
        },
    }

    feed._handle_message(json.dumps(binance_msg))
    first = feed.get_price("ETH")
    assert first is not None
    assert first.price == 1940.5
    assert first.source == "binance"
    assert list(feed._history.get("ETH") or []) == []

    feed._handle_message(json.dumps(chainlink_msg))
    latest = feed.get_price("ETH")
    assert latest is not None
    assert latest.price == 1939.8
    assert latest.source == "chainlink"

    by_source = feed.get_prices_by_source("ETH")
    assert set(by_source.keys()) == {"binance", "chainlink"}
    assert by_source["binance"].price == 1940.5
    assert by_source["chainlink"].price == 1939.8
    assert list(feed._history.get("ETH") or []) == [(ts_chainlink * 1000, 1939.8)]
