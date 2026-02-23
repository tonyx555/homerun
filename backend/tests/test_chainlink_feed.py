from __future__ import annotations

import json
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services.chainlink_feed import BINANCE_TOPIC, CHAINLINK_TOPIC, ChainlinkFeed


def test_handle_message_parses_snapshot_rows_with_parent_symbol():
    feed = ChainlinkFeed()
    msg = {
        "topic": CHAINLINK_TOPIC,
        "payload": {
            "symbol": "btc/usd",
            "data": [
                {"value": 67500.10, "timestamp": 1771780000},
                {"value": 67501.25, "timestamp": 1771780001},
            ],
        },
    }

    feed._handle_message(json.dumps(msg))

    latest = feed.get_price("BTC")
    assert latest is not None
    assert latest.price == 67501.25
    assert latest.updated_at_ms == 1771780001000
    assert latest.source == "chainlink"

    history = list(feed._history.get("BTC") or [])
    assert history == [(1771780000000, 67500.10), (1771780001000, 67501.25)]


def test_handle_message_parses_payload_list_and_respects_source_priority():
    feed = ChainlinkFeed()

    binance_msg = {
        "topic": BINANCE_TOPIC,
        "payload": [
            {
                "symbol": "ethusdt",
                "price": "1940.50",
                "timestamp": 1771780100,
            }
        ],
    }
    chainlink_msg = {
        "topic": CHAINLINK_TOPIC,
        "payload": {
            "symbol": "eth/usd",
            "value": 1939.80,
            "timestamp": 1771780101,
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
    assert list(feed._history.get("ETH") or []) == [(1771780101000, 1939.8)]
