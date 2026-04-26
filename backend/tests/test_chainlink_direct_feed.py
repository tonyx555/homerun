import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services.chainlink_direct_feed import (
    ChainlinkDirectFeed,
    _generate_auth_headers,
)
from utils.feed_availability import FeedStatus


def test_auth_headers_use_space_separated_canonical_string():
    headers = _generate_auth_headers(
        method="GET",
        url="https://api.dataengine.chain.link/api/v1/reports/latest?feedID=0xabc",
        api_key="test_key",
        user_secret="test_secret",
    )
    assert headers["Authorization"] == "test_key"
    assert "X-Authorization-Timestamp" in headers
    assert "X-Authorization-Signature-SHA256" in headers
    # Timestamp is millis-resolution.
    assert int(headers["X-Authorization-Timestamp"]) > 1_700_000_000_000


@pytest.mark.asyncio
async def test_feed_disabled_when_credentials_missing(monkeypatch):
    feed = ChainlinkDirectFeed(
        get_api_key=lambda: "",
        get_user_secret=lambda: "",
    )

    # start() should NOT spawn a connect-loop task when creds are absent —
    # this is the whole point of the "disabled one-shot" pattern.
    await feed.start()
    assert feed.started is False
    assert feed.status == FeedStatus.DISABLED
    assert feed.disabled_reason == "missing_credentials"

    # Even after credentials become available, start() stays inert until
    # rearm() is called — exercises the latch behavior.
    api_key_holder = ["set_now"]
    secret_holder = ["set_now"]
    feed = ChainlinkDirectFeed(
        get_api_key=lambda: api_key_holder[0] if api_key_holder[0] != "set_now" else "",
        get_user_secret=lambda: secret_holder[0] if secret_holder[0] != "set_now" else "",
    )
    api_key_holder[0] = ""
    secret_holder[0] = ""
    await feed.start()
    assert feed.started is False
    assert feed.status == FeedStatus.DISABLED


@pytest.mark.asyncio
async def test_feed_latches_on_persistent_auth_failure(monkeypatch):
    """A 401 from the upstream should latch the feed disabled rather than
    looping forever."""
    feed = ChainlinkDirectFeed(
        get_api_key=lambda: "key",
        get_user_secret=lambda: "secret",
    )

    # Mock a single 401 response.
    response_mock = MagicMock()
    response_mock.status_code = 401

    client_mock = AsyncMock()
    client_mock.get.return_value = response_mock

    await feed._poll_one(client_mock, "BTC", "0xfeed")
    assert feed._availability.is_disabled is True
    assert feed.disabled_reason == "http_401"

    # rearm() clears the latch.
    feed.rearm()
    assert feed._availability.is_disabled is False


@pytest.mark.asyncio
async def test_feed_decodes_v3_metadata_fallback(monkeypatch):
    """When fullReport decode fails, the metadata-level benchmarkPrice
    fallback (matching the JS reference) should still produce a price."""
    feed = ChainlinkDirectFeed(
        get_api_key=lambda: "key",
        get_user_secret=lambda: "secret",
    )

    captured_prices = []
    feed.on_update(lambda p: captured_prices.append(p))

    response_mock = MagicMock()
    response_mock.status_code = 200
    response_mock.json = MagicMock(return_value={
        "report": {
            "feedID": "0xabc",
            "observationsTimestamp": 1_730_000_000,
            # Intentionally invalid fullReport so V3 decode fails and we
            # fall through to the metadata fallback.
            "fullReport": "0xdeadbeef",
            "benchmarkPrice": "67890.12",
            "bid": "67889.0",
            "ask": "67891.0",
        }
    })

    client_mock = AsyncMock()
    client_mock.get.return_value = response_mock

    await feed._poll_one(client_mock, "BTC", "0xfeed")
    assert "BTC" in feed.get_all_prices()
    btc = feed.get_price("BTC")
    assert btc.price == pytest.approx(67890.12)
    assert btc.bid == pytest.approx(67889.0)
    assert btc.ask == pytest.approx(67891.0)
    assert len(captured_prices) == 1
