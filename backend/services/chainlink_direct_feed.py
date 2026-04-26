"""Direct Chainlink Data Streams price feed.

Polls Chainlink's `api.dataengine.chain.link` REST endpoint for V3 signed
price reports, decodes them locally, and pushes prices into the existing
:class:`~services.chainlink_feed.ChainlinkFeed` `_prices_by_source` map
under the source tag ``"chainlink_direct"``.

Why this exists alongside :mod:`services.chainlink_feed`:

- ``chainlink_feed`` consumes Polymarket's RTDS-relayed Chainlink prices.
  When RTDS is degraded or paused, that source goes silent — but
  Polymarket's settlement still uses the underlying Chainlink oracle.
- This module reads the same oracle directly. Comparing the two surfaces
  RTDS lag/staleness so strategies can see when they're trading on a
  stale relay.
- It also exposes Chainlink's bid/ask (V3 reports include both) which
  the relay does not relay.

Auth: requires a Chainlink Data Streams API key + user secret. When
credentials are missing the feed latches DISABLED via
:class:`~utils.feed_availability.FeedAvailability` and never enters the
poll loop.

Reference: https://docs.chain.link/data-streams
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import time
from dataclasses import dataclass
from typing import Callable, Optional
from urllib.parse import urlparse

import httpx

from utils.feed_availability import FeedAvailability, FeedStatus
from utils.logger import get_logger

logger = get_logger(__name__)


CHAINLINK_API_BASE = "https://api.dataengine.chain.link"

# Mainnet Data Streams V3 feed IDs. Verified against
# `/api/v1/reports/latest` returns at time of writing — re-validate if
# Chainlink rotates them. The 0x000003... prefix encodes report version 3.
FEED_IDS: dict[str, str] = {
    "BTC": "0x00039d9e45394f473ab1f050a1b963e6b05351e52d71e507509ada0c95ed75b8",
    "ETH": "0x000362205e10b3a147d02792eccee483dca6c7b44ecce7012cb8c6e0b68b3ae9",
    "SOL": "0x0003b778d3f6b2ac4991302b89cb313f99a42467d6c9c5f96f57c29c0d2bc24f",
    "XRP": "0x0003c16c6aed42294f5cb4741f6e59ba2d728f0eae2eb9e6d3f555808c59fc45",
}

POLL_INTERVAL_SECONDS = 2.0
REQUEST_TIMEOUT_SECONDS = 5.0


@dataclass
class ChainlinkDirectPrice:
    asset: str
    price: float
    bid: Optional[float]
    ask: Optional[float]
    observations_timestamp_ms: int
    received_at_ms: int


def _generate_auth_headers(
    method: str,
    url: str,
    api_key: str,
    user_secret: str,
    body: str = "",
) -> dict[str, str]:
    """Sign a Chainlink Data Streams request.

    HMAC-SHA256 over a SPACE-separated canonical string (per the official
    SDK; do not switch to newline separators — that's a different scheme).
    Mirrors the JS reference in
    ``UpDownWalletMonitor/src/app/api/crypto/chainlink-prices-stream/route.ts``.
    """
    ts = int(time.time() * 1000)
    parsed = urlparse(url)
    path_with_query = parsed.path + ("?" + parsed.query if parsed.query else "")
    body_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()
    base = f"{method} {path_with_query} {body_hash} {api_key} {ts}"
    signature = hmac.new(
        user_secret.encode("utf-8"),
        base.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return {
        "Authorization": api_key,
        "X-Authorization-Timestamp": str(ts),
        "X-Authorization-Signature-SHA256": signature,
    }


def _decode_v3_report(full_report_hex: str) -> Optional[dict]:
    """Decode a Chainlink Data Streams V3 signed report.

    Returns a dict with ``benchmark_price`` / ``bid`` / ``ask`` (floats,
    1e18-scaled fixed-point converted) and ``observations_timestamp``
    (Unix seconds), or ``None`` on decode failure.

    The fullReport is `abi.encode(bytes32[3] context, bytes report,
    bytes32[] rs, bytes32[] ss, bytes32 rawVs)`. The inner ``report`` blob
    is V3-encoded with bid/ask/benchmark prices.
    """
    try:
        from eth_abi import decode as abi_decode
    except ImportError:
        return None

    try:
        raw = bytes.fromhex(full_report_hex.removeprefix("0x"))
        _context, report_bytes, _rs, _ss, _raw_vs = abi_decode(
            ["bytes32[3]", "bytes", "bytes32[]", "bytes32[]", "bytes32"],
            raw,
        )
        (
            _feed_id,
            _valid_from,
            obs_ts,
            _native_fee,
            _link_fee,
            _expires,
            benchmark_price,
            bid,
            ask,
        ) = abi_decode(
            [
                "bytes32",
                "uint32",
                "uint32",
                "uint192",
                "uint192",
                "uint32",
                "int192",
                "int192",
                "int192",
            ],
            report_bytes,
        )
        scale = 10**18
        return {
            "observations_timestamp": int(obs_ts),
            "benchmark_price": float(benchmark_price) / scale,
            "bid": float(bid) / scale,
            "ask": float(ask) / scale,
        }
    except Exception as exc:
        logger.debug("chainlink V3 report decode failed", error=str(exc))
        return None


class ChainlinkDirectFeed:
    """Pollable Chainlink Data Streams REST client.

    Designed to be wired alongside :class:`~services.chainlink_feed.ChainlinkFeed`:
    forwards each decoded price into a callback so the same OraclePrice
    map can hold a ``chainlink_direct`` source entry next to the
    relay-sourced ``chainlink`` entry.
    """

    def __init__(
        self,
        get_api_key: Callable[[], str],
        get_user_secret: Callable[[], str],
        api_base: str = CHAINLINK_API_BASE,
        poll_interval: float = POLL_INTERVAL_SECONDS,
    ) -> None:
        self._get_api_key = get_api_key
        self._get_user_secret = get_user_secret
        self._api_base = api_base
        self._poll_interval = poll_interval
        self._task: Optional[asyncio.Task] = None
        self._stopped = False
        self._on_update: Optional[Callable[[ChainlinkDirectPrice], None]] = None
        self._availability = FeedAvailability(
            has_credentials=lambda: bool(self._get_api_key() and self._get_user_secret()),
            name="chainlink_direct",
        )
        self._latest: dict[str, ChainlinkDirectPrice] = {}

    @property
    def status(self) -> FeedStatus:
        if self._availability.is_disabled:
            return FeedStatus.DISABLED
        if not self.started:
            return FeedStatus.UNINITIALIZED
        return FeedStatus.HEALTHY if self._latest else FeedStatus.CONNECTING

    @property
    def started(self) -> bool:
        return self._task is not None and not self._task.done()

    @property
    def disabled_reason(self) -> Optional[str]:
        return self._availability.disabled_reason

    def get_price(self, asset: str) -> Optional[ChainlinkDirectPrice]:
        return self._latest.get(asset.upper())

    def get_all_prices(self) -> dict[str, ChainlinkDirectPrice]:
        return dict(self._latest)

    def on_update(self, callback: Callable[[ChainlinkDirectPrice], None]) -> None:
        self._on_update = callback

    def rearm(self) -> None:
        """Clear the disabled latch — call after credentials are updated."""
        self._availability.rearm()

    async def start(self) -> None:
        """Begin polling. No-op if credentials are absent (latched DISABLED)."""
        if self._task and not self._task.done():
            return
        if not self._availability.check():
            logger.info(
                "ChainlinkDirectFeed disabled — missing credentials. "
                "Set Chainlink API key + user secret in settings to enable."
            )
            return
        self._stopped = False
        self._task = asyncio.create_task(self._run_loop())
        logger.info("ChainlinkDirectFeed: started")

    async def stop(self) -> None:
        self._stopped = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("ChainlinkDirectFeed: stopped")

    async def _run_loop(self) -> None:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as client:
            while not self._stopped:
                if self._availability.is_disabled:
                    return
                for asset, feed_id in FEED_IDS.items():
                    if self._stopped:
                        break
                    try:
                        await self._poll_one(client, asset, feed_id)
                    except asyncio.CancelledError:
                        return
                    except Exception as exc:
                        logger.debug(
                            "ChainlinkDirectFeed poll error",
                            asset=asset,
                            error=str(exc),
                        )
                try:
                    await asyncio.sleep(self._poll_interval)
                except asyncio.CancelledError:
                    return

    async def _poll_one(
        self,
        client: httpx.AsyncClient,
        asset: str,
        feed_id: str,
    ) -> None:
        url = f"{self._api_base}/api/v1/reports/latest?feedID={feed_id}"
        api_key = self._get_api_key()
        user_secret = self._get_user_secret()
        if not api_key or not user_secret:
            self._availability.latch_auth_failure(reason="missing_credentials")
            logger.warning("ChainlinkDirectFeed: credentials cleared mid-flight, latching disabled")
            return

        headers = _generate_auth_headers("GET", url, api_key, user_secret)
        headers["Accept"] = "application/json"

        try:
            resp = await client.get(url, headers=headers)
        except httpx.HTTPError:
            return  # transient — retry next tick

        if resp.status_code in (401, 403):
            # Persistent auth failure — latch disabled to avoid a 401 storm.
            # Operator must rotate credentials and call rearm() (wired from
            # the settings-update path).
            self._availability.latch_auth_failure(reason=f"http_{resp.status_code}")
            logger.error(
                "ChainlinkDirectFeed: auth rejected, latching disabled. "
                "Rotate Chainlink API credentials and re-save settings.",
                status=resp.status_code,
            )
            return

        if resp.status_code != 200:
            return

        try:
            data = resp.json()
        except ValueError:
            return

        report = data.get("report") if isinstance(data, dict) else None
        if not isinstance(report, dict):
            return

        full_report = report.get("fullReport")
        decoded: Optional[dict] = None
        if isinstance(full_report, str):
            decoded = _decode_v3_report(full_report)

        # Fallback: some API responses surface benchmarkPrice at the
        # metadata level. Use that if the V3 decode fails (e.g. report
        # version we don't yet handle). Don't synthesize bid/ask.
        if decoded is None:
            try:
                bp = report.get("benchmarkPrice")
                if bp is not None:
                    decoded = {
                        "benchmark_price": float(bp),
                        "bid": float(report["bid"]) if report.get("bid") is not None else None,
                        "ask": float(report["ask"]) if report.get("ask") is not None else None,
                        "observations_timestamp": int(report.get("observationsTimestamp") or 0),
                    }
            except (TypeError, ValueError):
                decoded = None

        if not decoded or not (decoded.get("benchmark_price") or 0) > 0:
            return

        obs_ts_s = int(decoded.get("observations_timestamp") or 0)
        obs_ts_ms = obs_ts_s * 1000 if obs_ts_s and obs_ts_s < 1e12 else obs_ts_s
        if not obs_ts_ms:
            obs_ts_ms = int(time.time() * 1000)

        price = ChainlinkDirectPrice(
            asset=asset,
            price=float(decoded["benchmark_price"]),
            bid=decoded.get("bid"),
            ask=decoded.get("ask"),
            observations_timestamp_ms=obs_ts_ms,
            received_at_ms=int(time.time() * 1000),
        )
        self._latest[asset] = price

        if self._on_update is not None:
            try:
                self._on_update(price)
            except Exception:
                logger.exception("ChainlinkDirectFeed on_update callback raised")


# ---------------------------------------------------------------------------
# Singleton wiring
# ---------------------------------------------------------------------------

_instance: Optional[ChainlinkDirectFeed] = None


def get_chainlink_direct_feed(
    get_api_key: Optional[Callable[[], str]] = None,
    get_user_secret: Optional[Callable[[], str]] = None,
) -> ChainlinkDirectFeed:
    """Return the process-wide ChainlinkDirectFeed.

    First call must supply credential getters. Subsequent calls return the
    same instance and ignore the arguments.
    """
    global _instance
    if _instance is None:
        if get_api_key is None or get_user_secret is None:
            raise RuntimeError(
                "ChainlinkDirectFeed not yet initialized; pass credential "
                "getters on first call"
            )
        _instance = ChainlinkDirectFeed(
            get_api_key=get_api_key,
            get_user_secret=get_user_secret,
        )
    return _instance
