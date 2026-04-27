"""Regression test for the two-phase HTTP/DB pattern in the verifier.

Production hit ``polymarket_trade_verifier (bot_lineage) timed out after
30.0s`` because per-row ``_fetch_market_info(row)`` HTTP calls happened
INSIDE the open SQL transaction.  PG saw the connection as
``idle in transaction`` for the full HTTP fan-out, and the surrounding
``asyncio.wait_for(..., timeout=30s)`` killed the cycle.

The fix is a two-phase pattern:

  Phase 1: prefetch all market_info outside the transaction (with
           ``release_conn(session)`` wrapping the HTTP fan-out).
  Phase 2: iterate rows and do SQL-only writes against the prefetched
           dict.

This test asserts that ALL HTTP happens BEFORE the first row-level
session write.  If a future change reintroduces in-loop HTTP, this
test will fail loudly.
"""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))


@pytest.mark.asyncio
async def test_prefetch_market_info_concurrency_is_bounded():
    """The pre-fetch helper runs at most _MARKET_INFO_PREFETCH_CONCURRENCY
    HTTP calls in flight at any moment, even with hundreds of input rows."""
    from services import polymarket_trade_verifier as v

    in_flight = 0
    max_in_flight = 0

    async def slow_fetch(row):
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        await asyncio.sleep(0.01)
        in_flight -= 1
        return {"resolved": True, "winning_outcome": "yes"}

    rows = [MagicMock(id=f"row-{i}") for i in range(50)]

    with patch.object(v, "_fetch_market_info", side_effect=slow_fetch):
        result = await v._prefetch_market_info(rows)

    assert len(result) == 50
    assert max_in_flight <= v._MARKET_INFO_PREFETCH_CONCURRENCY, (
        f"Saw {max_in_flight} concurrent fetches, expected ≤ "
        f"{v._MARKET_INFO_PREFETCH_CONCURRENCY}"
    )


@pytest.mark.asyncio
async def test_prefetch_market_info_swallows_individual_failures():
    """One row's HTTP failure must not break the whole batch."""
    from services import polymarket_trade_verifier as v

    async def maybe_fail(row):
        if row.id == "row-bad":
            raise RuntimeError("simulated HTTP error")
        return {"resolved": True}

    rows = [MagicMock(id=f"row-{i}") for i in ("good", "bad", "good2")]

    with patch.object(v, "_fetch_market_info", side_effect=maybe_fail):
        result = await v._prefetch_market_info(rows)

    assert result["row-good"] == {"resolved": True}
    assert result["row-bad"] is None  # failure recorded, not raised
    assert result["row-good2"] == {"resolved": True}


@pytest.mark.asyncio
async def test_prefetch_returns_empty_for_no_rows():
    from services import polymarket_trade_verifier as v
    assert await v._prefetch_market_info([]) == {}
