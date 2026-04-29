"""Tests for the stuck-position surveillance + on-chain truth primitive.

Covers:
  * fetch_position_chain_status returns structured truth, doesn't write
  * Bad inputs return error sentinels rather than raising
  * RPC failures are caught and reported in the error field
  * scan_stuck_positions filters by pending_exit_status + age
  * classify_stuck_position routes correctly:
      balance=0       → recovered_externally
      balance>0+resolved → redemption_pending
      balance>0+unresolved → operator_intervention
  * alert_operator_on_stuck_positions respects per-order cooldown
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from models.database import Base, Trader, TraderOrder
from services import stuck_position_monitor as spm
from services.ctf_execution import ctf_execution_service
from tests.postgres_test_db import build_postgres_session_factory
from utils.utcnow import utcnow


# ── on-chain truth primitive ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_fetch_position_chain_status_rejects_missing_inputs():
    result = await ctf_execution_service.fetch_position_chain_status(
        wallet_address="",
        token_id="",
        condition_id="",
        outcome_index=0,
    )
    assert result["error"] == "missing_required_input"
    assert result["wallet_balance_shares"] == 0.0


@pytest.mark.asyncio
async def test_fetch_position_chain_status_rejects_bad_condition_id():
    result = await ctf_execution_service.fetch_position_chain_status(
        wallet_address="0x" + "a" * 40,
        token_id="1234567890",
        condition_id="not-a-bytes32",
        outcome_index=0,
    )
    assert result["error"] == "invalid_condition_id_format"


@pytest.mark.asyncio
async def test_fetch_position_chain_status_rejects_bad_token_id():
    result = await ctf_execution_service.fetch_position_chain_status(
        wallet_address="0x" + "a" * 40,
        token_id="not-a-number",
        condition_id="0x" + "0" * 64,
        outcome_index=0,
    )
    assert result["error"] == "invalid_token_id_format"


@pytest.mark.asyncio
async def test_fetch_position_chain_status_unresolved_market_returns_zero_payout():
    """When the market is NOT resolved (denominator==0), expected
    payout is 0 and ``winning`` is None — never an inferred number."""
    fake_w3 = MagicMock()
    fake_w3.eth.block_number = 12345
    fake_w3.to_checksum_address = lambda x: x
    contract = MagicMock()
    contract.functions.balanceOf.return_value.call.return_value = 8_690_000  # 8.69 shares
    contract.functions.payoutDenominator.return_value.call.return_value = 0  # unresolved
    contract.functions.payoutNumerators.return_value.call.return_value = 0
    fake_w3.eth.contract.return_value = contract

    async def fake_get_web3():
        return fake_w3

    with patch.object(ctf_execution_service, "_get_web3", side_effect=fake_get_web3):
        result = await ctf_execution_service.fetch_position_chain_status(
            wallet_address="0x" + "a" * 40,
            token_id="123456789012345678",
            condition_id="0x" + "1" * 64,
            outcome_index=0,
        )
    assert result["error"] is None
    assert result["wallet_balance_shares"] == pytest.approx(8.69)
    assert result["market_resolved"] is False
    assert result["winning"] is None
    assert result["expected_payout_usdc"] == 0.0


@pytest.mark.asyncio
async def test_fetch_position_chain_status_resolved_winner():
    """Binary winner: numerator=1, denominator=1 → full payout."""
    fake_w3 = MagicMock()
    fake_w3.eth.block_number = 99999
    fake_w3.to_checksum_address = lambda x: x
    contract = MagicMock()
    contract.functions.balanceOf.return_value.call.return_value = 5_000_000  # 5.0 shares
    contract.functions.payoutDenominator.return_value.call.return_value = 1
    contract.functions.payoutNumerators.return_value.call.return_value = 1
    fake_w3.eth.contract.return_value = contract

    async def fake_get_web3():
        return fake_w3

    with patch.object(ctf_execution_service, "_get_web3", side_effect=fake_get_web3):
        result = await ctf_execution_service.fetch_position_chain_status(
            wallet_address="0x" + "b" * 40,
            token_id="111",
            condition_id="0x" + "2" * 64,
            outcome_index=0,
        )
    assert result["error"] is None
    assert result["market_resolved"] is True
    assert result["winning"] is True
    assert result["expected_payout_usdc"] == pytest.approx(5.0)


@pytest.mark.asyncio
async def test_fetch_position_chain_status_resolved_loser():
    """Binary loser: numerator=0, denominator=1 → zero payout."""
    fake_w3 = MagicMock()
    fake_w3.eth.block_number = 100
    fake_w3.to_checksum_address = lambda x: x
    contract = MagicMock()
    contract.functions.balanceOf.return_value.call.return_value = 5_000_000
    contract.functions.payoutDenominator.return_value.call.return_value = 1
    contract.functions.payoutNumerators.return_value.call.return_value = 0
    fake_w3.eth.contract.return_value = contract

    async def fake_get_web3():
        return fake_w3

    with patch.object(ctf_execution_service, "_get_web3", side_effect=fake_get_web3):
        result = await ctf_execution_service.fetch_position_chain_status(
            wallet_address="0x" + "c" * 40,
            token_id="222",
            condition_id="0x" + "3" * 64,
            outcome_index=1,
        )
    assert result["error"] is None
    assert result["market_resolved"] is True
    assert result["winning"] is False
    assert result["expected_payout_usdc"] == 0.0


@pytest.mark.asyncio
async def test_fetch_position_chain_status_rpc_failure_doesnt_raise():
    async def boom_get_web3(self=None):
        raise RuntimeError("All Polygon RPC providers failed")

    with patch.object(ctf_execution_service, "_get_web3", side_effect=boom_get_web3):
        result = await ctf_execution_service.fetch_position_chain_status(
            wallet_address="0x" + "d" * 40,
            token_id="333",
            condition_id="0x" + "4" * 64,
            outcome_index=0,
        )
    assert result["error"] is not None
    assert result["error"].startswith("rpc_unavailable")


# ── stuck-position monitor ────────────────────────────────────────────


async def _seed_blocked(
    session,
    *,
    trader_id: str,
    order_id: str,
    pending_status: str,
    created_minutes_ago: int = 24 * 60,
    market_id: str = "market-untradable",
    condition_id: str = "0x" + "1" * 64,
    token_id: str = "9999999",
):
    session.add(
        Trader(
            id=trader_id,
            name=f"trader-{trader_id}",
            mode="live",
            source_configs_json=[],
            risk_limits_json={},
        )
    )
    now = utcnow()
    created_at = now - timedelta(minutes=created_minutes_ago)
    session.add(
        TraderOrder(
            id=order_id,
            trader_id=trader_id,
            source="scanner",
            market_id=market_id,
            direction="buy_yes",
            mode="live",
            status="executed",
            notional_usd=7.56,
            entry_price=0.87,
            payload_json={
                "selected_token_id": token_id,
                "condition_id": condition_id,
                "pending_live_exit": {
                    "status": pending_status,
                    "outcomeIndex": 0,
                    "consecutive_blocked_failure_count": 3,
                    "last_error": "TimeoutError",
                    "last_attempt_at": now.isoformat(),
                },
            },
            created_at=created_at,
            # updated_at intentionally set to "now" — simulates retry
            # storms touching the row.  The new age filter uses
            # created_at so this should NOT affect inclusion.
            updated_at=now,
            executed_at=created_at,
        )
    )
    await session.commit()


@pytest.mark.asyncio
async def test_scan_filters_by_age_and_pending_status(tmp_path):
    engine, session_factory = await build_postgres_session_factory(
        Base, "spm_scan_filter"
    )
    try:
        async with session_factory() as session:
            # Old + blocked → matches.  Note: ``updated_at`` is set to
            # "now" inside _seed_blocked to simulate retry storms; the
            # filter uses ``created_at`` so this still qualifies.
            await _seed_blocked(
                session,
                trader_id="t1",
                order_id="stuck-old",
                pending_status="blocked_persistent_timeout",
                created_minutes_ago=24 * 60,  # 1 day old
            )
            # Recently CREATED + blocked → too young, must be excluded.
            await _seed_blocked(
                session,
                trader_id="t2",
                order_id="stuck-young",
                pending_status="blocked_persistent_timeout",
                created_minutes_ago=1,
            )
            # Old but pending_status=failed → not blocked-terminal.
            await _seed_blocked(
                session,
                trader_id="t3",
                order_id="not-blocked",
                pending_status="failed",
                created_minutes_ago=24 * 60,
            )

        observations = await spm.scan_stuck_positions(
            age_hours=6.0, session_factory=session_factory
        )
        ids = {o["order_id"] for o in observations}
        assert ids == {"stuck-old"}
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_classify_zero_balance_is_recovered_externally(monkeypatch):
    """If the wallet holds 0 shares on-chain, classification is
    ``recovered_externally`` — the verifier will pick up the SELL on
    its next cycle.  No alert."""
    fake_status = {
        "wallet_balance_shares": 0.0,
        "market_resolved": False,
        "error": None,
    }

    async def fake_fetch(**kwargs):
        return fake_status

    monkeypatch.setattr(ctf_execution_service, "fetch_position_chain_status", fake_fetch)
    from services import live_execution_service as _les
    monkeypatch.setattr(
        _les.live_execution_service,
        "get_execution_wallet_address",
        lambda: "0x" + "a" * 40,
    )

    obs = {
        "order_id": "x",
        "trader_id": "t",
        "token_id": "111",
        "condition_id": "0x" + "1" * 64,
        "outcome_index": 0,
    }
    result = await spm.classify_stuck_position(obs)
    assert result["classification"] == "recovered_externally"


@pytest.mark.asyncio
async def test_classify_resolved_market_is_redemption_pending(monkeypatch):
    """Holdings + market_resolved → the redeemer worker will
    redeem on its next 120s cycle.  No alert."""
    async def fake_fetch(**kwargs):
        return {
            "wallet_balance_shares": 5.0,
            "market_resolved": True,
            "error": None,
        }

    monkeypatch.setattr(ctf_execution_service, "fetch_position_chain_status", fake_fetch)
    from services import live_execution_service as _les
    monkeypatch.setattr(
        _les.live_execution_service,
        "get_execution_wallet_address",
        lambda: "0x" + "a" * 40,
    )

    obs = {
        "order_id": "x",
        "trader_id": "t",
        "token_id": "111",
        "condition_id": "0x" + "1" * 64,
        "outcome_index": 0,
    }
    result = await spm.classify_stuck_position(obs)
    assert result["classification"] == "redemption_pending"


@pytest.mark.asyncio
async def test_classify_unresolved_with_holdings_and_venue_rejection_needs_operator(monkeypatch):
    """Holdings + market_unresolved + retry circuit-broken with
    *genuine venue-rejection evidence* in last_error = the case that
    actually needs human review.  The presence of a known
    venue-rejection marker (here: ``orderbook does not exist``)
    distinguishes a real venue-side problem from our own client-side
    timeout cascade."""
    async def fake_fetch(**kwargs):
        return {
            "wallet_balance_shares": 8.69,
            "market_resolved": False,
            "error": None,
        }

    monkeypatch.setattr(ctf_execution_service, "fetch_position_chain_status", fake_fetch)
    from services import live_execution_service as _les
    monkeypatch.setattr(
        _les.live_execution_service,
        "get_execution_wallet_address",
        lambda: "0x" + "a" * 40,
    )

    obs = {
        "order_id": "x",
        "trader_id": "t",
        "token_id": "111",
        "condition_id": "0x" + "1" * 64,
        "outcome_index": 0,
        "last_error": "Order rejected by CLOB: orderbook does not exist for this market",
    }
    result = await spm.classify_stuck_position(obs)
    assert result["classification"] == "operator_intervention"


@pytest.mark.asyncio
async def test_classify_pending_resolution_when_market_ends_within_window(monkeypatch):
    """If the market end_date is inside ``_PENDING_RESOLUTION_WINDOW_DAYS``
    we classify as ``pending_resolution`` regardless of last_error.

    Rationale: the redeemer worker auto-closes on resolution, so even
    a genuine venue-rejection (CLOB has stopped accepting SELLs in
    the last hours of a market — extremely common) should not page
    the operator.  By the time anyone could intervene the market will
    have resolved on its own."""
    async def fake_fetch(**kwargs):
        return {
            "wallet_balance_shares": 8.69,
            "market_resolved": False,
            "error": None,
        }

    monkeypatch.setattr(ctf_execution_service, "fetch_position_chain_status", fake_fetch)
    from services import live_execution_service as _les
    monkeypatch.setattr(
        _les.live_execution_service,
        "get_execution_wallet_address",
        lambda: "0x" + "a" * 40,
    )

    end_time = (utcnow() + timedelta(days=2)).isoformat().replace("+00:00", "Z")
    obs = {
        "order_id": "x",
        "trader_id": "t",
        "token_id": "111",
        "condition_id": "0x" + "1" * 64,
        "outcome_index": 0,
        # Even with a "real" venue rejection, the imminent resolution
        # gate fires first.
        "last_error": "Order rejected by CLOB: orderbook does not exist",
        "market_end_time": end_time,
    }
    result = await spm.classify_stuck_position(obs)
    assert result["classification"] == "pending_resolution"
    assert result["seconds_until_market_end"] is not None
    assert 0 < result["seconds_until_market_end"] < 7 * 86400


@pytest.mark.asyncio
async def test_classify_pending_resolution_includes_already_past_end_date(monkeypatch):
    """If the gamma end_date is in the past but the chain hasn't yet
    reported the resolution (oracle lag), we still classify as
    ``pending_resolution`` — the redeemer is going to fire any moment.
    Alerting now would just be noise."""
    async def fake_fetch(**kwargs):
        return {
            "wallet_balance_shares": 8.69,
            "market_resolved": False,
            "error": None,
        }

    monkeypatch.setattr(ctf_execution_service, "fetch_position_chain_status", fake_fetch)
    from services import live_execution_service as _les
    monkeypatch.setattr(
        _les.live_execution_service,
        "get_execution_wallet_address",
        lambda: "0x" + "a" * 40,
    )

    end_time = (utcnow() - timedelta(hours=2)).isoformat().replace("+00:00", "Z")
    obs = {
        "order_id": "x",
        "trader_id": "t",
        "token_id": "111",
        "condition_id": "0x" + "1" * 64,
        "outcome_index": 0,
        "last_error": "Order rejected by CLOB: orderbook does not exist",
        "market_end_time": end_time,
    }
    result = await spm.classify_stuck_position(obs)
    assert result["classification"] == "pending_resolution"


@pytest.mark.asyncio
async def test_classify_far_end_date_falls_through_to_venue_rejection(monkeypatch):
    """If end_date is far in the future (or unknown) AND last_error
    matches a venue-rejection marker, we still escalate to
    ``operator_intervention``.  The pending_resolution gate must NOT
    swallow genuinely stuck positions."""
    async def fake_fetch(**kwargs):
        return {
            "wallet_balance_shares": 8.69,
            "market_resolved": False,
            "error": None,
        }

    monkeypatch.setattr(ctf_execution_service, "fetch_position_chain_status", fake_fetch)
    from services import live_execution_service as _les
    monkeypatch.setattr(
        _les.live_execution_service,
        "get_execution_wallet_address",
        lambda: "0x" + "a" * 40,
    )

    end_time = (utcnow() + timedelta(days=30)).isoformat().replace("+00:00", "Z")
    obs = {
        "order_id": "x",
        "trader_id": "t",
        "token_id": "111",
        "condition_id": "0x" + "1" * 64,
        "outcome_index": 0,
        "last_error": "Order rejected by CLOB: orderbook does not exist",
        "market_end_time": end_time,
    }
    result = await spm.classify_stuck_position(obs)
    assert result["classification"] == "operator_intervention"

    # Same observation but no end_time at all — still escalates.
    obs2 = dict(obs)
    obs2["market_end_time"] = ""
    result2 = await spm.classify_stuck_position(obs2)
    assert result2["classification"] == "operator_intervention"


@pytest.mark.asyncio
async def test_classify_unresolved_with_holdings_and_client_timeout_is_transient(monkeypatch):
    """Holdings + market_unresolved + retry circuit-broken but
    last_error is a CLIENT-side failure (TimeoutError, ConnectionError,
    asyncpg, etc.) — classify as ``transient_client_failure`` so the
    alert path stays silent.  The lifecycle's auto-recovery will retry
    the SELL after _BLOCKED_PERSISTENT_TIMEOUT_AUTO_RETRY_AFTER_SECONDS;
    paging the operator on transient infrastructure noise is the
    failure mode that produced the 2026-04-28 incident."""
    async def fake_fetch(**kwargs):
        return {
            "wallet_balance_shares": 8.69,
            "market_resolved": False,
            "error": None,
        }

    monkeypatch.setattr(ctf_execution_service, "fetch_position_chain_status", fake_fetch)
    from services import live_execution_service as _les
    monkeypatch.setattr(
        _les.live_execution_service,
        "get_execution_wallet_address",
        lambda: "0x" + "a" * 40,
    )

    for last_error in (
        "TimeoutError",
        "ConnectionError",
        "asyncpg.exceptions._base.InternalClientError: cannot switch to state",
        "ReadTimeout",
        "",  # missing entirely
    ):
        obs = {
            "order_id": "x",
            "trader_id": "t",
            "token_id": "111",
            "condition_id": "0x" + "1" * 64,
            "outcome_index": 0,
            "last_error": last_error,
        }
        result = await spm.classify_stuck_position(obs)
        assert result["classification"] == "transient_client_failure", (
            f"last_error={last_error!r} should be transient, got "
            f"{result['classification']!r}"
        )


@pytest.mark.asyncio
async def test_alert_respects_per_order_cooldown(monkeypatch):
    """Two scans in quick succession on the same operator-intervention
    row must produce only ONE Telegram alert — the second is
    suppressed by the cooldown."""
    spm._last_alert_at.clear()

    sent: list[str] = []

    async def fake_send(text, *, category="operator"):
        sent.append(text)

    from services import notifier as _notifier_mod
    monkeypatch.setattr(_notifier_mod.notifier, "send_operator_alert", fake_send)

    classified = [
        {
            "order_id": "stuck-A",
            "trader_id": "t",
            "source": "scanner",
            "market_id": "m1",
            "direction": "buy_yes",
            "notional_usd": 5.0,
            "pending_exit_status": "blocked_persistent_timeout",
            "consecutive_blocked_failure_count": 3,
            "last_error": "TimeoutError",
            "classification": "operator_intervention",
            "chain_status": {
                "wallet_balance_shares": 5.0,
                "market_resolved": False,
                "block_number": 1234,
            },
        }
    ]
    s1 = await spm.alert_operator_on_stuck_positions(classified)
    s2 = await spm.alert_operator_on_stuck_positions(classified)

    assert s1["alerts_emitted"] == 1
    assert s2["alerts_emitted"] == 0
    assert s2["alerts_suppressed_by_cooldown"] == 1
    assert len(sent) == 1
    # Alert framing was deliberately softened away from "needs
    # operator review" to "informational" because by the time we
    # reach this code path the auto-recovery has had multiple chances
    # and the redeemer will close on resolution.  Regression-guard
    # the rename so a future copy edit doesn't silently bring back
    # the alarmist phrasing.
    assert "needs operator review" not in sent[0]
    assert "informational" in sent[0].lower()
    assert "No action required" in sent[0]


@pytest.mark.asyncio
async def test_alert_pending_resolution_is_counted_but_never_emitted(monkeypatch):
    """``pending_resolution`` rows must be counted in the summary so
    they show up on the heartbeat dashboard, but must NEVER produce a
    Telegram alert — there is nothing for the operator to do."""
    spm._last_alert_at.clear()

    sent: list[str] = []

    async def fake_send(text, *, category="operator"):
        sent.append(text)

    from services import notifier as _notifier_mod
    monkeypatch.setattr(_notifier_mod.notifier, "send_operator_alert", fake_send)

    classified = [
        {
            "order_id": "stuck-PR",
            "trader_id": "t",
            "source": "scanner",
            "market_id": "m1",
            "direction": "buy_yes",
            "notional_usd": 5.0,
            "pending_exit_status": "blocked_persistent_timeout",
            "consecutive_blocked_failure_count": 3,
            "last_error": "Order rejected by CLOB: orderbook does not exist",
            "classification": "pending_resolution",
            "chain_status": {
                "wallet_balance_shares": 5.0,
                "market_resolved": False,
                "block_number": 1234,
            },
        }
    ]
    summary = await spm.alert_operator_on_stuck_positions(classified)

    assert summary["pending_resolution"] == 1
    assert summary["alerts_emitted"] == 0
    assert summary["alerts_suppressed_by_cooldown"] == 0
    assert sent == []
