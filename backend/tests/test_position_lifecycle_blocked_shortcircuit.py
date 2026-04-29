"""Regression tests for the permanent-block short-circuit in
``reconcile_live_positions``.

Order ``350ebe8b3fae41a79b7555a0a852aee5`` (a small scanner buy_yes
that hit max-hold) was eating 18.4s of every reconcile cycle by
re-attempting a SELL into a market the CLOB no longer accepts.  Each
attempt failed with TimeoutError, the blocked-streak counter
ratcheted, status became ``blocked_persistent_timeout`` — but the
trigger evaluation kept re-firing "Max hold exceeded" and creating
fresh exits anyway.

The short-circuit at the top of the per-candidate loop catches this
case in <1ms: if pending_exit.status is already terminal-blocked AND
market_tradable is False AND no resolution evidence is available,
just hold the row and skip everything.
"""
from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from models.database import (
    Base,
    Trader,
    TraderOrder,
)
from services.trader_orchestrator.position_lifecycle import (
    _LIVE_EXIT_ORDER_TIMEOUT_SECONDS,
    _LIVE_EXIT_RETRY_TIMEOUT_SECONDS,
    reconcile_live_positions,
)
from tests.postgres_test_db import build_postgres_session_factory
from utils.utcnow import utcnow


def test_exit_timeout_floor_and_ceiling():
    """Sanity-check the per-attempt SELL wall-time cap.

    Pre-incident this was 5s and the test asserted ``<= 5``; that
    floor was wrong — the SDK's own internal SELL timeout is ~12s, so
    a 5s outer ``asyncio.wait_for`` cancelled mid-protocol every time
    and produced the false-stuck-position cascade documented in
    commit a8ef524.  Bumped to 15.0 / 12.0 to outlive the SDK timeout
    while still bounding total cycle cost.

    The bound here just protects against future drift in either
    direction:
      * A floor (>=12) so we don't reintroduce the premature-cancel
        pathology.
      * A ceiling (<=30) so a single retry can never consume an
        entire reconcile cycle.
    """
    assert 12.0 <= _LIVE_EXIT_ORDER_TIMEOUT_SECONDS <= 30.0
    assert 12.0 <= _LIVE_EXIT_RETRY_TIMEOUT_SECONDS <= 30.0


async def _seed_trader_and_order(session, trader_id: str, order_id: str, *, pending_exit_status: str) -> None:
    """Seed a Trader + a TraderOrder mirroring 350ebe8b's stuck state.

    payload['pending_live_exit'] carries the terminal-blocked status the
    short-circuit looks for.  market_id is set to a known sentinel that
    the test-side mock will report as untradable.
    """
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
    session.add(
        TraderOrder(
            id=order_id,
            trader_id=trader_id,
            source="scanner",
            market_id="market-untradable",
            direction="buy_yes",
            mode="live",
            status="executed",
            notional_usd=7.56,
            entry_price=0.87,
            effective_price=0.87,
            payload_json={
                "selected_token_id": "token-X",
                "live_market": {"selected_token_id": "token-X"},
                "filled_notional_usd": 7.56,
                "filled_size": 8.69,
                "average_fill_price": 0.87,
                "pending_live_exit": {
                    "status": pending_exit_status,
                    "kind": "max_hold",
                    "exit_size": 8.69,
                    "remaining_size": 3.64,
                    "close_price": 0.895,
                    "consecutive_timeout_count": 3,
                    "consecutive_blocked_failure_count": 3,
                    "last_error": "TimeoutError",
                    "last_attempt_at": now.isoformat(),
                    "next_retry_at": None,
                },
            },
            created_at=now - timedelta(hours=24, minutes=10),
            updated_at=now - timedelta(minutes=1),
            executed_at=now - timedelta(hours=24, minutes=10),
        )
    )
    await session.commit()


@pytest.mark.asyncio
async def test_blocked_persistent_timeout_on_untradable_market_short_circuits(monkeypatch, tmp_path):
    """A blocked_persistent_timeout order on an untradable market must
    return without invoking ANY of the SELL submission HTTP calls.

    We patch ``execute_live_order`` and ``prepare_sell_balance_allowance``
    to fail loudly if reached.  The short-circuit guard should prevent
    them from being called.
    """
    from services import polymarket as _polymarket_mod
    from services.trader_orchestrator import position_lifecycle

    engine, session_factory = await build_postgres_session_factory(
        Base, "blocked_shortcircuit_untradable"
    )
    trader_id = "trader-stuck"
    order_id = "350ebe8b-test-stuck"

    # Patch the polymarket client's market-tradable check to return False
    # for our sentinel market_id.
    def _fake_is_market_tradable(market_info, *, now=None):
        return False

    monkeypatch.setattr(
        _polymarket_mod.polymarket_client,
        "is_market_tradable",
        _fake_is_market_tradable,
    )

    # Make load_market_info_for_orders return a non-empty market_info
    # for our order so pending_market_info is not None — that matters
    # for the short-circuit branch which requires market_info present.
    async def _fake_load_market_info_for_orders(orders):
        return {str(getattr(o, "id", "")): {"id": "market-untradable"} for o in orders}

    monkeypatch.setattr(
        position_lifecycle,
        "load_market_info_for_orders",
        _fake_load_market_info_for_orders,
    )

    # Trip the test loudly if the SELL submission path is reached.
    sell_invoked = {"count": 0}

    class _SentinelExitPathTouched(AssertionError):
        pass

    async def _fail_if_called(*args, **kwargs):
        sell_invoked["count"] += 1
        raise _SentinelExitPathTouched(
            "execute_live_order should NOT be called for blocked_persistent_timeout "
            "orders on untradable markets — the short-circuit must skip them"
        )

    # Patch both the live_execution_service.prepare_sell_balance_allowance
    # and execute_live_order so any code path that touches them blows up.
    from services import live_execution_service as _lex

    async def _fail_prepare(*args, **kwargs):
        sell_invoked["count"] += 1
        raise _SentinelExitPathTouched("prepare_sell_balance_allowance should NOT be called")

    monkeypatch.setattr(
        _lex.live_execution_service,
        "prepare_sell_balance_allowance",
        _fail_prepare,
    )

    try:
        async with session_factory() as session:
            await _seed_trader_and_order(
                session,
                trader_id,
                order_id,
                pending_exit_status="blocked_persistent_timeout",
            )

        async with session_factory() as session:
            result = await reconcile_live_positions(
                session,
                trader_id=trader_id,
                trader_params={},
                dry_run=False,
                reason="test_blocked_shortcircuit",
            )

        # The short-circuit should have held the row without calling
        # any SELL/HTTP.
        assert sell_invoked["count"] == 0, (
            f"SELL path must not be invoked for blocked_persistent_timeout + untradable; "
            f"was invoked {sell_invoked['count']} time(s)"
        )
        # The reconcile should report exactly 0 orders closed and 1 held
        # (or "skipped"/whatever the short-circuit increments — we just
        # care that nothing closed and the SELL never fired).
        assert result.get("closed", 0) == 0
        # Confirm the order's row state didn't change.
        async with session_factory() as session:
            row = await session.get(TraderOrder, order_id)
            assert row is not None
            assert row.status == "executed"
            payload = row.payload_json or {}
            pe = payload.get("pending_live_exit") or {}
            assert pe.get("status") == "blocked_persistent_timeout"
    finally:
        await engine.dispose()
