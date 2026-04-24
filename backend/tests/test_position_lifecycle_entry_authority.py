from types import SimpleNamespace

import pytest

from services.trader_orchestrator.position_lifecycle import (
    _extract_live_fill_metrics,
    _is_rapid_close_trigger,
    _live_position_notional_cap,
    _pending_exit_is_live_fallback_manual_exit,
    _position_close_missing_terminal_fill_authority,
    _position_close_underfills_entry,
    _row_can_recover_entry_from_wallet_history,
    _row_has_wallet_trade_fill_authority,
)
from services.trader_order_verification import (
    append_trader_order_verification_event,
    apply_trader_order_verification,
)


def test_provider_order_id_without_fill_is_not_entry_fill_authority():
    row = SimpleNamespace(
        provider_clob_order_id="0xabc",
        provider_order_id=None,
        verification_status="venue_order",
    )
    payload = {
        "provider_clob_order_id": "0xabc",
        "provider_reconciliation": {
            "snapshot": {"status": "cancelled", "filled_size": 0.0, "average_fill_price": 0.0},
            "filled_size": 0.0,
            "filled_notional_usd": 0.0,
        },
    }

    assert not _row_has_wallet_trade_fill_authority(row, payload)
    assert _row_can_recover_entry_from_wallet_history(row, payload)


def test_positive_fill_metrics_are_entry_fill_authority():
    row = SimpleNamespace(
        provider_clob_order_id="0xabc",
        provider_order_id=None,
        verification_status="venue_order",
    )
    payload = {
        "provider_reconciliation": {
            "filled_size": 7.5,
            "average_fill_price": 0.42,
            "filled_notional_usd": 3.15,
        },
    }

    assert _row_has_wallet_trade_fill_authority(row, payload)


def test_entry_fill_metrics_cap_aggregate_wallet_snapshot_to_requested_size():
    payload = {
        "submission_intent": {
            "requested_shares": 9.89,
        },
        "provider_reconciliation": {
            "snapshot": {
                "status": "filled",
                "filled_size": 29.65,
                "average_fill_price": 0.9,
                "filled_notional_usd": 26.685,
            },
            "filled_size": 29.65,
            "average_fill_price": 0.9,
            "filled_notional_usd": 26.685,
        },
    }

    filled_notional, filled_size, fill_price = _extract_live_fill_metrics(payload)

    assert filled_size == pytest.approx(9.89)
    assert filled_notional == pytest.approx(8.901)
    assert fill_price == 0.9


def test_entry_fill_metrics_preserve_canonical_wallet_aggregate_size():
    payload = {
        "submission_intent": {
            "requested_shares": 9.89,
        },
        "wallet_position_aggregate_authority": {
            "aggregate_size": 29.65,
            "aggregate_notional_usd": 26.685,
        },
        "provider_reconciliation": {
            "snapshot": {
                "status": "filled",
                "filled_size": 29.65,
                "average_fill_price": 0.9,
                "filled_notional_usd": 26.685,
            },
            "filled_size": 29.65,
            "average_fill_price": 0.9,
            "filled_notional_usd": 26.685,
        },
    }

    filled_notional, filled_size, fill_price = _extract_live_fill_metrics(payload)

    assert filled_size == pytest.approx(29.65)
    assert filled_notional == pytest.approx(26.685)
    assert fill_price == 0.9


def test_live_position_notional_cap_uses_tightest_positive_limit():
    cap = _live_position_notional_cap(
        {
            "max_position_notional_usd": 20.0,
            "max_per_market_exposure_usd": 10.0,
        }
    )

    assert cap == 10.0


def test_risk_position_cap_exit_uses_rapid_close_execution():
    assert _is_rapid_close_trigger("risk_position_cap")


def test_wallet_trade_history_recovery_is_entry_fill_authority():
    row = SimpleNamespace(
        provider_clob_order_id="0xabc",
        provider_order_id=None,
        verification_status="venue_order",
    )
    payload = {
        "provider_reconciliation": {"filled_size": 0.0, "filled_notional_usd": 0.0},
        "entry_fill_recovery": {
            "source": "wallet_trade_history",
            "size": 4.0,
            "price": 0.75,
        },
    }

    assert _row_has_wallet_trade_fill_authority(row, payload)


def test_wallet_activity_close_must_cover_entry_size():
    row = SimpleNamespace(
        notional_usd=18.08374,
        entry_price=0.877,
        effective_price=0.877,
    )
    payload = {
        "provider_reconciliation": {
            "filled_size": 20.62,
            "average_fill_price": 0.877,
            "filled_notional_usd": 18.08374,
        },
    }
    position_close = {
        "price_source": "wallet_activity",
        "close_trigger": "wallet_activity",
        "filled_size": 7.26,
        "wallet_activity_size": 7.26,
    }

    assert _position_close_underfills_entry(row, payload, position_close)


def test_full_wallet_activity_close_can_terminal_entry():
    row = SimpleNamespace(
        notional_usd=18.08374,
        entry_price=0.877,
        effective_price=0.877,
    )
    payload = {
        "provider_reconciliation": {
            "filled_size": 20.62,
            "average_fill_price": 0.877,
            "filled_notional_usd": 18.08374,
        },
    }
    position_close = {
        "price_source": "wallet_activity",
        "close_trigger": "wallet_activity",
        "filled_size": 20.62,
        "wallet_activity_size": 20.62,
    }

    assert not _position_close_underfills_entry(row, payload, position_close)


def test_closed_live_trade_without_close_fill_authority_requires_repair():
    row = SimpleNamespace(
        status="closed_loss",
        notional_usd=10.0,
        entry_price=0.5,
        effective_price=0.5,
    )
    payload = {
        "provider_reconciliation": {
            "filled_size": 20.0,
            "average_fill_price": 0.5,
            "filled_notional_usd": 10.0,
        },
        "pending_live_exit": {
            "status": "failed",
            "exit_size": 20.0,
        },
    }
    position_close = {
        "price_source": "clob_midpoint",
        "close_trigger": "strategy:exit",
        "close_price": 0.25,
    }

    assert _position_close_missing_terminal_fill_authority(row, payload, position_close)


def test_closed_live_trade_with_provider_fill_authority_does_not_repair():
    row = SimpleNamespace(
        status="closed_win",
        notional_usd=10.0,
        entry_price=0.5,
        effective_price=0.5,
    )
    payload = {
        "provider_reconciliation": {
            "filled_size": 20.0,
            "average_fill_price": 0.5,
            "filled_notional_usd": 10.0,
        },
        "pending_live_exit": {
            "status": "filled",
            "filled_size": 20.0,
            "average_fill_price": 0.75,
            "provider_status": "filled",
        },
    }
    position_close = {
        "price_source": "provider_exit_fill",
        "close_trigger": "strategy:exit",
        "close_price": 0.75,
        "filled_size": 20.0,
    }

    assert not _position_close_missing_terminal_fill_authority(row, payload, position_close)


def test_worker_manual_mark_to_market_exit_is_fallback_exit():
    assert _pending_exit_is_live_fallback_manual_exit(
        {
            "close_trigger": "manual_mark_to_market",
            "reason": "circuit_breaker_safe_exit",
        }
    )


def test_strategy_exit_is_not_fallback_manual_exit():
    assert not _pending_exit_is_live_fallback_manual_exit(
        {
            "close_trigger": "strategy:Smart take profit",
            "reason": "reconciliation_worker",
        }
    )


def test_identical_verification_does_not_refresh_verified_at():
    verified_at = object()
    row = SimpleNamespace(
        verification_status="venue_order",
        verification_source="live_order_ack",
        verification_reason="filled",
        verified_at=verified_at,
        provider_order_id=None,
        provider_clob_order_id=None,
        execution_wallet_address=None,
        verification_tx_hash=None,
    )

    changed = apply_trader_order_verification(
        row,
        verification_status="venue_order",
        verification_source="live_order_ack",
        verification_reason="filled",
        verified_at=object(),
    )

    assert not changed
    assert row.verified_at is verified_at


def test_verification_event_append_dedupes_same_session_evidence():
    class FakeSession:
        def __init__(self):
            self.info = {}
            self.rows = []

        def add(self, row):
            self.rows.append(row)

    session = FakeSession()

    first = append_trader_order_verification_event(
        session,
        trader_order_id="order-1",
        verification_status="venue_order",
        source="live_order_ack",
        event_type="authority_adopted_existing",
        provider_clob_order_id="0xabc",
    )
    second = append_trader_order_verification_event(
        session,
        trader_order_id="order-1",
        verification_status="venue_order",
        source="live_order_ack",
        event_type="authority_adopted_existing",
        provider_clob_order_id="0xabc",
    )

    assert second is first
    assert len(session.rows) == 1
