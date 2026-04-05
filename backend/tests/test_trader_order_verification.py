import sys
from pathlib import Path


BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services.trader_order_verification import (
    TRADER_ORDER_VERIFICATION_DISPUTED,
    TRADER_ORDER_VERIFICATION_SUMMARY_ONLY,
    TRADER_ORDER_VERIFICATION_VENUE_FILL,
    TRADER_ORDER_VERIFICATION_VENUE_ORDER,
    TRADER_ORDER_VERIFICATION_WALLET_ACTIVITY,
    derive_trader_order_verification,
)


def test_summary_only_close_without_lineage_is_hidden_summary():
    payload = {
        "position_close": {
            "price_source": "closed_positions_api",
            "close_trigger": "wallet_summary_recovery",
        },
    }

    derived = derive_trader_order_verification(
        mode="live",
        status="closed_loss",
        reason="Tail carry summary close",
        payload=payload,
    )

    assert derived["verification_status"] == TRADER_ORDER_VERIFICATION_SUMMARY_ONLY
    assert derived["verification_source"] == "closed_positions_api"
    assert derived["verification_reason"] == "summary_only_close_recovery"


def test_summary_only_close_keeps_real_order_lineage_visible():
    payload = {
        "provider_clob_order_id": "0xabc123",
        "provider_reconciliation": {
            "snapshot_status": "filled",
            "filled_size": 12.0,
        },
        "position_close": {
            "price_source": "closed_positions_api",
            "close_trigger": "wallet_summary_recovery",
        },
    }

    derived = derive_trader_order_verification(
        mode="live",
        status="closed_win",
        reason="Tail carry close",
        payload=payload,
    )

    assert derived["verification_status"] == TRADER_ORDER_VERIFICATION_VENUE_FILL
    assert derived["provider_clob_order_id"] == "0xabc123"
    assert derived["verification_reason"] == "summary_only_close_recovery"


def test_recovered_summary_only_row_without_order_ids_is_disputed():
    payload = {
        "execution_wallet_address": "0xabc",
        "position_close": {
            "price_source": "closed_positions_api",
            "close_trigger": "wallet_summary_recovery",
        },
    }

    derived = derive_trader_order_verification(
        mode="live",
        status="resolved_loss",
        reason="Recovered from live venue authority",
        payload=payload,
    )

    assert derived["verification_status"] == TRADER_ORDER_VERIFICATION_DISPUTED
    assert derived["verification_reason"] == "recovered_row_without_order_lineage"


def test_wallet_activity_close_uses_wallet_activity_authority():
    payload = {
        "execution_wallet_address": "0xabc",
        "position_close": {
            "price_source": "wallet_activity",
            "close_trigger": "wallet_activity",
            "wallet_activity_transaction_hash": "0xtx",
        },
    }

    derived = derive_trader_order_verification(
        mode="live",
        status="resolved_win",
        reason="External wallet close",
        payload=payload,
    )

    assert derived["verification_status"] == TRADER_ORDER_VERIFICATION_WALLET_ACTIVITY
    assert derived["verification_source"] == "wallet_activity_api"
    assert derived["verification_tx_hash"] == "0xtx"


def test_provider_ack_without_fill_keeps_venue_order_authority():
    payload = {
        "provider_order_id": "live-order-123",
    }

    derived = derive_trader_order_verification(
        mode="live",
        status="submitted",
        reason="Selected",
        payload=payload,
    )

    assert derived["verification_status"] == TRADER_ORDER_VERIFICATION_VENUE_ORDER
    assert derived["provider_order_id"] == "live-order-123"
