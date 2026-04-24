from datetime import timedelta

from utils.utcnow import utcnow
from workers.trader_orchestrator_worker import _live_authority_pending_entry_blocks_market


def test_submit_timeout_failed_order_temporarily_blocks_same_market_entry():
    assert _live_authority_pending_entry_blocks_market(
        "failed",
        {"submission_intent": {"state": "submit_timeout"}},
        utcnow(),
    )


def test_old_submit_timeout_does_not_block_same_market_entry():
    assert not _live_authority_pending_entry_blocks_market(
        "failed",
        {"submission_intent": {"state": "submit_timeout"}},
        utcnow() - timedelta(hours=2),
    )


def test_plain_failed_order_does_not_block_same_market_entry():
    assert not _live_authority_pending_entry_blocks_market(
        "failed",
        {"submission_intent": {"state": "submit_failed"}},
        utcnow(),
    )
