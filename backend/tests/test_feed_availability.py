import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from utils.feed_availability import FeedAvailability, FeedStatus


def test_feed_availability_latches_when_creds_missing():
    creds = {"api_key": ""}
    avail = FeedAvailability(
        has_credentials=lambda: bool(creds["api_key"]),
        name="test-feed",
    )

    # Missing creds: check() returns False and latches.
    assert avail.check() is False
    assert avail.is_disabled is True
    assert avail.disabled_reason == "missing_credentials"
    assert avail.status() == FeedStatus.DISABLED

    # Once latched, subsequent check() does not re-evaluate even if
    # credentials become available — caller must explicitly rearm().
    creds["api_key"] = "secret"
    assert avail.check() is False
    assert avail.is_disabled is True

    # Rearm: latch cleared, next check() re-evaluates.
    avail.rearm()
    assert avail.is_disabled is False
    assert avail.check() is True
    assert avail.is_disabled is False


def test_feed_availability_latches_on_auth_failure():
    avail = FeedAvailability(
        has_credentials=lambda: True,
        name="test-feed",
    )
    assert avail.check() is True

    avail.latch_auth_failure(reason="bad_signature")
    assert avail.is_disabled is True
    assert avail.disabled_reason == "bad_signature"
    assert avail.check() is False

    avail.rearm()
    assert avail.check() is True
