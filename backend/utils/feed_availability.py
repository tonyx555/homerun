"""Feed availability helper for credential-dependent external data feeds.

Pattern (borrowed from UpDownWalletMonitor's chainlink-prices-stream route):

    A feed that requires API credentials should NOT enter its connect-loop
    when those credentials are missing. Doing so produces a 401-storm against
    the upstream and floods logs with retries. Instead the feed should:

        1. Check credentials once at start.
        2. If missing, emit a single structured log line, set status to
           "disabled (missing credentials)", and stop. No connect loop.
        3. On the first 401 mid-flight, do the same — latch disabled and
           cancel the reconnect schedule.
        4. Expose a ``rearm()`` method so the settings-update path can
           re-trigger the feed once credentials arrive.

This module provides the latch + status enum. Apply it in feed services
that wrap a credentialed upstream (Chainlink Data Streams, paid news APIs,
etc.). Public feeds (Binance public WS, Polymarket RTDS) don't need it.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Callable, Optional


class FeedStatus(str, Enum):
    UNINITIALIZED = "uninitialized"
    CONNECTING = "connecting"
    HEALTHY = "healthy"
    PARTIAL = "partial"
    UNAVAILABLE = "unavailable"
    ERROR = "error"
    # Latched: missing credentials or persistent auth failure. The feed
    # is intentionally not reconnecting. Distinct from ``ERROR`` so the
    # frontend can render "Disabled" (gray) vs "Offline" (red).
    DISABLED = "disabled"


@dataclass
class FeedAvailability:
    """Latches whether a credentialed feed is permitted to connect.

    Construct with a credential-check callable that returns True iff the
    feed has all required secrets. Call ``check()`` before starting the
    connect loop and after any auth failure. Call ``rearm()`` from the
    settings-update handler when credentials change.
    """

    has_credentials: Callable[[], bool]
    name: str
    _disabled: bool = False
    _disabled_reason: Optional[str] = None

    def check(self) -> bool:
        """Return True if the feed may attempt to connect.

        Latches disabled with reason ``missing_credentials`` if no creds
        are present. Once latched, subsequent calls return False without
        re-checking until ``rearm()`` is invoked.
        """
        if self._disabled:
            return False
        if not self.has_credentials():
            self._disabled = True
            self._disabled_reason = "missing_credentials"
            return False
        return True

    def latch_auth_failure(self, reason: str = "auth_failed") -> None:
        """Mark the feed disabled after an upstream rejected the credentials.

        Use this on persistent 401/403 from the upstream — distinct from a
        transient 5xx where reconnect is appropriate.
        """
        self._disabled = True
        self._disabled_reason = reason

    def rearm(self) -> None:
        """Clear the disabled latch so the next ``check()`` re-evaluates.

        Call from settings-update / credential-change handlers.
        """
        self._disabled = False
        self._disabled_reason = None

    @property
    def is_disabled(self) -> bool:
        return self._disabled

    @property
    def disabled_reason(self) -> Optional[str]:
        return self._disabled_reason

    def status(self) -> FeedStatus:
        return FeedStatus.DISABLED if self._disabled else FeedStatus.UNINITIALIZED
