"""Deterministic idempotency keys for CLOB order submission.

Both the fast and orchestrator submission paths face the same crash
survivability question: the DB row and the venue order live in
different stores, so a process kill between (a) the venue accepting
the order and (b) the post-submit DB update writing back the
``provider_clob_order_id`` leaves the row in a ``placing`` state with
no link to its venue counterpart.

The pre-submit row pattern (see ``fast_submit.execute_fast_signal``
and ``session_engine._commit_pre_submit_projection``) ensures the row
ALWAYS exists before the venue is touched, so the system never loses
track of intent. What it cannot guarantee is the precise mapping back
to the venue order if the post-submit flush is lost — a fuzzy match
on ``(token_id, side, price, size, timestamp)`` is unreliable.

To get exact reconcile precision we tag every CLOB submission with a
deterministic ``bytes32`` key derived from
``(trader_id, signal_id, leg_id)``. The reconcile sweep then queries
the venue's open orders, reads each one's ``metadata`` field, and
matches it back to the pre-submit ``TraderOrder`` row that holds the
same key in its payload. Once matched, the existing reconcile flow
patches ``provider_clob_order_id`` and normal status-syncing takes
over.

The key is stable across retries: any retry computes the same bytes,
so even if a downstream layer ever did dedup by metadata it would
correctly reject the second attempt. Polymarket's CLOB itself does
*not* (currently) appear to dedup by metadata — this is purely a
discovery key for our reconcile path.

The ``leg_id`` parameter distinguishes legs in a multi-leg execution
plan (orchestrator path); the fast path has exactly one leg per signal
and passes an empty ``leg_id``, preserving the pre-existing key shape.
"""

from __future__ import annotations

import hashlib
from typing import Final

# Same shape as ``py_clob_client_v2.constants.BYTES32_ZERO`` (0x + 64 hex
# chars). The CLOB SDK validates that ``metadata`` is a hex string of this
# length, so we must always emit exactly 32 bytes encoded the same way.
_BYTES32_PREFIX: Final[str] = "0x"
_BYTES32_HEX_LEN: Final[int] = 64

# Versioned namespace lets us evolve the derivation without colliding with
# previously-issued keys. Bump if the input shape ever changes.
_KEY_NAMESPACE: Final[str] = "homerun:fast-idempotency:v1"


def derive_clob_idempotency_key(
    *,
    trader_id: str,
    signal_id: str,
    leg_id: str = "",
) -> str:
    """Return the deterministic bytes32 metadata key for a CLOB submission.

    Args:
        trader_id: Owning trader.
        signal_id: Source signal that produced the execution intent.
        leg_id: Leg identifier within the execution plan. Empty string
            for the fast-tier single-leg case (preserves the original
            ``derive_fast_idempotency_key`` key shape so existing
            in-flight orders continue to match on reconcile).

    Returns ``BYTES32_ZERO`` for missing trader/signal inputs so
    callers don't have to branch — passing the zero key just disables
    venue-side discovery for that submission, which falls back to the
    fuzzy attribute-match reconcile baseline.
    """
    trader_norm = str(trader_id or "").strip().lower()
    signal_norm = str(signal_id or "").strip().lower()
    leg_norm = str(leg_id or "").strip().lower()
    if not trader_norm or not signal_norm:
        return _BYTES32_PREFIX + ("0" * _BYTES32_HEX_LEN)
    if leg_norm:
        payload = f"{_KEY_NAMESPACE}|{trader_norm}|{signal_norm}|{leg_norm}".encode("utf-8")
    else:
        # Backward-compatible single-leg shape: matches keys issued by
        # ``derive_fast_idempotency_key`` before the leg_id parameter
        # existed, so reconcile can still attach to in-flight fast-path
        # orders submitted under the prior derivation.
        payload = f"{_KEY_NAMESPACE}|{trader_norm}|{signal_norm}".encode("utf-8")
    digest = hashlib.sha256(payload).digest()
    return _BYTES32_PREFIX + digest.hex()


def derive_fast_idempotency_key(
    *,
    trader_id: str,
    signal_id: str,
) -> str:
    """Backward-compatible alias for the fast-tier single-leg case.

    Kept as a thin wrapper because tests and the fast-submit path call
    it by name; new code should call ``derive_clob_idempotency_key``
    directly so the leg_id is explicit at the call site.
    """
    return derive_clob_idempotency_key(trader_id=trader_id, signal_id=signal_id, leg_id="")


def is_fast_idempotency_key(value: str | None) -> bool:
    """Cheap shape check for ``derive_fast_idempotency_key`` outputs.

    Used by the reconcile sweep to skip venue orders whose metadata is
    obviously not one of ours (e.g. zero, missing, or a different size).
    A True result does not prove the key was issued by us — only the full
    DB lookup does — but it lets us prune obviously-irrelevant orders
    before the per-key comparison.
    """
    if not isinstance(value, str):
        return False
    text = value.strip()
    if not text.startswith(_BYTES32_PREFIX):
        return False
    body = text[len(_BYTES32_PREFIX):]
    if len(body) != _BYTES32_HEX_LEN:
        return False
    if all(ch == "0" for ch in body):
        return False
    try:
        int(body, 16)
    except ValueError:
        return False
    return True


def normalize_metadata_for_match(value: str | None) -> str:
    """Normalize a venue ``metadata`` field for equality comparisons.

    Polymarket may return the field with or without the ``0x`` prefix and
    in mixed case; collapse both before matching against an issued key.
    """
    if not value:
        return ""
    text = str(value).strip().lower()
    if not text:
        return ""
    if text.startswith(_BYTES32_PREFIX):
        text = text[len(_BYTES32_PREFIX):]
    if len(text) != _BYTES32_HEX_LEN:
        return ""
    try:
        int(text, 16)
    except ValueError:
        return ""
    return _BYTES32_PREFIX + text
