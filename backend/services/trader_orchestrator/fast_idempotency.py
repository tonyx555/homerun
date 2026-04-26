"""Deterministic idempotency keys for the fast-lane CLOB submission path.

The fast path's primary crash-survivability mechanism is a pre-submit
``TraderOrder`` skeleton row (see ``fast_submit.execute_fast_signal``).
That covers the ``DB row exists, cursor wasn't advanced`` case via the
duplicate-check guard, but it does not cover the rarer ``CLOB order
placed, DB row never written`` case — if the process is killed between
``submit_execution_leg`` returning success and ``session.flush()``, the
venue holds an order that no DB row points at.

To close that gap we tag every fast-tier order with a deterministic
``bytes32`` metadata key derived from ``trader_id + signal_id``, sent
to Polymarket via ``OrderArgsV2.metadata``. The reconcile sweep then
queries the venue's open orders, reads each one's ``metadata`` field,
and matches it back to the orphan ``TraderOrder`` row that holds the
same key. Once matched, the existing reconcile flow patches
``provider_clob_order_id`` and lets normal status-syncing take over.

The key is stable across retries: any retry computes the same bytes,
so even if a downstream layer ever did dedup by metadata it would
correctly reject the second attempt. Polymarket's CLOB itself does
*not* (currently) appear to dedup by metadata — this is purely a
discovery key for our reconcile path.
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


def derive_fast_idempotency_key(
    *,
    trader_id: str,
    signal_id: str,
) -> str:
    """Return the deterministic bytes32 metadata key for ``(trader, signal)``.

    Returns ``BYTES32_ZERO`` for missing inputs so callers don't have to
    branch — passing the zero key just disables venue-side discovery for
    that submission, which is no worse than the pre-v2 behavior.
    """
    trader_norm = str(trader_id or "").strip().lower()
    signal_norm = str(signal_id or "").strip().lower()
    if not trader_norm or not signal_norm:
        return _BYTES32_PREFIX + ("0" * _BYTES32_HEX_LEN)
    payload = f"{_KEY_NAMESPACE}|{trader_norm}|{signal_norm}".encode("utf-8")
    digest = hashlib.sha256(payload).digest()
    return _BYTES32_PREFIX + digest.hex()


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
