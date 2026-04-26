"""Tests for the deterministic fast-lane idempotency key helper.

The key derivation must be:
- Stable across calls for the same (trader_id, signal_id) pair.
- Different across pairs (collision-resistant up to a sane probability).
- The right shape for ``OrderArgsV2.metadata`` (0x + 64 hex chars).
- Robust to whitespace / case in inputs (since signals can flow through
  layers that may normalize strings differently).
"""

from __future__ import annotations

from services.trader_orchestrator.fast_idempotency import (
    derive_fast_idempotency_key,
    is_fast_idempotency_key,
    normalize_metadata_for_match,
)


def test_key_is_deterministic_for_same_inputs():
    a = derive_fast_idempotency_key(trader_id="trader-1", signal_id="sig-abc")
    b = derive_fast_idempotency_key(trader_id="trader-1", signal_id="sig-abc")
    assert a == b


def test_key_differs_across_inputs():
    a = derive_fast_idempotency_key(trader_id="trader-1", signal_id="sig-abc")
    b = derive_fast_idempotency_key(trader_id="trader-1", signal_id="sig-def")
    c = derive_fast_idempotency_key(trader_id="trader-2", signal_id="sig-abc")
    assert a != b
    assert a != c
    assert b != c


def test_key_is_normalized_against_whitespace_and_case():
    a = derive_fast_idempotency_key(trader_id="Trader-1", signal_id="SIG-abc")
    b = derive_fast_idempotency_key(trader_id="  trader-1 ", signal_id="sig-abc")
    assert a == b


def test_key_shape_matches_bytes32():
    key = derive_fast_idempotency_key(trader_id="t", signal_id="s")
    assert key.startswith("0x")
    assert len(key) == 66  # "0x" + 64 hex chars
    int(key[2:], 16)  # parses cleanly


def test_missing_inputs_return_zero_bytes32():
    zero = "0x" + ("0" * 64)
    assert derive_fast_idempotency_key(trader_id="", signal_id="x") == zero
    assert derive_fast_idempotency_key(trader_id="x", signal_id="") == zero
    assert derive_fast_idempotency_key(trader_id=None, signal_id=None) == zero  # type: ignore[arg-type]


def test_is_fast_idempotency_key_accepts_real_keys_and_rejects_zero():
    key = derive_fast_idempotency_key(trader_id="t", signal_id="s")
    assert is_fast_idempotency_key(key) is True
    assert is_fast_idempotency_key("0x" + "0" * 64) is False  # zero
    assert is_fast_idempotency_key("0xfoo") is False
    assert is_fast_idempotency_key(None) is False
    assert is_fast_idempotency_key("") is False
    assert is_fast_idempotency_key("not-hex") is False


def test_normalize_metadata_for_match_handles_prefix_and_case():
    raw = "ABCDEF" + "0" * 58
    expected = "0x" + ("abcdef" + "0" * 58)
    assert normalize_metadata_for_match(raw) == expected
    assert normalize_metadata_for_match(f"0x{raw}") == expected
    # Length mismatch returns empty so the caller doesn't false-match.
    assert normalize_metadata_for_match("0xabc") == ""
    assert normalize_metadata_for_match(None) == ""
    assert normalize_metadata_for_match("not-hex" + "0" * 58) == ""
