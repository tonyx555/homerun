"""Unit tests for the CLOB metadata sanitizer helpers.

Production logs showed ``non-hexadecimal number found in fromhex() arg
at position 43`` repeating every cycle for a live trader: the
orchestrator path was forwarding ``leg["metadata"]`` (often a Python
dict carrying ExecutionPlan bookkeeping) straight into
``OrderArgsV2.metadata``. The CLOB SDK then ran
``bytes.fromhex(value.replace("0x", "").zfill(64))`` on the
stringified dict and crashed every order.

The fix is two-layered:

* ``order_manager._clob_metadata_from_leg`` filters non-string leg
  metadata at the source.  Dicts here represent ExecutionPlan
  bookkeeping that was never meant for the venue (the fast-tier path
  explicitly overwrites the field with a hex idempotency key when it
  wants one), so dropping them returns the orchestrator path to the
  pre-fast-tier baseline of "no metadata, SDK uses BYTES32_ZERO".
* ``live_execution_service._normalize_clob_metadata`` validates that
  any string that did make it through is a real 0x-prefixed bytes32
  hex.  The boundary in ``place_order`` REFUSES the submission when
  this fails — see ``test_place_order_refuses_malformed_metadata_
  without_submitting`` in ``test_trading_service_safety`` for that
  contract.  Submitting without the idempotency key would leave a
  venue order undiscoverable by the reconcile sweep if the
  post-submit DB flush was lost; for a real-money path the safer
  behaviour is to fail loud.

This file pins the pure helpers; the integration test for the
fail-loud contract lives in ``test_trading_service_safety``.
"""

from __future__ import annotations

import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services.live_execution_service import _normalize_clob_metadata
from services.trader_orchestrator.order_manager import _clob_metadata_from_leg


def test_normalize_clob_metadata_accepts_proper_bytes32():
    key = "0x" + "abcdef" * 10 + "1234"
    assert _normalize_clob_metadata(key) == key


def test_normalize_clob_metadata_left_pads_short_hex():
    # The CLOB SDK does the same zfill internally, so a short hex value
    # is technically valid; we mirror that so the round-trip matches.
    assert _normalize_clob_metadata("0xabc") == "0x" + ("0" * 61) + "abc"
    assert _normalize_clob_metadata("abc") == "0x" + ("0" * 61) + "abc"


def test_normalize_clob_metadata_drops_dict_string():
    # The actual production bug — a Python dict gets stringified somewhere
    # upstream and lands here.
    assert _normalize_clob_metadata("{'fast_tier': True}") is None
    assert _normalize_clob_metadata("{'market_coverage': {'scope': 'event'}}") is None


def test_normalize_clob_metadata_drops_other_garbage():
    assert _normalize_clob_metadata("hello") is None
    assert _normalize_clob_metadata("0xZZZZ" + "0" * 60) is None
    assert _normalize_clob_metadata("0x" + "f" * 65) is None  # too long
    assert _normalize_clob_metadata(":not:hex:") is None


def test_normalize_clob_metadata_treats_empty_as_none():
    assert _normalize_clob_metadata(None) is None
    assert _normalize_clob_metadata("") is None
    assert _normalize_clob_metadata("   ") is None


def test_normalize_clob_metadata_handles_non_str_inputs():
    # Defensive: in case some upstream caller passes a number / bytes /
    # whatever, we coerce via str() first.
    assert _normalize_clob_metadata(123) == "0x" + ("0" * 61) + "123"
    assert _normalize_clob_metadata([1, 2, 3]) is None  # str repr is "[1, 2, 3]"


def test_clob_metadata_from_leg_accepts_string_idempotency_key():
    key = "0x" + "ab" * 32
    assert _clob_metadata_from_leg({"metadata": key}) == key


def test_clob_metadata_from_leg_drops_dict_metadata():
    # The real-world failure: leg["metadata"] is a dict carrying
    # ExecutionPlan bookkeeping, never meant for the venue.
    assert _clob_metadata_from_leg({"metadata": {"market_coverage": {"scope": "event"}}}) is None
    assert _clob_metadata_from_leg({"metadata": {"fast_tier": True}}) is None
    assert _clob_metadata_from_leg({"metadata": {}}) is None


def test_clob_metadata_from_leg_handles_missing_or_empty():
    assert _clob_metadata_from_leg({}) is None
    assert _clob_metadata_from_leg({"metadata": None}) is None
    assert _clob_metadata_from_leg({"metadata": ""}) is None
    assert _clob_metadata_from_leg({"metadata": "   "}) is None
