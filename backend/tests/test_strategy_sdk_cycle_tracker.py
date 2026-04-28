"""Tests for the CycleTracker idempotent-milestone helper.

CycleTracker is for fixed-cycle recurring markets (5m/15m/1h crypto
over-under). Each instance tracks one market; the cycle is identified
by the market's resolution timestamp so cycle rollovers self-detect.

These tests import the helper directly to avoid the in-flight strategy
SDK refactor's circular import.
"""

from __future__ import annotations

import pytest

from services.strategy_helpers.cycle_tracker import CycleTracker


# Helper constants — a single 5-minute cycle ending at this epoch-ms.
END_MS = 2_000_000_000_000  # arbitrary far-future timestamp
CYCLE_MS = 300_000  # 5 minutes
START_MS = END_MS - CYCLE_MS


def test_constructor_rejects_non_positive_cycle():
    with pytest.raises(ValueError):
        CycleTracker(cycle_seconds=0.0, milestones_s=())
    with pytest.raises(ValueError):
        CycleTracker(cycle_seconds=-30.0, milestones_s=())


def test_constructor_rejects_milestones_outside_cycle():
    with pytest.raises(ValueError):
        CycleTracker(cycle_seconds=300.0, milestones_s=(-1.0,))
    with pytest.raises(ValueError):
        CycleTracker(cycle_seconds=300.0, milestones_s=(301.0,))


def test_constructor_sorts_milestones():
    t = CycleTracker(cycle_seconds=300.0, milestones_s=(150.0, 0.0, 60.0))
    assert t.milestones_s == (0.0, 60.0, 150.0)


def test_crossed_returns_empty_before_cycle_starts():
    t = CycleTracker(cycle_seconds=300.0, milestones_s=(0.0, 150.0))
    # 1 second before cycle start
    assert t.crossed(END_MS, now_ms=START_MS - 1_000) == []


def test_crossed_fires_open_at_cycle_start():
    t = CycleTracker(cycle_seconds=300.0, milestones_s=(0.0, 150.0))
    assert t.crossed(END_MS, now_ms=START_MS) == [0.0]


def test_crossed_fires_midcycle_at_150s():
    t = CycleTracker(cycle_seconds=300.0, milestones_s=(0.0, 150.0))
    t.crossed(END_MS, now_ms=START_MS)
    assert t.crossed(END_MS, now_ms=START_MS + 150_000) == [150.0]


def test_crossed_is_idempotent_within_cycle():
    """Calling crossed() repeatedly past the same milestone fires once."""
    t = CycleTracker(cycle_seconds=300.0, milestones_s=(0.0, 150.0))
    assert t.crossed(END_MS, now_ms=START_MS + 200_000) == [0.0, 150.0]
    # subsequent calls at any later time should return nothing
    assert t.crossed(END_MS, now_ms=START_MS + 200_001) == []
    assert t.crossed(END_MS, now_ms=END_MS - 1) == []


def test_crossed_returns_all_passed_milestones_on_first_call():
    """If multiple milestones already crossed when first called, all return."""
    t = CycleTracker(cycle_seconds=300.0, milestones_s=(0.0, 150.0, 250.0))
    out = t.crossed(END_MS, now_ms=START_MS + 260_000)
    assert out == [0.0, 150.0, 250.0]


def test_cycle_rollover_resets_fired_set():
    t = CycleTracker(cycle_seconds=300.0, milestones_s=(0.0, 150.0))
    t.crossed(END_MS, now_ms=START_MS + 200_000)  # fires both
    assert t.crossed(END_MS, now_ms=START_MS + 250_000) == []  # idempotent

    next_end = END_MS + CYCLE_MS  # next 5-min cycle
    next_start = next_end - CYCLE_MS  # = END_MS
    # Rollover — same tracker, new cycle ID
    assert t.crossed(next_end, now_ms=next_start) == [0.0]
    assert t.crossed(next_end, now_ms=next_start + 150_000) == [150.0]


def test_phase_returns_clamped_fields():
    t = CycleTracker(cycle_seconds=300.0, milestones_s=(0.0,))

    p_before = t.phase(END_MS, now_ms=START_MS - 60_000)
    assert p_before["elapsed_s"] == 0.0
    assert p_before["remaining_s"] == 360.0
    assert p_before["fraction"] == 0.0

    p_mid = t.phase(END_MS, now_ms=START_MS + 150_000)
    assert p_mid["elapsed_s"] == pytest.approx(150.0, rel=1e-9)
    assert p_mid["remaining_s"] == pytest.approx(150.0, rel=1e-9)
    assert p_mid["fraction"] == pytest.approx(0.5, rel=1e-9)

    p_after = t.phase(END_MS, now_ms=END_MS + 60_000)
    assert p_after["elapsed_s"] == pytest.approx(360.0, rel=1e-9)
    assert p_after["remaining_s"] == 0.0
    assert p_after["fraction"] == 1.0


def test_reset_clears_fired_state():
    t = CycleTracker(cycle_seconds=300.0, milestones_s=(0.0, 150.0))
    t.crossed(END_MS, now_ms=START_MS + 200_000)
    t.reset()
    assert t.crossed(END_MS, now_ms=START_MS + 200_000) == [0.0, 150.0]


def test_milestones_only_at_zero_fires_only_at_open():
    """Strategies that only care about cycle open use milestones_s=(0.0,)."""
    t = CycleTracker(cycle_seconds=300.0, milestones_s=(0.0,))
    assert t.crossed(END_MS, now_ms=START_MS - 1_000) == []
    assert t.crossed(END_MS, now_ms=START_MS) == [0.0]
    assert t.crossed(END_MS, now_ms=START_MS + 150_000) == []


def test_no_milestones_never_fires():
    t = CycleTracker(cycle_seconds=300.0, milestones_s=())
    assert t.crossed(END_MS, now_ms=START_MS) == []
    assert t.crossed(END_MS, now_ms=START_MS + 200_000) == []


def test_milestone_at_cycle_end_fires_at_resolution():
    t = CycleTracker(cycle_seconds=300.0, milestones_s=(300.0,))
    assert t.crossed(END_MS, now_ms=END_MS - 1_000) == []
    assert t.crossed(END_MS, now_ms=END_MS) == [300.0]
