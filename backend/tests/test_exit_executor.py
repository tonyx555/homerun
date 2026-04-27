"""Tests for the orchestrator's exit_executor (laddered exit primitives).

Covers the pure planner, child aggregation, completion checks, and the
async submission/escalation/reprice helpers (with a fake place_order to
keep tests hermetic).
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

import pytest

from services.strategies.base import (
    BaseStrategy,
    EscalationSpec,
    ExitDecision,
    ExitPolicy,
    LadderSpec,
)
from services.strategy_sdk import StrategySDK
from services.trader_orchestrator import exit_executor


# ── Planner tests ─────────────────────────────────────────────────────────


class TestPlannerLadder:
    def test_uniform_ladder_no_chunking(self):
        policy = ExitPolicy(ladder=LadderSpec(levels=5, step_ticks=1, distribution="uniform"))
        plans = exit_executor.plan_children(
            target_size=50.0, trigger_price=0.50, side="SELL", policy=policy,
        )
        assert len(plans) == 5
        assert {round(p.price, 4) for p in plans} == {0.50, 0.49, 0.48, 0.47, 0.46}
        assert sum(p.size for p in plans) == pytest.approx(50.0)
        assert all(p.size == pytest.approx(10.0) for p in plans)

    def test_offset_ticks_pushes_inside_rung_below_trigger(self):
        policy = ExitPolicy(
            ladder=LadderSpec(levels=3, step_ticks=1, offset_ticks=3, distribution="uniform"),
        )
        plans = exit_executor.plan_children(
            target_size=30.0, trigger_price=0.50, side="SELL", policy=policy,
        )
        prices = sorted({round(p.price, 4) for p in plans}, reverse=True)
        assert prices[0] == 0.47  # inside-most rung sits 3 ticks below trigger
        assert prices[-1] == 0.45  # 3 levels: 0.47, 0.46, 0.45

    def test_buy_ladder_steps_above_trigger(self):
        policy = ExitPolicy(ladder=LadderSpec(levels=3, step_ticks=1, distribution="uniform"))
        plans = exit_executor.plan_children(
            target_size=30.0, trigger_price=0.50, side="BUY", policy=policy,
        )
        prices = sorted({round(p.price, 4) for p in plans})
        assert prices == [0.50, 0.51, 0.52]

    def test_chunked_back_loaded_emits_more_at_outer_levels(self):
        policy = ExitPolicy(
            ladder=LadderSpec(levels=5, step_ticks=1, distribution="back_loaded"),
            chunk_size=5.0,
        )
        plans = exit_executor.plan_children(
            target_size=50.0, trigger_price=0.50, side="SELL", policy=policy,
        )
        # back_loaded → most chunks at outer (lower) prices
        from collections import Counter
        buckets = Counter(p.distribution_bucket for p in plans)
        # outer levels have >= chunks of inner levels
        assert buckets["lvl4"] >= buckets["lvl0"]
        assert sum(p.size for p in plans) == pytest.approx(50.0)

    def test_chunked_front_loaded_emits_more_at_inner_levels(self):
        policy = ExitPolicy(
            ladder=LadderSpec(levels=5, step_ticks=1, distribution="front_loaded"),
            chunk_size=5.0,
        )
        plans = exit_executor.plan_children(
            target_size=50.0, trigger_price=0.50, side="SELL", policy=policy,
        )
        from collections import Counter
        buckets = Counter(p.distribution_bucket for p in plans)
        assert buckets["lvl0"] >= buckets["lvl4"]


class TestPlannerMinNotional:
    def test_low_price_collapses_to_single_when_no_ladder_fits(self):
        # At price 0.05, 2-contract chunks = $0.10 — well below $1 min.
        # Even bumped chunks of 20 only allow ~ 5 chunks across all levels.
        policy = ExitPolicy(
            ladder=LadderSpec(levels=10, step_ticks=1, distribution="uniform"),
            chunk_size=2.0,
        )
        plans = exit_executor.plan_children(
            target_size=100.0, trigger_price=0.05, side="SELL", policy=policy,
        )
        # Should be at least 1 child; total size preserved
        assert len(plans) >= 1
        assert sum(p.size for p in plans) == pytest.approx(100.0)
        # Every emitted child meets min notional
        for p in plans:
            assert p.price * p.size + 1e-9 >= 1.0

    def test_promotes_chunk_size_at_low_price_levels(self):
        # At trigger 0.10 with chunk_size=2, an order of 2 = $0.20 < $1 min.
        # The planner should bump chunk size up to >= 1/price = 10.
        policy = ExitPolicy(
            ladder=LadderSpec(levels=2, step_ticks=1, distribution="uniform"),
            chunk_size=2.0,
        )
        plans = exit_executor.plan_children(
            target_size=100.0, trigger_price=0.10, side="SELL", policy=policy,
        )
        assert len(plans) > 0
        for p in plans:
            assert p.price * p.size + 1e-9 >= 1.0

    def test_total_size_preserved_under_clamping(self):
        for price in [0.05, 0.10, 0.25, 0.50, 0.95]:
            policy = ExitPolicy(
                ladder=LadderSpec(levels=10, step_ticks=1, distribution="uniform"),
                chunk_size=2.0,
            )
            plans = exit_executor.plan_children(
                target_size=100.0, trigger_price=price, side="SELL", policy=policy,
            )
            assert sum(p.size for p in plans) == pytest.approx(
                100.0, rel=0.01
            ), f"price={price}"


class TestPlannerMaxChunks:
    def test_max_chunks_caps_total_count(self):
        policy = ExitPolicy(
            ladder=LadderSpec(levels=20, step_ticks=1, distribution="uniform"),
            chunk_size=1.0,
            max_chunks=10,
        )
        plans = exit_executor.plan_children(
            target_size=100.0, trigger_price=0.50, side="SELL", policy=policy,
        )
        assert len(plans) <= 10


class TestPlannerOrderTypeMix:
    def test_mix_assigns_aggressive_types_to_inside_rungs(self):
        policy = ExitPolicy(
            ladder=LadderSpec(levels=5, step_ticks=1, distribution="uniform"),
            chunk_size=10.0,
            order_type_mix=[("IOC", 0.4), ("GTC", 0.6)],
        )
        plans = exit_executor.plan_children(
            target_size=50.0, trigger_price=0.50, side="SELL", policy=policy,
        )
        ioc_indexes = [p.index for p in plans if p.tif == "IOC"]
        gtc_indexes = [p.index for p in plans if p.tif == "GTC"]
        # IOC should be assigned to the inside-most (lowest-index) children.
        if ioc_indexes and gtc_indexes:
            assert max(ioc_indexes) < min(gtc_indexes)
        # IOC children must not be post_only (they're takers).
        for p in plans:
            if p.tif == "IOC":
                assert p.post_only is False


class TestPlannerEdgeCases:
    def test_zero_target_returns_empty(self):
        policy = ExitPolicy(ladder=LadderSpec(levels=5, step_ticks=1))
        assert exit_executor.plan_children(
            target_size=0.0, trigger_price=0.50, side="SELL", policy=policy,
        ) == []

    def test_zero_price_returns_empty(self):
        policy = ExitPolicy(ladder=LadderSpec(levels=5, step_ticks=1))
        assert exit_executor.plan_children(
            target_size=50.0, trigger_price=0.0, side="SELL", policy=policy,
        ) == []

    def test_none_policy_returns_empty(self):
        assert exit_executor.plan_children(
            target_size=50.0, trigger_price=0.50, side="SELL", policy=None,
        ) == []

    def test_single_level_no_ladder(self):
        # Pure chunking, no laddering.
        policy = ExitPolicy(chunk_size=5.0)
        plans = exit_executor.plan_children(
            target_size=20.0, trigger_price=0.50, side="SELL", policy=policy,
        )
        assert len(plans) == 4
        assert all(round(p.price, 4) == 0.50 for p in plans)
        assert all(p.size == pytest.approx(5.0) for p in plans)


# ── Serialization roundtrip ───────────────────────────────────────────────


class TestPolicySerialization:
    def test_roundtrip_full_policy(self):
        policy = ExitPolicy(
            ladder=LadderSpec(levels=10, step_ticks=2, offset_ticks=3, distribution="back_loaded"),
            chunk_size=2.0,
            max_chunks=30,
            order_type_mix=[("IOC", 0.3), ("GTC", 0.7)],
            escalation=EscalationSpec(after_seconds=4.0, action="widen_bps", widen_bps=50, max_escalations=2),
            reprice_on_mid_drift_bps=80,
            min_chunk_notional_usd=1.0,
            min_reprice_interval_seconds=2.5,
        )
        snap = exit_executor.serialize_policy(policy)
        restored = exit_executor.deserialize_policy(snap)
        assert restored.ladder.levels == 10
        assert restored.ladder.step_ticks == 2
        assert restored.ladder.offset_ticks == 3
        assert restored.ladder.distribution == "back_loaded"
        assert restored.chunk_size == 2.0
        assert restored.max_chunks == 30
        assert restored.order_type_mix == [("IOC", 0.3), ("GTC", 0.7)]
        assert restored.escalation.after_seconds == 4.0
        assert restored.escalation.action == "widen_bps"
        assert restored.escalation.widen_bps == 50.0
        assert restored.escalation.max_escalations == 2
        assert restored.reprice_on_mid_drift_bps == 80.0
        assert restored.min_chunk_notional_usd == 1.0
        assert restored.min_reprice_interval_seconds == 2.5

    def test_empty_snapshot_returns_none(self):
        assert exit_executor.deserialize_policy(None) is None
        assert exit_executor.deserialize_policy({}) is None


# ── Aggregation / completion ──────────────────────────────────────────────


def _child_record(*, size, price, filled=0.0, status="planned", avg=None) -> dict:
    return {
        "child_id": f"c{int(price * 1000)}",
        "price": float(price),
        "size": float(size),
        "filled_size": float(filled),
        "average_fill_price": float(avg) if avg is not None else None,
        "status": status,
        "tif": "GTC",
        "post_only": True,
    }


class TestAggregation:
    def test_zero_fills(self):
        children = [_child_record(size=10, price=0.5) for _ in range(3)]
        agg = exit_executor.aggregate_children_fills(children)
        assert agg["filled_size"] == 0.0
        assert agg["filled_notional"] == 0.0
        assert agg["average_fill_price"] == 0.0

    def test_partial_fills_weighted_average(self):
        children = [
            _child_record(size=10, price=0.50, filled=10, avg=0.50, status="filled"),
            _child_record(size=10, price=0.49, filled=5, avg=0.49, status="partial"),
        ]
        agg = exit_executor.aggregate_children_fills(children)
        assert agg["filled_size"] == pytest.approx(15.0)
        assert agg["filled_notional"] == pytest.approx(10 * 0.50 + 5 * 0.49)
        assert agg["average_fill_price"] == pytest.approx(7.45 / 15.0)


class TestCompletion:
    def test_fully_filled_complete(self):
        pe = {
            "target_size": 30.0,
            "children": [
                _child_record(size=10, price=0.5, filled=10, avg=0.5, status="filled"),
                _child_record(size=10, price=0.5, filled=10, avg=0.5, status="filled"),
                _child_record(size=10, price=0.5, filled=10, avg=0.5, status="filled"),
            ],
        }
        assert exit_executor.is_exit_complete(pe) is True

    def test_partial_with_working_children_not_complete(self):
        pe = {
            "target_size": 30.0,
            "children": [
                _child_record(size=10, price=0.5, filled=10, avg=0.5, status="filled"),
                _child_record(size=10, price=0.5, filled=0, status="submitted"),
                _child_record(size=10, price=0.5, filled=0, status="planned"),
            ],
        }
        assert exit_executor.is_exit_complete(pe) is False

    def test_all_terminal_complete_even_if_partial(self):
        pe = {
            "target_size": 30.0,
            "children": [
                _child_record(size=10, price=0.5, filled=10, avg=0.5, status="filled"),
                _child_record(size=10, price=0.5, filled=0, status="cancelled"),
                _child_record(size=10, price=0.5, filled=2, avg=0.5, status="cancelled"),
            ],
        }
        assert exit_executor.is_exit_complete(pe) is True


# ── Selection helpers ─────────────────────────────────────────────────────


class TestSelection:
    def test_pick_planned_respects_max_count(self):
        pe = {"children": [_child_record(size=2, price=0.5, status="planned") for _ in range(10)]}
        idxs = exit_executor.pick_children_to_submit(pe, max_count=4)
        assert idxs == [0, 1, 2, 3]

    def test_skips_terminal_children(self):
        pe = {
            "children": [
                _child_record(size=2, price=0.5, status="filled"),
                _child_record(size=2, price=0.5, status="planned"),
                _child_record(size=2, price=0.5, status="cancelled"),
                _child_record(size=2, price=0.5, status="planned"),
            ]
        }
        assert exit_executor.pick_children_to_submit(pe, max_count=10) == [1, 3]

    def test_pick_reprice_when_drift_exceeds_bps(self):
        now = datetime.now(timezone.utc)
        pe = {
            "policy": {
                "reprice_on_mid_drift_bps": 50.0,  # 50bps = 0.5%
                "min_reprice_interval_seconds": 0.0,
            },
            "children": [
                {**_child_record(size=10, price=0.5000, status="submitted"),
                 "last_attempt_at": (now - timedelta(seconds=30)).isoformat()},
                {**_child_record(size=10, price=0.4990, status="submitted"),
                 "last_attempt_at": (now - timedelta(seconds=30)).isoformat()},
            ],
        }
        # current_mid 0.495: 0.5 vs 0.495 = 100bps drift, 0.499 vs 0.495 = 80bps drift
        idxs = exit_executor.pick_children_to_reprice(pe, current_mid=0.495, now=now)
        assert idxs == [0, 1]

    def test_reprice_debounce_blocks_recent_attempts(self):
        now = datetime.now(timezone.utc)
        pe = {
            "policy": {
                "reprice_on_mid_drift_bps": 10.0,
                "min_reprice_interval_seconds": 30.0,
            },
            "children": [
                {**_child_record(size=10, price=0.5000, status="submitted"),
                 "last_attempt_at": (now - timedelta(seconds=5)).isoformat()},
            ],
        }
        idxs = exit_executor.pick_children_to_reprice(pe, current_mid=0.45, now=now)
        assert idxs == []

    def test_pick_escalate_when_resting_too_long(self):
        now = datetime.now(timezone.utc)
        pe = {
            "policy": {
                "escalation": {
                    "after_seconds": 5.0,
                    "action": "marketable_ioc",
                    "max_escalations": 1,
                }
            },
            "children": [
                {**_child_record(size=10, price=0.5, status="submitted"),
                 "submitted_at": (now - timedelta(seconds=10)).isoformat()},
                {**_child_record(size=10, price=0.5, status="submitted"),
                 "submitted_at": (now - timedelta(seconds=2)).isoformat()},
                {**_child_record(size=10, price=0.5, status="submitted"),
                 "submitted_at": (now - timedelta(seconds=10)).isoformat(),
                 "escalation_count": 1},
            ],
        }
        idxs = exit_executor.pick_children_to_escalate(pe, now=now)
        # Only child 0: child 1 hasn't rested long enough, child 2 is escalated.
        assert idxs == [0]


# ── Async submission with fake adapter ────────────────────────────────────


@dataclass
class _FakeOrderResult:
    status: str = "submitted"
    order_id: str = "fake-1"
    error_message: str | None = None
    payload: dict | None = None


class _FakeAdapter:
    def __init__(self):
        self.calls: list[dict] = []
        self.cancel_calls: list[str] = []
        self._next_result: _FakeOrderResult = _FakeOrderResult(
            status="submitted",
            order_id="fake-1",
            payload={"clob_order_id": "clob-fake-1", "filled_size": 0.0},
        )

    def queue_result(self, result: _FakeOrderResult) -> None:
        self._next_result = result

    async def place_order(self, **kwargs):
        self.calls.append(kwargs)
        result = self._next_result
        # Reset to default after each call
        self._next_result = _FakeOrderResult(
            status="submitted",
            order_id=f"fake-{len(self.calls)}",
            payload={"clob_order_id": f"clob-fake-{len(self.calls)}", "filled_size": 0.0},
        )
        return result

    async def cancel(self, clob_id: str):
        self.cancel_calls.append(clob_id)
        return True


class TestAsyncSubmission:
    @pytest.mark.asyncio
    async def test_submit_child_marks_submitted(self):
        adapter = _FakeAdapter()
        child = _child_record(size=2.0, price=0.5)
        result = await exit_executor.submit_child_order(
            child=child,
            token_id="tok-1",
            side="SELL",
            min_order_size_usd=1.0,
            place_order=adapter.place_order,
        )
        assert result["status"] == exit_executor.CHILD_SUBMITTED
        assert result["provider_clob_order_id"] == "clob-fake-1"
        assert len(adapter.calls) == 1
        assert adapter.calls[0]["size"] == 2.0
        assert adapter.calls[0]["fallback_price"] == 0.5

    @pytest.mark.asyncio
    async def test_submit_child_marks_filled_when_executed(self):
        adapter = _FakeAdapter()
        adapter.queue_result(
            _FakeOrderResult(
                status="executed",
                order_id="fake-X",
                payload={"clob_order_id": "clob-X", "filled_size": 2.0, "average_fill_price": 0.498},
            )
        )
        child = _child_record(size=2.0, price=0.5)
        result = await exit_executor.submit_child_order(
            child=child, token_id="t", side="SELL",
            min_order_size_usd=1.0, place_order=adapter.place_order,
        )
        assert result["status"] == exit_executor.CHILD_FILLED
        assert result["filled_size"] == pytest.approx(2.0)
        assert result["average_fill_price"] == pytest.approx(0.498)

    @pytest.mark.asyncio
    async def test_submit_child_marks_failed_on_exception(self):
        async def raise_(**_):
            raise RuntimeError("boom")
        child = _child_record(size=2.0, price=0.5)
        result = await exit_executor.submit_child_order(
            child=child, token_id="t", side="SELL",
            min_order_size_usd=1.0, place_order=raise_,
        )
        assert result["status"] == exit_executor.CHILD_FAILED
        assert "boom" in (result.get("last_error") or "")

    @pytest.mark.asyncio
    async def test_escalate_marketable_ioc_swaps_tif_and_resubmits(self):
        adapter = _FakeAdapter()
        child = _child_record(size=2.0, price=0.5, status="submitted")
        child["provider_clob_order_id"] = "clob-old"
        await exit_executor.escalate_child_order(
            child=child,
            token_id="t",
            side="SELL",
            min_order_size_usd=1.0,
            policy_snapshot={"escalation": {"action": "marketable_ioc", "after_seconds": 5}},
            place_order=adapter.place_order,
            cancel_fn=adapter.cancel,
            current_mid=0.48,
        )
        assert adapter.cancel_calls == ["clob-old"]
        assert child["tif"] == "IOC"
        assert child["post_only"] is False
        assert child["price"] == pytest.approx(0.48)
        assert child["escalation_count"] == 1


# ── BaseStrategy.resolve_exit_policy ──────────────────────────────────────


class _StratWithPolicies(BaseStrategy):
    strategy_type = "test_resolver"
    name = "Resolver Test"
    description = "Test"
    exit_policies = {
        "stop_loss": StrategySDK.build_ladder_exit_policy(levels=10, step_ticks=1, chunk_size=2),
        "*": StrategySDK.build_ladder_exit_policy(levels=3, step_ticks=2, chunk_size=5),
    }


class TestStrategyResolveExitPolicy:
    def test_per_decision_override_wins(self):
        s = _StratWithPolicies()
        custom = StrategySDK.build_ladder_exit_policy(levels=99)
        d = ExitDecision("close", "...", exit_policy=custom)
        resolved = s.resolve_exit_policy(d, "stop_loss")
        assert resolved.ladder.levels == 99

    def test_trigger_key_lookup(self):
        s = _StratWithPolicies()
        d = ExitDecision("close", "...")
        resolved = s.resolve_exit_policy(d, "stop_loss")
        assert resolved.ladder.levels == 10

    def test_wildcard_fallback(self):
        s = _StratWithPolicies()
        d = ExitDecision("close", "...")
        resolved = s.resolve_exit_policy(d, "trailing_stop")
        assert resolved.ladder.levels == 3

    def test_no_policies_returns_none(self):
        class _Plain(BaseStrategy):
            strategy_type = "test_plain_resolver"
            name = "Plain"
            description = "Plain"

        d = ExitDecision("close", "...")
        assert _Plain().resolve_exit_policy(d, "stop_loss") is None


# ── End-to-end: build_initial_children + run_exit_pass ────────────────────


class TestEndToEnd:
    @pytest.mark.asyncio
    async def test_initial_pass_submits_all_children(self):
        policy = StrategySDK.build_ladder_exit_policy(
            levels=3, step_ticks=1, chunk_size=10.0, distribution="uniform",
            escalation_after_seconds=None,
        )
        exit_id, children = exit_executor.build_initial_children(
            target_size=30.0, trigger_price=0.50, side="SELL", policy=policy,
        )
        assert len(children) == 3
        assert all(c["status"] == exit_executor.CHILD_PLANNED for c in children)
        pending_exit = {
            "target_size": 30.0,
            "trigger_price": 0.50,
            "policy": exit_executor.serialize_policy(policy),
            "children": children,
        }

        adapter = _FakeAdapter()
        report = await exit_executor.run_exit_pass(
            pending_exit=pending_exit,
            token_id="tok",
            side="SELL",
            min_order_size_usd=1.0,
            place_order=adapter.place_order,
            cancel_fn=adapter.cancel,
            current_mid=0.50,
        )
        assert report["submitted"] == 3
        assert report["complete"] is False  # children only "submitted", not "filled"
        assert all(c["status"] == exit_executor.CHILD_SUBMITTED for c in pending_exit["children"])

    @pytest.mark.asyncio
    async def test_pass_completes_after_all_fills(self):
        policy = StrategySDK.build_ladder_exit_policy(
            levels=2, step_ticks=1, chunk_size=10.0, distribution="uniform",
            escalation_after_seconds=None,
        )
        exit_id, children = exit_executor.build_initial_children(
            target_size=20.0, trigger_price=0.50, side="SELL", policy=policy,
        )
        pending_exit = {
            "target_size": 20.0,
            "trigger_price": 0.50,
            "policy": exit_executor.serialize_policy(policy),
            "children": children,
        }
        # Set up adapter to immediately fill every order it sees
        async def filling_place_order(**kwargs):
            return _FakeOrderResult(
                status="executed",
                order_id=f"fk-{kwargs['fallback_price']}",
                payload={
                    "clob_order_id": f"clob-{kwargs['fallback_price']}",
                    "filled_size": kwargs["size"],
                    "average_fill_price": kwargs["fallback_price"],
                },
            )

        async def fake_cancel(_):
            return True

        report = await exit_executor.run_exit_pass(
            pending_exit=pending_exit,
            token_id="tok",
            side="SELL",
            min_order_size_usd=1.0,
            place_order=filling_place_order,
            cancel_fn=fake_cancel,
            current_mid=0.50,
        )
        assert report["complete"] is True
        assert report["filled_size"] == pytest.approx(20.0)
