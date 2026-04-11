from __future__ import annotations

from models import Market
from services.signal_bus import _normalize_execution_plan
from services.strategies.base import BaseStrategy


class DummyExecutionPlanStrategy(BaseStrategy):
    strategy_type = "dummy_execution_plan"
    name = "Dummy Execution Plan"
    description = "Dummy execution plan coverage"
    mispricing_type = "within_market"
    subscriptions = ["market_data_refresh"]

    def detect(self, events, markets, prices):
        return []


def _market() -> Market:
    return Market(
        id="market-1",
        condition_id="condition-1",
        question="Will example happen?",
        slug="example-market",
        clob_token_ids=["token-yes", "token-no"],
        outcome_prices=[0.48, 0.48],
        liquidity=2500.0,
        volume=5000.0,
    )


def test_create_opportunity_keeps_all_same_market_positions_in_execution_plan():
    strategy = DummyExecutionPlanStrategy()
    opportunity = strategy.create_opportunity(
        title="Example",
        description="Two-leg same-market arb",
        total_cost=0.96,
        markets=[_market()],
        positions=[
            {"action": "BUY", "outcome": "YES", "price": 0.48, "token_id": "token-yes"},
            {"action": "BUY", "outcome": "NO", "price": 0.48, "token_id": "token-no"},
        ],
        is_guaranteed=True,
    )

    assert opportunity is not None
    assert opportunity.execution_plan is not None
    assert len(opportunity.execution_plan.legs) == 2
    assert {leg.market_id for leg in opportunity.execution_plan.legs} == {"market-1"}
    assert {leg.outcome for leg in opportunity.execution_plan.legs} == {"yes", "no"}


def test_normalize_execution_plan_rebuilds_malformed_same_market_multileg_plan():
    strategy = DummyExecutionPlanStrategy()
    opportunity = strategy.create_opportunity(
        title="Example",
        description="Two-leg same-market arb",
        total_cost=0.96,
        markets=[_market()],
        positions=[
            {"action": "BUY", "outcome": "YES", "price": 0.48, "token_id": "token-yes"},
            {"action": "BUY", "outcome": "NO", "price": 0.48, "token_id": "token-no"},
        ],
        is_guaranteed=True,
    )

    bad_plan = opportunity.execution_plan.model_dump(mode="json")
    bad_plan["policy"] = "SINGLE_LEG"
    bad_plan["legs"] = [bad_plan["legs"][0]]
    opportunity.execution_plan = bad_plan

    repaired = _normalize_execution_plan(opportunity)

    assert repaired is not None
    assert repaired["policy"] == "PAIR_LOCK"
    assert len(repaired["legs"]) == 2
    assert {leg["market_id"] for leg in repaired["legs"]} == {"market-1"}
    assert {leg["outcome"] for leg in repaired["legs"]} == {"yes", "no"}


def test_normalize_execution_plan_preserves_leg_level_execution_contract():
    strategy = DummyExecutionPlanStrategy()
    opportunity = strategy.create_opportunity(
        title="Single leg",
        description="Preserve execution fields",
        total_cost=0.91,
        markets=[_market()],
        positions=[
            {
                "action": "BUY",
                "outcome": "YES",
                "price": 0.91,
                "token_id": "token-yes",
                "max_execution_price": 0.94,
                "max_entry_price": 0.94,
                "price_policy": "taker_limit",
                "time_in_force": "IOC",
                "allow_taker_limit_buy_above_signal": True,
                "aggressive_limit_buy_submit_as_gtc": True,
            }
        ],
        is_guaranteed=False,
    )

    normalized = _normalize_execution_plan(opportunity)

    assert normalized is not None
    assert len(normalized["legs"]) == 1
    leg = normalized["legs"][0]
    assert leg["max_execution_price"] == 0.94
    assert leg["max_entry_price"] == 0.94
    assert leg["allow_taker_limit_buy_above_signal"] is True
    assert leg["aggressive_limit_buy_submit_as_gtc"] is True


def test_create_opportunity_backfills_market_identity_for_multileg_positions():
    strategy = DummyExecutionPlanStrategy()
    market_a = _market()
    market_b = Market(
        id="market-2",
        condition_id="condition-2",
        question="Will a second example happen?",
        slug="example-market-2",
        clob_token_ids=["token-yes-2", "token-no-2"],
        outcome_prices=[0.43, 0.57],
        liquidity=2500.0,
        volume=5000.0,
    )

    opportunity = strategy.create_opportunity(
        title="Example pair",
        description="Legacy multileg labels should not become market ids",
        total_cost=0.91,
        markets=[market_a, market_b],
        positions=[
            {"action": "BUY", "outcome": "YES", "market": market_a.question[:50], "price": 0.48, "token_id": "token-yes"},
            {
                "action": "BUY",
                "outcome": "YES",
                "market": market_b.question[:50],
                "price": 0.43,
                "token_id": "token-yes-2",
            },
        ],
        is_guaranteed=True,
    )

    assert opportunity is not None
    assert [position["market_id"] for position in opportunity.positions_to_take] == ["market-1", "market-2"]
    assert [position["market_question"] for position in opportunity.positions_to_take] == [
        "Will example happen?",
        "Will a second example happen?",
    ]
    assert opportunity.execution_plan is not None
    assert [leg.market_id for leg in opportunity.execution_plan.legs] == ["market-1", "market-2"]
    assert [leg.market_question for leg in opportunity.execution_plan.legs] == [
        "Will example happen?",
        "Will a second example happen?",
    ]
