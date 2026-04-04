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
