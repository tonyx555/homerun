import sys
from datetime import timedelta
from pathlib import Path

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from models.market import Event, Market
from services.strategies.settlement_lag import SettlementLagStrategy
from utils.utcnow import utcnow


def test_binary_settlement_lag_execution_plan_uses_price_weights():
    strategy = SettlementLagStrategy()
    market = Market(
        id="market-binary",
        condition_id="condition-binary",
        question="Will Team A win?",
        slug="team-a-win",
        clob_token_ids=["yes-token", "no-token"],
        outcome_prices=[0.41, 0.44],
        liquidity=250.0,
        end_date=utcnow() - timedelta(hours=2),
    )

    opportunity = strategy._check_settlement_lag(market, 0.41, 0.44)

    assert opportunity is not None
    assert opportunity.execution_plan is not None
    assert [leg.notional_weight for leg in opportunity.execution_plan.legs] == pytest.approx([0.41, 0.44])


def test_negrisk_settlement_execution_plan_uses_price_weights():
    strategy = SettlementLagStrategy()
    markets = [
        Market(
            id="market-a",
            condition_id="condition-a",
            question="Outcome A",
            slug="outcome-a",
            clob_token_ids=["token-a"],
            outcome_prices=[0.62],
            liquidity=250.0,
            end_date=utcnow() - timedelta(hours=2),
            neg_risk=True,
        ),
        Market(
            id="market-b",
            condition_id="condition-b",
            question="Outcome B",
            slug="outcome-b",
            clob_token_ids=["token-b"],
            outcome_prices=[0.30],
            liquidity=250.0,
            end_date=utcnow() - timedelta(hours=2),
            neg_risk=True,
        ),
    ]
    event = Event(
        id="event-negrisk",
        slug="event-negrisk",
        title="NegRisk event",
        markets=markets,
        neg_risk=True,
    )

    opportunities = strategy._check_negrisk_settlement(event, {})

    assert len(opportunities) == 1
    assert opportunities[0].execution_plan is not None
    assert [leg.notional_weight for leg in opportunities[0].execution_plan.legs] == pytest.approx([0.62, 0.30])
