import sys
from datetime import timedelta
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from models.market import Event, Market
from services.strategies.negrisk import NegRiskStrategy
from utils.utcnow import utcnow


def _soccer_negrisk_event(*, liquidity: float) -> Event:
    end_date = utcnow() + timedelta(days=5)
    markets = [
        Market(
            id="market-home",
            condition_id="condition-home",
            question="Will Toronto FC win against CF Montreal?",
            slug="toronto-fc-win",
            clob_token_ids=["home-yes", "home-no"],
            outcome_prices=[0.31, 0.69],
            liquidity=liquidity,
            end_date=end_date,
            neg_risk=True,
            sports_market_type="moneyline",
        ),
        Market(
            id="market-draw",
            condition_id="condition-draw",
            question="Will Toronto FC vs CF Montreal end in a draw?",
            slug="toronto-fc-draw",
            clob_token_ids=["draw-yes", "draw-no"],
            outcome_prices=[0.32, 0.68],
            liquidity=liquidity,
            end_date=end_date,
            neg_risk=True,
            sports_market_type="moneyline",
        ),
        Market(
            id="market-away",
            condition_id="condition-away",
            question="Will CF Montreal win against Toronto FC?",
            slug="cf-montreal-win",
            clob_token_ids=["away-yes", "away-no"],
            outcome_prices=[0.33, 0.67],
            liquidity=liquidity,
            end_date=end_date,
            neg_risk=True,
            sports_market_type="moneyline",
        ),
    ]
    return Event(
        id="event-soccer-negrisk",
        slug="toronto-vs-montreal",
        title="Toronto FC vs. CF Montreal",
        category="Soccer",
        markets=markets,
        neg_risk=True,
    )


def test_negrisk_detect_rejects_non_actionable_three_way_bundle():
    strategy = NegRiskStrategy()
    event = _soccer_negrisk_event(liquidity=200.0)

    opportunities = strategy.detect([event], [], {})

    assert opportunities == []


def test_negrisk_detect_emits_structurally_complete_bundle():
    strategy = NegRiskStrategy()
    event = _soccer_negrisk_event(liquidity=20000.0)

    opportunities = strategy.detect([event], [], {})

    assert len(opportunities) == 1
    opportunity = opportunities[0]
    assert opportunity.net_profit > 0
    assert [market["id"] for market in opportunity.markets] == [
        "market-home",
        "market-draw",
        "market-away",
    ]
    assert all(market["neg_risk"] is True for market in opportunity.markets)
    assert [position["market_id"] for position in opportunity.positions_to_take] == [
        "market-home",
        "market-draw",
        "market-away",
    ]
    assert [position["market_question"] for position in opportunity.positions_to_take] == [
        "Will Toronto FC win against CF Montreal?",
        "Will Toronto FC vs CF Montreal end in a draw?",
        "Will CF Montreal win against Toronto FC?",
    ]
    assert opportunity.execution_plan is not None
    assert [leg.market_id for leg in opportunity.execution_plan.legs] == [
        "market-home",
        "market-draw",
        "market-away",
    ]
    assert [leg.market_question for leg in opportunity.execution_plan.legs] == [
        "Will Toronto FC win against CF Montreal?",
        "Will Toronto FC vs CF Montreal end in a draw?",
        "Will CF Montreal win against Toronto FC?",
    ]
    coverage = opportunity.execution_plan.metadata["market_coverage"]
    assert coverage["planned_market_ids"] == ["market-home", "market-draw", "market-away"]
    assert coverage["missing_market_ids"] == []
    assert coverage["event_internal_bundle"] is True
