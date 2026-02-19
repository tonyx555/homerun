import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from api import routes
from models.opportunity import Opportunity


def _make_opportunity(*, strategy: str, title: str, roi_percent: float) -> Opportunity:
    total_cost = 0.5
    net_profit = round(total_cost * (roi_percent / 100.0), 6)
    return Opportunity(
        strategy=strategy,
        title=title,
        description=title,
        total_cost=total_cost,
        expected_payout=total_cost + net_profit,
        gross_profit=net_profit,
        fee=0.0,
        net_profit=net_profit,
        roi_percent=roi_percent,
        markets=[{"id": f"{strategy}-{title.replace(' ', '-').lower()}"}],
        positions_to_take=[{"market": f"{strategy}-{title}", "outcome": "YES", "price": total_cost}],
    )


@pytest.mark.asyncio
async def test_get_opportunity_ids_returns_paginated_ids(monkeypatch):
    fake_session = object()
    opp_a = _make_opportunity(strategy="basic", title="alpha", roi_percent=5.0)
    opp_b = _make_opportunity(strategy="basic", title="beta", roi_percent=25.0)
    opp_c = _make_opportunity(strategy="basic", title="gamma", roi_percent=10.0)
    get_mock = AsyncMock(return_value=[opp_a, opp_b, opp_c])
    monkeypatch.setattr(routes.shared_state, "get_opportunities_from_db", get_mock)

    out = await routes.get_opportunity_ids(
        session=fake_session,
        limit=1,
        offset=1,
    )

    assert out == {
        "total": 3,
        "offset": 1,
        "limit": 1,
        "ids": [opp_c.id],
    }
    await_args = get_mock.await_args
    assert await_args is not None
    assert await_args.args[0] is fake_session
    assert await_args.kwargs.get("source") == "markets"
    filter_arg = await_args.args[1]
    assert filter_arg.min_profit == 0.0
    assert filter_arg.max_risk == 1.0
    assert filter_arg.strategies == []


@pytest.mark.asyncio
async def test_get_opportunity_ids_applies_search_and_exclude(monkeypatch):
    fake_session = object()
    opp_keep = _make_opportunity(strategy="basic", title="Win market", roi_percent=8.0)
    opp_excluded = _make_opportunity(strategy="negrisk", title="Win market", roi_percent=9.0)
    opp_nonmatch = _make_opportunity(strategy="basic", title="Lose market", roi_percent=15.0)
    get_mock = AsyncMock(return_value=[opp_keep, opp_excluded, opp_nonmatch])
    monkeypatch.setattr(routes.shared_state, "get_opportunities_from_db", get_mock)

    out = await routes.get_opportunity_ids(
        session=fake_session,
        strategy="basic",
        search="win",
        exclude_strategy="negrisk",
        limit=50,
        offset=0,
    )

    assert out["ids"] == [opp_keep.id]
    await_args = get_mock.await_args
    assert await_args is not None
    filter_arg = await_args.args[1]
    assert filter_arg.strategies == ["basic"]
