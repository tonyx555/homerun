import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from api.routes import _derive_opportunity_sub_strategy  # noqa: E402


def test_derive_news_edge_direction():
    opp = {
        "strategy": "news_edge",
        "title": "News Edge: Example",
        "positions_to_take": [
            {
                "action": "BUY",
                "outcome": "YES",
                "_news_edge": {"direction": "buy_yes"},
            }
        ],
    }

    assert _derive_opportunity_sub_strategy(opp) == "buy_yes"


def test_derive_cross_platform_leg_shape():
    opp = {
        "strategy": "cross_platform",
        "title": "Cross-Platform: Example",
        "positions_to_take": [
            {"platform": "polymarket", "outcome": "YES"},
            {"platform": "kalshi", "outcome": "NO"},
        ],
    }

    assert _derive_opportunity_sub_strategy(opp) == "poly_yes_kalshi_no"
