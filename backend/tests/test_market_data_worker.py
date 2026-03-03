from types import SimpleNamespace

from workers.market_data_worker import (
    _collect_catalog_index,
    _collect_tokens_from_payload,
    _select_with_priority,
)


def test_collect_catalog_index_maps_condition_and_market_ids() -> None:
    markets = [
        SimpleNamespace(
            condition_id="0xabc123",
            id="market-row-1",
            clob_token_ids=["tok_yes_1", "tok_no_1"],
        ),
        SimpleNamespace(
            condition_id="0xdef456",
            id=None,
            clob_token_ids=["tok_yes_2", "tok_no_2"],
        ),
    ]

    token_ids, market_tokens = _collect_catalog_index(markets)

    assert token_ids == {"tok_yes_1", "tok_no_1", "tok_yes_2", "tok_no_2"}
    assert market_tokens["0xabc123"] == ("tok_yes_1", "tok_no_1")
    assert market_tokens["market-row-1"] == ("tok_yes_1", "tok_no_1")
    assert market_tokens["0xdef456"] == ("tok_yes_2", "tok_no_2")


def test_collect_tokens_from_payload_recurses_token_fields() -> None:
    payload = {
        "live_market": {
            "selected_token_id": "tok_selected",
            "token_ids": ["tok_yes", "tok_no"],
            "nested": {
                "asset_id": "tok_asset",
            },
        },
        "legs": [
            {"token_id": "tok_leg_1"},
            {"metadata": {"clob_token_ids": ["tok_leg_2", "tok_leg_3"]}},
        ],
    }

    tokens = _collect_tokens_from_payload(payload)

    assert tokens == {
        "tok_selected",
        "tok_yes",
        "tok_no",
        "tok_asset",
        "tok_leg_1",
        "tok_leg_2",
        "tok_leg_3",
    }


def test_select_with_priority_keeps_all_critical_items() -> None:
    selected, dropped = _select_with_priority(
        critical_items={"c3", "c1", "c2"},
        background_items={"b1", "b2", "b3"},
        cap=2,
    )
    assert selected == {"c1", "c2", "c3"}
    assert dropped == 3

    selected, dropped = _select_with_priority(
        critical_items={"c1"},
        background_items={"b1", "b2", "b3"},
        cap=3,
    )
    assert selected == {"c1", "b1", "b2"}
    assert dropped == 1
