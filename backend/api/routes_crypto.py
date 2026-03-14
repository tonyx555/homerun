from __future__ import annotations

from fastapi import APIRouter

from services.market_runtime import get_market_runtime

router = APIRouter()


@router.get("/crypto/markets")
async def get_crypto_markets(viewer_active: bool = False) -> list[dict]:
    del viewer_active
    return get_market_runtime().get_crypto_markets()


@router.get("/crypto/oracle-prices")
async def get_oracle_prices() -> dict[str, dict]:
    return get_market_runtime().get_oracle_prices()
