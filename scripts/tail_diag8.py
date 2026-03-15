"""Count near-expiry markets from scanner's cached catalog via shared_state."""
import asyncio, sys, io
from datetime import datetime, timezone
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.path.insert(0, r"C:\homerun\.claude\worktrees\charming-rosalind\backend")

async def main():
    from models.database import init_database, AsyncSessionLocal
    from services.shared_state import read_market_catalog
    await init_database()

    async with AsyncSessionLocal() as session:
        events, markets, metadata = await read_market_catalog(
            session,
            include_events=True,
            include_markets=True,
            validate=False,
        )

    now = datetime.now(timezone.utc)
    print(f"Catalog: {len(events)} events, {len(markets)} markets")
    print(f"Current time: {now.isoformat()}")

    excluded_keywords = [
        "bitcoin", "ethereum", "lol:", "counter-strike",
        "tweets", "league of legends", "esports", "rift legends",
        "dota", "valorant", "cs2", "cs:", "esl pro",
    ]

    within_1d = []
    for m in markets:
        end_date = getattr(m, "end_date", None)
        if end_date is None:
            continue
        if hasattr(end_date, "tzinfo") and end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)
        elif isinstance(end_date, str):
            if end_date.endswith("Z"):
                end_date = end_date[:-1] + "+00:00"
            end_date = datetime.fromisoformat(end_date)
            if end_date.tzinfo is None:
                end_date = end_date.replace(tzinfo=timezone.utc)

        days = (end_date - now).total_seconds() / 86400.0
        if 0.01 < days <= 1.0:
            within_1d.append((m, days))

    print(f"\nWithin 1 day: {len(within_1d)}")

    # Filter pipeline
    active = [(m, d) for m, d in within_1d if not getattr(m, "closed", False) and getattr(m, "active", True)]
    print(f"Active: {len(active)}")

    # Keyword filter
    not_excluded = []
    kw_blocked = {}
    for m, d in active:
        q = str(getattr(m, "question", "") or "").lower()
        blocked = False
        for kw in excluded_keywords:
            if kw in q:
                kw_blocked[kw] = kw_blocked.get(kw, 0) + 1
                blocked = True
                break
        if not blocked:
            not_excluded.append((m, d))

    print(f"After keyword filter: {len(not_excluded)}")
    for kw, c in sorted(kw_blocked.items(), key=lambda x: -x[1]):
        print(f"  blocked by '{kw}': {c}")

    # Liquidity >= 3500
    has_liq = [(m, d) for m, d in not_excluded if float(getattr(m, "liquidity", 0) or 0) >= 3500]
    print(f"After liq >= 3500: {len(has_liq)}")

    # Has 2+ tokens
    has_tokens = [(m, d) for m, d in has_liq
                  if len(list(getattr(m, "clob_token_ids", []) or [])) >= 2
                  or len(list(getattr(m, "outcome_prices", []) or [])) >= 2]
    print(f"After 2+ tokens: {len(has_tokens)}")

    # Price in band 0.85-0.909
    in_band = []
    for m, d in has_tokens:
        prices = list(getattr(m, "outcome_prices", []) or [])
        try:
            float_prices = [float(p) for p in prices]
        except (ValueError, TypeError):
            continue
        if any(0.85 <= p <= 0.909 for p in float_prices):
            in_band.append((m, d, float_prices))

    print(f"After price in 0.85-0.909: {len(in_band)}")

    # Not spread
    not_spread = []
    for m, d, prices in in_band:
        stype = getattr(m, "sports_market_type", None)
        q = str(getattr(m, "question", "") or "").lower()
        if stype == "spreads" or "spread:" in q:
            continue
        not_spread.append((m, d, prices))

    print(f"After spread filter: {len(not_spread)}")

    # Not live game
    not_live = []
    for m, d, prices in not_spread:
        gs = getattr(m, "game_start_time", None)
        if gs is not None:
            if hasattr(gs, "timestamp"):
                if (gs.timestamp() - 15 * 60) <= now.timestamp():
                    continue
            elif isinstance(gs, str) and gs:
                try:
                    parsed = datetime.fromisoformat(gs.replace("Z", "+00:00"))
                    if (parsed.timestamp() - 15 * 60) <= now.timestamp():
                        continue
                except Exception:
                    pass
        not_live.append((m, d, prices))

    print(f"After live game filter: {len(not_live)}")

    if not_live:
        print(f"\n=== MARKETS PASSING ALL FILTERS ({len(not_live)}) ===")
        for m, d, prices in not_live[:30]:
            q = str(getattr(m, "question", "") or "")[:60]
            liq = float(getattr(m, "liquidity", 0) or 0)
            print(f"  {q:<60s} days={d:.3f} liq={liq:.0f} prices={prices}")

    # Also show: what if we expanded to 2 days?
    within_2d_count = sum(1 for m in markets
                         if getattr(m, "end_date", None) is not None
                         and not getattr(m, "closed", False)
                         and getattr(m, "active", True)
                         and 0.01 < _days_to(m, now) <= 2.0)
    print(f"\n=== EXPANSION ===")
    print(f"Within 2 days (active): {within_2d_count}")

def _days_to(m, now):
    end = getattr(m, "end_date", None)
    if end is None:
        return 999
    if hasattr(end, "tzinfo") and end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    return (end - now).total_seconds() / 86400.0

asyncio.run(main())
