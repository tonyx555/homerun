"""Count near-expiry markets in the DB catalog."""
import asyncio, json, sys, io
from datetime import datetime, timezone, timedelta
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.path.insert(0, r"C:\homerun\.claude\worktrees\charming-rosalind\backend")

async def main():
    from models.database import init_database, AsyncSessionLocal
    from sqlalchemy import text
    await init_database()

    now = datetime.now(timezone.utc)
    tomorrow = now + timedelta(days=1)
    two_days = now + timedelta(days=2)

    async with AsyncSessionLocal() as session:
        # Total active markets
        total = (await session.execute(text(
            "SELECT COUNT(*) FROM markets WHERE active = true AND closed = false"
        ))).scalar()
        print(f"Total active markets: {total}")

        # Within 1 day
        within_1d = (await session.execute(text(
            "SELECT COUNT(*) FROM markets WHERE active = true AND closed = false "
            "AND end_date > NOW() AND end_date <= NOW() + INTERVAL '1 day'"
        ))).scalar()
        print(f"Within 1 day: {within_1d}")

        # Within 2 days
        within_2d = (await session.execute(text(
            "SELECT COUNT(*) FROM markets WHERE active = true AND closed = false "
            "AND end_date > NOW() AND end_date <= NOW() + INTERVAL '2 days'"
        ))).scalar()
        print(f"Within 2 days: {within_2d}")

        # Within 1 day with liquidity >= 3500
        within_1d_liq = (await session.execute(text(
            "SELECT COUNT(*) FROM markets WHERE active = true AND closed = false "
            "AND end_date > NOW() AND end_date <= NOW() + INTERVAL '1 day' "
            "AND liquidity >= 3500"
        ))).scalar()
        print(f"Within 1 day + liq >= 3500: {within_1d_liq}")

        # Within 1 day, liq >= 3500, not keyword-excluded
        # Can't easily do keyword filter in SQL, so get the rows
        rows = (await session.execute(text(
            "SELECT question, liquidity, outcome_prices, end_date, condition_id, "
            "       sports_market_type, game_start_time "
            "FROM markets WHERE active = true AND closed = false "
            "AND end_date > NOW() AND end_date <= NOW() + INTERVAL '1 day' "
            "AND liquidity >= 3500 "
            "ORDER BY end_date ASC"
        ))).all()

        excluded_keywords = [
            "bitcoin", "ethereum", "lol:", "counter-strike",
            "tweets", "league of legends", "esports", "rift legends",
            "dota", "valorant", "cs2", "cs:", "esl pro",
        ]

        passing = []
        keyword_blocked = 0
        spread_blocked = 0
        no_price_in_band = 0

        for row in rows:
            question, liquidity, prices, end_date, cond_id, sports_type, game_start = row
            q_lower = str(question or "").lower()

            # keyword check
            blocked = False
            for kw in excluded_keywords:
                if kw in q_lower:
                    blocked = True
                    keyword_blocked += 1
                    break
            if blocked:
                continue

            # spread check
            if sports_type == "spreads" or "spread:" in q_lower:
                spread_blocked += 1
                continue

            # price band check (0.85-0.909)
            price_list = []
            if isinstance(prices, list):
                for p in prices:
                    try:
                        price_list.append(float(p))
                    except (ValueError, TypeError):
                        pass
            elif isinstance(prices, str):
                try:
                    parsed = json.loads(prices)
                    price_list = [float(p) for p in parsed]
                except Exception:
                    pass

            in_band = any(0.85 <= p <= 0.909 for p in price_list)
            if not in_band:
                no_price_in_band += 1
                continue

            passing.append({
                "question": str(question or "")[:60],
                "liq": float(liquidity or 0),
                "prices": price_list,
                "end": str(end_date),
                "sports_type": sports_type,
                "game_start": str(game_start) if game_start else None,
            })

        print(f"\n=== FILTER BREAKDOWN (within 1d, liq >= 3500) ===")
        print(f"  Total qualifying: {len(rows)}")
        print(f"  Keyword blocked: {keyword_blocked}")
        print(f"  Spread blocked: {spread_blocked}")
        print(f"  No price in 0.85-0.909 band: {no_price_in_band}")
        print(f"  PASSING ALL FILTERS: {len(passing)}")

        if passing:
            print(f"\n=== PASSING MARKETS ({len(passing)}) ===")
            for pm in passing[:20]:
                print(f"  {pm['question']:<60s} liq={pm['liq']:.0f} prices={pm['prices']} end={pm['end']}")
                if pm.get('game_start'):
                    print(f"    game_start={pm['game_start']}")

        # Also show what's blocked by keywords
        print(f"\n=== KEYWORD-BLOCKED SAMPLES (first 10) ===")
        shown = 0
        for row in rows:
            question = str(row[0] or "").lower()
            for kw in excluded_keywords:
                if kw in question:
                    print(f"  [{kw}] {str(row[0] or '')[:70]}")
                    shown += 1
                    break
            if shown >= 10:
                break

asyncio.run(main())
