"""Debug end_date format on market objects from catalog."""
import asyncio, sys, io
from datetime import datetime, timezone
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.path.insert(0, r"C:\homerun\.claude\worktrees\charming-rosalind\backend")

async def main():
    from models.database import init_database, AsyncSessionLocal
    from services.shared_state import read_market_catalog
    await init_database()

    async with AsyncSessionLocal() as session:
        _, markets, _ = await read_market_catalog(
            session, include_events=False, include_markets=True, validate=False,
        )

    now = datetime.now(timezone.utc)
    print(f"Total markets: {len(markets)}")

    # Sample first 5 markets - show ALL relevant attributes
    for i, m in enumerate(markets[:5]):
        print(f"\n--- Market {i} ---")
        print(f"  type: {type(m).__name__}")
        end_date = getattr(m, "end_date", "MISSING")
        print(f"  end_date: {end_date} (type={type(end_date).__name__})")
        print(f"  question: {str(getattr(m, 'question', '?'))[:60]}")
        print(f"  closed: {getattr(m, 'closed', '?')}")
        print(f"  active: {getattr(m, 'active', '?')}")
        print(f"  liquidity: {getattr(m, 'liquidity', '?')}")
        print(f"  outcome_prices: {getattr(m, 'outcome_prices', '?')}")

    # Find markets with smallest days-to-resolution
    def get_days(m):
        ed = getattr(m, "end_date", None)
        if ed is None:
            return 99999
        try:
            if isinstance(ed, str):
                if ed.endswith("Z"):
                    ed = ed[:-1] + "+00:00"
                ed = datetime.fromisoformat(ed)
            if hasattr(ed, "tzinfo") and ed.tzinfo is None:
                ed = ed.replace(tzinfo=timezone.utc)
            return (ed - now).total_seconds() / 86400.0
        except Exception:
            return 99999

    sorted_markets = sorted(markets, key=get_days)
    print(f"\n=== NEAREST EXPIRY MARKETS (top 20) ===")
    for m in sorted_markets[:20]:
        d = get_days(m)
        q = str(getattr(m, "question", "?"))[:60]
        ed = getattr(m, "end_date", None)
        liq = float(getattr(m, "liquidity", 0) or 0)
        print(f"  days={d:>8.3f} liq={liq:>8.0f} end={ed} q={q}")

asyncio.run(main())
