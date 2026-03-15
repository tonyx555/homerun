"""Debug market catalog - markets are dicts not objects."""
import asyncio, sys, io, json
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
    print(f"Type of first: {type(markets[0]).__name__}")

    if isinstance(markets[0], dict):
        print(f"Keys: {list(markets[0].keys())[:20]}")
        # Show first market
        m = markets[0]
        for k in sorted(m.keys()):
            v = str(m[k])
            if len(v) > 80:
                v = v[:80] + "..."
            print(f"  {k}: {v}")

        # Find nearest-expiry markets
        def get_days(m):
            ed = m.get("end_date")
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
            q = str(m.get("question") or m.get("title") or "?")[:60]
            liq = float(m.get("liquidity") or 0)
            ed = m.get("end_date")
            print(f"  days={d:>8.3f} liq={liq:>8.0f} end={ed} q={q}")
    else:
        # It's a Pydantic model or something
        print(f"Dir: {[a for a in dir(markets[0]) if not a.startswith('_')][:20]}")

asyncio.run(main())
