"""Count UPCOMING near-expiry markets (positive days only)."""
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
    print(f"Now: {now.isoformat()}")

    excluded_keywords = [
        "bitcoin", "ethereum", "lol:", "counter-strike",
        "tweets", "league of legends", "esports", "rift legends",
        "dota", "valorant", "cs2", "cs:", "esl pro",
    ]

    def parse_end(m):
        ed = m.get("end_date")
        if ed is None:
            return None
        try:
            if isinstance(ed, str):
                if ed.endswith("Z"):
                    ed = ed[:-1] + "+00:00"
                return datetime.fromisoformat(ed)
            if hasattr(ed, "tzinfo"):
                return ed if ed.tzinfo else ed.replace(tzinfo=timezone.utc)
        except Exception:
            return None

    # Count by time bucket
    buckets = {"0-1h": 0, "1-4h": 0, "4-12h": 0, "12-24h": 0, "1-2d": 0, "2-7d": 0}
    upcoming = []
    for m in markets:
        if m.get("closed") or not m.get("active", True):
            continue
        ed = parse_end(m)
        if ed is None:
            continue
        days = (ed - now).total_seconds() / 86400.0
        if days <= 0:
            continue
        hours = days * 24
        if hours <= 1: buckets["0-1h"] += 1
        elif hours <= 4: buckets["1-4h"] += 1
        elif hours <= 12: buckets["4-12h"] += 1
        elif hours <= 24: buckets["12-24h"] += 1
        elif days <= 2: buckets["1-2d"] += 1
        elif days <= 7: buckets["2-7d"] += 1

        if 0 < days <= 1.0:
            upcoming.append((m, days))

    print(f"\n=== MARKET DISTRIBUTION BY TIME TO EXPIRY ===")
    for bucket, count in buckets.items():
        print(f"  {bucket}: {count}")

    print(f"\nTotal within 1 day (upcoming): {len(upcoming)}")

    # Full filter pipeline on upcoming
    not_excluded = []
    kw_counts = {}
    for m, d in upcoming:
        q = str(m.get("question") or "").lower()
        blocked = False
        for kw in excluded_keywords:
            if kw in q:
                kw_counts[kw] = kw_counts.get(kw, 0) + 1
                blocked = True
                break
        if not blocked:
            not_excluded.append((m, d))

    print(f"After keywords: {len(not_excluded)}")
    if kw_counts:
        for kw, c in sorted(kw_counts.items(), key=lambda x: -x[1]):
            print(f"  blocked by '{kw}': {c}")

    has_liq = [(m, d) for m, d in not_excluded if float(m.get("liquidity") or 0) >= 3500]
    print(f"After liq >= 3500: {len(has_liq)}")

    # Liquidity distribution for markets that passed keyword filter
    liq_dist = {"<500": 0, "500-1k": 0, "1k-2k": 0, "2k-3.5k": 0, "3.5k+": 0}
    for m, d in not_excluded:
        liq = float(m.get("liquidity") or 0)
        if liq < 500: liq_dist["<500"] += 1
        elif liq < 1000: liq_dist["500-1k"] += 1
        elif liq < 2000: liq_dist["1k-2k"] += 1
        elif liq < 3500: liq_dist["2k-3.5k"] += 1
        else: liq_dist["3.5k+"] += 1
    print(f"\nLiquidity distribution (not keyword-blocked, within 1d):")
    for bucket, count in liq_dist.items():
        print(f"  {bucket}: {count}")

    # Show all passing markets
    has_tokens = [(m, d) for m, d in has_liq
                  if len(m.get("clob_token_ids") or []) >= 2 or len(m.get("outcome_prices") or []) >= 2]
    in_band = []
    for m, d in has_tokens:
        prices = [float(p) for p in (m.get("outcome_prices") or []) if p is not None]
        if any(0.85 <= p <= 0.909 for p in prices):
            in_band.append((m, d, prices))

    print(f"\nAfter tokens + price band: {len(in_band)}")
    for m, d, prices in in_band[:20]:
        q = str(m.get("question") or "?")[:60]
        liq = float(m.get("liquidity") or 0)
        gs = m.get("game_start_time")
        print(f"  {q:<60s} days={d:.3f} liq={liq:.0f} prices={prices} gs={gs}")

asyncio.run(main())
