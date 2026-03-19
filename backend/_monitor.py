"""20-minute monitoring script for post-fix health check."""
import asyncio
import sys

sys.path.insert(0, ".")
from models.database import AsyncSessionLocal
from sqlalchemy import text


async def health_check(label: str):
    print(f"\n{'='*60}")
    print(f"HEALTH CHECK: {label}")
    print(f"{'='*60}")
    async with AsyncSessionLocal() as s:
        # Recent errors (5 min)
        rows = (await s.execute(text(
            "SELECT event_type, severity, source, substring(message, 1, 120), created_at "
            "FROM trader_events "
            "WHERE created_at > now() - interval '5 minutes' "
            "AND severity IN ('error','warn') "
            "ORDER BY created_at DESC LIMIT 15"
        ))).all()
        print(f"\nRecent warnings/errors (5 min): {len(rows)}")
        for r in rows:
            print(f"  [{r[4]}] {r[1].upper()} {r[0]} | {r[2]} | {r[3]}")

        # Decision errors
        de = (await s.execute(text(
            "SELECT count(*) FROM trader_events "
            "WHERE created_at > now() - interval '5 minutes' "
            "AND event_type = 'decision_error'"
        ))).scalar()
        print(f"\nDecision errors (5 min): {de}")

        # IntegrityErrors
        ie = (await s.execute(text(
            "SELECT count(*) FROM trader_events "
            "WHERE created_at > now() - interval '5 minutes' "
            "AND message ILIKE '%IntegrityError%'"
        ))).scalar()
        print(f"IntegrityErrors (5 min): {ie}")

        # Pool exhaustion
        pe = (await s.execute(text(
            "SELECT count(*) FROM trader_events "
            "WHERE created_at > now() - interval '5 minutes' "
            "AND (message ILIKE '%pool%exhaust%' OR message ILIKE '%pool near%')"
        ))).scalar()
        print(f"Pool exhaustion events (5 min): {pe}")

        # New recovered orders (duplicates check)
        nr = (await s.execute(text(
            "SELECT count(*) FROM trader_orders "
            "WHERE mode = 'live' "
            "AND reason = 'Recovered from live venue authority' "
            "AND created_at > now() - interval '5 minutes'"
        ))).scalar()
        print(f"New recovered/duplicate orders (5 min): {nr}")

        # Existing duplicates
        rows3 = (await s.execute(text(
            "SELECT market_question, direction, count(*) as cnt "
            "FROM trader_orders "
            "WHERE mode = 'live' "
            "AND status IN ('open','submitted','executed') "
            "AND reason = 'Recovered from live venue authority' "
            "GROUP BY market_question, direction "
            "HAVING count(*) > 1 "
            "ORDER BY cnt DESC LIMIT 5"
        ))).all()
        print(f"\nExisting duplicate recovered orders: {len(rows3)}")
        for r in rows3:
            print(f"  {r[2]}x | {r[1]} | {r[0][:80]}")

        # Cycle timeouts
        ct = (await s.execute(text(
            "SELECT count(*) FROM trader_events "
            "WHERE created_at > now() - interval '5 minutes' "
            "AND event_type = 'cycle_timeout'"
        ))).scalar()
        print(f"\nCycle timeouts (5 min): {ct}")

        # Connection hold warnings (from logs - check events)
        ch = (await s.execute(text(
            "SELECT count(*) FROM trader_events "
            "WHERE created_at > now() - interval '5 minutes' "
            "AND message ILIKE '%connection held%'"
        ))).scalar()
        print(f"Connection hold warnings (5 min): {ch}")

        # Recent successful trades
        st = (await s.execute(text(
            "SELECT count(*) FROM trader_orders "
            "WHERE mode = 'live' "
            "AND status IN ('open','executed') "
            "AND created_at > now() - interval '5 minutes' "
            "AND reason <> 'Recovered from live venue authority'"
        ))).scalar()
        print(f"Successful new trades (5 min): {st}")

    print(f"\n{'='*60}\n")


async def main():
    intervals = [
        (0, "INITIAL (t=0)"),
        (300, "5-MINUTE CHECK (t=5m)"),
        (300, "10-MINUTE CHECK (t=10m)"),
        (300, "15-MINUTE CHECK (t=15m)"),
        (300, "20-MINUTE FINAL CHECK (t=20m)"),
    ]
    for delay, label in intervals:
        if delay > 0:
            print(f"Waiting {delay}s for next check...")
            await asyncio.sleep(delay)
        await health_check(label)

    print("MONITORING COMPLETE")


if __name__ == "__main__":
    asyncio.run(main())
