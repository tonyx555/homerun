"""Count near-expiry markets in the full catalog."""
import urllib.request, json, sys, io
from datetime import datetime, timezone
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

now = datetime.now(timezone.utc)
print(f"Current time: {now.isoformat()}")

# Get markets sorted by end_date ascending to find near-expiry ones
# Try different API params
for params in [
    "sort=end_date&order=asc&limit=200",
    "sort_by=end_date&limit=200",
    "limit=200&sort=end_date_asc",
    "limit=50000",
]:
    try:
        resp = urllib.request.urlopen(f'http://localhost:8000/api/markets?{params}', timeout=20)
        data = json.loads(resp.read())
        markets = data if isinstance(data, list) else data.get("markets", data.get("data", []))
        print(f"\nQuery '{params}': got {len(markets)} markets")

        within_1day = 0
        within_2day = 0
        for m in markets:
            end_str = m.get("end_date")
            if not end_str:
                continue
            try:
                if str(end_str).endswith("Z"):
                    end_str = str(end_str)[:-1] + "+00:00"
                end = datetime.fromisoformat(str(end_str))
                if end.tzinfo is None:
                    end = end.replace(tzinfo=timezone.utc)
                days = (end - now).total_seconds() / 86400.0
                if 0.01 < days <= 1.0:
                    within_1day += 1
                if 0.01 < days <= 2.0:
                    within_2day += 1
            except Exception:
                continue

        print(f"  Within 1 day: {within_1day}")
        print(f"  Within 2 days: {within_2day}")

        # Show first few near-expiry if we found any
        if within_1day > 0 or within_2day > 0:
            print(f"\n  Near-expiry samples:")
            shown = 0
            for m in markets:
                end_str = m.get("end_date")
                if not end_str:
                    continue
                try:
                    if str(end_str).endswith("Z"):
                        end_str = str(end_str)[:-1] + "+00:00"
                    end = datetime.fromisoformat(str(end_str))
                    if end.tzinfo is None:
                        end = end.replace(tzinfo=timezone.utc)
                    days = (end - now).total_seconds() / 86400.0
                    if 0.01 < days <= 2.0:
                        q = str(m.get("question") or "?")[:55]
                        liq = float(m.get("liquidity") or 0)
                        prices = m.get("outcome_prices") or []
                        print(f"    {q:<55s} days={days:.2f} liq={liq:.0f} prices={prices}")
                        shown += 1
                        if shown >= 20:
                            break
                except Exception:
                    continue

        if len(markets) > 1000:
            break  # got enough data
    except Exception as e:
        print(f"  Error: {e}")
        continue
