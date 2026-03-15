"""Check opportunity counts by strategy."""
import urllib.request, json, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

resp = urllib.request.urlopen('http://localhost:8000/api/opportunities', timeout=15)
data = json.loads(resp.read())

by_strategy = {}
for opp in data:
    s = opp.get("strategy", "unknown")
    by_strategy.setdefault(s, []).append(opp)

print(f"=== OPPORTUNITIES BY STRATEGY (total={len(data)}) ===")
for strategy, opps in sorted(by_strategy.items(), key=lambda x: -len(x[1])):
    print(f"  {strategy}: {len(opps)}")

# Show tail_end_carry details if present
tail = by_strategy.get("tail_end_carry", [])
if tail:
    print(f"\n=== TAIL END CARRY DETAILS ({len(tail)}) ===")
    for opp in tail[:10]:
        title = str(opp.get("title") or opp.get("market_question") or "?")[:60]
        roi = opp.get("roi_percent", 0) or 0
        print(f"  {title:<60s} roi={roi:.1f}%")
else:
    print("\n  NO TAIL END CARRY OPPORTUNITIES FOUND")

# Check scanner status
try:
    resp2 = urllib.request.urlopen('http://localhost:8000/api/worker-status', timeout=10)
    workers = json.loads(resp2.read())
    for w in workers if isinstance(workers, list) else [workers]:
        name = w.get("name", "")
        if "scan" in name.lower():
            print(f"\n=== SCANNER STATUS ===")
            print(f"  Name: {name}")
            print(f"  Running: {w.get('running')}")
            print(f"  Activity: {w.get('current_activity', 'N/A')}")
            print(f"  Last run: {w.get('last_run_at', 'N/A')}")
            stats = w.get("stats", {})
            if stats:
                for k, v in stats.items():
                    print(f"  {k}: {v}")
except Exception as e:
    print(f"\nCouldn't check worker status: {e}")

# Check market catalog size
try:
    resp3 = urllib.request.urlopen('http://localhost:8000/api/market-catalog-stats', timeout=10)
    catalog = json.loads(resp3.read())
    print(f"\n=== MARKET CATALOG ===")
    for k, v in catalog.items():
        print(f"  {k}: {v}")
except Exception:
    pass
