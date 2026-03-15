"""Check scanner and market catalog state."""
import urllib.request, json, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# Try various endpoints to find scanner info
endpoints = [
    '/api/scanner/status',
    '/api/scanner-status',
    '/api/workers',
    '/api/worker-snapshots',
    '/api/strategies',
    '/api/strategies/tail_end_carry',
]

for ep in endpoints:
    try:
        resp = urllib.request.urlopen(f'http://localhost:8000{ep}', timeout=5)
        data = json.loads(resp.read())
        print(f"=== {ep} ===")
        if isinstance(data, list):
            print(f"  (list of {len(data)} items)")
            if len(data) > 0:
                first = data[0]
                if isinstance(first, dict):
                    for k in list(first.keys())[:10]:
                        print(f"    {k}: {first[k]}")
        elif isinstance(data, dict):
            for k, v in list(data.items())[:20]:
                val_str = str(v)
                if len(val_str) > 100:
                    val_str = val_str[:100] + "..."
                print(f"  {k}: {val_str}")
        print()
    except urllib.error.HTTPError as e:
        pass  # skip 404s silently
    except Exception as e:
        print(f"  {ep}: {e}\n")

# Check total active markets in catalog
try:
    resp = urllib.request.urlopen('http://localhost:8000/api/markets?active=true&limit=5', timeout=10)
    data = json.loads(resp.read())
    if isinstance(data, dict):
        total = data.get("total", data.get("count", "?"))
        print(f"=== ACTIVE MARKETS ===")
        print(f"  Total: {total}")
    elif isinstance(data, list):
        print(f"=== ACTIVE MARKETS (from list) ===")
        print(f"  Returned: {len(data)}")
except Exception:
    pass

# Check events/markets catalog
try:
    resp = urllib.request.urlopen('http://localhost:8000/api/events?limit=5', timeout=10)
    data = json.loads(resp.read())
    if isinstance(data, dict):
        total = data.get("total", data.get("count", "?"))
        print(f"\n=== EVENTS ===")
        print(f"  Total: {total}")
    elif isinstance(data, list):
        print(f"\n=== EVENTS (from list) ===")
        print(f"  Returned: {len(data)}")
except Exception:
    pass
