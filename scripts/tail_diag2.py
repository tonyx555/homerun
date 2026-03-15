"""Diagnose tail carry - check market catalog directly."""
import urllib.request, json, sys, io
from datetime import datetime, timezone
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

now = datetime.now(timezone.utc)
print(f"Current time: {now.isoformat()}")

# Check what the markets API returns
resp = urllib.request.urlopen('http://localhost:8000/api/markets?limit=5', timeout=10)
data = json.loads(resp.read())
print(f"\nAPI response type: {type(data).__name__}")
if isinstance(data, dict):
    print(f"Keys: {list(data.keys())}")
    for k, v in data.items():
        if isinstance(v, (int, float, str, bool)):
            print(f"  {k}: {v}")
    items = data.get("markets") or data.get("data") or data.get("items") or []
elif isinstance(data, list):
    items = data
else:
    items = []

if items:
    print(f"\nFirst market sample:")
    m = items[0]
    for k, v in m.items():
        val_str = str(v)
        if len(val_str) > 80:
            val_str = val_str[:80] + "..."
        print(f"  {k}: {val_str}")

# Try scanner's internal state
try:
    resp2 = urllib.request.urlopen('http://localhost:8000/api/scanner/status', timeout=10)
    scanner = json.loads(resp2.read())
    tiered = scanner.get("tiered_scanning", {})
    print(f"\n=== SCANNER TIERED CONFIG ===")
    for k, v in tiered.items():
        print(f"  {k}: {v}")
except Exception as e:
    print(f"Scanner status error: {e}")

# Check how many markets the scanner actually has in cache
try:
    resp3 = urllib.request.urlopen('http://localhost:8000/api/scanner/diagnostics', timeout=10)
    diag = json.loads(resp3.read())
    print(f"\n=== SCANNER DIAGNOSTICS ===")
    for k, v in diag.items():
        val_str = str(v)
        if len(val_str) > 100:
            val_str = val_str[:100] + "..."
        print(f"  {k}: {val_str}")
except Exception:
    pass

# Check if there's an end_date field and what values look like
print(f"\n=== END DATE CHECK (first 5 markets) ===")
for m in items[:5]:
    end = m.get("end_date") or m.get("end_date_iso") or m.get("endDate") or "NONE"
    question = str(m.get("question") or m.get("title") or "?")[:50]
    print(f"  {question}: end_date={end}")
    if end != "NONE":
        try:
            if str(end).endswith("Z"):
                end = str(end)[:-1] + "+00:00"
            parsed = datetime.fromisoformat(str(end))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            days = (parsed - now).total_seconds() / 86400.0
            print(f"    -> days to resolution: {days:.2f}")
        except Exception as e:
            print(f"    -> parse error: {e}")
