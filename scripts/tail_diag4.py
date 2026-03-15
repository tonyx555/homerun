"""Check tail_end_carry config from DB and count markets at each filter stage."""
import urllib.request, json, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# Check the strategy config as loaded by the scanner
try:
    resp = urllib.request.urlopen('http://localhost:8000/api/scanner/status', timeout=10)
    status = json.loads(resp.read())
    strategies = status.get("strategies", [])
    for s in strategies:
        if s.get("type") == "tail_end_carry" or "tail" in str(s.get("name", "")).lower():
            print("=== TAIL END CARRY STRATEGY STATUS ===")
            for k, v in s.items():
                print(f"  {k}: {v}")
except Exception as e:
    print(f"Scanner status error: {e}")

# Check trader strategy params from the DB
try:
    resp = urllib.request.urlopen('http://localhost:8000/api/traders', timeout=10)
    traders = json.loads(resp.read())
    if isinstance(traders, dict):
        traders = traders.get("traders", traders.get("data", []))
    for t in traders if isinstance(traders, list) else []:
        strategies = t.get("strategies") or t.get("strategy_params") or {}
        tail_cfg = None
        if isinstance(strategies, dict):
            tail_cfg = strategies.get("tail_end_carry")
        elif isinstance(strategies, list):
            for sp in strategies:
                if isinstance(sp, dict) and sp.get("type") == "tail_end_carry":
                    tail_cfg = sp
                    break
        if tail_cfg:
            trader_name = t.get("name") or t.get("id", "?")
            print(f"\n=== TRADER '{trader_name}' TAIL CARRY CONFIG ===")
            if isinstance(tail_cfg, dict):
                for k, v in sorted(tail_cfg.items()):
                    print(f"  {k}: {v}")
            else:
                print(f"  value: {tail_cfg}")
except Exception as e:
    print(f"Traders error: {e}")

# Check the quality filter that runs AFTER detection
try:
    resp = urllib.request.urlopen('http://localhost:8000/api/opportunities?include_quality=true', timeout=10)
    opps = json.loads(resp.read())
    if isinstance(opps, list):
        tail_opps = [o for o in opps if o.get("strategy") == "tail_end_carry"]
        print(f"\n=== TAIL CARRY QUALITY REPORTS ({len(tail_opps)} opps) ===")
        for o in tail_opps:
            qr = o.get("quality_report") or {}
            print(f"  {str(o.get('title','?'))[:50]}: roi={o.get('roi_percent',0):.1f}% passed={qr.get('passed')} reason={qr.get('rejection_reason','OK')}")
except Exception:
    pass

# Check what the scanner's last detection returned
try:
    resp = urllib.request.urlopen('http://localhost:8000/api/scanner/last-detection?strategy=tail_end_carry', timeout=10)
    data = json.loads(resp.read())
    print(f"\n=== LAST DETECTION ===")
    if isinstance(data, dict):
        for k, v in data.items():
            val = str(v)
            if len(val) > 100:
                val = val[:100] + "..."
            print(f"  {k}: {val}")
except Exception:
    pass
