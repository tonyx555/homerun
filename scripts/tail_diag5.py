"""Check trader DB config for tail_end_carry keyword exclusions."""
import urllib.request, json, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# Get full trader details including strategy params
resp = urllib.request.urlopen('http://localhost:8000/api/traders', timeout=10)
traders = json.loads(resp.read())
if isinstance(traders, dict):
    traders = traders.get("traders", traders.get("data", []))

for t in (traders if isinstance(traders, list) else []):
    name = t.get("name", "?")
    if "tail" not in name.lower():
        continue
    print(f"=== TRADER: {name} (id={t.get('id','?')}) ===")

    # Check source_configs_json
    source_configs = t.get("source_configs_json") or t.get("source_configs") or []
    if isinstance(source_configs, str):
        source_configs = json.loads(source_configs)

    for cfg in (source_configs if isinstance(source_configs, list) else []):
        if not isinstance(cfg, dict):
            continue
        strategy_key = cfg.get("strategy_key", "?")
        params = cfg.get("strategy_params", {})
        print(f"  Strategy: {strategy_key}")
        if "tail" in strategy_key.lower():
            exclude_kws = params.get("exclude_market_keywords", [])
            print(f"  exclude_market_keywords: {exclude_kws}")
            max_days = params.get("max_days_to_resolution")
            print(f"  max_days_to_resolution: {max_days}")
            min_liq = params.get("min_liquidity")
            print(f"  min_liquidity: {min_liq}")
            # Print ALL params
            print(f"  ALL strategy_params:")
            for k, v in sorted(params.items()):
                print(f"    {k}: {v}")
        print()
