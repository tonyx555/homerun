"""Diagnose why tail_end_carry finds few opportunities."""
import urllib.request, json, sys, io
from datetime import datetime, timezone
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

resp = urllib.request.urlopen('http://localhost:8000/api/markets?limit=100000', timeout=30)
data = json.loads(resp.read())
markets = data if isinstance(data, list) else data.get("markets", data.get("data", []))
print(f"Total markets from API: {len(markets)}")

now = datetime.now(timezone.utc)
excluded_keywords = [
    "bitcoin", "ethereum", "lol:", "counter-strike",
    "tweets", "league of legends", "esports", "rift legends",
    "dota", "valorant", "cs2", "cs:", "esl pro",
]

stage_counts = {
    "total": 0,
    "active": 0,
    "has_end_date": 0,
    "within_1_day": 0,
    "not_keyword_excluded": 0,
    "has_liquidity_3500": 0,
    "has_2_tokens": 0,
    "price_in_band_85_91": 0,
}

passing_markets = []

for m in markets:
    stage_counts["total"] += 1
    if m.get("closed") or not m.get("active", True):
        continue
    stage_counts["active"] += 1

    end_date_str = m.get("end_date") or m.get("end_date_iso")
    if not end_date_str:
        continue
    stage_counts["has_end_date"] += 1

    try:
        if end_date_str.endswith("Z"):
            end_date_str = end_date_str[:-1] + "+00:00"
        end_date = datetime.fromisoformat(end_date_str)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)
    except Exception:
        continue

    days = (end_date - now).total_seconds() / 86400.0
    if days <= 0.01 or days > 1.0:
        continue
    stage_counts["within_1_day"] += 1

    question = str(m.get("question") or m.get("title") or "").lower()
    group_title = str(m.get("group_title") or m.get("event_title") or "").lower()
    market_text = question + " " + group_title
    keyword_blocked = False
    for kw in excluded_keywords:
        if kw.lower() in market_text:
            keyword_blocked = True
            break
    if keyword_blocked:
        continue
    stage_counts["not_keyword_excluded"] += 1

    liquidity = float(m.get("liquidity") or 0)
    if liquidity < 3500:
        continue
    stage_counts["has_liquidity_3500"] += 1

    tokens = m.get("clob_token_ids") or m.get("token_ids") or []
    outcome_prices = m.get("outcome_prices") or m.get("outcomePrices") or []
    if len(tokens) < 2 and len(outcome_prices) < 2:
        continue
    stage_counts["has_2_tokens"] += 1

    # Check if any side is in the 0.85 - 0.909 band
    prices_list = []
    if outcome_prices:
        for p in outcome_prices:
            try:
                prices_list.append(float(p))
            except (ValueError, TypeError):
                pass
    yes_price = float(m.get("yes_price") or m.get("outcomePrices", ["0"])[0] or 0)
    no_price = float(m.get("no_price") or 0)
    if yes_price > 0:
        prices_list.append(yes_price)
    if no_price > 0:
        prices_list.append(no_price)

    in_band = False
    for p in prices_list:
        if 0.85 <= p <= 0.909:
            in_band = True
            break
    if in_band:
        stage_counts["price_in_band_85_91"] += 1
        passing_markets.append({
            "question": question[:60],
            "days": days,
            "liquidity": liquidity,
            "prices": prices_list,
        })

print("\n=== FILTER FUNNEL ===")
for stage, count in stage_counts.items():
    print(f"  {stage}: {count}")

# Also check: how many markets are within 1 day but blocked by keywords?
keyword_blocked_count = 0
keyword_blocked_by = {}
for m in markets:
    if m.get("closed") or not m.get("active", True):
        continue
    end_date_str = m.get("end_date") or m.get("end_date_iso")
    if not end_date_str:
        continue
    try:
        if end_date_str.endswith("Z"):
            end_date_str = end_date_str[:-1] + "+00:00"
        end_date = datetime.fromisoformat(end_date_str)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)
    except Exception:
        continue
    days = (end_date - now).total_seconds() / 86400.0
    if days <= 0.01 or days > 1.0:
        continue
    question = str(m.get("question") or m.get("title") or "").lower()
    group_title = str(m.get("group_title") or m.get("event_title") or "").lower()
    market_text = question + " " + group_title
    for kw in excluded_keywords:
        if kw.lower() in market_text:
            keyword_blocked_count += 1
            keyword_blocked_by[kw] = keyword_blocked_by.get(kw, 0) + 1
            break

print(f"\n=== KEYWORD EXCLUSIONS (within 1 day) ===")
print(f"  Total blocked: {keyword_blocked_count}")
for kw, count in sorted(keyword_blocked_by.items(), key=lambda x: -x[1]):
    print(f"    '{kw}': {count}")

# How many within 1 day have liquidity >= 3500?
liq_analysis = {"<500": 0, "500-1000": 0, "1000-2000": 0, "2000-3500": 0, "3500+": 0}
for m in markets:
    if m.get("closed") or not m.get("active", True):
        continue
    end_date_str = m.get("end_date") or m.get("end_date_iso")
    if not end_date_str:
        continue
    try:
        if end_date_str.endswith("Z"):
            end_date_str = end_date_str[:-1] + "+00:00"
        end_date = datetime.fromisoformat(end_date_str)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)
    except Exception:
        continue
    days = (end_date - now).total_seconds() / 86400.0
    if days <= 0.01 or days > 1.0:
        continue
    question = str(m.get("question") or m.get("title") or "").lower()
    group_title = str(m.get("group_title") or m.get("event_title") or "").lower()
    market_text = question + " " + group_title
    keyword_blocked = any(kw.lower() in market_text for kw in excluded_keywords)
    if keyword_blocked:
        continue
    liq = float(m.get("liquidity") or 0)
    if liq >= 3500:
        liq_analysis["3500+"] += 1
    elif liq >= 2000:
        liq_analysis["2000-3500"] += 1
    elif liq >= 1000:
        liq_analysis["1000-2000"] += 1
    elif liq >= 500:
        liq_analysis["500-1000"] += 1
    else:
        liq_analysis["<500"] += 1

print(f"\n=== LIQUIDITY DISTRIBUTION (within 1 day, not keyword-blocked) ===")
for bucket, count in liq_analysis.items():
    print(f"  {bucket}: {count}")

if passing_markets:
    print(f"\n=== MARKETS PASSING ALL FILTERS ({len(passing_markets)}) ===")
    for pm in passing_markets[:15]:
        print(f"  {pm['question']:<60s} days={pm['days']:.2f} liq={pm['liquidity']:.0f}")
