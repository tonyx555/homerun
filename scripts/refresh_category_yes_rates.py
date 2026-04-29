"""Refresh ``data/category_yes_rates.json`` from live DB.

Computes per-category YES-resolution rates for resolved binary markets
(``len(outcome_prices)==2`` and ``outcome_prices`` terminal at [0,1]/[1,0]).

Multi-outcome events (NegRisk bundles, multi-candidate elections) are
NOT lumped into a single category rate — each binary sub-market is
counted independently, so a 5-candidate election contributes 5 rows
to its category, with YES rate ≈ 1/5 by construction.

Run periodically (e.g. nightly). The SDK reads the JSON lazily.

Usage:
    python scripts/refresh_category_yes_rates.py
    python scripts/refresh_category_yes_rates.py --min-samples 50
"""
from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import asyncpg

ROOT = Path(__file__).resolve().parents[1]
OUT_PATH = ROOT / "backend" / "data" / "category_yes_rates.json"
DB_DSN = "postgres://homerun:homerun@127.0.0.1:5432/homerun"

QUERY = """
WITH terminal AS (
  SELECT
    cm.condition_id,
    (cm.extra_data::jsonb->'outcome_prices'->>0)::float AS yes_p
  FROM cached_markets cm
  WHERE cm.extra_data::jsonb->>'closed' = 'true'
    AND jsonb_typeof(cm.extra_data::jsonb->'outcome_prices') = 'array'
    AND jsonb_array_length(cm.extra_data::jsonb->'outcome_prices') = 2
    AND ((cm.extra_data::jsonb->'outcome_prices'->>0)::float IN (0.0, 1.0))
),
opp_cats AS (
  SELECT DISTINCT ON (m.value->>'condition_id')
    m.value->>'condition_id' AS condition_id,
    LOWER(oh.positions_data::jsonb->>'category') AS category
  FROM opportunity_history oh
  CROSS JOIN LATERAL jsonb_array_elements(oh.positions_data::jsonb->'markets') m
  WHERE oh.positions_data::jsonb->>'category' IS NOT NULL
    AND m.value->>'condition_id' IS NOT NULL
  ORDER BY m.value->>'condition_id', oh.detected_at DESC
)
SELECT oc.category, COUNT(*) AS n, AVG(t.yes_p)::float AS yes_rate
FROM opp_cats oc
JOIN terminal t ON t.condition_id = oc.condition_id
GROUP BY oc.category
HAVING COUNT(*) >= $1
ORDER BY n DESC
"""


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-samples", type=int, default=30)
    ap.add_argument("--dsn", default=DB_DSN)
    args = ap.parse_args()

    conn = await asyncpg.connect(args.dsn)
    try:
        rows = await conn.fetch(QUERY, args.min_samples)
    finally:
        await conn.close()

    rates: dict[str, dict] = {}
    for row in rows:
        cat = (row["category"] or "").strip().lower()
        if not cat:
            continue
        rate = max(0.01, min(0.99, float(row["yes_rate"])))
        rates[cat] = {
            "rate": rate,
            "samples": int(row["n"]),
        }

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "min_samples": args.min_samples,
        "rates": {cat: row["rate"] for cat, row in rates.items()},
        "samples": {cat: row["samples"] for cat, row in rates.items()},
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload, indent=2))
    print(f"Wrote {len(rates)} categories to {OUT_PATH}")
    for cat, row in sorted(rates.items(), key=lambda kv: -kv[1]["samples"])[:20]:
        print(f"  {cat:<25} rate={row['rate']:.3f}  n={row['samples']}")


if __name__ == "__main__":
    asyncio.run(main())
