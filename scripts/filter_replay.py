"""Filter-replay harness — fast.

Pulls historical opportunities from opportunity_history and replays them
through a strategy's filter method (e.g. _is_sports_or_match_market).
Useful for fast A/B iteration on rejection logic without paying the cost
of a full live backtest.

Usage:
    python scripts/filter_replay.py stat_arb sports --limit 5000
    python scripts/filter_replay.py stat_arb sports --since 7days --strategy-version v1
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterable

import asyncpg

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "backend" / "services" / "strategies"
SNAP_DIR = ROOT / "scripts" / "iter_snapshots"

DB_DSN = "postgres://homerun:homerun@127.0.0.1:5432/homerun"


def load_strategy_module(source_path: Path):
    """Load a strategy file as an isolated module so we can call its
    instance methods (e.g. _is_sports_or_match_market) directly."""
    # Patch relative imports to absolute so the loader doesn't choke
    src = source_path.read_text(encoding="utf-8")
    src = re.sub(r"^from \.(\w+)", r"from services.strategies.\1", src, flags=re.MULTILINE)
    # Inject backend on path so imports resolve
    backend_path = str(ROOT / "backend")
    if backend_path not in sys.path:
        sys.path.insert(0, backend_path)
    # Compile and exec into a fresh module dict; avoid registering with sys.modules
    mod = SimpleNamespace()
    code = compile(src, str(source_path), "exec")
    ns: dict[str, Any] = {"__name__": f"strategy_replay_{source_path.stem}", "__file__": str(source_path)}
    exec(code, ns)  # noqa: S102 - intentional, sandboxed by import allowlist on disk
    return ns


def make_market(market_dict: dict) -> SimpleNamespace:
    """Wrap an opportunity_history market dict to look like a Market ORM row."""
    return SimpleNamespace(
        id=str(market_dict.get("id", "")),
        question=market_dict.get("question", "") or "",
        slug=market_dict.get("slug", "") or "",
        event_slug=market_dict.get("event_slug", "") or "",
        group_item_title=market_dict.get("group_item_title", "") or "",
        tags=market_dict.get("tags", []) or [],
        liquidity=float(market_dict.get("liquidity", 0) or 0),
        volume=float(market_dict.get("volume", 0) or 0),
        condition_id=market_dict.get("condition_id", "") or "",
        clob_token_ids=market_dict.get("clob_token_ids", []) or [],
        outcome_prices=market_dict.get("outcome_prices", []) or [],
        yes_price=float(market_dict.get("yes_price", 0.5) or 0.5),
        no_price=float(market_dict.get("no_price", 0.5) or 0.5),
        end_date=None,
        active=True,
        closed=False,
        platform=market_dict.get("platform", "polymarket") or "polymarket",
        neg_risk=bool(market_dict.get("neg_risk", False)),
    )


def make_event(positions_blob: dict) -> SimpleNamespace:
    return SimpleNamespace(
        title=positions_blob.get("event_title", "") or "",
        category=positions_blob.get("category", "") or "",
        markets=[make_market(m) for m in (positions_blob.get("markets") or [])],
        neg_risk=False,
        closed=False,
    )


async def fetch_opportunities(strategy_type: str, *, limit: int, since: datetime | None) -> list[dict]:
    where = ["strategy_type = $1"]
    params: list[Any] = [strategy_type]
    if since is not None:
        where.append(f"detected_at >= ${len(params) + 1}")
        params.append(since)
    params.append(limit)
    sql = (
        "SELECT id, title, total_cost, expected_roi, risk_score, positions_data, detected_at "
        f"FROM opportunity_history WHERE {' AND '.join(where)} "
        f"ORDER BY detected_at DESC LIMIT ${len(params)}"
    )

    conn = await asyncpg.connect(DB_DSN)
    try:
        rows = await conn.fetch(sql, *params)
    finally:
        await conn.close()

    out: list[dict] = []
    for row in rows:
        blob = row["positions_data"]
        if isinstance(blob, str):
            try:
                blob = json.loads(blob)
            except Exception:
                blob = {}
        out.append({
            "id": row["id"],
            "title": row["title"] or "",
            "expected_roi": float(row["expected_roi"] or 0.0),
            "risk_score": float(row["risk_score"] or 0.0),
            "total_cost": float(row["total_cost"] or 0.0),
            "positions": blob if isinstance(blob, dict) else {},
            "detected_at": row["detected_at"],
        })
    return out


def replay_filter(strategy: str, filter_name: str, opps: list[dict], ns_v0: dict, ns_v1: dict) -> dict:
    """Apply filter v0 (baseline) and v1 (candidate) to each opportunity.

    Returns counts + a sample of disagreements.
    """
    cls_name = "StatArbStrategy" if strategy == "stat_arb" else "NegRiskStrategy"
    inst_v0 = ns_v0[cls_name]()
    inst_v1 = ns_v1[cls_name]()

    fn_v0 = getattr(inst_v0, filter_name, None) or getattr(inst_v0, "_is_sports_parlay")
    fn_v1 = getattr(inst_v1, filter_name, None) or getattr(inst_v1, "_is_sports_parlay")

    rejected_v0 = 0
    rejected_v1 = 0
    new_rejects: list[dict] = []  # rejected by v1 only
    new_admits: list[dict] = []   # admitted by v1, was rejected by v0 (regression)

    for opp in opps:
        blob = opp["positions"]
        markets = (blob.get("markets") or [])[:1]
        if not markets:
            continue
        market = make_market(markets[0])
        event = make_event(blob)

        # v0 fn might or might not accept event arg
        try:
            r0 = bool(fn_v0(market, event=event))
        except TypeError:
            r0 = bool(fn_v0(market))
        try:
            r1 = bool(fn_v1(market, event=event))
        except TypeError:
            r1 = bool(fn_v1(market))

        if r0:
            rejected_v0 += 1
        if r1:
            rejected_v1 += 1
        if r1 and not r0 and len(new_rejects) < 20:
            new_rejects.append({"title": opp["title"][:90], "q": market.question[:80]})
        if r0 and not r1 and len(new_admits) < 20:
            new_admits.append({"title": opp["title"][:90], "q": market.question[:80]})

    return {
        "n": len(opps),
        "rejected_v0": rejected_v0,
        "rejected_v1": rejected_v1,
        "new_rejects_sample": new_rejects,
        "new_admits_sample": new_admits,
    }


def categorize_titles(opps: Iterable[dict]) -> Counter:
    sports_kw = (
        " vs.", " vs ", "draw", "set 1", "game 1", "game 2", "game 3",
        "map 1", "map 2", "map 3", "moneyline", "raptors", "cavaliers",
        "lakers", "celtics", "knicks", "lol:", "ufc", "atp", "wta", "nba",
        "nfl", "mlb", "nhl", "premier league", "la liga", "kbo", "lpl",
        "bundesliga", "champions league", "serie a", "ncaa", "valorant",
        "counter-strike", "esports",
    )
    crypto_kw = ("bitcoin", "ethereum", "btc", "eth", "doge", "xrp",
                 "solana", "crypto")
    weather_kw = ("temperature", "weather", "rain", "snow", "high in")
    out = Counter()
    for o in opps:
        t = (o.get("title") or "").lower()
        q = ((o.get("positions", {}).get("markets") or [{}])[0].get("question") or "").lower() if o.get("positions") else ""
        s = t + " " + q
        if any(k in s for k in sports_kw):
            out["sports"] += 1
        elif any(k in s for k in crypto_kw):
            out["crypto"] += 1
        elif any(k in s for k in weather_kw):
            out["weather"] += 1
        else:
            out["other"] += 1
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("strategy", choices=["stat_arb", "negrisk"])
    ap.add_argument("filter_name", help="method name on the strategy class")
    ap.add_argument("--limit", type=int, default=5000)
    ap.add_argument("--since", default=None,
                    help="restrict to detections in last N days, e.g. '7' or '24h'")
    ap.add_argument("--baseline", default=None,
                    help="path to baseline source (defaults to git HEAD)")
    args = ap.parse_args()

    since: datetime | None = None
    if args.since:
        s = args.since.strip().lower()
        if s.endswith("h"):
            since = datetime.now(timezone.utc) - timedelta(hours=int(s[:-1]))
        else:
            days = int(s.rstrip("d"))
            since = datetime.now(timezone.utc) - timedelta(days=days)
        # opportunity_history.detected_at is a TIMESTAMP (naive) — strip tz so
        # asyncpg accepts it.
        since = since.replace(tzinfo=None)

    print(f"\n{'='*72}")
    print(f"Filter replay: {args.strategy}.{args.filter_name}  limit={args.limit} since={since}")
    print(f"{'='*72}")

    print("[fetch] querying opportunity_history ...")
    opps = asyncio.run(fetch_opportunities(args.strategy, limit=args.limit, since=since))
    print(f"[fetch] got {len(opps)} rows")
    cat = categorize_titles(opps)
    print(f"[fetch] category mix: {dict(cat)}")

    baseline_path = Path(args.baseline) if args.baseline else SNAP_DIR / f"{args.strategy}_v0_baseline.py"
    candidate_path = SRC_DIR / f"{args.strategy}.py"

    print(f"[load] baseline:  {baseline_path}")
    print(f"[load] candidate: {candidate_path}")

    ns_v0 = load_strategy_module(baseline_path)
    ns_v1 = load_strategy_module(candidate_path)

    print(f"[replay] applying filter to {len(opps)} opportunities ...")
    res = replay_filter(args.strategy, args.filter_name, opps, ns_v0, ns_v1)

    n = res["n"]; rv0 = res["rejected_v0"]; rv1 = res["rejected_v1"]
    pct = lambda a, b: f"{(a / b * 100.0):.1f}%" if b else "0.0%"
    print(f"\n{'METRIC':<28} {'BASELINE':>12} {'CANDIDATE':>12}")
    print(f"{'opportunities':<28} {n:>12} {n:>12}")
    print(f"{'rejected':<28} {rv0:>12} {rv1:>12}")
    print(f"{'rejection rate':<28} {pct(rv0,n):>12} {pct(rv1,n):>12}")
    print(f"{'newly rejected by v1':<28} {len(res['new_rejects_sample']):>12}")
    print(f"{'newly admitted by v1 (regression)':<28} {len(res['new_admits_sample']):>12}")
    print()
    if res["new_rejects_sample"]:
        print("Sample of rows v1 rejects (v0 admitted):")
        for r in res["new_rejects_sample"][:10]:
            print(f"  - {r['q'][:80]}")
    if res["new_admits_sample"]:
        print("\nWARNING: rows v1 admitted that v0 rejected:")
        for r in res["new_admits_sample"][:10]:
            print(f"  - {r['q'][:80]}")


if __name__ == "__main__":
    main()
