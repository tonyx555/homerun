"""Post-run analysis of scripts/feed_latency_ticks.jsonl.

Produces:
  - Per (source, asset) latency + interarrival gap histograms
  - Chainlink-RTDS vs Binance-direct price drift (proxy for stream staleness)
  - Counts of fast (<200ms gap) bursts vs slow (>1500ms) idles on Chainlink

Run: backend/venv/Scripts/python.exe scripts/analyze_feed_latency.py
"""

from __future__ import annotations

import json
from collections import defaultdict


PATH = "scripts/feed_latency_ticks.jsonl"


def percentile(xs, p):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    k = (len(xs) - 1) * p / 100.0
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    if f == c:
        return xs[f]
    return xs[f] + (xs[c] - xs[f]) * (k - f)


def histogram(xs, bins):
    """Bucket xs into bins (right-edges). Returns list of (label, count)."""
    out = []
    for i, edge in enumerate(bins):
        prev = bins[i - 1] if i > 0 else float("-inf")
        n = sum(1 for x in xs if prev < x <= edge)
        out.append((f"<={edge}", n))
    n_over = sum(1 for x in xs if x > bins[-1])
    out.append((f">{bins[-1]}", n_over))
    return out


def main() -> None:
    rows = []
    with open(PATH) as f:
        for line in f:
            rows.append(json.loads(line))

    by = defaultdict(list)
    for r in rows:
        by[(r["source"], r["asset"])].append(r)

    print(f"Total ticks: {len(rows)}")
    print()

    # Per-source-asset stats
    for (src, asset), ts in sorted(by.items()):
        ts_sorted = sorted(ts, key=lambda r: r["recv_ts_ms"])
        recvs = [r["recv_ts_ms"] for r in ts_sorted]
        gaps = [recvs[i] - recvs[i - 1] for i in range(1, len(recvs))]
        if src.endswith("_rtds"):
            lats = [r["recv_ts_ms"] - r["msg_ts_ms"] for r in ts_sorted if r["msg_ts_ms"] != r["recv_ts_ms"]]
        else:
            lats = []
        elapsed_s = (recvs[-1] - recvs[0]) / 1000.0 if len(recvs) > 1 else 1.0
        rate = len(ts) / max(1.0, elapsed_s)
        print(f"=== {src} / {asset} ===")
        print(f"  n={len(ts)}  rate={rate:.2f}/s  elapsed={elapsed_s:.0f}s")
        if lats:
            print(f"  latency_ms  p50={percentile(lats,50):.0f}  p95={percentile(lats,95):.0f}  p99={percentile(lats,99):.0f}  max={max(lats):.0f}")
        print(f"  gap_ms      p50={percentile(gaps,50):.0f}  p95={percentile(gaps,95):.0f}  p99={percentile(gaps,99):.0f}  max={max(gaps) if gaps else 0:.0f}")

        if gaps:
            edges = [50, 100, 200, 500, 1000, 1500, 2000, 5000]
            h = histogram(gaps, edges)
            total = sum(c for _, c in h)
            print(f"  gap histogram (n={total}):")
            for label, count in h:
                pct = 100.0 * count / max(1, total)
                bar = "#" * int(pct / 2)
                print(f"    {label:>8s}  {count:5d}  {pct:5.1f}%  {bar}")
        print()

    # Cross-source price drift: Chainlink RTDS vs Binance direct, for BTC
    print("=== Chainlink RTDS BTC vs Binance Direct BTC: price drift ===")
    cl_btc = sorted(
        [r for r in rows if r["source"] == "chainlink_rtds" and r["asset"] == "BTC"],
        key=lambda r: r["recv_ts_ms"],
    )
    bn_btc = sorted(
        [r for r in rows if r["source"] == "binance_direct" and r["asset"] == "BTC"],
        key=lambda r: r["recv_ts_ms"],
    )
    if cl_btc and bn_btc:
        # For each Chainlink tick, find the closest Binance tick by recv time
        diffs = []
        bn_idx = 0
        for c in cl_btc:
            while bn_idx + 1 < len(bn_btc) and abs(bn_btc[bn_idx + 1]["recv_ts_ms"] - c["recv_ts_ms"]) < abs(bn_btc[bn_idx]["recv_ts_ms"] - c["recv_ts_ms"]):
                bn_idx += 1
            diff_usd = c["price"] - bn_btc[bn_idx]["price"]
            diffs.append(diff_usd)
        abs_diffs = [abs(d) for d in diffs]
        print(f"  Pairs compared: {len(diffs)}")
        print(f"  Chainlink - Binance (USD)  p50={percentile(diffs,50):+.2f}  p95={percentile(diffs,95):+.2f}  p99={percentile(diffs,99):+.2f}")
        print(f"  |Chainlink - Binance| (USD)  p50={percentile(abs_diffs,50):.2f}  p95={percentile(abs_diffs,95):.2f}  p99={percentile(abs_diffs,99):.2f}  max={max(abs_diffs):.2f}")
        ref_price = sum(c["price"] for c in cl_btc) / len(cl_btc)
        print(f"  As % of mid (~${ref_price:,.0f}): p50={100*percentile(abs_diffs,50)/ref_price:.4f}%  p99={100*percentile(abs_diffs,99)/ref_price:.4f}%")
    print()

    # Bursts vs idles on chainlink_rtds BTC
    if cl_btc and len(cl_btc) > 1:
        gaps = [cl_btc[i]["recv_ts_ms"] - cl_btc[i-1]["recv_ts_ms"] for i in range(1, len(cl_btc))]
        n_fast = sum(1 for g in gaps if g < 200)
        n_subsec = sum(1 for g in gaps if g < 1000)
        n_slow = sum(1 for g in gaps if g > 1500)
        print(f"=== Chainlink RTDS BTC inter-arrival regimes ===")
        print(f"  Total gaps: {len(gaps)}")
        print(f"  <200ms (sub-second burst):  {n_fast}  ({100*n_fast/len(gaps):.1f}%)")
        print(f"  <1000ms (sub-second total): {n_subsec}  ({100*n_subsec/len(gaps):.1f}%)")
        print(f"  >1500ms (idle/stale):       {n_slow}  ({100*n_slow/len(gaps):.1f}%)")


if __name__ == "__main__":
    main()
