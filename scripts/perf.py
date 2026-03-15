"""Fetch and display trading performance summary."""
import urllib.request, json, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

resp = urllib.request.urlopen('http://localhost:8000/api/trader-orchestrator/live/performance', timeout=30)
data = json.loads(resp.read())

summary = data.get("summary", {})
print("=== LIVE TRADING PERFORMANCE ===")
print(f"  Net Realized P&L:   ${summary.get('net_realized_pnl', 0):.2f}")
print(f"  Gross Profit:       ${summary.get('gross_profit', 0):.2f}")
print(f"  Gross Loss:         ${summary.get('gross_loss', 0):.2f}")
print(f"  Profit Factor:      {summary.get('profit_factor', 0):.2f}")
print(f"  Total Round Trips:  {summary.get('total_round_trips', 0)}")
print(f"  Winning:            {summary.get('winning_round_trips', 0)}")
print(f"  Losing:             {summary.get('losing_round_trips', 0)}")
print(f"  Win Rate:           {summary.get('win_rate_percent', 0):.1f}%")
print(f"  Avg ROI:            {summary.get('avg_roi_percent', 0):.2f}%")
print()

open_lots = data.get("open_lots", [])
print(f"=== OPEN POSITIONS ({len(open_lots)}) ===")
total_unrealized = 0.0
for lot in open_lots:
    title = lot.get("market_title", "?")[:50]
    outcome = lot.get("outcome", "?")
    entry = lot.get("entry_price", 0)
    current = lot.get("current_price", 0)
    size = lot.get("size", 0)
    notional = lot.get("notional", 0)
    unrealized = lot.get("unrealized_pnl", 0) or ((current - entry) * size if current and entry and size else 0)
    total_unrealized += unrealized
    pnl_str = f"${unrealized:+.2f}" if unrealized else "N/A"
    print(f"  {outcome:3s} {title:<50s} entry={entry:.3f} mark={current:.3f} notional=${notional:.1f} upnl={pnl_str}")

print(f"\n  Total Unrealized P&L: ${total_unrealized:.2f}")
print(f"  Total P&L (realized + unrealized): ${summary.get('net_realized_pnl', 0) + total_unrealized:.2f}")

round_trips = data.get("round_trips", [])
if round_trips:
    recent = sorted(round_trips, key=lambda x: x.get("closed_at", ""), reverse=True)[:15]
    print(f"\n=== RECENT CLOSED TRADES (last 15 of {len(round_trips)}) ===")
    for rt in recent:
        title = rt.get("market_title", "?")[:45]
        outcome = rt.get("outcome", "?")
        pnl = rt.get("realized_pnl", 0)
        roi = rt.get("roi_percent", 0)
        hold = rt.get("hold_time_hours", 0)
        print(f"  {outcome:3s} {title:<45s} pnl=${pnl:+.2f} roi={roi:+.1f}% hold={hold:.1f}h")
