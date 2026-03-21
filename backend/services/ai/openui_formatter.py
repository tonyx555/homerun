"""
OpenUI Lang formatter for structured AI tool results.

Converts structured Python dicts (from resolution analysis, sentiment analysis,
opportunity judgments, etc.) into OpenUI Lang markup that the frontend Renderer
can display as rich interactive components.

OpenUI Lang syntax: variable assignment, e.g.
  root = Stack([card1, scores])
  card1 = Card("Title", "Subtitle", "purple", [kv1, kv2])
  kv1 = KeyValue("Label", "Value", true)
"""

from __future__ import annotations


def _escape(text: str) -> str:
    """Escape quotes in string literals."""
    return text.replace('"', '\\"').replace("\n", " ")


def format_resolution_analysis(result: dict) -> str:
    """Convert resolution analysis result to OpenUI Lang."""
    clarity = result.get("clarity_score", 0)
    risk = result.get("risk_score", 0)
    confidence = result.get("confidence", 0)
    recommendation = result.get("recommendation", "review")
    findings = result.get("findings", [])
    question = _escape(result.get("question", "Unknown market"))

    lines = [
        'root = Stack([header, scores, rec])',
        f'header = Card("{_escape(question)}", "Resolution Analysis", "purple", [clarityG, riskG, confG])',
        f'clarityG = ScoreGauge("Clarity", {clarity}, "percent")',
        f'riskG = ScoreGauge("Risk", {risk}, "percent")',
        f'confG = ScoreGauge("Confidence", {confidence}, "percent")',
        'scores = Card("Findings", "", "cyan", [findingsList])',
    ]

    if findings:
        items_str = ", ".join(f'"{_escape(str(f))}"' for f in findings[:8])
        lines.append(f"findingsList = BulletList([{items_str}])")
    else:
        lines.append('findingsList = TextBlock("No specific findings.", "caption")')

    lines.append(f'rec = Recommendation("{recommendation}", "{_escape(result.get("reasoning", "See analysis above."))}")')

    return "\n".join(lines)


def format_sentiment_analysis(result: dict) -> str:
    """Convert news sentiment result to OpenUI Lang."""
    score = result.get("sentiment_score", 0)
    confidence = result.get("confidence", 0)
    sentiment = result.get("overall_sentiment", "neutral")
    impact = _escape(result.get("market_impact", "No clear impact."))
    takeaways = result.get("key_takeaways", [])

    lines = [
        "root = Stack([sentCard, takeawayCard])",
        f'sentCard = Card("News Sentiment", "{_escape(sentiment)}", "cyan", [sentBar, impactText])',
        f'sentBar = SentimentBar("Overall Sentiment", {score}, {confidence})',
        f'impactText = TextBlock("{impact}", "body")',
    ]

    if takeaways:
        items_str = ", ".join(f'"{_escape(str(t))}"' for t in takeaways[:6])
        lines.append('takeawayCard = Card("Key Takeaways", "", "purple", [takeawayList])')
        lines.append(f"takeawayList = BulletList([{items_str}])")
    else:
        lines.append('takeawayCard = Card("Key Takeaways", "", "purple", [noTakeaways])')
        lines.append('noTakeaways = TextBlock("No key takeaways identified.", "caption")')

    return "\n".join(lines)


def format_opportunity_judgment(result: dict) -> str:
    """Convert opportunity judgment result to OpenUI Lang."""
    scores = result.get("scores", {})
    recommendation = result.get("recommendation", "review")
    reasoning = _escape(result.get("reasoning", ""))

    lines = [
        "root = Stack([scoresCard, rec])",
        'scoresCard = Card("Opportunity Assessment", "", "purple", [profitG, resG, execG, effG])',
    ]

    score_map = {
        "profit_viability": ("Profit Viability", "profitG"),
        "resolution_safety": ("Resolution Safety", "resG"),
        "execution_feasibility": ("Execution Feasibility", "execG"),
        "market_efficiency": ("Market Efficiency", "effG"),
    }

    for key, (label, var) in score_map.items():
        val = scores.get(key, 0.5)
        lines.append(f'{var} = ScoreGauge("{label}", {val}, "percent")')

    lines.append(f'rec = Recommendation("{recommendation}", "{reasoning}")')

    return "\n".join(lines)


def format_market_summary(result: dict) -> str:
    """Convert market data to OpenUI Lang."""
    question = _escape(result.get("question", "Unknown"))
    yes_price = result.get("yes_price", 0.5)
    no_price = result.get("no_price", 0.5)
    volume = result.get("volume", "N/A")
    liquidity = result.get("liquidity", "N/A")

    lines = [
        "root = Stack([mkt])",
        f'mkt = MarketCard("{question}", {yes_price}, {no_price}, "{volume}", "{liquidity}", "Polymarket")',
    ]

    return "\n".join(lines)


def format_fleet_status(result: dict) -> str | None:
    """Convert cortex_get_fleet_status output to OpenUI Lang."""
    if "error" in result or "orchestrator" not in result:
        return None

    orch = result.get("orchestrator", {})
    snap = result.get("snapshot", {})
    traders = result.get("traders", [])
    portfolio = result.get("portfolio", {})

    mode = orch.get("mode", "unknown")
    paused = orch.get("paused", False)
    status_text = f"{'Paused' if paused else 'Active'} ({mode})"
    status_variant = "warning" if paused else "success"

    daily_pnl = snap.get("daily_pnl", 0)
    pnl_str = f"${daily_pnl:+.2f}"

    children = ["statusBadge", "exposure", "pnl", "positions", "traderTable"]
    lines = [
        f'root = Stack([header, traderCard])',
        f'header = Card("Fleet Status", "{status_text}", "{"amber" if paused else "emerald"}", [statusBadge, exposure, pnl, positions])',
        f'statusBadge = Badge("{status_text}", "{status_variant}")',
        f'exposure = KeyValue("Gross Exposure", "${snap.get("gross_exposure_usd", 0):.2f}", true)',
        f'pnl = KeyValue("Daily P&L", "{pnl_str}", true)',
        f'positions = KeyValue("Open Positions", "{portfolio.get("open_positions", 0)}", false)',
    ]

    # Trader table
    if traders:
        headers_str = '["Trader", "Mode", "Status"]'
        rows = []
        for t in traders[:10]:
            name = _escape(t.get("name", "Unknown"))
            tmode = t.get("mode", "?")
            paused_t = t.get("paused", False)
            enabled_t = t.get("enabled", False)
            st = "Paused" if paused_t else ("Active" if enabled_t else "Disabled")
            rows.append(f'["{name}", "{tmode}", "{st}"]')
        rows_str = "[" + ", ".join(rows) + "]"
        lines.append(f'traderCard = Card("Traders ({len(traders)})", "", "cyan", [traderTable])')
        lines.append(f'traderTable = DataTable({headers_str}, {rows_str})')
    else:
        lines.append('traderCard = Card("Traders", "", "cyan", [noTraders])')
        lines.append('noTraders = TextBlock("No traders configured.", "caption")')

    return "\n".join(lines)


def format_portfolio_overview(result: dict) -> str | None:
    """Convert get_portfolio_performance or get_open_positions output to OpenUI Lang."""
    if "error" in result:
        return None

    # Handle portfolio performance
    if "total_pnl" in result or "positions" in result:
        positions = result.get("positions", [])
        total_pnl = result.get("total_pnl", 0)
        total_cost = result.get("total_cost", 0)

        lines = [
            'root = Stack([summary, posCard])',
            f'summary = Card("Portfolio Summary", "", "purple", [pnl, cost, count])',
            f'pnl = KeyValue("Total P&L", "${total_pnl:.2f}", true)',
            f'cost = KeyValue("Total Cost", "${total_cost:.2f}", true)',
            f'count = KeyValue("Positions", "{len(positions)}", false)',
        ]

        if positions:
            headers_str = '["Market", "Side", "Cost", "Size"]'
            rows = []
            for p in positions[:15]:
                market = _escape(str(p.get("market_id", p.get("market_question", "?")))[:40])
                side = p.get("outcome", p.get("direction", "?"))
                cost = f"${float(p.get('average_cost', p.get('entry_price', 0))):.3f}"
                size = str(p.get("size", p.get("notional_usd", "?")))
                rows.append(f'["{market}", "{side}", "{cost}", "{size}"]')
            rows_str = "[" + ", ".join(rows) + "]"
            lines.append(f'posCard = Card("Positions ({len(positions)})", "", "cyan", [posTable])')
            lines.append(f'posTable = DataTable({headers_str}, {rows_str})')
        else:
            lines.append('posCard = Card("Positions", "", "cyan", [noPosText])')
            lines.append('noPosText = TextBlock("No open positions.", "caption")')

        return "\n".join(lines)

    return None


def format_strategy_performance(result: dict) -> str | None:
    """Convert get_strategy_performance output to OpenUI Lang."""
    if "error" in result or "total_trades" not in result:
        return None

    sid = _escape(result.get("strategy_id", "Unknown"))
    total = result.get("total_trades", 0)
    wins = result.get("wins", 0)
    win_rate = result.get("win_rate", 0)
    pnl = result.get("total_pnl_usd", 0)

    lines = [
        'root = Stack([perf])',
        f'perf = Card("{sid} Performance", "{total} trades", "purple", [wrGauge, pnlKv, winsKv])',
        f'wrGauge = ScoreGauge("Win Rate", {win_rate}, "percent")',
        f'pnlKv = KeyValue("Total P&L", "${pnl:.4f}", true)',
        f'winsKv = KeyValue("Wins / Total", "{wins}/{total}", false)',
    ]

    return "\n".join(lines)


def format_wallet_leaderboard(result: dict) -> str | None:
    """Convert wallet leaderboard output to OpenUI Lang."""
    if "error" in result:
        return None

    wallets = result.get("wallets", [])
    if not wallets:
        return None

    lines = [
        'root = Stack([lbCard])',
        f'lbCard = Card("Wallet Leaderboard", "{result.get("category", "overall")} - {result.get("time_period", "30d")}", "purple", [lbTable])',
    ]

    headers_str = '["Rank", "Address", "Win Rate", "PnL", "Score"]'
    rows = []
    for i, w in enumerate(wallets[:20], 1):
        addr = str(w.get("address", "?"))[:10] + "..."
        wr = f"{float(w.get('win_rate', 0)) * 100:.0f}%" if w.get("win_rate") else "N/A"
        pnl = f"${float(w.get('total_pnl', w.get('pnl', 0))):.2f}" if w.get("total_pnl") or w.get("pnl") else "N/A"
        score = f"{float(w.get('rank_score', 0)):.1f}" if w.get("rank_score") else "N/A"
        rows.append(f'["#{i}", "{addr}", "{wr}", "{pnl}", "{score}"]')

    rows_str = "[" + ", ".join(rows) + "]"
    lines.append(f'lbTable = DataTable({headers_str}, {rows_str})')

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Auto-format dispatcher: given tool name + output, return OpenUI if applicable
# ---------------------------------------------------------------------------

_TOOL_FORMATTERS: dict[str, callable] = {
    "cortex_get_fleet_status": format_fleet_status,
    "get_portfolio_performance": format_portfolio_overview,
    "get_open_positions": format_portfolio_overview,
    "get_strategy_performance": format_strategy_performance,
    "get_wallet_leaderboard": format_wallet_leaderboard,
}


def auto_format_tool_result(tool_name: str, output: dict) -> str | None:
    """Try to auto-format a tool result as OpenUI Lang.

    Returns the OpenUI Lang string wrapped in ```openui fences, or None.
    """
    formatter = _TOOL_FORMATTERS.get(tool_name)
    if not formatter:
        return None
    try:
        lang = formatter(output)
        if lang:
            return f"\n\n```openui\n{lang}\n```\n"
    except Exception:
        pass
    return None
