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
